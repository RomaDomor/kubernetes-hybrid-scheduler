import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Any, List, Optional

from kubernetes import client
from kubernetes.client import ApiException

from utils import log


# ---------------------------------------------------------------------------
# Controller Metrics (Prometheus scrape)
# ---------------------------------------------------------------------------

def collect_controller_metrics(
    v1: client.CoreV1Api,
    results_dir: Path,
    controller_ns: str,
    service_name: str,
    metrics_port: str,
    pod_label: str,
):
    """Scrape the scheduler controller's /metrics endpoint via the K8s API proxy."""
    log(f"Collecting metrics from service '{service_name}' in '{controller_ns}'...")
    try:
        pods = v1.list_namespaced_pod(
            namespace=controller_ns, label_selector=pod_label
        ).items
        running_pods = [p for p in pods if p.status.phase == "Running"]

        if not running_pods:
            msg = (f"Warning: No running controller pod with label '{pod_label}' "
                   f"in '{controller_ns}'. Cannot collect metrics.")
            log(msg)
            (results_dir / "controller_metrics.txt").write_text(f"ERROR: {msg}")
            return

        log(f"Found {len(running_pods)} running pod(s).")
        metrics_text = v1.connect_get_namespaced_service_proxy_with_path(
            name=service_name,
            namespace=controller_ns,
            path=f":{metrics_port}/metrics",
        )
        out_path = results_dir / "controller_metrics.txt"
        out_path.write_text(metrics_text)
        log(f"Controller metrics saved to {out_path}")

    except ApiException as e:
        msg = f"ERROR: Failed to connect to '{service_name}': {e.reason} (Status: {e.status})"
        log(msg)
        (results_dir / "controller_metrics.txt").write_text(msg)
    except Exception as e:
        msg = f"ERROR: Unexpected error collecting controller metrics: {e}"
        log(msg)
        (results_dir / "controller_metrics.txt").write_text(msg)


# ---------------------------------------------------------------------------
# Job Duration via API
# ---------------------------------------------------------------------------

def measure_jobs_via_api(
    batch_v1: client.BatchV1Api, ns: str, names: List[str]
) -> Dict[str, Any]:
    """Extract job duration from Kubernetes start_time / completion_time."""
    out = {}
    for name in names:
        try:
            job = batch_v1.read_namespaced_job(name=name, namespace=ns)
            st = job.status
            if st.start_time and st.completion_time:
                dur_ms = int(
                    (st.completion_time - st.start_time).total_seconds() * 1000
                )
                out[name] = {"metric": "duration_ms", "value_ms": dur_ms}
            elif st.start_time and not st.completion_time:
                # Job still running or failed without a completion time
                out[name] = {"metric": "duration_ms", "value_ms": None,
                             "reason": "no_completion_time"}
        except ApiException:
            continue
    return out


# ---------------------------------------------------------------------------
# Log Collection
# ---------------------------------------------------------------------------

def get_job_logs(v1: client.CoreV1Api, ns: str, job_name: str) -> str:
    """Concatenate logs from all pods created by a job."""
    try:
        pods = v1.list_namespaced_pod(
            namespace=ns, label_selector=f"job-name={job_name}"
        )
        logs = []
        for pod in pods.items:
            try:
                pod_logs = v1.read_namespaced_pod_log(
                    name=pod.metadata.name, namespace=ns
                )
                logs.append(f"=== Pod {pod.metadata.name} ===\n{pod_logs}")
            except ApiException as e:
                logs.append(f"=== Pod {pod.metadata.name} === ERROR: {e}")
        result = "\n".join(logs)
        sys.stdout.write(result)
        return result
    except ApiException as e:
        log(f"Error getting logs for job {job_name}: {e}")
        return ""


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

def get_events(v1: client.CoreV1Api, ns: str, out_path: Path):
    """Save all events from a namespace, sorted by timestamp."""
    try:
        events = sorted(
            [e.to_dict() for e in v1.list_namespaced_event(ns).items],
            key=lambda x: (x.get("last_timestamp") or x.get("event_time") or ""),
        )
        out_path.write_text(json.dumps(events, indent=2, default=str))
    except ApiException as e:
        log(f"Warning: could not retrieve events for {ns}: {e}")


# ---------------------------------------------------------------------------
# Node Info
# ---------------------------------------------------------------------------

def get_nodes_info(v1: client.CoreV1Api, out_path: Path):
    """Save node status table similar to 'kubectl get nodes'."""
    try:
        nodes = v1.list_node().items
        lines = ["NAME  STATUS  VERSION  CPU  MEMORY  LABELS"]
        for n in nodes:
            status = "Ready" if any(
                c.type == "Ready" and c.status == "True"
                for c in (n.status.conditions or [])
            ) else "NotReady"
            version = n.status.node_info.kubelet_version
            cpu     = n.status.allocatable.get("cpu", "?")
            mem     = n.status.allocatable.get("memory", "?")
            # Show only hybrid/liqo-related labels to keep the line readable
            interesting_keys = {
                "node.hybrid.io/location",
                "liqo.io/remote-cluster",
                "topology.kubernetes.io/zone",
                "topology.kubernetes.io/region",
                "kubernetes.io/hostname",
            }
            labels = {k: v for k, v in (n.metadata.labels or {}).items()
                      if k in interesting_keys}
            lines.append(
                f"{n.metadata.name}  {status}  {version}  {cpu}  {mem}  {labels}"
            )
        out_path.write_text("\n".join(lines))
    except ApiException as e:
        log(f"Warning: could not get node info: {e}")


# ---------------------------------------------------------------------------
# Pod → Node Mapping with Cluster Type Detection
# ---------------------------------------------------------------------------

def _detect_cluster_type(
    node: Any,
    rr_node_label: str,
) -> str:
    """
    Classify a node as edge / fog / cloud using the following heuristics
    (first match wins):

    1. The label key given by *rr_node_label* (e.g. ``node.hybrid.io/location``).
    2. ``liqo.io/remote-cluster`` = "true"  → "remote"
    3. Node name contains "cloud" / "fog" / "edge" substrings.
    4. Fallback: "edge" (assume local cluster if nothing matches).
    """
    labels = (node.metadata.labels or {}) if node else {}
    node_name = (node.metadata.name or "").lower() if node else ""

    # 1. Explicit location label
    if rr_node_label and rr_node_label in labels:
        return labels[rr_node_label]

    # 2. Liqo remote-cluster label
    if labels.get("liqo.io/remote-cluster") == "true":
        # Try to get a more specific name from the remote cluster ID
        cluster_id = labels.get("liqo.io/remote-cluster-id", "")
        if "cloud" in cluster_id.lower():
            return "cloud"
        if "fog" in cluster_id.lower():
            return "fog"
        return "remote"

    # 3. Node name heuristics
    for keyword in ("cloud", "fog", "edge"):
        if keyword in node_name:
            return keyword

    # 4. Fallback
    return "edge"


def get_pod_node_map(
    v1: client.CoreV1Api,
    ns: str,
    out_path: Path,
    rr_node_label: str = "node.hybrid.io/location",
):
    """
    Write a CSV mapping each pod to the node it ran on, including a
    ``cluster_type`` column derived from node labels/name.
    """
    try:
        pods  = v1.list_namespaced_pod(ns).items
        # Build node lookup (name → node object)
        nodes = {n.metadata.name: n for n in v1.list_node().items}

        lines = ['"pod_name","node_name","phase","cluster_type"']
        for p in pods:
            node_name    = p.spec.node_name or ""
            phase        = p.status.phase or "NA"
            node_obj     = nodes.get(node_name)
            cluster_type = _detect_cluster_type(node_obj, rr_node_label) if node_obj else "unknown"
            lines.append(
                f'"{p.metadata.name}","{node_name}","{phase}","{cluster_type}"'
            )
        out_path.write_text("\n".join(lines))
    except ApiException as e:
        log(f"Warning: could not generate pod-node map for {ns}: {e}")


# ---------------------------------------------------------------------------
# Placement Statistics
# ---------------------------------------------------------------------------

def collect_placement_stats(
    v1: client.CoreV1Api,
    ns: str,
    out_path: Path,
    rr_node_label: str = "node.hybrid.io/location",
):
    """
    Aggregate placement decisions into a summary JSON with per-cluster counts
    for each workload.  This is the primary artefact used by the visualiser
    to compare scheduling strategies.

    Output schema:
    {
      "by_cluster": {"edge": 3, "fog": 2, "cloud": 5},
      "by_workload": {
        "cpu-batch": {"edge": 0, "fog": 0, "cloud": 1},
        ...
      },
      "offload_rate_pct": 60.0   # % of pods NOT on edge
    }
    """
    try:
        pods  = v1.list_namespaced_pod(ns).items
        nodes = {n.metadata.name: n for n in v1.list_node().items}

        by_cluster: Dict[str, int] = defaultdict(int)
        by_workload: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        for p in pods:
            node_name    = p.spec.node_name or ""
            node_obj     = nodes.get(node_name)
            cluster_type = _detect_cluster_type(node_obj, rr_node_label) if node_obj else "unknown"

            # Derive workload name from pod labels
            labels   = p.metadata.labels or {}
            workload = (
                labels.get("job-name")
                or labels.get("app")
                or p.metadata.name
            )
            by_cluster[cluster_type] += 1
            by_workload[workload][cluster_type] += 1

        total = sum(by_cluster.values())
        edge  = by_cluster.get("edge", 0)
        offload_rate = round((1 - edge / total) * 100, 1) if total > 0 else 0.0

        stats = {
            "total_pods":      total,
            "by_cluster":      dict(by_cluster),
            "by_workload":     {k: dict(v) for k, v in by_workload.items()},
            "offload_rate_pct": offload_rate,
        }
        out_path.write_text(json.dumps(stats, indent=2))
        log(f"Placement stats: {dict(by_cluster)} (offload rate {offload_rate}%)")

    except ApiException as e:
        log(f"Warning: could not collect placement stats for {ns}: {e}")
