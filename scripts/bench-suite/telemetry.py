import json
import sys
from pathlib import Path
from typing import Dict, Any, List

from kubernetes import client
from kubernetes.client import ApiException

from utils import log


def collect_controller_metrics(v1: client.CoreV1Api, results_dir: Path, controller_ns: str, service_name: str,
                               metrics_port: str, pod_label: str):
    """
    Finds the controller service and scrapes its /metrics endpoint via the K8s API proxy.
    Also checks if the backing pod is running as a sanity check.
    """
    log(f"Attempting to collect metrics from service '{service_name}' in namespace '{controller_ns}'...")
    try:
        # Sanity Check: First, verify that a pod backing the service is actually running.
        pods = v1.list_namespaced_pod(namespace=controller_ns, label_selector=pod_label).items
        running_pods = [p for p in pods if p.status.phase == "Running"]

        if not running_pods:
            msg = f"Warning: No running controller pod found with label '{pod_label}' in namespace '{controller_ns}'. Cannot collect metrics."
            log(msg)
            (results_dir / "controller_metrics.txt").write_text(f"ERROR: {msg}")
            return

        log(f"Found {len(running_pods)} running pod(s) backing the service. Proceeding to scrape service.")

        # Use the API proxy to connect to the SERVICE, not the pod.
        # The API server will handle routing to a healthy pod.
        metrics_text = v1.connect_get_namespaced_service_proxy_with_path(
            name=service_name,
            namespace=controller_ns,
            path="metrics",
            port=metrics_port  # The service port name or number
        )

        # Save the output to a file
        out_path = results_dir / "controller_metrics.txt"
        out_path.write_text(metrics_text)
        log(f"Successfully saved controller metrics to {out_path}")

    except ApiException as e:
        error_msg = f"ERROR: Failed to connect to controller service '{service_name}': {e.reason} (Status: {e.status})"
        log(error_msg)
        (results_dir / "controller_metrics.txt").write_text(error_msg)
    except Exception as e:
        error_msg = f"ERROR: An unexpected error occurred while collecting controller metrics: {e}"
        log(error_msg)
        (results_dir / "controller_metrics.txt").write_text(error_msg)


def measure_jobs_via_api(batch_v1: client.BatchV1Api, ns: str, names: List[str]) -> Dict[str, Any]:
    """Measures job duration by querying the Kubernetes API for start/completion times."""
    out = {}
    for name in names:
        try:
            job = batch_v1.read_namespaced_job(name=name, namespace=ns)
            st = job.status
            if st.start_time and st.completion_time:
                dur_ms = int((st.completion_time - st.start_time).total_seconds() * 1000)
                out[name] = {"metric": "duration_ms", "value_ms": dur_ms}
        except ApiException:
            continue
    return out


def get_job_logs(v1: client.CoreV1Api, ns: str, job_name: str) -> str:
    """Gets logs from all pods created by a specific job."""
    try:
        pods = v1.list_namespaced_pod(namespace=ns, label_selector=f"job-name={job_name}")
        logs = []
        for pod in pods.items:
            try:
                pod_logs = v1.read_namespaced_pod_log(name=pod.metadata.name, namespace=ns)
                logs.append(f"=== Pod {pod.metadata.name} ===\n{pod_logs}")
            except ApiException as e:
                logs.append(f"=== Pod {pod.metadata.name} === ERROR: {e}")
        result = "\n".join(logs)
        sys.stdout.write(result)
        return result
    except ApiException as e:
        log(f"Error getting logs for job {job_name}: {e}")
        return ""


def get_events(v1: client.CoreV1Api, ns: str, out_path: Path):
    """Retrieves and saves all events from a namespace."""
    try:
        events = sorted([e.to_dict() for e in v1.list_namespaced_event(ns).items],
                        key=lambda x: (x.get("last_timestamp") or x.get("event_time") or ""))
        out_path.write_text(json.dumps(events, indent=2, default=str))
    except ApiException as e:
        log(f"Warning: could not retrieve events for {ns}: {e}")


def get_pod_node_map(v1: client.CoreV1Api, ns: str, out_path: Path):
    """Creates a CSV mapping pods to the nodes they are scheduled on."""
    try:
        pods = v1.list_namespaced_pod(ns).items
        lines = ['"pod_name","node_name","phase"']
        lines.extend([f'"{p.metadata.name}","{p.spec.node_name or ""}","{p.status.phase or "NA"}"' for p in pods])
        out_path.write_text("\n".join(lines))
    except ApiException as e:
        log(f"Warning: could not generate pod-node map for {ns}: {e}")


def get_nodes_info(v1: client.CoreV1Api, out_path: Path):
    """Saves node information similar to 'kubectl get nodes'."""
    try:
        nodes = v1.list_node().items
        lines = ["NAME STATUS VERSION"]
        for n in nodes:
            status = "Ready" if any(
                c.type == "Ready" and c.status == "True" for c in n.status.conditions or []) else "NotReady"
            version = n.status.node_info.kubelet_version
            lines.append(f"{n.metadata.name} {status} {version}")
        out_path.write_text("\n".join(lines))
    except ApiException as e:
        log(f"Warning: could not get node info: {e}")
