# telemetry.py
import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

from kubernetes import client
from kubernetes.client import ApiException

import k8s_helpers
from utils import log

def measure_http(v1: client.CoreV1Api, ns_local: str, results_dir: Path, args, http_svc_url: str) -> Dict[str, Any]:
    """Runs the HTTP benchmark using 'hey' from the toolbox pod."""
    log(f"HTTP warmup {args.http_warmup_sec}s @ {http_svc_url}")
    k8s_helpers.toolbox_exec(v1, ns_local, f"/hey -z {args.http_warmup_sec}s -q {args.http_qps} -c {args.http_conc} {http_svc_url}")

    log(f"Running HTTP benchmark for {args.http_test_sec}s...")
    out = k8s_helpers.toolbox_exec(v1, ns_local, f"/hey -z {args.http_test_sec}s -q {args.http_qps} -c {args.http_conc} {http_svc_url}")
    (results_dir / "http_benchmark.txt").write_text(out)

    def extract(line: str) -> Optional[float]:
        try:
            return float(line.strip().split()[-2]) * 1000.0
        except (ValueError, IndexError):
            return None

    p50 = p95 = p99 = rps = None
    for ln in out.splitlines():
        if "50% in" in ln: p50 = extract(ln)
        elif "95% in" in ln: p95 = extract(ln)
        elif "99% in" in ln: p99 = extract(ln)
        elif "Requests/sec:" in ln:
            try: rps = float(ln.split(":")[1].strip())
            except Exception: pass

    return {"http-latency": {"metric": "latency", "p50_ms": p50, "p95_ms": p95, "p99_ms": p99, "rps": rps}}

def measure_stream(apps_v1: client.AppsV1Api, ns_offloaded: str, results_dir: Path) -> Dict[str, Any]:
    """Parses stream-data-generator logs to extract stream processing metrics."""
    log_file = results_dir / "stream-data-generator_logs.txt"
    if not log_file.exists():
        return {}

    avg_ms = anomalies = slo_viol = None
    for ln in log_file.read_text().splitlines():
        if "Average latency:" in ln:
            try: avg_ms = float(ln.split(":")[-1].replace("ms", "").strip())
            except Exception: pass
        elif "Anomalies detected:" in ln:
            try: anomalies = int(ln.split(":")[-1].strip())
            except Exception: pass
        elif "SLO violations" in ln:
            try: slo_viol = int(ln.split(":")[-1].strip())
            except Exception: pass

    return {"stream-processor": {"metric": "latency", "avg_ms": avg_ms, "anomalies": anomalies, "slo_violations": slo_viol}}

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
        out_path.write_text(json.dumps(events, indent=2))
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
            status = "Ready" if any(c.type == "Ready" and c.status == "True" for c in n.status.conditions or []) else "NotReady"
            version = n.status.node_info.kubelet_version
            lines.append(f"{n.metadata.name} {status} {version}")
        out_path.write_text("\n".join(lines))
    except ApiException as e:
        log(f"Warning: could not get node info: {e}")