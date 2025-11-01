import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

from kubernetes import client
from kubernetes.client import ApiException

import k8s_helpers
from utils import log

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
            status = "Ready" if any(c.type == "Ready" and c.status == "True" for c in n.status.conditions or []) else "NotReady"
            version = n.status.node_info.kubelet_version
            lines.append(f"{n.metadata.name} {status} {version}")
        out_path.write_text("\n".join(lines))
    except ApiException as e:
        log(f"Warning: could not get node info: {e}")