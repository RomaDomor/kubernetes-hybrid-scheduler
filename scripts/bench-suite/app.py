#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

import yaml  # pip install pyyaml
from kubernetes import client, config, watch
from kubernetes.client import ApiException, V1DeleteOptions
from kubernetes.stream import stream

# --- Workloads designated as clients to run in the local namespace ---
CLIENT_WORKLOADS = {("Pod", "toolbox"), ("Job", "stream-data-generator")}

# -------------------- Utilities --------------------
def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def file_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


def read_yaml_multi(path: Path) -> List[Dict[str, Any]]:
    docs: List[Dict[str, Any]] = []
    with path.open() as f:
        for doc in yaml.safe_load_all(f):
            if doc:
                docs.append(doc)
    return docs


def get_annotations(doc: Dict[str, Any]) -> Dict[str, str]:
    ann = {}
    meta = doc.get("metadata", {})
    a1 = meta.get("annotations", {}) or {}
    ann.update(a1)
    # also check template.metadata.annotations for Pod templates
    spec = doc.get("spec", {}) or {}
    tmpl = spec.get("template", {}) or {}
    tmeta = tmpl.get("metadata", {}) or {}
    a2 = tmeta.get("annotations", {}) or {}
    ann.update(a2)
    return {k: str(v) for k, v in ann.items()}


def to_int(val: Optional[str]) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        return int(str(val))
    except Exception:
        try:
            return int(float(str(val)))
        except Exception:
            return None


# -------------------- K8s helpers --------------------
def kube_init(context: str, kubeconfig_path: str):
    try:
        if context:
            config.load_kube_config(context=context)
        else:
            config.load_kube_config(config_file=kubeconfig_path)
    except Exception:
        config.load_incluster_config()


def ensure_namespace(v1: client.CoreV1Api, ns: str):
    try:
        v1.read_namespace(ns)
    except ApiException as e:
        if e.status == 404:
            v1.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=ns)))
        else:
            raise


def apply_yaml_objects(docs: List[Dict[str, Any]], ns_offloaded: str, ns_local: str):
    """Apply YAML objects splitting client vs server namespaces."""
    from kubernetes import utils

    k8s_client = client.ApiClient()
    offloaded_docs, local_docs = [], []

    for doc in docs:
        kind = doc.get("kind")
        name = (doc.get("metadata") or {}).get("name")
        if (kind, name) in CLIENT_WORKLOADS:
            # Update PROCESSOR_URL for stream-data-generator
            if name == "stream-data-generator" and kind == "Job":
                containers = doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
                for container in containers:
                    env_vars = container.get("env", [])
                    for env_var in env_vars:
                        if env_var.get("name") == "PROCESSOR_URL":
                            env_var["value"] = f"http://stream-processor.{ns_offloaded}:8080"
            local_docs.append(doc)
        else:
            offloaded_docs.append(doc)

    def ensure_ns(doc: Dict[str, Any], ns: str):
        if doc.get("metadata") is None:
            doc["metadata"] = {}
        if (
                "namespace" not in doc["metadata"]
                and doc.get("kind") not in ["Namespace", "ClusterRole", "ClusterRoleBinding"]
        ):
            doc["metadata"]["namespace"] = ns

    if offloaded_docs:
        for d in offloaded_docs:
            ensure_ns(d, ns_offloaded)
        utils.create_from_yaml(k8s_client, yaml_objects=offloaded_docs, namespace=ns_offloaded)

    if local_docs:
        for d in local_docs:
            ensure_ns(d, ns_local)
        utils.create_from_yaml(k8s_client, yaml_objects=local_docs, namespace=ns_local)


def delete_yaml_objects(docs: List[Dict[str, Any]], ns_offloaded: str, ns_local: str):
    """Delete YAML objects from the correct namespaces."""
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    batch_v1 = client.BatchV1Api()

    for doc in docs:
        kind = doc.get("kind")
        name = (doc.get("metadata") or {}).get("name")
        if not kind or not name:
            continue
        ns = ns_local if (kind, name) in CLIENT_WORKLOADS else ns_offloaded

        try:
            if kind == "Pod":
                v1.delete_namespaced_pod(name=name, namespace=ns)
            elif kind == "Service":
                v1.delete_namespaced_service(name=name, namespace=ns)
            elif kind == "Deployment":
                apps_v1.delete_namespaced_deployment(name=name, namespace=ns)
            elif kind == "Job":
                batch_v1.delete_namespaced_job(name=name, namespace=ns)
            elif kind == "ConfigMap":
                v1.delete_namespaced_config_map(name=name, namespace=ns)
            elif kind == "Secret":
                v1.delete_namespaced_secret(name=name, namespace=ns)
        except ApiException as e:
            if e.status != 404:
                log(f"Error deleting {kind}/{name}: {e}")


def k_apply(ns_offloaded: str, ns_local: str, file_path: Path):
    docs = read_yaml_multi(file_path)
    apply_yaml_objects(docs, ns_offloaded, ns_local)


def k_delete(ns_offloaded: str, ns_local: str, file_path: Path):
    try:
        docs = read_yaml_multi(file_path)
        delete_yaml_objects(docs, ns_offloaded, ns_local)
    except Exception as e:
        log(f"Warning: Error during deletion: {e}")


def wait_pod_ready(v1: client.CoreV1Api, ns: str, name: str, timeout_sec: int = 180):
    """Wait for pod to be ready"""
    w = watch.Watch()
    timeout_time = time.time() + timeout_sec

    for event in w.stream(v1.list_namespaced_pod, namespace=ns,
                          field_selector=f"metadata.name={name}",
                          timeout_seconds=timeout_sec):
        pod = event["object"]
        event_type = event["type"]

        if event_type == "DELETED":
            w.stop()
            raise RuntimeError(f"Pod {name} was deleted before becoming ready")

        if pod.status.conditions:
            for condition in pod.status.conditions:
                if condition.type == "Ready" and condition.status == "True":
                    w.stop()
                    return

        if time.time() > timeout_time:
            w.stop()
            break

    raise TimeoutError(f"Pod {name} did not become ready within {timeout_sec} seconds")


def wait_deployment_ready(apps_v1: client.AppsV1Api, ns: str, name: str, timeout_sec: int = 300):
    """Wait for deployment rollout to complete"""
    w = watch.Watch()
    timeout_time = time.time() + timeout_sec

    for event in w.stream(apps_v1.list_namespaced_deployment, namespace=ns,
                          field_selector=f"metadata.name={name}",
                          timeout_seconds=timeout_sec):
        deployment = event["object"]
        event_type = event["type"]

        if event_type == "DELETED":
            w.stop()
            raise RuntimeError(f"Deployment {name} was deleted")

        status = deployment.status
        if (status.ready_replicas and
                status.replicas and
                status.ready_replicas == status.replicas and
                status.updated_replicas == status.replicas):
            w.stop()
            return

        if time.time() > timeout_time:
            w.stop()
            break

    raise TimeoutError(f"Deployment {name} did not become ready within {timeout_sec} seconds")


def wait_job_complete(batch_v1: client.BatchV1Api, ns: str, name: str, timeout_sec: int) -> int:
    """Wait for job to complete and return duration in seconds"""
    start_ts = int(time.time())
    log(f"Waiting for Job {ns}/{name} to complete (timeout {timeout_sec}s)...")

    w = watch.Watch()
    timeout_time = time.time() + timeout_sec

    for event in w.stream(batch_v1.list_namespaced_job, namespace=ns,
                          field_selector=f"metadata.name={name}",
                          timeout_seconds=timeout_sec):
        job = event["object"]
        event_type = event["type"]

        if event_type == "DELETED":
            w.stop()
            raise RuntimeError(f"Job {name} was deleted before completing")

        if job.status.conditions:
            for condition in job.status.conditions:
                if condition.type == "Complete" and condition.status == "True":
                    w.stop()
                    end_ts = int(time.time())
                    return end_ts - start_ts
                elif condition.type == "Failed" and condition.status == "True":
                    w.stop()
                    log(f"Job {ns}/{name} failed")
                    # Get job logs before raising
                    try:
                        get_job_logs(client.CoreV1Api(), ns, name)
                    except Exception:
                        pass
                    raise RuntimeError(f"Job {name} failed")

        if time.time() > timeout_time:
            w.stop()
            break

    log(f"Job {ns}/{name} did not complete in time; fetching logs.")
    try:
        get_job_logs(client.CoreV1Api(), ns, name)
    except Exception:
        pass
    raise TimeoutError(f"Job {name} did not complete within {timeout_sec} seconds")


def get_job_logs(v1: client.CoreV1Api, ns: str, job_name: str) -> str:
    """Get logs from all pods of a job"""
    try:
        pods = v1.list_namespaced_pod(namespace=ns,
                                      label_selector=f"job-name={job_name}")
        logs = []
        for pod in pods.items:
            try:
                pod_logs = v1.read_namespaced_pod_log(name=pod.metadata.name,
                                                      namespace=ns)
                logs.append(f"=== Pod {pod.metadata.name} ===\n{pod_logs}")
            except Exception as e:
                logs.append(f"=== Pod {pod.metadata.name} === ERROR: {e}")
        result = "\n".join(logs)
        sys.stdout.write(result)
        return result
    except Exception as e:
        log(f"Error getting job logs: {e}")
        return ""


def toolbox_exec(v1: client.CoreV1Api, ns: str, cmd: str, check=False) -> str:
    """Execute command in toolbox pod"""
    try:
        resp = stream(v1.connect_get_namespaced_pod_exec,
                      "toolbox", ns,
                      command=["sh", "-lc", cmd],
                      stderr=True, stdin=False,
                      stdout=True, tty=False,
                      _preload_content=False)

        output = ""
        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                output += resp.read_stdout()
            if resp.peek_stderr():
                output += resp.read_stderr()

        resp.close()
        if check and resp.returncode != 0:
            raise subprocess.CalledProcessError(resp.returncode, cmd, output)
        return output
    except Exception as e:
        if check:
            raise
        log(f"Command failed: {cmd}, error: {e}")
        return ""


def detect_virtual_node(v1: client.CoreV1Api) -> str:
    try:
        nodes = v1.list_node(label_selector="liqo.io/virtual-node=true").items
        if nodes:
            return nodes[0].metadata.name
    except Exception:
        pass
    return ""


# -------------------- Catalog (SLOs) --------------------
def build_catalog_from_manifests(
        ns: str, manifests_dir: Path, manifest_files: List[str]
) -> List[Dict[str, Any]]:
    catalog: List[Dict[str, Any]] = []
    for name in manifest_files:
        p = manifests_dir / name
        if not file_exists(p):
            continue
        for doc in read_yaml_multi(p):
            kind = doc.get("kind")
            meta = doc.get("metadata", {}) or {}
            obj_name = meta.get("name")
            if not kind or not obj_name:
                continue
            ann = get_annotations(doc)
            slo_class = ann.get("slo.hybrid.io/class")
            lat_ms = to_int(ann.get("slo.hybrid.io/latencyTargetMs"))
            ddl_ms = to_int(ann.get("slo.hybrid.io/deadlineMs"))
            max_offload_ms = to_int(ann.get("slo.hybrid.io/max-offload-penalty.ms"))
            catalog.append(
                {
                    "kind": kind,
                    "name": obj_name,
                    "namespace": ns,
                    "slo": {
                        "class": slo_class,
                        "latency_ms": lat_ms,
                        "deadline_ms": ddl_ms,
                        "max_offload_penalty_ms": max_offload_ms,
                    },
                    "source_manifest": str(p),
                }
            )
    return catalog


def save_catalog(catalog: List[Dict[str, Any]], results_dir: Path):
    (results_dir / "catalog.json").write_text(json.dumps(catalog, indent=2))
    # also CSV
    lines = ["kind,name,namespace,class,latency_ms,deadline_ms,max_offload_penalty_ms"]
    for w in catalog:
        s = w["slo"]
        lines.append(
            ",".join(
                [
                    w["kind"],
                    w["name"],
                    w["namespace"],
                    str(s.get("class") or ""),
                    str(s.get("latency_ms") or ""),
                    str(s.get("deadline_ms") or ""),
                    str(s.get("max_offload_penalty_ms") or ""),
                ]
            )
        )
    (results_dir / "catalog.csv").write_text("\n".join(lines))


# -------------------- Measurement --------------------
def measure_http(
        v1: client.CoreV1Api,
        ns: str,
        results_dir: Path,
        args: argparse.Namespace,
        http_svc_url: str,
) -> Dict[str, Any]:
    # warmup
    log(f"HTTP warmup {args.http_warmup_sec}s @ {http_svc_url}")
    try:
        toolbox_exec(
            v1,
            ns,
            f"/hey -z {args.http_warmup_sec}s -q {args.http_qps} -c {args.http_conc} {http_svc_url}",
            check=False,
        )
    except Exception:
        pass

    # run
    log(
        f"HTTP benchmark {args.http_test_sec}s, qps={args.http_qps}, conc={args.http_conc}"
    )
    out = toolbox_exec(
        v1,
        ns,
        f"/hey -z {args.http_test_sec}s -q {args.http_qps} -c {args.http_conc} {http_svc_url}",
        check=False,
    )
    (results_dir / "http_benchmark.txt").write_text(out)

    # parse p50/p95/p99 and requests/sec
    def extract(sec_line: str) -> Optional[float]:
        # lines like "  50% in 0.0041 secs"
        try:
            toks = sec_line.strip().split()
            val = float(toks[-2])  # seconds
            return val * 1000.0
        except Exception:
            return None

    p50 = p95 = p99 = None
    rps = None
    for ln in out.splitlines():
        if ln.strip().startswith("50% in"):
            p50 = extract(ln)
        elif ln.strip().startswith("95% in"):
            p95 = extract(ln)
        elif ln.strip().startswith("99% in"):
            p99 = extract(ln)
        elif ln.strip().startswith("Requests/sec:"):
            try:
                rps = float(ln.split(":")[1].strip())
            except Exception:
                pass

    measures = {
        "http-latency": {
            "metric": "latency",
            "p50_ms": p50,
            "p95_ms": p95,
            "p99_ms": p99,
            "rps": rps,
        }
    }
    return measures


def measure_stream(
        apps_v1: client.AppsV1Api,
        v1: client.CoreV1Api,
        ns_offloaded: str,
        ns_local: str,
        results_dir: Path,
        stream_svc_url: str,
        warmup_sec: int,
) -> Dict[str, Any]:
    # verify deployment exists
    try:
        apps_v1.read_namespaced_deployment(name="stream-processor", namespace=ns_offloaded)
    except ApiException as e:
        if e.status == 404:
            log("Stream processor not deployed; skipping stream measurement")
            return {}
        raise

    log(f"Stream processor warmup {warmup_sec}s @ {stream_svc_url}/health")
    try:
        toolbox_exec(v1, ns_local, f"curl -sf {stream_svc_url}/health", check=False)
    except Exception:
        pass

    # The generator is a Job; we will wait later and parse its logs
    # Parse generator logs if exist after job completes
    avg_ms = None
    anomalies = None
    slo_viol = None
    gen_log = results_dir / "stream-data-generator_logs.txt"
    if gen_log.exists():
        txt = gen_log.read_text()
        for ln in txt.splitlines():
            if "Average latency:" in ln:
                # Average latency: 10.89ms
                try:
                    v = ln.split("Average latency:")[1].strip().split("ms")[0].strip()
                    avg_ms = float(v)
                except Exception:
                    pass
            if "Anomalies detected:" in ln:
                try:
                    anomalies = int(ln.split(":")[1].strip())
                except Exception:
                    pass
            if "SLO violations" in ln:
                try:
                    slo_viol = int(ln.split(":")[1].strip())
                except Exception:
                    pass

    measures = {}
    measures["stream-processor"] = {
        "metric": "latency",
        "avg_ms": avg_ms,
        "anomalies": anomalies,
        "slo_violations": slo_viol,
    }
    return measures


def measure_jobs_via_api(batch: client.BatchV1Api, ns: str, names: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for name in names:
        try:
            job = batch.read_namespaced_job(name=name, namespace=ns)
        except ApiException:
            continue
        st = job.status
        dur_ms = None
        if st.start_time and st.completion_time:
            dur_ms = int((st.completion_time - st.start_time).total_seconds() * 1000)
        out[name] = {"metric": "duration_ms", "value_ms": dur_ms}
    return out


def save_measures(measures: Dict[str, Any], results_dir: Path):
    (results_dir / "measures.json").write_text(json.dumps(measures, indent=2))


# -------------------- SLO Evaluation --------------------
def evaluate_slos(
        catalog: List[Dict[str, Any]],
        measures: Dict[str, Any],
        results_dir: Path,
        latency_policy_metric: str,
) -> Dict[str, Any]:
    rows = ["workload,kind,class,slo_target_ms,measure_ms,metric,pass,reason"]
    report_lines: List[str] = []
    summary: Dict[str, Any] = {"items": [], "policy": {"latency_metric": latency_policy_metric}}
    def add(workload, kind, klass, slo_ms, meas_ms, metric, passed, reason=""):
        rows.append(f"{workload},{kind},{klass},{slo_ms},{meas_ms},{metric},{str(passed).lower()},{reason}")
        rtxt = f" ({reason})" if (reason and not passed) else ""
        line = f"[{workload}] {klass or 'NA'} target {slo_ms}ms {metric} -> {meas_ms}ms: {'PASS' if passed else 'FAIL'}{rtxt}"
        report_lines.append(line)
        summary["items"].append(
            {
                "workload": workload,
                "kind": kind,
                "class": klass,
                "metric": metric,
                "target_ms": slo_ms,
                "measured_ms": meas_ms,
                "pass": bool(passed),
                "reason": reason or None,
            }
        )

    # Build quick index of catalog by name
    idx = {(w["name"], w["kind"]): w for w in catalog}
    # HTTP latency (Deployment)
    http_meas = measures.get("http-latency") or {}
    http_cat = idx.get(("http-latency", "Deployment"))
    if http_cat:
        klass = (http_cat["slo"] or {}).get("class")
        slo_lat = (http_cat["slo"] or {}).get("latency_ms")
        metric = latency_policy_metric
        meas_ms = None
        if metric == "p50":
            meas_ms = http_meas.get("p50_ms")
        elif metric == "p99":
            meas_ms = http_meas.get("p99_ms")
        else:
            meas_ms = http_meas.get("p95_ms")
        if slo_lat and meas_ms is not None:
            add("http-latency", "Deployment", klass, slo_lat, round(meas_ms, 2), metric, meas_ms <= slo_lat)
        elif slo_lat:
            # Record missing measurement as FAIL
            add("http-latency", "Deployment", klass, slo_lat, None, metric, False, reason="no_measurement")

    # Stream processor (Deployment) using avg_ms
    stream_meas = measures.get("stream-processor") or {}
    stream_cat = idx.get(("stream-processor", "Deployment"))
    if stream_cat:
        klass = (stream_cat["slo"] or {}).get("class")
        slo_lat = (stream_cat["slo"] or {}).get("latency_ms")
        meas_ms = stream_meas.get("avg_ms")
        if slo_lat and meas_ms is not None:
            add("stream-processor", "Deployment", klass, slo_lat, round(meas_ms, 2), "avg", meas_ms <= slo_lat)
        elif slo_lat:
            add("stream-processor", "Deployment", klass, slo_lat, None, "avg", False, reason="no_measurement")

    # Jobs (batch): deadline_ms
    for job_name in ["cpu-batch", "io-job", "memory-intensive", "ml-infer", "build-job", "stream-data-generator"]:
        # Find catalog entry by kind Job
        job_cat = idx.get((job_name, "Job"))
        if not job_cat:
            continue
        klass = (job_cat["slo"] or {}).get("class")
        deadline = (job_cat["slo"] or {}).get("deadline_ms")
        if not deadline:
            # Some "ml" might have latency; handle if present
            slo_lat = (job_cat["slo"] or {}).get("latency_ms")
            if slo_lat:
                # Try to get from measures if any (most likely not available for Jobs)
                continue
            else:
                continue
        # measured duration_ms
        job_meas = measures.get(job_name) or {}
        dur_ms = job_meas.get("value_ms")
        if dur_ms is None:
            # fallback from jobs_durations.csv (if present)
            jd = (results_dir / "jobs_durations.csv")
            if jd.exists():
                for ln in jd.read_text().splitlines():
                    parts = ln.split(",")
                    if len(parts) >= 3 and parts[0] == job_name:
                        try:
                            sec = float(parts[2])
                            dur_ms = int(sec * 1000)
                        except Exception:
                            pass
        if dur_ms is not None:
            add(job_name, "Job", klass, deadline, int(dur_ms), "duration_ms", int(dur_ms) <= int(deadline))
        else:
            # No duration captured at all -> explicit FAIL
            add(job_name, "Job", klass, deadline, None, "duration_ms", False, reason="job_never_started_or_observed")

    # write files
    (results_dir / "slo_summary.csv").write_text("\n".join(rows))
    (results_dir / "slo_report.txt").write_text("\n".join(report_lines))
    (results_dir / "slo_summary.json").write_text(json.dumps(summary, indent=2))
    return summary


# -------------------- Orchestration --------------------
def deploy_local_load(
        apps_v1: client.AppsV1Api, ns: str, profile: str
):
    """Deploys a continuous CPU load generator to the local namespace."""
    if profile == "none":
        return

    log(f"Deploying local CPU load profile: {profile}")

    profiles = {
        "low": {"replicas": 1, "cpu_load": 50, "cpu_request": "500m", "cpu_limit": "1"},
        "medium": {"replicas": 2, "cpu_load": 75, "cpu_request": "750m", "cpu_limit": "1"},
        "high": {"replicas": 4, "cpu_load": 90, "cpu_request": "900m", "cpu_limit": "1"},
    }

    config = profiles.get(profile)
    if not config:
        log(f"Unknown load profile '{profile}', skipping.")
        return

    body = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "local-cpu-load", "labels": {"app": "local-cpu-load"}},
        "spec": {
            "replicas": config["replicas"],
            "selector": {"matchLabels": {"app": "local-cpu-load"}},
            "template": {
                "metadata": {"labels": {"app": "local-cpu-load"}},
                "spec": {
                    "restartPolicy": "Always",
                    "containers": [
                        {
                            "name": "stress",
                            "image": "debian:bookworm-slim",
                            "command": [
                                "bash",
                                "-c",
                                (
                                    "apt-get update -y && apt-get install -y --no-install-recommends stress-ng && "
                                    f"stress-ng --cpu 0 --cpu-load {config['cpu_load']} --timeout 0s"
                                ),
                            ],
                            "resources": {
                                "requests": {"cpu": config["cpu_request"], "memory": "128Mi"},
                                "limits": {"cpu": config["cpu_limit"], "memory": "256Mi"},
                            },
                        }
                    ],
                },
            },
        },
    }

    try:
        apps_v1.create_namespaced_deployment(namespace=ns, body=body)
        wait_deployment_ready(apps_v1, ns, "local-cpu-load", 240)
        log(f"Local CPU load profile '{profile}' is active.")
    except ApiException as e:
        if e.status == 409: # Already exists
            log("Local load deployment already exists. Skipping creation.")
        else:
            log(f"Error deploying local load: {e}")
            raise


def deploy_and_wait(ns_offloaded: str, ns_local: str, manifests_dir: Path, args: argparse.Namespace):
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()

    # Toolbox
    toolbox_path = manifests_dir / args.toolbox_file
    if not file_exists(toolbox_path):
        sys.exit(f"Required file not found: {toolbox_path}")
    log(f"Deploying toolbox pod from {toolbox_path} ...")
    k_apply(ns_offloaded, ns_local, toolbox_path)
    log("Waiting for toolbox to be Ready...")
    wait_pod_ready(v1, ns_local, "toolbox", 180)

    # http-latency
    http_path = manifests_dir / args.http_latency_file
    if not file_exists(http_path):
        sys.exit(f"Required file not found: {http_path}")
    log(f"Deploying http-latency from {http_path} ...")
    k_apply(ns_offloaded, ns_local, http_path)
    wait_deployment_ready(apps_v1, ns_offloaded, "http-latency", 240)

    # Jobs
    cpu_path = manifests_dir / args.cpu_batch_file
    if not file_exists(cpu_path):
        sys.exit(f"Required file not found: {cpu_path}")
    log(f"Applying CPU batch job from {cpu_path} ...")
    k_apply(ns_offloaded, ns_local, cpu_path)

    ml_path = manifests_dir / args.ml_infer_file
    if file_exists(ml_path):
        log(f"Applying ML infer job from {ml_path} ...")
        k_apply(ns_offloaded, ns_local, ml_path)
    else:
        log(f"ML infer manifest not found (optional): {ml_path}")

    io_path = manifests_dir / args.io_job_file
    if file_exists(io_path):
        log(f"Applying IO job from {io_path} ...")
        k_apply(ns_offloaded, ns_local, io_path)
    else:
        log(f"IO job manifest not found (optional): {io_path}")

    mem_path = manifests_dir / args.memory_intensive_file
    if file_exists(mem_path):
        log(f"Applying memory-intensive job from {mem_path} ...")
        k_apply(ns_offloaded, ns_local, mem_path)
    else:
        log(f"Memory-intensive manifest not found (optional): {mem_path}")

    stream_path = manifests_dir / args.stream_processor_file
    if file_exists(stream_path):
        log(f"Deploying stream processor from {stream_path} ...")
        k_apply(ns_offloaded, ns_local, stream_path)
        wait_deployment_ready(apps_v1, ns_offloaded, "stream-processor", 240)
    else:
        log(f"Stream processor manifest not found (optional): {stream_path}")

    build_path = manifests_dir / args.build_job_file
    if file_exists(build_path):
        log(f"Applying build job from {build_path} ...")
        k_apply(ns_offloaded, ns_local, build_path)
    else:
        log(f"Build job manifest not found (optional): {build_path}")


def record_job(
        batch_v1: client.BatchV1Api,
        v1: client.CoreV1Api,
        ns: str,
        results_dir: Path,
        name: str,
        timeout_sec: int,
):
    # Check if job exists
    try:
        batch_v1.read_namespaced_job(name=name, namespace=ns)
    except ApiException as e:
        if e.status == 404:
            log(f"Job {name} not found, skipping.")
            return
        raise

    dur = wait_job_complete(batch_v1, ns, name, timeout_sec)
    with (results_dir / "jobs_durations.csv").open("a") as f:
        f.write(f"{name},duration_sec,{dur}\n")

    # Save logs
    try:
        logs = get_job_logs(v1, ns, name)
        (results_dir / f"{name}_logs.txt").write_text(logs or "")
    except Exception:
        pass


def get_events(v1: client.CoreV1Api, ns: str, out_path: Path):
    try:
        ev = v1.list_namespaced_event(ns)
        events = sorted(
            [e.to_dict() for e in ev.items],
            key=lambda x: (x.get("last_timestamp") or x.get("event_time") or ""),
        )
        out_path.write_text(json.dumps(events, indent=2))
    except Exception as e:
        log(f"warn: events retrieval failed: {e}")


def get_pod_node_csv(v1: client.CoreV1Api, ns: str, out_path: Path):
    pods = v1.list_namespaced_pod(ns).items
    lines = []
    for p in pods:
        name = p.metadata.name
        node = p.spec.node_name or ""
        phase = p.status.phase or "NA"
        lines.append(f'"{name}","{node}","{phase}"')
    out_path.write_text("\n".join(lines))


def get_objects_info(v1: client.CoreV1Api, apps_v1: client.AppsV1Api, batch_v1: client.BatchV1Api, ns: str) -> str:
    """Get information about k8s objects like kubectl get"""
    lines = []

    # Get pods
    try:
        pods = v1.list_namespaced_pod(namespace=ns)
        for pod in pods.items:
            ready_count = sum(1 for c in pod.status.container_statuses or [] if c.ready)
            total_count = len(pod.status.container_statuses or [])
            lines.append(
                f"pod/{pod.metadata.name} {ready_count}/{total_count} {pod.status.phase} {pod.spec.node_name or 'N/A'}")
    except Exception as e:
        lines.append(f"Error getting pods: {e}")

    # Get deployments
    try:
        deployments = apps_v1.list_namespaced_deployment(namespace=ns)
        for dep in deployments.items:
            ready = dep.status.ready_replicas or 0
            desired = dep.status.replicas or 0
            lines.append(f"deployment/{dep.metadata.name} {ready}/{desired}")
    except Exception as e:
        lines.append(f"Error getting deployments: {e}")

    # Get services
    try:
        services = v1.list_namespaced_service(namespace=ns)
        for svc in services.items:
            cluster_ip = svc.spec.cluster_ip or "None"
            lines.append(f"service/{svc.metadata.name} {cluster_ip}")
    except Exception as e:
        lines.append(f"Error getting services: {e}")

    # Get jobs
    try:
        jobs = batch_v1.list_namespaced_job(namespace=ns)
        for job in jobs.items:
            completions = f"{job.status.succeeded or 0}/{job.spec.completions or 1}"
            lines.append(f"job/{job.metadata.name} {completions}")
    except Exception as e:
        lines.append(f"Error getting jobs: {e}")

    return "\n".join(lines)


def get_nodes_info(v1: client.CoreV1Api) -> str:
    """Get node information like kubectl get nodes"""
    try:
        nodes = v1.list_node()
        lines = []
        for node in nodes.items:
            status = "Ready" if any(c.type == "Ready" and c.status == "True"
                                    for c in node.status.conditions or []) else "NotReady"
            version = node.status.node_info.kubelet_version if node.status.node_info else "unknown"
            lines.append(f"{node.metadata.name} {status} {version}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error getting nodes: {e}"


def cleanup_workloads(ns_offloaded: str, ns_local: str, manifests_dir: Path, args: argparse.Namespace):
    log(f"Cleaning up workloads in namespaces {ns_offloaded}, {ns_local}...")
    # Clean up the local load generator if it was deployed
    if args.local_load_profile != "none":
        try:
            apps_v1 = client.AppsV1Api()
            log("Deleting local CPU load generator deployment...")
            apps_v1.delete_namespaced_deployment(name="local-cpu-load", namespace=ns_local)
        except ApiException as e:
            if e.status != 404:
                log(f"Warning: could not delete local-cpu-load deployment: {e}")
        except Exception as e:
            log(f"Warning: error during local load cleanup: {e}")

    paths = [
        manifests_dir / args.http_latency_file,
        manifests_dir / args.cpu_batch_file,
        manifests_dir / args.ml_infer_file,
        manifests_dir / args.io_job_file,
        manifests_dir / args.memory_intensive_file,
        manifests_dir / args.stream_processor_file,
        manifests_dir / args.build_job_file,
        manifests_dir / args.toolbox_file,
        ]
    for p in paths:
        if file_exists(p):
            k_delete(ns_offloaded, ns_local, p)

    try:
        # Sweep offloaded namespace
        force_delete_all_in_namespace(ns_offloaded, label_selector="app")
        # Sweep local namespace (clients)
        force_delete_all_in_namespace(ns_local, label_selector="app")
    except Exception as e:
        log(f"Force sweep (label app) failed: {e}")
        # As a last resort, sweep without selector in both namespaces
        try:
            force_delete_all_in_namespace(ns_offloaded, label_selector="")
            force_delete_all_in_namespace(ns_local, label_selector="")
        except Exception as e2:
            log(f"Force sweep (no selector) failed: {e2}")


def cleanup_namespace(ns: str):
    log(f"Deleting namespace {ns} (ALL resources)...")
    try:
        v1 = client.CoreV1Api()
        v1.delete_namespace(name=ns)
    except ApiException as e:
        if e.status != 404:
            log(f"Error deleting namespace: {e}")

# -------------------- Force cleanup (kubectl-like) --------------------
def wait_gone(check_fn, name: str, timeout_sec: int = 120, interval: float = 0.5):
    """Poll until the resource retrieval raises 404 or times out."""
    end = time.time() + timeout_sec
    while time.time() < end:
        try:
            check_fn()
            time.sleep(interval)
        except ApiException as e:
            if e.status == 404:
                return True
            time.sleep(interval)
        except Exception:
            # Treat other errors as not-gone-yet, continue
            time.sleep(interval)
    log(f"Timeout waiting for deletion: {name}")
    return False


def force_delete_all_in_namespace(ns: str, label_selector: str = "", timeout_sec: int = 180):
    """
    Force delete almost everything in a namespace, similar to:
      kubectl delete all,cm,secret,ingress,sa,role,rolebinding,pvc,job,cronjob,deploy,rs,po \
        -l <selector> -n <ns> --force --grace-period=0
    If label_selector is empty, deletes all matching kinds in the namespace.
    """
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    batch_v1 = client.BatchV1Api()
    networking_v1 = client.NetworkingV1Api()
    rbac_v1 = client.RbacAuthorizationV1Api()

    opts_fg = V1DeleteOptions(propagation_policy="Foreground")
    opts_bg = V1DeleteOptions(propagation_policy="Background")

    ls = label_selector or ""

    def del_and_wait_deploy():
        try:
            items = apps_v1.list_namespaced_deployment(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    apps_v1.delete_namespaced_deployment(name, ns, body=opts_fg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete deployment/{name}: {e}")
                wait_gone(lambda: apps_v1.read_namespaced_deployment(name, ns), f"deployment/{name}")
        except Exception as e:
            log(f"list deployments: {e}")

    def del_and_wait_rs():
        try:
            items = apps_v1.list_namespaced_replica_set(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    apps_v1.delete_namespaced_replica_set(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete rs/{name}: {e}")
                wait_gone(lambda: apps_v1.read_namespaced_replica_set(name, ns), f"rs/{name}")
        except Exception as e:
            log(f"list replicasets: {e}")

    def del_and_wait_jobs():
        try:
            items = batch_v1.list_namespaced_job(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    batch_v1.delete_namespaced_job(name, ns, body=opts_fg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete job/{name}: {e}")
                wait_gone(lambda: batch_v1.read_namespaced_job(name, ns), f"job/{name}")
        except Exception as e:
            log(f"list jobs: {e}")

    def del_and_wait_cronjobs():
        try:
            items = batch_v1.list_namespaced_cron_job(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    batch_v1.delete_namespaced_cron_job(name, ns, body=opts_fg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete cronjob/{name}: {e}")
                wait_gone(lambda: batch_v1.read_namespaced_cron_job(name, ns), f"cronjob/{name}")
        except Exception as e:
            log(f"list cronjobs: {e}")

    def del_and_wait_pods():
        try:
            items = v1.list_namespaced_pod(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    v1.delete_namespaced_pod(name, ns, body=opts_fg, grace_period_seconds=0)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete pod/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_pod(name, ns), f"pod/{name}")
        except Exception as e:
            log(f"list pods: {e}")

    def del_and_wait_svcs():
        try:
            items = v1.list_namespaced_service(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                if name == "kubernetes":  # cluster service; usually not in user ns
                    continue
                try:
                    v1.delete_namespaced_service(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete svc/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_service(name, ns), f"svc/{name}")
        except Exception as e:
            log(f"list services: {e}")

    def del_and_wait_cms():
        try:
            items = v1.list_namespaced_config_map(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    v1.delete_namespaced_config_map(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete cm/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_config_map(name, ns), f"cm/{name}")
        except Exception as e:
            log(f"list configmaps: {e}")

    def del_and_wait_secrets():
        try:
            items = v1.list_namespaced_secret(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    v1.delete_namespaced_secret(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete secret/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_secret(name, ns), f"secret/{name}")
        except Exception as e:
            log(f"list secrets: {e}")

    def del_and_wait_ingresses():
        try:
            items = networking_v1.list_namespaced_ingress(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    networking_v1.delete_namespaced_ingress(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete ingress/{name}: {e}")
                wait_gone(lambda: networking_v1.read_namespaced_ingress(name, ns), f"ingress/{name}")
        except Exception as e:
            log(f"list ingresses: {e}")

    def del_and_wait_sas():
        try:
            items = v1.list_namespaced_service_account(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    v1.delete_namespaced_service_account(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete sa/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_service_account(name, ns), f"sa/{name}")
        except Exception as e:
            log(f"list serviceaccounts: {e}")

    def del_and_wait_roles():
        try:
            items = rbac_v1.list_namespaced_role(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    rbac_v1.delete_namespaced_role(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete role/{name}: {e}")
                wait_gone(lambda: rbac_v1.read_namespaced_role(name, ns), f"role/{name}")
        except Exception as e:
            log(f"list roles: {e}")

    def del_and_wait_rolebindings():
        try:
            items = rbac_v1.list_namespaced_role_binding(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    rbac_v1.delete_namespaced_role_binding(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete rolebinding/{name}: {e}")
                wait_gone(lambda: rbac_v1.read_namespaced_role_binding(name, ns), f"rolebinding/{name}")
        except Exception as e:
            log(f"list rolebindings: {e}")

    def del_and_wait_pvcs():
        try:
            items = v1.list_namespaced_persistent_volume_claim(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    v1.delete_namespaced_persistent_volume_claim(name, ns, body=opts_fg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete pvc/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_persistent_volume_claim(name, ns), f"pvc/{name}")
        except Exception as e:
            log(f"list pvcs: {e}")

    # Order: controllers first, then workload pods, then services/config, then RBAC/storage
    del_and_wait_cronjobs()
    del_and_wait_jobs()
    del_and_wait_deploy()
    del_and_wait_rs()
    del_and_wait_pods()
    del_and_wait_svcs()
    del_and_wait_ingresses()
    del_and_wait_cms()
    del_and_wait_secrets()
    del_and_wait_sas()
    del_and_wait_roles()
    del_and_wait_rolebindings()
    del_and_wait_pvcs()
    log("Force delete sweep finished.")

# -------------------- WAN --------------------
def ssh_run(router: str, cmd: str, timeout: int = 20) -> str:
    # router is like "user@10.0.0.1"
    full = (
        f"ssh -o BatchMode=yes -o ConnectTimeout=5 "
        f"{shlex.quote(router)} {shlex.quote(cmd)}"
    )
    return subprocess.check_output(
        full, shell=True, timeout=timeout, text=True
    )


def wan_apply_and_record(results_dir: Path, router: str, profile: str) -> dict:
    meta = {"router": router, "wan_profile": profile, "applied": False}
    try:
        # Apply profile
        ssh_run(
            router,
            "sudo -n /usr/local/sbin/wan/apply_wan.sh "
            + shlex.quote(profile),
            )
        meta["applied"] = True
        # Save env
        env_txt = ssh_run(router, "cat /etc/wan/env || true", timeout=5)
        (results_dir / "router_env.txt").write_text(env_txt)
        # Save qdisc state
        qdisc = ssh_run(
            router,
            '. /etc/wan/env; (tc qdisc show dev "$EDGE_IF"; echo "---"; '
            'tc qdisc show dev "$CLOUD_IF") || true',
            timeout=8,
        )
        (results_dir / "router_qdisc.txt").write_text(qdisc)
    except Exception as e:
        (results_dir / "router_error.txt").write_text(str(e))
    return meta


# -------------------- Main --------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kubernetes SLO Benchmark Suite",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Core Configuration
    core = parser.add_argument_group("Core Configuration")
    core.add_argument(
        "--namespace",
        "-n",
        required=True,
        help="Kubernetes namespace for the benchmark.",
    )
    core.add_argument(
        "--local-namespace",
        default="local-clients",
        help="Kubernetes namespace for local clients (toolbox, generators). Must not be offloaded.",
    )
    core.add_argument(
        "--manifests-dir",
        required=True,
        help="Directory containing the workload YAML manifests.",
    )
    core.add_argument(
        "--results-root",
        required=True,
        help="Root directory to store benchmark results. A timestamped sub-directory will be created here.",
    )
    core.add_argument("--context", default="", help="Kubernetes context to use.")
    core.add_argument(
        "--kubeconfig",
        default=os.path.expanduser("~/.kube/config"),
        help="Path to the kubeconfig file.",
    )

    # Manifest Filenames
    manifests = parser.add_argument_group("Manifest Filenames")
    manifests.add_argument(
        "--http-latency-file",
        default="http-latency.yaml",
        help="Filename for the HTTP latency workload.",
    )
    manifests.add_argument(
        "--toolbox-file", default="toolbox.yaml", help="Filename for the toolbox pod."
    )
    manifests.add_argument(
        "--cpu-batch-file",
        default="cpu-batch.yaml",
        help="Filename for the CPU batch job.",
    )
    manifests.add_argument(
        "--ml-infer-file",
        default="ml-infer.yaml",
        help="Filename for the ML inference job.",
    )
    manifests.add_argument(
        "--io-job-file", default="io-job.yaml", help="Filename for the I/O job."
    )
    manifests.add_argument(
        "--memory-intensive-file",
        default="memory-intensive.yaml",
        help="Filename for the memory-intensive job.",
    )
    manifests.add_argument(
        "--stream-processor-file",
        default="stream-processor.yaml",
        help="Filename for the stream processor workload.",
    )
    manifests.add_argument(
        "--build-job-file", default="build-job.yaml", help="Filename for the build job."
    )

    # Benchmark Parameters
    params = parser.add_argument_group("Benchmark Parameters")
    params.add_argument(
        "--stream-warmup-sec",
        type=int,
        default=5,
        help="Stream processor warmup duration.",
    )
    params.add_argument(
        "--stream-test-sec",
        type=int,
        default=120,
        help="Stream processor test duration.",
    )
    params.add_argument(
        "--http-warmup-sec", type=int, default=10, help="HTTP benchmark warmup duration."
    )
    params.add_argument(
        "--http-test-sec", type=int, default=30, help="HTTP benchmark test duration."
    )
    params.add_argument("--http-qps", type=int, default=20, help="HTTP benchmark QPS.")
    params.add_argument(
        "--http-conc",
        type=int,
        default=20,
        help="HTTP benchmark concurrency level.",
    )
    params.add_argument(
        "--timeout-job-sec",
        type=int,
        default=600,
        help="Timeout in seconds for waiting on batch jobs to complete.",
    )
    params.add_argument(
        "--latency-policy-metric",
        default="p95",
        choices=["p50", "p95", "p99", "avg"],
        help="Metric to use for evaluating latency-based SLOs.",
    )

    # Control Flags
    control = parser.add_argument_group("Control Flags")
    control.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Disable cleanup of workloads after the benchmark.",
    )
    control.add_argument(
        "--cleanup-namespace",
        action="store_true",
        help="Delete the entire namespace during cleanup. Implies --no-cleanup is not set.",
    )

    wan = parser.add_argument_group("WAN Emulation")
    wan.add_argument("--wan-router", default="", help="SSH target for router, e.g., user@10.0.0.1")
    wan.add_argument("--wan-profile", default="none", choices=["none", "good", "moderate", "poor", "clear"])
    wan.add_argument("--wan-verify", action="store_true", help="Verify WAN with a short ping from toolbox")
    wan.add_argument("--wan-verify-target", default="", help="IP to ping for verification (e.g., a cloud node IP)")

    # Local Load Generation
    load = parser.add_argument_group("Local Load Generation")
    load.add_argument(
        "--local-load-profile",
        default="none",
        choices=["none", "low", "medium", "high"],
        help="Apply a CPU load profile to the local (edge) cluster to simulate background activity.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Derived configuration
    ns_offloaded = args.namespace
    ns_local = args.local_namespace
    manifests_dir = Path(args.manifests_dir)
    results_dir = Path(args.results_root) / datetime.now().strftime(
        "%Y%m%d-%H%M%S"
    )
    ensure_dir(results_dir)

    # Run metadata (start)
    run_meta = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "context": args.context,
        "namespace_offloaded": ns_offloaded,
        "namespace_local": ns_local,
        "latency_policy": args.latency_policy_metric,
        "local_load_profile": args.local_load_profile,
        "wan": {
            "router": args.wan_router,
            "profile": args.wan_profile,
            "applied": False,
        },
    }

    (results_dir / "run_meta.json").write_text(
        json.dumps(run_meta, indent=2)
    )

    http_svc_url = f"http://http-latency.{ns_offloaded}/"
    stream_svc_url = f"http://stream-processor.{ns_offloaded}:8080"

    log(f"Namespace (offloaded): {ns_offloaded}")
    log(f"Namespace (local): {ns_local}")
    log(f"Manifests: {manifests_dir}")
    log(f"Results:   {results_dir}")

    kube_init(args.context, args.kubeconfig)
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    batch = client.BatchV1Api()

    ensure_namespace(v1, ns_offloaded)
    ensure_namespace(v1, ns_local)

    # Deploy local load generator BEFORE other workloads
    if args.local_load_profile != "none":
        deploy_local_load(apps_v1, ns_local, args.local_load_profile)

    # Optionally apply WAN profile on router BEFORE deployments
    if args.wan_router and args.wan_profile and args.wan_profile != "none":
        log(
            f"Applying WAN profile '{args.wan_profile}' on router "
            f"{args.wan_router} ..."
        )
        wan_meta = wan_apply_and_record(
            results_dir, args.wan_router, args.wan_profile
        )
        # merge into run_meta.json
        run_meta["wan"].update(wan_meta)
        (results_dir / "run_meta.json").write_text(
            json.dumps(run_meta, indent=2)
        )
    else:
        log("WAN profile: none (skipping router configuration).")

    # Build SLO catalog from manifests (before deploy, so we capture intent)
    manifest_files = [
        args.http_latency_file,
        args.cpu_batch_file,
        args.ml_infer_file,
        args.io_job_file,
        args.memory_intensive_file,
        args.stream_processor_file,
        args.build_job_file,
    ]
    catalog = build_catalog_from_manifests(ns_offloaded, manifests_dir, manifest_files)
    save_catalog(catalog, results_dir)

    # Deploy workloads and wait for ready
    deploy_and_wait(ns_offloaded, ns_local, manifests_dir, args)

    # Snapshots
    log("Capturing cluster state...")
    try:
        nodes_info = get_nodes_info(v1)
        (results_dir / "nodes.txt").write_text(nodes_info)
    except Exception:
        pass

    try:
        pods_info = get_objects_info(v1, apps_v1, batch, ns_offloaded)
        (results_dir / "pods_initial.txt").write_text(pods_info)
        (results_dir / "k8s_objects_initial.txt").write_text(pods_info)
    except Exception:
        pass
    try:
        pods_info_local = get_objects_info(v1, apps_v1, batch, ns_local)
        (results_dir / "pods_initial_local.txt").write_text(pods_info_local)
    except Exception:
        pass

    # HTTP benchmark
    http_meas = measure_http(v1, ns_local, results_dir, args, http_svc_url)

    try:
        toolbox_exec(v1, ns_local, f"curl -sf {stream_svc_url}/health", check=False)
    except Exception:
        pass

    api_gateway_url = f"http://api-gateway.{ns_offloaded}:8080/aggregate"
    log(f"Testing API Gateway (multi-hop latency)...")
    out = toolbox_exec(
        v1, ns_local,
        f"/hey -z 30s -q 10 -c 10 {api_gateway_url}",
        check=False
    )
    (results_dir / "api_gateway_benchmark.txt").write_text(out)

    # Jobs (wait + logs)
    # Offloaded jobs
    for job in ["cpu-batch", "ml-infer", "io-job", "memory-intensive", "build-job"]:
        try:
            record_job(batch, v1, ns_offloaded, results_dir, job, args.timeout_job_sec)
        except Exception as e:
            log(f"Job {job} wait/log failed: {e}")

    # Local client job
    try:
        record_job(batch, v1, ns_local, results_dir, "stream-data-generator", args.timeout_job_sec)
    except Exception as e:
        log(f"Job stream-data-generator wait/log failed: {e}")

    # Final snapshots
    log("Capturing final placement and events...")
    try:
        pods_info_final = get_objects_info(v1, apps_v1, batch, ns_offloaded)
        (results_dir / "pods_final.txt").write_text(pods_info_final)
    except Exception:
        pass
    get_events(v1, ns_offloaded, results_dir / "events.json")
    try:
        get_pod_node_csv(v1, ns_offloaded, results_dir / "pod_node_map.csv")
    except Exception:
        pass

    try:
        pods_info_final_local = get_objects_info(v1, apps_v1, batch, ns_local)
        (results_dir / "pods_final_local.txt").write_text(pods_info_final_local)
    except Exception:
        pass

    get_events(v1, ns_local, results_dir / "events_local.json")
    try:
        get_pod_node_csv(v1, ns_local, results_dir / "pod_node_map_local.csv")
    except Exception:
        pass

    # Build job measures via API (more accurate)
    job_meas = measure_jobs_via_api(
        batch,
        ns_offloaded,
        ["cpu-batch", "ml-infer", "io-job", "memory-intensive", "build-job"],
    )
    job_meas_local = measure_jobs_via_api(batch, ns_local, ["stream-data-generator"])
    job_meas.update(job_meas_local)
    # Stream measures from generator logs (now should exist)
    stream_meas = measure_stream(
        apps_v1, v1, ns_offloaded, ns_local, results_dir, stream_svc_url, args.stream_warmup_sec
    )

    # Combine measures
    measures: Dict[str, Any] = {}
    measures.update(http_meas)
    measures.update(stream_meas)
    measures.update(job_meas)
    save_measures(measures, results_dir)

    # Evaluate SLOs
    summary = evaluate_slos(catalog, measures, results_dir, args.latency_policy_metric)

    # Print concise summary to stdout
    log("SLO summary:")
    try:
        print((results_dir / "slo_report.txt").read_text())
    except Exception:
        pass

    # Cleanup
    if not args.no_cleanup:
        if args.cleanup_namespace:
            cleanup_namespace(ns_offloaded)
            cleanup_namespace(ns_local)
        else:
            cleanup_workloads(ns_offloaded, ns_local, manifests_dir, args)
        log("Cleanup done.")
    else:
        log("Cleanup skipped.")
    # Finalize run_meta with summary counts if available
    try:
        items = summary.get("items", [])
        passed = sum(1 for x in items if x.get("pass"))
        total = len(items)
        run_meta["slo_pass"] = {"passed": passed, "total": total}
        (results_dir / "run_meta.json").write_text(json.dumps(run_meta, indent=2))
    except Exception:
        pass

    log(f"Benchmark complete. Artifacts -> {results_dir}")


if __name__ == "__main__":
    main()