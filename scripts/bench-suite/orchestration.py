import argparse
import os
import shlex
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import yaml
from kubernetes import client
from kubernetes.client import ApiException

import k8s_helpers
from utils import log, file_exists


# ---------------------------------------------------------------------------
# Argument Parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kubernetes SLO Benchmark Suite",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Core Configuration
    core = parser.add_argument_group("Core Configuration")
    core.add_argument("--namespace", "-n", required=True,
                      help="Kubernetes namespace for offloaded benchmark workloads.")
    core.add_argument("--local-namespace", default="local-clients",
                      help="Kubernetes namespace for local client pods.")
    core.add_argument("--manifests-dir", required=True,
                      help="Directory containing workload YAML manifests.")
    core.add_argument("--results-root", required=True,
                      help="Root directory to store benchmark results.")
    core.add_argument("--context", default="",
                      help="Kubernetes context to use.")
    core.add_argument("--kubeconfig", default=os.path.expanduser("~/.kube/config"),
                      help="Path to the kubeconfig file.")

    # Scheduler Controller Configuration
    ctrl = parser.add_argument_group("Controller Configuration")
    ctrl.add_argument("--controller-namespace", default="kube-system")
    ctrl.add_argument("--controller-service-name", default="smart-scheduler-webhook")
    ctrl.add_argument("--controller-metrics-port", default="8080")
    ctrl.add_argument("--controller-label-selector",
                      default="app.kubernetes.io/name=smart-scheduler")

    # Manifest Filenames
    manifests = parser.add_argument_group("Manifest Filenames")
    manifests.add_argument("--http-latency-file",    default="http-latency-job.yaml")
    manifests.add_argument("--toolbox-file",         default="toolbox.yaml")
    manifests.add_argument("--cpu-batch-file",       default="cpu-batch.yaml")
    manifests.add_argument("--ml-infer-file",        default="ml-infer.yaml")
    manifests.add_argument("--io-job-file",          default="io-job.yaml")
    manifests.add_argument("--memory-intensive-file", default="memory-intensive.yaml")
    manifests.add_argument("--stream-batch-file",    default="stream-batch-job.yaml")
    manifests.add_argument("--build-job-file",       default="build-job.yaml")

    # Benchmark Parameters
    params = parser.add_argument_group("Benchmark Parameters")
    params.add_argument("--stream-warmup-sec",    type=int, default=5)
    params.add_argument("--http-warmup-sec",      type=int, default=10)
    params.add_argument("--http-test-sec",        type=int, default=30)
    params.add_argument("--http-qps",             type=int, default=20)
    params.add_argument("--http-conc",            type=int, default=20)
    params.add_argument("--timeout-job-sec",      type=int, default=600)
    params.add_argument("--latency-policy-metric", default="p95",
                        choices=["p50", "p95", "p99", "avg"])

    # Control Flags
    control = parser.add_argument_group("Control Flags")
    control.add_argument("--no-cleanup",            action="store_true")
    control.add_argument("--no-controller-metrics", action="store_true",
                         help="Disable metrics collection from the scheduler controller.")

    # Scheduling Strategy
    sched = parser.add_argument_group("Scheduling Strategy")
    sched.add_argument(
        "--scheduler-mode",
        default="smart",
        choices=["smart", "single-cluster", "liqo-native", "round-robin"],
        help=(
            "Scheduling strategy under test. "
            "'smart' uses the hybrid webhook. "
            "'single-cluster' and 'liqo-native' rely on default k8s/Liqo placement "
            "(webhook must have been uninstalled by the caller). "
            "'round-robin' cycles workloads across edge→fog→cloud using nodeSelectors."
        ),
    )
    sched.add_argument(
        "--rr-node-label-key",
        default="node.cluster/id",
        help="Node label key used for round-robin nodeSelector. Must match labels on target nodes. "
             "Use 'node.cluster/id' to match the controller's remote-cluster label.",
    )
    sched.add_argument(
        "--rr-clusters",
        default="cloud-1",
        help="Comma-separated ordered list of node label values for round-robin. "
             "Must match 'node.cluster/id' values on virtual nodes (e.g. 'cloud-1,cloud-2').",
    )

    # WAN Emulation
    wan = parser.add_argument_group("WAN Emulation")
    wan.add_argument("--wan-router",  default="")
    wan.add_argument("--wan-profile", default="none",
                     choices=["none", "good", "moderate", "poor", "clear"])

    # Local Load Generation
    load = parser.add_argument_group("Local Load Generation")
    load.add_argument("--local-load-profile", default="none",
                      choices=["none", "low", "medium", "high"])

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Round-Robin Placer
# ---------------------------------------------------------------------------

class RoundRobinPlacer:
    """
    Assigns each submitted workload manifest to the next cluster location in
    a fixed rotation by injecting a nodeSelector into the pod template spec.

    The rotation order is determined by ``clusters`` (e.g. ["cloud-1","cloud-2"]).
    The ``label_key`` must match a real node label on your clusters
    (default: ``node.cluster/id``, matching the controller's remote-cluster label).
    """

    def __init__(self, clusters: list[str], label_key: str):
        self.clusters  = clusters
        self.label_key = label_key
        self._counter  = 0

    def _next_cluster(self) -> str:
        cluster = self.clusters[self._counter % len(self.clusters)]
        self._counter += 1
        return cluster

    def patch_manifest_yaml(self, raw_yaml: str) -> str:
        """
        Return a modified YAML string with nodeSelector injected into every
        Job, CronJob, Deployment, StatefulSet, or DaemonSet pod template.
        Each top-level document gets a separate round-robin cluster assignment.
        """
        docs = list(yaml.safe_load_all(raw_yaml))
        patched = []
        for doc in docs:
            if doc is None:
                continue
            kind = doc.get("kind", "")
            if kind in ("Job", "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet"):
                cluster = self._next_cluster()
                log(f"  [round-robin] {doc.get('metadata',{}).get('name','?')} → {self.label_key}={cluster}")
                self._inject_node_selector(doc, "spec", cluster)
            elif kind == "CronJob":
                cluster = self._next_cluster()
                log(f"  [round-robin] {doc.get('metadata',{}).get('name','?')} → {self.label_key}={cluster}")
                job_tmpl = (doc.get("spec", {}) or {}).get("jobTemplate", {})
                self._inject_node_selector(job_tmpl, "spec", cluster)
                doc["spec"]["jobTemplate"] = job_tmpl
            patched.append(doc)
        return yaml.dump_all(patched, default_flow_style=False)

    def _inject_node_selector(self, parent: dict, spec_key: str, cluster: str):
        """Mutate *parent[spec_key].template.spec.nodeSelector* in place."""
        spec = parent.setdefault(spec_key, {})
        template = spec.setdefault("template", {})
        pod_spec = template.setdefault("spec", {})
        node_sel = pod_spec.setdefault("nodeSelector", {})
        node_sel[self.label_key] = cluster

    def apply_file(self, ns_offloaded: str, ns_local: str, manifest_path: Path):
        """
        Read *manifest_path*, patch with nodeSelectors, write to a temp file,
        then apply via k8s_helpers.k_apply.
        """
        raw = manifest_path.read_text()
        patched = self.patch_manifest_yaml(raw)
        with tempfile.NamedTemporaryFile(
            suffix=".yaml", mode="w", delete=False
        ) as tmp:
            tmp.write(patched)
            tmp_path = Path(tmp.name)
        try:
            k8s_helpers.k_apply(ns_offloaded, ns_local, tmp_path)
        finally:
            tmp_path.unlink(missing_ok=True)


def make_placer(args: argparse.Namespace) -> Optional[RoundRobinPlacer]:
    """Return a RoundRobinPlacer if the mode requires it, else None."""
    if args.scheduler_mode == "round-robin":
        clusters = [c.strip() for c in args.rr_clusters.split(",") if c.strip()]
        log(f"Round-robin placer initialised: {clusters} via label '{args.rr_node_label_key}'")
        return RoundRobinPlacer(clusters=clusters, label_key=args.rr_node_label_key)
    return None


# ---------------------------------------------------------------------------
# Local CPU Load Generator
# ---------------------------------------------------------------------------

def deploy_local_load(apps_v1: client.AppsV1Api, ns: str, profile: str):
    """Deploy a continuous CPU load generator in the local namespace."""
    if profile == "none":
        return

    log(f"Deploying local CPU load profile: {profile}")
    profiles = {
        "low":    {"replicas": 1, "cpu_load": 50, "cpu_request": "500m",  "cpu_limit": "1"},
        "medium": {"replicas": 2, "cpu_load": 75, "cpu_request": "750m",  "cpu_limit": "1"},
        "high":   {"replicas": 4, "cpu_load": 90, "cpu_request": "900m",  "cpu_limit": "1"},
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
                    "containers": [{
                        "name": "stress",
                        "image": "debian:bookworm-slim",
                        "command": ["bash", "-c", (
                            "apt-get update -y && "
                            "apt-get install -y --no-install-recommends stress-ng && "
                            f"stress-ng --cpu 0 --cpu-load {config['cpu_load']} --timeout 0s"
                        )],
                        "resources": {
                            "requests": {"cpu": config["cpu_request"], "memory": "128Mi"},
                            "limits":   {"cpu": config["cpu_limit"],   "memory": "256Mi"},
                        },
                    }],
                },
            },
        },
    }

    try:
        apps_v1.create_namespaced_deployment(namespace=ns, body=body)
        k8s_helpers.wait_deployment_ready(apps_v1, ns, "local-cpu-load", 240)
        log(f"Local CPU load profile '{profile}' is active.")
    except ApiException as e:
        if e.status == 409:
            log("Local load deployment already exists. Skipping creation.")
        else:
            log(f"Error deploying local load: {e}")
            raise


# ---------------------------------------------------------------------------
# Cluster Preparation & Workload Deployment
# ---------------------------------------------------------------------------

def deploy_and_prepare_cluster(
    ns_offloaded: str,
    ns_local: str,
    manifests_dir: Path,
    args: argparse.Namespace,
    placer: Optional[RoundRobinPlacer] = None,
):
    """
    Deploy the toolbox pod (always local), then launch all batch jobs.
    If *placer* is provided (round-robin mode), each manifest is patched with
    a nodeSelector before being applied.
    """
    v1, apps_v1 = client.CoreV1Api(), client.AppsV1Api()

    log("Deploying toolbox and waiting for it to be ready...")
    # Toolbox always runs locally; do not round-robin it
    k8s_helpers.k_apply(ns_offloaded, ns_local, manifests_dir / args.toolbox_file)
    k8s_helpers.wait_pod_ready(v1, ns_local, "toolbox", 180)

    job_files = [
        args.http_latency_file,
        args.stream_batch_file,
        args.cpu_batch_file,
        args.ml_infer_file,
        args.io_job_file,
        args.memory_intensive_file,
        args.build_job_file,
    ]

    log(f"Applying all batch jobs (mode={args.scheduler_mode})...")
    for filename in job_files:
        path = manifests_dir / filename
        if not file_exists(path):
            continue
        log(f"  Applying {filename}...")
        if placer is not None:
            placer.apply_file(ns_offloaded, ns_local, path)
        else:
            k8s_helpers.k_apply(ns_offloaded, ns_local, path)


# ---------------------------------------------------------------------------
# Job Monitoring
# ---------------------------------------------------------------------------

def record_job(
    batch_v1: client.BatchV1Api,
    v1: client.CoreV1Api,
    ns: str,
    results_dir: Path,
    name: str,
    timeout_sec: int,
):
    """Wait for a single job, then save its duration and logs."""
    try:
        batch_v1.read_namespaced_job(name=name, namespace=ns)
    except ApiException as e:
        if e.status == 404:
            log(f"Job {name} not found, skipping.")
            return
        raise

    duration = k8s_helpers.wait_job_complete(batch_v1, ns, name, timeout_sec)
    with (results_dir / "jobs_durations.csv").open("a") as f:
        f.write(f"{name},duration_sec,{duration}\n")

    logs = k8s_helpers.get_job_logs(v1, ns, name)
    (results_dir / f"{name}_logs.txt").write_text(logs or "")


def wait_for_all_jobs(
    batch_v1: client.BatchV1Api,
    v1: client.CoreV1Api,
    ns_offloaded: str,
    ns_local: str,
    results_dir: Path,
    args: argparse.Namespace,
):
    """Wait for all deployed batch jobs to finish."""
    log("Waiting for all batch jobs to complete...")
    offloaded_jobs = [
        "http-latency-job", "stream-batch-job", "cpu-batch",
        "ml-infer", "io-job", "memory-intensive", "build-job",
    ]
    for job_name in offloaded_jobs:
        try:
            record_job(batch_v1, v1, ns_offloaded, results_dir, job_name, args.timeout_job_sec)
        except (TimeoutError, RuntimeError) as e:
            log(f"ERROR: Job {job_name} in {ns_offloaded} failed or timed out: {e}")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup_workloads(
    ns_offloaded: str,
    ns_local: str,
    manifests_dir: Path,
    args: argparse.Namespace,
):
    """Delete all benchmark-related workloads from the cluster."""
    log(f"Cleaning up workloads in {ns_offloaded}, {ns_local}...")
    if args.local_load_profile != "none":
        try:
            apps_v1 = client.AppsV1Api()
            apps_v1.delete_namespaced_deployment(name="local-cpu-load", namespace=ns_local)
        except ApiException as e:
            if e.status != 404:
                log(f"Warning: could not delete local-cpu-load: {e}")

    all_files = [
        args.http_latency_file, args.cpu_batch_file, args.ml_infer_file,
        args.io_job_file, args.memory_intensive_file, args.stream_batch_file,
        args.build_job_file, args.toolbox_file,
    ]
    for filename in reversed(all_files):
        path = manifests_dir / filename
        if file_exists(path):
            k8s_helpers.k_delete(ns_offloaded, ns_local, path)


# ---------------------------------------------------------------------------
# WAN
# ---------------------------------------------------------------------------

def ssh_run(router: str, cmd: str, timeout: int = 20) -> str:
    full = (
        f"ssh -o BatchMode=yes -o ConnectTimeout=5 "
        f"{shlex.quote(router)} {shlex.quote(cmd)}"
    )
    return subprocess.check_output(full, shell=True, timeout=timeout, text=True)


def wan_apply_and_record(results_dir: Path, router: str, profile: str) -> dict:
    meta = {"router": router, "wan_profile": profile, "applied": False}
    try:
        ssh_run(router, "sudo -n /usr/local/sbin/wan/apply_wan.sh " + shlex.quote(profile))
        meta["applied"] = True
        env_txt  = ssh_run(router, "cat /etc/wan/env || true", timeout=5)
        (results_dir / "router_env.txt").write_text(env_txt)
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
