import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path

from kubernetes import client
from kubernetes.client import ApiException

import k8s_helpers
import telemetry
from utils import log, file_exists

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


def deploy_and_prepare_cluster(ns_offloaded: str, ns_local: str, manifests_dir: Path, args: argparse.Namespace):
    """Deploys essential services, waits for them, then launches batch jobs asynchronously."""
    v1, apps_v1 = client.CoreV1Api(), client.AppsV1Api()

    log("Deploying toolbox and waiting for it to be ready...")
    k8s_helpers.k_apply(ns_offloaded, ns_local, manifests_dir / args.toolbox_file)
    k8s_helpers.wait_pod_ready(v1, ns_local, "toolbox", 180)

    log("Deploying interactive services and waiting for them...")
    if file_exists(manifests_dir / args.http_latency_file):
        k8s_helpers.k_apply(ns_offloaded, ns_local, manifests_dir / args.http_latency_file)
        k8s_helpers.wait_deployment_ready(apps_v1, ns_offloaded, "http-latency", 240)
    if file_exists(manifests_dir / args.stream_processor_file):
        k8s_helpers.k_apply(ns_offloaded, ns_local, manifests_dir / args.stream_processor_file)
        k8s_helpers.wait_deployment_ready(apps_v1, ns_offloaded, "stream-processor", 240)

    log("Applying all batch jobs asynchronously...")
    job_files = [
        args.cpu_batch_file, args.ml_infer_file, args.io_job_file,
        args.memory_intensive_file, args.build_job_file, args.stream_generator_file
    ]
    for filename in job_files:
        path = manifests_dir / filename
        if file_exists(path):
            log(f"Applying job from {path}...")
            k8s_helpers.k_apply(ns_offloaded, ns_local, path)

def record_job(batch_v1: client.BatchV1Api, v1: client.CoreV1Api, ns: str, results_dir: Path, name: str, timeout_sec: int):
    """Waits for a single job to complete and saves its duration and logs."""
    try:
        batch_v1.read_namespaced_job(name=name, namespace=ns)
    except ApiException as e:
        if e.status == 404: log(f"Job {name} not found, skipping."); return
        raise

    duration = k8s_helpers.wait_job_complete(batch_v1, ns, name, timeout_sec)
    with (results_dir / "jobs_durations.csv").open("a") as f:
        f.write(f"{name},duration_sec,{duration}\n")

    logs = telemetry.get_job_logs(v1, ns, name)
    (results_dir / f"{name}_logs.txt").write_text(logs or "")

def wait_for_all_jobs(batch_v1: client.BatchV1Api, v1: client.CoreV1Api, ns_offloaded: str, ns_local: str, results_dir: Path, args: argparse.Namespace):
    """Waits for all deployed batch jobs to finish."""
    log("Waiting for all background batch jobs to complete...")
    offloaded_jobs = ["cpu-batch", "ml-infer", "io-job", "memory-intensive", "build-job"]
    for job_name in offloaded_jobs:
        try:
            record_job(batch_v1, v1, ns_offloaded, results_dir, job_name, args.timeout_job_sec)
        except (TimeoutError, RuntimeError) as e:
            log(f"ERROR: Job {job_name} in {ns_offloaded} failed or timed out: {e}")

    try:
        record_job(batch_v1, v1, ns_local, results_dir, "stream-data-generator", args.timeout_job_sec)
    except (TimeoutError, RuntimeError) as e:
        log(f"ERROR: Job stream-data-generator in {ns_local} failed or timed out: {e}")

def cleanup_workloads(ns_offloaded: str, ns_local: str, manifests_dir: Path, args: argparse.Namespace):
    """Deletes all benchmark-related workloads from the cluster."""
    log(f"Cleaning up workloads in namespaces {ns_offloaded}, {ns_local}...")
    all_files = [
        args.http_latency_file, args.cpu_batch_file, args.ml_infer_file,
        args.io_job_file, args.memory_intensive_file, args.stream_processor_file,
        args.build_job_file, args.toolbox_file, args.stream_generator_file,
    ]
    for filename in reversed(all_files):
        path = manifests_dir / filename
        if file_exists(path):
            k8s_helpers.k_delete(ns_offloaded, ns_local, path)

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
