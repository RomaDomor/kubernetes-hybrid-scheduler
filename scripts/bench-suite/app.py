import json
from datetime import datetime
from pathlib import Path

from kubernetes import client

import orchestration
import k8s_helpers
import telemetry
import slo
from utils import log, ensure_dir

def main():
    args = orchestration.parse_args()

    # --- Setup ---
    ns_offloaded, ns_local = args.namespace, args.local_namespace
    manifests_dir = Path(args.manifests_dir)
    results_dir = Path(args.results_root) / datetime.now().strftime("%Y%m%d-%H%M%S")
    ensure_dir(results_dir)
    log(f"Results will be stored in: {results_dir}")

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

    k8s_helpers.kube_init(args.context, args.kubeconfig)
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    batch_v1 = client.BatchV1Api()

    k8s_helpers.ensure_namespace(v1, ns_offloaded)
    k8s_helpers.ensure_namespace(v1, ns_local)

    # --- Step 1: Build SLO Catalog ---
    manifest_files = [
        args.http_latency_file, args.cpu_batch_file, args.ml_infer_file, args.io_job_file,
        args.memory_intensive_file, args.stream_processor_file, args.build_job_file
    ]
    catalog = slo.build_catalog_from_manifests(ns_offloaded, manifests_dir, manifest_files)
    slo.save_catalog(catalog, results_dir)

    # --- Step 2: Deploy Workloads ---
    orchestration.deploy_and_prepare_cluster(ns_offloaded, ns_local, manifests_dir, args)

    # --- Step 3: Capture Initial State & Run Interactive Benchmarks ---
    log("Capturing initial cluster state...")
    telemetry.get_nodes_info(v1, results_dir / "nodes.txt")
    # Add calls to get_objects_info if needed

    http_svc_url = f"http://http-latency.{ns_offloaded}/"
    http_meas = telemetry.measure_http(v1, ns_local, results_dir, args, http_svc_url)

    # --- Step 4: Wait for Batch Jobs ---
    orchestration.wait_for_all_jobs(batch_v1, v1, ns_offloaded, ns_local, results_dir, args)

    # --- Step 5: Capture Final State & Gather All Measures ---
    log("Capturing final cluster state and events...")
    telemetry.get_pod_node_map(v1, ns_offloaded, results_dir / "pod_node_map.csv")
    telemetry.get_events(v1, ns_offloaded, results_dir / "events.json")

    log("Gathering all measurements...")
    job_meas = telemetry.measure_jobs_via_api(batch_v1, ns_offloaded, ["cpu-batch", "ml-infer", "io-job", "memory-intensive", "build-job"])
    job_meas_local = telemetry.measure_jobs_via_api(batch_v1, ns_local, ["stream-data-generator"])
    stream_meas = telemetry.measure_stream(apps_v1, ns_offloaded, results_dir)

    measures = {**http_meas, **stream_meas, **job_meas, **job_meas_local}
    (results_dir / "measures.json").write_text(json.dumps(measures, indent=2))

    # --- Step 6: Evaluate SLOs ---
    summary = slo.evaluate_slos(catalog, measures, results_dir, args.latency_policy_metric)
    log("SLO Summary:")
    print((results_dir / "slo_report.txt").read_text())

    # --- Step 7: Cleanup ---
    if not args.no_cleanup:
        orchestration.cleanup_workloads(ns_offloaded, ns_local, manifests_dir, args)
        log("Cleanup complete.")
    else:
        log("Skipping cleanup.")

    # --- Finalize ---
    items = summary.get("items", [])
    run_meta["slo_results"] = {"passed": sum(1 for x in items if x.get("pass")), "total": len(items)}
    (results_dir / "run_meta.json").write_text(json.dumps(run_meta, indent=2))
    log(f"Benchmark finished. All artifacts are in {results_dir}")

if __name__ == "__main__":
    main()