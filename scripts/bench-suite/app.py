import json
from datetime import datetime
from pathlib import Path

from kubernetes import client

import k8s_helpers
import orchestration
import slo
import telemetry
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
    v1, apps_v1, batch_v1 = client.CoreV1Api(), client.AppsV1Api(), client.BatchV1Api()
    k8s_helpers.ensure_namespace(v1, ns_offloaded)
    k8s_helpers.ensure_namespace(v1, ns_local)

    # --- Step 1: Optional Pre-Configuration (Load & WAN) ---
    if args.local_load_profile != "none":
        orchestration.deploy_local_load(apps_v1, ns_local, args.local_load_profile)
    if args.wan_router and args.wan_profile != "none":
        wan_meta = orchestration.wan_apply_and_record(results_dir, args.wan_router, args.wan_profile)
        run_meta["wan"].update(wan_meta)

    # --- Step 2: Build SLO Catalog ---
    manifest_files = [args.http_latency_file, args.cpu_batch_file, args.ml_infer_file, args.io_job_file,
                      args.memory_intensive_file, args.stream_processor_file, args.build_job_file]
    catalog = slo.build_catalog_from_manifests(ns_offloaded, manifests_dir, manifest_files)
    slo.save_catalog(catalog, results_dir)

    # --- Step 3: Deploy Workloads ---
    orchestration.deploy_and_prepare_cluster(ns_offloaded, ns_local, manifests_dir, args)

    # --- Step 4: Capture Initial State & Run Interactive Benchmarks ---
    log("Capturing initial cluster state...")
    telemetry.get_nodes_info(v1, results_dir / "nodes.txt")

    # --- Step 5: Wait for Batch Jobs ---
    orchestration.wait_for_all_jobs(batch_v1, v1, ns_offloaded, ns_local, results_dir, args)

    # --- Step 6: Capture Final State & Gather All Measures ---
    log("Capturing final state and gathering all measurements...")
    telemetry.get_pod_node_map(v1, ns_offloaded, results_dir / "pod_node_map.csv")
    telemetry.get_events(v1, ns_offloaded, results_dir / "events.json")
    job_meas = telemetry.measure_jobs_via_api(batch_v1, ns_offloaded,
                                              ["http-latency-job", "stream-batch-job", "cpu-batch", "ml-infer", "io-job", "memory-intensive", "build-job"])
    measures = {**job_meas}
    (results_dir / "measures.json").write_text(json.dumps(measures, indent=2))

    # --- Step 7: Evaluate SLOs ---
    summary = slo.evaluate_slos(catalog, measures, results_dir, args.latency_policy_metric)
    log("SLO Summary:")
    print((results_dir / "slo_report.txt").read_text())

    # --- Step 8: Cleanup ---
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
