import json
from pathlib import Path
from typing import Dict, Any, List

from utils import read_yaml_multi, get_annotations, to_int

def build_catalog_from_manifests(ns_offloaded: str, manifests_dir: Path, manifest_files: List[str]) -> List[Dict[str, Any]]:
    """Parses SLO annotations from a list of manifest files to build a catalog."""
    catalog = []
    for filename in manifest_files:
        for doc in read_yaml_multi(manifests_dir / filename):
            kind, name = doc.get("kind"), (doc.get("metadata") or {}).get("name")
            if not kind or not name: continue

            ann = get_annotations(doc)
            catalog.append({
                "kind": kind, "name": name, "namespace": ns_offloaded,
                "slo": {
                    "class": ann.get("slo.hybrid.io/class"),
                    "latency_ms": to_int(ann.get("slo.hybrid.io/latencyTargetMs")),
                    "deadline_ms": to_int(ann.get("slo.hybrid.io/deadlineMs")),
                    "max_offload_penalty_ms": to_int(ann.get("slo.hybrid.io/max-offload-penalty.ms")),
                },
                "source_manifest": str(manifests_dir / filename),
            })
    return catalog

def save_catalog(catalog: List[Dict[str, Any]], results_dir: Path):
    """Saves the SLO catalog to JSON and CSV formats."""
    (results_dir / "catalog.json").write_text(json.dumps(catalog, indent=2))
    lines = ["kind,name,namespace,class,latency_ms,deadline_ms,max_offload_penalty_ms"]
    for w in catalog:
        s = w["slo"]
        lines.append(",".join(map(str, [
            w["kind"], w["name"], w["namespace"], s.get("class") or "",
                                                  s.get("latency_ms") or "", s.get("deadline_ms") or "", s.get("max_offload_penalty_ms") or ""
        ])))
    (results_dir / "catalog.csv").write_text("\n".join(lines))

def evaluate_slos(catalog: List[Dict[str, Any]], measures: Dict[str, Any], results_dir: Path, latency_policy_metric: str) -> Dict[str, Any]:
    """Compares collected measures against the SLO catalog and generates a report."""
    summary = {"items": [], "policy": {"latency_metric": latency_policy_metric}}
    report_lines = []

    cat_idx = {(w["name"], w["kind"]): w for w in catalog}

    def add_result(workload, kind, klass, slo_ms, meas_ms, metric, reason=""):
        passed = False
        if slo_ms is not None and meas_ms is not None:
            passed = meas_ms <= slo_ms
        elif slo_ms is not None and meas_ms is None:
            reason = reason or "no_measurement"

        report_lines.append(f"[{workload}] {klass or 'NA'} target {slo_ms}ms {metric} -> {meas_ms}ms: {'PASS' if passed else 'FAIL'}")
        summary["items"].append({
            "workload": workload, "kind": kind, "class": klass, "metric": metric,
            "target_ms": slo_ms, "measured_ms": meas_ms, "pass": passed, "reason": reason or None,
        })

    # Evaluate HTTP Latency
    if "http-latency" in measures and ("http-latency", "Deployment") in cat_idx:
        cat = cat_idx[("http-latency", "Deployment")]
        slo_lat = cat["slo"].get("latency_ms")
        meas_ms = measures["http-latency"].get(f"{latency_policy_metric}_ms")
        add_result("http-latency", "Deployment", cat["slo"].get("class"), slo_lat, meas_ms, latency_policy_metric)

    # Evaluate Stream Latency
    if "stream-processor" in measures and ("stream-processor", "Deployment") in cat_idx:
        cat = cat_idx[("stream-processor", "Deployment")]
        slo_lat = cat["slo"].get("latency_ms")
        meas_ms = measures["stream-processor"].get("avg_ms")
        add_result("stream-processor", "Deployment", cat["slo"].get("class"), slo_lat, meas_ms, "avg")

    # Evaluate Job Deadlines
    for name, meas in measures.items():
        if meas.get("metric") == "duration_ms" and (name, "Job") in cat_idx:
            cat = cat_idx[(name, "Job")]
            deadline = cat["slo"].get("deadline_ms")
            add_result(name, "Job", cat["slo"].get("class"), deadline, meas.get("value_ms"), "duration_ms")

    (results_dir / "slo_report.txt").write_text("\n".join(report_lines))
    (results_dir / "slo_summary.json").write_text(json.dumps(summary, indent=2))
    return summary