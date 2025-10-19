#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Visualize WAN impact: compare one edge baseline run against all cloud profiles.

- Edge: one baseline run (no WAN relevance)
- Cloud: all profiles (clear, good, moderate, ...), each compared to edge

Outputs per-cloud-profile detailed charts and a global dashboard.
"""

import argparse
import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
import seaborn as sns
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
})

COLORS = {
    "edge": "#2E86AB",
    "cloud": "#A23B72",
    "pass": "#06A77D",
    "fail": "#D62839",
    "budget": "#F77F00",
    "penalty": "#D62839",
    "gain": "#06A77D",
}

# Profile colors for multi-profile graphs
PROFILE_COLORS = {
    "edge": "#2E86AB",
    "clear": "#06A77D",
    "good": "#90C978",
    "moderate": "#F77F00",
    "poor": "#D62839",
    "degraded": "#A23B72",
}

def load_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(p.read_text())
    except Exception:
        return None

def load_first_run_dir(profile_dir: Path) -> Optional[Path]:
    runs = sorted([d for d in profile_dir.iterdir() if d.is_dir()])
    return runs[0] if runs else None

def find_cloud_profiles(root: Path) -> List[str]:
    out = []
    for p in sorted(root.glob("wan-*_*")):
        name = p.name
        if name.startswith("wan-") and "_load-" in name:
            prof = name.split("_load-")[0].replace("wan-", "")
            if prof not in out:
                out.append(prof)
    return out

def resolve_cloud_run(root: Path, profile: str, load: str) -> Optional[Path]:
    pdir = root / f"wan-{profile}_load-{load}"
    return load_first_run_dir(pdir) if pdir.exists() else None

def select_edge_run(root: Path, load: str) -> Optional[Path]:
    # Accept any wan-*_load-<load> subdir, pick its first run
    candidates = sorted(root.glob(f"wan-*_load-{load}"))
    if not candidates:
        # fallback: any wan-*_* subdir
        candidates = sorted(root.glob("wan-*_*"))
    if not candidates:
        # fallback: if root itself is a run (contains slo_summary.json)
        if (root / "slo_summary.json").exists():
            return root
        return None
    return load_first_run_dir(candidates[0])

def load_slo_summary(run_dir: Path) -> Optional[Dict[str, Any]]:
    return load_json(run_dir / "slo_summary.json")

def load_catalog(run_dir: Path) -> Dict[Tuple[str, str], Dict[str, Any]]:
    data = load_json(run_dir / "catalog.json")
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if isinstance(data, list):
        for item in data:
            out[(item.get("name"), item.get("kind"))] = item
    return out

def items_to_map(slo_summary: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    return {(i.get("workload"), i.get("kind")): i for i in slo_summary.get("items", [])}

def get_budget_ms(catalog: Dict[Tuple[str, str], Dict[str, Any]], name: str, kind: str) -> Optional[float]:
    item = catalog.get((name, kind))
    if not item:
        return None
    slo = item.get("slo") or {}
    v = slo.get("max_offload_penalty_ms")
    return float(v) if v is not None else None

def get_wan_rtt(run_dir: Path) -> Optional[float]:
    qdisc = run_dir / "router_qdisc.txt"
    if not qdisc.exists():
        return None
    try:
        for line in qdisc.read_text().splitlines():
            if "delay" in line:
                parts = line.split()
                idx = parts.index("delay")
                if idx + 1 < len(parts):
                    return float(parts[idx + 1].replace("ms", ""))
    except Exception:
        pass
    return None

def safe_float(val, default=0.0):
    """Safely convert value to float, handling None."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def analyze_profile(edge_run: Path, cloud_run: Path) -> Dict[str, Any]:
    edge_sum = load_slo_summary(edge_run)
    cloud_sum = load_slo_summary(cloud_run)
    cloud_cat = load_catalog(cloud_run)
    if not edge_sum or not cloud_sum:
        return {"error": "missing_slo_summary"}

    edge_map = items_to_map(edge_sum)
    cloud_map = items_to_map(cloud_sum)
    result = {"latency": [], "batch": [], "pass": {}}

    # pass rates
    eitems = edge_sum.get("items", [])
    citems = cloud_sum.get("items", [])
    result["pass"]["edge"] = (sum(1 for i in eitems if i.get("pass")), len(eitems))
    result["pass"]["cloud"] = (sum(1 for i in citems if i.get("pass")), len(citems))

    for key, e in edge_map.items():
        c = cloud_map.get(key)
        if not c:
            continue
        name, kind = key
        cls = e.get("class") or c.get("class")
        if kind == "Deployment" and cls == "latency":
            edge_ms = safe_float(e.get("measured_ms"))
            cloud_ms = safe_float(c.get("measured_ms"))
            budget = get_budget_ms(cloud_cat, name, kind)
            result["latency"].append({
                "workload": name,
                "edge_ms": edge_ms,
                "cloud_ms": cloud_ms,
                "target_ms": safe_float(e.get("target_ms") or c.get("target_ms")),
                "budget_ms": budget,
                "edge_pass": e.get("pass"),
                "cloud_pass": c.get("pass"),
            })
        if kind == "Job" and cls == "batch":
            edge_ms = safe_float(e.get("measured_ms"))
            cloud_ms = safe_float(c.get("measured_ms"))
            result["batch"].append({
                "workload": name,
                "edge_ms": edge_ms,
                "cloud_ms": cloud_ms,
                "deadline_ms": safe_float(e.get("target_ms") or c.get("target_ms")),
                "edge_pass": e.get("pass"),
                "cloud_pass": c.get("pass"),
            })
    return result

def plot_latency_profile(profile: str, data: List[Dict[str, Any]], rtt: Optional[float], out_dir: Path):
    if not data:
        return
    workloads = [d["workload"] for d in data]
    edge_vals = [safe_float(d["edge_ms"]) for d in data]
    cloud_vals = [safe_float(d["cloud_ms"]) for d in data]
    targets = [d.get("target_ms") for d in data]
    budgets = [d.get("budget_ms") for d in data]
    edge_pass = [d["edge_pass"] for d in data]
    cloud_pass = [d["cloud_pass"] for d in data]

    x = np.arange(len(workloads))
    w = 0.35
    fig, ax1 = plt.subplots(figsize=(max(8, len(workloads)*1.2), 5))

    ax1.bar(x - w/2, edge_vals, width=w, color=[COLORS["pass"] if p else COLORS["fail"] for p in edge_pass],
            edgecolor="black", linewidth=0.5, label="Edge")
    ax1.bar(x + w/2, cloud_vals, width=w, color=[COLORS["pass"] if p else COLORS["fail"] for p in cloud_pass],
            edgecolor="black", linewidth=0.5, label=f"Cloud ({profile})")

    # Targets
    if any(targets):
        tgt = next((t for t in targets if t), None)
        if tgt:
            ax1.axhline(tgt, color="gray", linestyle="--", linewidth=1.5, label=f"SLO target ({tgt} ms)")

    # Budget markers
    for i, (e, b) in enumerate(zip(edge_vals, budgets)):
        if e and b:
            ax1.plot([i + w/2], [e + b], marker="o", color=COLORS["budget"], markersize=6)

    # Penalty annotations
    for i, (e, c, b) in enumerate(zip(edge_vals, cloud_vals, budgets)):
        if e and c:
            pen = c - e
            color = COLORS["gain"] if pen <= 0 else (COLORS["pass"] if (b and pen <= b) else COLORS["fail"])
            ax1.annotate(f"{pen:+.1f}", (i + w/2, c), textcoords="offset points", xytext=(0, 6),
                         ha="center", fontsize=8, color=color, weight="bold")

    # RTT
    if rtt is not None:
        ax2 = ax1.twinx()
        ax2.set_ylim(0, max(max(edge_vals+cloud_vals)*1.1, rtt*3))
        ax2.axhline(rtt, color="#F77F00", linestyle=":", linewidth=1.8, label=f"WAN RTT ≈ {rtt:.0f} ms")
        ax2.set_ylabel("WAN RTT (ms)", color="#F77F00")
        ax2.tick_params(axis="y", labelcolor="#F77F00")

    ax1.set_xticks(x)
    ax1.set_xticklabels(workloads, rotation=15, ha="right")
    ax1.set_ylabel("Latency (ms)")
    ax1.set_title(f"Latency vs Edge baseline — Cloud profile: {profile}")
    ax1.legend(loc="upper left")
    ax1.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    out_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_dir / f"latency_profile_{profile}.png", bbox_inches="tight")
    plt.close()

def plot_batch_profile(profile: str, data: List[Dict[str, Any]], out_dir: Path):
    if not data:
        return
    workloads = [d["workload"] for d in data]
    edge_vals = [safe_float(d["edge_ms"]) for d in data]
    cloud_vals = [safe_float(d["cloud_ms"]) for d in data]
    deadlines = [d.get("deadline_ms") for d in data]
    edge_pass = [d["edge_pass"] for d in data]
    cloud_pass = [d["cloud_pass"] for d in data]

    gains_sec = []
    gains_pct = []
    for e, c in zip(edge_vals, cloud_vals):
        if e and c and e > 0:
            gains_sec.append((e - c) / 1000.0)
            gains_pct.append((e - c) / e * 100.0)
        else:
            gains_sec.append(0.0)
            gains_pct.append(0.0)

    x = np.arange(len(workloads))
    w = 0.35
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(max(10, len(workloads)*1.6), 5))

    # Durations
    ax1.bar(x - w/2, np.array(edge_vals)/1000.0, width=w,
            color=[COLORS["pass"] if p else COLORS["fail"] for p in edge_pass],
            edgecolor="black", linewidth=0.5, label="Edge")
    ax1.bar(x + w/2, np.array(cloud_vals)/1000.0, width=w,
            color=[COLORS["pass"] if p else COLORS["fail"] for p in cloud_pass],
            edgecolor="black", linewidth=0.5, label=f"Cloud ({profile})")

    # Deadline
    if any(deadlines):
        d = next((d for d in deadlines if d), None)
        if d:
            ax1.axhline(d/1000.0, color="gray", linestyle="--", linewidth=1.5, label=f"Deadline ({int(d/1000)} s)")

    # Annotate durations
    max_val = max(max(edge_vals), max(cloud_vals)) if edge_vals and cloud_vals else 1
    for i, (e, c) in enumerate(zip(edge_vals, cloud_vals)):
        if e:
            ax1.text(i - w/2, e/1000 + 0.03*max(1, max_val/1000),
                     f"{e/1000:.0f}s", ha="center", fontsize=8)
        if c:
            ax1.text(i + w/2, c/1000 + 0.03*max(1, max_val/1000),
                     f"{c/1000:.0f}s", ha="center", fontsize=8)

    ax1.set_xticks(x)
    ax1.set_xticklabels(workloads, rotation=15, ha="right")
    ax1.set_ylabel("Duration (s)")
    ax1.set_title("Job durations vs edge")
    ax1.legend(loc="upper right")
    ax1.grid(axis="y", alpha=0.3)

    # Gains
    colors = [COLORS["gain"] if g >= 0 else COLORS["penalty"] for g in gains_sec]
    ax2.bar(x, gains_sec, color=colors, edgecolor="black", linewidth=0.5)
    max_gain = max(abs(g) for g in gains_sec) if gains_sec else 1
    for i, (g, pct) in enumerate(zip(gains_sec, gains_pct)):
        if abs(g) > 0.1:
            ax2.text(i, g + (0.03*max(1, max_gain)) * (1 if g >= 0 else -1),
                     f"{pct:+.0f}%", ha="center",
                     va="bottom" if g >= 0 else "top", fontsize=9, weight="bold")
    ax2.axhline(0, color="black", linewidth=0.8)
    ax2.set_xticks(x)
    ax2.set_xticklabels(workloads, rotation=15, ha="right")
    ax2.set_ylabel("Gain vs edge (s) [positive = cloud faster]")
    ax2.set_title(f"Offload gains — Cloud profile: {profile}")
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    out_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_dir / f"batch_profile_{profile}.png", bbox_inches="tight")
    plt.close()

def summary_dashboard(edge_run: Path, cloud_root: Path, profiles: List[str], load: str, out_dir: Path):
    # Build per-profile average metrics
    edge_sum = load_slo_summary(edge_run)
    if not edge_sum:
        return
    edge_map = items_to_map(edge_sum)

    avg_lat_pen = []
    avg_batch_gain_pct = []
    rtts = []

    for prof in profiles:
        cloud_run = resolve_cloud_run(cloud_root, prof, load)
        if not cloud_run:
            avg_lat_pen.append(0); avg_batch_gain_pct.append(0); rtts.append(0); continue
        rtt = get_wan_rtt(cloud_run) or 0
        rtts.append(rtt)

        cloud_sum = load_slo_summary(cloud_run)
        if not cloud_sum:
            avg_lat_pen.append(0); avg_batch_gain_pct.append(0); continue
        cloud_map = items_to_map(cloud_sum)

        # Latency penalties
        pens = []
        for (name, kind), e in edge_map.items():
            if kind == "Deployment" and (e.get("class") == "latency"):
                c = cloud_map.get((name, kind))
                if c:
                    e_ms = safe_float(e.get("measured_ms"))
                    c_ms = safe_float(c.get("measured_ms"))
                    if e_ms and c_ms:
                        pens.append(c_ms - e_ms)
        avg_lat_pen.append(np.mean(pens) if pens else 0)

        # Batch gains %
        gains = []
        for (name, kind), e in edge_map.items():
            if kind == "Job" and (e.get("class") in ("batch", "ml")):
                c = cloud_map.get((name, kind))
                if c:
                    e_ms = safe_float(e.get("measured_ms"))
                    c_ms = safe_float(c.get("measured_ms"))
                    if e_ms > 0 and c_ms:
                        gains.append((e_ms - c_ms) / e_ms * 100.0)
        avg_batch_gain_pct.append(np.mean(gains) if gains else 0)

    # Plot
    x = np.arange(len(profiles))
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax2 = ax1.twinx()

    ax1.plot(x, avg_lat_pen, marker="o", color=COLORS["penalty"], linewidth=2.5, label="Avg latency penalty (ms)")
    ax2.plot(x, avg_batch_gain_pct, marker="s", color=COLORS["gain"], linewidth=2.5, label="Avg batch gain (%)")
    ax1.set_xticks(x); ax1.set_xticklabels(profiles)
    ax1.set_xlabel("Cloud WAN profile")
    ax1.set_ylabel("Avg latency penalty (ms)", color=COLORS["penalty"])
    ax2.set_ylabel("Avg batch offload gain (%)", color=COLORS["gain"])
    ax1.grid(alpha=0.3)

    # RTT scatter on ax1 for context
    ax1.scatter(x, rtts, color="#F77F00", marker="^", s=60, label="WAN RTT (ms)")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.title("WAN impact vs edge baseline (cloud profiles)")
    plt.tight_layout()
    plt.savefig(out_dir / "summary_wan_impact.png", bbox_inches="tight")
    plt.close()

# ============================================================================
# NEW: All-profiles combined graphs
# ============================================================================

def collect_all_data(edge_run: Path, cloud_root: Path, profiles: List[str], load: str):
    """Collect all latency and batch data across all profiles."""
    edge_sum = load_slo_summary(edge_run)
    if not edge_sum:
        return None

    edge_map = items_to_map(edge_sum)

    # Structure: {workload_name: {"edge": value, "clear": value, ...}}
    latency_data = {}
    batch_data = {}
    rtt_map = {"edge": 0}

    # Add edge data
    for (name, kind), e in edge_map.items():
        cls = e.get("class")
        if kind == "Deployment" and cls == "latency":
            latency_data[name] = {"edge": safe_float(e.get("measured_ms"))}
        elif kind == "Job" and cls in ("batch", "ml"):
            batch_data[name] = {"edge": safe_float(e.get("measured_ms"))}

    # Add cloud profile data
    for prof in profiles:
        cloud_run = resolve_cloud_run(cloud_root, prof, load)
        if not cloud_run:
            continue

        rtt_map[prof] = get_wan_rtt(cloud_run) or 0
        cloud_sum = load_slo_summary(cloud_run)
        if not cloud_sum:
            continue

        cloud_map = items_to_map(cloud_sum)

        for (name, kind), c in cloud_map.items():
            cls = c.get("class")
            if kind == "Deployment" and cls == "latency" and name in latency_data:
                latency_data[name][prof] = safe_float(c.get("measured_ms"))
            elif kind == "Job" and cls in ("batch", "ml") and name in batch_data:
                batch_data[name][prof] = safe_float(c.get("measured_ms"))

    return {
        "latency": latency_data,
        "batch": batch_data,
        "rtts": rtt_map,
        "profiles": ["edge"] + profiles
    }

def plot_latency_all_profiles(all_data: Dict, out_dir: Path):
    """Line chart showing each latency workload across all profiles."""
    if not all_data or not all_data["latency"]:
        return

    latency_data = all_data["latency"]
    profiles = all_data["profiles"]

    fig, ax = plt.subplots(figsize=(12, 6))

    for workload, values in latency_data.items():
        y_vals = [safe_float(values.get(p)) for p in profiles]
        ax.plot(profiles, y_vals, marker="o", linewidth=2, label=workload)

    ax.set_xlabel("Profile")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency Workloads Across All Profiles")
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(alpha=0.3)
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(out_dir / "latency_all_profiles_line.png", bbox_inches="tight")
    plt.close()

def plot_batch_all_profiles(all_data: Dict, out_dir: Path):
    """Line chart showing each batch job across all profiles."""
    if not all_data or not all_data["batch"]:
        return

    batch_data = all_data["batch"]
    profiles = all_data["profiles"]

    fig, ax = plt.subplots(figsize=(12, 6))

    for workload, values in batch_data.items():
        y_vals = [safe_float(values.get(p)) / 1000.0 for p in profiles]  # Convert to seconds
        ax.plot(profiles, y_vals, marker="s", linewidth=2, label=workload)

    ax.set_xlabel("Profile")
    ax.set_ylabel("Duration (s)")
    ax.set_title("Batch Job Durations Across All Profiles")
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(alpha=0.3)
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(out_dir / "batch_all_profiles_line.png", bbox_inches="tight")
    plt.close()

def plot_latency_heatmap(all_data: Dict, out_dir: Path):
    """Heatmap of latency penalties compared to edge."""
    if not all_data or not all_data["latency"]:
        return

    latency_data = all_data["latency"]
    profiles = [p for p in all_data["profiles"] if p != "edge"]
    workloads = sorted(latency_data.keys())

    # Build penalty matrix
    penalties = []
    for wl in workloads:
        edge_val = safe_float(latency_data[wl].get("edge"))
        row = []
        for prof in profiles:
            cloud_val = safe_float(latency_data[wl].get(prof))
            penalty = cloud_val - edge_val if edge_val else 0
            row.append(penalty)
        penalties.append(row)

    fig, ax = plt.subplots(figsize=(10, max(6, len(workloads) * 0.5)))
    im = ax.imshow(penalties, cmap="RdYlGn_r", aspect="auto")

    ax.set_xticks(np.arange(len(profiles)))
    ax.set_yticks(np.arange(len(workloads)))
    ax.set_xticklabels(profiles)
    ax.set_yticklabels(workloads)
    plt.setp(ax.get_xticklabels(), rotation=15, ha="right")

    # Annotate cells
    for i in range(len(workloads)):
        for j in range(len(profiles)):
            text = ax.text(j, i, f"{penalties[i][j]:+.0f}",
                           ha="center", va="center", color="black", fontsize=8)

    ax.set_title("Latency Penalty vs Edge (ms) — Heatmap")
    fig.colorbar(im, ax=ax, label="Penalty (ms)")
    plt.tight_layout()
    plt.savefig(out_dir / "latency_penalty_heatmap.png", bbox_inches="tight")
    plt.close()

def plot_batch_heatmap(all_data: Dict, out_dir: Path):
    """Heatmap of batch gains (%) compared to edge."""
    if not all_data or not all_data["batch"]:
        return

    batch_data = all_data["batch"]
    profiles = [p for p in all_data["profiles"] if p != "edge"]
    workloads = sorted(batch_data.keys())

    # Build gain matrix
    gains = []
    for wl in workloads:
        edge_val = safe_float(batch_data[wl].get("edge"))
        row = []
        for prof in profiles:
            cloud_val = safe_float(batch_data[wl].get(prof))
            if edge_val > 0:
                gain_pct = (edge_val - cloud_val) / edge_val * 100.0
            else:
                gain_pct = 0
            row.append(gain_pct)
        gains.append(row)

    fig, ax = plt.subplots(figsize=(10, max(6, len(workloads) * 0.5)))
    im = ax.imshow(gains, cmap="RdYlGn", aspect="auto")

    ax.set_xticks(np.arange(len(profiles)))
    ax.set_yticks(np.arange(len(workloads)))
    ax.set_xticklabels(profiles)
    ax.set_yticklabels(workloads)
    plt.setp(ax.get_xticklabels(), rotation=15, ha="right")

    # Annotate cells
    for i in range(len(workloads)):
        for j in range(len(profiles)):
            text = ax.text(j, i, f"{gains[i][j]:+.0f}%",
                           ha="center", va="center", color="black", fontsize=8)

    ax.set_title("Batch Offload Gain vs Edge (%) — Heatmap")
    fig.colorbar(im, ax=ax, label="Gain (%)")
    plt.tight_layout()
    plt.savefig(out_dir / "batch_gain_heatmap.png", bbox_inches="tight")
    plt.close()

def plot_grouped_bar_latency(all_data: Dict, out_dir: Path):
    """Grouped bar chart: all profiles side-by-side for each latency workload."""
    if not all_data or not all_data["latency"]:
        return

    latency_data = all_data["latency"]
    profiles = all_data["profiles"]
    workloads = sorted(latency_data.keys())

    x = np.arange(len(workloads))
    width = 0.8 / len(profiles)

    fig, ax = plt.subplots(figsize=(max(12, len(workloads) * 1.5), 6))

    for i, prof in enumerate(profiles):
        vals = [safe_float(latency_data[wl].get(prof)) for wl in workloads]
        color = PROFILE_COLORS.get(prof, "#555555")
        ax.bar(x + i * width, vals, width, label=prof, color=color, edgecolor="black", linewidth=0.5)

    ax.set_xticks(x + width * (len(profiles) - 1) / 2)
    ax.set_xticklabels(workloads, rotation=15, ha="right")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency Workloads — All Profiles Grouped")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "latency_grouped_bar.png", bbox_inches="tight")
    plt.close()

def plot_grouped_bar_batch(all_data: Dict, out_dir: Path):
    """Grouped bar chart: all profiles side-by-side for each batch job."""
    if not all_data or not all_data["batch"]:
        return

    batch_data = all_data["batch"]
    profiles = all_data["profiles"]
    workloads = sorted(batch_data.keys())

    x = np.arange(len(workloads))
    width = 0.8 / len(profiles)

    fig, ax = plt.subplots(figsize=(max(12, len(workloads) * 1.5), 6))

    for i, prof in enumerate(profiles):
        vals = [safe_float(batch_data[wl].get(prof)) / 1000.0 for wl in workloads]
        color = PROFILE_COLORS.get(prof, "#555555")
        ax.bar(x + i * width, vals, width, label=prof, color=color, edgecolor="black", linewidth=0.5)

    ax.set_xticks(x + width * (len(profiles) - 1) / 2)
    ax.set_xticklabels(workloads, rotation=15, ha="right")
    ax.set_ylabel("Duration (s)")
    ax.set_title("Batch Job Durations — All Profiles Grouped")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "batch_grouped_bar.png", bbox_inches="tight")
    plt.close()

def plot_rtt_vs_avg_penalty(all_data: Dict, out_dir: Path):
    """Scatter: WAN RTT vs average latency penalty."""
    if not all_data:
        return

    latency_data = all_data["latency"]
    rtts = all_data["rtts"]
    profiles = [p for p in all_data["profiles"] if p != "edge"]

    rtt_vals = []
    avg_pens = []
    labels = []

    for prof in profiles:
        rtt = rtts.get(prof, 0)
        penalties = []
        for wl, vals in latency_data.items():
            edge_val = safe_float(vals.get("edge"))
            cloud_val = safe_float(vals.get(prof))
            if edge_val and cloud_val:
                penalties.append(cloud_val - edge_val)

        if penalties:
            rtt_vals.append(rtt)
            avg_pens.append(np.mean(penalties))
            labels.append(prof)

    if not rtt_vals:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = [PROFILE_COLORS.get(p, "#555555") for p in labels]
    ax.scatter(rtt_vals, avg_pens, s=150, c=colors, edgecolor="black", linewidth=1.5, alpha=0.8)

    for i, lbl in enumerate(labels):
        ax.annotate(lbl, (rtt_vals[i], avg_pens[i]), xytext=(5, 5), textcoords="offset points", fontsize=10)

    ax.set_xlabel("WAN RTT (ms)")
    ax.set_ylabel("Avg Latency Penalty (ms)")
    ax.set_title("WAN RTT vs Average Latency Penalty")
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "rtt_vs_penalty_scatter.png", bbox_inches="tight")
    plt.close()

def plot_rtt_vs_avg_batch_gain(all_data: Dict, out_dir: Path):
    """Scatter: WAN RTT vs average batch gain %."""
    if not all_data:
        return

    batch_data = all_data["batch"]
    rtts = all_data["rtts"]
    profiles = [p for p in all_data["profiles"] if p != "edge"]

    rtt_vals = []
    avg_gains = []
    labels = []

    for prof in profiles:
        rtt = rtts.get(prof, 0)
        gains = []
        for wl, vals in batch_data.items():
            edge_val = safe_float(vals.get("edge"))
            cloud_val = safe_float(vals.get(prof))
            if edge_val > 0 and cloud_val:
                gains.append((edge_val - cloud_val) / edge_val * 100.0)

        if gains:
            rtt_vals.append(rtt)
            avg_gains.append(np.mean(gains))
            labels.append(prof)

    if not rtt_vals:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = [PROFILE_COLORS.get(p, "#555555") for p in labels]
    ax.scatter(rtt_vals, avg_gains, s=150, c=colors, edgecolor="black", linewidth=1.5, alpha=0.8)

    for i, lbl in enumerate(labels):
        ax.annotate(lbl, (rtt_vals[i], avg_gains[i]), xytext=(5, 5), textcoords="offset points", fontsize=10)

    ax.set_xlabel("WAN RTT (ms)")
    ax.set_ylabel("Avg Batch Gain (%)")
    ax.set_title("WAN RTT vs Average Batch Offload Gain")
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "rtt_vs_batch_gain_scatter.png", bbox_inches="tight")
    plt.close()

def plot_pass_rates_all_profiles(edge_run: Path, cloud_root: Path, profiles: List[str], load: str, out_dir: Path):
    """Bar chart of pass rates across all profiles."""
    edge_sum = load_slo_summary(edge_run)
    if not edge_sum:
        return

    all_profiles = ["edge"] + profiles
    pass_counts = []
    total_counts = []

    # Edge
    eitems = edge_sum.get("items", [])
    pass_counts.append(sum(1 for i in eitems if i.get("pass")))
    total_counts.append(len(eitems))

    # Cloud profiles
    for prof in profiles:
        cloud_run = resolve_cloud_run(cloud_root, prof, load)
        if not cloud_run:
            pass_counts.append(0)
            total_counts.append(0)
            continue
        cloud_sum = load_slo_summary(cloud_run)
        if not cloud_sum:
            pass_counts.append(0)
            total_counts.append(0)
            continue
        citems = cloud_sum.get("items", [])
        pass_counts.append(sum(1 for i in citems if i.get("pass")))
        total_counts.append(len(citems))

    pass_rates = [p/t*100 if t > 0 else 0 for p, t in zip(pass_counts, total_counts)]

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = [PROFILE_COLORS.get(p, "#555555") for p in all_profiles]
    bars = ax.bar(all_profiles, pass_rates, color=colors, edgecolor="black", linewidth=1)

    # Annotate
    for i, (bar, cnt, tot) in enumerate(zip(bars, pass_counts, total_counts)):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, height + 1,
                f"{cnt}/{tot}\n{height:.0f}%", ha="center", va="bottom", fontsize=9, weight="bold")

    ax.set_ylabel("Pass Rate (%)")
    ax.set_title("SLO Pass Rates Across All Profiles")
    ax.set_ylim(0, 110)
    ax.grid(axis="y", alpha=0.3)
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(out_dir / "pass_rates_all_profiles.png", bbox_inches="tight")
    plt.close()

def plot_penalty_distribution(all_data: Dict, out_dir: Path):
    """Box plot showing distribution of latency penalties per profile."""
    if not all_data or not all_data["latency"]:
        return

    latency_data = all_data["latency"]
    profiles = [p for p in all_data["profiles"] if p != "edge"]

    penalty_dist = {prof: [] for prof in profiles}

    for wl, vals in latency_data.items():
        edge_val = safe_float(vals.get("edge"))
        for prof in profiles:
            cloud_val = safe_float(vals.get(prof))
            if edge_val and cloud_val:
                penalty_dist[prof].append(cloud_val - edge_val)

    data_to_plot = [penalty_dist[prof] for prof in profiles if penalty_dist[prof]]
    active_profiles = [prof for prof in profiles if penalty_dist[prof]]

    if not data_to_plot:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    bp = ax.boxplot(data_to_plot, labels=active_profiles, patch_artist=True)

    for patch, prof in zip(bp['boxes'], active_profiles):
        patch.set_facecolor(PROFILE_COLORS.get(prof, "#CCCCCC"))

    ax.axhline(0, color="red", linestyle="--", linewidth=1, label="No penalty")
    ax.set_ylabel("Latency Penalty (ms)")
    ax.set_title("Latency Penalty Distribution by Profile")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(out_dir / "penalty_distribution_boxplot.png", bbox_inches="tight")
    plt.close()

def plot_batch_gain_distribution(all_data: Dict, out_dir: Path):
    """Box plot showing distribution of batch gains (%) per profile."""
    if not all_data or not all_data["batch"]:
        return

    batch_data = all_data["batch"]
    profiles = [p for p in all_data["profiles"] if p != "edge"]

    gain_dist = {prof: [] for prof in profiles}

    for wl, vals in batch_data.items():
        edge_val = safe_float(vals.get("edge"))
        for prof in profiles:
            cloud_val = safe_float(vals.get(prof))
            if edge_val > 0 and cloud_val:
                gain_dist[prof].append((edge_val - cloud_val) / edge_val * 100.0)

    data_to_plot = [gain_dist[prof] for prof in profiles if gain_dist[prof]]
    active_profiles = [prof for prof in profiles if gain_dist[prof]]

    if not data_to_plot:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    bp = ax.boxplot(data_to_plot, labels=active_profiles, patch_artist=True)

    for patch, prof in zip(bp['boxes'], active_profiles):
        patch.set_facecolor(PROFILE_COLORS.get(prof, "#CCCCCC"))

    ax.axhline(0, color="red", linestyle="--", linewidth=1, label="No gain")
    ax.set_ylabel("Batch Offload Gain (%)")
    ax.set_title("Batch Offload Gain Distribution by Profile")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(out_dir / "batch_gain_distribution_boxplot.png", bbox_inches="tight")
    plt.close()

def plot_stacked_bar_workload_composition(all_data: Dict, out_dir: Path):
    """Stacked bar showing composition of total latency per profile."""
    if not all_data or not all_data["latency"]:
        return

    latency_data = all_data["latency"]
    profiles = all_data["profiles"]
    workloads = sorted(latency_data.keys())

    # Build matrix: profiles x workloads
    data = []
    for prof in profiles:
        data.append([safe_float(latency_data[wl].get(prof)) for wl in workloads])

    data = np.array(data)

    fig, ax = plt.subplots(figsize=(12, 6))

    colors_list = [PROFILE_COLORS.get(prof, "#555555") for prof in profiles]
    x = np.arange(len(workloads))

    bottoms = np.zeros(len(workloads))
    for i, prof in enumerate(profiles):
        ax.bar(x, data[i], bottom=bottoms, label=prof, color=colors_list[i], edgecolor="black", linewidth=0.3)
        bottoms += data[i]

    ax.set_xticks(x)
    ax.set_xticklabels(workloads, rotation=15, ha="right")
    ax.set_ylabel("Cumulative Latency (ms)")
    ax.set_title("Stacked Latency Composition by Workload")
    ax.legend(loc="upper left")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "latency_stacked_composition.png", bbox_inches="tight")
    plt.close()

def plot_radar_chart(all_data: Dict, out_dir: Path):
    """Radar chart comparing profiles across different workloads (normalized)."""
    if not all_data or not all_data["latency"]:
        return

    latency_data = all_data["latency"]
    profiles = all_data["profiles"]
    workloads = sorted(latency_data.keys())

    if len(workloads) < 3:
        return  # Radar needs at least 3 axes

    # Normalize each workload by edge value
    normalized = {prof: [] for prof in profiles}
    for wl in workloads:
        edge_val = safe_float(latency_data[wl].get("edge"), 1)
        for prof in profiles:
            val = safe_float(latency_data[wl].get(prof))
            normalized[prof].append(val / edge_val if edge_val > 0 else 0)

    angles = np.linspace(0, 2 * np.pi, len(workloads), endpoint=False).tolist()
    angles += angles[:1]  # Close the circle

    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))

    for prof in profiles:
        vals = normalized[prof] + normalized[prof][:1]
        color = PROFILE_COLORS.get(prof, "#555555")
        ax.plot(angles, vals, 'o-', linewidth=2, label=prof, color=color)
        ax.fill(angles, vals, alpha=0.15, color=color)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(workloads, size=9)
    max_val = max(max(normalized[p]) for p in profiles if normalized[p])
    ax.set_ylim(0, max_val * 1.1 if max_val > 0 else 1)
    ax.set_title("Normalized Latency Radar Chart (vs Edge)", size=14, pad=20)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1))
    ax.grid(True)

    plt.tight_layout()
    plt.savefig(out_dir / "latency_radar_normalized.png", bbox_inches="tight")
    plt.close()

def plot_profile_comparison_matrix(edge_run: Path, cloud_root: Path, profiles: List[str], load: str, out_dir: Path):
    """Grid showing key metrics for each profile."""
    edge_sum = load_slo_summary(edge_run)
    if not edge_sum:
        return

    all_profiles = ["edge"] + profiles

    metrics = []
    for prof in all_profiles:
        if prof == "edge":
            run = edge_run
            rtt = 0
        else:
            run = resolve_cloud_run(cloud_root, prof, load)
            if not run:
                continue
            rtt = get_wan_rtt(run) or 0

        summ = load_slo_summary(run)
        if not summ:
            continue

        items = summ.get("items", [])
        pass_count = sum(1 for i in items if i.get("pass"))
        total = len(items)
        pass_rate = pass_count / total * 100 if total > 0 else 0

        # Avg latency
        lat_items = [i for i in items if i.get("kind") == "Deployment" and i.get("class") == "latency"]
        avg_lat = np.mean([safe_float(i.get("measured_ms")) for i in lat_items]) if lat_items else 0

        # Avg batch duration
        batch_items = [i for i in items if i.get("kind") == "Job" and i.get("class") in ("batch", "ml")]
        avg_batch = np.mean([safe_float(i.get("measured_ms")) for i in batch_items]) / 1000.0 if batch_items else 0

        metrics.append({
            "profile": prof,
            "rtt": rtt,
            "pass_rate": pass_rate,
            "avg_latency": avg_lat,
            "avg_batch": avg_batch,
        })

    if not metrics:
        return

    df = pd.DataFrame(metrics)

    fig, ax = plt.subplots(figsize=(12, max(6, len(metrics) * 0.6)))
    ax.axis('tight')
    ax.axis('off')

    table_data = []
    for _, row in df.iterrows():
        table_data.append([
            row["profile"],
            f"{row['rtt']:.0f} ms",
            f"{row['pass_rate']:.0f}%",
            f"{row['avg_latency']:.1f} ms",
            f"{row['avg_batch']:.1f} s"
        ])

    table = ax.table(cellText=table_data,
                     colLabels=["Profile", "WAN RTT", "Pass Rate", "Avg Latency", "Avg Batch"],
                     cellLoc='center',
                     loc='center',
                     colWidths=[0.15, 0.15, 0.15, 0.2, 0.2])

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)

    # Color rows
    for i, prof in enumerate(df["profile"], start=1):
        color = PROFILE_COLORS.get(prof, "#FFFFFF")
        for j in range(5):
            table[(i, j)].set_facecolor(color)
            table[(i, j)].set_alpha(0.3)

    plt.title("Profile Comparison Matrix", fontsize=14, pad=20)
    plt.tight_layout()
    plt.savefig(out_dir / "profile_comparison_matrix.png", bbox_inches="tight")
    plt.close()

def plot_relative_performance_bars(all_data: Dict, out_dir: Path):
    """Bar chart showing relative performance of each profile vs edge (lower is better for latency)."""
    if not all_data:
        return

    latency_data = all_data["latency"]
    batch_data = all_data["batch"]
    profiles = [p for p in all_data["profiles"] if p != "edge"]

    # Calculate average relative performance
    lat_rel = []
    batch_rel = []

    for prof in profiles:
        # Latency: avg(cloud/edge)
        ratios = []
        for wl, vals in latency_data.items():
            edge_val = safe_float(vals.get("edge"))
            cloud_val = safe_float(vals.get(prof))
            if edge_val > 0:
                ratios.append(cloud_val / edge_val)
        lat_rel.append(np.mean(ratios) if ratios else 1.0)

        # Batch: avg(cloud/edge)
        ratios = []
        for wl, vals in batch_data.items():
            edge_val = safe_float(vals.get("edge"))
            cloud_val = safe_float(vals.get(prof))
            if edge_val > 0:
                ratios.append(cloud_val / edge_val)
        batch_rel.append(np.mean(ratios) if ratios else 1.0)

    if not lat_rel or not batch_rel:
        return

    x = np.arange(len(profiles))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))

    bars1 = ax.bar(x - width/2, lat_rel, width, label="Latency (lower=better)",
                   color=[PROFILE_COLORS.get(p, "#555555") for p in profiles],
                   edgecolor="black", linewidth=1, alpha=0.7)
    bars2 = ax.bar(x + width/2, batch_rel, width, label="Batch (lower=better)",
                   color=[PROFILE_COLORS.get(p, "#555555") for p in profiles],
                   edgecolor="black", linewidth=1, alpha=0.4)

    ax.axhline(1.0, color="red", linestyle="--", linewidth=1.5, label="Edge baseline")
    ax.set_xticks(x)
    ax.set_xticklabels(profiles, rotation=15, ha="right")
    ax.set_ylabel("Relative Performance (vs Edge)")
    ax.set_title("Average Relative Performance vs Edge")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "relative_performance_bars.png", bbox_inches="tight")
    plt.close()

def plot_individual_workload_profiles(all_data: Dict, out_dir: Path):
    """Create individual line charts for each workload showing all profiles."""
    if not all_data:
        return

    latency_data = all_data["latency"]
    batch_data = all_data["batch"]
    profiles = all_data["profiles"]

    # Latency workloads
    for wl, vals in latency_data.items():
        fig, ax = plt.subplots(figsize=(8, 5))
        y_vals = [safe_float(vals.get(p)) for p in profiles]
        colors = [PROFILE_COLORS.get(p, "#555555") for p in profiles]
        ax.bar(profiles, y_vals, color=colors, edgecolor="black", linewidth=1)
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"Latency: {wl} Across All Profiles")
        ax.grid(axis="y", alpha=0.3)
        plt.xticks(rotation=15, ha="right")
        plt.tight_layout()
        safe_name = wl.replace("/", "_").replace(" ", "_")
        plt.savefig(out_dir / f"workload_latency_{safe_name}.png", bbox_inches="tight")
        plt.close()

    # Batch workloads
    for wl, vals in batch_data.items():
        fig, ax = plt.subplots(figsize=(8, 5))
        y_vals = [safe_float(vals.get(p)) / 1000.0 for p in profiles]
        colors = [PROFILE_COLORS.get(p, "#555555") for p in profiles]
        ax.bar(profiles, y_vals, color=colors, edgecolor="black", linewidth=1)
        ax.set_ylabel("Duration (s)")
        ax.set_title(f"Batch: {wl} Across All Profiles")
        ax.grid(axis="y", alpha=0.3)
        plt.xticks(rotation=15, ha="right")
        plt.tight_layout()
        safe_name = wl.replace("/", "_").replace(" ", "_")
        plt.savefig(out_dir / f"workload_batch_{safe_name}.png", bbox_inches="tight")
        plt.close()

def main():
    ap = argparse.ArgumentParser(description="Compare one edge baseline vs all cloud profiles")
    ap.add_argument("--edge", required=True, help="Edge results root (baseline)")
    ap.add_argument("--cloud", required=True, help="Cloud results root (multi-profile)")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--load", default="low", help="Local load profile to use (default: low)")
    ap.add_argument("--edge-run", default="", help="Optional: explicit path to edge run dir containing slo_summary.json")
    ap.add_argument("--print", action="store_true", help="Print brief console summary")
    args = ap.parse_args()

    edge_root = Path(args.edge).resolve()
    cloud_root = Path(args.cloud).resolve()
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Select edge baseline run
    if args.edge_run:
        edge_run = Path(args.edge_run).resolve()
    else:
        edge_run = select_edge_run(edge_root, args.load)
    if not edge_run or not (edge_run / "slo_summary.json").exists():
        print("Error: could not find edge baseline run (slo_summary.json). Use --edge-run to specify explicitly.")
        return 2

    # Discover all cloud profiles
    profiles = find_cloud_profiles(cloud_root)
    if not profiles:
        print("No cloud profiles found under:", cloud_root)
        return 2

    print(f"Found profiles: {', '.join(profiles)}")

    # Generate per-profile charts (original)
    for prof in profiles:
        cloud_run = resolve_cloud_run(cloud_root, prof, args.load)  # FIXED: load -> args.load
        if not cloud_run:
            continue
        res = analyze_profile(edge_run, cloud_run)
        if "error" in res:
            continue
        rtt = get_wan_rtt(cloud_run)

        plot_latency_profile(prof, res["latency"], rtt, out_dir)
        plot_batch_profile(prof, res["batch"], out_dir)

        if args.print:
            ep, et = res["pass"]["edge"]
            cp, ct = res["pass"]["cloud"]
            print(f"[{prof}] pass edge {ep}/{et}, cloud {cp}/{ct}; RTT≈{rtt or 0} ms")

    # Summary dashboard (original)
    summary_dashboard(edge_run, cloud_root, profiles, args.load, out_dir)

    # NEW: Collect all data for combined graphs
    all_data = collect_all_data(edge_run, cloud_root, profiles, args.load)

    if all_data:
        print("Generating combined profile graphs...")

        # Line charts
        plot_latency_all_profiles(all_data, out_dir)
        plot_batch_all_profiles(all_data, out_dir)

        # Grouped bar charts
        plot_grouped_bar_latency(all_data, out_dir)
        plot_grouped_bar_batch(all_data, out_dir)

        # Heatmaps
        plot_latency_heatmap(all_data, out_dir)
        plot_batch_heatmap(all_data, out_dir)

        # Scatter plots
        plot_rtt_vs_avg_penalty(all_data, out_dir)
        plot_rtt_vs_avg_batch_gain(all_data, out_dir)

        # Pass rates
        plot_pass_rates_all_profiles(edge_run, cloud_root, profiles, args.load, out_dir)

        # Distribution plots
        plot_penalty_distribution(all_data, out_dir)
        plot_batch_gain_distribution(all_data, out_dir)

        # Advanced visualizations
        plot_stacked_bar_workload_composition(all_data, out_dir)
        plot_radar_chart(all_data, out_dir)
        plot_relative_performance_bars(all_data, out_dir)

        # Comparison matrix
        plot_profile_comparison_matrix(edge_run, cloud_root, profiles, args.load, out_dir)

        # Individual workload charts
        plot_individual_workload_profiles(all_data, out_dir)

    print(f"\n✓ Saved all figures to: {out_dir}")
    print("\nGenerated graphs:")
    print("  PER-PROFILE (original):")
    print("    - latency_profile_<profile>.png")
    print("    - batch_profile_<profile>.png")
    print("    - summary_wan_impact.png")
    print("\n  ALL-PROFILES COMBINED:")
    print("    - latency_all_profiles_line.png")
    print("    - batch_all_profiles_line.png")
    print("    - latency_grouped_bar.png")
    print("    - batch_grouped_bar.png")
    print("    - latency_penalty_heatmap.png")
    print("    - batch_gain_heatmap.png")
    print("    - rtt_vs_penalty_scatter.png")
    print("    - rtt_vs_batch_gain_scatter.png")
    print("    - pass_rates_all_profiles.png")
    print("    - penalty_distribution_boxplot.png")
    print("    - batch_gain_distribution_boxplot.png")
    print("    - latency_stacked_composition.png")
    print("    - latency_radar_normalized.png")
    print("    - relative_performance_bars.png")
    print("    - profile_comparison_matrix.png")
    print("\n  INDIVIDUAL WORKLOADS:")
    print("    - workload_latency_<name>.png")
    print("    - workload_batch_<name>.png")

if __name__ == "__main__":
    raise SystemExit(main())