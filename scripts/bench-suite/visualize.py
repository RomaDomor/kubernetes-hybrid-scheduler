#!/usr/bin/env python3
"""
Benchmark visualiser for the hybrid-scheduler comparison study.

Generates two groups of plots:
  • Per-scheduler detailed reports  (Section A–D)
  • Cross-scheduler comparison suite (Section E)  ← the primary academic contribution

Run:
    python3 visualize.py <results_dir> [--warmup-runs N]
"""
import argparse
import json
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.ticker import MaxNLocator, FuncFormatter

# ---------------------------------------------------------------------------
# Global Configuration
# ---------------------------------------------------------------------------

# Ordered lists used for consistent axis labelling across all plots.
WAN_ORDER        = ['clear', 'good', 'moderate', 'poor']
LOAD_ORDER       = ['none', 'low', 'medium', 'high']
# The four strategies under comparison (the central variable of the study)
SCHEDULER_ORDER  = ['smart', 'single-cluster', 'liqo-native', 'round-robin']

# Human-readable labels for scheduler modes (for paper-quality axis ticks)
SCHEDULER_LABELS = {
    'smart':          'Smart\n(Hybrid)',
    'single-cluster': 'Single-Cluster\n(WAN-unaware)',
    'liqo-native':    'Liqo\nNative',
    'round-robin':    'Round-Robin',
}

# Colour palette for the four schedulers (consistent across all comparison plots)
SCHEDULER_PALETTE = {
    'smart':          '#2ca02c',   # green  – the algorithm under test
    'single-cluster': '#d62728',   # red    – WAN-unaware baseline
    'liqo-native':    '#ff7f0e',   # orange – Liqo default
    'round-robin':    '#1f77b4',   # blue   – deterministic baseline
}

# Cluster-type colour palette used in placement plots
CLUSTER_PALETTE = {
    'edge':    '#F5A623',
    'fog':     '#7B68EE',
    'cloud':   '#4A90E2',
    'remote':  '#8B8B8B',
    'unknown': '#CCCCCC',
}

# The identifier used to detect cloud-cluster nodes from node names.
# Adjust if your cloud nodes have a different naming convention.
CLOUD_NODE_IDENTIFIER = 'mypool'

# Professional colour palettes for other plots
CATEGORICAL_PALETTE = "Set2"
SEQUENTIAL_PALETTE  = "YlOrRd"
DIVERGING_PALETTE   = "RdYlBu_r"


# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------

def setup_theme():
    sns.set_theme(style="whitegrid", palette=CATEGORICAL_PALETTE, font_scale=1.0)
    plt.rcParams.update({
        'figure.facecolor': 'white',
        'axes.facecolor':   '#f8f9fa',
        'axes.edgecolor':   '#e0e0e0',
        'grid.color':       '#e8e8e8',
        'grid.linewidth':   0.7,
        'font.family':      'sans-serif',
        'font.sans-serif':  ['Arial', 'Helvetica'],
        'axes.labelsize':   11,
        'axes.titlesize':   13,
        'xtick.labelsize':  10,
        'ytick.labelsize':  10,
        'legend.fontsize':  10,
        'lines.linewidth':  1.8,
        'patch.linewidth':  0.5,
    })


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def _parse_dir_name(dir_name: str):
    """
    Parse both naming conventions:
      Old: config-<name>_wan-<profile>_load-<load>
      New: scheduler-<mode>_wan-<profile>_load-<load>
    Returns (config_name, wan_profile, load_profile) or None.
    """
    m = re.match(r"(?:scheduler|config)-(.+)_wan-(.+)_load-(.+)", dir_name)
    return m.groups() if m else None


def load_all_run_data(base_dir: Path, warmup_runs: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load and aggregate SLO summary data from all run directories."""
    all_rows = []
    print("Scanning for SLO summary data…")

    for profile_dir in sorted(base_dir.glob("*_wan-*_load-*")):
        parsed = _parse_dir_name(profile_dir.name)
        if not parsed:
            print(f"  Warning: skipping malformed dir: {profile_dir.name}")
            continue
        config_name, wan_profile, load_profile = parsed

        for run_dir in sorted(profile_dir.glob("run-*")):
            try:
                result_dir = next(d for d in run_dir.iterdir() if d.is_dir())
            except StopIteration:
                continue

            try:
                run_number = int(run_dir.name.replace("run-", ""))
            except ValueError:
                continue

            http_data = parse_http_benchmark(result_dir / "http_benchmark.txt")
            slo_file  = result_dir / "slo_summary.json"
            meta_file = result_dir / "run_meta.json"

            if not slo_file.is_file():
                continue

            with open(slo_file) as f:
                slo_data = json.load(f)

            # Prefer the scheduler_mode field from run_meta if available
            scheduler_mode = config_name
            if meta_file.is_file():
                with open(meta_file) as f:
                    meta = json.load(f)
                scheduler_mode = meta.get("scheduler_mode", config_name)

            for item in slo_data.get("items", []):
                all_rows.append({
                    "config_name":  scheduler_mode,
                    "wan_profile":  wan_profile,
                    "local_load":   load_profile,
                    "run":          run_number,
                    **item,
                    **http_data,
                })

    if not all_rows:
        sys.exit("Error: No valid SLO summary data found.")

    df_all = pd.DataFrame(all_rows)
    _apply_categorical_order(df_all)

    print(f"Loaded {len(df_all)} SLO records from all runs.")
    df_stable = df_all[df_all['run'] > warmup_runs].copy()
    if not df_stable.empty:
        df_stable = df_stable.copy()
        df_stable['run'] = df_stable['run'] - warmup_runs
    print(f"  → Using {len(df_stable)} stable records (post warmup).")
    return df_all, df_stable


def _apply_categorical_order(df: pd.DataFrame):
    """Apply consistent categorical ordering to standard columns."""
    present_schedulers = [s for s in SCHEDULER_ORDER if s in df.get('config_name', pd.Series()).unique()]
    if not present_schedulers:
        present_schedulers = sorted(df['config_name'].unique()) if 'config_name' in df.columns else []
    if 'config_name' in df.columns:
        df['config_name'] = pd.Categorical(df['config_name'],
                                           categories=present_schedulers, ordered=True)
    if 'wan_profile' in df.columns:
        df['wan_profile'] = pd.Categorical(df['wan_profile'], categories=WAN_ORDER, ordered=True)
    if 'local_load' in df.columns:
        df['local_load'] = pd.Categorical(df['local_load'], categories=LOAD_ORDER, ordered=True)


def parse_http_benchmark(path: Path) -> dict:
    if not path.is_file():
        return {}
    content, results = path.read_text(), {}
    for p in ["50", "90", "95", "99"]:
        if match := re.search(rf"\s+{p}%\s+in\s+([\d.]+)", content):
            results[f'http_p{p}_ms'] = float(match.group(1)) * 1000
    return results


def load_placement_data(base_dir: Path, warmup_runs: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load pod → node mapping data from all run directories."""
    all_placements = []
    print("\nScanning for pod placement data…")

    for profile_dir in sorted(base_dir.glob("*_wan-*_load-*")):
        parsed = _parse_dir_name(profile_dir.name)
        if not parsed:
            continue
        config_name, wan_profile, load_profile = parsed

        for run_dir in sorted(profile_dir.glob("run-*")):
            try:
                result_dir = next(d for d in run_dir.iterdir() if d.is_dir())
            except StopIteration:
                continue
            try:
                run_number = int(run_dir.name.replace("run-", ""))
            except ValueError:
                continue

            # Prefer scheduler_mode from meta
            meta_file = result_dir / "run_meta.json"
            scheduler_mode = config_name
            if meta_file.is_file():
                with open(meta_file) as f:
                    scheduler_mode = json.load(f).get("scheduler_mode", config_name)

            placement_file = result_dir / "pod_node_map.csv"
            if not placement_file.is_file():
                continue

            df_p = pd.read_csv(placement_file)
            for _, row in df_p.iterrows():
                cluster_type = row.get('cluster_type', None)
                if not cluster_type or pd.isna(cluster_type):
                    cluster_type = (
                        'cloud' if CLOUD_NODE_IDENTIFIER in str(row.get('node_name', ''))
                        else 'edge'
                    )
                all_placements.append({
                    "config_name":  scheduler_mode,
                    "wan_profile":  wan_profile,
                    "local_load":   load_profile,
                    "run":          run_number,
                    "pod_name":     row.get('pod_name', ''),
                    "node_name":    row.get('node_name', ''),
                    "workload":     _workload_from_pod_name(str(row.get('pod_name', ''))),
                    "node_type":    cluster_type,
                })

    if not all_placements:
        return pd.DataFrame(), pd.DataFrame()

    df_all = pd.DataFrame(all_placements)
    _apply_categorical_order(df_all)

    df_stable = df_all[df_all['run'] > warmup_runs].copy()
    if not df_stable.empty:
        df_stable['run'] = df_stable['run'] - warmup_runs

    return df_all, df_stable


def load_placement_stats(base_dir: Path, warmup_runs: int) -> pd.DataFrame:
    """
    Load per-run placement_stats.json files produced by telemetry.collect_placement_stats.
    Returns a flat dataframe with one row per (scheduler_mode, wan, load, run, workload, cluster).
    """
    rows = []
    for profile_dir in sorted(base_dir.glob("*_wan-*_load-*")):
        parsed = _parse_dir_name(profile_dir.name)
        if not parsed:
            continue
        config_name, wan_profile, load_profile = parsed

        for run_dir in sorted(profile_dir.glob("run-*")):
            try:
                result_dir = next(d for d in run_dir.iterdir() if d.is_dir())
                run_number = int(run_dir.name.replace("run-", ""))
            except (StopIteration, ValueError):
                continue

            if run_number <= warmup_runs:
                continue

            meta_file = result_dir / "run_meta.json"
            scheduler_mode = config_name
            if meta_file.is_file():
                with open(meta_file) as f:
                    scheduler_mode = json.load(f).get("scheduler_mode", config_name)

            stats_file = result_dir / "placement_stats.json"
            if not stats_file.is_file():
                continue

            with open(stats_file) as f:
                stats = json.load(f)

            for workload, clusters in stats.get("by_workload", {}).items():
                for cluster, count in clusters.items():
                    rows.append({
                        "scheduler_mode": scheduler_mode,
                        "wan_profile":    wan_profile,
                        "local_load":     load_profile,
                        "run":            run_number,
                        "workload":       workload,
                        "cluster":        cluster,
                        "count":          count,
                        "offload_rate":   stats.get("offload_rate_pct", 0.0),
                    })

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    _apply_categorical_order(df)
    return df


def _workload_from_pod_name(pod_name: str) -> str:
    base = re.split(r'-(?=[a-z0-9]{5}$)', pod_name)[0]
    base = re.split(r'-(?=[a-z0-9]{9,10}-[a-z0-9]{5}$)', base)[0]
    return base


def _set_integer_run_ticks(grid, dataframe_with_run):
    for ax in grid.axes.flat:
        xmin, xmax = ax.get_xlim()
        runs  = sorted(dataframe_with_run['run'].unique())
        ticks = [r for r in runs if xmin <= r <= xmax]
        if ticks:
            ax.set_xticks(ticks)
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{int(x)}"))


# ---------------------------------------------------------------------------
# Per-Scheduler Plots (Sections A–D)
# ---------------------------------------------------------------------------

def plot_2_job_duration_bars(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: A2. Mean Job Duration Bars")
    job_df = df[df['kind'] == 'Job'].copy()
    if job_df.empty: return
    job_df['duration_s'] = job_df['measured_ms'] / 1000.0
    g = sns.catplot(data=job_df, x='workload', y='duration_s', hue='wan_profile',
                    col='local_load', kind='bar', errorbar='sd', capsize=0.1,
                    aspect=1.3, height=5.5, palette="husl",
                    col_order=job_df['local_load'].cat.categories)
    g.set_axis_labels("Job", "Mean Duration (s)", fontsize=11, fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')
    if g.figure.legends: g.figure.legends[0].set_title("WAN Profile")
    for ax in g.axes.flat: ax.tick_params(axis='x', rotation=45); sns.despine(ax=ax)
    g.figure.suptitle(f"Mean Job Duration ({config_name})", fontsize=14, fontweight='bold', y=0.995)
    g.tight_layout()
    g.savefig(output_dir / "A2_mean_job_duration_bars.png", dpi=300)
    plt.close()


def plot_3_slo_pass_rate_heatmap(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: A3. SLO Pass Rate Heatmap")
    pass_rate = (df.groupby(['wan_profile', 'local_load', 'workload'], observed=True)['pass']
                 .value_counts(normalize=True).unstack(fill_value=0))
    pass_rate['pass_pct'] = pass_rate.get(True, 0) * 100
    heatmap_data = pass_rate.pivot_table(index='workload', columns=['local_load', 'wan_profile'],
                                         values='pass_pct', observed=True)
    if heatmap_data.empty: return
    fig, ax = plt.subplots(figsize=(14, 7))
    sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap="RdYlGn", linewidths=1,
                linecolor='white', cbar_kws={'label': 'SLO Pass Rate (%)'}, vmin=0, vmax=100, ax=ax)
    ax.set_title(f"SLO Pass Rate ({config_name})", fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel("Load / WAN Profile", fontsize=12, fontweight='semibold')
    ax.set_ylabel("Workload", fontsize=12, fontweight='semibold')
    plt.xticks(rotation=45, ha='right')
    plt.savefig(output_dir / "A3_slo_pass_rate_heatmap.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_5_performance_interaction(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: B2. Performance Interaction (build-job)")
    job_to_plot = 'build-job'
    plot_df = df[df['workload'] == job_to_plot].copy()
    if plot_df.empty: return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0
    fig, ax = plt.subplots(figsize=(11, 6.5))
    sns.pointplot(data=plot_df, x='wan_profile', y='duration_s', hue='local_load',
                  errorbar='sd', capsize=0.15, palette='Set2', ax=ax, order=WAN_ORDER,
                  hue_order=plot_df['local_load'].cat.categories)
    ax.set_title(f"WAN × Load Interaction on '{job_to_plot}' ({config_name})", fontsize=13, fontweight='bold')
    ax.set_xlabel("WAN Profile", fontsize=11, fontweight='semibold')
    ax.set_ylabel("Mean Duration (s)", fontsize=11, fontweight='semibold')
    ax.legend(title="Local Load")
    plt.savefig(output_dir / "B2_performance_interaction_plot.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_6_slo_failure_magnitude(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: B3. SLO Failure Magnitude")
    plot_df = df[(df['pass'] == False) & df['measured_ms'].notna() & df['target_ms'].notna()].copy()
    if plot_df.empty: return
    plot_df['overshoot_pct'] = (plot_df['measured_ms'] - plot_df['target_ms']) / plot_df['target_ms'] * 100
    g = sns.catplot(data=plot_df, x='workload', y='overshoot_pct', hue='wan_profile',
                    col='local_load', kind='bar', errorbar='sd', capsize=0.08, height=5.5,
                    aspect=1.3, palette="OrRd", col_order=plot_df['local_load'].cat.categories)
    g.set_axis_labels("Workload", "Deadline Overshoot (%)", fontsize=11, fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')
    g.legend.set_title("WAN Profile")
    for ax in g.axes.flat: ax.tick_params(axis='x', rotation=30)
    g.figure.suptitle(f"SLO Failure Magnitude ({config_name})", fontsize=14, fontweight='bold', y=0.995)
    g.tight_layout()
    g.savefig(output_dir / "B3_slo_failure_magnitude.png", dpi=300)
    plt.close()


def plot_7_raw_data_variance(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: B4. Raw Variance (cpu-batch)")
    job_to_plot = 'cpu-batch'
    plot_df = df[df['workload'] == job_to_plot].copy()
    if plot_df.empty: return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0
    fig, ax = plt.subplots(figsize=(13, 7))
    sns.boxplot(data=plot_df, x='wan_profile', y='duration_s', hue='local_load', showfliers=False,
                palette='Set2', ax=ax, order=WAN_ORDER, hue_order=plot_df['local_load'].cat.categories)
    sns.stripplot(data=plot_df, x='wan_profile', y='duration_s', hue='local_load', dodge=True,
                  ax=ax, alpha=0.4, size=5, palette='Set2', legend=False, order=WAN_ORDER,
                  hue_order=plot_df['local_load'].cat.categories)
    ax.set_title(f"Runtime Variance – '{job_to_plot}' ({config_name})", fontsize=13, fontweight='bold')
    ax.set_xlabel("WAN Profile", fontsize=11, fontweight='semibold')
    ax.set_ylabel("Duration (s)", fontsize=11, fontweight='semibold')
    handles, labels = ax.get_legend_handles_labels()
    if handles: ax.legend(handles, labels, title='Local Load')
    plt.savefig(output_dir / "B4_raw_data_variance.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_8_placement_analysis(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: C1. Workload Placement Matrix")
    if df.empty: return
    mean_counts = df.groupby(['wan_profile', 'local_load', 'workload', 'node_type'],
                             observed=True).size().reset_index(name='count')
    if mean_counts.empty: return
    palette = {k: CLUSTER_PALETTE.get(k, '#AAAAAA') for k in mean_counts['node_type'].unique()}
    g = sns.catplot(data=mean_counts, x='workload', y='count', hue='node_type',
                    col='wan_profile', row='local_load', kind='bar', height=4.5, aspect=1.6,
                    palette=palette, legend=False,
                    row_order=df['local_load'].cat.categories,
                    col_order=df['wan_profile'].cat.categories)
    g.set_axis_labels("Workload", "Pod Count", fontsize=11, fontweight='semibold')
    g.set_titles(row_template="Load: {row_name}", col_template="WAN: {col_name}", fontsize=11)
    handles = [mpatches.Patch(fc=CLUSTER_PALETTE.get(k, '#AAA'), label=k)
               for k in sorted(mean_counts['node_type'].unique())]
    g.figure.legend(handles=handles, title='Cluster', loc='upper center',
                    bbox_to_anchor=(0.5, -0.02), ncol=len(handles))
    for ax in g.axes.flat: ax.tick_params(axis='x', rotation=45); sns.despine(ax=ax)
    g.figure.suptitle(f"Workload Placement ({config_name})", fontsize=14, fontweight='bold', y=0.995)
    g.tight_layout()
    g.savefig(output_dir / "C1_workload_placement_matrix.png", dpi=300)
    plt.close()


def plot_10_learning_performance_over_runs(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: D2. Learning Curve")
    workload_to_plot = 'cpu-batch'
    plot_df = df[df['workload'] == workload_to_plot].copy()
    if plot_df.empty: return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0
    g = sns.relplot(data=plot_df, x='run', y='duration_s', col='local_load', row='wan_profile',
                    kind='line', marker='o', errorbar='sd', height=4, aspect=1.5,
                    row_order=plot_df['wan_profile'].cat.categories,
                    col_order=plot_df['local_load'].cat.categories)
    g.set_axis_labels("Run Number", f"{workload_to_plot} Duration (s)", fontweight='semibold')
    g.set_titles(row_template="WAN: {row_name}", col_template="Load: {col_name}", fontweight='semibold')
    g.figure.suptitle(f"Scheduler Learning – '{workload_to_plot}' ({config_name})",
                      fontsize=14, fontweight='bold', y=0.995)
    _set_integer_run_ticks(g, plot_df)
    slo_target_ms = plot_df['target_ms'].max()
    if pd.notna(slo_target_ms):
        for ax in g.axes.flat:
            ax.axhline(slo_target_ms / 1000.0, ls='--', color='red', alpha=0.8, lw=1.5)
    g.tight_layout()
    g.savefig(output_dir / "D2_learning_curve_performance.png", dpi=300)
    plt.close()


def plot_11_learning_slo_improvement(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: D3. SLO Improvement Over Runs")
    if df.empty: return
    pass_by_run = (df.groupby(['wan_profile', 'local_load', 'run'], observed=True)['pass']
                   .apply(lambda x: (x == True).sum() / len(x) * 100).reset_index(name='pass_rate'))
    g = sns.relplot(data=pass_by_run, x='run', y='pass_rate', hue='wan_profile',
                    col='local_load', kind='line', marker='o', markersize=6, linewidth=2,
                    height=4.5, aspect=1.4, palette='magma',
                    col_order=pass_by_run['local_load'].cat.categories)
    g.set_axis_labels("Run Number", "SLO Pass Rate (%)", fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')
    _set_integer_run_ticks(g, pass_by_run)
    for ax in g.axes.flat:
        ax.set_ylim(-5, 105)
        ax.axhline(90, ls='--', color='green', alpha=0.6, lw=1)
    g.figure.suptitle(f"Reliability Improvement Over Runs ({config_name})",
                      fontsize=14, fontweight='bold', y=0.995)
    g.tight_layout()
    g.savefig(output_dir / "D3_slo_improvement_over_runs.png", dpi=300)
    plt.close()


# ---------------------------------------------------------------------------
# Section E: Cross-Scheduler Comparison (the key academic contribution)
# ---------------------------------------------------------------------------

def _scheduler_palette(df: pd.DataFrame) -> dict:
    """Return SCHEDULER_PALETTE filtered to modes present in df['config_name']."""
    present = df['config_name'].unique() if 'config_name' in df.columns else []
    return {m: SCHEDULER_PALETTE.get(m, '#888888') for m in present}


def plot_E1_overall_slo_comparison(df: pd.DataFrame, output_dir: Path):
    """
    E1 – Headline plot: overall SLO pass rate for each scheduler mode,
    faceted by WAN profile.  Smart should be highest in every facet.
    """
    print("  - Generating: E1. Overall SLO Pass Rate Comparison (headline)")
    if df.empty: return

    rate = (df.groupby(['config_name', 'wan_profile', 'local_load'], observed=True)['pass']
            .apply(lambda x: (x == True).sum() / len(x) * 100)
            .reset_index(name='pass_pct'))

    palette = _scheduler_palette(df)

    g = sns.catplot(
        data=rate, x='config_name', y='pass_pct', hue='local_load',
        col='wan_profile', kind='bar', errorbar='sd', capsize=0.12,
        height=5, aspect=1.1, palette='Blues_d',
        col_order=[w for w in WAN_ORDER if w in rate['wan_profile'].values],
        hue_order=[l for l in LOAD_ORDER if l in rate['local_load'].values],
        order=[s for s in SCHEDULER_ORDER if s in rate['config_name'].values],
    )
    g.set_axis_labels("Scheduler", "SLO Pass Rate (%)", fontsize=11, fontweight='semibold')
    g.set_titles("WAN: {col_name}", fontsize=12, fontweight='semibold')
    for ax in g.axes.flat:
        ax.axhline(90, ls='--', color='#2ca02c', lw=1.5, alpha=0.8, label='90% target')
        ax.set_ylim(0, 108)
        ax.tick_params(axis='x', rotation=20)
    if g.legend: g.legend.set_title("Local Load")
    g.figure.suptitle("SLO Compliance: Smart vs Baselines",
                      fontsize=15, fontweight='bold', y=1.01)
    g.tight_layout()
    g.savefig(output_dir / "E1_overall_slo_comparison.png", dpi=300)
    plt.close()


def plot_E2_scheduler_slo_by_workload(df: pd.DataFrame, output_dir: Path):
    """
    E2 – SLO pass rate per workload per scheduler, averaged over all WAN/load
    conditions.  Reveals which workloads each scheduler handles well or poorly.
    """
    print("  - Generating: E2. SLO Pass Rate by Workload and Scheduler")
    if df.empty: return

    rate = (df.groupby(['config_name', 'workload'], observed=True)['pass']
            .apply(lambda x: (x == True).sum() / len(x) * 100)
            .reset_index(name='pass_pct'))
    if rate.empty: return

    palette = _scheduler_palette(df)
    fig, ax = plt.subplots(figsize=(14, 6))
    sns.barplot(data=rate, x='workload', y='pass_pct', hue='config_name',
                palette=palette, ax=ax,
                order=sorted(rate['workload'].unique()),
                hue_order=[s for s in SCHEDULER_ORDER if s in rate['config_name'].values])
    ax.axhline(90, ls='--', color='gray', lw=1.2, alpha=0.8)
    ax.set_ylim(0, 108)
    ax.set_xlabel("Workload", fontsize=12, fontweight='semibold')
    ax.set_ylabel("SLO Pass Rate (%)", fontsize=12, fontweight='semibold')
    ax.set_title("SLO Pass Rate per Workload by Scheduler",
                 fontsize=14, fontweight='bold')
    ax.tick_params(axis='x', rotation=30)
    handles = [mpatches.Patch(fc=palette.get(m, '#888'), label=SCHEDULER_LABELS.get(m, m))
               for m in SCHEDULER_ORDER if m in palette]
    ax.legend(handles=handles, title='Scheduler', loc='lower right')
    sns.despine()
    plt.tight_layout()
    plt.savefig(output_dir / "E2_slo_by_workload_and_scheduler.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_E3_job_duration_comparison(df: pd.DataFrame, output_dir: Path):
    """
    E3 – Median job duration per workload per scheduler.
    Batch jobs should be faster under 'smart' (offloaded to cloud).
    """
    print("  - Generating: E3. Job Duration by Scheduler")
    job_df = df[df['kind'] == 'Job'].copy()
    if job_df.empty: return
    job_df['duration_s'] = job_df['measured_ms'] / 1000.0

    palette = _scheduler_palette(df)
    fig, ax = plt.subplots(figsize=(14, 6))
    sns.boxplot(data=job_df, x='workload', y='duration_s', hue='config_name',
                palette=palette, showfliers=False, ax=ax,
                order=sorted(job_df['workload'].unique()),
                hue_order=[s for s in SCHEDULER_ORDER if s in job_df['config_name'].values])
    ax.set_xlabel("Workload", fontsize=12, fontweight='semibold')
    ax.set_ylabel("Completion Time (s)", fontsize=12, fontweight='semibold')
    ax.set_title("Job Completion Time by Scheduler (all WAN/load conditions)",
                 fontsize=14, fontweight='bold')
    ax.tick_params(axis='x', rotation=30)
    handles = [mpatches.Patch(fc=palette.get(m, '#888'), label=SCHEDULER_LABELS.get(m, m))
               for m in SCHEDULER_ORDER if m in palette]
    ax.legend(handles=handles, title='Scheduler')
    sns.despine()
    plt.tight_layout()
    plt.savefig(output_dir / "E3_job_duration_by_scheduler.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_E4_slo_vs_wan_profile(df: pd.DataFrame, output_dir: Path):
    """
    E4 – SLO pass rate per WAN profile per scheduler.
    Smart should degrade most gracefully as WAN worsens.
    """
    print("  - Generating: E4. SLO Pass Rate vs WAN Profile")
    if df.empty: return

    rate = (df.groupby(['config_name', 'wan_profile'], observed=True)['pass']
            .apply(lambda x: (x == True).sum() / len(x) * 100)
            .reset_index(name='pass_pct'))
    if rate.empty: return

    palette = _scheduler_palette(df)
    fig, ax = plt.subplots(figsize=(10, 6))
    for mode in [s for s in SCHEDULER_ORDER if s in rate['config_name'].values]:
        sub = rate[rate['config_name'] == mode]
        sub = sub.set_index('wan_profile').reindex(WAN_ORDER).reset_index()
        ax.plot(sub['wan_profile'], sub['pass_pct'],
                marker='o', linewidth=2.2, markersize=8,
                color=palette.get(mode, '#888'),
                label=SCHEDULER_LABELS.get(mode, mode))
    ax.axhline(90, ls='--', color='gray', lw=1.2, alpha=0.7, label='90% target')
    ax.set_ylim(0, 108)
    ax.set_xlabel("WAN Profile (increasing degradation →)", fontsize=12, fontweight='semibold')
    ax.set_ylabel("SLO Pass Rate (%)", fontsize=12, fontweight='semibold')
    ax.set_title("SLO Compliance Degradation Under WAN Stress",
                 fontsize=14, fontweight='bold')
    ax.legend(title='Scheduler', loc='lower left')
    sns.despine()
    plt.tight_layout()
    plt.savefig(output_dir / "E4_slo_vs_wan_profile.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_E5_placement_distribution(df_placement: pd.DataFrame, output_dir: Path):
    """
    E5 – Pod placement distribution (edge / fog / cloud) per scheduler.
    Smart should put batch workloads on cloud and latency workloads on edge.
    """
    print("  - Generating: E5. Placement Distribution by Scheduler")
    if df_placement.empty: return

    counts = (df_placement.groupby(['config_name', 'node_type'], observed=True)
              .size().reset_index(name='count'))
    totals = counts.groupby('config_name', observed=True)['count'].transform('sum')
    counts['pct'] = counts['count'] / totals * 100
    if counts.empty: return

    palette = {k: CLUSTER_PALETTE.get(k, '#AAAAAA') for k in counts['node_type'].unique()}
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=counts, x='config_name', y='pct', hue='node_type',
                palette=palette, ax=ax,
                order=[s for s in SCHEDULER_ORDER if s in counts['config_name'].values])
    ax.set_xlabel("Scheduler", fontsize=12, fontweight='semibold')
    ax.set_ylabel("Pod Distribution (%)", fontsize=12, fontweight='semibold')
    ax.set_title("Where Pods Are Placed: Cluster Distribution by Scheduler",
                 fontsize=14, fontweight='bold')
    ax.tick_params(axis='x', rotation=15)
    ax.set_ylim(0, 105)
    handles = [mpatches.Patch(fc=CLUSTER_PALETTE.get(k, '#AAA'), label=k)
               for k in sorted(counts['node_type'].unique())]
    ax.legend(handles=handles, title='Cluster')
    sns.despine()
    plt.tight_layout()
    plt.savefig(output_dir / "E5_placement_distribution_by_scheduler.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_E6_placement_by_workload_heatmap(df_placement: pd.DataFrame, output_dir: Path):
    """
    E6 – Heatmap: percentage of pods on cloud for each (scheduler, workload) cell.
    Shows that 'smart' moves batch workloads to cloud while keeping latency workloads local.
    """
    print("  - Generating: E6. Placement Heatmap: Cloud % by Workload × Scheduler")
    if df_placement.empty: return

    dedup_cols = ['config_name', 'wan_profile', 'local_load', 'run', 'pod_name']
    df_clean = df_placement.drop_duplicates(
        subset=[c for c in dedup_cols if c in df_placement.columns]
    ).copy()

    counts = (df_clean.groupby(['config_name', 'workload', 'node_type'], observed=True)
              .size().reset_index(name='count'))
    total = counts.groupby(['config_name', 'workload'], observed=True)['count'].transform('sum')
    counts['pct'] = counts['count'] / total * 100
    cloud_pct = (counts[counts['node_type'] == 'cloud']
                 .pivot_table(index='workload', columns='config_name', values='pct', fill_value=0,
                              observed=True))
    if cloud_pct.empty: return

    # Reorder columns
    col_order = [s for s in SCHEDULER_ORDER if s in cloud_pct.columns]
    cloud_pct = cloud_pct[col_order]

    fig, ax = plt.subplots(figsize=(10, 7))
    sns.heatmap(cloud_pct, annot=True, fmt=".0f", cmap="YlGnBu",
                vmin=0, vmax=100, linewidths=0.8, linecolor='white',
                cbar_kws={'label': 'Cloud Placement (%)'}, ax=ax)
    ax.set_title("Cloud Placement Rate: Workload × Scheduler\n"
                 "(higher = more offloading to cloud cluster)",
                 fontsize=13, fontweight='bold', pad=15)
    ax.set_xlabel("Scheduler", fontsize=12, fontweight='semibold')
    ax.set_ylabel("Workload", fontsize=12, fontweight='semibold')
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(output_dir / "E6_placement_heatmap_cloud_pct.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_E7_slo_improvement_over_runs_comparison(df: pd.DataFrame, output_dir: Path):
    """
    E7 – SLO pass rate trajectory over successive runs per scheduler.
    'Smart' should improve fastest (online learning via Lyapunov histograms).
    """
    print("  - Generating: E7. SLO Learning Curve Comparison")
    if df.empty: return

    pass_by_run = (df.groupby(['config_name', 'run'], observed=True)['pass']
                   .apply(lambda x: (x == True).sum() / len(x) * 100)
                   .reset_index(name='pass_rate'))
    if pass_by_run.empty: return

    palette = _scheduler_palette(df)
    fig, ax = plt.subplots(figsize=(11, 6))
    for mode in [s for s in SCHEDULER_ORDER if s in pass_by_run['config_name'].values]:
        sub = pass_by_run[pass_by_run['config_name'] == mode]
        ax.plot(sub['run'], sub['pass_rate'],
                marker='o', linewidth=2.2, markersize=7,
                color=palette.get(mode, '#888'),
                label=SCHEDULER_LABELS.get(mode, mode))
    ax.axhline(90, ls='--', color='gray', lw=1.2, alpha=0.7)
    ax.set_ylim(0, 108)
    ax.set_xlabel("Run Number", fontsize=12, fontweight='semibold')
    ax.set_ylabel("SLO Pass Rate (%)", fontsize=12, fontweight='semibold')
    ax.set_title("Online Learning Effect: SLO Pass Rate Trajectory by Scheduler",
                 fontsize=14, fontweight='bold')
    ax.legend(title='Scheduler', loc='lower right')
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    sns.despine()
    plt.tight_layout()
    plt.savefig(output_dir / "E7_slo_learning_curve_comparison.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_E8_slo_failure_cost_comparison(df: pd.DataFrame, output_dir: Path):
    """
    E8 – Deadline overshoot (%) for failed SLOs, per scheduler.
    When the naive schedulers fail, how badly do they fail?
    """
    print("  - Generating: E8. SLO Failure Cost Comparison")
    fail_df = df[(df['pass'] == False) & df['measured_ms'].notna() & df['target_ms'].notna()].copy()
    if fail_df.empty: return
    fail_df['overshoot_pct'] = (
        (fail_df['measured_ms'] - fail_df['target_ms']) / fail_df['target_ms'] * 100
    )

    palette = _scheduler_palette(df)
    fig, ax = plt.subplots(figsize=(11, 6))
    sns.boxplot(data=fail_df, x='config_name', y='overshoot_pct', palette=palette,
                showfliers=False, ax=ax,
                order=[s for s in SCHEDULER_ORDER if s in fail_df['config_name'].values])
    sns.stripplot(data=fail_df, x='config_name', y='overshoot_pct', color='black',
                  alpha=0.3, size=3, ax=ax,
                  order=[s for s in SCHEDULER_ORDER if s in fail_df['config_name'].values])
    ax.set_xlabel("Scheduler", fontsize=12, fontweight='semibold')
    ax.set_ylabel("Deadline Overshoot (%)", fontsize=12, fontweight='semibold')
    ax.set_title("Severity of SLO Failures: Deadline Overshoot by Scheduler",
                 fontsize=14, fontweight='bold')
    ax.axhline(0, ls='--', color='gray', lw=1, alpha=0.6)
    ax.tick_params(axis='x', rotation=15)
    sns.despine()
    plt.tight_layout()
    plt.savefig(output_dir / "E8_slo_failure_cost_comparison.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_E9_io_and_build_wan_sensitivity(df: pd.DataFrame, output_dir: Path):
    """
    E9 – Duration of the most resource-hungry workloads (io-job, build-job, ml-infer)
    per scheduler × WAN profile (used as proxy for run conditions).
    Cloud (8 CPU, 24 GB) completes these faster; smart scheduler offloads them there.
    Naive schedulers may leave them on edge (4 CPU, 8 GB), causing deadline misses.
    """
    print("  - Generating: E9. Resource-Sensitive Workload Duration Comparison")
    jobs = ['io-job', 'build-job', 'ml-infer']
    plot_df = df[df['workload'].isin(jobs) & df['measured_ms'].notna()].copy()
    if plot_df.empty: return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    palette = _scheduler_palette(df)
    g = sns.FacetGrid(
        plot_df,
        col='workload', row='wan_profile',
        col_order=[j for j in jobs if j in plot_df['workload'].values],
        row_order=[w for w in WAN_ORDER if w in plot_df['wan_profile'].values],
        height=3.5, aspect=1.4,
        sharey=False,
    )
    g.map_dataframe(
        sns.barplot,
        x='config_name', y='duration_s',
        palette=palette, errorbar='sd', capsize=0.12,
        order=[s for s in SCHEDULER_ORDER if s in plot_df['config_name'].values],
    )
    g.set_axis_labels("Scheduler", "Duration (s)", fontsize=10)
    g.set_titles(row_template="WAN: {row_name}", col_template="{col_name}", fontsize=11)
    for ax in g.axes.flat:
        ax.tick_params(axis='x', rotation=20)
        sns.despine(ax=ax)
    g.figure.suptitle("Resource-Hungry Jobs: Smart Offloads to Cloud, Others Don't",
                       fontsize=13, fontweight='bold', y=1.01)
    g.tight_layout()
    g.savefig(output_dir / "E9_wan_sensitive_workload_comparison.png", dpi=300)
    plt.close()


def plot_E10_summary_radar(df: pd.DataFrame, output_dir: Path):
    """
    E10 – Radar/spider chart comparing schedulers across key dimensions:
      SLO compliance, mean job duration (inverted), placement quality,
      WAN degradation resistance.
    """
    print("  - Generating: E10. Summary Radar Chart")
    if df.empty: return

    schedulers = [s for s in SCHEDULER_ORDER if s in df['config_name'].values]
    if not schedulers: return

    # Compute normalised scores (0–100, higher is better)
    scores: dict[str, dict] = {s: {} for s in schedulers}

    # 1. Overall SLO pass rate
    for s in schedulers:
        sub = df[df['config_name'] == s]
        scores[s]['SLO\nCompliance'] = sub['pass'].mean() * 100 if not sub.empty else 0

    # 2. Batch job speed (inverted median duration, normalised)
    batch_jobs = ['cpu-batch', 'ml-infer', 'io-job', 'build-job', 'memory-intensive']
    batch_df = df[df['workload'].isin(batch_jobs) & df['measured_ms'].notna()].copy()
    if not batch_df.empty:
        med_dur = batch_df.groupby('config_name', observed=True)['measured_ms'].median()
        max_dur = med_dur.max()
        for s in schedulers:
            scores[s]['Batch\nSpeed'] = (1 - med_dur.get(s, max_dur) / max_dur) * 100

    # 3. SLO pass rate under poor WAN (resilience)
    poor_df = df[df['wan_profile'] == 'poor'] if 'poor' in df['wan_profile'].values else pd.DataFrame()
    for s in schedulers:
        sub = poor_df[poor_df['config_name'] == s] if not poor_df.empty else pd.DataFrame()
        scores[s]['WAN\nResilience'] = sub['pass'].mean() * 100 if not sub.empty else 0

    # 4. SLO pass rate under high local load (overload resilience)
    load_df = (df[df['local_load'].isin(['medium', 'high'])]
               if any(l in df['local_load'].values for l in ['medium', 'high'])
               else pd.DataFrame())
    for s in schedulers:
        sub = load_df[load_df['config_name'] == s] if not load_df.empty else pd.DataFrame()
        scores[s]['Load\nResilience'] = sub['pass'].mean() * 100 if not sub.empty else 0

    # 5. Consistency (1 – normalised std of pass rate across runs)
    for s in schedulers:
        sub = df[df['config_name'] == s]
        if not sub.empty:
            run_rates = (sub.groupby('run', observed=True)['pass']
                         .apply(lambda x: x.mean() * 100))
            std = run_rates.std() if len(run_rates) > 1 else 0
            scores[s]['Consistency'] = max(0, 100 - std)
        else:
            scores[s]['Consistency'] = 0

    categories = list(next(iter(scores.values())).keys())
    n = len(categories)
    if n < 3: return

    angles = [i / n * 2 * np.pi for i in range(n)]
    angles += angles[:1]

    palette = _scheduler_palette(df)
    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))
    for s in schedulers:
        vals = [scores[s].get(c, 0) for c in categories]
        vals += vals[:1]
        ax.plot(angles, vals, linewidth=2.2, color=palette.get(s, '#888'),
                label=SCHEDULER_LABELS.get(s, s))
        ax.fill(angles, vals, color=palette.get(s, '#888'), alpha=0.12)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, size=11)
    ax.set_ylim(0, 100)
    ax.set_yticks([20, 40, 60, 80, 100])
    ax.set_yticklabels(['20', '40', '60', '80', '100'], size=8, color='gray')
    ax.set_title("Scheduler Comparison: Multi-Dimensional Summary",
                 fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='upper right', bbox_to_anchor=(1.35, 1.1), title='Scheduler')
    plt.tight_layout()
    plt.savefig(output_dir / "E10_summary_radar_chart.png", dpi=300, bbox_inches="tight")
    plt.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate visualisations for the multi-scheduler SLO benchmark."
    )
    parser.add_argument("results_dir", type=Path,
                        help="Path to the multi-run results directory.")
    parser.add_argument("--warmup-runs", type=int, default=2,
                        help="Initial runs per combination to discard (default: 2).")
    args = parser.parse_args()

    results_dir = args.results_dir
    output_dir  = results_dir / "_analysis_plots"
    output_dir.mkdir(exist_ok=True)

    setup_theme()

    print("=" * 60)
    print("Loading SLO summary data…")
    df_all_slo, df_stable_slo = load_all_run_data(results_dir, args.warmup_runs)

    print("Loading pod placement data…")
    df_all_placement, df_stable_placement = load_placement_data(results_dir, args.warmup_runs)

    # Save aggregated CSVs for reproducibility
    df_all_slo.to_csv(results_dir / "agg_slo_all.csv", index=False)
    df_stable_slo.to_csv(results_dir / "agg_slo_stable.csv", index=False)
    df_all_placement.to_csv(results_dir / "agg_placement_all.csv", index=False)
    df_stable_placement.to_csv(results_dir / "agg_placement_stable.csv", index=False)
    print("✅ Aggregated CSVs saved.")

    # -----------------------------------------------------------------------
    # Per-Scheduler Detailed Reports (Sections A–D)
    # -----------------------------------------------------------------------
    for config_name in df_stable_slo['config_name'].cat.categories:
        print(f"\n{'='*60}")
        print(f"Detailed report for scheduler: {config_name}")
        print('='*60)
        per_dir = output_dir / f"scheduler-{config_name}"
        per_dir.mkdir(exist_ok=True)

        slo_s  = df_stable_slo[df_stable_slo['config_name'] == config_name].copy()
        plc_s  = df_stable_placement[df_stable_placement['config_name'] == config_name].copy()
        slo_a  = df_all_slo[df_all_slo['config_name'] == config_name].copy()

        for df in [slo_s, plc_s, slo_a]:
            for col in ['local_load', 'wan_profile', 'config_name']:
                if col in df.columns and hasattr(df[col], 'cat'):
                    df[col] = df[col].cat.remove_unused_categories()

        plot_2_job_duration_bars(slo_s, per_dir, config_name)
        plot_3_slo_pass_rate_heatmap(slo_s, per_dir, config_name)
        plot_5_performance_interaction(slo_s, per_dir, config_name)
        plot_6_slo_failure_magnitude(slo_s, per_dir, config_name)
        plot_7_raw_data_variance(slo_s, per_dir, config_name)
        plot_8_placement_analysis(plc_s, per_dir, config_name)
        plot_10_learning_performance_over_runs(slo_a, per_dir, config_name)
        plot_11_learning_slo_improvement(slo_a, per_dir, config_name)

    # -----------------------------------------------------------------------
    # Section E: Cross-Scheduler Comparison (PRIMARY CONTRIBUTION)
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Section E: Cross-Scheduler Comparison")
    print('='*60)
    cmp_dir = output_dir / "_comparison"
    cmp_dir.mkdir(exist_ok=True)

    plot_E1_overall_slo_comparison(df_stable_slo, cmp_dir)
    plot_E2_scheduler_slo_by_workload(df_stable_slo, cmp_dir)
    plot_E3_job_duration_comparison(df_stable_slo, cmp_dir)
    plot_E4_slo_vs_wan_profile(df_stable_slo, cmp_dir)
    plot_E5_placement_distribution(df_stable_placement, cmp_dir)
    plot_E6_placement_by_workload_heatmap(df_stable_placement, cmp_dir)
    plot_E7_slo_improvement_over_runs_comparison(df_all_slo, cmp_dir)
    plot_E8_slo_failure_cost_comparison(df_stable_slo, cmp_dir)
    plot_E9_io_and_build_wan_sensitivity(df_stable_slo, cmp_dir)
    plot_E10_summary_radar(df_stable_slo, cmp_dir)

    print(f"\n✅ All visualisations complete.")
    print(f"Per-scheduler plots: {output_dir}/scheduler-<mode>/")
    print(f"Comparison plots:    {cmp_dir}/")


if __name__ == "__main__":
    main()
