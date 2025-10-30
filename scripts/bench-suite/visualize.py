#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# --- Global Configuration for Consistency ---

WAN_ORDER = ['clear', 'good', 'moderate', 'poor']
LOAD_ORDER = ['none', 'low', 'medium', 'high']
CLOUD_NODE_IDENTIFIER = 'mypool'

# Professional color palettes
CATEGORICAL_PALETTE = "Set2"
SEQUENTIAL_PALETTE = "YlOrRd"
DIVERGING_PALETTE = "RdYlBu_r"


# --- Theme Configuration ---

def setup_theme():
    """Configure global seaborn and matplotlib styling."""
    sns.set_theme(
        style="whitegrid",
        palette=CATEGORICAL_PALETTE,
        font_scale=1.0
    )
    plt.rcParams.update({
        'figure.facecolor': 'white',
        'axes.facecolor': '#f8f9fa',
        'axes.edgecolor': '#e0e0e0',
        'grid.color': '#e8e8e8',
        'grid.linewidth': 0.7,
        'font.family': 'sans-serif',
        'font.sans-serif': ['Arial', 'Helvetica'],
        'axes.labelsize': 11,
        'axes.titlesize': 13,
        'xtick.labelsize': 10,
        'ytick.labelsize': 10,
        'legend.fontsize': 10,
        'lines.linewidth': 1.8,
        'patch.linewidth': 0.5,
    })


# --- Data Loading and Parsing Engine ---

def load_all_run_data(base_dir: Path, warmup_runs: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load and aggregate all run data, returning both a full and a stable (post-warmup) dataframe."""
    all_rows = []
    print("Scanning for runs and parsing all raw data files...")

    for profile_dir in sorted(base_dir.glob("wan-*_load-*")):
        for run_dir in sorted(profile_dir.glob("run-*")):
            try:
                result_dir = next(d for d in run_dir.iterdir() if d.is_dir())
            except StopIteration:
                print(f"Warning: No result subdirectory found in {run_dir}. Skipping.")
                continue

            profile_dir_name = result_dir.parts[-3]
            try:
                run_number = int(result_dir.parts[-2].replace("run-", ""))
                wan_part, load_part = profile_dir_name.split("_load-")
                wan_profile = wan_part.replace("wan-", "")
                load_profile = load_part
            except (ValueError, IndexError):
                print(f"Warning: Skipping malformed directory: {profile_dir_name}/{run_dir.name}")
                continue

            # --- Parse run-level raw files once ---
            http_data = parse_http_benchmark(result_dir / "http_benchmark.txt")

            # --- Parse the detailed SLO summary for workload-level data ---
            slo_file = result_dir / "slo_summary.json"
            if not slo_file.is_file():
                continue

            with open(slo_file) as f:
                slo_data = json.load(f)
                for item in slo_data.get("items", []):
                    all_rows.append({
                        "wan_profile": wan_profile,
                        "local_load": load_profile,
                        "run": run_number,
                        **item,
                        **http_data,
                    })

    if not all_rows:
        sys.exit("Error: No valid SLO summary data found.")

    df_all = pd.DataFrame(all_rows)
    df_all['wan_profile'] = pd.Categorical(df_all['wan_profile'], categories=WAN_ORDER, ordered=True)
    df_all['local_load'] = pd.Categorical(df_all['local_load'], categories=LOAD_ORDER, ordered=True)

    print(f"\nLoaded {len(df_all)} total SLO records from all runs.")

    # Create the stable DataFrame by filtering out warm-up runs
    df_stable = df_all[df_all['run'] > warmup_runs].copy()

    # Re-index the run number for stable plots to start from 1
    if not df_stable.empty:
        df_stable['run'] = df_stable['run'] - warmup_runs

    print(f"  -> Discarding {warmup_runs} runs per profile for stable analysis.")
    print(f"  -> Using {len(df_stable)} records for stable plots.")

    return df_all, df_stable


def parse_http_benchmark(path: Path) -> dict:
    """Parse 'hey' output for latency percentiles."""
    if not path.is_file():
        return {}
    content, results = path.read_text(), {}
    for p in ["50", "90", "95", "99"]:
        if match := re.search(rf"\s+{p}%\s+in\s+([\d.]+)", content):
            results[f'http_p{p}_ms'] = float(match.group(1)) * 1000
    return results


def load_placement_data(base_dir: Path, warmup_runs: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load pod placement data, returning both full and stable dataframes."""
    all_placements = []
    print("\nScanning for pod placement data...")

    for profile_dir in sorted(base_dir.glob("wan-*_load-*")):
        for run_dir in sorted(profile_dir.glob("run-*")):
            try:
                result_dir = next(d for d in run_dir.iterdir() if d.is_dir())
            except StopIteration:
                continue

            # --- Extract metadata from the path ---
            profile_dir_name = result_dir.parts[-3]
            try:
                run_number = int(result_dir.parts[-2].replace("run-", ""))
                wan_part, load_part = profile_dir_name.split("_load-")
                wan_profile = wan_part.replace("wan-", "")
                load_profile = load_part
            except (ValueError, IndexError):
                continue

            # --- Parse the placement file ---
            placement_file = result_dir / "pod_node_map.csv"
            if not placement_file.is_file():
                continue

            df_placements = pd.read_csv(placement_file)
            for _, row in df_placements.iterrows():
                all_placements.append({
                    "wan_profile": wan_profile,
                    "local_load": load_profile,
                    "run": run_number,
                    "pod_name": row['pod_name'],
                    "node_name": row['node_name'],
                    "workload": get_workload_from_pod_name(row['pod_name']),
                    "node_type": ('cloud' if CLOUD_NODE_IDENTIFIER == row['node_name'] else 'edge')
                })

    if not all_placements:
        return pd.DataFrame(), pd.DataFrame()

    df_all = pd.DataFrame(all_placements)
    df_all['wan_profile'] = pd.Categorical(df_all['wan_profile'], categories=WAN_ORDER, ordered=True)
    df_all['local_load'] = pd.Categorical(df_all['local_load'], categories=LOAD_ORDER, ordered=True)

    # Create the stable DataFrame
    df_stable = df_all[df_all['run'] > warmup_runs].copy()
    if not df_stable.empty:
        df_stable['run'] = df_stable['run'] - warmup_runs

    return df_all, df_stable


def get_workload_from_pod_name(pod_name: str) -> str:
    """Extract workload name from pod name."""
    base_name = re.split(r'-(?=[a-z0-9]{5}$)', pod_name)[0]
    base_name = re.split(r'-(?=[a-z0-9]{9,10}-[a-z0-9]{5}$)', base_name)[0]
    return base_name


# --- Plotting Library ---

def plot_1_latency_comparison_bars(df: pd.DataFrame, output_dir: Path):
    """(Graph 1) Mean latency comparison."""
    print("Generating: 1. Mean Latency Comparison (Bar Chart)")
    latency_df = df[
        df['workload'].isin(['http-latency', 'stream-processor'])
    ].copy()
    if latency_df.empty:
        print("  Skipping: No latency workload data found.")
        return

    for load_profile in LOAD_ORDER:
        group = latency_df[latency_df['local_load'] == load_profile]
        if group.empty:
            continue

        g = sns.catplot(
            data=group,
            x='wan_profile',
            y='measured_ms',
            hue='workload',
            kind='bar',
            errorbar='sd',
            capsize=0.15,
            aspect=1.6,
            height=5,
            palette="Set2",
        )
        g.set_axis_labels(
            "WAN Profile",
            "Mean Latency (ms)",
            fontsize=12,
            fontweight='semibold'
        )
        g.legend.set_title("Workload")
        g.figure.suptitle(
            f'Mean Latency under "{load_profile}" Local Load',
            fontsize=14,
            fontweight='bold',
            y=0.98
        )
        g.tight_layout()
        sns.despine(ax=g.ax)

        path = output_dir / f"A1_mean_latency_bars_load_{load_profile}.png"
        g.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
        plt.close()
        print(f"  Saved: {path}")


def plot_2_job_duration_bars(df: pd.DataFrame, output_dir: Path):
    """(Graph 2) Job duration comparison."""
    print("Generating: 2. Mean Job Duration Comparison (Bar Chart)")
    job_df = df[df['kind'] == 'Job'].copy()
    if job_df.empty:
        return
    job_df['duration_s'] = job_df['measured_ms'] / 1000.0

    # Only use load profiles that have actual data
    available_loads = sorted(
        job_df['local_load'].unique(),
        key=lambda x: LOAD_ORDER.index(str(x)) if str(x) in LOAD_ORDER else 999
    )

    g = sns.catplot(
        data=job_df,
        x='workload',
        y='duration_s',
        hue='wan_profile',
        col='local_load',
        kind='bar',
        errorbar='sd',
        capsize=0.1,
        aspect=1.3,
        height=5.5,
        palette="husl",
        col_order=available_loads,
    )
    g.set_axis_labels(
        "Job Name",
        "Mean Duration (seconds)",
        fontsize=11,
        fontweight='semibold'
    )
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')

    # Update the existing legend title (don't create a new one)
    if g.figure.legends:
        g.figure.legends[0].set_title("WAN Profile")

    for ax in g.axes.flat:
        ax.tick_params(axis='x', rotation=45)
        sns.despine(ax=ax)

    g.figure.suptitle(
        "Mean Batch Job Duration by Condition",
        fontsize=14,
        fontweight='bold',
        y=0.995
    )
    g.tight_layout()

    path = output_dir / "A2_mean_job_duration_bars.png"
    g.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_3_slo_pass_rate_heatmap(df: pd.DataFrame, output_dir: Path):
    """(Graph 3) SLO pass rate heatmap."""
    print("Generating: 3. SLO Pass Rate Reliability Heatmap")
    pass_rate = (
        df.groupby(['wan_profile', 'local_load', 'workload'], observed=True)['pass']
        .value_counts(normalize=True)
        .unstack(fill_value=0)
    )
    pass_rate['pass_pct'] = pass_rate.get(True, 0) * 100

    heatmap_data = pass_rate.pivot_table(
        index='workload',
        columns=['local_load', 'wan_profile'],
        values='pass_pct',
        observed=True
    )

    heatmap_data = heatmap_data.dropna(axis=1, how='all').dropna(axis=0, how='all')

    if heatmap_data.empty:
        print("  Skipping: No SLO data to display.")
        return

    fig, ax = plt.subplots(figsize=(14, 7))
    sns.heatmap(
        heatmap_data,
        annot=True,
        fmt=".1f",
        cmap="RdYlGn",
        linewidths=1,
        linecolor='white',
        cbar_kws={'label': 'SLO Pass Rate (%)'},
        vmin=0,
        vmax=100,
        ax=ax,
        square=False,
    )
    ax.set_title(
        "System Reliability: SLO Pass Rate",
        fontsize=14,
        fontweight='bold',
        pad=20
    )
    ax.set_xlabel("Load & WAN Profile", fontsize=12, fontweight='semibold')
    ax.set_ylabel("Workload", fontsize=12, fontweight='semibold')
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)

    path = output_dir / "A3_slo_pass_rate_heatmap.png"
    plt.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_4_http_latency_full_distribution(df: pd.DataFrame, output_dir: Path):
    """(Graph 4) HTTP latency distribution."""
    print("Generating: 4. Full HTTP Latency Distribution (Violin Plot)")
    plot_df = df[df['workload'] == 'http-latency'].melt(
        id_vars=['wan_profile', 'local_load'],
        value_vars=['http_p50_ms', 'http_p95_ms', 'http_p99_ms'],
        var_name='percentile',
        value_name='latency_ms'
    )
    if plot_df.empty:
        return
    plot_df['percentile'] = (
        plot_df['percentile']
        .str.replace('http_p', '')
        .str.replace('_ms', '')
    )

    available_loads = sorted(
        plot_df['local_load'].unique(),
        key=lambda x: LOAD_ORDER.index(str(x)) if str(x) in LOAD_ORDER else 999
    )

    g = sns.catplot(
        data=plot_df,
        x='wan_profile',
        y='latency_ms',
        hue='percentile',
        col='local_load',
        kind='violin',
        split=False,
        inner='quartiles',
        height=5.5,
        aspect=1.3,
        palette='muted',
        col_order=available_loads,
    )
    g.set_axis_labels(
        "WAN Profile",
        "Latency (ms)",
        fontsize=11,
        fontweight='semibold'
    )
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')
    g.legend.set_title("Percentile")
    g.figure.suptitle(
        "HTTP Latency Distribution by Percentile",
        fontsize=14,
        fontweight='bold',
        y=0.995
    )
    for ax in g.axes.flat:
        sns.despine(ax=ax)
    g.tight_layout()

    path = output_dir / "B1_http_latency_distribution.png"
    g.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_5_performance_interaction(df: pd.DataFrame, output_dir: Path):
    """(Graph 5) WAN and load interaction."""
    print("Generating: 5. Performance Interaction Plot")
    job_to_plot = 'build-job'
    plot_df = df[df['workload'] == job_to_plot].copy()
    if plot_df.empty:
        return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    # Only include load profiles with actual data
    available_loads = sorted(
        plot_df['local_load'].unique(),
        key=lambda x: LOAD_ORDER.index(str(x)) if str(x) in LOAD_ORDER else 999
    )
    plot_df = plot_df[plot_df['local_load'].isin(available_loads)]

    # Reset categorical to only include available categories
    plot_df['local_load'] = plot_df['local_load'].cat.remove_unused_categories()

    fig, ax = plt.subplots(figsize=(11, 6.5))
    sns.pointplot(
        data=plot_df,
        x='wan_profile',
        y='duration_s',
        hue='local_load',
        errorbar='sd',
        capsize=0.15,
        palette='Set2',
        ax=ax,
        markers='o',
        markersize=8,
        linewidth=2,
        order=WAN_ORDER,
        hue_order=available_loads,
    )
    ax.set_title(
        f"Interaction: WAN & Load on '{job_to_plot}' Runtime",
        fontsize=13,
        fontweight='bold',
        pad=15
    )
    ax.set_xlabel("WAN Profile", fontsize=11, fontweight='semibold')
    ax.set_ylabel("Mean Duration (s)", fontsize=11, fontweight='semibold')
    ax.legend(title="Local Load", title_fontsize=10, fontsize=9)
    sns.despine(ax=ax)
    plt.tight_layout()

    path = output_dir / "B2_performance_interaction_plot.png"
    plt.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_6_slo_failure_magnitude(df: pd.DataFrame, output_dir: Path):
    """(Graph 6) SLO failure magnitude."""
    print("Generating: 6. SLO Failure Magnitude Analysis")
    plot_df = df[df['pass'] == False].copy()
    plot_df = plot_df[
        plot_df['measured_ms'].notna() & plot_df['target_ms'].notna()
        ]
    if plot_df.empty:
        print("  Skipping: No SLO failures found.")
        return
    plot_df['overshoot_pct'] = (
            (plot_df['measured_ms'] - plot_df['target_ms'])
            / plot_df['target_ms']
            * 100
    )

    available_loads = sorted(
        plot_df['local_load'].unique(),
        key=lambda x: LOAD_ORDER.index(str(x)) if str(x) in LOAD_ORDER else 999
    )

    g = sns.catplot(
        data=plot_df,
        x='workload',
        y='overshoot_pct',
        hue='wan_profile',
        col='local_load',
        kind='bar',
        errorbar='sd',
        capsize=0.08,
        height=5.5,
        aspect=1.3,
        palette="OrRd",
        col_order=available_loads,
    )
    g.set_axis_labels(
        "Workload",
        "Deadline Overshoot (%)",
        fontsize=11,
        fontweight='semibold'
    )
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')

    g.legend.set_title("WAN Profile")

    for ax in g.axes.flat:
        ax.tick_params(axis='x', rotation=30)
        sns.despine(ax=ax)

    g.figure.suptitle(
        "Magnitude of SLO Failures (Deadline Overshoot %)",
        fontsize=14,
        fontweight='bold',
        y=0.995
    )
    g.tight_layout()

    path = output_dir / "B3_slo_failure_magnitude.png"
    g.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_7_raw_data_variance(df: pd.DataFrame, output_dir: Path):
    """(Graph 7) Raw data variance."""
    print("Generating: 7. Raw Data Point Variance Plot")
    job_to_plot = 'cpu-batch'
    plot_df = df[df['workload'] == job_to_plot].copy()
    if plot_df.empty:
        return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    # Only include load profiles with actual data
    available_loads = sorted(
        plot_df['local_load'].unique(),
        key=lambda x: LOAD_ORDER.index(str(x)) if str(x) in LOAD_ORDER else 999
    )
    plot_df = plot_df[plot_df['local_load'].isin(available_loads)]

    # Reset categorical to only include available categories
    plot_df['local_load'] = plot_df['local_load'].cat.remove_unused_categories()

    fig, ax = plt.subplots(figsize=(13, 7))
    sns.boxplot(
        data=plot_df,
        x='wan_profile',
        y='duration_s',
        hue='local_load',
        showfliers=False,
        palette='Set2',
        ax=ax,
        order=WAN_ORDER,
        hue_order=available_loads,
    )
    sns.stripplot(
        data=plot_df,
        x='wan_profile',
        y='duration_s',
        hue='local_load',
        dodge=True,
        ax=ax,
        alpha=0.4,
        size=5,
        palette='Set2',
        legend=False,
        order=WAN_ORDER,
        hue_order=available_loads,
    )
    ax.set_title(
        f"Runtime Variance of '{job_to_plot}' Across All Runs",
        fontsize=13,
        fontweight='bold',
        pad=15
    )
    ax.set_xlabel("WAN Profile", fontsize=11, fontweight='semibold')
    ax.set_ylabel("Duration (s)", fontsize=11, fontweight='semibold')
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles[:len(available_loads)],
        labels[:len(available_loads)],
        title='Local Load',
        loc='upper left'
    )
    sns.despine(ax=ax)
    plt.tight_layout()

    path = output_dir / "B4_raw_data_variance.png"
    plt.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_8_placement_analysis(df: pd.DataFrame, output_dir: Path):
    """(Graph 8) Workload placement analysis."""
    print("Generating: 8. Workload Placement Analysis")
    if df.empty:
        print("  Skipping: No pod placement data found.")
        return

    placement_counts = df.groupby(
        ['wan_profile', 'local_load', 'workload', 'node_type'],
        observed=True
    ).size().reset_index(name='count')

    mean_counts = placement_counts.groupby(
        ['wan_profile', 'local_load', 'workload', 'node_type'],
        observed=True
    )['count'].mean().reset_index()

    if mean_counts.empty:
        print("  Skipping: No placement data.")
        return

    actual_combos = (
        mean_counts.groupby(['wan_profile', 'local_load'], observed=True)
        .size()
        .reset_index(name='_count')
    )

    available_wans = sorted(
        actual_combos['wan_profile'].unique(),
        key=lambda x: WAN_ORDER.index(str(x)) if str(x) in WAN_ORDER else 999
    )
    available_loads = sorted(
        actual_combos['local_load'].unique(),
        key=lambda x: LOAD_ORDER.index(str(x)) if str(x) in LOAD_ORDER else 999
    )

    g = sns.catplot(
        data=mean_counts,
        x='workload',
        y='count',
        hue='node_type',
        col='local_load',
        row='wan_profile',
        kind='bar',
        height=4.5,
        aspect=1.6,
        palette={'cloud': '#4A90E2', 'edge': '#F5A623'},
        legend=False,
        row_order=available_wans,
        col_order=available_loads,
    )
    g.set_axis_labels(
        "Workload",
        "Mean Pod Count",
        fontsize=11,
        fontweight='semibold'
    )
    g.set_titles(
        row_template="WAN: {row_name}",
        col_template="Load: {col_name}",
        fontsize=11,
        fontweight='semibold'
    )

    # Add legend with proper placement
    handles = [
        plt.Rectangle((0, 0), 1, 1, fc='#4A90E2'),
        plt.Rectangle((0, 0), 1, 1, fc='#F5A623')
    ]
    g.figure.legend(
        handles,
        ['cloud', 'edge'],
        title='Node Type',
        loc='upper center',
        bbox_to_anchor=(0.5, -0.02),
        ncol=2,
        frameon=True
    )

    for ax in g.axes.flat:
        ax.tick_params(axis='x', rotation=45)
        sns.despine(ax=ax)

    g.figure.suptitle(
        "Workload Placement by Node Type Across All Conditions",
        fontsize=14,
        fontweight='bold',
        y=0.995
    )
    g.tight_layout()

    path = output_dir / "C1_workload_placement_matrix.png"
    g.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_9_learning_placement_over_runs(df: pd.DataFrame, output_dir: Path):
    """(Graph 9) Stacked area of edge vs cloud share over runs, per workload."""
    print("Generating 9. Edge vs Cloud Share Over Runs (Stacked Area)")
    if df.empty or 'node_type' not in df.columns:
        print("  Skipping: No placement data available.")
        return

    # Clean duplicates but keep multiple pods
    dedup_cols = [
        'wan_profile', 'local_load', 'run', 'workload', 'pod_name',
        'node_name', 'node_type'
    ]
    present_cols = [c for c in dedup_cols if c in df.columns]
    df_clean = df.drop_duplicates(subset=present_cols).copy()

    # Ensure ordered categories, drop unused
    for col, order in [('wan_profile', WAN_ORDER), ('local_load', LOAD_ORDER)]:
        if col in df_clean.columns:
            df_clean[col] = pd.Categorical(df_clean[col], categories=order, ordered=True)
            df_clean[col] = df_clean[col].cat.remove_unused_categories()

    group_cols = ['wan_profile', 'local_load', 'run', 'workload']

    counts = (
        df_clean.groupby(group_cols + ['node_type'], observed=True)
        .size()
        .reset_index(name='count')
    )

    # Pivot to edge/cloud counts
    pivot = (
        counts.pivot_table(
            index=group_cols, columns='node_type', values='count',
            aggfunc='sum', fill_value=0, observed=True
        )
        .reset_index()
    )
    for col in ['edge', 'cloud']:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot['total'] = pivot['edge'] + pivot['cloud']
    pivot = pivot[pivot['total'] > 0].copy()
    pivot['edge_pct'] = pivot['edge'] / pivot['total'] * 100
    pivot['cloud_pct'] = pivot['cloud'] / pivot['total'] * 100

    if pivot.empty:
        print("  Skipping: No placement counts found to plot.")
        return

    # Build facets by WAN x Load, one small stacked area per workload
    # We’ll plot multiple workloads in each facet as separate stacked areas
    # arranged by workload as small multiples (rows).
    # For simplicity and readability, we do one workload per small subplot
    # using FacetGrid with col=local_load, row=wan_profile, and loop workloads inside.
    import matplotlib.pyplot as plt
    import seaborn as sns
    sns.set_style("whitegrid")

    # Determine unique facets and workloads
    wans = list(pivot['wan_profile'].cat.categories) if hasattr(pivot['wan_profile'], 'cat') else sorted(
        pivot['wan_profile'].unique())
    loads = list(pivot['local_load'].cat.categories) if hasattr(pivot['local_load'], 'cat') else sorted(
        pivot['local_load'].unique())
    # Only keep facets that exist
    wans = [w for w in wans if w in pivot['wan_profile'].unique()]
    loads = [l for l in loads if l in pivot['local_load'].unique()]
    workloads = sorted(pivot['workload'].unique())

    # Limit to top N workloads by total pods if too many
    MAX_WORKLOADS = 8
    if len(workloads) > MAX_WORKLOADS:
        totals = (pivot.groupby('workload')['total'].sum().sort_values(ascending=False))
        workloads = list(totals.head(MAX_WORKLOADS).index)

    # Prepare figure grid: rows=wan*workload, cols=load
    # To keep it compact, we’ll facet by WAN x Load and inside each facet draw multiple workloads as separate small stacked areas vertically.
    # Simpler and more recognizable: facet by WAN (rows) and Load (cols) and use hue=node_type with 100% stacked bars per run per workload averaged.
    # Final choice for readability: 100% stacked bars per run aggregated across all workloads (Option B) AND a separate figure per workload (Option A lite).

    # Option A lite: one figure per workload to keep it very readable
    colors = {'edge': '#4A90E2', 'cloud': '#F5A623'}
    for wl in workloads:
        sub = pivot[pivot['workload'] == wl].copy()
        if sub.empty:
            continue

        g = sns.FacetGrid(
            sub,
            row='wan_profile',
            col='local_load',
            margin_titles=True,
            sharey=True,
            sharex=True,
            despine=True,
            height=3.0,
            aspect=1.4
        )

        def _area(data, color=None, label=None, **kwargs):
            ax = plt.gca()
            data = data.sort_values('run')
            x = data['run'].values
            y_edge = data['edge_pct'].values
            y_cloud = data['cloud_pct'].values
            ax.stackplot(
                x,
                [y_edge, y_cloud],
                colors=[colors['edge'], colors['cloud']],
                labels=['edge', 'cloud'],
                alpha=0.9
            )
            ax.set_ylim(0, 100)
            ax.set_xlim(min(x), max(x))
            ax.set_title("")
            ax.grid(True, axis='y', alpha=0.3)

        g.map_dataframe(_area)

        # Titles and labels
        g.set_axis_labels("Run", "Share (%)")
        g.set_titles(row_template="WAN: {row_name}", col_template="Load: {col_name}")
        g.figure.subplots_adjust(top=0.88)
        g.figure.suptitle(f"Placement Share Over Runs (Stacked) — {wl}", fontsize=12, fontweight='bold')

        # Single legend at top
        handles = [
            plt.Rectangle((0, 0), 1, 1, fc=colors['edge']),
            plt.Rectangle((0, 0), 1, 1, fc=colors['cloud'])
        ]
        g.figure.legend(handles, ['edge', 'cloud'], loc='upper center', ncol=2)

        path = output_dir / f"D1_placement_share_stacked_{wl}.png"
        g.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
        plt.close(g.figure)
        print(f"  Saved: {path}")

    print("  Note: Generated one figure per workload to maximize readability.")


def plot_9b_overview_placement_trends(df: pd.DataFrame, output_dir: Path):
    """(Graph 9B) Aggregated edge placement % over runs across all workloads."""
    print("Generating: 9B. System-Wide Placement Trends (Overview)")
    if df.empty or 'node_type' not in df.columns:
        print("  Skipping: No placement data available.")
        return

    # Deduplicate
    dedup_cols = ['wan_profile', 'local_load', 'run', 'workload', 'pod_name', 'node_name', 'node_type']
    present_cols = [c for c in dedup_cols if c in df.columns]
    df_clean = df.drop_duplicates(subset=present_cols).copy()

    # Ensure ordered categories
    for col, order in [('wan_profile', WAN_ORDER), ('local_load', LOAD_ORDER)]:
        if col in df_clean.columns:
            df_clean[col] = pd.Categorical(df_clean[col], categories=order, ordered=True)
            df_clean[col] = df_clean[col].cat.remove_unused_categories()

    # Aggregate across all workloads: count edge vs cloud per (wan, load, run)
    group_cols = ['wan_profile', 'local_load', 'run']
    counts = (
        df_clean.groupby(group_cols + ['node_type'], observed=True)
        .size()
        .reset_index(name='count')
    )

    pivot = (
        counts.pivot_table(
            index=group_cols, columns='node_type', values='count',
            aggfunc='sum', fill_value=0, observed=True
        )
        .reset_index()
    )
    for col in ['edge', 'cloud']:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot['total'] = pivot['edge'] + pivot['cloud']
    pivot = pivot[pivot['total'] > 0].copy()
    pivot['edge_pct'] = pivot['edge'] / pivot['total'] * 100

    if pivot.empty:
        print("  Skipping: No placement counts found.")
        return

    # Plot as line chart
    g = sns.relplot(
        data=pivot,
        x='run',
        y='edge_pct',
        hue='wan_profile',
        col='local_load',
        kind='line',
        marker='o',
        markersize=6,
        linewidth=2.5,
        height=4.5,
        aspect=1.4,
        palette='Set2',
        legend='full',
        col_order=[c for c in LOAD_ORDER if c in pivot['local_load'].cat.categories],
    )

    g.set_axis_labels("Run Number", "Edge Placement (%)", fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')

    for ax in g.axes.flat:
        ax.set_ylim(-5, 105)
        ax.axhline(50, ls='--', color='gray', alpha=0.6, lw=1)
        ax.fill_between(ax.get_xlim(), 0, 100, alpha=0.05, color='blue')
        sns.despine(ax=ax)

    g.figure.suptitle(
        "System-Wide Edge Placement Trend (All Workloads Aggregated)",
        fontsize=14,
        fontweight='bold',
        y=0.995
    )
    g.tight_layout()

    path = output_dir / "D1_overview_placement_trend.png"
    g.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_10_learning_performance_over_runs(df: pd.DataFrame, output_dir: Path):
    """(Graph 10) Show how performance of a key job changes across runs."""
    print("Generating: 10. Learning Curve: Job Performance Over Runs")

    workload_to_plot = 'cpu-batch'
    plot_df = df[df['workload'] == workload_to_plot].copy()

    if plot_df.empty:
        print(f"  Skipping: No data found for workload '{workload_to_plot}'.")
        return

    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    g = sns.relplot(
        data=plot_df,
        x='run',
        y='duration_s',
        col='local_load',
        row='wan_profile',
        kind='line',
        marker='o',
        errorbar='sd',
        height=4,
        aspect=1.5,
        palette='viridis',
        row_order=WAN_ORDER,
        col_order=LOAD_ORDER,
    )

    g.set_axis_labels("Run Number", f"{workload_to_plot} Duration (s)", fontweight='semibold')
    g.set_titles(row_template="WAN: {row_name}", col_template="Load: {col_name}", fontweight='semibold')
    g.figure.suptitle(
        f"Scheduler Learning: '{workload_to_plot}' Performance vs. Run Number",
        fontsize=14,
        fontweight='bold',
        y=0.995
    )

    # Add SLO deadline as a horizontal line for context
    slo_target_ms = plot_df['target_ms'].max()
    if pd.notna(slo_target_ms):
        for ax in g.axes.flat:
            ax.axhline(slo_target_ms / 1000.0, ls='--', color='red', alpha=0.8, lw=1.5,
                       label=f'SLO Deadline ({slo_target_ms / 1000.0:.0f}s)')

    g.tight_layout()
    path = output_dir / "D2_learning_curve_performance.png"
    g.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_11_slo_improvement_over_runs(df: pd.DataFrame, output_dir: Path):
    """(Graph 11) SLO pass rate evolution showing learning effect."""
    print("Generating: 11. SLO Pass Rate Evolution Over Runs")
    if df.empty:
        return

    # Calculate pass rate per run per condition
    pass_by_run = (
        df.groupby(['wan_profile', 'local_load', 'run'], observed=True)['pass']
        .apply(lambda x: (x == True).sum() / len(x) * 100)
        .reset_index(name='pass_rate')
    )

    g = sns.relplot(
        data=pass_by_run,
        x='run',
        y='pass_rate',
        hue='wan_profile',
        col='local_load',
        kind='line',
        marker='o',
        markersize=6,
        linewidth=2,
        height=4.5,
        aspect=1.4,
        palette='RdYlGn',
        col_order=[c for c in LOAD_ORDER if c in pass_by_run['local_load'].unique()],
    )

    g.set_axis_labels("Run Number", "SLO Pass Rate (%)", fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')

    for ax in g.axes.flat:
        ax.set_ylim(-5, 105)
        ax.axhline(90, ls='--', color='green', alpha=0.6, lw=1, label='90% target')
        sns.despine(ax=ax)

    g.figure.suptitle(
        "System Reliability Improvement Over Runs",
        fontsize=14,
        fontweight='bold',
        y=0.995
    )
    g.tight_layout()

    path = output_dir / "D3_slo_improvement_over_runs.png"
    g.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_12_placement_performance_correlation(df_slo: pd.DataFrame, df_placement: pd.DataFrame, output_dir: Path):
    """(Graph 12) Correlation between edge placement and SLO success."""
    print("Generating: 12. Placement-Performance Correlation")
    if df_slo.empty or df_placement.empty:
        return

    # Merge SLO data with placement data
    # Compute edge% per (wan, load, run, workload)
    dedup_cols = ['wan_profile', 'local_load', 'run', 'workload', 'pod_name', 'node_name', 'node_type']
    present_cols = [c for c in dedup_cols if c in df_placement.columns]
    df_p = df_placement.drop_duplicates(subset=present_cols).copy()

    placement_summary = (
        df_p.groupby(['wan_profile', 'local_load', 'run', 'workload', 'node_type'], observed=True)
        .size()
        .reset_index(name='count')
    )

    pivot = placement_summary.pivot_table(
        index=['wan_profile', 'local_load', 'run', 'workload'],
        columns='node_type',
        values='count',
        fill_value=0,
        observed=True
    ).reset_index()

    for col in ['edge', 'cloud']:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot['total'] = pivot['edge'] + pivot['cloud']
    pivot = pivot[pivot['total'] > 0].copy()
    pivot['edge_pct'] = pivot['edge'] / pivot['total'] * 100

    # Merge with SLO pass data
    slo_summary = (
        df_slo.groupby(['wan_profile', 'local_load', 'run', 'workload'], observed=True)['pass']
        .apply(lambda x: (x == True).sum() / len(x) * 100)
        .reset_index(name='slo_pass_rate')
    )

    merged = pd.merge(
        pivot[['wan_profile', 'local_load', 'run', 'workload', 'edge_pct']],
        slo_summary,
        on=['wan_profile', 'local_load', 'run', 'workload'],
        how='inner'
    )

    if merged.empty:
        print("  Skipping: No merged data.")
        return

    # Plot scatter with regression
    g = sns.lmplot(
        data=merged,
        x='edge_pct',
        y='slo_pass_rate',
        hue='wan_profile',
        col='local_load',
        height=5,
        aspect=1.3,
        scatter_kws={'alpha': 0.6, 's': 50},
        line_kws={'linewidth': 2},
        palette='Set2',
        col_order=[c for c in LOAD_ORDER if c in merged['local_load'].unique()],
    )

    g.set_axis_labels("Edge Placement (%)", "SLO Pass Rate (%)", fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')

    for ax in g.axes.flat:
        ax.axhline(90, ls='--', color='green', alpha=0.4, lw=1)
        ax.axvline(50, ls='--', color='gray', alpha=0.4, lw=1)
        sns.despine(ax=ax)

    g.figure.suptitle(
        "Does Edge Placement Improve SLO Achievement?",
        fontsize=14,
        fontweight='bold',
        y=1.02
    )

    path = output_dir / "E1_placement_performance_correlation.png"
    g.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_13_workload_preference_heatmap(df: pd.DataFrame, output_dir: Path):
    """(Graph 13) Workload preference for edge vs cloud by condition."""
    print("Generating: 13. Workload Preference Heatmap")
    if df.empty:
        return

    # Deduplicate
    dedup_cols = ['wan_profile', 'local_load', 'run', 'workload', 'pod_name', 'node_name', 'node_type']
    present_cols = [c for c in dedup_cols if c in df.columns]
    df_clean = df.drop_duplicates(subset=present_cols).copy()

    # Compute average edge% per workload per (wan, load) across all runs
    counts = (
        df_clean.groupby(['wan_profile', 'local_load', 'workload', 'node_type'], observed=True)
        .size()
        .reset_index(name='count')
    )

    pivot = counts.pivot_table(
        index=['wan_profile', 'local_load', 'workload'],
        columns='node_type',
        values='count',
        fill_value=0,
        observed=True
    ).reset_index()

    for col in ['edge', 'cloud']:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot['total'] = pivot['edge'] + pivot['cloud']
    pivot = pivot[pivot['total'] > 0].copy()
    pivot['edge_pct'] = pivot['edge'] / pivot['total'] * 100

    # Create composite condition label
    pivot['condition'] = pivot['wan_profile'].astype(str) + ' / ' + pivot['local_load'].astype(str)

    heatmap_data = pivot.pivot_table(
        index='workload',
        columns='condition',
        values='edge_pct',
        observed=True
    )

    if heatmap_data.empty:
        print("  Skipping: No data.")
        return

    fig, ax = plt.subplots(figsize=(16, 8))
    sns.heatmap(
        heatmap_data,
        annot=True,
        fmt=".0f",
        cmap="RdYlBu_r",
        center=50,
        linewidths=1,
        linecolor='white',
        cbar_kws={'label': 'Edge Placement (%)'},
        vmin=0,
        vmax=100,
        ax=ax,
    )
    ax.set_title(
        "Workload Preference: Edge Placement % by Condition",
        fontsize=14,
        fontweight='bold',
        pad=20
    )
    ax.set_xlabel("WAN / Load Profile", fontsize=12, fontweight='semibold')
    ax.set_ylabel("Workload", fontsize=12, fontweight='semibold')
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)

    path = output_dir / "E2_workload_preference_heatmap.png"
    plt.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


def plot_14_placement_stability(df: pd.DataFrame, output_dir: Path):
    """(Graph 14) Placement decision stability over runs."""
    print("Generating: 14. Placement Decision Stability")
    if df.empty:
        return

    # For each workload, count how many times it switches node_type between consecutive runs
    dedup_cols = ['wan_profile', 'local_load', 'run', 'workload', 'pod_name', 'node_name', 'node_type']
    present_cols = [c for c in dedup_cols if c in df.columns]
    df_clean = df.drop_duplicates(subset=present_cols).copy()

    # Determine majority node_type per workload per run
    majority_placement = (
        df_clean.groupby(['wan_profile', 'local_load', 'run', 'workload', 'node_type'], observed=True)
        .size()
        .reset_index(name='count')
    )

    idx = majority_placement.groupby(['wan_profile', 'local_load', 'run', 'workload'], observed=True)['count'].idxmax()
    majority = majority_placement.loc[idx, ['wan_profile', 'local_load', 'run', 'workload', 'node_type']].copy()
    majority = majority.sort_values(['wan_profile', 'local_load', 'workload', 'run'])

    # Calculate switches
    majority['prev_node_type'] = majority.groupby(['wan_profile', 'local_load', 'workload'], observed=True)[
        'node_type'].shift(1)
    majority['switched'] = (majority['node_type'] != majority['prev_node_type']) & majority['prev_node_type'].notna()

    # Count switches per condition
    switches = (
        majority.groupby(['wan_profile', 'local_load'], observed=True)['switched']
        .sum()
        .reset_index(name='total_switches')
    )

    # Plot as heatmap
    heatmap_data = switches.pivot_table(
        index='wan_profile',
        columns='local_load',
        values='total_switches',
        observed=True
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(
        heatmap_data,
        annot=True,
        fmt=".0f",
        cmap="YlOrRd",
        linewidths=1,
        linecolor='white',
        cbar_kws={'label': 'Total Placement Switches'},
        ax=ax,
    )
    ax.set_title(
        "Placement Decision Stability (Lower = More Stable)",
        fontsize=14,
        fontweight='bold',
        pad=20
    )
    ax.set_xlabel("Load Profile", fontsize=12, fontweight='semibold')
    ax.set_ylabel("WAN Profile", fontsize=12, fontweight='semibold')
    plt.xticks(rotation=0)
    plt.yticks(rotation=0)

    path = output_dir / "E3_placement_stability.png"
    plt.savefig(path, dpi=300, bbox_inches="tight", facecolor='white')
    plt.close()
    print(f"  Saved: {path}")


# --- Main Execution Logic ---

def main():
    parser = argparse.ArgumentParser(
        description="Generate professional visualizations for SLO benchmarks."
    )
    parser.add_argument(
        "results_dir",
        type=Path,
        help="Path to the multi-run results directory."
    )
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=2,
        help="Number of initial runs to discard from each profile for stable analysis."
    )
    args = parser.parse_args()

    results_dir = args.results_dir
    output_dir = results_dir / "_analysis_plots"
    output_dir.mkdir(exist_ok=True)

    setup_theme()

    # Load data into two sets: one with all runs, one with only stable runs
    df_all_slo, df_stable_slo = load_all_run_data(results_dir, args.warmup_runs)
    df_all_placement, df_stable_placement = load_placement_data(results_dir, args.warmup_runs)

    # Save aggregated data for both sets for transparency and future analysis
    df_all_slo.to_csv(results_dir / "aggregated_slo_data_ALL.csv", index=False)
    df_stable_slo.to_csv(results_dir / "aggregated_slo_data_STABLE.csv", index=False)
    df_all_placement.to_csv(results_dir / "aggregated_placement_data_ALL.csv", index=False)
    df_stable_placement.to_csv(results_dir / "aggregated_placement_data_STABLE.csv", index=False)
    print(f"\n✅ Aggregated data for ALL and STABLE runs saved.")

    # --- Generate plots for STABLE data ---
    print("\n--- Group A/B/C: Stable-State Performance Analysis ---")
    # Note: Pass the 'df_stable_...' dataframes to the original plotting functions
    plot_1_latency_comparison_bars(df_stable_slo, output_dir)
    plot_2_job_duration_bars(df_stable_slo, output_dir)
    plot_3_slo_pass_rate_heatmap(df_stable_slo, output_dir)
    plot_4_http_latency_full_distribution(df_stable_slo, output_dir)
    plot_5_performance_interaction(df_stable_slo, output_dir)
    plot_6_slo_failure_magnitude(df_stable_slo, output_dir)
    plot_7_raw_data_variance(df_stable_slo, output_dir)
    plot_8_placement_analysis(df_stable_placement, output_dir)

    # --- Group D: Learning Curves ---
    print("\n--- Group D: Scheduler Learning Curve Analysis ---")
    plot_9_learning_placement_over_runs(df_all_placement, output_dir)
    plot_9b_overview_placement_trends(df_all_placement, output_dir)
    plot_10_learning_performance_over_runs(df_all_slo, output_dir)
    plot_11_slo_improvement_over_runs(df_all_slo, output_dir)

    # --- Group E: Advanced Analytics ---
    print("\n--- Group E: Placement-Performance Analysis ---")
    plot_12_placement_performance_correlation(df_all_slo, df_all_placement, output_dir)
    plot_13_workload_preference_heatmap(df_stable_placement, output_dir)
    plot_14_placement_stability(df_all_placement, output_dir)

    print("\n✅ Visualization complete!")
    print(f"All graphs saved in: {output_dir}")


if __name__ == "__main__":
    main()
