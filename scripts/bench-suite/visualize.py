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
        'lines.linewidth': 1.5,
        'patch.linewidth': 0.5,
    })


# --- Data Loading and Parsing Engine ---

def load_all_run_data(base_dir: Path) -> pd.DataFrame:
    """Load and aggregate all run data."""
    all_rows = []
    print("Scanning for runs and parsing all raw data files...")

    for profile_dir in base_dir.glob("wan-*_load-*"):
        for run_dir in profile_dir.glob("run-*"):
            try:
                result_dir = next(d for d in run_dir.iterdir() if d.is_dir())
            except StopIteration:
                print(f"Warning: No result subdirectory found in {run_dir}. Skipping.")
                continue

            profile_dir_name = result_dir.parts[-3]
            run_number = int(result_dir.parts[-2].replace("run-", ""))
            try:
                wan_part, load_part = profile_dir_name.split("_load-")
                wan_profile = wan_part.replace("wan-", "")
                load_profile = load_part
            except ValueError:
                print(f"Warning: Skipping malformed directory: {profile_dir_name}")
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

    # Convert profile columns to Categorical type to enforce sorting in all plots
    df = pd.DataFrame(all_rows)
    df['wan_profile'] = pd.Categorical(
        df['wan_profile'], categories=WAN_ORDER, ordered=True
    )
    df['local_load'] = pd.Categorical(
        df['local_load'], categories=LOAD_ORDER, ordered=True
    )
    return df


def parse_http_benchmark(path: Path) -> dict:
    """Parse 'hey' output for latency percentiles."""
    if not path.is_file():
        return {}
    content, results = path.read_text(), {}
    for p in ["50", "90", "95", "99"]:
        if match := re.search(rf"\s+{p}%\s+in\s+([\d.]+)", content):
            results[f'http_p{p}_ms'] = float(match.group(1)) * 1000
    return results


def load_placement_data(base_dir: Path) -> pd.DataFrame:
    """Load pod placement data."""
    all_placements = []
    print("\nScanning for pod placement data...")

    for profile_dir in base_dir.glob("wan-*_load-*"):
        for run_dir in profile_dir.glob("run-*"):
            try:
                result_dir = next(d for d in run_dir.iterdir() if d.is_dir())
            except StopIteration:
                continue

            # --- Extract metadata from the path ---
            profile_dir_name = result_dir.parts[-3]
            run_number = int(result_dir.parts[-2].replace("run-", ""))
            try:
                wan_part, load_part = profile_dir_name.split("_load-")
                wan_profile = wan_part.replace("wan-", "")
                load_profile = load_part
            except ValueError:
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
                    "node_type": (
                        'cloud' if CLOUD_NODE_IDENTIFIER in row['node_name']
                        else 'edge'
                    )
                })

    if not all_placements:
        return pd.DataFrame()

    df = pd.DataFrame(all_placements)
    df['wan_profile'] = pd.Categorical(
        df['wan_profile'], categories=WAN_ORDER, ordered=True
    )
    df['local_load'] = pd.Categorical(
        df['local_load'], categories=LOAD_ORDER, ordered=True
    )
    return df


def get_workload_from_pod_name(pod_name: str) -> str:
    """Extract workload name from pod name."""
    match = re.match(r'([a-z-]+)-[a-z0-9]{5}', pod_name)
    if match:
        return match.group(1)
    match = re.match(r'([a-z-]+)-[a-z0-9]{9,10}-[a-z0-9]{5}', pod_name)
    if match:
        return match.group(1)
    return pod_name


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
        df.groupby(['wan_profile', 'local_load', 'workload'])['pass']
        .value_counts(normalize=True)
        .unstack(fill_value=0)
    )
    pass_rate['pass_pct'] = pass_rate.get(True, 0) * 100

    heatmap_data = pass_rate.pivot_table(
        index='workload',
        columns=['local_load', 'wan_profile'],
        values='pass_pct'
    )

    # Drop columns (load/wan combinations) that have no data
    heatmap_data = heatmap_data.dropna(axis=1, how='all')
    # Drop rows (workloads) that have all NaN
    heatmap_data = heatmap_data.dropna(axis=0, how='all')

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
        "System Reliability: SLO Pass Rate Across All Conditions",
        fontsize=14,
        fontweight='bold',
        pad=20
    )
    ax.set_xlabel(
        "Load & WAN Profile",
        fontsize=12,
        fontweight='semibold'
    )
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
        scale=1.2,
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
    g.add_legend(title="WAN Profile")
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
        ['wan_profile', 'local_load', 'workload', 'node_type']
    ).size().reset_index(name='count')

    mean_counts = placement_counts.groupby(
        ['wan_profile', 'local_load', 'workload', 'node_type']
    )['count'].mean().reset_index()

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
        row_order=WAN_ORDER,
        col_order=LOAD_ORDER,
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
    g.add_legend(title="Node Type", bbox_to_anchor=(1.02, 1), loc='upper left')
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


def main():
    parser = argparse.ArgumentParser(
        description="Generate professional visualizations for SLO benchmarks."
    )
    parser.add_argument(
        "results_dir",
        type=Path,
        help="Path to the multi-run results directory."
    )
    args = parser.parse_args()

    results_dir = args.results_dir
    output_dir = results_dir / "_analysis_plots"
    output_dir.mkdir(exist_ok=True)

    # Setup professional styling
    setup_theme()

    # Load data
    df_slo = load_all_run_data(results_dir)
    df_placement = load_placement_data(results_dir)

    # Save aggregated data
    df_slo.to_csv(results_dir / "aggregated_slo_data.csv", index=False)
    df_placement.to_csv(
        results_dir / "aggregated_placement_data.csv",
        index=False
    )
    print(f"\n✅ Aggregated data saved")

    # Generate plots
    print("\n--- Group A: High-Level Summary Plots ---")
    plot_1_latency_comparison_bars(df_slo, output_dir)
    plot_2_job_duration_bars(df_slo, output_dir)
    plot_3_slo_pass_rate_heatmap(df_slo, output_dir)

    print("\n--- Group B: Deep-Dive Statistical Plots ---")
    plot_4_http_latency_full_distribution(df_slo, output_dir)
    plot_5_performance_interaction(df_slo, output_dir)
    plot_6_slo_failure_magnitude(df_slo, output_dir)
    plot_7_raw_data_variance(df_slo, output_dir)

    print("\n--- Group C: Scheduler Behavior Plots ---")
    plot_8_placement_analysis(df_placement, output_dir)

    print("\n✅ Visualization complete!")
    print(f"All graphs saved in: {output_dir}")


if __name__ == "__main__":
    main()
