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

# Define the desired order for profiles. This ensures all graphs are consistent.
WAN_ORDER = ['clear', 'good', 'moderate', 'poor']
LOAD_ORDER = ['none', 'low', 'medium', 'high']
EDGE_NODE_IDENTIFIER = 'edge'

# --- Data Loading and Parsing Engine (Unchanged) ---

def load_all_run_data(base_dir: Path) -> pd.DataFrame:
    """
    Walks the entire results tree, parses raw data from each run's files,
    and aggregates everything into a single DataFrame for analysis.
    """
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
                print(f"Warning: Skipping malformed directory name: {profile_dir_name}")
                continue

            # --- Parse run-level raw files once ---
            http_data = parse_http_benchmark(result_dir / "http_benchmark.txt")

            # --- Parse the detailed SLO summary for workload-level data ---
            slo_file = result_dir / "slo_summary.json"
            if not slo_file.is_file(): continue

            with open(slo_file) as f:
                slo_data = json.load(f)
                for item in slo_data.get("items", []):
                    all_rows.append({
                        "wan_profile": wan_profile, "local_load": load_profile,
                        "run": run_number, **item, **http_data,
                    })

    if not all_rows:
        sys.exit("Error: No valid SLO summary data found. Did the benchmark runs complete?")

    # Convert profile columns to Categorical type to enforce sorting in all plots
    df = pd.DataFrame(all_rows)
    df['wan_profile'] = pd.Categorical(df['wan_profile'], categories=WAN_ORDER, ordered=True)
    df['local_load'] = pd.Categorical(df['local_load'], categories=LOAD_ORDER, ordered=True)
    return df

def parse_http_benchmark(path: Path) -> dict:
    """Parses 'hey' output to extract latency percentiles."""
    if not path.is_file(): return {}
    content, results = path.read_text(), {}
    for p in ["50", "90", "95", "99"]:
        if match := re.search(rf"\s+{p}%\s+in\s+([\d.]+)", content):
            results[f'http_p{p}_ms'] = float(match.group(1)) * 1000
    return results

def load_placement_data(base_dir: Path) -> pd.DataFrame:
    """
    Scans all pod_node_map.csv files to build a specific DataFrame for placement analysis.
    """
    all_placements = []
    print("\nScanning for pod placement data (pod_node_map.csv)...")

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
                    "node_type": 'edge' if EDGE_NODE_IDENTIFIER in row['node_name'] else 'cloud'
                })

    if not all_placements:
        return pd.DataFrame()

    df = pd.DataFrame(all_placements)
    df['wan_profile'] = pd.Categorical(df['wan_profile'], categories=WAN_ORDER, ordered=True)
    df['local_load'] = pd.Categorical(df['local_load'], categories=LOAD_ORDER, ordered=True)
    return df

def get_workload_from_pod_name(pod_name: str) -> str:
    """Heuristic to extract the base workload name from a generated pod name."""
    # For Jobs like 'cpu-batch-sn5xx'
    match = re.match(r'([a-z-]+)-[a-z0-9]{5}', pod_name)
    if match:
        return match.group(1)
    # For Deployments like 'http-latency-58bd4fb596-lcvgt'
    match = re.match(r'([a-z-]+)-[a-z0-9]{9,10}-[a-z0-9]{5}', pod_name)
    if match:
        return match.group(1)
    return pod_name # Fallback

# --- Plotting Library (All 7 Graphs) ---

# --- Group A: High-Level Comparative Summaries ---

def plot_1_latency_comparison_bars(df: pd.DataFrame, output_dir: Path):
    """(Graph 1) High-level bar chart comparing mean latency."""
    print("Generating: 1. Mean Latency Comparison (Bar Chart)")
    latency_df = df[df['workload'].isin(['http-latency', 'stream-processor'])].copy()
    if latency_df.empty:
        print("  Skipping: No latency workload data found.")
        return

    for load_profile in LOAD_ORDER:
        group = latency_df[latency_df['local_load'] == load_profile]
        if group.empty:  # Skip this specific load profile
            continue

        group = latency_df[latency_df['local_load'] == load_profile]
        g = sns.catplot(
            data=group, x='wan_profile', y='measured_ms', hue='workload',
            kind='bar', errorbar='sd', capsize=.1, aspect=1.5
        )
        g.set_axis_labels("WAN Profile", "Mean Measured Latency (ms)")
        g.legend.set_title("Workload")
        plt.suptitle(f'Mean Latency under "{load_profile}" Local Load', y=1.05)
        path = output_dir / f"A1_mean_latency_bars_load_{load_profile}.png"
        g.savefig(path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"  Saved: {path}")

def plot_2_job_duration_bars(df: pd.DataFrame, output_dir: Path):
    """(Graph 2) High-level bar chart comparing mean job durations."""
    print("Generating: 2. Mean Job Duration Comparison (Bar Chart)")
    job_df = df[df['kind'] == 'Job'].copy()
    if job_df.empty: return
    job_df['duration_s'] = job_df['measured_ms'] / 1000.0

    g = sns.catplot(
        data=job_df, x='workload', y='duration_s', hue='wan_profile',
        col='local_load', kind='bar', errorbar='sd', capsize=.05,
        aspect=1.2, height=5
    )
    g.set_axis_labels("Job Name", "Mean Duration (seconds)")
    g.set_titles('Local Load: "{col_name}"')
    g.add_legend(title="WAN Profile") # add_legend ensures it's drawn
    g.set_xticklabels(rotation=45, ha='right')
    plt.suptitle("Mean Batch Job Duration Comparison", y=1.03)
    path = output_dir / "A2_mean_job_duration_bars.png"
    g.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")

def plot_3_slo_pass_rate_heatmap(df: pd.DataFrame, output_dir: Path):
    """(Graph 3) High-level heatmap of overall system reliability."""
    print("Generating: 3. SLO Pass Rate Reliability Heatmap")
    pass_rate = df.groupby(['wan_profile', 'local_load', 'workload'])['pass'].value_counts(normalize=True).unstack(fill_value=0)
    pass_rate['pass_pct'] = pass_rate.get(True, 0) * 100

    # Pivot respects the categorical order set during data loading
    heatmap_data = pass_rate.pivot_table(index='workload', columns=['local_load', 'wan_profile'], values='pass_pct')

    plt.figure(figsize=(16, 8))
    sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap="viridis_r", linewidths=.5, cbar_kws={'label': 'SLO Pass Rate (%)'})
    plt.title("System Reliability: SLO Pass Rate (%) Under All Conditions", fontsize=16)
    plt.xlabel("Local Load & WAN Profile Combination", fontsize=12)
    plt.ylabel("Workload", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    path = output_dir / "A3_slo_pass_rate_heatmap.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")

# --- Group B: Deep-Dive Statistical and Variance Analyses ---

def plot_4_http_latency_full_distribution(df: pd.DataFrame, output_dir: Path):
    """(Graph 4) Compares full latency distributions using violin plots."""
    print("Generating: 4. Full HTTP Latency Distribution (Violin Plot)")
    plot_df = df[df['workload'] == 'http-latency'].melt(
        id_vars=['wan_profile', 'local_load'], value_vars=['http_p50_ms', 'http_p95_ms', 'http_p99_ms'],
        var_name='percentile', value_name='latency_ms'
    )
    if plot_df.empty: return
    plot_df['percentile'] = plot_df['percentile'].str.replace('http_p', '').str.replace('_ms', '')

    g = sns.catplot(
        data=plot_df, x='wan_profile', y='latency_ms', hue='percentile',
        col='local_load', kind='violin', split=True, inner='quartiles',
        height=6, aspect=1.2, palette='viridis'
    )
    g.set_axis_labels("WAN Profile", "Latency (ms)")
    g.set_titles("Local Load: {col_name}")
    g.legend.set_title("Percentile")
    plt.suptitle("HTTP Latency Distribution by Percentile", y=1.03)
    path = output_dir / "B1_http_latency_distribution.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")

def plot_5_performance_interaction(df: pd.DataFrame, output_dir: Path):
    """(Graph 5) Visualizes the interaction between WAN and local load."""
    print("Generating: 5. Performance Interaction Plot")
    job_to_plot = 'build-job'
    plot_df = df[df['workload'] == job_to_plot].copy()
    if plot_df.empty: return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    plt.figure(figsize=(10, 6))
    sns.pointplot(
        data=plot_df, x='wan_profile', y='duration_s', hue='local_load',
        errorbar='sd', capsize=.1
    )
    plt.title(f"Interaction of WAN and Local Load on '{job_to_plot}' Runtime")
    plt.xlabel("WAN Profile")
    plt.ylabel("Mean Duration (s) with StdDev")
    plt.legend(title="Local Load")
    path = output_dir / "B2_performance_interaction_plot.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")

def plot_6_slo_failure_magnitude(df: pd.DataFrame, output_dir: Path):
    """(Graph 6) Shows how badly jobs miss their deadline when they fail."""
    print("Generating: 6. SLO Failure Magnitude Analysis")
    plot_df = df[df['pass'] == False].copy()
    plot_df = plot_df[plot_df['measured_ms'].notna() & plot_df['target_ms'].notna()]
    if plot_df.empty:
        print("  Skipping: No SLO failures with targets found to plot.")
        return
    plot_df['overshoot_pct'] = (plot_df['measured_ms'] - plot_df['target_ms']) / plot_df['target_ms'] * 100

    g = sns.catplot(
        data=plot_df, x='workload', y='overshoot_pct', hue='wan_profile',
        col='local_load', kind='bar', errorbar='sd', capsize=.05,
        height=5, aspect=1.5, palette='autumn'
    )
    g.set_axis_labels("Workload", "Deadline Overshoot (%)")
    g.set_titles("Local Load: {col_name}")
    g.add_legend(title="WAN Profile")
    g.set_xticklabels(rotation=30, ha='right')
    plt.suptitle("Magnitude of SLO Failures (How badly deadlines are missed)", y=1.03)
    path = output_dir / "B3_slo_failure_magnitude.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")

def plot_7_raw_data_variance(df: pd.DataFrame, output_dir: Path):
    """(Graph 7) Shows every single data point to reveal outliers and variance."""
    print("Generating: 7. Raw Data Point Variance Plot")
    job_to_plot = 'cpu-batch'
    plot_df = df[df['workload'] == job_to_plot].copy()
    if plot_df.empty: return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    plt.figure(figsize=(14, 8))
    ax = sns.boxplot(data=plot_df, x='wan_profile', y='duration_s', hue='local_load', showfliers=False)
    sns.stripplot(data=plot_df, x='wan_profile', y='duration_s', hue='local_load',
                  dodge=True, ax=ax, alpha=0.5, legend=False, palette='dark:black')

    plt.title(f"Runtime Variance of '{job_to_plot}' Across All Runs")
    plt.xlabel("WAN Profile")
    plt.ylabel("Duration (s)")
    ax.legend(title='Local Load') # Get the legend from the boxplot axis and set its title
    path = output_dir / "B4_raw_data_variance.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")

def plot_8_placement_analysis(df: pd.DataFrame, output_dir: Path):
    """
    (Graph 8) Visualizes where pods for each workload were scheduled (cloud vs. edge)
    across all experimental conditions.
    """
    print("Generating: 8. Workload Placement Analysis")
    if df.empty:
        print("  Skipping: No pod placement data found to analyze.")
        return

    # Count pods per group
    placement_counts = df.groupby(
        ['wan_profile', 'local_load', 'workload', 'node_type']
    ).size().reset_index(name='count')

    # We take the mean count across runs to normalize for any failed runs
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
        height=4,
        aspect=1.5,
        palette={'cloud': 'skyblue', 'edge': 'salmon'},
        legend=False,
        row_order=WAN_ORDER,
        col_order=LOAD_ORDER
    )

    g.set_axis_labels("Workload", "Mean Pod Count")
    g.set_titles(row_template="WAN: {row_name}", col_template="Local Load: {col_name}")
    g.add_legend(title="Node Type")
    g.set_xticklabels(rotation=45, ha='right')

    plt.suptitle("Workload Placement by Node Type Across All Conditions", y=1.03)
    path = output_dir / "C1_workload_placement_matrix.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")

def main():
    parser = argparse.ArgumentParser(description="Generate a comprehensive set of visualizations for SLO benchmarks.")
    parser.add_argument("results_dir", type=Path, help="Path to the multi-run results directory.")
    args = parser.parse_args()

    results_dir = args.results_dir
    output_dir = results_dir / "_analysis_plots"
    output_dir.mkdir(exist_ok=True)

    # --- Data Loading ---
    df_slo = load_all_run_data(results_dir) # For SLO and performance plots
    df_placement = load_placement_data(results_dir) # For placement plot

    # --- Save Aggregated Data ---
    df_slo.to_csv(results_dir / "aggregated_slo_data.csv", index=False)
    df_placement.to_csv(results_dir / "aggregated_placement_data.csv", index=False)
    print(f"\n✅ Aggregated data saved to CSV files in {results_dir}")

    sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)

    print("\n--- Generating High-Level Summary Plots (Group A) ---")
    plot_1_latency_comparison_bars(df_slo, output_dir)
    plot_2_job_duration_bars(df_slo, output_dir)
    plot_3_slo_pass_rate_heatmap(df_slo, output_dir)

    print("\n--- Generating Deep-Dive Statistical Plots (Group B) ---")
    plot_4_http_latency_full_distribution(df_slo, output_dir)
    plot_5_performance_interaction(df_slo, output_dir)
    plot_6_slo_failure_magnitude(df_slo, output_dir)
    plot_7_raw_data_variance(df_slo, output_dir)

    print("\n--- Generating Scheduler Behavior Plots (Group C) ---")
    plot_8_placement_analysis(df_placement, output_dir)

    print("\n✅ Visualization complete!")
    print(f"All graphs saved in: {output_dir}")


if __name__ == "__main__":
    main()