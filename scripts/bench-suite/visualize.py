#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# --- Data Loading and Parsing Engine ---

def load_all_run_data(base_dir: Path) -> pd.DataFrame:
    """
    Walks the entire results tree, parses raw data from each run's JSON and TXT files,
    and aggregates everything into a single, powerful DataFrame for analysis.
    This is the core data ingestion engine for all plots.
    """
    all_rows = []
    print("Scanning for runs and parsing all raw data files...")

    for run_dir in base_dir.glob("wan-*/load-*/run-*/"):
        result_subdirs = list(run_dir.glob("20*"))
        if not result_subdirs: continue
        result_dir = result_subdirs[0]

        # --- Extract metadata from the path ---
        path_parts = result_dir.parts
        wan_profile = path_parts[-4].split("_")[0].replace("wan-", "")
        load_profile = path_parts[-4].split("_")[1].replace("load-", "")
        run_number = int(path_parts[-2].replace("run-", ""))

        # --- Parse run-level raw files once ---
        http_data = parse_http_benchmark(result_dir / "http_benchmark.txt")

        # --- Parse the detailed SLO summary for workload-level data ---
        slo_file = result_dir / "slo_summary.json"
        if not slo_file.is_file(): continue

        with open(slo_file) as f:
            slo_data = json.load(f)
            for item in slo_data.get("items", []):
                row = {
                    "wan_profile": wan_profile,
                    "local_load": load_profile,
                    "run": run_number,
                    **item,  # Unpack all SLO item details (workload, kind, metric, etc.)
                    **http_data, # Add the run-level http data to each workload row
                }
                all_rows.append(row)

    if not all_rows:
        sys.exit("Error: No valid SLO summary data found. Did the benchmark runs complete?")

    return pd.DataFrame(all_rows)

def parse_http_benchmark(path: Path) -> dict:
    """Parses the raw text output from 'hey' to extract latency percentiles."""
    if not path.is_file(): return {}
    content, results = path.read_text(), {}
    for p in ["50", "90", "95", "99"]:
        if match := re.search(rf"\s+{p}%\s+in\s+([\d.]+)", content):
            results[f'http_p{p}_ms'] = float(match.group(1)) * 1000
    return results

# --- Plotting Library (All 7 Graphs) ---

# --- Group A: High-Level Comparative Summaries ---

def plot_1_latency_comparison_bars(df: pd.DataFrame, output_dir: Path):
    """(Graph 1) High-level bar chart comparing mean latency of interactive workloads."""
    print("Generating: 1. Mean Latency Comparison (Bar Chart)")
    latency_df = df[df['workload'].isin(['http-latency', 'stream-processor'])].copy()
    if latency_df.empty: return

    for load_profile, group in latency_df.groupby('local_load'):
        g = sns.catplot(
            data=group, x='wan_profile', y='measured_ms', hue='workload',
            kind='bar', errorbar='sd', capsize=.1, aspect=1.5, legend=False,
            order=['clear', 'good', 'moderate', 'poor']
        )
        g.set_axis_labels("WAN Profile", "Mean Measured Latency (ms)")
        g.add_legend(title="Workload")
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
        aspect=1.2, height=5, legend=False,
        hue_order=['clear', 'good', 'moderate', 'poor']
    )
    g.set_axis_labels("Job Name", "Mean Duration (seconds)")
    g.set_titles('Local Load: "{col_name}"')
    g.add_legend(title="WAN Profile")
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
    heatmap_data = pass_rate.pivot_table(index='workload', columns=['local_load', 'wan_profile'], values='pass_pct')
    heatmap_data.columns = pd.MultiIndex.from_tuples(heatmap_data.columns)
    heatmap_data = heatmap_data.reindex(sorted(heatmap_data.columns, key=lambda c: (c[0], ['clear', 'good', 'moderate', 'poor'].index(c[1]))), axis=1)

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
        id_vars=['wan_profile', 'local_load'],
        value_vars=['http_p50_ms', 'http_p95_ms', 'http_p99_ms'],
        var_name='percentile', value_name='latency_ms'
    )
    plot_df['percentile'] = plot_df['percentile'].str.replace('http_p', '').str.replace('_ms', '')

    g = sns.catplot(
        data=plot_df, x='wan_profile', y='latency_ms', hue='percentile',
        col='local_load', kind='violin', split=True, inner='quartiles',
        height=6, aspect=1.2, palette='viridis',
        order=['clear', 'good', 'moderate', 'poor']
    )
    g.set_axis_labels("WAN Profile", "Latency (ms)")
    g.set_titles("Local Load: {col_name}")
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
    if plot_df.empty:
        print(f"  Skipping: '{job_to_plot}' data not found.")
        return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    plt.figure(figsize=(10, 6))
    sns.pointplot(data=plot_df, x='wan_profile', y='duration_s', hue='local_load',
                  errorbar='sd', capsize=.1, order=['clear', 'good', 'moderate', 'poor'])
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
    plot_df = plot_df.dropna(subset=['measured_ms', 'target_ms'])
    if plot_df.empty:
        print("  Skipping: No SLO failures with targets found to plot.")
        return
    plot_df['overshoot_pct'] = (plot_df['measured_ms'] - plot_df['target_ms']) / plot_df['target_ms'] * 100

    g = sns.catplot(
        data=plot_df, x='workload', y='overshoot_pct', hue='wan_profile',
        col='local_load', kind='bar', errorbar='sd', capsize=.05,
        height=5, aspect=1.5, palette='autumn',
        hue_order=['clear', 'good', 'moderate', 'poor']
    )
    g.set_axis_labels("Workload", "Deadline Overshoot (%)")
    g.set_titles("Local Load: {col_name}")
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
    if plot_df.empty:
        print(f"  Skipping: '{job_to_plot}' data not found.")
        return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    plt.figure(figsize=(14, 8))
    ax = sns.boxplot(data=plot_df, x='wan_profile', y='duration_s', hue='local_load',
                     order=['clear', 'good', 'moderate', 'poor'], showfliers=False)
    sns.stripplot(data=plot_df, x='wan_profile', y='duration_s', hue='local_load',
                  order=['clear', 'good', 'moderate', 'poor'], dodge=True,
                  ax=ax, alpha=0.5, legend=False, palette='dark:black')
    plt.title(f"Runtime Variance of '{job_to_plot}' Across All Runs")
    plt.xlabel("WAN Profile")
    plt.ylabel("Duration (s)")
    ax.legend(title='Local Load')
    path = output_dir / "B4_raw_data_variance.png"
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

    df = load_all_run_data(results_dir)
    agg_csv_path = results_dir / "aggregated_full_data_for_analysis.csv"
    df.to_csv(agg_csv_path, index=False)
    print(f"\n✅ Fully aggregated raw data saved to {agg_csv_path}")

    sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)

    print("\n--- Generating High-Level Summary Plots ---")
    plot_1_latency_comparison_bars(df, output_dir)
    plot_2_job_duration_bars(df, output_dir)
    plot_3_slo_pass_rate_heatmap(df, output_dir)

    print("\n--- Generating Deep-Dive Statistical Plots ---")
    plot_4_http_latency_full_distribution(df, output_dir)
    plot_5_performance_interaction(df, output_dir)
    plot_6_slo_failure_magnitude(df, output_dir)
    plot_7_raw_data_variance(df, output_dir)

    print("\n✅ Visualization complete!")
    print(f"All graphs saved in: {output_dir}")

if __name__ == "__main__":
    main()