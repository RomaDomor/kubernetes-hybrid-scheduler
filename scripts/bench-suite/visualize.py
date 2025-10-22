#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Any, List

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# --- Data Parsing Functions ---

def parse_slo_summary(path: Path) -> Dict[str, Any]:
    """Parses the slo_summary.json to get pass counts."""
    if not path.is_file():
        return {}
    with open(path) as f:
        data = json.load(f)
        items = data.get("items", [])
        return {
            "slo_passed": sum(1 for item in items if item.get("pass")),
            "slo_total": len(items),
        }

def parse_http_benchmark(path: Path) -> Dict[str, Any]:
    """Parses the raw text output from 'hey' (http_benchmark.txt)."""
    if not path.is_file():
        return {}

    content = path.read_text()
    results = {}

    # Percentiles
    for p in ["50", "90", "95", "99"]:
        match = re.search(rf"\s+{p}%\s+in\s+([\d.]+)", content)
        if match:
            results[f'http_p{p}_ms'] = float(match.group(1)) * 1000

    # Requests per second
    match = re.search(r"Requests/sec:\s*([\d.]+)", content)
    if match:
        results['http_rps'] = float(match.group(1))

    return results

def parse_stream_logs(path: Path) -> Dict[str, Any]:
    """Parses the stream-data-generator_logs.txt for key metrics."""
    if not path.is_file():
        return {}

    content = path.read_text()
    results = {}

    match = re.search(r"Average latency:\s*([\d.]+)\s*ms", content)
    if match:
        results['stream_avg_latency_ms'] = float(match.group(1))

    match = re.search(r"Anomalies detected:\s*(\d+)", content)
    if match:
        results['stream_anomalies'] = int(match.group(1))

    return results

def parse_wan_state(path: Path) -> Dict[str, Any]:
    """Parses router_qdisc.txt to find the applied netem configuration."""
    if not path.is_file():
        return {}

    content = path.read_text()
    # Look for the netem line, which contains the settings
    match = re.search(r"netem.*delay ([\d.]+ms)(?: ([\d.]+ms))?.*loss ([\d.]+%?)", content)
    if not match:
        return {"wan_actual_delay_ms": 0, "wan_actual_jitter_ms": 0, "wan_actual_loss_pct": 0}

    delay_str, jitter_str, loss_str = match.groups()

    delay = float(delay_str.replace("ms", ""))
    jitter = float(jitter_str.replace("ms", "")) if jitter_str else 0
    loss = float(loss_str.replace("%", ""))

    return {
        "wan_actual_delay_ms": delay,
        "wan_actual_jitter_ms": jitter,
        "wan_actual_loss_pct": loss,
    }

def load_all_run_data(base_dir: Path) -> pd.DataFrame:
    """Walks the results directory, parses raw data from each run, and returns a DataFrame."""
    all_runs_data = []
    print("Scanning for runs and parsing raw data...")

    for run_dir in base_dir.glob("wan-*/load-*/run-*/"):
        # The actual results are in a timestamped subdirectory
        result_subdirs = list(run_dir.glob("20*"))
        if not result_subdirs:
            continue
        result_dir = result_subdirs[0]

        # Extract profile info from path
        parts = result_dir.parts
        wan_profile = parts[-4].replace("wan-", "")
        load_profile = parts[-4].split("_")[-1].replace("load-", "")
        run_number = int(parts[-2].replace("run-", ""))

        # Parse all relevant files
        run_data = {
            "wan_profile": wan_profile,
            "local_load": load_profile,
            "run": run_number,
            **parse_slo_summary(result_dir / "slo_summary.json"),
            **parse_http_benchmark(result_dir / "http_benchmark.txt"),
            **parse_stream_logs(result_dir / "stream-data-generator_logs.txt"),
            **parse_wan_state(result_dir / "router_qdisc.txt"),
        }
        all_runs_data.append(run_data)

    if not all_runs_data:
        sys.exit("Error: No valid run data found. Did the benchmark complete successfully?")

    return pd.DataFrame(all_runs_data)

# --- Plotting Functions ---

def plot_wan_verification(df: pd.DataFrame, output_dir: Path):
    """Verifies that the WAN emulation settings were applied correctly."""
    print("Generating WAN state verification plot...")
    wan_df = df[['wan_profile', 'wan_actual_delay_ms', 'wan_actual_loss_pct']].drop_duplicates()
    wan_df = wan_df.set_index('wan_profile').reindex(['clear', 'good', 'moderate', 'poor']).reset_index()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    sns.barplot(data=wan_df, x='wan_profile', y='wan_actual_delay_ms', ax=ax1, palette="Blues_d")
    ax1.set_title('Applied Network Latency')
    ax1.set_xlabel('WAN Profile')
    ax1.set_ylabel('Delay (ms)')

    sns.barplot(data=wan_df, x='wan_profile', y='wan_actual_loss_pct', ax=ax2, palette="Reds_d")
    ax2.set_title('Applied Packet Loss')
    ax2.set_xlabel('WAN Profile')
    ax2.set_ylabel('Packet Loss (%)')

    plt.suptitle("WAN Emulation State Verification")
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    path = output_dir / "wan_verification.png"
    plt.savefig(path, dpi=300)
    plt.close()
    print(f"  Saved: {path}")

def plot_http_latency_distribution(df: pd.DataFrame, output_dir: Path):
    """Plots the distribution of p99 latency to show variance and outliers."""
    print("Generating HTTP p99 latency distribution plot...")
    if 'http_p99_ms' not in df.columns:
        print("  Skipping: No http_p99_ms data found.")
        return

    g = sns.displot(
        data=df, x='http_p99_ms',
        col='wan_profile', row='local_load',
        kind='kde', fill=True,
        col_order=['clear', 'good', 'moderate', 'poor'],
        facet_kws=dict(margin_titles=True)
    )
    g.set_axis_labels("HTTP p99 Latency (ms)", "Density")
    g.fig.suptitle("Distribution of HTTP p99 Latency Across All Runs", y=1.03)
    path = output_dir / "http_latency_distribution.png"
    plt.savefig(path, dpi=300)
    plt.close()
    print(f"  Saved: {path}")

def plot_stream_processing_details(df: pd.DataFrame, output_dir: Path):
    """Plots stream latency and anomalies to give a fuller picture."""
    print("Generating stream processing details plot...")
    stream_df = df[['wan_profile', 'local_load', 'stream_avg_latency_ms', 'stream_anomalies']].copy()
    if stream_df.empty or stream_df['stream_avg_latency_ms'].isnull().all():
        print("  Skipping: No stream processing data found.")
        return

    fig, axes = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    sns.boxplot(data=stream_df, x='wan_profile', y='stream_avg_latency_ms', hue='local_load', ax=axes[0],
                order=['clear', 'good', 'moderate', 'poor'])
    axes[0].set_title('Stream Processor Average Latency Distribution')
    axes[0].set_ylabel('Average Latency (ms)')
    axes[0].set_xlabel('')

    sns.stripplot(data=stream_df, x='wan_profile', y='stream_anomalies', hue='local_load', ax=axes[1],
                  order=['clear', 'good', 'moderate', 'poor'], dodge=True, alpha=0.7)
    axes[1].set_title('Anomalies Detected by Stream Processor')
    axes[1].set_ylabel('Anomaly Count')
    axes[1].set_xlabel('WAN Profile')

    plt.suptitle("Stream Processing Performance Details")
    plt.tight_layout(rect=[0, 0.03, 1, 0.97])
    path = output_dir / "stream_details.png"
    plt.savefig(path, dpi=300)
    plt.close()
    print(f"  Saved: {path}")

def plot_slo_adherence_variance(df: pd.DataFrame, output_dir: Path):
    """Shows the variance in SLO pass counts across multiple runs."""
    print("Generating SLO adherence variance plot...")
    if 'slo_passed' not in df.columns:
        print("  Skipping: No SLO data found.")
        return

    df['slo_pass_rate'] = (df['slo_passed'] / df['slo_total']) * 100

    plt.figure(figsize=(12, 7))
    sns.boxplot(
        data=df,
        x='wan_profile',
        y='slo_pass_rate',
        hue='local_load',
        order=['clear', 'good', 'moderate', 'poor']
    )
    plt.title('Variance of SLO Pass Rate Across Runs')
    plt.ylabel('SLO Pass Rate (%)')
    plt.xlabel('WAN Profile')
    plt.legend(title='Local Load')
    plt.ylim(0, 105) # Set y-axis from 0% to 105%
    path = output_dir / "slo_adherence_variance.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def main():
    parser = argparse.ArgumentParser(description="Visualize Kubernetes SLO benchmark results from raw run data.")
    parser.add_argument("results_dir", type=Path, help="Path to the multi-run results directory (e.g., ./results/multi-run-...).")
    args = parser.parse_args()

    results_dir = args.results_dir
    output_dir = results_dir / "_analysis_plots"
    output_dir.mkdir(exist_ok=True)

    # The new script first aggregates all data from scratch
    df = load_all_run_data(results_dir)

    # Save the aggregated data to a CSV for manual inspection
    agg_csv_path = results_dir / "aggregated_raw_data.csv"
    df.to_csv(agg_csv_path, index=False)
    print(f"\n✅ Aggregated raw data from {len(df)} runs saved to {agg_csv_path}")

    # --- Generate Plots ---
    sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)

    # Plot 1: Verify the environment was set up correctly
    plot_wan_verification(df, output_dir)

    # Plot 2 & 3: Deep dive into specific workload performance and variance
    plot_http_latency_distribution(df, output_dir)
    plot_stream_processing_details(df, output_dir)

    # Plot 4: High-level view of system reliability and stability
    plot_slo_adherence_variance(df, output_dir)

    print("\n✅ Visualization complete!")
    print(f"Graphs saved in: {output_dir}")

if __name__ == "__main__":
    main()