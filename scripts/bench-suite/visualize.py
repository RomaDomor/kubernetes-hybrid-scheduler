#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Any

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
        result_subdirs = list(run_dir.glob("20*"))
        if not result_subdirs:
            continue
        result_dir = result_subdirs[0]

        wan_profile = result_dir.parts[-4].replace("wan-", "")
        load_profile = result_dir.parts[-4].split("_")[-1].replace("load-", "")
        run_number = int(result_dir.parts[-2].replace("run-", ""))

        # We now parse the detailed SLO file, not just the summary
        slo_file = result_dir / "slo_summary.json"
        if not slo_file.is_file():
            continue

        with open(slo_file) as f:
            slo_data = json.load(f)
            for item in slo_data.get("items", []):
                run_data = {
                    "wan_profile": wan_profile,
                    "local_load": load_profile,
                    "run": run_number,
                    **item # Unpack all SLO item details (workload, kind, metric, etc.)
                }
                all_runs_data.append(run_data)

    if not all_runs_data:
        sys.exit("Error: No valid run data found. Check results directory structure.")

    return pd.DataFrame(all_runs_data)


# --- COMPARATIVE PLOTTING FUNCTIONS ---
def plot_latency_trends(df: pd.DataFrame, output_dir: Path):
    """
    **Insight:** How does latency for interactive workloads degrade as WAN conditions worsen,
    and how is this trend affected by local cluster load?

    This plot uses lines to clearly show the performance trend. A steeper line indicates
    a greater sensitivity to network conditions. Comparing the lines shows the impact
    of local load.
    """
    print("Generating: 1. Latency Degradation Trends")
    latency_df = df[df['workload'].isin(['http-latency', 'stream-processor'])].copy()
    if latency_df.empty:
        print("  Skipping: No latency data found.")
        return

    g = sns.relplot(
        data=latency_df,
        x='wan_profile',
        y='measured_ms',
        hue='local_load',
        col='workload',
        kind='line',
        errorbar='sd', # Show standard deviation as a shaded area
        marker='o',
        height=5,
        aspect=1.2,
        facet_kws=dict(sharey=False),
        col_order=sorted(latency_df['workload'].unique()),
        order=['clear', 'good', 'moderate', 'poor']
    )

    g.set_axis_labels("WAN Profile", "Latency (ms)")
    g.set_titles("Workload: {col_name}")
    g.legend.set_title("Local Load")
    plt.suptitle("Interactive Workload Latency Trends vs. WAN & Local Load", y=1.05)

    # Add SLO lines for context
    for ax, workload in zip(g.axes.flat, g.col_names):
        slo_val = latency_df[latency_df['workload'] == workload]['target_ms'].median()
        if pd.notna(slo_val):
            ax.axhline(slo_val, ls='--', color='red', label=f'SLO Target ({slo_val:.0f}ms)')
            ax.legend()

    path = output_dir / "1_latency_degradation_trends.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def plot_job_duration_impact(df: pd.DataFrame, output_dir: Path):
    """
    **Insight:** How do WAN conditions and local load independently and jointly impact
    the completion time of different types of batch jobs?

    This plot uses faceting to create a matrix, allowing direct comparison of how a single
    job behaves under all tested conditions.
    """
    print("Generating: 2. Job Duration Impact Analysis")
    job_df = df[df['kind'] == 'Job'].copy()
    if job_df.empty:
        print("  Skipping: No job data found.")
        return

    job_df['duration_s'] = job_df['measured_ms'] / 1000.0

    # Select key jobs for clarity, or plot all
    key_jobs = ['cpu-batch', 'build-job', 'io-job', 'stream-data-generator']
    plot_df = job_df[job_df['workload'].isin(key_jobs)]
    if plot_df.empty:
        plot_df = job_df # Fallback to all jobs if key jobs aren't found

    g = sns.catplot(
        data=plot_df,
        x='wan_profile',
        y='duration_s',
        hue='local_load',
        col='workload',
        kind='bar',
        errorbar='sd',
        capsize=.05,
        height=5,
        aspect=0.8,
        col_wrap=2, # Arrange plots in a grid
        sharey=False,
        order=['clear', 'good', 'moderate', 'poor'],
        col_order=sorted(plot_df['workload'].unique())
    )

    g.set_axis_labels("WAN Profile", "Mean Duration (s)")
    g.set_titles("Job: {col_name}")
    g.add_legend(title="Local Load")
    g.set_xticklabels(rotation=30, ha='right')
    plt.suptitle("Impact of WAN and Local Load on Batch Job Runtimes", y=1.05)
    path = output_dir / "2_job_duration_impact_matrix.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def plot_slo_pass_rate_heatmap(df: pd.DataFrame, output_dir: Path):
    """
    **Insight:** What is the overall system reliability under each combination of conditions?
    Where are the "failure zones"?

    A heatmap provides a high-level, dense summary of performance, making it easy to
    spot which combinations of factors lead to poor outcomes.
    """
    print("Generating: 3. SLO Pass Rate Heatmap")

    # Calculate pass rate for each group
    pass_rate = df.groupby(['wan_profile', 'local_load', 'workload'])['pass'].value_counts(normalize=True).unstack(fill_value=0)
    pass_rate['pass_pct'] = pass_rate.get(True, 0) * 100

    heatmap_data = pass_rate.pivot_table(index='workload', columns=['local_load', 'wan_profile'], values='pass_pct')
    # Create multi-level columns and sort them logically
    heatmap_data.columns = pd.MultiIndex.from_tuples(heatmap_data.columns)
    heatmap_data = heatmap_data.reindex(sorted(heatmap_data.columns), axis=1)

    plt.figure(figsize=(16, 8))
    sns.heatmap(
        heatmap_data,
        annot=True,
        fmt=".1f",
        cmap="viridis_r", # Reversed: green=high, yellow=low
        linewidths=.5,
        cbar_kws={'label': 'SLO Pass Rate (%)'}
    )

    plt.title("System Reliability: SLO Pass Rate (%) Under All Conditions", fontsize=16)
    plt.xlabel("Local Load & WAN Profile Combination", fontsize=12)
    plt.ylabel("Workload", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    path = output_dir / "3_slo_pass_rate_heatmap.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")

def plot_performance_vs_slo(df: pd.DataFrame, output_dir: Path):
    """
    **Insight:** How close is each workload to its SLO target? Is it consistently meeting
    the target, or is it highly variable and sometimes failing?

    This plot shows the distribution of raw measurements against the SLO target, giving a
    clear picture of performance margin and stability.
    """
    print("Generating: 4. Performance vs. SLO Target Distribution")

    # Filter for data that has a valid SLO target
    plot_df = df.dropna(subset=['measured_ms', 'target_ms']).copy()
    if plot_df.empty:
        print("  Skipping: No data with both measured and target values.")
        return

    plot_df['performance_margin_pct'] = ((plot_df['target_ms'] - plot_df['measured_ms']) / plot_df['target_ms']) * 100

    g = sns.catplot(
        data=plot_df,
        x='wan_profile',
        y='performance_margin_pct',
        hue='local_load',
        col='workload',
        kind='box',
        col_wrap=3,
        sharey=False,
        showfliers=False, # Hide outliers for a cleaner look
        order=['clear', 'good', 'moderate', 'poor'],
        col_order=sorted(plot_df['workload'].unique())
    )

    g.set_axis_labels("WAN Profile", "Performance Margin (%)")
    g.set_titles("Workload: {col_name}")
    g.add_legend(title="Local Load")

    # Add a horizontal line at 0%, which represents the SLO target
    for ax in g.axes.flat:
        ax.axhline(0, ls='--', color='red')

    plt.suptitle("Performance Margin Relative to SLO Target (0% = SLO Met Exactly)", y=1.05)
    path = output_dir / "4_performance_vs_slo_margin.png"
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def main():
    parser = argparse.ArgumentParser(description="Visualize and compare Kubernetes SLO benchmark results.")
    parser.add_argument("results_dir", type=Path, help="Path to the multi-run results directory (e.g., ./results/multi-run-...).")
    args = parser.parse_args()

    results_dir = args.results_dir
    output_dir = results_dir / "_analysis_plots"
    output_dir.mkdir(exist_ok=True)

    # Step 1: Aggregate all raw data into a single DataFrame
    df = load_all_run_data(results_dir)

    # Save the powerful, newly aggregated data for external use
    agg_csv_path = results_dir / "aggregated_detailed_data.csv"
    df.to_csv(agg_csv_path, index=False)
    print(f"\n✅ Aggregated detailed data from {len(df)} measurements saved to {agg_csv_path}")

    # --- Generate Plots ---
    sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)

    # Generate the 4 key comparative plots
    plot_latency_trends(df, output_dir)
    plot_job_duration_impact(df, output_dir)
    plot_slo_pass_rate_heatmap(df, output_dir)
    plot_performance_vs_slo(df, output_dir)

    print("\n✅ Visualization complete!")
    print(f"Graphs saved in: {output_dir}")

if __name__ == "__main__":
    main()