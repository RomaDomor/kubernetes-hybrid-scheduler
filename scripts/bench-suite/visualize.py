#!/usr/bin/env python3
import argparse
import json
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.ticker import MaxNLocator, FuncFormatter

# --- Global Configuration for Consistency ---

WAN_ORDER = ['clear', 'good', 'moderate', 'poor']
LOAD_ORDER = ['none', 'low', 'medium', 'high']
CONFIG_ORDER = ['baseline', 'tuned-gentle', 'tuned-aggressive']

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

    for profile_dir in sorted(base_dir.glob("config-*_wan-*_load-*")):
        for run_dir in sorted(profile_dir.glob("run-*")):
            try:
                result_dir = next(d for d in run_dir.iterdir() if d.is_dir())
            except StopIteration:
                print(f"Warning: No result subdirectory found in {run_dir}. Skipping.")
                continue

            profile_dir_name = result_dir.parts[-3]
            try:
                run_number = int(result_dir.parts[-2].replace("run-", ""))
                match = re.match(r"config-(.*)_wan-(.*)_load-(.*)", profile_dir_name)
                if not match:
                    print(f"Warning: Skipping malformed directory name: {profile_dir_name}")
                    continue
                config_name, wan_profile, load_profile = match.groups()
            except (ValueError, IndexError):
                print(f"Warning: Skipping malformed directory path: {profile_dir_name}/{run_dir.name}")
                continue

            http_data = parse_http_benchmark(result_dir / "http_benchmark.txt")
            slo_file = result_dir / "slo_summary.json"
            if not slo_file.is_file():
                continue

            with open(slo_file) as f:
                slo_data = json.load(f)
                for item in slo_data.get("items", []):
                    all_rows.append({
                        "config_name": config_name,
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
    df_all['config_name'] = pd.Categorical(df_all['config_name'], categories=CONFIG_ORDER, ordered=True)

    print(f"\nLoaded {len(df_all)} total SLO records from all runs.")
    df_stable = df_all[df_all['run'] > warmup_runs].copy()
    if not df_stable.empty:
        df_stable['run'] = df_stable['run'] - warmup_runs
    print(f"  -> Discarding {warmup_runs} runs per profile for stable analysis.")
    print(f"  -> Using {len(df_stable)} records for stable plots.")

    return df_all, df_stable


def parse_http_benchmark(path: Path) -> dict:
    if not path.is_file(): return {}
    content, results = path.read_text(), {}
    for p in ["50", "90", "95", "99"]:
        if match := re.search(rf"\s+{p}%\s+in\s+([\d.]+)", content):
            results[f'http_p{p}_ms'] = float(match.group(1)) * 1000
    return results


def load_placement_data(base_dir: Path, warmup_runs: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_placements = []
    print("\nScanning for pod placement data...")

    for profile_dir in sorted(base_dir.glob("config-*_wan-*_load-*")):
        for run_dir in sorted(profile_dir.glob("run-*")):
            try:
                result_dir = next(d for d in run_dir.iterdir() if d.is_dir())
            except StopIteration:
                continue

            profile_dir_name = result_dir.parts[-3]
            try:
                run_number = int(result_dir.parts[-2].replace("run-", ""))
                match = re.match(r"config-(.*)_wan-(.*)_load-(.*)", profile_dir_name)
                if not match: continue
                config_name, wan_profile, load_profile = match.groups()
            except (ValueError, IndexError):
                continue

            placement_file = result_dir / "pod_node_map.csv"
            if not placement_file.is_file(): continue

            df_placements = pd.read_csv(placement_file)
            for _, row in df_placements.iterrows():
                all_placements.append({
                    "config_name": config_name,
                    "wan_profile": wan_profile,
                    "local_load": load_profile,
                    "run": run_number,
                    "pod_name": row['pod_name'],
                    "node_name": row['node_name'],
                    "workload": get_workload_from_pod_name(row['pod_name']),
                    "node_type": ('cloud' if CLOUD_NODE_IDENTIFIER in str(row['node_name']) else 'edge')
                })

    if not all_placements:
        return pd.DataFrame(), pd.DataFrame()

    df_all = pd.DataFrame(all_placements)
    df_all['wan_profile'] = pd.Categorical(df_all['wan_profile'], categories=WAN_ORDER, ordered=True)
    df_all['local_load'] = pd.Categorical(df_all['local_load'], categories=LOAD_ORDER, ordered=True)
    df_all['config_name'] = pd.Categorical(df_all['config_name'], categories=CONFIG_ORDER, ordered=True)

    df_stable = df_all[df_all['run'] > warmup_runs].copy()
    if not df_stable.empty:
        df_stable['run'] = df_stable['run'] - warmup_runs

    return df_all, df_stable


def get_workload_from_pod_name(pod_name: str) -> str:
    base_name = re.split(r'-(?=[a-z0-9]{5}$)', str(pod_name))[0]
    base_name = re.split(r'-(?=[a-z0-9]{9,10}-[a-z0-9]{5}$)', base_name)[0]
    return base_name


def _set_integer_run_ticks(grid, dataframe_with_run):
    for ax in grid.axes.flat:
        xmin, xmax = ax.get_xlim()
        runs = sorted(dataframe_with_run['run'].unique())
        ticks = [r for r in runs if xmin <= r <= xmax]
        if ticks:
            ax.set_xticks(ticks)
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{int(x)}"))


# --- PLOTTING LIBRARY (FOR SINGLE CONFIG ANALYSIS) ---

def plot_2_job_duration_bars(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: 2. Mean Job Duration Comparison")
    job_df = df[df['kind'] == 'Job'].copy()
    if job_df.empty: return
    job_df['duration_s'] = job_df['measured_ms'] / 1000.0

    g = sns.catplot(
        data=job_df, x='workload', y='duration_s', hue='wan_profile',
        col='local_load', kind='bar', errorbar='sd', capsize=0.1,
        aspect=1.3, height=5.5, palette="husl",
        col_order=job_df['local_load'].cat.categories,
    )
    g.set_axis_labels("Job Name", "Mean Duration (seconds)", fontsize=11, fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')
    if g.figure.legends: g.figure.legends[0].set_title("WAN Profile")
    for ax in g.axes.flat:
        ax.tick_params(axis='x', rotation=45)
        sns.despine(ax=ax)
    g.figure.suptitle(f"Mean Job Duration ({config_name})", fontsize=14, fontweight='bold', y=0.995)
    g.tight_layout()
    g.savefig(output_dir / "A2_mean_job_duration_bars.png", dpi=300)
    plt.close()


def plot_3_slo_pass_rate_heatmap(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: 3. SLO Pass Rate Reliability Heatmap")
    pass_rate = (df.groupby(['wan_profile', 'local_load', 'workload'], observed=True)['pass']
                 .value_counts(normalize=True).unstack(fill_value=0))
    pass_rate['pass_pct'] = pass_rate.get(True, 0) * 100
    heatmap_data = pass_rate.pivot_table(index='workload', columns=['local_load', 'wan_profile'],
                                         values='pass_pct', observed=True)
    if heatmap_data.empty: return

    fig, ax = plt.subplots(figsize=(14, 7))
    sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap="RdYlGn", linewidths=1,
                linecolor='white', cbar_kws={'label': 'SLO Pass Rate (%)'}, vmin=0, vmax=100, ax=ax)
    ax.set_title(f"System Reliability: SLO Pass Rate ({config_name})", fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel("Load & WAN Profile", fontsize=12, fontweight='semibold')
    ax.set_ylabel("Workload", fontsize=12, fontweight='semibold')
    plt.xticks(rotation=45, ha='right')
    plt.savefig(output_dir / "A3_slo_pass_rate_heatmap.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_5_performance_interaction(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: 5. Performance Interaction Plot")
    job_to_plot = 'build-job'
    plot_df = df[df['workload'] == job_to_plot].copy()
    if plot_df.empty: return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    fig, ax = plt.subplots(figsize=(11, 6.5))
    sns.pointplot(data=plot_df, x='wan_profile', y='duration_s', hue='local_load',
                  errorbar='sd', capsize=0.15, palette='Set2', ax=ax, order=WAN_ORDER,
                  hue_order=plot_df['local_load'].cat.categories)
    ax.set_title(f"Interaction: WAN & Load on '{job_to_plot}' ({config_name})", fontsize=13, fontweight='bold')
    ax.set_xlabel("WAN Profile", fontsize=11, fontweight='semibold')
    ax.set_ylabel("Mean Duration (s)", fontsize=11, fontweight='semibold')
    ax.legend(title="Local Load")
    plt.savefig(output_dir / "B2_performance_interaction_plot.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_6_slo_failure_magnitude(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: 6. SLO Failure Magnitude Analysis")
    plot_df = df[(df['pass'] == False) & df['measured_ms'].notna() & df['target_ms'].notna()].copy()
    if plot_df.empty: return
    plot_df['overshoot_pct'] = ((plot_df['measured_ms'] - plot_df['target_ms']) / plot_df['target_ms'] * 100)

    g = sns.catplot(data=plot_df, x='workload', y='overshoot_pct', hue='wan_profile',
                    col='local_load', kind='bar', errorbar='sd', capsize=0.08, height=5.5,
                    aspect=1.3, palette="OrRd",
                    col_order=plot_df['local_load'].cat.categories)
    g.set_axis_labels("Workload", "Deadline Overshoot (%)", fontsize=11, fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')
    g.legend.set_title("WAN Profile")
    for ax in g.axes.flat: ax.tick_params(axis='x', rotation=30)
    g.figure.suptitle(f"Magnitude of SLO Failures ({config_name})", fontsize=14, fontweight='bold', y=0.995)
    g.tight_layout()
    g.savefig(output_dir / "B3_slo_failure_magnitude.png", dpi=300)
    plt.close()


def plot_7_raw_data_variance(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: 7. Raw Data Point Variance Plot")
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

    ax.set_title(f"Runtime Variance of '{job_to_plot}' ({config_name})", fontsize=13, fontweight='bold')
    ax.set_xlabel("WAN Profile", fontsize=11, fontweight='semibold')
    ax.set_ylabel("Duration (s)", fontsize=11, fontweight='semibold')

    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(handles, labels, title='Local Load')

    plt.savefig(output_dir / "B4_raw_data_variance.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_8_placement_analysis(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: 8. Workload Placement Analysis")
    if df.empty: return

    mean_counts = df.groupby(['wan_profile', 'local_load', 'workload', 'node_type'],
                             observed=True).size().reset_index(name='count')
    if mean_counts.empty: return

    g = sns.catplot(
        data=mean_counts, x='workload', y='count', hue='node_type',
        col='wan_profile',
        row='local_load',
        kind='bar', height=4.5, aspect=1.6,
        palette={'cloud': '#4A90E2', 'edge': '#F5A623'}, legend=False,
        row_order=df['local_load'].cat.categories,
        col_order=df['wan_profile'].cat.categories,
    )
    g.set_axis_labels("Workload", "Mean Pod Count", fontsize=11, fontweight='semibold')

    g.set_titles(row_template="Load: {row_name}", col_template="WAN: {col_name}", fontsize=11)

    handles = [plt.Rectangle((0, 0), 1, 1, fc='#4A90E2'), plt.Rectangle((0, 0), 1, 1, fc='#F5A623')]
    g.figure.legend(handles, ['cloud', 'edge'], title='Node Type', loc='upper center', bbox_to_anchor=(0.5, -0.02), ncol=2)

    for ax in g.axes.flat:
        ax.tick_params(axis='x', rotation=45)
        sns.despine(ax=ax)

    g.figure.suptitle(f"Workload Placement ({config_name})", fontsize=14, fontweight='bold', y=0.995)
    g.tight_layout()
    g.savefig(output_dir / "C1_workload_placement_matrix.png", dpi=300)
    plt.close()


def plot_10_learning_performance_over_runs(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: 10. Learning Curve: Job Performance")
    workload_to_plot = 'cpu-batch'
    plot_df = df[df['workload'] == workload_to_plot].copy()
    if plot_df.empty: return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    g = sns.relplot(
        data=plot_df, x='run', y='duration_s', col='local_load', row='wan_profile',
        kind='line', marker='o', errorbar='sd', height=4, aspect=1.5,
        row_order=plot_df['wan_profile'].cat.categories,
        col_order=plot_df['local_load'].cat.categories,
    )
    g.set_axis_labels("Run Number", f"{workload_to_plot} Duration (s)", fontweight='semibold')
    g.set_titles(row_template="WAN: {row_name}", col_template="Load: {col_name}", fontweight='semibold')
    g.figure.suptitle(f"Scheduler Learning: '{workload_to_plot}' Performance ({config_name})", fontsize=14, fontweight='bold', y=0.995)
    _set_integer_run_ticks(g, plot_df)
    slo_target_ms = plot_df['target_ms'].max()
    if pd.notna(slo_target_ms):
        for ax in g.axes.flat:
            ax.axhline(slo_target_ms / 1000.0, ls='--', color='red', alpha=0.8, lw=1.5)
    g.tight_layout()
    g.savefig(output_dir / "D2_learning_curve_performance.png", dpi=300)
    plt.close()

def plot_11_learning_slo_improvement(df: pd.DataFrame, output_dir: Path, config_name: str):
    """(Graph 11, per-config) SLO pass rate evolution showing learning effect."""
    print("  - Generating: 11. SLO Improvement Over Runs")
    if df.empty: return

    pass_by_run = (df.groupby(['wan_profile', 'local_load', 'run'], observed=True)['pass']
                   .apply(lambda x: (x == True).sum() / len(x) * 100).reset_index(name='pass_rate'))

    g = sns.relplot(
        data=pass_by_run, x='run', y='pass_rate', hue='wan_profile',
        col='local_load', kind='line', marker='o', markersize=6, linewidth=2,
        height=4.5, aspect=1.4, palette='magma',
        col_order=pass_by_run['local_load'].cat.categories,
    )
    g.set_axis_labels("Run Number", "SLO Pass Rate (%)", fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')
    _set_integer_run_ticks(g, pass_by_run)
    for ax in g.axes.flat:
        ax.set_ylim(-5, 105)
        ax.axhline(90, ls='--', color='green', alpha=0.6, lw=1)
    g.figure.suptitle(f"Reliability Improvement Over Runs ({config_name})", fontsize=14, fontweight='bold', y=0.995)
    g.tight_layout()
    g.savefig(output_dir / "D3_slo_improvement_over_runs.png", dpi=300)
    plt.close()


def plot_12_placement_performance_correlation(df_slo: pd.DataFrame, df_placement: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: 12. Placement-Performance Correlation")
    if df_slo.empty or df_placement.empty: return

    dedup_cols = ['wan_profile', 'local_load', 'run', 'workload', 'pod_name', 'node_name', 'node_type']
    df_p = df_placement.drop_duplicates(subset=[c for c in dedup_cols if c in df_placement.columns]).copy()
    placement_summary = (df_p.groupby(['wan_profile', 'local_load', 'run', 'workload', 'node_type'], observed=True)
                         .size().reset_index(name='count'))
    pivot = placement_summary.pivot_table(index=['wan_profile', 'local_load', 'run', 'workload'],
                                          columns='node_type', values='count', fill_value=0,
                                          observed=True).reset_index()
    for col in ['edge', 'cloud']:
        if col not in pivot.columns: pivot[col] = 0
    pivot['total'] = pivot['edge'] + pivot['cloud']
    pivot = pivot[pivot['total'] > 0].copy()
    pivot['edge_pct'] = pivot['edge'] / pivot['total'] * 100

    slo_summary = (df_slo.groupby(['wan_profile', 'local_load', 'run', 'workload'], observed=True)['pass']
                   .apply(lambda x: (x == True).sum() / len(x) * 100).reset_index(name='slo_pass_rate'))
    merged = pd.merge(pivot, slo_summary, on=['wan_profile', 'local_load', 'run', 'workload'], how='inner')
    if merged.empty: return

    g = sns.lmplot(
        data=merged, x='edge_pct', y='slo_pass_rate',
        hue='wan_profile',
        col='local_load',
        height=5, aspect=1.3,
        scatter_kws={'alpha': 0.6, 's': 50},
        line_kws={'linewidth': 2},
        palette='viridis',
        col_order=merged['local_load'].cat.categories
    )
    g.set_axis_labels("Edge Placement (%)", "SLO Pass Rate (%)", fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')
    for ax in g.axes.flat:
        ax.axhline(90, ls='--', color='green', alpha=0.4, lw=1)
        ax.axvline(50, ls='--', color='gray', alpha=0.4, lw=1)
    g.figure.suptitle(f"Correlation: Edge Placement vs. SLO Pass Rate ({config_name})", fontsize=14, fontweight='bold', y=1.02)
    g.savefig(output_dir / "E1_placement_performance_correlation.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_13_workload_preference_heatmap(df: pd.DataFrame, output_dir: Path, config_name: str):
    print("  - Generating: 13. Workload Preference Heatmap")
    if df.empty: return

    dedup_cols = ['wan_profile', 'local_load', 'run', 'workload', 'pod_name', 'node_name', 'node_type']
    df_clean = df.drop_duplicates(subset=[c for c in dedup_cols if c in df.columns]).copy()
    counts = (df_clean.groupby(['wan_profile', 'local_load', 'workload', 'node_type'], observed=True)
              .size().reset_index(name='count'))
    pivot = counts.pivot_table(index=['wan_profile', 'local_load', 'workload'], columns='node_type',
                               values='count', fill_value=0, observed=True).reset_index()
    for col in ['edge', 'cloud']:
        if col not in pivot.columns: pivot[col] = 0
    pivot['total'] = pivot['edge'] + pivot['cloud']
    pivot = pivot[pivot['total'] > 0].copy()
    pivot['edge_pct'] = pivot['edge'] / pivot['total'] * 100
    pivot['condition'] = pivot['wan_profile'].astype(str) + ' / ' + pivot['local_load'].astype(str)
    heatmap_data = pivot.pivot_table(index='workload', columns='condition', values='edge_pct', observed=True)
    if heatmap_data.empty: return

    fig, ax = plt.subplots(figsize=(16, 8))
    sns.heatmap(heatmap_data, annot=True, fmt=".0f", cmap="RdYlBu_r", center=50, linewidths=1,
                linecolor='white', cbar_kws={'label': 'Edge Placement (%)'}, vmin=0, vmax=100, ax=ax)
    ax.set_title(f"Workload Preference: Edge Placement % ({config_name})", fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel("WAN / Load Profile", fontsize=12, fontweight='semibold')
    ax.set_ylabel("Workload", fontsize=12, fontweight='semibold')
    plt.xticks(rotation=45, ha='right')
    plt.savefig(output_dir / "E2_workload_preference_heatmap.png", dpi=300, bbox_inches="tight")
    plt.close()


# --- COMPARATIVE PLOTS ---

def plot_compare_overall_slo_rate(df: pd.DataFrame, output_dir: Path):
    print("  - Generating: COMP_1. Overall SLO Pass Rate Comparison")
    if df.empty: return

    slo_rate = df.groupby(['config_name', 'wan_profile', 'local_load'], observed=True)['pass'].value_counts(normalize=True).unstack(fill_value=0)
    slo_rate['pass_pct'] = slo_rate.get(True, 0) * 100

    g = sns.catplot(
        data=slo_rate.reset_index(),
        x='config_name',
        y='pass_pct',
        hue='local_load',
        col='wan_profile',
        kind='bar',
        height=5,
        aspect=1,
        palette='YlGnBu',
        col_order=df['wan_profile'].cat.categories,
        hue_order=df['local_load'].cat.categories
    )
    g.set_axis_labels("Scheduler Configuration", "Mean SLO Pass Rate (%)")
    g.set_titles("WAN Profile: {col_name}")
    g.figure.suptitle("Comparative Reliability: Overall SLO Pass Rate", fontsize=14, fontweight='bold', y=1.02)
    for ax in g.axes.flat:
        ax.axhline(90, ls='--', color='green', alpha=0.7)
        ax.set_ylim(0, 105)
    g.tight_layout()
    g.savefig(output_dir / "COMP_1_overall_slo_pass_rate.png", dpi=300)
    plt.close()


def plot_compare_key_job_performance(df: pd.DataFrame, output_dir: Path, workload: str):
    print(f"  - Generating: COMP_2. Key Job Performance Comparison for '{workload}'")
    plot_df = df[df['workload'] == workload].copy()
    if plot_df.empty: return
    plot_df['duration_s'] = plot_df['measured_ms'] / 1000.0

    g = sns.relplot(
        data=plot_df,
        x='wan_profile',
        y='duration_s',
        hue='config_name',
        style='config_name',
        col='local_load',
        kind='line',
        marker='o',
        errorbar='sd',
        height=5,
        aspect=1.2,
        palette='viridis',
        col_order=plot_df['local_load'].cat.categories,
    )
    g.set_axis_labels("WAN Profile", f"Mean '{workload}' Duration (s)")
    g.set_titles("Local Load: {col_name}")
    g.figure.suptitle(f"Comparative Performance: '{workload}' Runtime", fontsize=14, fontweight='bold', y=1.02)
    g.tight_layout()
    g.savefig(output_dir / f"COMP_2_performance_{workload}.png", dpi=300)
    plt.close()


def plot_compare_placement_stability(df: pd.DataFrame, output_dir: Path):
    print("  - Generating: COMP_3. Placement Stability Comparison")
    if df.empty: return

    dedup_cols = ['config_name', 'wan_profile', 'local_load', 'run', 'workload', 'pod_name', 'node_name', 'node_type']
    df_clean = df.drop_duplicates(subset=[c for c in dedup_cols if c in df.columns]).copy()

    majority_placement = (df_clean.groupby(['config_name', 'wan_profile', 'local_load', 'run', 'workload', 'node_type'], observed=True)
                          .size().reset_index(name='count'))
    idx = majority_placement.groupby(['config_name', 'wan_profile', 'local_load', 'run', 'workload'], observed=True)['count'].idxmax()
    majority = majority_placement.loc[idx, ['config_name', 'wan_profile', 'local_load', 'run', 'workload', 'node_type']].copy()
    majority = majority.sort_values(['config_name', 'wan_profile', 'local_load', 'workload', 'run'])

    majority['prev_node_type'] = majority.groupby(['config_name', 'wan_profile', 'local_load', 'workload'], observed=True)['node_type'].shift(1)
    majority['switched'] = (majority['node_type'] != majority['prev_node_type']) & majority['prev_node_type'].notna()

    switches = majority.groupby(['config_name', 'wan_profile', 'local_load'], observed=True)['switched'].sum().reset_index(name='total_switches')

    g = sns.catplot(
        data=switches,
        x='config_name',
        y='total_switches',
        col='wan_profile',
        hue='local_load',
        kind='bar',
        height=5,
        aspect=1,
        palette='YlOrRd',
        col_order=switches['wan_profile'].cat.categories
    )
    g.set_axis_labels("Scheduler Configuration", "Total Placement Switches (Lower is Better)")
    g.set_titles("WAN Profile: {col_name}")
    g.figure.suptitle("Comparative Stability: Placement Decisions", fontsize=14, fontweight='bold', y=1.02)
    g.tight_layout()
    g.savefig(output_dir / "COMP_3_placement_stability.png", dpi=300)
    plt.close()


def plot_9b_overview_placement_trends(df: pd.DataFrame, output_dir: Path):
    print("  - Generating: COMP_4. System-Wide Placement Trends")
    if df.empty or 'node_type' not in df.columns: return

    dedup_cols = ['config_name', 'wan_profile', 'local_load', 'run', 'workload', 'pod_name', 'node_name', 'node_type']
    df_clean = df.drop_duplicates(subset=[c for c in dedup_cols if c in df.columns]).copy()

    group_cols = ['config_name', 'wan_profile', 'local_load', 'run']
    counts = (df_clean.groupby(group_cols + ['node_type'], observed=True).size().reset_index(name='count'))
    pivot = (counts.pivot_table(index=group_cols, columns='node_type', values='count', aggfunc='sum', fill_value=0, observed=True).reset_index())
    for col in ['edge', 'cloud']:
        if col not in pivot.columns: pivot[col] = 0
    pivot['total'] = pivot['edge'] + pivot['cloud']
    pivot = pivot[pivot['total'] > 0].copy()
    pivot['edge_pct'] = pivot['edge'] / pivot['total'] * 100

    if pivot.empty: return

    g = sns.relplot(
        data=pivot, x='run', y='edge_pct', hue='config_name', style='wan_profile',
        col='local_load', kind='line', marker='o', markersize=6, linewidth=2.5,
        height=4.5, aspect=1.4, palette='viridis', legend='full',
        col_order=pivot['local_load'].cat.categories,
    )
    g.set_axis_labels("Run Number", "Edge Placement (%)", fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')
    _set_integer_run_ticks(g, pivot)
    for ax in g.axes.flat:
        ax.set_ylim(-5, 105)
        ax.axhline(50, ls='--', color='gray', alpha=0.6, lw=1)
    g.figure.suptitle("Comparative Placement Strategy: Edge Share Over Runs", fontsize=14, fontweight='bold', y=0.995)
    g.tight_layout()
    g.savefig(output_dir / "COMP_4_overview_placement_trend.png", dpi=300)
    plt.close()


def plot_compare_slo_improvement_over_runs(df: pd.DataFrame, output_dir: Path):
    print("  - Generating: COMP_5. SLO Improvement Over Runs")
    if df.empty: return

    pass_by_run = (df.groupby(['config_name', 'wan_profile', 'local_load', 'run'], observed=True)['pass']
                   .apply(lambda x: (x == True).sum() / len(x) * 100).reset_index(name='pass_rate'))

    g = sns.relplot(
        data=pass_by_run, x='run', y='pass_rate', hue='config_name', style='wan_profile',
        col='local_load', kind='line', marker='o', markersize=6, linewidth=2,
        height=4.5, aspect=1.4, palette='magma',
        col_order=pass_by_run['local_load'].cat.categories,
    )
    g.set_axis_labels("Run Number", "SLO Pass Rate (%)", fontweight='semibold')
    g.set_titles('Load: {col_name}', fontsize=12, fontweight='semibold')
    _set_integer_run_ticks(g, pass_by_run)
    for ax in g.axes.flat:
        ax.set_ylim(-5, 105)
        ax.axhline(90, ls='--', color='green', alpha=0.6, lw=1)
    g.figure.suptitle("Comparative Learning: Reliability Improvement Over Runs", fontsize=14, fontweight='bold', y=0.995)
    g.tight_layout()
    g.savefig(output_dir / "COMP_5_slo_improvement_over_runs.png", dpi=300)
    plt.close()


# --- MAIN EXECUTION LOGIC ---

def main():
    parser = argparse.ArgumentParser(description="Generate visualizations for SLO benchmarks.")
    parser.add_argument("results_dir", type=Path, help="Path to the multi-run results directory.")
    parser.add_argument("--warmup-runs", type=int, default=2, help="Initial runs to discard for stable analysis.")
    args = parser.parse_args()

    results_dir = args.results_dir
    output_dir = results_dir / "_analysis_plots"
    output_dir.mkdir(exist_ok=True)

    setup_theme()

    df_all_slo, df_stable_slo = load_all_run_data(results_dir, args.warmup_runs)
    df_all_placement, df_stable_placement = load_placement_data(results_dir, args.warmup_runs)

    df_all_slo.to_csv(results_dir / "aggregated_slo_data_ALL.csv", index=False)
    df_stable_slo.to_csv(results_dir / "aggregated_slo_data_STABLE.csv", index=False)
    df_all_placement.to_csv(results_dir / "aggregated_placement_data_ALL.csv", index=False)
    df_stable_placement.to_csv(results_dir / "aggregated_placement_data_STABLE.csv", index=False)
    print(f"\n✅ Aggregated data for ALL and STABLE runs saved.")

    # --- Generate Per-Configuration Plots ---
    for config_name in df_stable_slo['config_name'].cat.categories:
        print(f"\n--- Generating detailed report for configuration: {config_name} ---")
        config_output_dir = output_dir / f"config-{config_name}"
        config_output_dir.mkdir(exist_ok=True)

        # Filter data for the current configuration
        df_slo_config_stable = df_stable_slo[df_stable_slo['config_name'] == config_name].copy()
        df_placement_config_stable = df_stable_placement[df_stable_placement['config_name'] == config_name].copy()
        df_slo_config_all = df_all_slo[df_all_slo['config_name'] == config_name].copy()
        df_placement_config_all = df_all_placement[df_all_placement['config_name'] == config_name].copy()

        for df in [df_slo_config_stable, df_placement_config_stable, df_slo_config_all, df_placement_config_all]:
            for col in ['local_load', 'wan_profile', 'config_name']:
                if col in df.columns:
                    df[col] = df[col].cat.remove_unused_categories()

        plot_2_job_duration_bars(df_slo_config_stable, config_output_dir, config_name)
        plot_3_slo_pass_rate_heatmap(df_slo_config_stable, config_output_dir, config_name)
        plot_5_performance_interaction(df_slo_config_stable, config_output_dir, config_name)
        plot_6_slo_failure_magnitude(df_slo_config_stable, config_output_dir, config_name)
        plot_7_raw_data_variance(df_slo_config_stable, config_output_dir, config_name)
        plot_8_placement_analysis(df_placement_config_stable, config_output_dir, config_name)
        plot_10_learning_performance_over_runs(df_slo_config_all, config_output_dir, config_name)
        plot_11_learning_slo_improvement(df_slo_config_all, config_output_dir, config_name)
        plot_12_placement_performance_correlation(df_slo_config_all, df_placement_config_all, config_output_dir, config_name)
        plot_13_workload_preference_heatmap(df_stable_placement, config_output_dir, config_name)

    # --- Generate Comparative Plots ---
    print("\n--- Generating Comparative Plots Across All Configurations ---")
    comparative_output_dir = output_dir / "_comparative_plots"
    comparative_output_dir.mkdir(exist_ok=True)

    plot_compare_overall_slo_rate(df_stable_slo, comparative_output_dir)
    plot_compare_key_job_performance(df_stable_slo, comparative_output_dir, 'build-job')
    plot_compare_key_job_performance(df_stable_slo, comparative_output_dir, 'cpu-batch')
    plot_compare_placement_stability(df_all_placement, comparative_output_dir)
    plot_9b_overview_placement_trends(df_all_placement, comparative_output_dir)
    plot_compare_slo_improvement_over_runs(df_all_slo, comparative_output_dir)

    print("\n✅ Visualization complete!")
    print(f"All graphs saved in: {output_dir}")


if __name__ == "__main__":
    main()