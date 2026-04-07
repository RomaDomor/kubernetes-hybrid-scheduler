#!/bin/bash
set -euo pipefail

# --- User Configuration ---

# The four scheduler strategies under comparison:
#   smart        - Our hybrid Lyapunov scheduler (the algorithm under test)
#   single-cluster - No smart scheduler; default k8s places pods across all
#                    Liqo virtual nodes treating the federation as one cluster.
#                    This simulates WAN-unaware "big cluster" scheduling.
#   liqo-native  - No smart scheduler; Liqo's default namespace-level offloading
#                    decides placement without SLO awareness.
#   round-robin  - No smart scheduler; benchmark Python script forces placements
#                    to cycle deterministically: edge → fog → cloud → edge …
SCHEDULER_MODES=("smart" "single-cluster" "liqo-native" "round-robin")

# Helm args used when deploying the smart scheduler (tuned-aggressive).
# Only applied for the "smart" mode; the others uninstall the webhook.
# costFactors format: "local=0,<cluster-id>=<cost>" matching REMOTE_ENDPOINTS IDs.
SMART_HELM_PARAMS="--set config.costFactors='local=0,cloud-1=1.0,fog-1=0.5' --set config.lyapunovBeta='0.8'"

# Remote cluster endpoints passed to the controller: "cloud-1=<ip>,cloud-2=<ip>"
# These IDs must match the node.cluster/id labels on the virtual nodes.
REMOTE_ENDPOINTS="cloud-1=10.20.0.3,fog-1=10.30.0.3"

# Node label key on virtual nodes used by round-robin nodeSelector.
# Must match the node.cluster/id label key set by the controller on virtual nodes.
RR_NODE_LABEL_KEY="node.cluster/id"

# Ordered list of cluster IDs for round-robin cycling.
# Must match the actual node.cluster/id label values on virtual nodes.
RR_CLUSTERS="edge,cloud-1,fog-1"

# Default benchmark parameters (can be overridden by command-line flags)
DEFAULT_NAMESPACE="offload"
DEFAULT_LOCAL_NAMESPACE="local"
DEFAULT_MANIFESTS_DIR="./manifests/workloads"
DEFAULT_RESULTS_ROOT="./results"
DEFAULT_KUBECONFIG="$HOME/.kube/config"
DEFAULT_WAN_ROUTER="router"
DEFAULT_PROFILES="clear,good,moderate,poor"
DEFAULT_LOCAL_LOAD_PROFILES="none,low,medium"
DEFAULT_VENV_PATH="scripts/bench-suite/.venv"
DEFAULT_HELM_CHART="./deployments/smart-scheduler"
DEFAULT_PRE_CLEAN=true
DEFAULT_RUNS=15
DEFAULT_IMAGE_TAG="latest"
# --- End of User Configuration ---


# Function to show usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run multi-scheduler Kubernetes SLO benchmarks against four scheduling strategies:
  smart          - Hybrid Lyapunov scheduler (the algorithm under test)
  single-cluster - Default k8s across all Liqo virtual nodes (WAN-unaware)
  liqo-native    - Liqo namespace-level offloading without SLO awareness
  round-robin    - Deterministic edge→fog→cloud rotation

OPTIONS:
    -n, --namespace NAMESPACE       Kubernetes namespace for offloaded workloads (default: ${DEFAULT_NAMESPACE})
        --local-namespace NS        Namespace for local clients (default: ${DEFAULT_LOCAL_NAMESPACE})
    -m, --manifests-dir DIR         Directory containing workload manifests (default: ${DEFAULT_MANIFESTS_DIR})
    -r, --results-root DIR          Root directory for results (default: ${DEFAULT_RESULTS_ROOT})
    -k, --kubeconfig PATH           Path to kubeconfig file (default: ${DEFAULT_KUBECONFIG})
    -w, --wan-router ROUTER         WAN router SSH target (default: ${DEFAULT_WAN_ROUTER})
    -p, --profiles PROFILES         Comma-separated WAN profiles to test (default: ${DEFAULT_PROFILES})
    -l, --local-load-profiles LOAD  Comma-separated local CPU load levels (default: ${DEFAULT_LOCAL_LOAD_PROFILES})
        --modes MODES               Comma-separated scheduler modes to run (default: all four)
        --runs N                    Runs per combination (default: ${DEFAULT_RUNS})
        --image-tag TAG             Docker image tag for the smart scheduler (default: ${DEFAULT_IMAGE_TAG})
    -v, --venv-path PATH            Python virtual environment path (default: ${DEFAULT_VENV_PATH})
        --helm-chart PATH           Path or repo/chart for smart-scheduler Helm chart (default: ${DEFAULT_HELM_CHART})
        --remote-endpoints CSV      Remote cluster endpoints: "cloud-1=ip,cloud-2=ip" (default: ${REMOTE_ENDPOINTS})
    -s, --sleep SECONDS             Sleep between runs (default: 30)
    -t, --timeout SECONDS           Per-job timeout in seconds (default: 600)
        --pre-clean                 Clean namespaces before each run
        --no-controller-metrics     Disable controller metrics collection
        --dry-run                   Print commands without executing
    -h, --help                      Show this help

EXAMPLES:
    # Full comparison with 5 runs per combination
    $0 --runs 5 -p "clear,moderate,poor" -l "none,medium"

    # Dry run to inspect the execution plan
    $0 --dry-run --runs 2 --modes "smart,round-robin"
EOF
}

# --- Script Logic ---

NAMESPACE="${DEFAULT_NAMESPACE}"
LOCAL_NAMESPACE="${DEFAULT_LOCAL_NAMESPACE}"
MANIFESTS_DIR="${DEFAULT_MANIFESTS_DIR}"
RESULTS_ROOT="${DEFAULT_RESULTS_ROOT}"
KUBECONFIG="${DEFAULT_KUBECONFIG}"
WAN_ROUTER="${DEFAULT_WAN_ROUTER}"
PROFILES_STR="${DEFAULT_PROFILES}"
LOCAL_LOAD_PROFILES_STR="${DEFAULT_LOCAL_LOAD_PROFILES}"
MODES_STR="${SCHEDULER_MODES[*]}"  # all modes by default
VENV_PATH="${DEFAULT_VENV_PATH}"
HELM_CHART="${DEFAULT_HELM_CHART}"
SLEEP_BETWEEN=30
JOB_TIMEOUT=600
PRE_CLEAN="${DEFAULT_PRE_CLEAN}"
DRY_RUN=false
RUNS="${DEFAULT_RUNS}"
COLLECT_CONTROLLER_METRICS=true
IMAGE_TAG="${DEFAULT_IMAGE_TAG}"

while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--namespace)            NAMESPACE="$2";               shift 2 ;;
        --local-namespace)         LOCAL_NAMESPACE="$2";          shift 2 ;;
        -m|--manifests-dir)        MANIFESTS_DIR="$2";            shift 2 ;;
        -r|--results-root)         RESULTS_ROOT="$2";             shift 2 ;;
        -k|--kubeconfig)           KUBECONFIG="$2";               shift 2 ;;
        -w|--wan-router)           WAN_ROUTER="$2";               shift 2 ;;
        -p|--profiles)             PROFILES_STR="$2";             shift 2 ;;
        -l|--local-load-profiles)  LOCAL_LOAD_PROFILES_STR="$2";  shift 2 ;;
        --modes)                   MODES_STR="$2";                shift 2 ;;
        --runs)                    RUNS="$2";                     shift 2 ;;
        --image-tag)               IMAGE_TAG="$2";                shift 2 ;;
        -v|--venv-path)            VENV_PATH="$2";                shift 2 ;;
        --helm-chart)              HELM_CHART="$2";               shift 2 ;;
        --remote-endpoints)        REMOTE_ENDPOINTS="$2";         shift 2 ;;
        -s|--sleep)                SLEEP_BETWEEN="$2";            shift 2 ;;
        -t|--timeout)              JOB_TIMEOUT="$2";              shift 2 ;;
        --pre-clean)               PRE_CLEAN=true;                shift ;;
        --dry-run)                 DRY_RUN=true;                  shift ;;
        --no-controller-metrics)   COLLECT_CONTROLLER_METRICS=false; shift ;;
        -h|--help)                 usage; exit 0 ;;
        *) echo "Unknown option $1"; usage; exit 1 ;;
    esac
done

# Convert comma-separated strings to arrays
IFS=',' read -ra PROFILES <<< "${PROFILES_STR}"
IFS=',' read -ra LOCAL_LOAD_PROFILES <<< "${LOCAL_LOAD_PROFILES_STR}"
IFS=' ' read -ra ACTIVE_MODES <<< "${MODES_STR}"
# Handle comma-separated modes too
if [[ "${MODES_STR}" == *","* ]]; then
    IFS=',' read -ra ACTIVE_MODES <<< "${MODES_STR}"
fi

# Validate inputs
valid_loads=("none" "low" "medium" "high")
for load in "${LOCAL_LOAD_PROFILES[@]}"; do
    if [[ ! " ${valid_loads[*]} " =~ " ${load} " ]]; then
        echo "Error: Invalid local load profile '${load}'. Must be one of: ${valid_loads[*]}"
        exit 1
    fi
done

valid_modes=("smart" "single-cluster" "liqo-native" "round-robin")
for mode in "${ACTIVE_MODES[@]}"; do
    if [[ ! " ${valid_modes[*]} " =~ " ${mode} " ]]; then
        echo "Error: Invalid scheduler mode '${mode}'. Must be one of: ${valid_modes[*]}"
        exit 1
    fi
done

if [[ ! -d "${MANIFESTS_DIR}" ]]; then
    echo "Error: Manifests directory '${MANIFESTS_DIR}' does not exist"
    exit 1
fi

if [[ -f "${VENV_PATH}/bin/activate" ]]; then
    # shellcheck disable=SC1090
    source "${VENV_PATH}/bin/activate"
else
    echo "Warning: Virtual environment not found at ${VENV_PATH}, continuing without activation"
fi

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BASE_RESULTS="${RESULTS_ROOT}/multi-run-${TIMESTAMP}"

echo "========================================="
echo "Scheduler Comparison Benchmark"
echo "========================================="
echo "Namespace (offloaded): ${NAMESPACE}"
echo "Local Namespace:       ${LOCAL_NAMESPACE}"
echo "Manifests Dir:         ${MANIFESTS_DIR}"
echo "Results Root:          ${BASE_RESULTS}"
echo "Kubeconfig:            ${KUBECONFIG}"
echo "WAN Router:            ${WAN_ROUTER}"
echo "WAN Profiles (inter-cluster link): ${PROFILES[*]}"
echo "Local Load Profiles:   ${LOCAL_LOAD_PROFILES[*]}"
echo "Scheduler Modes:       ${ACTIVE_MODES[*]}"
echo "Runs per combination:  ${RUNS}"
echo "Sleep Between:         ${SLEEP_BETWEEN}s"
echo "Job Timeout:           ${JOB_TIMEOUT}s"
echo "Pre-Clean (pre-run):   ${PRE_CLEAN}"
echo "Collect Ctrl Metrics:  ${COLLECT_CONTROLLER_METRICS}"
echo "Dry Run:               ${DRY_RUN}"
echo "Scheduler Image Tag:   ${IMAGE_TAG}"
echo "Helm Chart:            ${HELM_CHART}"
echo "Remote Endpoints:      ${REMOTE_ENDPOINTS}"
echo "RR Node Label:         ${RR_NODE_LABEL_KEY}"
echo "RR Clusters:           ${RR_CLUSTERS}"
echo "========================================="
echo ""
echo "Execution Matrix:"
echo "-----------------"
total_combinations=$(( ${#ACTIVE_MODES[@]} * ${#PROFILES[@]} * ${#LOCAL_LOAD_PROFILES[@]} ))
for mode in "${ACTIVE_MODES[@]}"; do
    for wan_profile in "${PROFILES[@]}"; do
        for local_load in "${LOCAL_LOAD_PROFILES[@]}"; do
            echo "  - Scheduler: ${mode}, WAN: ${wan_profile}, Load: ${local_load}"
        done
    done
done
echo "  Total combinations: ${total_combinations} × ${RUNS} runs = $((total_combinations * RUNS)) benchmark invocations"
echo "========================================="

if [[ "${DRY_RUN}" == "true" ]]; then
    echo "DRY RUN MODE - Commands that would be executed:"
    echo ""
fi

# ---------------------------------------------------------------------------
# Helper: wait for a namespace to drain of all workload resources
# ---------------------------------------------------------------------------
wait_ns_empty() {
  local ns="$1" kubeconfig="$2"
  local timeout="${3:-300}" interval="${4:-3}"
  local end=$((SECONDS + timeout))
  local kinds=(pods deployments.apps replicasets.apps statefulsets.apps
               daemonsets.apps jobs.batch cronjobs.batch services
               persistentvolumeclaims)

  echo "⏳ Waiting for namespace '${ns}' to be empty (timeout ${timeout}s)..."
  while (( SECONDS < end )); do
    local remaining=0
    for kind in "${kinds[@]}"; do
      local cnt
      cnt=$(kubectl get "$kind" -n "$ns" --kubeconfig "$kubeconfig" -o name 2>/dev/null | wc -l | tr -d ' ')
      [[ "$cnt" =~ ^[0-9]+$ ]] || cnt=0
      remaining=$((remaining + cnt))
    done
    (( remaining == 0 )) && { echo "✅ Namespace '${ns}' is empty."; return 0; }
    sleep "$interval"
  done
  echo "⚠️ Timeout waiting for namespace '${ns}' to be empty."
  return 1
}

# ---------------------------------------------------------------------------
# Helper: delete all resources in a namespace and wait
# ---------------------------------------------------------------------------
cleanup_namespace() {
  local ns="$1" kubeconfig_path="$2" wait_timeout="${3:-300}"
  echo "🧹 Pre-cleaning namespace '${ns}'..."
  set +e
  kubectl delete cronjobs.batch  --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --cascade=foreground
  kubectl delete jobs.batch      --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --cascade=foreground
  kubectl delete deployments.apps --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --cascade=foreground
  kubectl delete statefulsets.apps --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --cascade=foreground
  kubectl delete daemonsets.apps   --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --cascade=foreground
  kubectl delete pods        --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found
  kubectl delete services    --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found
  kubectl delete configmaps  --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found
  kubectl delete secrets     --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found
  kubectl delete persistentvolumeclaims --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found
  set -e
  wait_ns_empty "${ns}" "${kubeconfig_path}" "${wait_timeout}" 3 || true
}

# ---------------------------------------------------------------------------
# Helper: configure the smart-scheduler for a given mode
#   smart        → uninstall + reinstall with tuned params
#   single-cluster, liqo-native, round-robin → uninstall only (no webhook)
# ---------------------------------------------------------------------------
configure_scheduler_for_mode() {
  local mode="$1" kubeconfig_path="$2" image_tag="$3"

  echo "🧠 Configuring scheduler for mode '${mode}'..."

  # Always uninstall any previous release first
  echo "  - Uninstalling Helm release 'smart-scheduler' (if present)..."
  if ! helm uninstall smart-scheduler -n kube-system \
        --kubeconfig "${kubeconfig_path}" --wait --ignore-not-found 2>/dev/null; then
    echo "  - Uninstall returned non-zero, continuing..."
  fi
  echo "  - Deleting WorkloadProfile CRD to ensure clean state..."
  kubectl delete crd workloadprofiles.scheduling.hybrid.io \
    --kubeconfig "${kubeconfig_path}" --ignore-not-found 2>/dev/null || true

  if [[ "${mode}" == "smart" ]]; then
    local helm_cmd="helm install smart-scheduler '${HELM_CHART}' \
      --set image.tag='${image_tag}' \
      --set logLevel=5 \
      --set config.remoteEndpoints='${REMOTE_ENDPOINTS}' \
      -n kube-system \
      --kubeconfig '${kubeconfig_path}' \
      ${SMART_HELM_PARAMS} \
      --wait"

    if [[ "${DRY_RUN}" == "true" ]]; then
      echo "  - Would run: ${helm_cmd}"
    else
      echo "  - Installing smart-scheduler with tuned-aggressive params..."
      eval "${helm_cmd}"
      echo "✅ Smart scheduler installed."
    fi
  else
    echo "  - Mode '${mode}': webhook left uninstalled (default k8s + Liqo scheduling)."
  fi
}

# ---------------------------------------------------------------------------
# Helper: dump WorkloadProfile CRDs (only meaningful for smart mode)
# ---------------------------------------------------------------------------
dump_crd_profiles() {
  local kubeconfig_path="$1" output_dir="$2"
  local output_file="${output_dir}/final_workload_profiles.yaml"
  echo "💾 Dumping WorkloadProfile CRDs to ${output_file}..."
  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "  - Would run: kubectl get workloadprofiles ... > ${output_file}"
  else
    mkdir -p "${output_dir}"
    kubectl get workloadprofiles.scheduling.hybrid.io \
      -n kube-system --kubeconfig "${kubeconfig_path}" -o yaml \
      > "${output_file}" 2>/dev/null || true
    echo "  - Dump complete."
  fi
}

# ---------------------------------------------------------------------------
# Main benchmark loop
# ---------------------------------------------------------------------------
current_combination=0

for mode in "${ACTIVE_MODES[@]}"; do
  for wan_profile in "${PROFILES[@]}"; do
    for local_load in "${LOCAL_LOAD_PROFILES[@]}"; do
      current_combination=$((current_combination + 1))
      echo "========================================="
      echo "[$current_combination/$total_combinations] Starting combination"
      echo "  Scheduler Mode:     ${mode}"
      echo "  WAN Profile:        ${wan_profile}"
      echo "  Local Load Profile: ${local_load}"
      echo "  Runs to perform:    ${RUNS}"
      echo "========================================="

      if [[ "${DRY_RUN}" != "true" ]]; then
        echo "  - Applying WAN profile '${wan_profile}'..."
        ssh "${WAN_ROUTER}" "sudo -n /usr/local/sbin/wan/apply_wan.sh '${wan_profile}'"

        configure_scheduler_for_mode "${mode}" "${KUBECONFIG}" "${IMAGE_TAG}"

        echo "  - Sleeping 15s after scheduler reset..."
        sleep 15
      else
        echo "  - Would apply WAN '${wan_profile}' and configure mode '${mode}'."
      fi

      PROFILE_RESULTS_BASE="${BASE_RESULTS}/scheduler-${mode}_wan-${wan_profile}_load-${local_load}"

      for run_num in $(seq 1 "${RUNS}"); do
        if [[ "${PRE_CLEAN}" == "true" && "${DRY_RUN}" != "true" ]]; then
          cleanup_namespace "${NAMESPACE}" "${KUBECONFIG}"
          cleanup_namespace "${LOCAL_NAMESPACE}" "${KUBECONFIG}"
          echo "Sleeping 10s after cleanup..."
          sleep 10
        elif [[ "${PRE_CLEAN}" == "true" && "${DRY_RUN}" == "true" ]]; then
          echo "  - Would pre-clean namespaces '${NAMESPACE}' and '${LOCAL_NAMESPACE}'"
        fi

        echo "-----------------------------------------"
        echo "  Run [$run_num/$RUNS] — mode='${mode}'"
        echo "-----------------------------------------"

        RUN_RESULTS_DIR="${PROFILE_RESULTS_BASE}/run-${run_num}"

        cmd=(
          python3 scripts/bench-suite/app.py
          --namespace          "${NAMESPACE}"
          --local-namespace    "${LOCAL_NAMESPACE}"
          --manifests-dir      "${MANIFESTS_DIR}"
          --results-root       "${RUN_RESULTS_DIR}"
          --kubeconfig         "${KUBECONFIG}"
          --wan-router         "${WAN_ROUTER}"
          --wan-profile        "${wan_profile}"
          --local-load-profile "${local_load}"
          --timeout-job-sec    "${JOB_TIMEOUT}"
          --scheduler-mode     "${mode}"
          --rr-node-label-key  "${RR_NODE_LABEL_KEY}"
          --rr-clusters        "${RR_CLUSTERS}"
        )

        if [[ "${COLLECT_CONTROLLER_METRICS}" == "false" ]]; then
          cmd+=(--no-controller-metrics)
        fi

        if [[ "${DRY_RUN}" == "true" ]]; then
          echo "Would run: ${cmd[*]}"
        else
          echo "Executing: ${cmd[*]}"
          "${cmd[@]}"
          echo "✅ Completed run [$run_num/$RUNS]"
          echo "📁 Results saved to: ${RUN_RESULTS_DIR}"

          is_last_run_of_all=$(( current_combination == total_combinations && run_num == RUNS ))
          if [[ $is_last_run_of_all -eq 0 && $SLEEP_BETWEEN -gt 0 ]]; then
            echo "⏳ Waiting ${SLEEP_BETWEEN}s before next run..."
            sleep "${SLEEP_BETWEEN}"
          fi
        fi
      done

      if [[ "${DRY_RUN}" != "true" ]]; then
        dump_crd_profiles "${KUBECONFIG}" "${PROFILE_RESULTS_BASE}"
      fi
    done
  done
done

echo "🎉 All benchmarks completed!"
echo "📊 Generating aggregated summary report..."

WARMUP_RUNS=2

python3 - <<EOF
import json, os, sys
from pathlib import Path

warmup_runs   = ${WARMUP_RUNS}
base_dir      = Path("${BASE_RESULTS}")
if not base_dir.exists():
    sys.exit(f"Error: Results directory {base_dir} does not exist")

summary = {
    "timestamp": "${TIMESTAMP}",
    "config": {
        "namespace": "${NAMESPACE}",
        "total_runs_per_combination": ${RUNS},
        "warmup_runs_discarded": warmup_runs,
        "scheduler_modes": "${MODES_STR}".split(","),
    },
    "runs": []
}

runs_processed = 0
# New naming: scheduler-<mode>_wan-<profile>_load-<load>
import re
for profile_dir in sorted(base_dir.glob("scheduler-*_wan-*_load-*")):
    dir_name = profile_dir.name
    match = re.match(r"scheduler-(.+)_wan-(.+)_load-(.+)", dir_name)
    if not match:
        print(f"  -> Skipping malformed directory: {dir_name}")
        continue
    scheduler_mode, wan_profile, load_profile = match.groups()

    for run_dir in sorted(profile_dir.glob("run-*")):
        try:
            run_num = int(run_dir.name.replace("run-", ""))
        except ValueError:
            continue
        if run_num <= warmup_runs:
            print(f"  -> Discarding warm-up run: {profile_dir.name}/{run_dir.name}")
            continue

        result_subdirs = list(run_dir.glob("*"))
        if not result_subdirs:
            continue
        result_dir = result_subdirs[0]
        slo_file   = result_dir / "slo_summary.json"
        meta_file  = result_dir / "run_meta.json"

        if slo_file.exists():
            with open(slo_file) as f:
                slo_data = json.load(f)
            run_info = {
                "scheduler_mode":   scheduler_mode,
                "wan_profile":      wan_profile,
                "local_load_profile": load_profile,
                "run_number":       run_num,
                "slo_results":      slo_data,
            }
            if meta_file.exists():
                with open(meta_file) as f:
                    run_info["meta"] = json.load(f)
            summary["runs"].append(run_info)
            runs_processed += 1

comparison_file = base_dir / "comparative_summary.json"
with open(comparison_file, "w") as f:
    json.dump(summary, f, indent=2)

print(f"\n📊 Comparative summary saved to: {comparison_file}")
print(f"✅ Processed {runs_processed} run(s)")

if runs_processed == 0:
    sys.exit("⚠️ No results found. Check directory structure and glob pattern.")

# --- Aggregated CSV for the visualizer ---
csv_file = base_dir / "summary.csv"
with open(csv_file, "w") as f:
    f.write("scheduler_mode,wan_profile,local_load,run,workload,kind,metric,target_ms,measured_ms,pass\n")
    for run in summary["runs"]:
        prefix = (f"{run['scheduler_mode']},{run['wan_profile']},"
                  f"{run['local_load_profile']},{run['run_number']}")
        for item in run["slo_results"].get("items", []):
            f.write(
                f"{prefix},{item.get('workload','')},{item.get('kind','')},"
                f"{item.get('metric','')},{item.get('target_ms','')},"
                f"{item.get('measured_ms','')},{item.get('pass',False)}\n"
            )

print(f"📊 Aggregated CSV saved to: {csv_file}")

# --- Per-scheduler SLO pass-rate summary ---
from collections import defaultdict
stats = defaultdict(lambda: {"passed": 0, "total": 0})
for run in summary["runs"]:
    mode = run["scheduler_mode"]
    for item in run["slo_results"].get("items", []):
        stats[mode]["total"] += 1
        if item.get("pass"):
            stats[mode]["passed"] += 1

print("\n📈 Overall SLO Pass Rate by Scheduler Mode:")
for mode, s in sorted(stats.items()):
    pct = 100 * s["passed"] / s["total"] if s["total"] else 0
    print(f"  {mode:20s}: {s['passed']:4d}/{s['total']:4d} = {pct:.1f}%")
EOF

echo ""
echo "🏁 Multi-scheduler benchmark suite completed!"
echo "📁 All results available in: ${BASE_RESULTS}"
