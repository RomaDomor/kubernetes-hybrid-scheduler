#!/bin/bash
set -euo pipefail

# --- User Configuration ---

# Define the different scheduler configurations you want to benchmark.
# Each configuration is a name followed by the Helm --set parameters.
declare -A CONFIGS
CONFIGS=(
    ["baseline"]="--set config.edgeCostFactor='0.0' --set config.cloudCostFactor='1.0' --set config.lyapunovBeta='1.0'"
    ["tuned-gentle"]="--set config.edgeCostFactor='0.1' --set config.cloudCostFactor='1.0' --set config.lyapunovBeta='1.0'"
    ["tuned-aggressive"]="--set config.edgeCostFactor='0.1' --set config.cloudCostFactor='0.5' --set config.lyapunovBeta='0.8'"
)

# Default benchmark parameters (can be overridden by command-line flags)
DEFAULT_NAMESPACE="cloud"
DEFAULT_LOCAL_NAMESPACE="local-clients"
DEFAULT_MANIFESTS_DIR="./manifests/workloads"
DEFAULT_RESULTS_ROOT="./results"
DEFAULT_KUBECONFIG="$HOME/.kube/config"
DEFAULT_WAN_ROUTER="router"
DEFAULT_PROFILES="clear,good,moderate,poor"
DEFAULT_LOCAL_LOAD_PROFILES="none,low,medium"
DEFAULT_VENV_PATH="scripts/bench-suite/.venv"
DEFAULT_PRE_CLEAN=true
DEFAULT_RUNS=15
DEFAULT_IMAGE_TAG="latest"
# --- End of User Configuration ---


# Function to show usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run multi-profile Kubernetes SLO benchmarks with different WAN conditions.

OPTIONS:
    -n, --namespace NAMESPACE       Kubernetes namespace (default: ${DEFAULT_NAMESPACE})
        --local-namespace NAMESPACE   Local clients namespace (default: ${DEFAULT_LOCAL_NAMESPACE})
    -m, --manifests-dir DIR         Directory containing workload manifests (default: ${DEFAULT_MANIFESTS_DIR})
    -r, --results-root DIR          Root directory for results (default: ${DEFAULT_RESULTS_ROOT})
    -k, --kubeconfig PATH           Path to kubeconfig file (default: ${DEFAULT_KUBECONFIG})
    -w, --wan-router ROUTER         WAN router SSH target (default: ${DEFAULT_WAN_ROUTER})
    -p, --profiles PROFILES         Comma-separated WAN profiles (default: ${DEFAULT_PROFILES})
    -l, --local-load-profiles LOAD  Local load profiles (default: ${DEFAULT_LOCAL_LOAD_PROFILES})
    --runs N                        Number of times to repeat each profile run (default: ${DEFAULT_RUNS})
    --image-tag TAG                 Docker image tag for the scheduler (default: ${DEFAULT_IMAGE_TAG})
    -v, --venv-path PATH            Path to Python virtual environment (default: ${DEFAULT_VENV_PATH})
    -s, --sleep SECONDS             Sleep time between runs in seconds (default: 30)
    -t, --timeout SECONDS           Job timeout in seconds (default: 900)
    --pre-clean                     Run kubectl cleanup on namespaces before each combination
    --no-controller-metrics         Disable collection of metrics from the scheduler controller.
    --dry-run                       Show what would be run without executing
    -h, --help                      Show this help message

EXAMPLES:
    # Run all WAN profiles against all specified local load profiles, 3 times each
    $0 --runs 3 -l "none,low,medium" -p "clear,good,moderate,poor"

    # Dry run to see the new run structure for a matrix of combinations
    $0 --dry-run --runs 2 --profiles "good,poor" --local-load-profiles "medium,high"
EOF
}

# --- Script Logic ---

# Initialize variables with defaults
NAMESPACE="${DEFAULT_NAMESPACE}"
LOCAL_NAMESPACE="${DEFAULT_LOCAL_NAMESPACE}"
MANIFESTS_DIR="${DEFAULT_MANIFESTS_DIR}"
RESULTS_ROOT="${DEFAULT_RESULTS_ROOT}"
KUBECONFIG="${DEFAULT_KUBECONFIG}"
WAN_ROUTER="${DEFAULT_WAN_ROUTER}"
PROFILES_STR="${DEFAULT_PROFILES}"
LOCAL_LOAD_PROFILES_STR="${DEFAULT_LOCAL_LOAD_PROFILES}"
VENV_PATH="${DEFAULT_VENV_PATH}"
SLEEP_BETWEEN=30
JOB_TIMEOUT=600
PRE_CLEAN="${DEFAULT_PRE_CLEAN}"
DRY_RUN=false
RUNS="${DEFAULT_RUNS}"
COLLECT_CONTROLLER_METRICS=true
IMAGE_TAG="${DEFAULT_IMAGE_TAG}"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--namespace) NAMESPACE="$2"; shift 2 ;;
        --local-namespace) LOCAL_NAMESPACE="$2"; shift 2 ;;
        -m|--manifests-dir) MANIFESTS_DIR="$2"; shift 2 ;;
        -r|--results-root) RESULTS_ROOT="$2"; shift 2 ;;
        -k|--kubeconfig) KUBECONFIG="$2"; shift 2 ;;
        -w|--wan-router) WAN_ROUTER="$2"; shift 2 ;;
        -p|--profiles) PROFILES_STR="$2"; shift 2 ;;
        -l|--local-load-profiles) LOCAL_LOAD_PROFILES_STR="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --image-tag) IMAGE_TAG="$2"; shift 2 ;;
        -v|--venv-path) VENV_PATH="$2"; shift 2 ;;
        -s|--sleep) SLEEP_BETWEEN="$2"; shift 2 ;;
        -t|--timeout) JOB_TIMEOUT="$2"; shift 2 ;;
        --pre-clean) PRE_CLEAN=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --no-controller-metrics) COLLECT_CONTROLLER_METRICS=false; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option $1"; usage; exit 1 ;;
    esac
done

# Convert comma-separated profiles to arrays
IFS=',' read -ra PROFILES <<< "${PROFILES_STR}"
IFS=',' read -ra LOCAL_LOAD_PROFILES <<< "${LOCAL_LOAD_PROFILES_STR}"

# Validate local load profile values
valid_loads=("none" "low" "medium" "high")
for load in "${LOCAL_LOAD_PROFILES[@]}"; do
    if [[ ! " ${valid_loads[*]} " =~ " ${load} " ]]; then
        echo "Error: Invalid local load profile '${load}'. Must be one of: ${valid_loads[*]}"
        exit 1
    fi
done

# Validate inputs
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

# Add timestamp to results
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BASE_RESULTS="${RESULTS_ROOT}/multi-run-${TIMESTAMP}"

echo "========================================="
echo "Multi-Profile Benchmark Configuration"
echo "========================================="
echo "Namespace (offloaded): ${NAMESPACE}"
echo "Local Namespace:       ${LOCAL_NAMESPACE}"
echo "Manifests Dir:         ${MANIFESTS_DIR}"
echo "Results Root:          ${BASE_RESULTS}"
echo "Kubeconfig:            ${KUBECONFIG}"
echo "WAN Router:            ${WAN_ROUTER}"
echo "WAN Profiles:          ${PROFILES[*]}"
echo "Local Load Profiles:   ${LOCAL_LOAD_PROFILES[*]}"
echo "Runs per combination:  ${RUNS}"
echo "Sleep Between:         ${SLEEP_BETWEEN}s"
echo "Job Timeout:           ${JOB_TIMEOUT}s"
echo "Pre-Clean (pre-run):   ${PRE_CLEAN}"
echo "Collect Ctrl Metrics:  ${COLLECT_CONTROLLER_METRICS}"
echo "Dry Run:               ${DRY_RUN}"
echo "Scheduler Image Tag:   ${IMAGE_TAG}"
echo "========================================="

# Print mapping table
echo ""
echo "Execution Matrix:"
echo "-----------------"
for wan_profile in "${PROFILES[@]}"; do
    for local_load in "${LOCAL_LOAD_PROFILES[@]}"; do
        echo "  - WAN: ${wan_profile}, Local Load: ${local_load}"
    done
done
echo "========================================="


if [[ "${DRY_RUN}" == "true" ]]; then
    echo "DRY RUN MODE - Commands that would be executed:"
    echo ""
fi

# Poll until the namespace has no remaining workload/config/storage resources
wait_ns_empty() {
  local ns="$1"
  local kubeconfig="$2"
  local timeout="${3:-300}"   # seconds
  local interval="${4:-3}"    # seconds
  local end=$((SECONDS + timeout))

  # Kinds to watch (namespaced, common)
  local kinds=(
    pods
    deployments.apps
    replicasets.apps
    statefulsets.apps
    daemonsets.apps
    jobs.batch
    cronjobs.batch
    services
    persistentvolumeclaims
  )

  echo "⏳ Waiting for namespace '${ns}' to be empty (timeout ${timeout}s)..."
  while (( SECONDS < end )); do
    local remaining=0
    for kind in "${kinds[@]}"; do
      # get -o name to avoid header issues; ignore errors (some kinds may not exist)
      local cnt
      cnt=$(kubectl get "$kind" -n "$ns" --kubeconfig "$kubeconfig" -o name 2>/dev/null | wc -l | tr -d ' ')
      [[ "$cnt" =~ ^[0-9]+$ ]] || cnt=0
      remaining=$((remaining + cnt))
    done
    if (( remaining == 0 )); then
      echo "✅ Namespace '${ns}' appears empty."
      return 0
    fi
    sleep "$interval"
  done
  echo "⚠️ Timeout waiting for namespace '${ns}' to be empty."
  return 1
}

# Clean a namespace and wait until resources terminate
cleanup_namespace() {
  local ns="$1"
  local kubeconfig_path="$2"
  local wait_timeout="${3:-300}"

  echo "🧹 Pre-cleaning namespace '${ns}' using kubeconfig '${kubeconfig_path}' ..."

  set +e
  # 1) Controllers first with foreground cascading
  kubectl delete cronjobs.batch --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --cascade=foreground
  kubectl delete jobs.batch     --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --cascade=foreground
  kubectl delete deployments.apps   --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --cascade=foreground
  kubectl delete statefulsets.apps  --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --cascade=foreground
  kubectl delete daemonsets.apps    --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --cascade=foreground

  # 2) Workload pods (normal delete; uncomment force if needed)
  kubectl delete pods --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found
  # If pods hang: use force, but be cautious
  # kubectl delete pods --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found --force --grace-period=0

  # 3) Services and configs (service will skip the 'kubernetes' svc if not in user ns)
  kubectl delete services   --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found
  kubectl delete configmaps --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found
  kubectl delete secrets    --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found

  # 4) Storage
  kubectl delete persistentvolumeclaims --all -n "${ns}" --kubeconfig "${kubeconfig_path}" --ignore-not-found
  set -e

  # 5) Wait for emptiness
  wait_ns_empty "${ns}" "${kubeconfig_path}" "${wait_timeout}" 3 || true
}

reset_scheduler_state() {
  local kubeconfig_path="$1"
  local helm_args="$2"
  local image_tag="$3"

  echo "🧠 Resetting scheduler state with new configuration..."

  echo "  - Uninstalling Helm release 'smart-scheduler'..."
  if ! helm uninstall smart-scheduler -n kube-system --kubeconfig "${kubeconfig_path}" --wait --ignore-not-found; then
      echo "  - Helm uninstall failed, but attempting to continue..."
  fi

  echo "  - Force-deleting WorkloadProfile CRD to ensure a clean state..."
  kubectl delete crd workloadprofiles.scheduling.hybrid.io --kubeconfig "${kubeconfig_path}" --ignore-not-found

  local helm_install_cmd="helm install smart-scheduler smart-scheduler/smart-scheduler \
    --set image.tag='${image_tag}' \
    --set logLevel=5 \
    -n kube-system \
    --kubeconfig '${kubeconfig_path}' \
    ${helm_args} \
    --wait"

  echo "  - Installing new Helm release with tag '${image_tag}' and args: ${helm_args}"
  if [[ "${DRY_RUN}" == "true" ]]; then
      echo "  - Would run: ${helm_install_cmd}"
  else
      eval "${helm_install_cmd}"
  fi

  echo "✅ Scheduler reset and reinstalled."
}

dump_crd_profiles() {
    local kubeconfig_path="$1"
    local output_dir="$2"
    local output_file="${output_dir}/final_workload_profiles.yaml"

    echo "💾 Dumping final WorkloadProfile CRDs to ${output_file}..."

    local cmd="kubectl get workloadprofiles.scheduling.hybrid.io -n kube-system --kubeconfig '${kubeconfig_path}' -o yaml"

    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "  - Would run: ${cmd} > ${output_file}"
    else
        # Ensure the directory exists
        mkdir -p "${output_dir}"
        # Execute and redirect output; '|| true' prevents script exit if no CRDs are found
        eval "${cmd}" > "${output_file}" || true
        echo "  - Dump complete."
    fi
}

total_combinations=$(( ${#CONFIGS[@]} * ${#PROFILES[@]} * ${#LOCAL_LOAD_PROFILES[@]} ))
current_combination=0

for config_name in "${!CONFIGS[@]}"; do
    helm_params="${CONFIGS[$config_name]}"

    # Run benchmarks for each profile combination
    for wan_profile in "${PROFILES[@]}"; do
        for local_load in "${LOCAL_LOAD_PROFILES[@]}"; do
            current_combination=$((current_combination + 1))
            echo "========================================="
            echo "[$current_combination/$total_combinations] Starting Profile Combination"
            echo "  Scheduler Config:   ${config_name}"
            echo "  WAN Profile:        ${wan_profile}"
            echo "  Local Load Profile: ${local_load}"
            echo "  Runs to perform:    ${RUNS}"
            echo "========================================="

            # This now happens ONCE per (config, wan, load) combination
            if [[ "${DRY_RUN}" != "true" ]]; then
                echo "  - Applying WAN profile '${wan_profile}'..."
                ssh "${WAN_ROUTER}" "sudo -n /usr/local/sbin/wan/apply_wan.sh '${wan_profile}'"

                # <--- Pass image tag to reset function ---
                reset_scheduler_state "${KUBECONFIG}" "${helm_params}" "${IMAGE_TAG}"

                echo "  - Sleeping 15s after scheduler reset..."
                sleep 15
            else
                echo "Would apply WAN profile '${wan_profile}' and reset scheduler state with config '${config_name}' and image tag '${IMAGE_TAG}'."
            fi

            # Create the base directory for this profile combination
            PROFILE_RESULTS_BASE="${BASE_RESULTS}/config-${config_name}_wan-${wan_profile}_load-${local_load}"

            # Inner loop for multiple runs
            for run_num in $(seq 1 "${RUNS}"); do
                if [[ "${PRE_CLEAN}" == "true" && "${DRY_RUN}" != "true" ]]; then
                    cleanup_namespace "${NAMESPACE}" "${KUBECONFIG}"
                    cleanup_namespace "${LOCAL_NAMESPACE}" "${KUBECONFIG}"
                    echo "Sleeping 10 seconds after cleanup..."
                    sleep 10
                elif [[ "${PRE_CLEAN}" == "true" && "${DRY_RUN}" == "true" ]]; then
                    echo "Would pre-clean namespaces '${NAMESPACE}' and '${LOCAL_NAMESPACE}'"
                fi

                echo "-----------------------------------------"
                echo "  Running iteration [$run_num/$RUNS] for config '${config_name}'..."
                echo "-----------------------------------------"

                # Create profile-specific results directory for this run
                RUN_RESULTS_DIR="${PROFILE_RESULTS_BASE}/run-${run_num}"

                cmd=(
                    python3 scripts/bench-suite/app.py
                    --namespace "${NAMESPACE}"
                    --local-namespace "${LOCAL_NAMESPACE}"
                    --manifests-dir "${MANIFESTS_DIR}"
                    --results-root "${RUN_RESULTS_DIR}"
                    --kubeconfig "${KUBECONFIG}"
                    --wan-router "${WAN_ROUTER}"
                    --wan-profile "${wan_profile}"
                    --local-load-profile "${local_load}"
                    --timeout-job-sec "${JOB_TIMEOUT}"
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
                        echo "⏳ Waiting ${SLEEP_BETWEEN} seconds before next run..."
                        sleep "${SLEEP_BETWEEN}"
                    fi
                fi
            done

            # <--- Dump CRDs after all runs for this profile are done ---
            if [[ "${DRY_RUN}" != "true" ]]; then
                dump_crd_profiles "${KUBECONFIG}" "${PROFILE_RESULTS_BASE}"
            else
                echo "Would dump final CRD profiles to ${PROFILE_RESULTS_BASE}/final_workload_profiles.yaml"
            fi
        done
    done
done

echo "🎉 All benchmarks completed!"
echo "📊 Generating aggregated summary report..."

WARMUP_RUNS=2

python3 - <<EOF
import json
import os
import sys
from pathlib import Path

warmup_runs = ${WARMUP_RUNS}

base_dir = Path("${BASE_RESULTS}")
if not base_dir.exists():
    sys.exit(f"Error: Results directory {base_dir} does not exist")

summary = {
    "timestamp": "${TIMESTAMP}",
    "config": {
        "namespace": "${NAMESPACE}",
        "total_runs_per_profile": ${RUNS},
        "warmup_runs_discarded": warmup_runs,
    },
    "runs": []
}

runs_processed = 0
for profile_dir in sorted(base_dir.glob("config-*_wan-*_load-*")):
    dir_name = profile_dir.name

    try:
        config_part, rest = dir_name.split("_wan-", 1)
        wan_profile, load_profile = rest.split("_load-", 1)
        config_name = config_part.replace("config-", "")
    except ValueError:
        print(f"  -> Skipping malformed directory: {dir_name}")
        continue

    # Find all runs for this profile
    for run_dir in sorted(profile_dir.glob("run-*")):
        try:
            run_num = int(run_dir.name.replace("run-", ""))
        except ValueError:
            continue # Skip directories that aren't named correctly

        if run_num <= warmup_runs:
            print(f"  -> Discarding warm-up run: {profile_dir.name}/{run_dir.name}")
            continue

        result_subdirs = list(run_dir.glob("*"))
        if not result_subdirs:
            continue

        result_dir = result_subdirs[0]
        slo_file = result_dir / "slo_summary.json"

        if slo_file.exists():
            with open(slo_file) as f:
                data = json.load(f)
                run_info = {
                    "config_name": config_name,
                    "wan_profile": wan_profile,
                    "local_load_profile": load_profile,
                    "run_number": run_num,
                    "slo_results": data
                }
                summary["runs"].append(run_info)
                runs_processed += 1

# Save detailed comparative summary
comparison_file = base_dir / "comparative_summary.json"
with open(comparison_file, "w") as f:
    json.dump(summary, f, indent=2)

print(f"\\n📊 Detailed summary with all runs saved to: {comparison_file}")
print(f"✅ Processed {runs_processed} total run(s)")

if runs_processed == 0:
    sys.exit("⚠️ Warning: No profile results found! Check glob pattern and directory structure.")

# Generate aggregated CSV for easy analysis by the visualizer
csv_file = base_dir / "summary.csv"
with open(csv_file, "w") as f:
    f.write("config_name,wan_profile,local_load,run,workload,kind,metric,target_ms,measured_ms,pass\\n")
    for run in summary["runs"]:
        for item in run["slo_results"].get("items", []):
            f.write(f"{run['config_name']},{run['wan_profile']},{run['local_load_profile']},{run['run_number']},")
            f.write(f"{item.get('workload','')},{item.get('kind','')},{item.get('metric','')},")
            f.write(f"{item.get('target_ms','')},{item.get('measured_ms','')},{item.get('pass',False)}\\n")

print(f"📊 Aggregated CSV for visualization saved to: {csv_file}")
EOF

echo ""
echo "🏁 Multi-profile benchmark suite completed!"
echo "📁 All results available in: ${BASE_RESULTS}"
