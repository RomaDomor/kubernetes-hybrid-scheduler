#!/bin/bash
set -euo pipefail

# Default configuration
DEFAULT_NAMESPACE="cloud"
DEFAULT_LOCAL_NAMESPACE="local-clients"
DEFAULT_MANIFESTS_DIR="./manifests/workloads"
DEFAULT_RESULTS_ROOT="./results"
DEFAULT_KUBECONFIG="$HOME/.kube/config"
DEFAULT_WAN_ROUTER="router"
DEFAULT_PROFILES="clear,good,moderate,poor"
DEFAULT_LOCAL_LOAD_PROFILES="none"  # Can be single value or comma-separated
DEFAULT_VENV_PATH="scripts/bench-suite/.venv"
DEFAULT_PRE_CLEAN=false

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
    -l, --local-load-profiles LOAD  Local load profiles: single value or comma-separated list
                                    Options: none, low, medium, high (default: ${DEFAULT_LOCAL_LOAD_PROFILES})
                                    If single value: applied to all WAN profiles
                                    If multiple: must match count of WAN profiles
    -v, --venv-path PATH            Path to Python virtual environment (default: ${DEFAULT_VENV_PATH})
    -s, --sleep SECONDS             Sleep time between runs in seconds (default: 30)
    -t, --timeout SECONDS           Job timeout in seconds (default: 900)
    --no-cleanup                    Don't cleanup namespace between runs
    --pre-clean                     Run kubectl cleanup on the offloaded namespace before each profile
    --dry-run                       Show what would be run without executing
    -h, --help                      Show this help message

EXAMPLES:
    # Run with defaults (no local load)
    $0

    # Apply medium load to all profiles
    $0 --local-load-profiles medium --profiles "clear,good,poor"

    # Different load per profile (must match profile count)
    $0 --profiles "clear,good,poor" --local-load-profiles "none,medium,high"

    # Test local cluster under load with different WAN conditions
    $0 -p "clear,good,moderate,poor" -l "low,medium,medium,high"

    # Dry run to see what would be executed
    $0 --dry-run --profiles "good,poor" --local-load-profiles "medium,high"

    # Pre-clean namespace before each profile with medium local load
    $0 --pre-clean -l medium
EOF
}

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
JOB_TIMEOUT=900
NO_CLEANUP=false
PRE_CLEAN="${DEFAULT_PRE_CLEAN}"
DRY_RUN=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --local-namespace)
            LOCAL_NAMESPACE="$2"
            shift 2
            ;;
        -m|--manifests-dir)
            MANIFESTS_DIR="$2"
            shift 2
            ;;
        -r|--results-root)
            RESULTS_ROOT="$2"
            shift 2
            ;;
        -k|--kubeconfig)
            KUBECONFIG="$2"
            shift 2
            ;;
        -w|--wan-router)
            WAN_ROUTER="$2"
            shift 2
            ;;
        -p|--profiles)
            PROFILES_STR="$2"
            shift 2
            ;;
        -l|--local-load-profiles)
            LOCAL_LOAD_PROFILES_STR="$2"
            shift 2
            ;;
        -v|--venv-path)
            VENV_PATH="$2"
            shift 2
            ;;
        -s|--sleep)
            SLEEP_BETWEEN="$2"
            shift 2
            ;;
        -t|--timeout)
            JOB_TIMEOUT="$2"
            shift 2
            ;;
        --no-cleanup)
            NO_CLEANUP=true
            shift
            ;;
        --pre-clean)
            PRE_CLEAN=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option $1"
            usage
            exit 1
            ;;
    esac
done

# Convert comma-separated profiles to arrays
IFS=',' read -ra PROFILES <<< "${PROFILES_STR}"
IFS=',' read -ra LOCAL_LOAD_PROFILES_ARRAY <<< "${LOCAL_LOAD_PROFILES_STR}"

# Process local load profiles
# If single value, replicate it for all WAN profiles
# If multiple values, must match WAN profile count
if [[ ${#LOCAL_LOAD_PROFILES_ARRAY[@]} -eq 1 ]]; then
    # Single value - replicate for all profiles
    single_load="${LOCAL_LOAD_PROFILES_ARRAY[0]}"
    LOCAL_LOAD_PROFILES=()
    for _ in "${PROFILES[@]}"; do
        LOCAL_LOAD_PROFILES+=("$single_load")
    done
elif [[ ${#LOCAL_LOAD_PROFILES_ARRAY[@]} -eq ${#PROFILES[@]} ]]; then
    # Multiple values matching profile count
    LOCAL_LOAD_PROFILES=("${LOCAL_LOAD_PROFILES_ARRAY[@]}")
else
    echo "Error: Local load profile count (${#LOCAL_LOAD_PROFILES_ARRAY[@]}) must be 1 or match WAN profile count (${#PROFILES[@]})"
    echo "WAN profiles: ${PROFILES_STR}"
    echo "Local load profiles: ${LOCAL_LOAD_PROFILES_STR}"
    exit 1
fi

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

# Build cleanup flag
CLEANUP_FLAG=""
#if [[ "${NO_CLEANUP}" == "false" ]]; then
#    CLEANUP_FLAG="--cleanup-namespace"
#fi

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
echo "Sleep Between:         ${SLEEP_BETWEEN}s"
echo "Job Timeout:           ${JOB_TIMEOUT}s"
echo "Cleanup (post-run):    ${NO_CLEANUP}"
echo "Pre-Clean (pre-run):   ${PRE_CLEAN}"
echo "Dry Run:               ${DRY_RUN}"
echo "========================================="

# Print mapping table
echo ""
echo "Profile Mapping:"
echo "----------------"
for i in "${!PROFILES[@]}"; do
    echo "  ${PROFILES[$i]:10} → local-load: ${LOCAL_LOAD_PROFILES[$i]}"
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

# Run benchmarks for each profile
for i in "${!PROFILES[@]}"; do
    wan_profile="${PROFILES[$i]}"
    local_load="${LOCAL_LOAD_PROFILES[$i]}"
    profile_num=$((i + 1))
    total_profiles="${#PROFILES[@]}"

    echo ""
    echo "========================================="
    echo "[$profile_num/$total_profiles] Running benchmark"
    echo "  WAN Profile:        ${wan_profile}"
    echo "  Local Load Profile: ${local_load}"
    echo "========================================="

    # Create profile-specific results directory
    PROFILE_RESULTS="${BASE_RESULTS}/wan-${wan_profile}_load-${local_load}"

    # Optional pre-clean per profile (offloaded namespace)
    if [[ "${PRE_CLEAN}" == "true" && "${DRY_RUN}" != "true" ]]; then
        cleanup_namespace "${NAMESPACE}" "${KUBECONFIG}"
        cleanup_namespace "${LOCAL_NAMESPACE}" "${KUBECONFIG}"
    elif [[ "${PRE_CLEAN}" == "true" && "${DRY_RUN}" == "true" ]]; then
        echo "Would pre-clean namespace '${NAMESPACE}' with kubeconfig '${KUBECONFIG}'"
    fi

    # Build the command
    cmd=(
        python3 scripts/bench-suite/app.py
        --namespace "${NAMESPACE}"
        --local-namespace "${LOCAL_NAMESPACE}"
        --manifests-dir "${MANIFESTS_DIR}"
        --results-root "${PROFILE_RESULTS}"
        --kubeconfig "${KUBECONFIG}"
        --wan-router "${WAN_ROUTER}"
        --wan-profile "${wan_profile}"
        --local-load-profile "${local_load}"
        --timeout-job-sec "${JOB_TIMEOUT}"
    )

    # Add cleanup flag if not disabled
    if [[ -n "${CLEANUP_FLAG}" ]]; then
        cmd+=("${CLEANUP_FLAG}")
    fi

    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "Would run: ${cmd[*]}"
    else
        # Run the benchmark
        echo "Executing: ${cmd[*]}"
        "${cmd[@]}"

        echo "✅ Completed: WAN=${wan_profile}, Load=${local_load}"
        echo "📁 Results saved to: ${PROFILE_RESULTS}"

        # Sleep between runs (except after the last one)
        if [[ $profile_num -lt $total_profiles && $SLEEP_BETWEEN -gt 0 ]]; then
            echo "⏳ Waiting ${SLEEP_BETWEEN} seconds before next run..."
            sleep "${SLEEP_BETWEEN}"
        fi
    fi
done

if [[ "${DRY_RUN}" == "true" ]]; then
    echo ""
    echo "DRY RUN completed. Use without --dry-run to execute."
    exit 0
fi

echo ""
echo "🎉 All benchmarks completed!"
echo "📊 Generating summary report..."

# Generate comparative summary
python3 - <<EOF
import json
import os
import sys
from pathlib import Path

base_dir = Path("${BASE_RESULTS}")
if not base_dir.exists():
    print(f"Error: Results directory {base_dir} does not exist")
    sys.exit(1)

summary = {
    "timestamp": "${TIMESTAMP}",
    "config": {
        "namespace": "${NAMESPACE}",
        "local_namespace": "${LOCAL_NAMESPACE}",
        "wan_router": "${WAN_ROUTER}",
        "wan_profiles": "${PROFILES_STR}".split(","),
        "local_load_profiles": "${LOCAL_LOAD_PROFILES_STR}".split(","),
        "sleep_between": ${SLEEP_BETWEEN},
        "job_timeout": ${JOB_TIMEOUT},
        "pre_clean": $([[ "${PRE_CLEAN}" == "true" ]] && echo "True" || echo "False")
    },
    "runs": []
}

profiles_found = 0
for profile_dir in sorted(base_dir.glob("wan-*_load-*")):
    # Parse directory name
    dir_name = profile_dir.name
    parts = dir_name.split("_")
    if len(parts) != 2:
        continue
    wan_profile = parts[0].replace("wan-", "")
    load_profile = parts[1].replace("load-", "")

    # Find the actual results directory (timestamped subdirectory)
    result_dirs = sorted(profile_dir.glob("*"))
    if result_dirs:
        result_dir = result_dirs[0]  # Should be only one

        # Read SLO summary
        slo_file = result_dir / "slo_summary.json"
        if slo_file.exists():
            with open(slo_file) as f:
                data = json.load(f)

                run_info = {
                    "wan_profile": wan_profile,
                    "local_load_profile": load_profile,
                    "slo_results": data
                }
                summary["runs"].append(run_info)
                profiles_found += 1

                # Print quick summary
                items = data.get("items", [])
                passed = sum(1 for item in items if item.get("pass", False))
                total = len(items)
                pass_rate = (passed/total)*100 if total > 0 else 0
                print(f"📋 WAN: {wan_profile:10} Load: {load_profile:6} → {passed:2}/{total:2} ({pass_rate:5.1f}%)")

# Save comparative summary
comparison_file = base_dir / "comparative_summary.json"
with open(comparison_file, "w") as f:
    json.dump(summary, f, indent=2)

print(f"\\n📊 Comparative summary saved to: {comparison_file}")
print(f"✅ Processed {profiles_found} run(s)")

if profiles_found == 0:
    print("⚠️  Warning: No profile results found!")
    sys.exit(1)

# Generate CSV for easy analysis
csv_file = base_dir / "summary.csv"
with open(csv_file, "w") as f:
    f.write("wan_profile,local_load,workload,kind,metric,target_ms,measured_ms,pass\\n")
    for run in summary["runs"]:
        wan = run["wan_profile"]
        load = run["local_load_profile"]
        for item in run["slo_results"].get("items", []):
            f.write(f"{wan},{load},{item.get('workload','')},{item.get('kind','')},")
            f.write(f"{item.get('metric','')},{item.get('target_ms','')},")
            f.write(f"{item.get('measured_ms','')},{item.get('pass',False)}\\n")

print(f"📊 CSV summary saved to: {csv_file}")
EOF

echo ""
echo "🏁 Multi-profile benchmark suite completed!"
echo "📁 All results available in: ${BASE_RESULTS}"