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
    -v, --venv-path PATH            Path to Python virtual environment (default: ${DEFAULT_VENV_PATH})
    -s, --sleep SECONDS             Sleep time between runs in seconds (default: 30)
    -t, --timeout SECONDS           Job timeout in seconds (default: 900)
    --no-cleanup                    Don't cleanup namespace between runs
    --pre-clean                     Run kubectl cleanup on the offloaded namespace before each profile
    --dry-run                       Show what would be run without executing
    -h, --help                      Show this help message

EXAMPLES:
    # Run with defaults
    $0

    # Custom namespace and profiles
    $0 --namespace test --profiles "clear,moderate,poor"

    # Different kubeconfig and no sleep between runs
    $0 --kubeconfig ~/.kube/config --sleep 0

    # Dry run to see what would be executed
    $0 --dry-run --profiles "good,poor"

    # Pre-clean 'cloud' namespace (offloaded) using a specific kubeconfig before each profile
    $0 --pre-clean -k ~/.kube/edge-config
EOF
}

# Initialize variables with defaults
NAMESPACE="${DEFAULT_NAMESPACE}"
LOCAL_NAMESPACE="${DEFAULT_LOCAL_NAMESPACE}"
MANIFSETS_DIR_PLACEHOLDER="IGNORE" # placeholder to catch typos
MANIFESTS_DIR="${DEFAULT_MANIFESTS_DIR}"
RESULTS_ROOT="${DEFAULT_RESULTS_ROOT}"
KUBECONFIG="${DEFAULT_KUBECONFIG}"
WAN_ROUTER="${DEFAULT_WAN_ROUTER}"
PROFILES_STR="${DEFAULT_PROFILES}"
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

# Convert comma-separated profiles to array
IFS=',' read -ra PROFILES <<< "${PROFILES_STR}"

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
echo "Profiles:              ${PROFILES[*]}"
echo "Sleep Between:         ${SLEEP_BETWEEN}s"
echo "Job Timeout:           ${JOB_TIMEOUT}s"
echo "Cleanup (post-run):    ${NO_CLEANUP}"
echo "Pre-Clean (pre-run):   ${PRE_CLEAN}"
echo "Dry Run:               ${DRY_RUN}"
echo "========================================="

if [[ "${DRY_RUN}" == "true" ]]; then
    echo "DRY RUN MODE - Commands that would be executed:"
    echo ""
fi

# Pre-clean function
cleanup_namespace() {
    local ns="$1"
    local kubeconfig_path="$2"

    echo "🧹 Pre-cleaning namespace '${ns}' using kubeconfig '${kubeconfig_path}' ..."
    set +e
    kubectl delete all --all -n "${ns}" --kubeconfig "${kubeconfig_path}"
    set -e
}

# Run benchmarks for each profile
for i in "${!PROFILES[@]}"; do
    profile="${PROFILES[$i]}"
    profile_num=$((i + 1))
    total_profiles="${#PROFILES[@]}"

    echo ""
    echo "========================================="
    echo "[$profile_num/$total_profiles] Running benchmark with WAN profile: ${profile}"
    echo "========================================="

    # Create profile-specific results directory
    PROFILE_RESULTS="${BASE_RESULTS}/profile-${profile}"

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
        --wan-profile "${profile}"
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

        echo "✅ Completed profile: ${profile}"
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
        "profiles": "${PROFILES_STR}".split(","),
        "sleep_between": ${SLEEP_BETWEEN},
        "job_timeout": ${JOB_TIMEOUT},
        "pre_clean": ${PRE_CLEAN}
    },
    "profiles": {}
}

profiles_found = 0
for profile_dir in base_dir.glob("profile-*"):
    profile = profile_dir.name.replace("profile-", "")

    # Find the actual results directory (timestamped subdirectory)
    result_dirs = sorted(profile_dir.glob("*"))
    if result_dirs:
        result_dir = result_dirs[0]  # Should be only one

        # Read SLO summary
        slo_file = result_dir / "slo_summary.json"
        if slo_file.exists():
            with open(slo_file) as f:
                data = json.load(f)
                summary["profiles"][profile] = data
                profiles_found += 1

                # Print quick summary
                items = data.get("items", [])
                passed = sum(1 for item in items if item.get("pass", False))
                total = len(items)
                pass_rate = (passed/total)*100 if total > 0 else 0
                print(f"📋 {profile:12} {passed:2}/{total:2} ({pass_rate:5.1f}%)")

# Save comparative summary
comparison_file = base_dir / "comparative_summary.json"
with open(comparison_file, "w") as f:
    json.dump(summary, f, indent=2)

print(f"\\n📊 Comparative summary saved to: {comparison_file}")
print(f"✅ Processed {profiles_found} profile(s)")

if profiles_found == 0:
    print("⚠️  Warning: No profile results found!")
    sys.exit(1)
EOF

echo ""
echo "🏁 Multi-profile benchmark suite completed!"
echo "📁 All results available in: ${BASE_RESULTS}"