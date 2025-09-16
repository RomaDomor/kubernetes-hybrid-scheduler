#!/usr/bin/env bash
set -euo pipefail

# ---------- Configurable variables (override via env) ----------
NAMESPACE="${NAMESPACE:-default}"
CTX="${CTX:-$(kubectl config current-context)}"

# Paths
#REPO_ROOT="${REPO_ROOT:-$(pwd)}"
REPO_ROOT=".."
MANIFESTS_DIR="${MANIFESTS_DIR:-${REPO_ROOT}/manifests/workloads}"
SCRIPTS_DIR="${SCRIPTS_DIR:-${REPO_ROOT}/scripts}"
RESULTS_ROOT="${RESULTS_ROOT:-${REPO_ROOT}/results}"
RESULTS_DIR="${RESULTS_DIR:-${RESULTS_ROOT}/$(date +%Y%m%d-%H%M%S)}"

# Manifest filenames (relative to MANIFESTS_DIR)
HTTP_LATENCY_FILE="${HTTP_LATENCY_FILE:-http-latency.yaml}"
TOOLBOX_FILE="${TOOLBOX_FILE:-toolbox.yaml}"
CPU_BATCH_FILE="${CPU_BATCH_FILE:-cpu-batch.yaml}"
ML_INFER_FILE="${ML_INFER_FILE:-ml-infer.yaml}"
IO_JOB_FILE="${IO_JOB_FILE:-io-job.yaml}"         # optional, may not exist

# HTTP benchmark parameters
HTTP_SVC_URL="${HTTP_SVC_URL:-http://http-latency.${NAMESPACE}.svc.cluster.local/}"
HTTP_WARMUP_SEC="${HTTP_WARMUP_SEC:-10}"
HTTP_TEST_SEC="${HTTP_TEST_SEC:-30}"
HTTP_QPS="${HTTP_QPS:-20}"
HTTP_CONC="${HTTP_CONC:-20}"

# Job timeout
TIMEOUT_JOB_SEC="${TIMEOUT_JOB_SEC:-900}" # 15 minutes

# Cleanup options
CLEANUP="${CLEANUP:-true}"                  # delete workloads after benchmark
CLEANUP_NAMESPACE="${CLEANUP_NAMESPACE:-false}"  # delete the entire namespace (DANGEROUS)

# ---------------------------------------------------------------

mkdir -p "$RESULTS_DIR"

log() {
  echo "[$(date +%H:%M:%S)] $*"
}

req_file() {
  local f="$1"
  if [[ ! -f "$f" ]]; then
    echo "Required file not found: $f" >&2
    exit 1
  fi
}

wait_deployment() {
  local ns="$1" name="$2" timeout="${3:-300s}"
  kubectl -n "$ns" rollout status deploy/"$name" --timeout="$timeout"
}

wait_job() {
  local ns="$1" name="$2" timeout="${3:-$TIMEOUT_JOB_SEC}"
  local start_ts end_ts
  start_ts=$(date +%s)
  log "Waiting for Job $ns/$name to complete (timeout ${timeout}s)..."
  if ! kubectl -n "$ns" wait --for=condition=complete job/"$name" --timeout="${timeout}s" >/dev/null 2>&1; then
    log "Job $ns/$name did not complete in time; fetching logs."
    kubectl -n "$ns" logs job/"$name" || true
    exit 1
  fi
  end_ts=$(date +%s)
  echo $((end_ts - start_ts))
}

cleanup_workloads() {
  log "Cleaning up workloads in namespace ${NAMESPACE}..."
  # Delete only what we applied
  [[ -f "${MANIFESTS_DIR}/${HTTP_LATENCY_FILE}" ]] && kubectl -n "$NAMESPACE" delete -f "${MANIFESTS_DIR}/${HTTP_LATENCY_FILE}" --ignore-not-found
  [[ -f "${MANIFESTS_DIR}/${CPU_BATCH_FILE}"     ]] && kubectl -n "$NAMESPACE" delete -f "${MANIFESTS_DIR}/${CPU_BATCH_FILE}"     --ignore-not-found
  [[ -f "${MANIFESTS_DIR}/${ML_INFER_FILE}"      ]] && kubectl -n "$NAMESPACE" delete -f "${MANIFESTS_DIR}/${ML_INFER_FILE}"      --ignore-not-found || true
  [[ -f "${MANIFESTS_DIR}/${IO_JOB_FILE}"        ]] && kubectl -n "$NAMESPACE" delete -f "${MANIFESTS_DIR}/${IO_JOB_FILE}"        --ignore-not-found || true
  [[ -f "${MANIFESTS_DIR}/${TOOLBOX_FILE}"       ]] && kubectl -n "$NAMESPACE" delete -f "${MANIFESTS_DIR}/${TOOLBOX_FILE}"       --ignore-not-found
}

cleanup_namespace() {
  log "Deleting namespace ${NAMESPACE} (this removes ALL resources in it)..."
  kubectl delete ns "$NAMESPACE" --ignore-not-found
}

detect_virtual_node() {
  if kubectl get nodes -l liqo.io/virtual-node=true >/dev/null 2>&1; then
    kubectl get nodes -l liqo.io/virtual-node=true -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
  else
    echo ""
  fi
}

VNODE="$(detect_virtual_node || true)"

log "Context: $CTX | Namespace: $NAMESPACE"
log "Repo root: $REPO_ROOT"
log "Manifests: $MANIFESTS_DIR"
log "Results:   $RESULTS_DIR"
[[ -n "$VNODE" ]] && log "Detected Liqo virtual node: ${VNODE}" || log "No Liqo virtual node detected."

# Ensure namespace
kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || kubectl create ns "$NAMESPACE"

# Apply toolbox
TOOLBOX_PATH="${MANIFESTS_DIR}/${TOOLBOX_FILE}"
req_file "$TOOLBOX_PATH"
log "Deploying toolbox pod from $TOOLBOX_PATH ..."
kubectl -n "$NAMESPACE" apply -f "$TOOLBOX_PATH"
log "Waiting for toolbox to be Ready..."
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/toolbox --timeout=180s

# HTTP service
HTTP_LATENCY_PATH="${MANIFESTS_DIR}/${HTTP_LATENCY_FILE}"
req_file "$HTTP_LATENCY_PATH"
log "Deploying http-latency from $HTTP_LATENCY_PATH ..."
kubectl -n "$NAMESPACE" apply -f "$HTTP_LATENCY_PATH"
wait_deployment "$NAMESPACE" "http-latency" "240s"

# Jobs
CPU_BATCH_PATH="${MANIFESTS_DIR}/${CPU_BATCH_FILE}"
req_file "$CPU_BATCH_PATH"
log "Applying CPU batch job from $CPU_BATCH_PATH ..."
kubectl -n "$NAMESPACE" apply -f "$CPU_BATCH_PATH"

ML_INFER_PATH="${MANIFESTS_DIR}/${ML_INFER_FILE}"
if [[ -f "$ML_INFER_PATH" ]]; then
  log "Applying ML infer job from $ML_INFER_PATH ..."
  kubectl -n "$NAMESPACE" apply -f "$ML_INFER_PATH"
else
  log "ML infer manifest not found (optional): $ML_INFER_PATH"
fi

IO_JOB_PATH="${MANIFESTS_DIR}/${IO_JOB_FILE}"
if [[ -f "$IO_JOB_PATH" ]]; then
  log "Applying IO job from $IO_JOB_PATH ..."
  kubectl -n "$NAMESPACE" apply -f "$IO_JOB_PATH"
else
  log "IO job manifest not found (optional): $IO_JOB_PATH"
fi

# Snapshots
log "Capturing cluster state..."
kubectl get nodes -o wide > "${RESULTS_DIR}/nodes.txt"
kubectl -n "$NAMESPACE" get pods -o wide > "${RESULTS_DIR}/pods_initial.txt"
kubectl -n "$NAMESPACE" get deploy,svc,job,pod -o wide > "${RESULTS_DIR}/k8s_objects_initial.txt"

# HTTP benchmark
log "HTTP warmup ${HTTP_WARMUP_SEC}s @ ${HTTP_SVC_URL}"
kubectl -n "$NAMESPACE" exec toolbox -- sh -lc \
  "/hey -z ${HTTP_WARMUP_SEC}s -q ${HTTP_QPS} -c ${HTTP_CONC} ${HTTP_SVC_URL}" >/dev/null 2>&1 || true

log "HTTP benchmark ${HTTP_TEST_SEC}s, qps=${HTTP_QPS}, conc=${HTTP_CONC}"
kubectl -n "$NAMESPACE" exec toolbox -- sh -lc \
  "/hey -z ${HTTP_TEST_SEC}s -q ${HTTP_QPS} -c ${HTTP_CONC} ${HTTP_SVC_URL}" \
  | tee "${RESULTS_DIR}/http_benchmark.txt"

# Jobs wait + logs
record_job() {
  local name="$1"
  if kubectl -n "$NAMESPACE" get job "$name" >/dev/null 2>&1; then
    local dur
    dur=$(wait_job "$NAMESPACE" "$name")
    echo "${name},duration_sec,${dur}" | tee -a "${RESULTS_DIR}/jobs_durations.csv"
    kubectl -n "$NAMESPACE" logs job/"$name" > "${RESULTS_DIR}/${name}_logs.txt" || true
  else
    log "Job $name not found, skipping."
  fi
}
record_job "cpu-batch"
record_job "ml-infer"
record_job "io-job"

# Final snapshots
log "Capturing final placement and events..."
kubectl -n "$NAMESPACE" get pods -o wide > "${RESULTS_DIR}/pods_final.txt"
kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp > "${RESULTS_DIR}/events.txt" || true

# Pod->node CSV (requires jq)
if command -v jq >/dev/null 2>&1; then
  log "Extracting pod/node mapping CSV..."
  kubectl -n "$NAMESPACE" get pod -o json \
    | jq -r '.items[] | [.metadata.name, .spec.nodeName, (.status.phase // "NA")] | @csv' \
    > "${RESULTS_DIR}/pod_node_map.csv" || true
else
  log "jq not found; skipping pod_node_map.csv generation."
fi

# Offload stats (if VNODE known)
if [[ -n "${VNODE}" && -f "${RESULTS_DIR}/pod_node_map.csv" ]]; then
  OFFLOADED=$(awk -F, -v vnode="$VNODE" '{gsub(/"/,""); if ($2==vnode) c++} END{print c+0}' "${RESULTS_DIR}/pod_node_map.csv" || echo 0)
  TOTAL=$(wc -l < "${RESULTS_DIR}/pod_node_map.csv" || echo 0)
  echo "offloaded_pods,${OFFLOADED}" | tee -a "${RESULTS_DIR}/summary.csv"
  echo "total_pods,${TOTAL}" | tee -a "${RESULTS_DIR}/summary.csv"
fi

# hey summary extraction
if [[ -f "${RESULTS_DIR}/http_benchmark.txt" ]]; then
  grep -E "Requests/sec|Latency|99%|95%|50%" -n "${RESULTS_DIR}/http_benchmark.txt" \
    > "${RESULTS_DIR}/http_summary.txt" || true
fi

if [[ "${CLEANUP}" == "true" ]]; then
  if [[ "${CLEANUP_NAMESPACE}" == "true" ]]; then
    cleanup_namespace
  else
    cleanup_workloads
  fi
  log "Cleanup done."
else
  log "Cleanup skipped. Set CLEANUP=true to delete workloads (or CLEANUP_NAMESPACE=true to delete the namespace)."
fi

log "Benchmark complete. Artifacts -> ${RESULTS_DIR}"
