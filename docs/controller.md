# Design Document: SLO- and WAN-aware Hybrid Scheduler for Kubernetes

**Version:** 0.1 (Draft)  
**Date:** 14.10.2025  
**Author:** Roman Domoratskiy  
**Project:** Hybrid Task Scheduling for Edge–Cloud Kubernetes Federations

---

## 1. Overview and Goals

### 1.1 Purpose
This document specifies the architecture, data model, and decision logic for a Kubernetes-native controller that intelligently schedules workloads across edge and cloud clusters based on Service Level Objectives (SLOs) and Wide Area Network (WAN) telemetry.

### 1.2 Goals
- **Maximize SLO compliance:** Ensure workload deadlines and latency targets are met.
- **Minimize unnecessary offloading:** Prefer local (edge) execution when feasible to reduce WAN dependency and cost.
- **Resilience:** Gracefully degrade to safe defaults when telemetry is unavailable or WAN is degraded.
- **Simplicity:** Use standard Kubernetes primitives (annotations, taints/tolerations, nodeSelectors) for maximum portability.

### 1.3 Non-Goals
- Live pod migration between clusters (out of scope for this phase).
- Multi-cloud or multi-region scenarios beyond one cloud cluster.
- Cost optimization beyond SLO compliance (future work).

---

## 2. System Architecture

### 2.1 High-Level Components

```
┌─────────────────────────────────────────────────────────┐
│                    Edge Cluster                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Smart Scheduler Controller                       │  │
│  │  - Watches Pending Pods with SLO annotations      │  │
│  │  - Fetches telemetry (local + WAN)                │  │
│  │  - Decides: EDGE or CLOUD                         │  │
│  │  - Patches pod with nodeSelector + tolerations    │  │
│  └───────────────────────────────────────────────────┘  │
│                         ↓                               │
│   ┌──────────────┐  ┌──────────────┐  ┌─────────────┐   │
│   │ Edge Nodes   │  │ Telemetry    │  │ Virtual Node│   │
│   │ (physical)   │  │ Collector    │  │ (cloud via  │   │
│   │              │  │ (metrics API,│  │  Liqo)      │   │
│   │              │  │  WAN probe)  │  │             │   │
│   └──────────────┘  └──────────────┘  └─────────────┘   │
└─────────────────────────────────────────────────────────┘
                            │ WAN
                            ↓
              ┌──────────────────────────┐
              │    Cloud Cluster         │
              │  (actual compute nodes)  │
              └──────────────────────────┘
```

### 2.2 Integration Path: Controller-based Approach

**Chosen approach:** Simple custom controller.

**Rationale:**
- Faster implementation timeline.
- Works with any federation substrate (Liqo, Karmada, or even simulated virtual nodes).
- Easier to debug and iterate.
- Can be upgraded to scheduler extender later if needed.

**Behavior:**
1. Controller runs as a Deployment in the edge cluster control plane.
2. Watches Pods in `Pending` state with label `scheduling.example.io/managed: "true"`.
3. Reads SLO annotations and telemetry.
4. Computes decision (EDGE or CLOUD).
5. Patches pod with appropriate `nodeSelector` and `tolerations`.
6. Kubernetes native scheduler then binds pod to selected node.

---

## 3. Data Model

### 3.1 SLO Annotations

Pods MUST include these annotations to be managed by the smart scheduler:

| Annotation Key                  | Type    | Required | Description                                      | Example       |
|---------------------------------|---------|----------|--------------------------------------------------|---------------|
| `slo.example.io/deadlineMs`     | integer | No       | End-to-end deadline (milliseconds)               | `"200"`       |
| `slo.example.io/latencyTargetMs`| integer | No       | Target p95 latency (milliseconds)                | `"50"`        |
| `slo.example.io/class`          | string  | Yes      | Workload class: `latency`, `throughput`, `batch` | `"latency"`   |
| `slo.example.io/priority`       | integer | No       | Priority (0–10, higher = more important)         | `"7"`         |
| `slo.example.io/offloadAllowed` | boolean | No       | Can this pod be offloaded to cloud? (default: true) | `"true"`   |

**At least one of `deadlineMs` or `latencyTargetMs` MUST be present for SLO-aware scheduling.**

**Example Pod Manifest:**
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: user-request-handler
  labels:
    scheduling.example.io/managed: "true"
  annotations:
    slo.example.io/latencyTargetMs: "80"
    slo.example.io/class: "latency"
    slo.example.io/priority: "8"
    slo.example.io/offloadAllowed: "true"
spec:
  containers:
    - name: handler
      image: myapp:latest
      resources:
        requests:
          cpu: "500m"
          memory: "512Mi"
```

### 3.2 Node Labels and Taints

**Edge Nodes:**
- Label: `node.role/edge: "true"`
- No taint (schedulable by default).

**Cloud Virtual Node(s):**
- Label: `node.role/cloud: "true"`
- Taint: `cloud=cloud:NoSchedule`  
  (Ensures only explicitly tolerated pods can be offloaded.)

**Setup commands:**
```bash
# Label edge nodes
kubectl label nodes <edge-node-name> node.role/edge=true

# Label and taint cloud virtual node
kubectl label nodes <cloud-virtual-node> node.role/cloud=true
kubectl taint nodes <cloud-virtual-node> cloud=cloud:NoSchedule
```

### 3.3 Decision Annotation (Controller-added)

After making a decision, the controller annotates the pod:

| Annotation Key                     | Example Value                          |
|------------------------------------|----------------------------------------|
| `scheduling.example.io/decision`   | `"edge"` or `"cloud"`                  |
| `scheduling.example.io/timestamp`  | `"2025-10-14T16:00:00Z"`               |
| `scheduling.example.io/reason`     | `"edge_feasible_slo_met"`              |
| `scheduling.example.io/wanRttMs`   | `"45"`                                 |

---

## 4. Telemetry Inputs

### 4.1 Local (Edge) State

**Data needed:**
- Available CPU (millicores) and memory (MiB) on edge nodes.
- Number of pending pods of the same SLO class.
- Historical average processing time per workload class.

**Source:**  
[GUIDANCE: Specify how you will collect this. Options:
- Kubernetes Metrics API (`kubectl top nodes`)
- Prometheus with node-exporter and kube-state-metrics
- Custom metrics adapter
  Fill in your choice and endpoint/query here.]

**Example placeholder:**
```
Source: Prometheus at http://prometheus.edge-cluster.svc:9090
Query for free CPU: sum(kube_node_status_allocatable{resource="cpu"}) - sum(kube_pod_container_resource_requests{resource="cpu"})
```

### 4.2 WAN State

**Data needed:**
- RTT (round-trip time) to cloud cluster endpoint (milliseconds).
- Packet loss percentage (optional).
- Available bandwidth estimate (optional, future).

**Source:**  
Method: Controller runs `ping -c 5 <cloud-endpoint-ip>` every 10 seconds.
Parses avg RTT and loss% from output.
Falls back to cached value if ping fails

### 4.3 Cloud State (Optional)

**Data needed:**
- Is cloud cluster reachable/peered?
- Remote capacity (simple: boolean flag; advanced: actual free resources).

**Source:**  
[GUIDANCE: For MVP, assume cloud always has capacity. For later:
- Query Liqo virtual node status
- Scrape remote Prometheus if accessible
- Use a readiness probe endpoint
  Fill in if implementing.]

---

## 5. Decision Function

### 5.1 Overview

The decision function maps:
```
f(Pod, SLO, LocalState, WANState) → {EDGE, CLOUD}
```

### 5.2 Estimation Models

**Processing Time Estimates:**

[GUIDANCE: Define how you estimate execution time. Options:
- Use a fixed lookup table per workload class
- Linear model based on CPU request: `procTime = α × cpuRequest + β`
- Historical profiling from previous runs (requires database)
  Start simple; fill in your model here.]

**Example placeholder:**
```python
procTimeEdge(pod):
  class = pod.annotations["slo.example.io/class"]
  cpuRequest = pod.spec.containers[0].resources.requests.cpu  # in millicores
  
  if class == "latency":
    return 0.5 * cpuRequest + 20  # ms
  elif class == "throughput":
    return 1.0 * cpuRequest + 50
  elif class == "batch":
    return 2.0 * cpuRequest + 100
```

**Queue Wait Estimates:**

[GUIDANCE: Model congestion. Simple approach:
`queueWait = pendingPodsOfSameClass × avgProcTime`
Fill in your formula.]

**Example placeholder:**
```python
queueWaitEdge(pod):
  pendingCount = len(getPendingPods(class=pod.class, location="edge"))
  return pendingCount * avgProcTimeEdge(pod.class)
```

**WAN Overhead:**

For request-response workloads:
```
wanOverhead = 2 × RTT  (round-trip)
```

For batch jobs:
```
wanOverhead = RTT  (data upload/result download; simplify for now)
```

### 5.3 Feasibility Checks

**Local Feasibility:**
```python
def isEdgeFeasible(pod, localState, slo):
  hasCPU = localState.freeCPU >= pod.cpuRequest
  hasMem = localState.freeMem >= pod.memRequest
  etaEdge = queueWaitEdge(pod) + procTimeEdge(pod)
  meetsDeadline = etaEdge <= slo.deadlineMs
  return hasCPU and hasMem and meetsDeadline
```

**Cloud Feasibility:**
```python
def isCloudFeasible(pod, wanState, slo):
  wanHealthy = (wanState.rttMs < RTT_THRESHOLD) and (wanState.lossPct < LOSS_THRESHOLD)
  etaCloud = 2*wanState.rttMs + queueWaitCloud(pod) + procTimeCloud(pod)
  meetsDeadline = etaCloud <= slo.deadlineMs
  return wanHealthy and meetsDeadline
```

**Thresholds (initial):**

[GUIDANCE: Set initial conservative values; tune after experiments. Fill these in based on your testbed WAN profile.]

```python
RTT_THRESHOLD = 100  # ms
LOSS_THRESHOLD = 2.0  # percent
```

### 5.4 Decision Policy (Pseudocode)

```python
def decide(pod, slo, localState, wanState):
  # 1. Check if offload is allowed
  if not slo.offloadAllowed:
    return EDGE
  
  # 2. Check edge feasibility first (prefer local)
  if isEdgeFeasible(pod, localState, slo):
    return EDGE
  
  # 3. Check cloud feasibility
  if isCloudFeasible(pod, wanState, slo):
    return CLOUD
  
  # 4. Neither is feasible; use priority for tie-break
  if slo.priority >= 7:
    # High-priority: pick best-effort option
    etaEdge = queueWaitEdge(pod) + procTimeEdge(pod)
    etaCloud = 2*wanState.rttMs + procTimeCloud(pod)
    return CLOUD if etaCloud < etaEdge else EDGE
  else:
    # Low-priority: default to edge (fail-soft)
    return EDGE
```

---

## 6. Controller Implementation

### 6.1 Watch and Filter Logic

**Reconciliation Loop:**
1. List all Pods with:
    - Phase: `Pending`
    - Label: `scheduling.example.io/managed: "true"`
    - Field: `spec.nodeName` is empty
2. Filter out pods that already have annotation `scheduling.example.io/decision`.
3. For each remaining pod, run decision function.
4. Patch pod with nodeSelector, tolerations, and decision annotation.

**Polling Interval:** 200ms (fast reaction for latency-sensitive workloads).

### 6.2 Pod Patching

**For EDGE decision:**
```json
{
  "metadata": {
    "annotations": {
      "scheduling.example.io/decision": "edge",
      "scheduling.example.io/timestamp": "<iso8601>",
      "scheduling.example.io/reason": "<reason-string>"
    }
  },
  "spec": {
    "nodeSelector": {
      "node.role/edge": "true"
    }
  }
}
```

**For CLOUD decision:**
```json
{
  "metadata": {
    "annotations": {
      "scheduling.example.io/decision": "cloud",
      "scheduling.example.io/timestamp": "<iso8601>",
      "scheduling.example.io/reason": "<reason-string>",
      "scheduling.example.io/wanRttMs": "<value>"
    }
  },
  "spec": {
    "nodeSelector": {
      "node.role/cloud": "true"
    },
    "tolerations": [
      {
        "key": "cloud",
        "operator": "Equal",
        "value": "cloud",
        "effect": "NoSchedule"
      }
    ]
  }
}
```

### 6.3 RBAC Permissions

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: smart-scheduler-controller
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch", "patch", "update"]
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: smart-scheduler-controller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: smart-scheduler-controller
subjects:
  - kind: ServiceAccount
    name: smart-scheduler-controller
    namespace: kube-system
```

---

## 7. Failure Modes and Fallbacks

| Failure Scenario                     | Behavior                                             |
|--------------------------------------|------------------------------------------------------|
| Telemetry unavailable (metrics down) | Use cached values; if stale >60s, assume EDGE       |
| WAN unreachable (ping timeout)       | Treat as high RTT; force EDGE for all new pods      |
| Cloud virtual node missing           | Force EDGE                                           |
| No capacity on edge or cloud         | Emit Kubernetes Event; pod stays Pending; retry loop|
| Controller crash/restart             | Idempotent: skip already-decided pods; resume watch |

**Logging and Events:**
- Log all decisions with inputs (RTT, queue, ETA) at INFO level.
- Emit Kubernetes Events on pods for audit trail (e.g., "ScheduledToCloud due to edge overload").

---

## 8. Configuration

**Controller ConfigMap:**

[GUIDANCE: Define runtime-tunable parameters. Example structure:]

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: scheduler-config
  namespace: kube-system
data:
  rttThresholdMs: "100"
  lossThresholdPct: "2.0"
  pollingIntervalMs: "200"
  telemetryCacheTTL: "60"
  cloudEndpoint: "10.0.1.100"  # IP to ping for WAN probe
  enableOffload: "true"
```

Controller reads this on startup and watches for updates (reload on change or use default signal handling).

---

## 9. Testing Plan

### 9.1 Unit Tests
- Decision function with mocked inputs (edge feasible, cloud feasible, neither).
- Estimation functions (procTime, queueWait).
- SLO annotation parsing.

### 9.2 Integration Tests
- Deploy controller + mock workloads in test cluster.
- Inject synthetic telemetry and verify correct pod placement.

### 9.3 End-to-End Scenarios
1. **Good WAN, low load:** All latency pods run on edge; batch may offload if needed.
2. **High edge load, good WAN:** Latency pods offload to cloud to meet SLO.
3. **Poor WAN (high RTT/loss):** All pods forced to edge even under load.

---

## 10. Constraints and Limitations

- **No stateful workload handling:** PVCs not automatically migrated; out of scope.
- **Single cloud cluster:** Multi-cloud not supported in this phase.
- **WAN telemetry granularity:** Simple RTT ping; does not model jitter or congestion in detail.
- **No cost model:** Decisions purely SLO-driven; no per-GB or per-hour cost consideration yet.

---

## 11. Future Work

- Implement CRD for SLO policies (reusable across workloads).
- Add bandwidth estimation and application-level throughput metrics.
- Scheduler extender implementation for tighter integration.
- Multi-cloud offload (choose among multiple clouds).
- ML-based workload performance prediction.

---

## 12. References

- Kubernetes Scheduling Framework: https://kubernetes.io/docs/concepts/scheduling-eviction/
- Liqo Documentation: https://docs.liqo.io/
- Karmada Scheduling: https://karmada.io/docs/userguide/scheduling/
- Taints and Tolerations: https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/

---

## Appendix A: Glossary

- **SLO (Service Level Objective):** Performance target (latency, deadline).
- **WAN (Wide Area Network):** Network link between edge and cloud.
- **Offload:** Sending a workload from edge to cloud for execution.
- **Virtual Node:** Kubernetes node representing remote cluster resources (via Liqo or similar).

---

**Document Status:** Draft for Week 4  
**Next Steps:** Review with advisor; implement prototype controller in Week 5.

---

## GUIDANCE FOR FILLING IN BLANKS

### Section 4.1 (Local State Source):
- If using Prometheus: specify exact PromQL queries.
- If using Metrics API: write Go code snippet using `metrics.k8s.io` client.
- If mocking for now: state "Mock telemetry service returns hardcoded values for testing."

### Section 4.2 (WAN Probe):
- Describe actual implementation: will controller run `ping` directly? Or deploy a sidecar pod?
- Provide sample code or command.
- State update frequency (e.g., every 10 seconds).

### Section 5.2 (Estimation Models):
- Fill in actual formulas or lookup tables you'll use.
- If using historical data, describe storage (in-memory cache, database, etc.).

### Section 5.3 (Thresholds):
- Replace placeholder values with results from your testbed measurements (after Week 2 setup).

### Section 8 (Configuration):
- Add any additional parameters your implementation will need (e.g., cloud cluster kubeconfig path if using remote scraping).

---

**Once filled in, this document becomes your implementation contract for Week 5 onward.**