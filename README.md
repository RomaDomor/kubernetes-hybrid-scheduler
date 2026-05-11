# Deadline-driven Scheduler for Multi-Cluster Kubernetes

A Kubernetes-native controller that intelligently schedules workloads across edge and cloud clusters based on **per-workload deadlines** and **Wide Area Network (WAN) telemetry**, using **Lyapunov optimization** to balance deadline compliance against cloud offloading cost.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Decision Algorithm](#decision-algorithm)
- [Workload Classes](#workload-classes)
- [Prerequisites](#prerequisites)
- [Building](#building)
- [Deployment](#deployment)
- [Configuration](#configuration)
- [Workload Annotations](#workload-annotations)
- [Observability](#observability)
- [Benchmarking](#benchmarking)
- [Testing](#testing)
- [Project Structure](#project-structure)

## Overview

In edge-cloud Kubernetes federations (e.g., via [Liqo](https://liqo.io/)), deciding whether a workload should run locally on an edge node or be offloaded to a remote cloud cluster is a non-trivial problem. Network conditions fluctuate, edge resources are constrained, and different workloads have different latency and deadline requirements.

This project implements a **mutating admission webhook** that intercepts pod creation and patches each pod with the appropriate `nodeSelector` and tolerations to route it to the optimal cluster. The scheduling decision is driven by:

- **Local telemetry** -- real-time edge CPU/memory utilization and pending pod counts.
- **WAN probes** -- continuous ICMP-based measurement of round-trip time and packet loss to remote clusters.
- **Deadline annotations** -- per-pod deadline, latency target, priority, and workload class.
- **Lyapunov drift-plus-cost optimization** -- a control-theoretic framework that maintains virtual queues of deadline violations per workload class, dynamically adjusting placement bias to minimize cumulative violations while penalizing unnecessary cloud usage.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                      Edge Cluster                        │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │         Smart Scheduler Controller                 │  │
│  │                                                    │  │
│  │  Admission Webhook ◄── Pod CREATE events           │  │
│  │        │                                           │  │
│  │        ▼                                           │  │
│  │  ┌───────────┐   ┌──────────────┐   ┌──────────┐   │  │
│  │  │ Telemetry │   │  Lyapunov    │   │ Profile  │   │  │
│  │  │ Collector │──►│  Decision    │◄──│  Store   │   │  │
│  │  │ (local +  │   │  Engine      │   │ (CRD)    │   │  │
│  │  │  WAN)     │   └──────┬───────┘   └──────────┘   │  │
│  │  └───────────┘          │                          │  │
│  │                         ▼                          │  │
│  │              JSONPatch: nodeSelector               │  │
│  │              + tolerations                         │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌────────────┐      ┌──────────────┐                    │
│  │ Edge Nodes │      │ Virtual Node │ ─── WAN ──► Cloud  │
│  │ (physical) │      │ (Liqo)       │              Node  │
│  └────────────┘      └──────────────┘                    │
└──────────────────────────────────────────────────────────┘
```

The controller runs as a single pod in the edge cluster. It watches pod lifecycle events to learn actual execution times (fed back into the profile store), and exposes Prometheus metrics and debug endpoints for observability.

## Key Features

- **Deadline-driven placement** -- respects per-pod deadline, latency target, priority, and workload class annotations.
- **WAN-aware** -- continuously probes remote clusters and factors RTT and packet loss into placement decisions.
- **Lyapunov optimization** -- uses drift-plus-cost framework with virtual queues to maintain long-term deadline compliance guarantees per workload class.
- **Performance profiling** -- learns per-workload execution characteristics via the `WorkloadProfile` CRD and uses historical p95 latency for future decisions.
- **Fail-safe defaults** -- gracefully degrades to edge-local scheduling when telemetry is unavailable or WAN is degraded.
- **Standard Kubernetes primitives** -- uses annotations, labels, nodeSelectors, taints, and tolerations for maximum portability.
- **Helm-based deployment** -- production-ready chart with auto-generated TLS certificates, RBAC, and configurable parameters.

## Decision Algorithm

The engine uses a **Lyapunov drift-plus-cost** framework:

1. **Feasibility check** -- for each candidate cluster (local edge, remote cloud), estimate total time: `queue_wait + processing_time + network_overhead`. Discard clusters that cannot meet the pod's deadline or lack sufficient resources.
2. **Virtual queue update** -- per workload class `c`, maintain two queues:
   - **Z[c]** (magnitude): tracks cumulative deadline violation severity.
   - **Zp[c]** (probability): tracks the fraction of requests that violate deadlines.
3. **Cost computation** -- for each feasible cluster, compute weighted cost:
   ```
   cost = Beta * cost_factor[cluster] + queue_pressure[class]
   ```
   where `Beta` controls the tradeoff between cost minimization and deadline compliance.
4. **Selection** -- choose the cluster with the lowest total cost. Ties favor edge (lower cost factor).
5. **Decay** -- periodically reduce virtual queue backlogs by a configurable decay factor to prevent historical violations from permanently biasing decisions.

## Workload Classes

| Class         | Target Violation % | Decay Interval | Beta | Typical Use Case              |
|---------------|-------------------|----------------|------|-------------------------------|
| `latency`     | 5%                | 1h             | 1.0  | User-facing APIs              |
| `interactive` | 5%                | 1h             | 1.0  | Dashboards, interactive shells|
| `streaming`   | 10%               | 1h             | 0.8  | Real-time video, data streams |
| `throughput`  | 10%               | 1h             | 0.8  | Parallel batch processing     |
| `batch`       | 20%               | 2h             | 0.5  | Background jobs, analytics    |

## Prerequisites

- Kubernetes 1.21+ cluster with [metrics-server](https://github.com/kubernetes-sigs/metrics-server) installed
- Edge-cloud federation via [Liqo](https://liqo.io/) (or any mechanism that exposes a virtual node)
- Go 1.24+ (for building from source)
- Helm 3 (for deployment)
- Docker (for building the container image)

### Node Setup

Label and taint your nodes before deploying:

```bash
# Edge nodes
kubectl label nodes <edge-node> node.role/edge=true

# Cloud virtual node (created by Liqo)
kubectl label nodes <cloud-node> node.cluster/id=cloud-1
kubectl taint nodes <cloud-node> cloud=cloud:NoSchedule
```

## Building

### From Source

```bash
cd controller
go build -o smart-scheduler ./cmd/controller
```

### Docker Image

```bash
cd controller
docker build -t ghcr.io/romadomor/kubernetes-smart-scheduler:latest .
docker push ghcr.io/romadomor/kubernetes-smart-scheduler:latest
```

## Deployment

### Helm (Recommended)

```bash
helm install smart-scheduler ./deployments/smart-scheduler \
  --namespace kube-system \
  --set config.remoteEndpoints="cloud-1=<CLOUD_NODE_IP>" \
  --set config.costFactors="local=0,cloud-1=1.0"
```

The Helm chart handles:
- TLS certificate generation (via a pre-install Job)
- `MutatingWebhookConfiguration` registration
- RBAC (ClusterRole, ClusterRoleBinding, ServiceAccount)
- `WorkloadProfile` CRD installation
- ConfigMap-based runtime configuration

### Uninstall

```bash
helm uninstall smart-scheduler --namespace kube-system
```

For full Helm chart documentation, see [`deployments/smart-scheduler/README.md`](deployments/smart-scheduler/README.md).

## Configuration

All parameters are configurable via Helm values or environment variables / CLI flags:

| Parameter | Helm Value | Flag | Default | Description |
|-----------|-----------|------|---------|-------------|
| WAN RTT threshold | `config.rttUnusable` | `--rtt-unusable` | `100` ms | RTT above which a remote cluster is considered unusable |
| WAN loss threshold | `config.lossUnusable` | `--loss-unusable` | `2.0` % | Packet loss above which a remote cluster is unusable |
| Lyapunov Beta | `config.lyapunovBeta` | `--lyapunov-beta` | `1.0` | Cost-performance tradeoff (lower = prioritize deadlines) |
| Cost factors | `config.costFactors` | `--cost-factors` | `local=0` | Per-cluster cost weights (CSV) |
| Remote endpoints | `config.remoteEndpoints` | `--remote-endpoints` | `""` | Remote cluster probe targets (CSV) |
| Edge pessimism | `config.edgePendingPessimismPct` | -- | `10` % | Additional resource reservation for pending pods |
| Decay interval | `config.decayInterval` | -- | `1h` | Virtual queue decay period |
| Min sample count | `config.minSampleCount` | `--min-sample-count` | `10` | Minimum profiles before using p95 estimates |

### Tuning Examples

**Latency-sensitive (strict deadline compliance):**
```bash
--lyapunov-beta=0.5 --rtt-unusable=50 --loss-unusable=1.0 --cost-factors="local=0,cloud-1=2.0"
```

**Cost-optimized (aggressive offloading):**
```bash
--lyapunov-beta=2.0 --cost-factors="local=0,cloud-1=0.8" --edge-pending-pessimism-pct=25
```

## Workload Annotations

Annotate your pods (or pod templates in Jobs/Deployments) to enable smart scheduling:

```yaml
metadata:
  labels:
    scheduling.hybrid.io/managed: "true"        # Required: opt into smart scheduling
  annotations:
    slo.hybrid.io/class: "latency"              # Workload class
    slo.hybrid.io/deadlineMs: "5000"            # End-to-end deadline (ms)
    slo.hybrid.io/latencyTargetMs: "200"        # p95 latency target (ms)
    slo.hybrid.io/priority: "8"                 # Priority 0-10 (higher = more important)
    slo.hybrid.io/offloadAllowed: "true"        # Allow cloud offloading (default: true)
```

The controller adds decision metadata to scheduled pods:

```yaml
annotations:
  scheduling.hybrid.io/decision: "edge"         # or "cloud"
  scheduling.hybrid.io/reason: "edge-feasible"  # Decision reason
  scheduling.hybrid.io/timestamp: "2026-..."    # ISO 8601 timestamp
  scheduling.hybrid.io/wanRttMs: "12"           # WAN RTT used (if cloud)
```

## Observability

### Prometheus Metrics

The controller exports metrics on `:8080/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `scheduler_decisions_by_class` | Counter | Decisions broken down by class and location |
| `scheduler_violations_by_class` | Counter | Deadline violations by class and location |
| `scheduler_lyapunov_virtual_queue` | Gauge | Virtual queue depth per class |
| `scheduler_lyapunov_prob_queue` | Gauge | Probability queue depth per class |
| `scheduler_admission_latency_seconds` | Histogram | Webhook decision latency |
| `scheduler_edge_free_cpu_millicores` | Gauge | Available edge CPU |
| `scheduler_edge_free_memory_mebibytes` | Gauge | Available edge memory |
| `scheduler_wan_rtt_ms` | Gauge | WAN latency per remote cluster |
| `scheduler_wan_loss_percent` | Gauge | WAN packet loss per remote cluster |

### Health Endpoints

| Endpoint | Description |
|----------|-------------|
| `:8080/healthz` | Liveness probe |
| `:8080/readyz` | Readiness probe |
| `:8080/metrics` | Prometheus scrape endpoint |
| `:8080/debug/profiles` | Export all workload profiles (JSON) |
| `:8080/debug/lyapunov` | Export Lyapunov scheduler state (JSON) |

## Benchmarking

The repository includes a comprehensive benchmark suite that compares four scheduling strategies under varying WAN and load conditions.

### Scheduler Strategies

| Strategy | Description |
|----------|-------------|
| **smart** | Hybrid Lyapunov-based scheduler (this project) |
| **single-cluster** | Default Kubernetes scheduler across all Liqo-federated nodes (WAN-unaware) |
| **liqo-native** | Liqo namespace-level offloading (no deadline awareness) |
| **round-robin** | Deterministic cycling: edge, fog, cloud |

### Running Benchmarks

```bash
./run-multi-benchmark.sh
```

The benchmark orchestrator (`scripts/bench-suite/`) controls:
- **WAN profiles**: clear, good, moderate, poor (simulated via `tc netem` on the router)
- **Local load profiles**: none, low, medium (CPU stress on edge nodes)
- **Workload mix**: 9 workload types spanning all 5 workload classes

Results are written to timestamped directories (`multi-run-<timestamp>/`) with per-run metrics, latency distributions, and deadline violation rates. Visualization is handled by `scripts/bench-suite/visualize.py`.

### Test Workloads

Located in `manifests/workloads/`:

| Workload | Class | Description |
|----------|-------|-------------|
| `api-gateway` | latency | HTTP latency-sensitive gateway |
| `http-latency-job` | latency | HTTP benchmark (curl loops) |
| `ml-infer` | interactive | ML inference simulation |
| `stream-batch-job` | streaming | Streaming + batch hybrid |
| `io-job` | throughput | I/O intensive workload |
| `cpu-batch` | batch | CPU-bound batch processing |
| `memory-intensive` | batch | Memory-heavy workload |
| `build-job` | batch | Compilation/build workload |

## Testing

### Unit Tests

```bash
cd controller
go test ./...
```

Tests cover the decision engine, Lyapunov optimizer, profile store, telemetry collectors, webhook server, annotation parser, and utility functions.

## Project Structure

```
kubernetes-hybrid-scheduler/
├── controller/
│   ├── cmd/controller/
│   │   └── main.go                  # Entry point
│   ├── pkg/
│   │   ├── api/v1alpha1/            # Type definitions (Deadline, Result, ClusterState)
│   │   ├── constants/               # Annotations, labels, reason codes
│   │   ├── decision/                # Core scheduling engine & Lyapunov optimizer
│   │   ├── telemetry/               # Local metrics & WAN probe collectors
│   │   ├── webhook/                 # Admission webhook server & pod patching
│   │   ├── slo/                     # Deadline annotation parser
│   │   ├── signals/                 # Graceful shutdown handling
│   │   └── util/                    # Helpers (env parsing, JSON pointer)
│   ├── gen/mocks/                   # Generated test mocks
│   ├── Dockerfile
│   ├── go.mod
│   └── go.sum
├── deployments/
│   └── smart-scheduler/             # Helm chart
│       ├── Chart.yaml
│       ├── values.yaml
│       ├── crds/                    # WorkloadProfile CRD
│       └── templates/               # Deployment, RBAC, Webhook, etc.
├── manifests/
│   └── workloads/                   # Benchmark workload manifests
├── scripts/
│   └── bench-suite/                 # Python benchmark orchestrator
├── docs/
│   └── controller.md               # Detailed design document
└── run-multi-benchmark.sh           # Multi-strategy benchmark runner
```

## Failure Modes

| Scenario | Behavior |
|----------|----------|
| Metrics API unavailable | Uses cached state; falls back to edge if stale >60s |
| WAN unreachable | Treats as RTT=999ms, Loss=100%; forces edge placement |
| Cloud virtual node missing | All pods scheduled to edge |
| No capacity on any cluster | Pod stays Pending; Kubernetes Event emitted |
| Controller crash/restart | Idempotent: already-decided pods are skipped |

## License

This project was developed as part of a bachelor's thesis on deadline-driven scheduling of workloads in multi-cluster environments.

**Author:** Roman Domoratskiy
