package decision

import (
	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"

	corev1 "k8s.io/api/core/v1"
)

type Location string

const (
	Edge  Location = "edge"
	Cloud Location = "cloud"
)

type Result struct {
	Location Location
	Reason   string
}

type Engine struct {
	config Config
}

type Config struct {
	RTTThresholdMs   int
	LossThresholdPct float64
	// Add from your Section 8 ConfigMap
}

func NewEngine(config Config) *Engine {
	return &Engine{config: config}
}

func (e *Engine) Decide(
	pod *corev1.Pod,
	slo *slo.SLO,
	local *telemetry.LocalState,
	wan *telemetry.WANState,
) Result {
	// Step 1: Check if offload allowed
	if !slo.OffloadAllowed {
		return Result{Edge, "offload_disabled"}
	}

	// Step 2: Check edge feasibility
	if e.isEdgeFeasible(pod, local, slo) {
		return Result{Edge, "edge_feasible_slo_met"}
	}

	// Step 3: Check cloud feasibility
	if e.isCloudFeasible(pod, wan, slo) {
		return Result{Cloud, "edge_overload_cloud_feasible"}
	}

	// Step 4: Fallback logic (Section 5.4)
	if slo.Priority >= 7 {
		etaEdge := e.estimateETAEdge(pod, local)
		etaCloud := e.estimateETACloud(pod, wan)
		if etaCloud < etaEdge {
			return Result{Cloud, "best_effort_cloud_faster"}
		}
	}

	return Result{Edge, "fallback_edge"}
}

func (e *Engine) isEdgeFeasible(pod *corev1.Pod, local *telemetry.LocalState, slo *slo.SLO) bool {
	cpuReq := getCPURequest(pod)
	memReq := getMemoryRequest(pod)

	hasCPU := local.FreeCPU >= cpuReq
	hasMem := local.FreeMem >= memReq

	eta := e.estimateETAEdge(pod, local)
	meetsDeadline := eta <= float64(slo.DeadlineMs)

	return hasCPU && hasMem && meetsDeadline
}

func (e *Engine) isCloudFeasible(pod *corev1.Pod, wan *telemetry.WANState, slo *slo.SLO) bool {
	wanHealthy := wan.RTTMs < e.config.RTTThresholdMs && wan.LossPct < e.config.LossThresholdPct

	eta := e.estimateETACloud(pod, wan)
	meetsDeadline := eta <= float64(slo.DeadlineMs)

	return wanHealthy && meetsDeadline
}

func (e *Engine) estimateETAEdge(pod *corev1.Pod, local *telemetry.LocalState) float64 {
	// Implement Section 5.2 estimation models
	queueWait := e.estimateQueueWait(pod, local)
	procTime := e.estimateProcTime(pod)
	return queueWait + procTime
}

func (e *Engine) estimateETACloud(pod *corev1.Pod, wan *telemetry.WANState) float64 {
	wanOverhead := 2.0 * float64(wan.RTTMs) // Round-trip
	procTime := e.estimateProcTime(pod)
	// Assume cloud has no queue for MVP
	return wanOverhead + procTime
}

func (e *Engine) estimateProcTime(pod *corev1.Pod) float64 {
	// Your Section 5.2 formula
	class := pod.Annotations["slo.hybrid.io/class"]
	cpuRequest := float64(getCPURequest(pod)) // millicores

	switch class {
	case "latency":
		return 0.5*cpuRequest + 20
	case "throughput":
		return 1.0*cpuRequest + 50
	case "batch":
		return 2.0*cpuRequest + 100
	default:
		return 50.0 // default
	}
}

func (e *Engine) estimateQueueWait(pod *corev1.Pod, local *telemetry.LocalState) float64 {
	// Simplified: pendingCount × avgProcTime
	class := pod.Annotations["slo.hybrid.io/class"]
	if local.PendingPodsPerClass == nil {
		return 0
	}
	pendingCount := local.PendingPodsPerClass[class]
	avgProc := e.estimateProcTime(pod)
	return float64(pendingCount) * avgProc
}

func getCPURequest(pod *corev1.Pod) int64 {
	if len(pod.Spec.Containers) == 0 {
		return 0
	}
	return pod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
}

func getMemoryRequest(pod *corev1.Pod) int64 {
	if len(pod.Spec.Containers) == 0 {
		return 0
	}
	return pod.Spec.Containers[0].Resources.Requests.Memory().Value() / (1024 * 1024) // MiB
}
