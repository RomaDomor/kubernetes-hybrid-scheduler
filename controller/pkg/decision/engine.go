package decision

import (
	"math/rand"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

type Location string

const (
	Edge  Location = "edge"
	Cloud Location = "cloud"
)

type Result struct {
	Location       Location
	Reason         string
	PredictedETAMs float64
}

type Engine struct {
	config       Config
	profileStore *ProfileStore
}

type Config struct {
	RTTThresholdMs   int
	LossThresholdPct float64
	ProfileStore     *ProfileStore
}

func NewEngine(config Config) *Engine {
	return &Engine{
		config:       config,
		profileStore: config.ProfileStore,
	}
}

type ETAEstimate struct {
	Mean float64
	P95  float64
}

func (e *Engine) Decide(
	pod *corev1.Pod,
	slo *slo.SLO,
	local *telemetry.LocalState,
	wan *telemetry.WANState,
) Result {
	// 1. Hard safety constraints
	if !slo.OffloadAllowed {
		return Result{Edge, "offload_disabled", 0}
	}

	if wan.RTTMs > 300 || wan.LossPct > 10 {
		return Result{Edge, "wan_unusable", 0}
	}

	// 2. Get historical profiles for both locations
	edgeKey := GetProfileKey(pod, Edge)
	cloudKey := GetProfileKey(pod, Cloud)

	edgeProfile := e.profileStore.GetOrDefault(edgeKey)
	cloudProfile := e.profileStore.GetOrDefault(cloudKey)

	// 3. Predict ETA for each location
	edgeETA := e.predictETA(pod, Edge, edgeProfile, local, wan)
	cloudETA := e.predictETA(pod, Cloud, cloudProfile, local, wan)

	// 4. Confidence-aware exploration
	edgeConf := edgeProfile.ConfidenceScore
	cloudConf := cloudProfile.ConfidenceScore

	explorationBonus := 0.0
	if edgeConf < 0.5 || cloudConf < 0.5 {
		if rand.Float64() < 0.2 { // 20% exploration
			explorationBonus = 50.0
			klog.V(4).Infof("Exploration triggered for %s", pod.Name)
		}
	}

	// 5. Feasibility check
	edgeFeasible := edgeETA.P95 <= float64(slo.DeadlineMs) &&
		local.FreeCPU >= getCPURequest(pod)

	cloudFeasible := (cloudETA.P95 + explorationBonus) <= float64(slo.DeadlineMs)

	// 6. Decision logic
	if edgeFeasible && !cloudFeasible {
		return Result{
			Location:       Edge,
			Reason:         "edge_feasible_only",
			PredictedETAMs: edgeETA.Mean,
		}
	}

	if cloudFeasible && !edgeFeasible {
		return Result{
			Location:       Cloud,
			Reason:         "cloud_feasible_only",
			PredictedETAMs: cloudETA.Mean,
		}
	}

	if edgeFeasible && cloudFeasible {
		// Both feasible: score-based selection
		edgeScore := e.computeScore(Edge, edgeETA, edgeConf, slo)
		cloudScore := e.computeScore(Cloud, cloudETA, cloudConf, slo)

		if edgeScore >= cloudScore {
			return Result{Edge, "edge_preferred", edgeETA.Mean}
		}
		return Result{Cloud, "cloud_faster", cloudETA.Mean}
	}

	// 7. Neither feasible: best-effort with priority tie-break
	if slo.Priority >= 7 && cloudETA.Mean < edgeETA.Mean {
		return Result{Cloud, "best_effort_cloud", cloudETA.Mean}
	}
	return Result{Edge, "best_effort_edge", edgeETA.Mean}
}

func (e *Engine) predictETA(
	pod *corev1.Pod,
	loc Location,
	profile *ProfileStats,
	local *telemetry.LocalState,
	wan *telemetry.WANState,
) ETAEstimate {
	// Base execution time from learned profile
	execTime := profile.MeanDurationMs
	execTimeP95 := profile.P95DurationMs

	// Queue wait (location-specific)
	queueWait := 0.0
	if loc == Edge {
		class := pod.Annotations["slo.hybrid.io/class"]
		pendingCount := local.PendingPodsPerClass[class]
		queueWait = float64(pendingCount) * profile.MeanDurationMs
	}

	// WAN overhead for cloud
	wanOverhead := 0.0
	if loc == Cloud {
		wanOverhead = 2.0 * float64(wan.RTTMs)
	}

	return ETAEstimate{
		Mean: queueWait + execTime + wanOverhead,
		P95:  queueWait + execTimeP95 + wanOverhead,
	}
}

func (e *Engine) computeScore(
	loc Location,
	eta ETAEstimate,
	confidence float64,
	slo *slo.SLO,
) float64 {
	score := 0.0

	// Locality preference
	if loc == Edge {
		score += 50.0
	}

	// Performance: deadline margin
	deadlineMargin := float64(slo.DeadlineMs) - eta.P95
	score += deadlineMargin * 0.5

	// Confidence bonus
	score += confidence * 30.0

	// High-priority critical workloads prefer edge
	if slo.Priority >= 8 && loc == Edge {
		score += 20.0
	}

	return score
}

func getCPURequest(pod *corev1.Pod) int64 {
	if len(pod.Spec.Containers) == 0 {
		return 0
	}
	return pod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
}
