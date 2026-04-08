package decision

import (
	"fmt"
	"math"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
)

// EngineConfig holds all configurable parameters for the decision engine.
type EngineConfig struct {
	RTTUnusableMs           int
	LossUnusablePct         float64
	MaxProfileCount         int
	EdgePendingPessimismPct int
	ProfileStore            *ProfileStore
	LyapunovScheduler       *LyapunovScheduler
	// CostFactors maps ClusterID → relative cost.
	// Local cluster defaults to 0, remote defaults to 1.
	CostFactors map[constants.ClusterID]float64
}

// Engine implements the core scheduling logic.
type Engine struct {
	config   EngineConfig
	lyapunov *LyapunovScheduler
}

func NewEngine(config EngineConfig) *Engine {
	if config.ProfileStore == nil {
		klog.Fatal("ProfileStore must be provided in EngineConfig")
	}
	if config.LyapunovScheduler == nil {
		klog.Fatal("LyapunovScheduler must be provided in EngineConfig")
	}
	if config.CostFactors == nil {
		config.CostFactors = make(map[constants.ClusterID]float64)
	}
	return &Engine{
		config:   config,
		lyapunov: config.LyapunovScheduler,
	}
}

// clusterCost returns the cost factor for a cluster.
func (e *Engine) clusterCost(id constants.ClusterID) float64 {
	if cost, ok := e.config.CostFactors[id]; ok {
		return cost
	}
	if constants.IsLocal(id) {
		return 0.0
	}
	return 1.0
}

// candidateEval holds per-cluster evaluation data.
type candidateEval struct {
	clusterID         constants.ClusterID
	state             *apis.ClusterState
	profile           *apis.ProfileStats
	eta               float64
	feasible          bool
	feasibilityReason string
	cost              float64
	weight            float64
}

// Decide determines the optimal placement for a pod across all clusters.
func (e *Engine) Decide(
	pod *corev1.Pod,
	slo *apis.SLO,
	clusterStates map[constants.ClusterID]*apis.ClusterState,
) apis.Result {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))

	// --- 1. Safety checks ---
	localState := clusterStates[constants.LocalCluster]
	if localState == nil {
		klog.Warningf("%s: Local state is nil, forcing to local with best-effort", podID)
		return apis.Result{
			Location: constants.LocalCluster,
			Reason:   constants.ReasonTelemetryCircuitBreaker,
		}
	}

	// --- 2. Hard constraints ---
	if !slo.OffloadAllowed {
		klog.V(4).Infof("%s: Offload disabled by SLO, forcing to local", podID)
		result := apis.Result{Location: constants.LocalCluster, Reason: constants.ReasonOffloadDisabled}
		recordDecision(result, slo.Class)
		return result
	}

	reqCPU := util.GetCPURequest(pod)
	reqMem := util.GetMemRequestMi(pod)
	deadline := float64(slo.DeadlineMs)

	// --- 3. Evaluate each cluster ---
	candidates := make([]candidateEval, 0, len(clusterStates))

	for clusterID, state := range clusterStates {
		if state == nil {
			continue
		}

		// Safety: check WAN for remote clusters
		if !constants.IsLocal(clusterID) {
			if result, skip := e.checkRemoteSafety(podID, clusterID, state, slo); skip {
				klog.V(4).Infof("%s: Cluster %s failed safety check: %s", podID, clusterID, result.Reason)
				continue // skip this cluster
			}
		} else {
			// Check local telemetry staleness
			if result, skip := e.checkLocalSafety(podID, state, slo); skip {
				// If local is unsafe, we still want to consider remotes
				klog.V(4).Infof("%s: Local cluster failed safety: %s", podID, result.Reason)
				// Don't add local as candidate
				continue
			}
		}

		profileKey := apis.GetProfileKey(pod, clusterID)
		profile := e.config.ProfileStore.GetOrDefault(profileKey)
		eta := e.predictETA(clusterID, profile, state, slo)

		feasible, reason := e.isFeasible(clusterID, pod, state, reqCPU, reqMem, eta, deadline)

		candidates = append(candidates, candidateEval{
			clusterID:         clusterID,
			state:             state,
			profile:           profile,
			eta:               eta,
			feasible:          feasible,
			feasibilityReason: reason,
			cost:              e.clusterCost(clusterID),
		})

		klog.V(4).Infof("%s: Cluster %s: ETA=%.0fms feasible=%v reason=%s cost=%.2f",
			podID, clusterID, eta, feasible, reason, e.clusterCost(clusterID))
	}

	// --- 4. Fallback: no candidates at all ---
	if len(candidates) == 0 {
		klog.Warningf("%s: No viable clusters, fallback to local", podID)
		return apis.Result{
			Location: constants.LocalCluster,
			Reason:   constants.ReasonLocalBestEffort,
		}
	}

	// --- 5. Lyapunov optimization across all candidates ---
	probCalc := func(stats *apis.ProfileStats, threshold float64) float64 {
		return e.config.ProfileStore.ComputeViolationProbability(stats, threshold)
	}

	bestIdx, bestWeight := e.lyapunov.DecideN(slo.Class, deadline, candidates, probCalc)
	chosen := candidates[bestIdx]

	// --- 6. Result assembly ---
	reason := e.determineReason(chosen, candidates)
	wanRtt := 0
	if !constants.IsLocal(chosen.clusterID) {
		wanRtt = chosen.state.RTTMs
	}

	result := apis.Result{
		Location:       chosen.clusterID,
		Reason:         reason,
		PredictedETAMs: chosen.eta,
		WanRttMs:       wanRtt,
		LyapunovWeight: bestWeight,
	}

	recordDecision(result, slo.Class)
	klog.V(3).Infof("%s: Decision: cluster=%s reason=%s eta=%.0fms weight=%.2f",
		podID, result.Location, result.Reason, result.PredictedETAMs, bestWeight)

	return result
}

// predictETA computes the predicted total time for a cluster.
func (e *Engine) predictETA(
	clusterID constants.ClusterID,
	profile *apis.ProfileStats,
	state *apis.ClusterState,
	slo *apis.SLO,
) float64 {
	if constants.IsLocal(clusterID) {
		return e.predictLocalETA(profile, state, slo)
	}
	return e.predictRemoteETA(profile, state)
}

// predictLocalETA = Twait + Texec (Tnet ≈ 0 for local cluster).
func (e *Engine) predictLocalETA(profile *apis.ProfileStats, state *apis.ClusterState, slo *apis.SLO) float64 {
	execTime := profile.P95DurationMs

	pendingCount := state.PendingPodsPerClass[slo.Class]
	if pendingCount == 0 {
		return execTime
	}

	classDemand := state.TotalDemand[slo.Class]
	if classDemand.CPU == 0 {
		return execTime
	}

	avgPodCPU := classDemand.CPU / int64(pendingCount)
	if avgPodCPU == 0 {
		avgPodCPU = 100
	}
	cpuParallelism := int64(1)
	if state.FreeCPU > 0 {
		cpuParallelism = state.FreeCPU / avgPodCPU
		if cpuParallelism < 1 {
			cpuParallelism = 1
		}
	}

	parallelism := cpuParallelism
	const maxParallelism = 64
	if parallelism > maxParallelism {
		parallelism = maxParallelism
	}

	queueDrainTime := float64(pendingCount) * profile.MeanDurationMs / float64(parallelism)

	varianceBuffer := 0.0
	managedUtilization := 0.0
	if state.EffectiveAllocatableCPU > 0 {
		managedActiveCPU := state.EffectiveAllocatableCPU - state.FreeCPU
		managedUtilization = float64(managedActiveCPU) / float64(state.EffectiveAllocatableCPU)
	}

	if managedUtilization < 0.95 && profile.MeanDurationMs > 0 {
		cv2 := math.Pow(profile.StdDevDurationMs/profile.MeanDurationMs, 2)
		rho := managedUtilization
		varianceBuffer = (cv2 / 2.0) * (rho / (1.0 - rho)) * profile.MeanDurationMs
		if parallelism > 1 {
			varianceBuffer /= math.Sqrt(float64(parallelism))
		}
	} else if managedUtilization >= 0.95 {
		varianceBuffer = profile.StdDevDurationMs * 2.0
	}

	queueWait := queueDrainTime + varianceBuffer

	if state.MeasurementConfidence < 1.0 {
		uncertaintyPenalty := queueWait * (1.0 - state.MeasurementConfidence) * 0.5
		queueWait += uncertaintyPenalty
	}

	totalETA := queueWait + execTime
	klog.V(4).Infof("Local ETA: exec=%.0fms queue=%.0fms total=%.0fms", execTime, queueWait, totalETA)
	return totalETA
}

// predictRemoteETA = Tnet + Texec (Twait ≈ 0 per assumption 5, section 1.2).
func (e *Engine) predictRemoteETA(profile *apis.ProfileStats, state *apis.ClusterState) float64 {
	return 2.0*float64(state.RTTMs) + profile.P95DurationMs
}

// isFeasible checks if the pod can be scheduled on a given cluster.
func (e *Engine) isFeasible(
	clusterID constants.ClusterID,
	pod *corev1.Pod,
	state *apis.ClusterState,
	reqCPU, reqMem int64,
	eta, deadline float64,
) (bool, string) {
	if constants.IsLocal(clusterID) {
		return e.isLocalFeasible(pod, state, reqCPU, reqMem, eta, deadline)
	}
	return e.isRemoteFeasible(state, reqCPU, reqMem, eta, deadline)
}

// isLocalFeasible checks hard and soft resource constraints for the local cluster.
func (e *Engine) isLocalFeasible(
	pod *corev1.Pod,
	state *apis.ClusterState,
	reqCPU, reqMem int64,
	eta, deadline float64,
) (bool, string) {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))

	// Hard: can the pod fit on the best node at all?
	if state.BestNode.Name == "" ||
		state.BestNode.EffectiveAllocatableCPU < reqCPU ||
		state.BestNode.EffectiveAllocatableMem < reqMem {
		klog.V(4).Infof("%s: Local HARD infeasible: best node can't fit pod", podID)
		return false, constants.ReasonInfeasibleResourcesHard
	}

	// Hard: cluster-wide utilization limit
	const utilizationLimit = 0.90
	if state.EffectiveAllocatableCPU > 0 {
		utilAfter := float64(state.EffectiveAllocatableCPU-state.FreeCPU+reqCPU) / float64(state.EffectiveAllocatableCPU)
		if utilAfter > utilizationLimit {
			klog.V(4).Infof("%s: Local HARD infeasible: utilization %.1f%% > %.1f%%",
				podID, utilAfter*100, utilizationLimit*100)
			return false, constants.ReasonInfeasibleResourcesHard
		}
	}

	// Soft: time
	if eta > deadline {
		return false, constants.ReasonInfeasibleTime
	}

	// Soft: current resources
	if state.FreeCPU < reqCPU || state.FreeMem < reqMem {
		return false, constants.ReasonInfeasibleResourcesSoft
	}

	return true, ""
}

// isRemoteFeasible checks if the pod can be scheduled on a remote cluster.
func (e *Engine) isRemoteFeasible(state *apis.ClusterState, reqCPU, reqMem int64, eta, deadline float64) (bool, string) {
	// Resource check: reqCPU ≤ AvailableCPU_k(t) (thesis §1.2, K_valid)
	if state.FreeCPU < reqCPU || state.FreeMem < reqMem {
		return false, constants.ReasonInfeasibleResourcesSoft
	}
	if eta > deadline {
		return false, constants.ReasonInfeasibleTime
	}
	return true, ""
}

// checkRemoteSafety checks WAN health for a remote cluster.
// Latency-sensitive SLO classes apply a hard RTT/loss gate since they cannot
// tolerate WAN jitter. Batch-oriented classes skip the hard gate and let the
// Lyapunov optimizer weigh WAN overhead against edge resource saturation.
func (e *Engine) checkRemoteSafety(
	podID string,
	clusterID constants.ClusterID,
	state *apis.ClusterState,
	slo *apis.SLO,
) (apis.Result, bool) {
	if state.StaleDuration > 10*time.Minute {
		return apis.Result{}, true // skip stale clusters regardless of class
	}
	switch slo.Class {
	case "latency", "interactive":
		if state.RTTMs > e.config.RTTUnusableMs || state.LossPct > e.config.LossUnusablePct {
			return apis.Result{}, true
		}
	}
	return apis.Result{}, false
}

// checkLocalSafety checks local telemetry health.
func (e *Engine) checkLocalSafety(
	podID string,
	state *apis.ClusterState,
	slo *apis.SLO,
) (apis.Result, bool) {
	staleThresholds := map[string]time.Duration{
		"latency": 5 * time.Second, "interactive": 10 * time.Second,
		"throughput": 30 * time.Second, "streaming": 20 * time.Second,
		"batch": 120 * time.Second,
	}
	threshold := staleThresholds[slo.Class]
	if threshold == 0 {
		threshold = 30 * time.Second
	}

	if state.StaleDuration > threshold {
		return apis.Result{
			Location: constants.LocalCluster,
			Reason:   constants.ReasonTelemetryCircuitBreaker,
		}, true
	}

	if (!state.IsCompleteSnapshot || state.MeasurementConfidence < 0.5) &&
		(slo.Class == "latency" || slo.Class == "interactive") {
		return apis.Result{
			Location: constants.LocalCluster,
			Reason:   constants.ReasonLowMeasurementConf,
		}, true
	}

	return apis.Result{}, false
}

// determineReason provides a human-readable reason for the decision.
func (e *Engine) determineReason(chosen candidateEval, all []candidateEval) string {
	// Count how many candidates are feasible
	feasibleCount := 0
	for _, c := range all {
		if c.feasible {
			feasibleCount++
		}
	}

	if constants.IsLocal(chosen.clusterID) {
		if !chosen.feasible {
			return constants.ReasonLocalBestEffort
		}
		if feasibleCount == 1 {
			return constants.ReasonLocalFeasibleOnly
		}
		return constants.ReasonLocalPreferred
	}

	// Remote
	if !chosen.feasible {
		return constants.ReasonRemoteBestEffort
	}
	if feasibleCount == 1 {
		return constants.ReasonRemoteFeasibleOnly
	}
	return constants.ReasonRemoteFaster
}

func (e *Engine) GetLyapunovScheduler() *LyapunovScheduler { return e.lyapunov }
func (e *Engine) GetProfileStore() *ProfileStore           { return e.config.ProfileStore }

func fmtProfile(p *apis.ProfileStats) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("count=%d conf=%.2f mean=%.0fms p95=%.0fms slo=%.0f%%",
		p.Count, p.ConfidenceScore, p.MeanDurationMs, p.P95DurationMs, p.SLOComplianceRate*100)
}
