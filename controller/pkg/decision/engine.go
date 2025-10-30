package decision

import (
	"fmt"
	"math"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
)

type Result struct {
	Location       constants.Location
	Reason         string
	PredictedETAMs float64
	WanRttMs       int
}

type EngineConfig struct {
	RTTUnusableMs           int
	LossUnusablePct         float64
	LocalityBonus           float64
	ConfidenceWeight        float64
	ExplorationRate         float64
	MaxProfileCount         int
	CloudMarginOverridePct  float64
	WanStaleConfFactor      float64
	EdgeHeadroomOverridePct float64
	EdgePendingPessimismPct int
	ProfileStore            *ProfileStore
}

type Engine struct {
	config EngineConfig
}

func NewEngine(config EngineConfig) *Engine {
	if config.ProfileStore == nil {
		klog.Fatal("ProfileStore must be provided")
	}
	return &Engine{config: config}
}

func (e *Engine) Decide(
	pod *corev1.Pod,
	slo *slo.SLO,
	local *telemetry.LocalState,
	wan *telemetry.WANState,
) Result {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))
	if wan == nil {
		klog.Warningf("%s: WAN state is nil, using pessimistic defaults", podID)
		wan = &telemetry.WANState{RTTMs: 999, LossPct: 100, IsStale: true}
	}
	if local == nil {
		klog.Warningf("%s: Local state is nil, using empty state", podID)
		local = &telemetry.LocalState{
			FreeCPU:             0,
			FreeMem:             0,
			PendingPodsPerClass: make(map[string]int),
			TotalDemand:         make(map[string]telemetry.DemandByClass),
			BestEdgeNode:        telemetry.BestNode{},
		}
	}

	// Circuit breakers
	if local.StaleDuration > 5*time.Minute {
		klog.Errorf("%s: Local telemetry stale >5min, forcing edge-only mode", podID)
		result := Result{constants.Edge, "telemetry_circuit_breaker", 0, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if wan.StaleDuration > 10*time.Minute {
		klog.Errorf("%s: WAN telemetry stale >10min, disabling cloud", podID)
		result := Result{constants.Edge, "wan_circuit_breaker", 0, 999}
		recordDecision(result, slo.Class)
		return result
	}

	reqCPU := util.GetCPURequest(pod)
	reqMem := util.GetMemRequestMi(pod)

	klog.V(4).Infof(
		"%s: Deciding scheduling: req={cpu=%dm mem=%dMi} class=%s prio=%d deadline=%d offload=%v wan={rtt=%dms loss=%.1f%% stale=%v} edge={clusterFreeCPU=%dm clusterFreeMem=%dMi totalAlloc=%dm}",
		podID,
		reqCPU, reqMem,
		slo.Class, slo.Priority, slo.DeadlineMs, slo.OffloadAllowed,
		wan.RTTMs, wan.LossPct, wan.IsStale,
		local.FreeCPU, local.FreeMem, local.TotalAllocatableCPU,
	)

	// Hard safety constraints
	if !slo.OffloadAllowed {
		klog.V(4).Infof("%s: Offload disabled by SLO", podID)
		result := Result{constants.Edge, "offload_disabled", 0, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if wan.RTTMs > e.config.RTTUnusableMs || wan.LossPct > e.config.LossUnusablePct {
		klog.V(4).Infof("%s: WAN deemed unusable (rtt=%d>%d or loss=%.1f>%.1f)",
			podID, wan.RTTMs, e.config.RTTUnusableMs, wan.LossPct, e.config.LossUnusablePct)
		result := Result{constants.Edge, "wan_unusable", 0, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	// Get profiles for historical execution time
	edgeKey := GetProfileKey(pod, constants.Edge)
	cloudKey := GetProfileKey(pod, constants.Cloud)
	edgeProfile := e.config.ProfileStore.GetOrDefault(edgeKey)
	cloudProfile := e.config.ProfileStore.GetOrDefault(cloudKey)

	klog.V(4).Infof("%s: Profiles edge[%s]: %s | cloud[%s]: %s",
		podID, edgeKey.String(), fmtProfile(edgeProfile), cloudKey.String(), fmtProfile(cloudProfile))

	// Predict time-to-completion using 1/(1-ρ) slowdown
	edgeETA := e.predictETA(pod, constants.Edge, edgeProfile, local, wan, slo)
	cloudETA := e.predictETA(pod, constants.Cloud, cloudProfile, local, wan, slo)

	klog.V(4).Infof("%s: ETA edge=%.0fms cloud=%.0fms", podID, edgeETA, cloudETA)

	// Confidence adjustment for staleness
	cloudConfAdjustment := 1.0
	if wan.IsStale {
		klog.V(4).Infof("%s: WAN telemetry is stale, reducing confidence by %.1f%%",
			podID, (1-e.config.WanStaleConfFactor)*100)
		cloudConfAdjustment *= e.config.WanStaleConfFactor
	}

	edgeConf := edgeProfile.ConfidenceScore
	cloudConf := cloudProfile.ConfidenceScore * cloudConfAdjustment

	// Feasibility checks
	edgeHasNode := e.canNodeFitWithHeadroom(local.BestEdgeNode, reqCPU, reqMem)
	edgeHasCapacity := local.FreeCPU >= reqCPU && local.FreeMem >= reqMem

	if !edgeHasCapacity || !edgeHasNode {
		// Compare edge queue drain time to cloud cost
		queueDrainTime := e.estimateEdgeQueueDrainTime(local, slo.Class, edgeProfile)

		cloudETA := e.predictETA(pod, constants.Cloud, cloudProfile, local, wan, slo)

		klog.V(4).Infof(
			"%s: Edge capacity insufficient. Queue drain time=%.0fms vs Cloud ETA=%.0fms",
			podID, queueDrainTime, cloudETA,
		)

		// If waiting on edge queue is significantly faster than cloud offload,
		// keep it queued on edge (return Edge decision, don't force cloud)
		queueBenefit := cloudETA - queueDrainTime
		if queueBenefit > 200 { // >200ms faster to wait on edge
			klog.V(3).Infof(
				"%s: Queuing on edge (benefit=%.0fms). Queue drain=%.0fms < Cloud ETA=%.0fms",
				podID, queueBenefit, queueDrainTime, cloudETA,
			)
			result := Result{
				constants.Edge,
				"edge_queue_preferred",
				queueDrainTime + edgeProfile.P95DurationMs,
				wan.RTTMs,
			}
			recordDecision(result, slo.Class)
			return result
		}

		// Otherwise, cloud is worth it despite the capacity issue
		klog.Warningf(
			"%s: Edge overloaded and cloud is faster. Forcing cloud (drain=%.0fms, cloud=%.0fms)",
			podID, queueDrainTime, cloudETA,
		)
		result := Result{constants.Cloud, "edge_overloaded_cloud_faster", cloudETA, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	edgeFeasible := edgeETA <= float64(slo.DeadlineMs)
	cloudFeasible := cloudETA <= float64(slo.DeadlineMs)

	klog.V(5).Infof("%s: Feasibility edge=%v cloud=%v (deadline=%dms, reqCPU=%dm, reqMem=%dMi)",
		podID, edgeFeasible, cloudFeasible, slo.DeadlineMs, reqCPU, reqMem)

	// Decision logic
	if edgeFeasible && !cloudFeasible {
		result := Result{constants.Edge, "edge_feasible_only", edgeETA, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if cloudFeasible && !edgeFeasible {
		result := Result{constants.Cloud, "cloud_feasible_only", cloudETA, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if edgeFeasible && cloudFeasible {
		// Margin override logic
		deadline := float64(slo.DeadlineMs)
		edgeMargin := deadline - edgeETA
		cloudMargin := deadline - cloudETA
		marginDiff := cloudMargin - edgeMargin
		marginThreshold := deadline * e.config.CloudMarginOverridePct

		if marginDiff >= marginThreshold {
			result := Result{constants.Cloud, "cloud_margin_override", cloudETA, wan.RTTMs}
			recordDecision(result, slo.Class)
			return result
		}

		// Score-based comparison
		edgeScore := e.computeScore(constants.Edge, edgeETA, edgeConf, slo, deadline)
		cloudScore := e.computeScore(constants.Cloud, cloudETA, cloudConf, slo, deadline)
		klog.V(5).Infof("%s: Scores edge=%.1f cloud=%.1f", podID, edgeScore, cloudScore)

		if edgeScore >= cloudScore {
			result := Result{constants.Edge, "edge_preferred", edgeETA, wan.RTTMs}
			recordDecision(result, slo.Class)
			return result
		}

		result := Result{constants.Cloud, "cloud_faster", cloudETA, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	// Neither feasible - best effort
	if slo.Priority >= 7 && cloudETA < edgeETA {
		result := Result{constants.Cloud, "best_effort_cloud", cloudETA, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	result := Result{constants.Edge, "best_effort_edge", edgeETA, wan.RTTMs}
	recordDecision(result, slo.Class)
	return result
}

// predictETA implements the Universal Resource Contention Slowdown Model
// Based on the proven queueing theory formula: T_actual = T_ideal * (1 / (1 - ρ_eff))
func (e *Engine) predictETA(
	pod *corev1.Pod,
	loc constants.Location,
	profile *ProfileStats,
	local *telemetry.LocalState,
	wan *telemetry.WANState,
	slo *slo.SLO,
) float64 {
	// Step 1: Get ideal execution time from profile (P95 for conservative estimate)
	idealExecTime := profile.P95DurationMs

	// Step 2: For cloud, add network overhead immediately
	if loc == constants.Cloud {
		return idealExecTime + 2.0*float64(wan.RTTMs)
	}

	// Step 3: For edge, compute slowdown factor based on resource contention

	// Determine pod's priority tier
	podTier := constants.GetTierForClass(slo.Class)

	// Step 4: Calculate effective demand (only higher/equal priority contends)
	var effectiveDemandCPU int64 = 0
	for class, demand := range local.TotalDemand {
		if constants.GetTierForClass(class) <= podTier {
			effectiveDemandCPU += demand.CPU
		}
	}

	// Step 5: Calculate effective utilization (ρ_eff)
	totalCapacityCPU := local.TotalAllocatableCPU
	if totalCapacityCPU == 0 {
		// No capacity, cannot run
		return math.Inf(1)
	}

	rhoEff := float64(effectiveDemandCPU) / float64(totalCapacityCPU)

	// Step 6: Calculate slowdown factor using 1/(1-ρ)
	// Clamp to 0.99 to prevent division by zero and represent extreme but finite slowdown
	if rhoEff >= 1.0 {
		rhoEff = 0.99
	}
	slowdownFactor := 1.0 / (1.0 - rhoEff)

	// Step 7: Calculate final predicted time to completion
	predictedTimeToCompletion := idealExecTime * slowdownFactor

	klog.V(5).Infof("Universal model: class=%s tier=%d effectiveDemand=%dm/%dm rho=%.3f slowdown=%.2fx ideal=%.0fms predicted=%.0fms",
		slo.Class, podTier, effectiveDemandCPU, totalCapacityCPU, rhoEff, slowdownFactor, idealExecTime, predictedTimeToCompletion)

	return predictedTimeToCompletion
}

func (e *Engine) computeScore(
	loc constants.Location,
	eta float64,
	confidence float64,
	slo *slo.SLO,
	deadline float64,
) float64 {
	score := 0.0

	if loc == constants.Edge {
		score += e.config.LocalityBonus
	}

	deadlineMargin := deadline - eta
	score += deadlineMargin * 0.5

	score += confidence * e.config.ConfidenceWeight

	if slo.Priority >= 8 && loc == constants.Edge {
		score += 20.0
	}

	return score
}

// estimateEdgeQueueDrainTime estimates how long the current queue will take to clear
func (e *Engine) estimateEdgeQueueDrainTime(
	local *telemetry.LocalState,
	podClass string,
	profile *ProfileStats,
) float64 {
	// Get the number of pending pods in this class
	pendingCount := local.PendingPodsPerClass[podClass]
	if pendingCount == 0 {
		// No queue, pod can start immediately (well, after current pods)
		// Estimate as average pod duration to be conservative
		return profile.MeanDurationMs
	}

	// Estimate parallelism: how many pods can run concurrently on edge?
	// Conservative approach: assume 1-4 pods in parallel based on cluster size
	avgPodCPU := int64(200) // Assume 200m per pod on average
	parallelism := int64(1)

	if local.TotalAllocatableCPU > 0 {
		// Pods that can run in parallel
		parallelism = local.TotalAllocatableCPU / avgPodCPU
		if parallelism < 1 {
			parallelism = 1
		}
		if parallelism > 8 {
			parallelism = 8 // Cap at 8 to be conservative
		}
	}

	// Rough queue drain time
	// Assume each pending pod takes meanDuration to run
	queueDrainSeconds := float64(pendingCount) * profile.MeanDurationMs / 1000.0 / float64(parallelism)
	queueDrainMs := queueDrainSeconds * 1000

	klog.V(5).Infof(
		"Queue drain estimate: pending=%d pods × %.0fms / %d parallelism = %.0fms",
		pendingCount, profile.MeanDurationMs, parallelism, queueDrainMs,
	)

	return queueDrainMs
}

func (e *Engine) canNodeFitWithHeadroom(node telemetry.BestNode, reqCPU, reqMem int64) bool {
	if node.Name == "" {
		return false
	}

	headroomMultiplier := 1.0 + e.config.EdgeHeadroomOverridePct
	requiredCPU := int64(float64(reqCPU) * headroomMultiplier)
	requiredMem := int64(float64(reqMem) * headroomMultiplier)

	fits := node.FreeCPU >= requiredCPU && node.FreeMem >= requiredMem
	if !fits {
		klog.V(5).Infof("Node %s headroom check failed: have CPU=%d (need %d), mem=%d (need %d)",
			node.Name, node.FreeCPU, requiredCPU, node.FreeMem, requiredMem)
	}
	return fits
}

func fmtProfile(p *ProfileStats) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("count=%d conf=%.2f mean=%.0fms p95=%.0fms slo=%.0f%%",
		p.Count, p.ConfidenceScore, p.MeanDurationMs, p.P95DurationMs, p.SLOComplianceRate*100)
}
