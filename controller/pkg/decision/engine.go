package decision

import (
	"fmt"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
	"math/rand"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
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
	WanRttMs       int
}

type EngineConfig struct {
	RTTThresholdMs          int
	LossThresholdPct        float64
	RTTUnusableMs           int
	LossUnusablePct         float64
	LocalityBonus           float64
	ConfidenceWeight        float64
	ExplorationRate         float64
	MaxProfileCount         int
	CloudMarginOverridePct  float64 // % of deadline where cloud is preferred if safer
	WanStaleConfFactor      float64 // confidence multiplier when WAN is stale
	EdgeHeadroomOverridePct float64 // % buffer needed on BestNode to allow scheduling
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
			BestEdgeNode:        telemetry.BestNode{},
		}
	}

	// Circuit breaker
	if local.StaleDuration > 5*time.Minute {
		klog.Errorf("%s: Local telemetry stale >5min, forcing edge-only mode", podID)
		result := Result{Edge, "telemetry_circuit_breaker", 0, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if wan.StaleDuration > 10*time.Minute {
		klog.Errorf("%s: WAN telemetry stale >10min, disabling cloud", podID)
		result := Result{Edge, "wan_circuit_breaker", 0, 999}
		recordDecision(result, slo.Class)
		return result
	}

	// Observability
	defer func(start time.Time) {
		decisionLatency.Observe(time.Since(start).Seconds())
	}(time.Now())

	reqCPU := getCPURequest(pod)
	reqMem := getMemRequestMi(pod)
	pending := local.PendingPodsPerClass[slo.Class]

	klog.V(4).Infof(
		"%s: Deciding scheduling: req={cpu=%dm mem=%dMi} class=%s prio=%d deadline=%d offload=%v wan={rtt=%dms loss=%.1f%% stale=%v} edge={clusterFreeCPU=%dm clusterFreeMem=%dMi pending[%s]=%d bestNode=%s(freeCPU=%dm freeMem=%dMi)}",
		podID,
		reqCPU, reqMem,
		slo.Class, slo.Priority, slo.DeadlineMs, slo.OffloadAllowed,
		wan.RTTMs, wan.LossPct, wan.IsStale,
		local.FreeCPU, local.FreeMem, slo.Class, pending,
		local.BestEdgeNode.Name, local.BestEdgeNode.FreeCPU, local.BestEdgeNode.FreeMem,
	)

	// 1. Hard safety constraints
	if !slo.OffloadAllowed {
		klog.V(4).Infof("%s: Offload disabled by SLO", podID)
		result := Result{Edge, "offload_disabled", 0, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if wan.RTTMs > e.config.RTTUnusableMs || wan.LossPct > e.config.LossUnusablePct {
		klog.V(4).Infof("%s: WAN deemed unusable (rtt=%d>%d or loss=%.1f>%.1f)",
			podID, wan.RTTMs, e.config.RTTUnusableMs, wan.LossPct, e.config.LossUnusablePct)
		result := Result{Edge, "wan_unusable", 0, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	// Check telemetry staleness and adjust confidence
	confidenceAdjustment := 1.0
	cloudConfAdjustment := 1.0
	if wan.IsStale {
		klog.V(4).Infof("%s: WAN telemetry is stale, reducing confidence by %.1f%%",
			podID, (1-e.config.WanStaleConfFactor)*100)
		cloudConfAdjustment *= e.config.WanStaleConfFactor
	}
	if local.IsStale {
		klog.V(4).Infof("%s: Local telemetry is stale, reducing confidence", podID)
		confidenceAdjustment *= 0.7
	}

	// 2. Get profiles
	edgeKey := GetProfileKey(pod, Edge)
	cloudKey := GetProfileKey(pod, Cloud)
	edgeProfile := e.config.ProfileStore.GetOrDefault(edgeKey)
	cloudProfile := e.config.ProfileStore.GetOrDefault(cloudKey)

	klog.V(4).Infof("%s: Profiles edge[%s]: %s | cloud[%s]: %s",
		podID, edgeKey.String(), fmtProfile(edgeProfile), cloudKey.String(), fmtProfile(cloudProfile))

	// 3. Predict ETA
	edgeETA := e.predictETA(pod, Edge, edgeProfile, local, wan)
	cloudETA := e.predictETA(pod, Cloud, cloudProfile, local, wan)
	klog.V(4).Infof("%s: ETA edge=%s cloud=%s", podID, fmtETA(edgeETA), fmtETA(cloudETA))

	// 4. Exploration with configurable rate
	edgeConf := edgeProfile.ConfidenceScore * confidenceAdjustment
	cloudConf := cloudProfile.ConfidenceScore * cloudConfAdjustment
	explorationBonus := 0.0

	if edgeConf < 0.5 || cloudConf < 0.5 {
		if rand.Float64() < e.config.ExplorationRate {
			explorationBonus = 50.0
			klog.V(5).Infof("%s: Exploration bonus applied: +%.0fms", podID, explorationBonus)
		}
	}

	// 5. Feasibility checks
	nodeOK := false
	if local.BestEdgeNode.Name != "" {
		nodeOK = local.BestEdgeNode.FreeCPU >= reqCPU && local.BestEdgeNode.FreeMem >= reqMem
	}

	if local.FreeCPU < reqCPU || local.FreeMem < reqMem {
		klog.V(4).Infof("%s: Cluster-wide free below request; treating edge as capacity-constrained", podID)
		nodeOK = false
	}

	edgeFeasible := edgeETA.P95 <= float64(slo.DeadlineMs) && nodeOK
	cloudFeasible := (cloudETA.P95 + explorationBonus) <= float64(slo.DeadlineMs)

	klog.V(5).Infof("%s: Feasibility edge=%v cloud=%v (deadline=%dms, reqCPU=%dm, reqMem=%dMi)",
		podID, edgeFeasible, cloudFeasible, slo.DeadlineMs, reqCPU, reqMem)

	// 6. Decision logic
	if edgeFeasible && !cloudFeasible {
		result := Result{Edge, "edge_feasible_only", edgeETA.Mean, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if cloudFeasible && !edgeFeasible {
		result := Result{Cloud, "cloud_feasible_only", cloudETA.Mean, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if edgeFeasible && cloudFeasible {
		// Margin override: if cloud's deadline margin is significantly better than edge's,
		// prefer cloud despite locality bonus
		deadline := float64(slo.DeadlineMs)
		edgeMargin := deadline - edgeETA.P95
		cloudMargin := deadline - cloudETA.P95
		marginDiff := cloudMargin - edgeMargin
		marginThreshold := deadline * e.config.CloudMarginOverridePct

		if marginDiff >= marginThreshold {
			result := Result{Cloud, "cloud_margin_override", cloudETA.Mean, wan.RTTMs}
			recordDecision(result, slo.Class)
			return result
		}

		// Sanity check: if BestEdgeNode has insufficient headroom, prefer cloud
		if !e.canNodeFitWithHeadroom(local.BestEdgeNode, reqCPU, reqMem) {
			result := Result{Cloud, "edge_low_headroom", cloudETA.Mean, wan.RTTMs}
			recordDecision(result, slo.Class)
			return result
		}

		edgeScore := e.computeScore(Edge, edgeETA, edgeConf, slo)
		cloudScore := e.computeScore(Cloud, cloudETA, cloudConf, slo)
		klog.V(5).Infof("%s: Scores edge=%.1f cloud=%.1f", podID, edgeScore, cloudScore)

		if edgeScore >= cloudScore {
			result := Result{Edge, "edge_preferred", edgeETA.Mean, wan.RTTMs}
			recordDecision(result, slo.Class)
			return result
		}

		result := Result{Cloud, "cloud_faster", cloudETA.Mean, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	// 7. Neither feasible - best effort
	if slo.Priority >= 7 && cloudETA.Mean < edgeETA.Mean {
		result := Result{Cloud, "best_effort_cloud", cloudETA.Mean, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	result := Result{Edge, "best_effort_edge", edgeETA.Mean, wan.RTTMs}
	recordDecision(result, slo.Class)
	return result
}

func (e *Engine) predictETA(
	pod *corev1.Pod,
	loc Location,
	profile *ProfileStats,
	local *telemetry.LocalState,
	wan *telemetry.WANState,
) ETAEstimate {
	execTime := profile.MeanDurationMs
	execTimeP95 := profile.P95DurationMs

	queueWaitMean := 0.0
	queueWaitP95 := 0.0
	if loc == Edge {
		class := pod.Annotations[constants.AnnotationSLOClass]
		pendingCount := local.PendingPodsPerClass[class]
		queueWaitMean = float64(pendingCount) * profile.MeanDurationMs
		queueWaitP95 = float64(pendingCount) * profile.P95DurationMs
	}

	wanOverhead := 0.0
	if loc == Cloud {
		wanOverhead = 2.0 * float64(wan.RTTMs)
	}

	return ETAEstimate{
		Mean: queueWaitMean + execTime + wanOverhead,
		P95:  queueWaitP95 + execTimeP95 + wanOverhead,
	}
}

func (e *Engine) computeScore(
	loc Location,
	eta ETAEstimate,
	confidence float64,
	slo *slo.SLO,
) float64 {
	score := 0.0

	if loc == Edge {
		score += e.config.LocalityBonus
	}

	deadlineMargin := float64(slo.DeadlineMs) - eta.P95
	score += deadlineMargin * 0.5

	score += confidence * e.config.ConfidenceWeight

	if slo.Priority >= 8 && loc == Edge {
		score += 20.0
	}

	return score
}

func getCPURequest(pod *corev1.Pod) int64 {
	var total int64
	for _, c := range pod.Spec.Containers {
		total += c.Resources.Requests.Cpu().MilliValue()
	}
	return total
}

func getMemRequestMi(pod *corev1.Pod) int64 {
	var total int64
	for _, c := range pod.Spec.Containers {
		total += c.Resources.Requests.Memory().Value() / (1024 * 1024)
	}
	return total
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

func fmtETA(e ETAEstimate) string {
	return fmt.Sprintf("mean=%.0fms p95=%.0fms", e.Mean, e.P95)
}

func fmtProfile(p *ProfileStats) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("count=%d conf=%.2f mean=%.0fms p95=%.0fms slo=%.0f%%",
		p.Count, p.ConfidenceScore, p.MeanDurationMs, p.P95DurationMs, p.SLOComplianceRate*100)
}
