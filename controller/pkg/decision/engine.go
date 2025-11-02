package decision

import (
	"fmt"
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
	LyapunovWeight float64 // Expose weight for debugging
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
	LyapunovScheduler       *LyapunovScheduler

	// Lyapunov configuration
	CloudCostFactor float64 // Relative cost of cloud vs edge (default: 1.0)
	EdgeCostFactor  float64 // Relative cost of edge (default: 0.0)
}

type Engine struct {
	config   EngineConfig
	lyapunov *LyapunovScheduler
}

func NewEngine(config EngineConfig) *Engine {
	if config.ProfileStore == nil {
		klog.Fatal("ProfileStore must be provided")
	}

	if config.LyapunovScheduler == nil {
		klog.Fatal("LyapunovScheduler must be provided")
	}

	// Set cost defaults if not provided
	if config.CloudCostFactor == 0 {
		config.CloudCostFactor = 1.0
	}

	return &Engine{
		config:   config,
		lyapunov: config.LyapunovScheduler,
	}
}

func (e *Engine) Decide(
	pod *corev1.Pod,
	slo *slo.SLO,
	local *telemetry.LocalState,
	wan *telemetry.WANState,
) Result {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))

	// Circuit breakers and safety checks
	if wan == nil {
		klog.Warningf("%s: WAN state is nil, using pessimistic defaults", podID)
		wan = &telemetry.WANState{RTTMs: 999, LossPct: 100, IsStale: true}
	}
	if local == nil {
		klog.Warningf("%s: Local state is nil, using empty state", podID)
		local = &telemetry.LocalState{
			FreeCPU:               0,
			FreeMem:               0,
			PendingPodsPerClass:   make(map[string]int),
			TotalDemand:           make(map[string]telemetry.DemandByClass),
			BestEdgeNode:          telemetry.BestNode{},
			MeasurementConfidence: 0.0,
		}
	}

	// Class-specific staleness thresholds
	classStaleThreshold := map[string]time.Duration{
		"latency":     5 * time.Second,
		"interactive": 10 * time.Second,
		"throughput":  30 * time.Second,
		"streaming":   20 * time.Second,
		"batch":       120 * time.Second,
	}

	threshold, ok := classStaleThreshold[slo.Class]
	if !ok {
		threshold = 30 * time.Second
	}

	if wan.StaleDuration > 10*time.Minute {
		klog.Errorf("%s: WAN telemetry stale >10min, disabling cloud", podID)
		result := Result{constants.Edge, "wan_circuit_breaker", 0, 999, 0}
		recordDecision(result, slo.Class)
		return result
	}

	if local.StaleDuration > threshold {
		klog.Errorf("%s: Local telemetry stale >%v for class %s, forcing cloud",
			podID, threshold, slo.Class)
		result := Result{constants.Cloud, "telemetry_circuit_breaker", 0, wan.RTTMs, 0}
		recordDecision(result, slo.Class)
		return result
	}

	// Incomplete snapshots or low measurement quality check
	if (!local.IsCompleteSnapshot || local.MeasurementConfidence < 0.5) &&
		(slo.Class == "latency" || slo.Class == "interactive") {
		klog.Warningf("%s: Low measurement confidence (%.2f), using conservative decision",
			podID, local.MeasurementConfidence)
		result := Result{constants.Cloud, "low_measurement_confidence", 0, wan.RTTMs, 0}
		recordDecision(result, slo.Class)
		return result
	}

	reqCPU := util.GetCPURequest(pod)
	reqMem := util.GetMemRequestMi(pod)

	klog.V(4).Infof(
		"%s: Deciding (Lyapunov): req={cpu=%dm mem=%dMi} class=%s prio=%d deadline=%d offload=%v "+
			"wan={rtt=%dms loss=%.1f%%} edge={free=%dm/%dMi alloc=%dm conf=%.2f} Z=%.2f Zp=%.2f",
		podID, reqCPU, reqMem, slo.Class, slo.Priority, slo.DeadlineMs, slo.OffloadAllowed,
		wan.RTTMs, wan.LossPct,
		local.FreeCPU, local.FreeMem, local.TotalAllocatableCPU,
		local.MeasurementConfidence,
		e.lyapunov.GetVirtualQueue(slo.Class),
		e.lyapunov.GetVirtualProbQueue(slo.Class),
	)

	// Hard constraints
	if !slo.OffloadAllowed {
		klog.V(4).Infof("%s: Offload disabled by SLO", podID)
		result := Result{constants.Edge, "offload_disabled", 0, wan.RTTMs, 0}
		recordDecision(result, slo.Class)
		return result
	}

	if wan.RTTMs > e.config.RTTUnusableMs || wan.LossPct > e.config.LossUnusablePct {
		klog.V(4).Infof("%s: WAN deemed unusable", podID)
		result := Result{constants.Edge, "wan_unusable", 0, wan.RTTMs, 0}
		recordDecision(result, slo.Class)
		return result
	}

	// Get execution profiles
	edgeKey := GetProfileKey(pod, constants.Edge)
	cloudKey := GetProfileKey(pod, constants.Cloud)
	edgeProfile := e.config.ProfileStore.GetOrDefault(edgeKey)
	cloudProfile := e.config.ProfileStore.GetOrDefault(cloudKey)

	klog.V(4).Infof("%s: Profiles edge[%s]: %s | cloud[%s]: %s",
		podID, edgeKey.String(), fmtProfile(edgeProfile),
		cloudKey.String(), fmtProfile(cloudProfile))

	// Predict completion times
	edgeETA := e.predictEdgeETA(edgeProfile, local, slo)
	cloudETA := e.predictCloudETA(cloudProfile, wan)

	klog.V(4).Infof("%s: Predicted ETA edge=%.0fms cloud=%.0fms (deadline=%dms)",
		podID, edgeETA, cloudETA, slo.DeadlineMs)

	// Feasibility checks
	deadline := float64(slo.DeadlineMs)
	edgeFeasible := e.isEdgeFeasibleWithConfidence(pod, local, reqCPU, reqMem, edgeETA, deadline)
	cloudFeasible := cloudETA <= deadline

	klog.V(4).Infof("%s: Feasibility edge=%v cloud=%v", podID, edgeFeasible, cloudFeasible)

	// Cost calculation
	localCost := e.config.EdgeCostFactor
	cloudCost := e.config.CloudCostFactor

	// Lyapunov decision with profiles for probability estimation
	location, weight := e.lyapunov.Decide(
		slo.Class,
		deadline,
		edgeETA,
		cloudETA,
		edgeProfile,
		cloudProfile,
		edgeFeasible,
		cloudFeasible,
		localCost,
		cloudCost,
	)

	// Determine reason based on feasibility and choice
	reason := e.determineReason(location, edgeFeasible, cloudFeasible, edgeETA, cloudETA)
	predictedETA := edgeETA
	if location == constants.Cloud {
		predictedETA = cloudETA
	}

	result := Result{
		Location:       location,
		Reason:         reason,
		PredictedETAMs: predictedETA,
		WanRttMs:       wan.RTTMs,
		LyapunovWeight: weight,
	}

	recordDecision(result, slo.Class)
	klog.V(3).Infof("%s: Decision: %s (reason=%s, eta=%.0fms, weight=%.2f, Z=%.2f, Zp=%.2f, conf=%.2f)",
		podID, result.Location, result.Reason, result.PredictedETAMs,
		weight, e.lyapunov.GetVirtualQueue(slo.Class), e.lyapunov.GetVirtualProbQueue(slo.Class),
		local.MeasurementConfidence)

	return result
}

// Confidence-aware feasibility check
func (e *Engine) isEdgeFeasibleWithConfidence(
	pod *corev1.Pod,
	local *telemetry.LocalState,
	reqCPU int64,
	reqMem int64,
	edgeETA float64,
	deadline float64,
) bool {
	// Check 1: Does it meet deadline?
	if edgeETA > deadline {
		return false
	}

	// Check 2: Adjust headroom based on measurement confidence
	baseHeadroom := e.config.EdgeHeadroomOverridePct

	// Lower confidence → higher headroom (more conservative)
	// confidence=1.0 → 1.0x headroom
	// confidence=0.5 → 2.0x headroom
	// confidence=0.0 → 3.0x headroom
	confidenceFactor := 1.0 + (2.0 * (1.0 - local.MeasurementConfidence))
	adjustedHeadroom := baseHeadroom * confidenceFactor

	klog.V(5).Infof("Adjusted headroom: base=%.2f%% confidence=%.2f adjusted=%.2f%%",
		baseHeadroom*100, local.MeasurementConfidence, adjustedHeadroom*100)

	// Calculate demand from OTHER classes
	podClass := pod.Annotations[constants.AnnotationSLOClass]
	if podClass == "" {
		podClass = constants.DefaultSLOClass
	}

	var otherClassDemandCPU int64
	for class, demand := range local.TotalDemand {
		if class != podClass {
			otherClassDemandCPU += demand.CPU
		}
	}

	// Available capacity after other classes
	availableAfterOthers := local.TotalAllocatableCPU - otherClassDemandCPU

	// Apply adjusted headroom
	headroom := float64(availableAfterOthers) * adjustedHeadroom
	effectiveAvailable := availableAfterOthers - int64(headroom)

	if effectiveAvailable < reqCPU {
		klog.V(5).Infof("Edge not feasible: need %dm but only %dm available (after other classes + %.1f%% headroom)",
			reqCPU, effectiveAvailable, adjustedHeadroom*100)
		return false
	}

	// Check 3: Can best node fit it (with adjusted headroom)?
	if local.BestEdgeNode.Name != "" {
		headroomMultiplier := 1.0 + adjustedHeadroom
		requiredCPU := int64(float64(reqCPU) * headroomMultiplier)
		requiredMem := int64(float64(reqMem) * headroomMultiplier)

		if local.BestEdgeNode.FreeCPU < requiredCPU || local.BestEdgeNode.FreeMem < requiredMem {
			klog.V(5).Infof("Best edge node cannot fit pod with adjusted headroom")
			return false
		}
	}

	return true
}

// predictEdgeETA predicts completion time on edge using M/G/1 queueing model
func (e *Engine) predictEdgeETA(profile *ProfileStats, local *telemetry.LocalState, slo *slo.SLO) float64 {
	// Base execution time (P95 for conservative estimate)
	execTime := profile.P95DurationMs

	// Estimate queue wait using M/G/1 Pollaczek-Khinchine formula
	pendingCount := local.PendingPodsPerClass[slo.Class]
	if pendingCount == 0 {
		// No queue, just execution time
		return execTime
	}

	// Rough parallelism estimate
	avgPodCPU := int64(200) // Assume 200m average
	parallelism := int64(1)
	if local.FreeCPU > 0 {
		parallelism = local.FreeCPU / avgPodCPU
		if parallelism < 1 {
			parallelism = 1
		}
		if parallelism > 8 {
			parallelism = 8
		}
	}

	// Simple queue drain estimate
	queueWait := float64(pendingCount) * profile.MeanDurationMs / float64(parallelism)

	// Add variance buffer for high utilization
	if local.TotalAllocatableCPU > 0 {
		utilization := float64(local.TotalAllocatableCPU-local.FreeCPU) / float64(local.TotalAllocatableCPU)
		if utilization > 0.7 {
			// Add buffer: higher utilization → higher variance
			buffer := profile.StdDevDurationMs * (utilization - 0.7) * 10
			queueWait += buffer
		}
	}

	return queueWait + execTime
}

// predictCloudETA predicts completion time on cloud
func (e *Engine) predictCloudETA(
	profile *ProfileStats,
	wan *telemetry.WANState,
) float64 {
	// Cloud: WAN transfer + execution (assume no queue)
	return 2.0*float64(wan.RTTMs) + profile.P95DurationMs
}

func (e *Engine) determineReason(location constants.Location, edgeFeasible bool, cloudFeasible bool, edgeETA float64, cloudETA float64) string {
	if location == constants.Edge {
		if edgeFeasible && !cloudFeasible {
			return "edge_feasible_only"
		}
		if edgeFeasible && cloudFeasible {
			if edgeETA <= cloudETA {
				return "edge_preferred"
			}
			return "edge_violation_control"
		}
		return "edge_best_effort"
	}

	// Cloud
	if cloudFeasible && !edgeFeasible {
		return "cloud_feasible_only"
	}
	if cloudFeasible && edgeFeasible {
		if cloudETA < edgeETA {
			return "cloud_faster"
		}
		return "cloud_violation_control"
	}
	return "cloud_best_effort"
}

// GetLyapunovScheduler exposes the Lyapunov scheduler for observer updates
func (e *Engine) GetLyapunovScheduler() *LyapunovScheduler {
	return e.lyapunov
}

// GetProfileStore exposes the profile store for testing
func (e *Engine) GetProfileStore() *ProfileStore {
	return e.config.ProfileStore
}

func fmtProfile(p *ProfileStats) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("count=%d conf=%.2f mean=%.0fms p95=%.0fms slo=%.0f%%",
		p.Count, p.ConfidenceScore, p.MeanDurationMs, p.P95DurationMs, p.SLOComplianceRate*100)
}
