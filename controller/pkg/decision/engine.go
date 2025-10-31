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
			FreeCPU:             0,
			FreeMem:             0,
			PendingPodsPerClass: make(map[string]int),
			TotalDemand:         make(map[string]telemetry.DemandByClass),
			BestEdgeNode:        telemetry.BestNode{},
		}
	}

	if local.StaleDuration > 5*time.Minute {
		klog.Errorf("%s: Local telemetry stale >5min, forcing edge-only mode", podID)
		result := Result{constants.Edge, "telemetry_circuit_breaker", 0, wan.RTTMs, 0}
		recordDecision(result, slo.Class)
		return result
	}

	if wan.StaleDuration > 10*time.Minute {
		klog.Errorf("%s: WAN telemetry stale >10min, disabling cloud", podID)
		result := Result{constants.Edge, "wan_circuit_breaker", 0, 999, 0}
		recordDecision(result, slo.Class)
		return result
	}

	reqCPU := util.GetCPURequest(pod)
	reqMem := util.GetMemRequestMi(pod)

	klog.V(4).Infof(
		"%s: Deciding (Lyapunov): req={cpu=%dm mem=%dMi} class=%s prio=%d deadline=%d offload=%v "+
			"wan={rtt=%dms loss=%.1f%%} edge={free=%dm/%dMi alloc=%dm} Z=%.2f",
		podID, reqCPU, reqMem, slo.Class, slo.Priority, slo.DeadlineMs, slo.OffloadAllowed,
		wan.RTTMs, wan.LossPct,
		local.FreeCPU, local.FreeMem, local.TotalAllocatableCPU,
		e.lyapunov.GetVirtualQueue(slo.Class),
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
	edgeETA := e.predictEdgeETA(pod, edgeProfile, local, slo)
	cloudETA := e.predictCloudETA(cloudProfile, wan)

	klog.V(4).Infof("%s: Predicted ETA edge=%.0fms cloud=%.0fms (deadline=%dms)",
		podID, edgeETA, cloudETA, slo.DeadlineMs)

	// Feasibility checks
	deadline := float64(slo.DeadlineMs)
	edgeFeasible := e.isEdgeFeasible(pod, local, reqCPU, reqMem, edgeETA, deadline)
	cloudFeasible := cloudETA <= deadline

	klog.V(4).Infof("%s: Feasibility edge=%v cloud=%v", podID, edgeFeasible, cloudFeasible)

	// Cost calculation
	localCost := e.config.EdgeCostFactor
	cloudCost := e.config.CloudCostFactor

	// Lyapunov decision
	location, weight := e.lyapunov.Decide(
		slo.Class,
		deadline,
		edgeETA,
		cloudETA,
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
	klog.V(3).Infof("%s: Decision: %s (reason=%s, eta=%.0fms, weight=%.2f, Z=%.2f)",
		podID, result.Location, result.Reason, result.PredictedETAMs,
		weight, e.lyapunov.GetVirtualQueue(slo.Class))

	return result
}

// predictEdgeETA predicts completion time on edge using M/G/1 queueing model
func (e *Engine) predictEdgeETA(
	pod *corev1.Pod,
	profile *ProfileStats,
	local *telemetry.LocalState,
	slo *slo.SLO,
) float64 {
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

// isEdgeFeasible checks if pod can physically fit on edge cluster
func (e *Engine) isEdgeFeasible(
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

	// Check 2: Can it eventually fit? (considering other classes)
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

	// Apply headroom (reserve space for new arrivals)
	headroom := float64(availableAfterOthers) * e.config.EdgeHeadroomOverridePct
	effectiveAvailable := availableAfterOthers - int64(headroom)

	if effectiveAvailable < reqCPU {
		klog.V(5).Infof("Edge not feasible: need %dm but only %dm available (after other classes + headroom)",
			reqCPU, effectiveAvailable)
		return false
	}

	// Check 3: Can best node fit it (with headroom)?
	if local.BestEdgeNode.Name != "" {
		headroomMultiplier := 1.0 + e.config.EdgeHeadroomOverridePct
		requiredCPU := int64(float64(reqCPU) * headroomMultiplier)
		requiredMem := int64(float64(reqMem) * headroomMultiplier)

		if local.BestEdgeNode.FreeCPU < requiredCPU || local.BestEdgeNode.FreeMem < requiredMem {
			klog.V(5).Infof("Best edge node cannot fit pod with headroom")
			return false
		}
	}

	return true
}

func (e *Engine) determineReason(location constants.Location, edgeFeasible bool, cloudFeasible bool, edgeETA float64, cloudETA float64) string {
	if location == constants.Edge {
		if edgeFeasible && !cloudFeasible {
			return "edge_feasible_only"
		}
		if edgeFeasible && cloudFeasible {
			if edgeETA <= cloudETA {
				return "lyapunov_edge_preferred"
			}
			return "lyapunov_edge_violation_control"
		}
		return "lyapunov_edge_best_effort"
	}

	// Cloud
	if cloudFeasible && !edgeFeasible {
		return "cloud_feasible_only"
	}
	if cloudFeasible && edgeFeasible {
		if cloudETA < edgeETA {
			return "lyapunov_cloud_faster"
		}
		return "lyapunov_cloud_violation_control"
	}
	return "lyapunov_cloud_best_effort"
}

// GetLyapunovScheduler exposes the Lyapunov scheduler for observer updates
func (e *Engine) GetLyapunovScheduler() *LyapunovScheduler {
	return e.lyapunov
}

func fmtProfile(p *ProfileStats) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("count=%d conf=%.2f mean=%.0fms p95=%.0fms slo=%.0f%%",
		p.Count, p.ConfidenceScore, p.MeanDurationMs, p.P95DurationMs, p.SLOComplianceRate*100)
}
