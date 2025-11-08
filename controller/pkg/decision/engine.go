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
	CloudCostFactor         float64
	EdgeCostFactor          float64
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
	slo *apis.SLO,
	local *apis.LocalState,
	wan *apis.WANState,
) apis.Result {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))

	// Circuit breakers and safety checks
	if wan == nil {
		klog.Warningf("%s: WAN state is nil, using pessimistic defaults", podID)
		wan = &apis.WANState{RTTMs: 999, LossPct: 100, IsStale: true}
	}
	if local == nil {
		klog.Warningf("%s: Local state is nil, using empty state", podID)
		local = &apis.LocalState{
			FreeCPU:               0,
			FreeMem:               0,
			PendingPodsPerClass:   make(map[string]int),
			TotalDemand:           make(map[string]apis.DemandByClass),
			BestEdgeNode:          apis.BestNode{},
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
		result := apis.Result{Location: constants.Edge, Reason: "wan_circuit_breaker", WanRttMs: 999}
		recordDecision(result, slo.Class)
		return result
	}

	if local.StaleDuration > threshold {
		klog.Errorf("%s: Local telemetry stale >%v for class %s, forcing cloud",
			podID, threshold, slo.Class)
		result := apis.Result{Location: constants.Cloud, Reason: "telemetry_circuit_breaker", WanRttMs: wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	// Incomplete snapshots or low measurement quality check
	if (!local.IsCompleteSnapshot || local.MeasurementConfidence < 0.5) &&
		(slo.Class == "latency" || slo.Class == "interactive") {
		klog.Warningf("%s: Low measurement confidence (%.2f), using conservative decision",
			podID, local.MeasurementConfidence)
		result := apis.Result{Location: constants.Cloud, Reason: "low_measurement_confidence", WanRttMs: wan.RTTMs}
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
		result := apis.Result{Location: constants.Edge, Reason: "offload_disabled", WanRttMs: wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if wan.RTTMs > e.config.RTTUnusableMs || wan.LossPct > e.config.LossUnusablePct {
		klog.V(4).Infof("%s: WAN deemed unusable", podID)
		result := apis.Result{Location: constants.Edge, Reason: "wan_unusable", WanRttMs: wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	edgeKey := apis.GetProfileKey(pod, constants.Edge)
	cloudKey := apis.GetProfileKey(pod, constants.Cloud)
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

	result := apis.Result{
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
	local *apis.LocalState,
	reqCPU int64,
	reqMem int64,
	edgeETA float64,
	deadline float64,
) bool {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))

	// Guard: Check for degenerate cluster state
	if local.EffectiveAllocatableCPU == 0 || local.EffectiveAllocatableMem == 0 {
		klog.V(5).Infof("%s: No effective edge capacity available", podID)
		return false
	}

	// 1. Check deadline feasibility (fails fast for time-critical checks)
	if edgeETA > deadline {
		klog.V(5).Infof("%s: Edge ETA %.0fms > deadline %.0fms", podID, edgeETA, deadline)
		return false
	}

	// 2. Check basic resource availability
	if local.FreeCPU < reqCPU || local.FreeMem < reqMem {
		klog.V(5).Infof("%s: Insufficient free resources: need %dm/%dMi, have %dm/%dMi",
			podID, reqCPU, reqMem, local.FreeCPU, local.FreeMem)
		return false
	}

	// 3. Check node-level feasibility with confidence-adjusted safety margin
	if local.BestEdgeNode.Name != "" {
		// Class-aware safety margins
		baseSafetyMargin := 0.2
		switch pod.Annotations[constants.AnnotationSLOClass] {
		case "latency", "interactive":
			baseSafetyMargin = 0.1 // Less conservative for latency-sensitive
		case "batch":
			baseSafetyMargin = 0.3 // More conservative for batch
		}

		// Scale safety margin by measurement confidence
		safetyFactor := 1.0 + baseSafetyMargin*(1.0-local.MeasurementConfidence)
		requiredCPU := int64(float64(reqCPU) * safetyFactor)
		requiredMem := int64(float64(reqMem) * safetyFactor)

		if local.BestEdgeNode.FreeCPU < requiredCPU ||
			local.BestEdgeNode.FreeMem < requiredMem {
			klog.V(5).Infof("%s: Best node %s cannot fit pod with safety margin %.2fx (conf=%.2f)",
				podID, local.BestEdgeNode.Name, safetyFactor, local.MeasurementConfidence)
			return false
		}
	}

	// 4. Cluster-wide admission control: prevent exceeding utilization threshold
	utilizationLimit := 0.90
	utilizationAfterScheduling := float64(local.EffectiveAllocatableCPU-local.FreeCPU+reqCPU) /
		float64(local.EffectiveAllocatableCPU)

	if utilizationAfterScheduling > utilizationLimit {
		klog.V(5).Infof("%s: Would exceed cluster utilization: %.1f%% > %.1f%%",
			podID, utilizationAfterScheduling*100, utilizationLimit*100)
		return false
	}

	return true
}

func (e *Engine) predictEdgeETA(profile *apis.ProfileStats, local *apis.LocalState, slo *apis.SLO) float64 {
	execTime := profile.P95DurationMs

	// If no pending pressure, return execution time only
	pendingCount := local.PendingPodsPerClass[slo.Class]
	if pendingCount == 0 {
		return execTime
	}

	// Get actual resource demand from pending pods in this class
	classDemand := local.TotalDemand[slo.Class]
	if classDemand.CPU == 0 {
		// Fallback: no demand recorded, assume no queue
		return execTime
	}

	// Compute effective parallelism based on ACTUAL resource constraints
	// parallelism = min(
	//   floor(FreeCPU / AvgPodCPU),
	//   floor(FreeMem / AvgPodMem),
	//   physical_node_count * cores_per_node  // approximate upper bound
	// )
	avgPodCPU := classDemand.CPU / int64(pendingCount)
	avgPodMem := classDemand.Mem / int64(pendingCount)

	if avgPodCPU == 0 {
		avgPodCPU = 100
	}
	if avgPodMem == 0 {
		avgPodMem = 64
	}

	// Compute parallelism limited by CPU
	cpuParallelism := local.FreeCPU / avgPodCPU
	if cpuParallelism < 1 {
		cpuParallelism = 1
	}

	// Compute parallelism limited by memory
	memParallelism := local.FreeMem / avgPodMem
	if memParallelism < 1 {
		memParallelism = 1
	}

	// Effective parallelism is the bottleneck resource
	parallelism := cpuParallelism
	if memParallelism < cpuParallelism {
		parallelism = memParallelism
	}

	// Cap at realistic physical parallelism (assume ~8 cores per node, multiple nodes)
	// Use allocatable CPU as proxy for total cluster capacity
	physicalParallelism := local.EffectiveAllocatableCPU / 1000 // rough cores estimate
	if physicalParallelism < 1 {
		physicalParallelism = 1
	}
	if physicalParallelism > 64 {
		physicalParallelism = 64
	}
	if parallelism > physicalParallelism {
		parallelism = physicalParallelism
	}

	klog.V(5).Infof(
		"Queue model: class=%s pending=%d avgCPU=%dm "+
			"effectiveAlloc=%dm nonManaged=%dm freeCPU=%dm → parallelism=%d",
		slo.Class, pendingCount, avgPodCPU,
		local.EffectiveAllocatableCPU, local.NonManagedCPU,
		local.FreeCPU, parallelism,
	)

	// G/G/c queue delay estimation using Kingman-Whitt approximation
	//
	// For G/G/c queue:
	//   W_q ≈ (C² + 1) / 2 * ρ^(√2(c+1)) / (c * (1-ρ)) * E[S]
	//
	// Where:
	//   C² = coefficient of variation = (σ / μ)²
	//   ρ = utilization = λ * E[S] / c
	//   E[S] = mean service time
	//   c = number of servers (parallelism)
	//
	// Simplified for production: use deterministic queue drain + variance buffer

	// Deterministic queue drain time
	queueDrainTime := float64(pendingCount) * profile.MeanDurationMs / float64(parallelism)

	// Compute managedUtilization for variance adjustment
	// This represents transient load that actually participates in queuing
	managedUtilization := 0.0
	if local.EffectiveAllocatableCPU > 0 {
		managedActiveCPU := local.EffectiveAllocatableCPU - local.FreeCPU
		managedUtilization = float64(managedActiveCPU) /
			float64(local.EffectiveAllocatableCPU)
	}

	// Kingman's approximation for variance penalty
	// VarPenalty = (C² / 2) * (ρ / (1-ρ)) * E[S]
	// Where C² ≈ (σ/μ)² is squared coefficient of variation
	varianceBuffer := 0.0
	if managedUtilization < 0.95 && profile.MeanDurationMs > 0 {
		cv2 := (profile.StdDevDurationMs / profile.MeanDurationMs) *
			(profile.StdDevDurationMs / profile.MeanDurationMs)
		rho := managedUtilization
		varianceBuffer = (cv2 / 2.0) * (rho / (1.0 - rho)) * profile.MeanDurationMs

		// Scale by sqrt(c) for multi-server effect (variance reduction)
		if parallelism > 1 {
			varianceBuffer /= math.Sqrt(float64(parallelism))
		}

		klog.V(5).Infof(
			"Variance buffer: cv²=%.2f ρ_managed=%.2f buffer=%.0fms",
			cv2, rho, varianceBuffer,
		)
	} else if managedUtilization >= 0.95 {
		// High managedUtilization: use worst-case buffer (system near saturation)
		varianceBuffer = profile.StdDevDurationMs * 2.0
		klog.V(5).Infof(
			"High managed managedUtilization (%.2f), worst-case buffer: %.0fms",
			managedUtilization, varianceBuffer,
		)
	}

	queueWait := queueDrainTime + varianceBuffer

	// Confidence adjustment: degrade prediction when measurement quality is low
	// Lower confidence → increase predicted wait (conservative)
	if local.MeasurementConfidence < 1.0 {
		uncertaintyPenalty := queueWait * (1.0 - local.MeasurementConfidence) * 0.5
		klog.V(5).Infof("Confidence adjustment: conf=%.2f penalty=%.0fms",
			local.MeasurementConfidence, uncertaintyPenalty)
		queueWait += uncertaintyPenalty
	}

	totalETA := queueWait + execTime

	klog.V(4).Infof(
		"Edge ETA: exec=%.0fms queue=%.0fms "+
			"(drain=%.0fms variance=%.0fms ρ_managed=%.2f) total=%.0fms",
		execTime, queueWait, queueDrainTime, varianceBuffer,
		managedUtilization, totalETA,
	)

	return totalETA
}

// predictCloudETA predicts completion time on cloud
func (e *Engine) predictCloudETA(
	profile *apis.ProfileStats,
	wan *apis.WANState,
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

func fmtProfile(p *apis.ProfileStats) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("count=%d conf=%.2f mean=%.0fms p95=%.0fms slo=%.0f%%",
		p.Count, p.ConfidenceScore, p.MeanDurationMs, p.P95DurationMs, p.SLOComplianceRate*100)
}
