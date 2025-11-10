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
	CloudCostFactor         float64
	EdgeCostFactor          float64
}

// Engine implements the core scheduling logic.
type Engine struct {
	config   EngineConfig
	lyapunov *LyapunovScheduler
}

// NewEngine creates a new decision engine.
func NewEngine(config EngineConfig) *Engine {
	if config.ProfileStore == nil {
		klog.Fatal("ProfileStore must be provided in EngineConfig")
	}

	if config.LyapunovScheduler == nil {
		klog.Fatal("LyapunovScheduler must be provided in EngineConfig")
	}

	// Set cost defaults if not provided.
	if config.CloudCostFactor == 0 {
		config.CloudCostFactor = 1.0
	}

	return &Engine{
		config:   config,
		lyapunov: config.LyapunovScheduler,
	}
}

// Decide determines the optimal placement for a pod based on SLOs, telemetry, and profiles.
func (e *Engine) Decide(
	pod *corev1.Pod,
	slo *apis.SLO,
	local *apis.LocalState,
	wan *apis.WANState,
) apis.Result {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))

	// --- 1. Safety Checks and Circuit Breakers ---
	if result, ok := e.runSafetyChecks(podID, slo, local, wan); ok {
		recordDecision(result, slo.Class)
		return result
	}

	reqCPU := util.GetCPURequest(pod)
	reqMem := util.GetMemRequestMi(pod)

	klog.V(4).Infof(
		"%s: Deciding: req={cpu=%dm mem=%dMi} class=%s deadline=%dms offload=%v | wan={rtt=%dms loss=%.1f%%} | edge={free_cpu=%dm free_mem=%dMi conf=%.2f} | lyapunov={Z=%.2f Zp=%.2f}",
		podID, reqCPU, reqMem, slo.Class, slo.DeadlineMs, slo.OffloadAllowed,
		wan.RTTMs, wan.LossPct,
		local.FreeCPU, local.FreeMem, local.MeasurementConfidence,
		e.lyapunov.GetVirtualQueue(slo.Class),
		e.lyapunov.GetVirtualProbQueue(slo.Class),
	)

	// --- 2. Hard Constraints ---
	if !slo.OffloadAllowed {
		klog.V(4).Infof("%s: Offload disabled by SLO, forcing to edge", podID)
		result := apis.Result{Location: constants.Edge, Reason: constants.ReasonOffloadDisabled, WanRttMs: wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if wan.RTTMs > e.config.RTTUnusableMs || wan.LossPct > e.config.LossUnusablePct {
		klog.V(4).Infof("%s: WAN is unusable (rtt=%dms loss=%.1f%%), forcing to edge", podID, wan.RTTMs, wan.LossPct)
		result := apis.Result{Location: constants.Edge, Reason: constants.ReasonWANUnusable, WanRttMs: wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	// --- 3. Profile Loading and ETA Prediction ---
	edgeKey := apis.GetProfileKey(pod, constants.Edge)
	cloudKey := apis.GetProfileKey(pod, constants.Cloud)
	edgeProfile := e.config.ProfileStore.GetOrDefault(edgeKey)
	cloudProfile := e.config.ProfileStore.GetOrDefault(cloudKey)
	klog.V(4).Infof("%s: Profiles edge[%s]: %s | cloud[%s]: %s",
		podID, edgeKey.String(), fmtProfile(edgeProfile),
		cloudKey.String(), fmtProfile(cloudProfile))

	edgeETA := e.predictEdgeETA(edgeProfile, local, slo)
	cloudETA := e.predictCloudETA(cloudProfile, wan)
	klog.V(4).Infof("%s: Predicted ETA: edge=%.0fms, cloud=%.0fms (deadline=%dms)",
		podID, edgeETA, cloudETA, slo.DeadlineMs)

	// --- 4. Feasibility Analysis ---
	deadline := float64(slo.DeadlineMs)
	edgeFeasible := e.isEdgeFeasibleWithConfidence(pod, local, reqCPU, reqMem, edgeETA, deadline)
	cloudFeasible := cloudETA <= deadline
	klog.V(4).Infof("%s: Feasibility: edge=%v, cloud=%v", podID, edgeFeasible, cloudFeasible)

	// --- 5. Lyapunov-based Decision ---
	localCost := e.config.EdgeCostFactor
	cloudCost := e.config.CloudCostFactor

	probCalc := func(stats *apis.ProfileStats, threshold float64) float64 {
		return e.config.ProfileStore.ComputeViolationProbability(stats, threshold)
	}

	location, weight := e.lyapunov.Decide(
		slo.Class, deadline, edgeETA, cloudETA,
		edgeProfile, cloudProfile, edgeFeasible, cloudFeasible,
		localCost, cloudCost, probCalc,
	)

	// --- 6. Result Assembly ---
	reason := e.determineReason(location, edgeFeasible, cloudFeasible, edgeETA, cloudETA)
	predictedETA := cloudETA
	if location == constants.Edge {
		predictedETA = edgeETA
	}

	result := apis.Result{
		Location:       location,
		Reason:         reason,
		PredictedETAMs: predictedETA,
		WanRttMs:       wan.RTTMs,
		LyapunovWeight: weight,
	}

	recordDecision(result, slo.Class)
	klog.V(3).Infof("%s: Final Decision: %s (reason=%s, eta=%.0fms, weight=%.2f, conf=%.2f)",
		podID, result.Location, result.Reason, result.PredictedETAMs,
		weight, local.MeasurementConfidence)

	return result
}

// runSafetyChecks performs initial validation and handles telemetry issues.
func (e *Engine) runSafetyChecks(podID string, slo *apis.SLO, local *apis.LocalState, wan *apis.WANState) (apis.Result, bool) {
	if wan == nil {
		klog.Warningf("%s: WAN state is nil, using pessimistic defaults and forcing to edge", podID)
		return apis.Result{Location: constants.Edge, Reason: constants.ReasonWANCircuitBreaker, WanRttMs: 999}, true
	}
	if local == nil {
		klog.Warningf("%s: Local state is nil, using empty state and forcing to cloud", podID)
		return apis.Result{Location: constants.Cloud, Reason: constants.ReasonTelemetryCircuitBreaker, WanRttMs: wan.RTTMs}, true
	}

	// Class-specific staleness thresholds
	staleThresholds := map[string]time.Duration{
		"latency":     5 * time.Second,
		"interactive": 10 * time.Second,
		"throughput":  30 * time.Second,
		"streaming":   20 * time.Second,
		"batch":       120 * time.Second,
	}
	threshold := staleThresholds[slo.Class]
	if threshold == 0 {
		threshold = 30 * time.Second
	}

	if wan.StaleDuration > 10*time.Minute {
		klog.Errorf("%s: WAN telemetry stale for >10min, circuit breaker triggered. Forcing to edge.", podID)
		return apis.Result{Location: constants.Edge, Reason: constants.ReasonWANCircuitBreaker, WanRttMs: 999}, true
	}

	if local.StaleDuration > threshold {
		klog.Errorf("%s: Local telemetry stale for >%v for class %s, circuit breaker triggered. Forcing to cloud.",
			podID, threshold, slo.Class)
		return apis.Result{Location: constants.Cloud, Reason: constants.ReasonTelemetryCircuitBreaker, WanRttMs: wan.RTTMs}, true
	}

	if (!local.IsCompleteSnapshot || local.MeasurementConfidence < 0.5) &&
		(slo.Class == "latency" || slo.Class == "interactive") {
		klog.Warningf("%s: Low measurement confidence (%.2f) for critical class, forcing conservative cloud decision",
			podID, local.MeasurementConfidence)
		return apis.Result{Location: constants.Cloud, Reason: constants.ReasonLowMeasurementConf, WanRttMs: wan.RTTMs}, true
	}

	return apis.Result{}, false
}

// isEdgeFeasibleWithConfidence checks if the pod can be scheduled on the edge, considering resources and confidence.
func (e *Engine) isEdgeFeasibleWithConfidence(
	pod *corev1.Pod,
	local *apis.LocalState,
	reqCPU int64,
	reqMem int64,
	edgeETA float64,
	deadline float64,
) bool {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))

	// Guard against a non-functional edge cluster.
	if local.EffectiveAllocatableCPU <= 0 || local.EffectiveAllocatableMem <= 0 {
		klog.V(5).Infof("%s: Edge feasibility check failed: no effective edge capacity available", podID)
		return false
	}

	// 1. Deadline feasibility: Can the job finish in time?
	if edgeETA > deadline {
		klog.V(5).Infof("%s: Edge feasibility check failed: predicted ETA %.0fms exceeds deadline %.0fms", podID, edgeETA, deadline)
		return false
	}

	// 2. Resource availability: Are there enough free resources cluster-wide?
	if local.FreeCPU < reqCPU || local.FreeMem < reqMem {
		klog.V(5).Infof("%s: Edge feasibility check failed: insufficient free resources (need %dm/%dMi, have %dm/%dMi)",
			podID, reqCPU, reqMem, local.FreeCPU, local.FreeMem)
		return false
	}

	// 3. Node-level feasibility: Can the pod fit on the best available node with a safety margin?
	if local.BestEdgeNode.Name != "" {
		// Adjust safety margin based on SLO class and telemetry confidence.
		baseSafetyMargin := 0.20 // Default margin
		if pod.Annotations[constants.AnnotationSLOClass] == "latency" || pod.Annotations[constants.AnnotationSLOClass] == "interactive" {
			baseSafetyMargin = 0.10 // Tighter margin for latency-sensitive workloads
		}
		safetyFactor := 1.0 + baseSafetyMargin*(1.0-local.MeasurementConfidence)

		requiredCPU := int64(float64(reqCPU) * safetyFactor)
		requiredMem := int64(float64(reqMem) * safetyFactor)

		if local.BestEdgeNode.FreeCPU < requiredCPU || local.BestEdgeNode.FreeMem < requiredMem {
			klog.V(5).Infof("%s: Edge feasibility check failed: best node %s cannot fit pod with safety margin %.2fx (conf=%.2f)",
				podID, local.BestEdgeNode.Name, safetyFactor, local.MeasurementConfidence)
			return false
		}
	}

	// 4. Cluster-wide admission control: Would scheduling this pod exceed a safe utilization threshold?
	const utilizationLimit = 0.90
	utilAfterScheduling := float64(local.EffectiveAllocatableCPU-local.FreeCPU+reqCPU) / float64(local.EffectiveAllocatableCPU)
	if utilAfterScheduling > utilizationLimit {
		klog.V(5).Infof("%s: Edge feasibility check failed: would exceed cluster utilization limit (%.1f%% > %.1f%%)",
			podID, utilAfterScheduling*100, utilizationLimit*100)
		return false
	}

	return true
}

// predictEdgeETA predicts the end-to-end completion time for a pod on the edge.
func (e *Engine) predictEdgeETA(profile *apis.ProfileStats, local *apis.LocalState, slo *apis.SLO) float64 {
	// Base time is the P95 execution duration from the profile.
	execTime := profile.P95DurationMs

	// If no pending pressure, return execution time only
	pendingCount := local.PendingPodsPerClass[slo.Class]
	if pendingCount == 0 {
		return execTime // No queue, just execution time.
	}

	classDemand := local.TotalDemand[slo.Class]
	if classDemand.CPU == 0 {
		return execTime // No recorded demand for this class, assume no queue.
	}

	// Estimate parallelism based on available resources and average pod size for the class.
	avgPodCPU := classDemand.CPU / int64(pendingCount)
	if avgPodCPU == 0 {
		avgPodCPU = 100 // Avoid division by zero with a small default.
	}
	cpuParallelism := int64(1)
	if local.FreeCPU > 0 {
		cpuParallelism = local.FreeCPU / avgPodCPU
		if cpuParallelism < 1 {
			cpuParallelism = 1
		}
	}

	// Effective parallelism is bottlenecked by the most constrained resource (CPU).
	parallelism := cpuParallelism
	// Cap parallelism to a realistic maximum to avoid over-optimistic predictions.
	const maxParallelism = 64
	if parallelism > maxParallelism {
		parallelism = maxParallelism
	}

	// Estimate queue wait time using a simplified queueing model.
	// 1. Deterministic drain time: How long to clear the existing queue.
	queueDrainTime := float64(pendingCount) * profile.MeanDurationMs / float64(parallelism)

	// 2. Variance buffer: Add a buffer for variability, based on Kingman's approximation.
	varianceBuffer := 0.0
	managedUtilization := 0.0
	if local.EffectiveAllocatableCPU > 0 {
		managedActiveCPU := local.EffectiveAllocatableCPU - local.FreeCPU
		managedUtilization = float64(managedActiveCPU) / float64(local.EffectiveAllocatableCPU)
	}

	if managedUtilization < 0.95 && profile.MeanDurationMs > 0 {
		cv2 := math.Pow(profile.StdDevDurationMs/profile.MeanDurationMs, 2)
		rho := managedUtilization
		varianceBuffer = (cv2 / 2.0) * (rho / (1.0 - rho)) * profile.MeanDurationMs
		if parallelism > 1 {
			varianceBuffer /= math.Sqrt(float64(parallelism)) // Reduce variance for multi-server queue
		}
	} else if managedUtilization >= 0.95 {
		// System is near saturation, use a more pessimistic buffer.
		varianceBuffer = profile.StdDevDurationMs * 2.0
	}

	queueWait := queueDrainTime + varianceBuffer

	// 3. Confidence penalty: Increase predicted wait time if telemetry is stale.
	if local.MeasurementConfidence < 1.0 {
		uncertaintyPenalty := queueWait * (1.0 - local.MeasurementConfidence) * 0.5
		queueWait += uncertaintyPenalty
	}

	totalETA := queueWait + execTime
	klog.V(4).Infof(
		"Edge ETA breakdown: exec=%.0fms queue_wait=%.0fms (drain=%.0fms variance=%.0fms) total=%.0fms",
		execTime, queueWait, queueDrainTime, varianceBuffer, totalETA,
	)

	return totalETA
}

// predictCloudETA predicts completion time on the cloud.
func (e *Engine) predictCloudETA(profile *apis.ProfileStats, wan *apis.WANState) float64 {
	// Cloud ETA = Network transfer time (round trip) + P95 execution time.
	// Assumes the cloud has infinite capacity (no queueing).
	return 2.0*float64(wan.RTTMs) + profile.P95DurationMs
}

// determineReason translates the decision logic into a human-readable reason string.
func (e *Engine) determineReason(location constants.Location, edgeFeasible, cloudFeasible bool, edgeETA, cloudETA float64) string {
	if location == constants.Edge {
		if edgeFeasible && !cloudFeasible {
			return constants.ReasonEdgeFeasibleOnly
		}
		if edgeFeasible && cloudFeasible {
			returnIf := edgeETA <= cloudETA
			if returnIf {
				return constants.ReasonEdgePreferred
			}
			return constants.ReasonEdgeViolationControl
		}
		return constants.ReasonEdgeBestEffort
	}

	// Cloud decision
	if cloudFeasible && !edgeFeasible {
		return constants.ReasonCloudFeasibleOnly
	}
	if cloudFeasible && edgeFeasible {
		returnIf := cloudETA < edgeETA
		if returnIf {
			return constants.ReasonCloudFaster
		}
		return constants.ReasonCloudViolationControl
	}
	return constants.ReasonCloudBestEffort
}

// GetLyapunovScheduler exposes the Lyapunov scheduler for observer updates.
func (e *Engine) GetLyapunovScheduler() *LyapunovScheduler {
	return e.lyapunov
}

// GetProfileStore exposes the profile store for testing.
func (e *Engine) GetProfileStore() *ProfileStore {
	return e.config.ProfileStore
}

// fmtProfile provides a compact string representation of profile stats for logging.
func fmtProfile(p *apis.ProfileStats) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("count=%d conf=%.2f mean=%.0fms p95=%.0fms slo=%.0f%%",
		p.Count, p.ConfidenceScore, p.MeanDurationMs, p.P95DurationMs, p.SLOComplianceRate*100)
}
