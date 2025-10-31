package decision

import (
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
	Simulator               *ScheduleSimulator // NEW: Pass simulator directly
}

type Engine struct {
	config    EngineConfig
	simulator *ScheduleSimulator
}

func NewEngine(config EngineConfig) *Engine {
	if config.ProfileStore == nil {
		klog.Fatal("ProfileStore must be provided")
	}
	if config.Simulator == nil {
		klog.Fatal("ScheduleSimulator must be provided")
	}

	return &Engine{
		config:    config,
		simulator: config.Simulator,
	}
}

func (e *Engine) Decide(
	pod *corev1.Pod,
	slo *slo.SLO,
	local *telemetry.LocalState,
	wan *telemetry.WANState,
) Result {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))

	// Default pessimistic values if telemetry missing
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

	// Circuit breakers for stale telemetry
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
		"%s: Deciding: req={cpu=%dm mem=%dMi} class=%s prio=%d deadline=%dms offload=%v wan={rtt=%dms loss=%.1f%%}",
		podID, reqCPU, reqMem, slo.Class, slo.Priority, slo.DeadlineMs, slo.OffloadAllowed,
		wan.RTTMs, wan.LossPct,
	)

	// Hard safety constraints
	if !slo.OffloadAllowed {
		klog.V(4).Infof("%s: Offload disabled by SLO", podID)
		result := Result{constants.Edge, "offload_disabled", 0, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if wan.RTTMs > e.config.RTTUnusableMs || wan.LossPct > e.config.LossUnusablePct {
		klog.V(4).Infof("%s: WAN unusable (rtt=%d>%d or loss=%.1f>%.1f)",
			podID, wan.RTTMs, e.config.RTTUnusableMs, wan.LossPct, e.config.LossUnusablePct)
		result := Result{constants.Edge, "wan_unusable", 0, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	// Get cloud execution estimate
	cloudKey := GetProfileKey(pod, constants.Cloud)
	cloudProfile := e.config.ProfileStore.GetOrDefault(cloudKey)

	// Cloud ETA: WAN RTT (2x for round-trip) + execution time
	cloudETA := 2.0*float64(wan.RTTMs) + cloudProfile.P95DurationMs

	// Simulate local completion using real pending pods
	localETA, localFeasible := e.simulator.SimulateLocalCompletion(pod, slo, local)

	klog.V(4).Infof("%s: ETA local=%.0fms (feasible=%v) cloud=%.0fms deadline=%dms",
		podID, localETA, localFeasible, cloudETA, slo.DeadlineMs)

	deadline := float64(slo.DeadlineMs)

	// Feasibility checks
	localMeetsDeadline := localFeasible && localETA <= deadline
	cloudMeetsDeadline := cloudETA <= deadline

	// Decision matrix
	if localMeetsDeadline && !cloudMeetsDeadline {
		result := Result{constants.Edge, "edge_feasible_only", localETA, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if cloudMeetsDeadline && !localMeetsDeadline {
		result := Result{constants.Cloud, "cloud_feasible_only", cloudETA, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	if localMeetsDeadline && cloudMeetsDeadline {
		// Both feasible - apply heuristics

		// Margin override: if cloud has significantly more slack, use it
		localMargin := deadline - localETA
		cloudMargin := deadline - cloudETA
		marginDiff := cloudMargin - localMargin
		marginThreshold := deadline * e.config.CloudMarginOverridePct

		if marginDiff >= marginThreshold {
			klog.V(4).Infof("%s: Cloud has %.0fms more margin (threshold=%.0fms)",
				podID, marginDiff, marginThreshold)
			result := Result{constants.Cloud, "cloud_margin_override", cloudETA, wan.RTTMs}
			recordDecision(result, slo.Class)
			return result
		}

		// Prefer local if within 20% of cloud time (locality bonus)
		if localETA <= cloudETA*1.2 {
			result := Result{constants.Edge, "edge_preferred", localETA, wan.RTTMs}
			recordDecision(result, slo.Class)
			return result
		}

		// Cloud significantly faster
		result := Result{constants.Cloud, "cloud_faster", cloudETA, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	// Neither meets deadline - best effort
	if slo.Priority >= 7 && cloudETA < localETA {
		klog.V(4).Infof("%s: Neither meets deadline, high-priority choosing cloud", podID)
		result := Result{constants.Cloud, "best_effort_cloud", cloudETA, wan.RTTMs}
		recordDecision(result, slo.Class)
		return result
	}

	klog.V(4).Infof("%s: Best effort edge", podID)
	result := Result{constants.Edge, "best_effort_edge", localETA, wan.RTTMs}
	recordDecision(result, slo.Class)
	return result
}
