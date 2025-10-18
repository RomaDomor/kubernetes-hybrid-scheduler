package decision

import (
	"fmt"
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
	WanRttMs       int
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
	e := &Engine{
		config:       config,
		profileStore: config.ProfileStore,
	}
	if e.profileStore == nil {
		e.profileStore = NewProfileStore()
		klog.Warning("ProfileStore not provided; using in-memory store without persistence")
	}
	return e
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
	// Defensive defaults for nil telemetry to avoid panics in tests or degraded modes
	if wan == nil {
		wan = &telemetry.WANState{RTTMs: 999, LossPct: 100}
	}
	if local == nil {
		local = &telemetry.LocalState{
			FreeCPU:             0,
			FreeMem:             0,
			PendingPodsPerClass: map[string]int{},
		}
	}

	// Inputs summary
	pending := 0
	if local.PendingPodsPerClass != nil {
		pending = local.PendingPodsPerClass[slo.Class]
	}
	klog.V(4).Infof(
		"Decide for pod=%s class=%s prio=%d deadline=%d offload=%v wan={rtt=%dms loss=%.1f%%} edge={freeCPU=%dm freeMem=%dMi pending[%s]=%d}",
		PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID)), slo.Class, slo.Priority, slo.DeadlineMs, slo.OffloadAllowed,
		wan.RTTMs, wan.LossPct, local.FreeCPU, local.FreeMem, slo.Class, pending,
	)

	// 1. Hard safety constraints
	if !slo.OffloadAllowed {
		klog.V(4).Info("Offload disabled by SLO")
		return Result{Edge, "offload_disabled", 0, wan.RTTMs}
	}
	if wan.RTTMs > 300 || wan.LossPct > 10 {
		klog.V(4).Info("WAN deemed unusable by thresholds (rtt>300ms or loss>10%)")
		return Result{Edge, "wan_unusable", 0, wan.RTTMs}
	}

	// 2. Profiles
	edgeKey := GetProfileKey(pod, Edge)
	cloudKey := GetProfileKey(pod, Cloud)
	edgeProfile := e.profileStore.GetOrDefault(edgeKey)
	cloudProfile := e.profileStore.GetOrDefault(cloudKey)

	klog.V(4).Infof("Profiles edge[%s]: %s | cloud[%s]: %s",
		edgeKey.String(), fmtProfile(edgeProfile), cloudKey.String(), fmtProfile(cloudProfile))

	// 3. ETA
	edgeETA := e.predictETA(pod, Edge, edgeProfile, local, wan)
	cloudETA := e.predictETA(pod, Cloud, cloudProfile, local, wan)
	klog.V(4).Infof("ETA edge=%s cloud=%s", fmtETA(edgeETA), fmtETA(cloudETA))

	// 4. Exploration
	edgeConf := edgeProfile.ConfidenceScore
	cloudConf := cloudProfile.ConfidenceScore
	explorationBonus := 0.0
	if edgeConf < 0.5 || cloudConf < 0.5 {
		if rand.Float64() < 0.2 { // 20% exploration
			explorationBonus = 50.0
			klog.V(5).Infof("Exploration bonus applied: +%.0fms to cloud P95 feasibility", explorationBonus)
		}
	}

	// 5. Feasibility
	edgeFeasible := edgeETA.P95 <= float64(slo.DeadlineMs) && local.FreeCPU >= getCPURequest(pod)
	cloudFeasible := (cloudETA.P95 + explorationBonus) <= float64(slo.DeadlineMs)
	klog.V(5).Infof("Feasibility edge=%v cloud=%v (deadline=%dms, reqCPU=%dm, freeCPU=%dm)",
		edgeFeasible, cloudFeasible, slo.DeadlineMs, getCPURequest(pod), local.FreeCPU)

	// 6. Decision logic
	if edgeFeasible && !cloudFeasible {
		klog.Infof("Decision for %s: EDGE reason=edge_feasible_only eta=%.0fms wanRTT=%dms",
			PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID)), edgeETA.Mean, wan.RTTMs)
		return Result{Edge, "edge_feasible_only", edgeETA.Mean, wan.RTTMs}
	}
	if cloudFeasible && !edgeFeasible {
		klog.Infof("Decision for %s: CLOUD reason=cloud_feasible_only eta=%.0fms wanRTT=%dms",
			PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID)), cloudETA.Mean, wan.RTTMs)
		return Result{Cloud, "cloud_feasible_only", cloudETA.Mean, wan.RTTMs}
	}

	if edgeFeasible && cloudFeasible {
		edgeScore := e.computeScore(Edge, edgeETA, edgeConf, slo)
		cloudScore := e.computeScore(Cloud, cloudETA, cloudConf, slo)
		klog.V(5).Infof("Scores edge=%.1f cloud=%.1f (conf edge=%.2f cloud=%.2f)", edgeScore, cloudScore, edgeConf, cloudConf)

		if edgeScore >= cloudScore {
			klog.Infof("Decision for %s: EDGE reason=edge_preferred eta=%.0fms wanRTT=%dms",
				PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID)), edgeETA.Mean, wan.RTTMs)
			return Result{Edge, "edge_preferred", edgeETA.Mean, wan.RTTMs}
		}
		klog.Infof("Decision for %s: CLOUD reason=cloud_faster eta=%.0fms wanRTT=%dms",
			PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID)), cloudETA.Mean, wan.RTTMs)
		return Result{Cloud, "cloud_faster", cloudETA.Mean, wan.RTTMs}
	}

	// 7. Neither feasible
	if slo.Priority >= 7 && cloudETA.Mean < edgeETA.Mean {
		klog.Infof("Decision for %s: CLOUD reason=best_effort_cloud eta=%.0fms wanRTT=%dms",
			PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID)), cloudETA.Mean, wan.RTTMs)
		return Result{Cloud, "best_effort_cloud", cloudETA.Mean, wan.RTTMs}
	}

	klog.Infof("Decision for %s: EDGE reason=best_effort_edge eta=%.0fms wanRTT=%dms",
		PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID)), edgeETA.Mean, wan.RTTMs)
	return Result{Edge, "best_effort_edge", edgeETA.Mean, wan.RTTMs}
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
	var total int64
	for _, c := range pod.Spec.Containers {
		total += c.Resources.Requests.Cpu().MilliValue()
	}
	return total
}

func fmtETA(e ETAEstimate) string {
	return fmt.Sprintf("mean=%.0fms p95=%.0fms", e.Mean, e.P95)
}

func fmtProfile(p *ProfileStats) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("count=%d conf=%.2f mean=%.0fms p95=%.0fms slo=%.0f%% qwait=%.0fms",
		p.Count, p.ConfidenceScore, p.MeanDurationMs, p.P95DurationMs, p.SLOComplianceRate*100, p.MeanQueueWaitMs)
}

func PodID(ns string, name string, genName string, uid string) string {
	// Prefer name if present
	if name != "" {
		if ns == "" {
			ns = "default"
		}
		return ns + "/" + name
	}
	// Fall back to generateName + UID tail for uniqueness
	// Example: offload/build-job-xxxxx (uid: abcde)
	shortUID := uid
	if len(shortUID) > 8 {
		shortUID = shortUID[:8]
	}
	if ns == "" {
		ns = "default"
	}
	if genName == "" {
		genName = "<no-generateName>"
	}
	return fmt.Sprintf("%s/%s* (uid=%s)", ns, genName, shortUID)
}
