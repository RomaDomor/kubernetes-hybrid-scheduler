package decision

import (
	"math"
	"os"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

type QueueModel string

const (
	QueueModelLinear QueueModel = "linear"
	QueueModelMG1    QueueModel = "mg1"
	QueueModelBlend  QueueModel = "blend"
)

var (
	queueModelFlag = QueueModel(getEnvOrDefault("QUEUE_MODEL", "blend"))
	mg1BlendAlpha  = getEnvFloat("MG1_BLEND_ALPHA", 0.7)

	queueWaitEstimateGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_queue_wait_estimate_ms",
			Help: "Queue wait time estimate by model and class",
		},
		[]string{"model", "class"},
	)
)

// EstimateQueueWait computes queue waiting time using the configured model
func EstimateQueueWait(
	class string,
	profile *ProfileStats,
	pendingCount int,
	queueStats *telemetry.QueueStatsCollector,
) float64 {
	if profile == nil {
		return 0
	}

	// Linear baseline: N * E[S]
	baselineWq := float64(pendingCount) * profile.MeanDurationMs
	queueWaitEstimateGauge.WithLabelValues("linear", class).Set(baselineWq)

	if queueModelFlag == QueueModelLinear {
		return math.Max(0, baselineWq)
	}

	// M/G/1 estimate
	lambda := 0.0
	if queueStats != nil {
		lambda = queueStats.LambdaForClass(class)
	}

	wqMG1 := estimateWqMG1(lambda, profile.MeanDurationMs, profile.StdDevDurationMs)
	queueWaitEstimateGauge.WithLabelValues("mg1", class).Set(wqMG1)

	if queueModelFlag == QueueModelMG1 {
		return wqMG1
	}

	// Blend mode (default)
	if wqMG1 <= 0 || lambda <= 0 {
		return math.Max(0, baselineWq)
	}

	// Clamp MG1 to avoid pathological explosion
	maxClamp := 3.0 * baselineWq
	if maxClamp < profile.MeanDurationMs {
		maxClamp = profile.MeanDurationMs
	}
	if wqMG1 > maxClamp {
		wqMG1 = maxClamp
		klog.V(4).Infof("Clamped MG1 estimate for class %s from %.1fms to %.1fms", class, wqMG1, maxClamp)
	}

	// Convex combination
	blended := mg1BlendAlpha*wqMG1 + (1-mg1BlendAlpha)*baselineWq
	queueWaitEstimateGauge.WithLabelValues("blend", class).Set(blended)

	return math.Max(0, blended)
}

// estimateWqMG1 implements Pollaczek-Khinchine formula for M/G/1 queue
func estimateWqMG1(lambdaPerMs, meanMs, stddevMs float64) float64 {
	if meanMs <= 0 || stddevMs < 0 || lambdaPerMs <= 0 {
		return 0
	}

	es := meanMs              // E[S]
	vs := stddevMs * stddevMs // Var(S)
	es2 := vs + es*es         // E[S²] = Var(S) + (E[S])²

	rho := lambdaPerMs * es // Utilization ρ = λ * E[S]

	// Cap utilization to avoid instability
	if rho >= 0.99 {
		klog.V(4).Infof("High utilization %.3f capped to 0.99", rho)
		rho = 0.99
	}

	if rho <= 0 {
		return 0
	}

	// Pollaczek-Khinchine: Wq = (λ * E[S²]) / (2 * (1 - ρ))
	wq := (lambdaPerMs * es2) / (2.0 * (1.0 - rho))

	klog.V(5).Infof("MG1 estimate: λ=%.6f, E[S]=%.1f, Var(S)=%.1f, ρ=%.3f, Wq=%.1fms",
		lambdaPerMs, es, vs, rho, wq)

	return wq
}

func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
