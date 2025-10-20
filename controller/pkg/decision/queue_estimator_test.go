package decision_test

import (
	"testing"
	"time"

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

func TestEstimateQueueWait_Linear(t *testing.T) {
	// Force linear mode via env (or directly set queueModelFlag in production)
	t.Setenv("QUEUE_MODEL", "linear")

	profile := &decision.ProfileStats{
		MeanDurationMs:   100,
		StdDevDurationMs: 20,
	}

	pendingCount := 5
	wait := decision.EstimateQueueWait("latency", profile, pendingCount, nil)

	expected := float64(pendingCount) * profile.MeanDurationMs
	if wait != expected {
		t.Errorf("Linear model: expected %.1fms, got %.1fms", expected, wait)
	}
}

func TestEstimateQueueWait_MG1(t *testing.T) {
	t.Setenv("QUEUE_MODEL", "mg1")

	profile := &decision.ProfileStats{
		MeanDurationMs:   100, // E[S]
		StdDevDurationMs: 50,  // σ
	}

	// Create queue stats with known λ
	queueStats := telemetry.NewQueueStatsCollector(5*time.Minute, 5)

	// Simulate arrivals: 10 arrivals over ~1000ms → λ ≈ 0.01 per ms
	for i := 0; i < 10; i++ {
		queueStats.RecordArrival("latency")
	}
	time.Sleep(10 * time.Millisecond) // Ensure window elapsed

	// λ = 0.01, E[S] = 100 → ρ = 1.0 (would be capped at 0.99)
	// E[S²] = Var + E[S]² = 2500 + 10000 = 12500
	// Wq = (0.01 * 12500) / (2 * (1 - 0.99)) = 125 / 0.02 = 6250ms

	wait := decision.EstimateQueueWait("latency", profile, 5, queueStats)

	// Should be > 0 and reflect M/G/1 calculation
	if wait <= 0 {
		t.Errorf("MG1 model returned non-positive wait: %.1fms", wait)
	}

	t.Logf("MG1 estimate: %.1fms (λ=%.6f)", wait, queueStats.LambdaForClass("latency"))
}

func TestEstimateQueueWait_Blend(t *testing.T) {
	t.Setenv("QUEUE_MODEL", "blend")
	t.Setenv("MG1_BLEND_ALPHA", "0.5")

	profile := &decision.ProfileStats{
		MeanDurationMs:   100,
		StdDevDurationMs: 30,
	}

	queueStats := telemetry.NewQueueStatsCollector(5*time.Minute, 1)
	for i := 0; i < 5; i++ {
		queueStats.RecordArrival("batch")
	}
	time.Sleep(10 * time.Millisecond)

	pendingCount := 3
	wait := decision.EstimateQueueWait("batch", profile, pendingCount, queueStats)

	baseline := float64(pendingCount) * profile.MeanDurationMs // 300ms

	// Blend should be between baseline and MG1 (unless clamped)
	if wait < 0 {
		t.Errorf("Blend returned negative wait: %.1fms", wait)
	}

	t.Logf("Blend: baseline=%.1fms, actual=%.1fms", baseline, wait)
}

func TestEstimateQueueWait_NoLambda(t *testing.T) {
	t.Setenv("QUEUE_MODEL", "blend")

	profile := &decision.ProfileStats{
		MeanDurationMs:   100,
		StdDevDurationMs: 20,
	}

	// No queue stats → λ = 0 → fallback to linear
	wait := decision.EstimateQueueWait("latency", profile, 5, nil)

	expected := 500.0 // 5 * 100
	if wait != expected {
		t.Errorf("Blend with no λ should fallback to linear: expected %.1f, got %.1f", expected, wait)
	}
}

func TestEstimateQueueWait_HighVariance(t *testing.T) {
	// Force pure M/G/1 mode
	t.Setenv("QUEUE_MODEL", "mg1")

	// Two profiles: same mean but different stddev
	lowVar := &decision.ProfileStats{
		MeanDurationMs:   100, // E[S]
		StdDevDurationMs: 10,  // small variance
	}
	highVar := &decision.ProfileStats{
		MeanDurationMs:   100,
		StdDevDurationMs: 80, // large variance
	}

	// Build a queueStats collector with a large window so it doesn't reset
	queueStats := telemetry.NewQueueStatsCollector(5*time.Minute, 1)

	// Record one arrival of class "test"
	class := "test"
	queueStats.RecordArrival(class)

	// Sleep so that elapsed >> 0 and λ = 1 / elapsed_ms is small (ρ < 1)
	time.Sleep(200 * time.Millisecond)

	// Estimate waits (pendingCount is ignored in pure mg1)
	waitLow := decision.EstimateQueueWait(class, lowVar, 5, queueStats)
	waitHigh := decision.EstimateQueueWait(class, highVar, 5, queueStats)

	t.Logf("MG1 queue wait: lowVar=%.1fms, highVar=%.1fms", waitLow, waitHigh)

	if waitHigh <= waitLow {
		t.Errorf("expected high-variance wait > low-variance wait; got low=%.1fms, high=%.1fms",
			waitLow, waitHigh)
	}
}
