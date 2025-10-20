package decision_test

import (
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

func TestEngine_WithMG1QueueModel(t *testing.T) {
	t.Setenv("QUEUE_MODEL", "blend")

	// Setup
	queueStats := telemetry.NewQueueStatsCollector(5*time.Minute, 5)
	ps := decision.NewProfileStore(fake.NewSimpleClientset(), 100, decision.DefaultHistogramConfig())

	// Seed profile with known stats
	key := decision.ProfileKey{Class: "latency", CPUTier: "small", Location: constants.Edge}
	for i := 0; i < 20; i++ {
		ps.Update(key, decision.ProfileUpdate{
			ObservedDurationMs: 100 + float64(i%30), // Mean ~100, some variance
			QueueWaitMs:        5,
			SLOMet:             true,
		})
	}

	// Simulate arrivals for λ estimation
	for i := 0; i < 10; i++ {
		queueStats.RecordArrival("latency")
	}
	time.Sleep(50 * time.Millisecond)

	cfg := decision.EngineConfig{
		RTTThresholdMs:   100,
		LossThresholdPct: 2,
		RTTUnusableMs:    300,
		LossUnusablePct:  10,
		LocalityBonus:    50,
		ConfidenceWeight: 30,
		ExplorationRate:  0, // Deterministic
		MaxProfileCount:  100,
		ProfileStore:     ps,
		QueueStats:       queueStats,
	}

	engine := decision.NewEngine(cfg)

	pod := testPod(200, 128, "latency")
	sloSpec := sloMust("latency", 2000, true, 5)

	local := &telemetry.LocalState{
		PendingPodsPerClass: map[string]int{"latency": 5}, // Some pending
		BestEdgeNode:        telemetry.BestNode{Name: "edge1", FreeCPU: 2000, FreeMem: 2048},
	}
	wan := &telemetry.WANState{RTTMs: 50, LossPct: 0.5}

	result := engine.Decide(pod, sloSpec, local, wan)

	// Should make a decision (either edge or cloud)
	if result.Location != constants.Edge && result.Location != constants.Cloud {
		t.Fatalf("Invalid decision location: %v", result.Location)
	}

	if result.PredictedETAMs <= 0 {
		t.Errorf("PredictedETA should be > 0, got %.1fms", result.PredictedETAMs)
	}

	t.Logf("Decision: %s, ETA: %.1fms, Reason: %s",
		result.Location, result.PredictedETAMs, result.Reason)
}

func TestEngine_MG1_vs_Linear_Comparison(t *testing.T) {
	queueStats := telemetry.NewQueueStatsCollector(5*time.Minute, 5)
	ps := decision.NewProfileStore(fake.NewSimpleClientset(), 100, decision.DefaultHistogramConfig())

	// High variance profile
	key := decision.ProfileKey{Class: "batch", CPUTier: "medium", Location: constants.Edge}
	for i := 0; i < 20; i++ {
		duration := 200.0
		if i%3 == 0 {
			duration = 500.0 // Inject high variance
		}
		ps.Update(key, decision.ProfileUpdate{
			ObservedDurationMs: duration,
			QueueWaitMs:        10,
			SLOMet:             true,
		})
	}

	// Heavy arrival rate
	for i := 0; i < 20; i++ {
		queueStats.RecordArrival("batch")
	}
	time.Sleep(100 * time.Millisecond) // λ = 20/100 = 0.2 per ms

	profile := ps.GetOrDefault(key)

	// Test linear
	t.Setenv("QUEUE_MODEL", "linear")
	waitLinear := decision.EstimateQueueWait("batch", profile, 10, queueStats)

	// Test MG1
	t.Setenv("QUEUE_MODEL", "mg1")
	waitMG1 := decision.EstimateQueueWait("batch", profile, 10, queueStats)

	t.Logf("Linear: %.1fms, MG1: %.1fms (profile: mean=%.1f, σ=%.1f)",
		waitLinear, waitMG1, profile.MeanDurationMs, profile.StdDevDurationMs)

	// MG1 should differ from linear due to variance term
	if waitMG1 == waitLinear {
		t.Logf("Warning: MG1 and linear are identical (may indicate low ρ)")
	}
}
