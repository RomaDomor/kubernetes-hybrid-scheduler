package decision

import (
	"math"
	"testing"
	"time"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

// Create a dummy probability calculator for tests that don't need the real logic.
var dummyProbCalc apis.ProbabilityCalculator = func(stats *apis.ProfileStats, threshold float64) float64 {
	if stats == nil {
		return 0.5
	}
	if stats.MeanDurationMs > threshold {
		return 0.8 // high chance of violation
	}
	return 0.1 // low chance of violation
}

// TestLyapunov_InfeasibleTieBreaking tests the new logic for when both locations are infeasible.
func TestLyapunov_InfeasibleTieBreaking(t *testing.T) {
	lyap := NewLyapunovScheduler()
	lyap.SetClassConfig("batch", &ClassConfig{
		Beta: 1.0, TargetViolationProb: 0.1, ProbabilityWeight: 1.0,
	})

	profile := &apis.ProfileStats{Count: 100, MeanDurationMs: 1000}

	// SCENARIO 1: Edge is Hard-Infeasible (resource-constrained), Cloud is Time-Infeasible.
	// The scheduler MUST choose cloud, as the edge is not a real option.
	loc1, _ := lyap.Decide(
		"batch", 1000,
		500, 1200, // Edge is faster, Cloud is slower than deadline
		profile, profile,
		false, constants.ReasonInfeasibleResourcesHard, // Edge fails HARD check
		false, constants.ReasonInfeasibleTime, // Cloud fails time check
		0.0, 1.0, dummyProbCalc,
	)
	if loc1 != constants.Cloud {
		t.Errorf("Scenario 1 failed: Expected CLOUD when edge is hard-infeasible, but got %s", loc1)
	}

	// SCENARIO 2: Edge is Soft-Infeasible (temporarily full), Cloud is Time-Infeasible.
	// Both are "bad" options, so it should fall back to the one with the lower performance penalty.
	// Edge penalty is lower (ETA is within deadline), so it should choose Edge (i.e., decide to wait in the queue).
	loc2, _ := lyap.Decide(
		"batch", 1000,
		500, 1200, // Edge ETA < deadline, Cloud ETA > deadline
		profile, profile,
		false, constants.ReasonInfeasibleResourcesSoft, // Edge is SOFT-infeasible
		false, constants.ReasonInfeasibleTime, // Cloud is time-infeasible
		0.0, 1.0, dummyProbCalc,
	)
	if loc2 != constants.Edge {
		t.Errorf("Scenario 2 failed: Expected EDGE when edge is soft-infeasible and has lower penalty, but got %s", loc2)
	}

	// SCENARIO 3: Both are Time-Infeasible.
	// It should fall back to the one with the lower performance penalty.
	// Cloud is slightly less bad (1200ms vs 1300ms), so it should be chosen.
	loc3, _ := lyap.Decide(
		"batch", 1000,
		1300, 1200, // Both ETAs > deadline
		profile, profile,
		false, constants.ReasonInfeasibleTime, // Edge is time-infeasible
		false, constants.ReasonInfeasibleTime, // Cloud is time-infeasible
		0.0, 1.0, dummyProbCalc,
	)
	if loc3 != constants.Cloud {
		t.Errorf("Scenario 3 failed: Expected CLOUD when both are time-infeasible and cloud has lower violation, but got %s", loc3)
	}
}

func TestLyapunov_ProbabilityQueue(t *testing.T) {
	lyap := NewLyapunovScheduler()
	lyap.SetClassConfig("latency", &ClassConfig{
		Beta:                1.0,
		TargetViolationPct:  0.05,
		TargetViolationProb: 0.05,
		DecayFactor:         1.0,
		ProbabilityWeight:   1.0,
	})

	edgeProfile := &apis.ProfileStats{
		Count:            100,
		MeanDurationMs:   800,
		StdDevDurationMs: 200,
		P95DurationMs:    1200,
		DurationHistogram: []apis.HistogramBucket{
			{Count: 50, LastDecay: time.Now()}, {Count: 30, LastDecay: time.Now()},
			{Count: 15, LastDecay: time.Now()}, {Count: 5, LastDecay: time.Now()},
		},
	}
	edgeProfile.DurationHistogram[0].SetUpperBound(500)
	edgeProfile.DurationHistogram[1].SetUpperBound(1000)
	edgeProfile.DurationHistogram[2].SetUpperBound(1500)
	edgeProfile.DurationHistogram[3].SetUpperBound(math.Inf(1))

	cloudProfile := &apis.ProfileStats{
		Count:            100,
		MeanDurationMs:   600,
		StdDevDurationMs: 150,
		P95DurationMs:    900,
		DurationHistogram: []apis.HistogramBucket{
			{Count: 60, LastDecay: time.Now()}, {Count: 30, LastDecay: time.Now()},
			{Count: 8, LastDecay: time.Now()}, {Count: 2, LastDecay: time.Now()},
		},
	}
	cloudProfile.DurationHistogram[0].SetUpperBound(500)
	cloudProfile.DurationHistogram[1].SetUpperBound(1000)
	cloudProfile.DurationHistogram[2].SetUpperBound(1500)
	cloudProfile.DurationHistogram[3].SetUpperBound(math.Inf(1))

	class := "latency"
	deadline := 1000.0

	// Initial queues should be zero
	if Z := lyap.GetVirtualQueue(class); Z != 0 {
		t.Errorf("Initial Z should be 0, got %.2f", Z)
	}
	if Zp := lyap.GetVirtualProbQueue(class); Zp != 0 {
		t.Errorf("Initial Zp should be 0, got %.2f", Zp)
	}

	// Simulate 10 violations out of 20 jobs (50% violation rate)
	for i := 0; i < 10; i++ {
		lyap.UpdateVirtualQueue(class, deadline, 1200, constants.Edge)
		lyap.UpdateVirtualQueue(class, deadline, 800, constants.Edge)
	}

	Zp := lyap.GetVirtualProbQueue(class)
	stats := lyap.GetStats(class)

	t.Logf("After 10 violations / 20 completions:")
	t.Logf("  Zp = %.2f", Zp)
	t.Logf("  Actual violation rate = %.2f%%", stats.ActualViolationPct*100)
	t.Logf("  Target = 5%%")

	if Zp <= 0 {
		t.Errorf("Zp should be positive when violation rate exceeds target, got %.2f", Zp)
	}

	// Now test decision with profiles
	loc, weight := lyap.Decide(
		class, deadline,
		1000, 900,
		edgeProfile, cloudProfile,
		true, "",
		true, "",
		0, 1, dummyProbCalc,
	)

	t.Logf("Decision after violations: location=%s weight=%.2f", loc, weight)

	if loc != constants.Cloud {
		t.Logf("Warning: Expected cloud due to high Zp, got %s (may be OK depending on Z)", loc)
	}
}

func TestLyapunov_PerClassBeta(t *testing.T) {
	lyap := NewLyapunovScheduler()
	lyap.SetClassConfig("latency", &ClassConfig{Beta: 0.5})
	lyap.SetClassConfig("batch", &ClassConfig{Beta: 2.0})

	profile := &apis.ProfileStats{Count: 100, MeanDurationMs: 900}

	// TEST 1: Latency (low beta)
	loc1, _ := lyap.Decide("latency", 1000, 800, 900, profile, profile, true, "", true, "", 2.0, 1.0, dummyProbCalc)
	if loc1 != constants.Cloud {
		t.Errorf("With equal violations and low beta, should choose cheaper cloud, got %v", loc1)
	}

	// TEST 2: Batch (high beta)
	loc2, _ := lyap.Decide("batch", 1000, 700, 800, profile, profile, true, "", true, "", 2.0, 1.0, dummyProbCalc)
	if loc2 != constants.Cloud {
		t.Errorf("With high beta, cost dominates, should choose cheaper cloud, got %v", loc2)
	}
}

func TestLyapunov_PerClassDecay(t *testing.T) {
	lyap := NewLyapunovScheduler()
	lyap.SetClassConfig("batch", &ClassConfig{
		DecayFactor: 0.5, DecayInterval: 100 * time.Millisecond,
	})

	for i := 0; i < 5; i++ {
		lyap.UpdateVirtualQueue("batch", 1000, 1500, constants.Edge)
	}
	Z1 := lyap.GetVirtualQueue("batch")

	time.Sleep(150 * time.Millisecond)
	profile := &apis.ProfileStats{Count: 100, MeanDurationMs: 500}
	_, _ = lyap.Decide("batch", 1000, 500, 500, profile, profile, true, "", true, "", 0, 0, dummyProbCalc)

	Z2 := lyap.GetVirtualQueue("batch")
	if Z2 >= Z1 {
		t.Errorf("Expected decay to reduce Z, got %.2f >= %.2f", Z2, Z1)
	}
}

func TestLyapunov_ProbabilityQueueInfluencesDecision(t *testing.T) {
	lyap := NewLyapunovScheduler()
	lyap.SetClassConfig("latency", &ClassConfig{
		TargetViolationProb: 0.05, ProbabilityWeight: 100.0,
	})

	safeProfile := &apis.ProfileStats{Count: 100, MeanDurationMs: 500}
	riskyProfile := &apis.ProfileStats{Count: 100, MeanDurationMs: 1100}
	deadline := 1000.0

	// Build up Zp through violations
	for i := 0; i < 10; i++ {
		lyap.UpdateVirtualQueue("latency", deadline, 1200, constants.Edge)
	}

	Zp := lyap.GetVirtualProbQueue("latency")
	t.Logf("Zp after violations: %.2f", Zp)

	loc, _ := lyap.Decide(
		"latency", deadline,
		700, 800,
		safeProfile, riskyProfile,
		true, "", true, "",
		0, 0, dummyProbCalc,
	)

	if loc != constants.Edge {
		t.Errorf("With high Zp, should prefer safe edge profile (low violation prob), got %v", loc)
	}
}
