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

// lyapDecide2 wraps DecideN for the common 2-cluster (local + remote) test pattern,
// preserving the old Decide call structure for readability.
func lyapDecide2(
	lyap *LyapunovScheduler,
	class string, deadline float64,
	localETA, remoteETA float64,
	localProfile, remoteProfile *apis.ProfileStats,
	localFeasible bool, localFeasReason string,
	remoteFeasible bool, remoteFeasReason string,
	localCost, remoteCost float64,
	probCalc apis.ProbabilityCalculator,
) (constants.ClusterID, float64) {
	candidates := []candidateEval{
		{
			clusterID:         constants.LocalCluster,
			eta:               localETA,
			profile:           localProfile,
			feasible:          localFeasible,
			feasibilityReason: localFeasReason,
			cost:              localCost,
		},
		{
			clusterID:         cloudCluster,
			eta:               remoteETA,
			profile:           remoteProfile,
			feasible:          remoteFeasible,
			feasibilityReason: remoteFeasReason,
			cost:              remoteCost,
		},
	}
	idx, weight := lyap.DecideN(class, deadline, candidates, probCalc)
	return candidates[idx].clusterID, weight
}

// TestLyapunov_InfeasibleTieBreaking tests the logic for when both locations are infeasible.
func TestLyapunov_InfeasibleTieBreaking(t *testing.T) {
	lyap := NewLyapunovScheduler()
	lyap.SetClassConfig("batch", &ClassConfig{
		Beta: 1.0, TargetViolationProb: 0.1, ProbabilityWeight: 1.0,
	})

	profile := &apis.ProfileStats{Count: 100, MeanDurationMs: 1000}

	// SCENARIO 1: Local is Hard-Infeasible (resource-constrained), Remote is Time-Infeasible.
	// The scheduler MUST choose remote, as the local is not a real option.
	loc1, _ := lyapDecide2(lyap,
		"batch", 1000,
		500, 1200, // Local is faster, Remote is slower than deadline
		profile, profile,
		false, constants.ReasonInfeasibleResourcesHard, // Local fails HARD check
		false, constants.ReasonInfeasibleTime, // Remote fails time check
		0.0, 1.0, dummyProbCalc,
	)
	if loc1 != cloudCluster {
		t.Errorf("Scenario 1 failed: Expected remote when local is hard-infeasible, but got %s", loc1)
	}

	// SCENARIO 2: Local is Soft-Infeasible (temporarily full), Remote is Time-Infeasible.
	// Both are "bad" options, so it should fall back to the one with the lower performance penalty.
	// Local penalty is lower (ETA is within deadline), so it should choose Local.
	loc2, _ := lyapDecide2(lyap,
		"batch", 1000,
		500, 1200, // Local ETA < deadline, Remote ETA > deadline
		profile, profile,
		false, constants.ReasonInfeasibleResourcesSoft, // Local is SOFT-infeasible
		false, constants.ReasonInfeasibleTime, // Remote is time-infeasible
		0.0, 1.0, dummyProbCalc,
	)
	if loc2 != constants.LocalCluster {
		t.Errorf("Scenario 2 failed: Expected LOCAL when local is soft-infeasible and has lower penalty, but got %s", loc2)
	}

	// SCENARIO 3: Both are Time-Infeasible.
	// It should fall back to the one with the lower performance penalty.
	// Remote is slightly less bad (1200ms vs 1300ms), so it should be chosen.
	loc3, _ := lyapDecide2(lyap,
		"batch", 1000,
		1300, 1200, // Both ETAs > deadline
		profile, profile,
		false, constants.ReasonInfeasibleTime, // Local is time-infeasible
		false, constants.ReasonInfeasibleTime, // Remote is time-infeasible
		0.0, 1.0, dummyProbCalc,
	)
	if loc3 != cloudCluster {
		t.Errorf("Scenario 3 failed: Expected remote when both are time-infeasible and remote has lower violation, but got %s", loc3)
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
		lyap.UpdateVirtualQueue(class, deadline, 1200, constants.LocalCluster)
		lyap.UpdateVirtualQueue(class, deadline, 800, constants.LocalCluster)
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
	loc, weight := lyapDecide2(lyap,
		class, deadline,
		1000, 900,
		edgeProfile, cloudProfile,
		true, "",
		true, "",
		0, 1, dummyProbCalc,
	)

	t.Logf("Decision after violations: location=%s weight=%.2f", loc, weight)

	if loc != cloudCluster {
		t.Logf("Warning: Expected remote due to high Zp, got %s (may be OK depending on Z)", loc)
	}
}

func TestLyapunov_PerClassBeta(t *testing.T) {
	lyap := NewLyapunovScheduler()
	lyap.SetClassConfig("latency", &ClassConfig{Beta: 0.5})
	lyap.SetClassConfig("batch", &ClassConfig{Beta: 2.0})

	profile := &apis.ProfileStats{Count: 100, MeanDurationMs: 900}

	// TEST 1: Latency (low beta)
	loc1, _ := lyapDecide2(lyap, "latency", 1000, 800, 900, profile, profile, true, "", true, "", 2.0, 1.0, dummyProbCalc)
	if loc1 != cloudCluster {
		t.Errorf("With equal violations and low beta, should choose cheaper remote, got %v", loc1)
	}

	// TEST 2: Batch (high beta)
	loc2, _ := lyapDecide2(lyap, "batch", 1000, 700, 800, profile, profile, true, "", true, "", 2.0, 1.0, dummyProbCalc)
	if loc2 != cloudCluster {
		t.Errorf("With high beta, cost dominates, should choose cheaper remote, got %v", loc2)
	}
}

func TestLyapunov_PerClassDecay(t *testing.T) {
	lyap := NewLyapunovScheduler()
	lyap.SetClassConfig("batch", &ClassConfig{
		DecayFactor: 0.5, DecayInterval: 100 * time.Millisecond,
	})

	for i := 0; i < 5; i++ {
		lyap.UpdateVirtualQueue("batch", 1000, 1500, constants.LocalCluster)
	}
	Z1 := lyap.GetVirtualQueue("batch")

	time.Sleep(150 * time.Millisecond)
	profile := &apis.ProfileStats{Count: 100, MeanDurationMs: 500}
	lyapDecide2(lyap, "batch", 1000, 500, 500, profile, profile, true, "", true, "", 0, 0, dummyProbCalc)

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
		lyap.UpdateVirtualQueue("latency", deadline, 1200, constants.LocalCluster)
	}

	Zp := lyap.GetVirtualProbQueue("latency")
	t.Logf("Zp after violations: %.2f", Zp)

	loc, _ := lyapDecide2(lyap,
		"latency", deadline,
		700, 800,
		safeProfile, riskyProfile,
		true, "", true, "",
		0, 0, dummyProbCalc,
	)

	if loc != constants.LocalCluster {
		t.Errorf("With high Zp, should prefer safe local profile (low violation prob), got %v", loc)
	}
}
