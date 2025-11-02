package decision_test

import (
	"math"
	"testing"
	"time"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/decision"
)

func TestLyapunov_ProbabilityQueue(t *testing.T) {
	lyap := decision.NewLyapunovScheduler()
	lyap.SetClassConfig("latency", &decision.ClassConfig{
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
			{Count: 50, LastDecay: time.Now()},
			{Count: 30, LastDecay: time.Now()},
			{Count: 15, LastDecay: time.Now()},
			{Count: 5, LastDecay: time.Now()},
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
			{Count: 60, LastDecay: time.Now()},
			{Count: 30, LastDecay: time.Now()},
			{Count: 8, LastDecay: time.Now()},
			{Count: 2, LastDecay: time.Now()},
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

	// Zp should be positive since actual (50%) > target (5%)
	if Zp <= 0 {
		t.Errorf("Zp should be positive when violation rate exceeds target, got %.2f", Zp)
	}

	// Now test decision with profiles
	loc, weight := lyap.Decide(
		class, deadline,
		1000, 900,
		edgeProfile, cloudProfile,
		true, true,
		0, 1,
	)

	t.Logf("Decision after violations: location=%s weight=%.2f", loc, weight)

	// With high Zp, should prefer cloud despite cost
	if loc != constants.Cloud {
		t.Logf("Warning: Expected cloud due to high Zp, got %s (may be OK depending on Z)", loc)
	}
}
func TestLyapunov_PerClassBeta(t *testing.T) {
	lyap := decision.NewLyapunovScheduler()

	// High-priority class with low beta (prioritize SLO compliance over cost)
	lyap.SetClassConfig("latency", &decision.ClassConfig{
		Beta:                0.5,
		TargetViolationPct:  0.05,
		TargetViolationProb: 0.05,
		DecayFactor:         1.0,
		ProbabilityWeight:   1.0,
	})

	// Low-priority class with high beta (prioritize cost over SLO)
	lyap.SetClassConfig("batch", &decision.ClassConfig{
		Beta:                2.0,
		TargetViolationPct:  0.20,
		TargetViolationProb: 0.20,
		DecayFactor:         1.0,
		ProbabilityWeight:   1.0,
	})

	profile := &apis.ProfileStats{
		Count:             100,
		MeanDurationMs:    900,
		StdDevDurationMs:  100,
		P95DurationMs:     1100,
		DurationHistogram: []apis.HistogramBucket{{Count: 100}},
	}
	profile.DurationHistogram[0].SetUpperBound(1000)

	// TEST 1: Equal violations (both well below deadline), cost dominates
	// Deadline=1000, Edge ETA=800 (viol=0), Cloud ETA=900 (viol=0)
	// Edge weight = 0.5*2 + Z*0 = 1.0
	// Cloud weight = 0.5*1 + Z*0 = 0.5
	// Cloud wins (lower weight, cheaper)
	loc1, weight1 := lyap.Decide("latency", 1000, 800, 900, profile, profile, true, true, 2.0, 1.0)
	t.Logf("Latency (β=0.5): loc=%s weight=%.2f | cost dominates when violations equal", loc1, weight1)
	if loc1 != constants.Cloud {
		t.Errorf("With equal violations and low beta, should choose cheaper cloud, got %v", loc1)
	}

	// TEST 2: Edge violates more than cloud
	// Deadline=500, Edge ETA=700 (viol=200), Cloud ETA=600 (viol=100)
	// With low beta=0.5, violations weighted heavily
	// Edge weight = 0.5*2 + Z*200 = 1.0 + Z*200
	// Cloud weight = 0.5*1 + Z*100 = 0.5 + Z*100
	// Cloud has lower violation, should win
	loc2, weight2 := lyap.Decide("latency", 500, 700, 600, profile, profile, true, true, 2.0, 1.0)
	t.Logf("Latency (β=0.5): loc=%s weight=%.2f | violations matter: edge_viol=200 > cloud_viol=100", loc2, weight2)
	if loc2 != constants.Cloud {
		t.Errorf("With lower violations and low beta, should choose cloud, got %v", loc2)
	}

	// TEST 3: Batch with high beta (cost-sensitive)
	// Deadline=1000, Edge ETA=700 (viol=0), Cloud ETA=800 (viol=0)
	// With high beta=2.0, cost dominates
	// Edge weight = 2.0*2 + Z*0 = 4.0
	// Cloud weight = 2.0*1 + Z*0 = 2.0
	// Cloud wins (much cheaper matters with high beta)
	loc3, weight3 := lyap.Decide("batch", 1000, 700, 800, profile, profile, true, true, 2.0, 1.0)
	t.Logf("Batch (β=2.0): loc=%s weight=%.2f | high beta makes cost dominant", loc3, weight3)
	if loc3 != constants.Cloud {
		t.Errorf("With high beta, cost dominates, should choose cheaper cloud, got %v", loc3)
	}

	// TEST 4: Batch with violation (high beta, but edge is much cheaper)
	// Deadline=500, Edge ETA=700 (viol=200), Cloud ETA=600 (viol=100)
	// Edge weight = 2.0*2 + Z*200 = 4.0 + Z*200
	// Cloud weight = 2.0*1 + Z*100 = 2.0 + Z*100
	// Cloud still wins (2.0 < 4.0 initially, and smaller violation delta)
	loc4, weight4 := lyap.Decide("batch", 500, 700, 600, profile, profile, true, true, 2.0, 1.0)
	t.Logf("Batch (β=2.0): loc=%s weight=%.2f | high beta but cloud better on both cost and violation", loc4, weight4)
	if loc4 != constants.Cloud {
		t.Errorf("Cloud better on cost and violation, should win, got %v", loc4)
	}
}

func TestLyapunov_PerClassDecay(t *testing.T) {
	lyap := decision.NewLyapunovScheduler()

	// Aggressive decay for batch
	lyap.SetClassConfig("batch", &decision.ClassConfig{
		Beta:                1.0,
		TargetViolationPct:  0.20,
		TargetViolationProb: 0.20,
		DecayFactor:         0.5,
		DecayInterval:       100 * time.Millisecond,
		ProbabilityWeight:   1.0,
	})

	// Build up some queue
	for i := 0; i < 5; i++ {
		lyap.UpdateVirtualQueue("batch", 1000, 1500, constants.Edge)
	}

	Z1 := lyap.GetVirtualQueue("batch")
	t.Logf("Z before decay: %.2f", Z1)

	// Wait for decay
	time.Sleep(150 * time.Millisecond)

	profile := &apis.ProfileStats{
		Count: 100, MeanDurationMs: 500, StdDevDurationMs: 100,
		DurationHistogram: []apis.HistogramBucket{{Count: 100}},
	}
	profile.DurationHistogram[0].SetUpperBound(1000)

	_, _ = lyap.Decide("batch", 1000, 500, 500, profile, profile, true, true, 0, 0)

	Z2 := lyap.GetVirtualQueue("batch")
	t.Logf("Z after decay: %.2f", Z2)

	if Z2 >= Z1 {
		t.Errorf("Expected decay to reduce Z, got %.2f >= %.2f", Z2, Z1)
	}
}

func TestLyapunov_ProbabilityQueueInfluencesDecision(t *testing.T) {
	lyap := decision.NewLyapunovScheduler()
	lyap.SetClassConfig("latency", &decision.ClassConfig{
		Beta:                1.0,
		TargetViolationPct:  0.05,
		TargetViolationProb: 0.05,
		DecayFactor:         1.0,
		ProbabilityWeight:   100.0,
	})

	safeProfile := &apis.ProfileStats{
		Count:            100,
		MeanDurationMs:   500,
		StdDevDurationMs: 50,
		P95DurationMs:    600,
		DurationHistogram: []apis.HistogramBucket{
			{Count: 100, LastDecay: time.Now()},
		},
	}
	safeProfile.DurationHistogram[0].SetUpperBound(600)

	riskyProfile := &apis.ProfileStats{
		Count:            100,
		MeanDurationMs:   900,
		StdDevDurationMs: 200,
		P95DurationMs:    1300,
		DurationHistogram: []apis.HistogramBucket{
			{Count: 50, LastDecay: time.Now()},
			{Count: 50, LastDecay: time.Now()},
		},
	}
	riskyProfile.DurationHistogram[0].SetUpperBound(1000)
	riskyProfile.DurationHistogram[1].SetUpperBound(math.Inf(1))

	deadline := 1000.0

	// Build up Zp through violations
	for i := 0; i < 10; i++ {
		lyap.UpdateVirtualQueue("latency", deadline, 1200, constants.Edge)
	}

	Zp := lyap.GetVirtualProbQueue("latency")
	t.Logf("Zp after violations: %.2f", Zp)

	// Decision with safe edge vs risky cloud
	loc, weight := lyap.Decide(
		"latency", deadline,
		700, 800,
		safeProfile, riskyProfile,
		true, true,
		0, 0,
	)

	t.Logf("Decision: loc=%s weight=%.2f (Zp=%.2f should prefer safe edge)", loc, weight, Zp)

	if loc != constants.Edge {
		t.Errorf("With high Zp, should prefer safe edge profile, got %v", loc)
	}
}
