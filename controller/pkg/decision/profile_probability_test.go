package decision

import (
	"math"
	"testing"
	"time"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"

	"k8s.io/client-go/kubernetes/fake"
)

// Helper to create a ProfileStore for testing probability calculations.
// It will use the default histogram config, where MinSampleCount is 10.
func newTestProfileStore() *ProfileStore {
	return NewProfileStore(fake.NewSimpleClientset(), 100, DefaultHistogramConfig())
}

func TestComputeViolationProbability_HistogramBased(t *testing.T) {
	ps := newTestProfileStore()

	profile := &apis.ProfileStats{
		Count:             100,
		MeanDurationMs:    500,
		StdDevDurationMs:  100,
		P95DurationMs:     700,
		ConfidenceScore:   1.0,
		SLOComplianceRate: 0.85,
		LastUpdated:       time.Now(),
		DurationHistogram: []apis.HistogramBucket{
			{Count: 10, LastDecay: time.Now()},
			{Count: 20, LastDecay: time.Now()},
			{Count: 30, LastDecay: time.Now()},
			{Count: 25, LastDecay: time.Now()},
			{Count: 10, LastDecay: time.Now()},
			{Count: 5, LastDecay: time.Now()},
		},
	}

	// Set upper bounds
	profile.DurationHistogram[0].SetUpperBound(100)
	profile.DurationHistogram[1].SetUpperBound(200)
	profile.DurationHistogram[2].SetUpperBound(500)
	profile.DurationHistogram[3].SetUpperBound(1000)
	profile.DurationHistogram[4].SetUpperBound(2000)
	profile.DurationHistogram[5].SetUpperBound(math.Inf(1))

	tests := []struct {
		deadline    float64
		wantProbMin float64
		wantProbMax float64
		description string
	}{
		{
			deadline:    50,
			wantProbMin: 0.90,
			wantProbMax: 1.00,
			description: "Very tight deadline: most samples exceed it",
		},
		{
			deadline:    500,
			wantProbMin: 0.35,
			wantProbMax: 0.45,
			description: "Median deadline: ~40% should exceed",
		},
		{
			deadline:    1000,
			wantProbMin: 0.10,
			wantProbMax: 0.20,
			description: "P90 deadline: ~15% should exceed",
		},
		{
			deadline:    2500,
			wantProbMin: 0.00,
			wantProbMax: 0.10,
			description: "Very loose deadline: few samples exceed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			prob := ps.ComputeViolationProbability(profile, tt.deadline)
			t.Logf("Deadline=%.0fms: Pr[violation]=%.3f (expected %.3f-%.3f)",
				tt.deadline, prob, tt.wantProbMin, tt.wantProbMax)
			if prob < tt.wantProbMin || prob > tt.wantProbMax {
				t.Errorf("Probability %.3f outside expected range [%.3f, %.3f]",
					prob, tt.wantProbMin, tt.wantProbMax)
			}
		})
	}
}

func TestComputeViolationProbability_LowSampleCount(t *testing.T) {
	ps := newTestProfileStore()

	profile := &apis.ProfileStats{
		Count:             5,
		MeanDurationMs:    500,
		StdDevDurationMs:  100,
		P95DurationMs:     700,
		ConfidenceScore:   0.25,
		SLOComplianceRate: 0.80,
		LastUpdated:       time.Now(),
		DurationHistogram: []apis.HistogramBucket{},
	}

	tests := []struct {
		deadline    float64
		description string
	}{
		{400, "Deadline below mean"},
		{500, "Deadline at mean"},
		{600, "Deadline above mean"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			prob := ps.ComputeViolationProbability(profile, tt.deadline)
			t.Logf("%s: deadline=%.0fms mean=%.0fms → Pr[violation]=%.3f",
				tt.description, tt.deadline, profile.MeanDurationMs, prob)
			if prob < 0 || prob > 1 {
				t.Errorf("Invalid probability: %.3f", prob)
			}
			if tt.deadline > profile.MeanDurationMs+profile.StdDevDurationMs {
				if prob > 0.5 {
					t.Errorf("Deadline well above mean should have low violation prob, got %.3f", prob)
				}
			}
		})
	}
}

func TestComputeViolationProbability_EdgeCases(t *testing.T) {
	ps := newTestProfileStore()

	t.Run("nil profile", func(t *testing.T) {
		var profile *apis.ProfileStats
		prob := ps.ComputeViolationProbability(profile, 1000)
		if prob != 0.5 {
			t.Errorf("nil profile should return 0.5 (max uncertainty), got %.3f", prob)
		}
	})

	t.Run("zero_variance_deterministic", func(t *testing.T) {
		profile := &apis.ProfileStats{
			Count:             100,
			MeanDurationMs:    500,
			StdDevDurationMs:  0,
			DurationHistogram: []apis.HistogramBucket{},
		}

		// Deadline BELOW mean: completion time is deterministically 500, which is >= 400
		// So Pr[completion > 400] = 1.0
		prob1 := ps.ComputeViolationProbability(profile, 400)
		if prob1 != 1.0 {
			t.Errorf("Deadline below deterministic mean should be 1 (completion exceeds it), got %.3f", prob1)
		}

		// Deadline ABOVE mean: completion time is deterministically 500, which is < 600
		// So Pr[completion > 600] = 0.0
		prob2 := ps.ComputeViolationProbability(profile, 600)
		if prob2 != 0.0 {
			t.Errorf("Deadline above deterministic mean should be 0 (completion doesn't exceed it), got %.3f", prob2)
		}

		// Deadline EQUAL to mean: completion time is deterministically 500, which is NOT > 500
		// So Pr[completion > 500] = 0.0
		prob3 := ps.ComputeViolationProbability(profile, 500)
		if prob3 != 0.0 {
			t.Errorf("Deadline equal to mean should be 0 (not greater), got %.3f", prob3)
		}
	})

	t.Run("empty histogram after decay", func(t *testing.T) {
		profile := &apis.ProfileStats{
			Count:            50,
			MeanDurationMs:   500,
			StdDevDurationMs: 100,
			DurationHistogram: []apis.HistogramBucket{
				{Count: 0, LastDecay: time.Now()},
				{Count: 0, LastDecay: time.Now()},
			},
		}
		profile.DurationHistogram[0].SetUpperBound(500)
		profile.DurationHistogram[1].SetUpperBound(1000)

		prob := ps.ComputeViolationProbability(profile, 600)
		// Should fall back to stats-based (normal approximation)
		if prob < 0 || prob > 1 {
			t.Errorf("Should fall back to stats, got invalid prob %.3f", prob)
		}
		// With mean=500, stddev=100, deadline=600: z=(600-500)/100=1
		// Φ(1) ≈ 0.84, so P(X>600) ≈ 0.16
		if prob < 0.1 || prob > 0.25 {
			t.Errorf("Expected prob around 0.16, got %.3f", prob)
		}
	})
}

func TestComputeViolationProbability_Monotonicity(t *testing.T) {
	ps := newTestProfileStore()

	profile := &apis.ProfileStats{
		Count:            100,
		MeanDurationMs:   500,
		StdDevDurationMs: 150,
		P95DurationMs:    800,
		DurationHistogram: []apis.HistogramBucket{
			{Count: 20, LastDecay: time.Now()},
			{Count: 40, LastDecay: time.Now()},
			{Count: 30, LastDecay: time.Now()},
			{Count: 10, LastDecay: time.Now()},
		},
	}
	profile.DurationHistogram[0].SetUpperBound(200)
	profile.DurationHistogram[1].SetUpperBound(500)
	profile.DurationHistogram[2].SetUpperBound(1000)
	profile.DurationHistogram[3].SetUpperBound(math.Inf(1))

	// Violation probability should decrease as deadline increases
	deadlines := []float64{100, 300, 500, 700, 1000, 1500}
	probs := make([]float64, len(deadlines))

	for i, d := range deadlines {
		probs[i] = ps.ComputeViolationProbability(profile, d)
		t.Logf("Deadline=%.0fms: Pr[violation]=%.3f", d, probs[i])
	}

	for i := 1; i < len(probs); i++ {
		if probs[i] > probs[i-1] {
			t.Errorf("Non-monotonic: Pr[%.0fms]=%.3f > Pr[%.0fms]=%.3f",
				deadlines[i], probs[i], deadlines[i-1], probs[i-1])
		}
	}
}
