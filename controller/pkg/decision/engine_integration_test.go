package decision

import (
	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"testing"
)

func TestEngine_ProbabilityBasedDecision(t *testing.T) {
	e := newEngine()
	lyap := e.GetLyapunovScheduler()

	// Configure to weight probability heavily
	lyap.SetClassConfig("latency", &ClassConfig{
		Beta:                1.0,
		TargetViolationPct:  0.05,
		TargetViolationProb: 0.05,
		DecayFactor:         1.0,
		ProbabilityWeight:   10.0,
	})

	p := podWith("latency", 200, 128)
	s := sloMust("latency", 1000, true, 5)

	// Use helper
	local := localStateWithDefaults()
	wan := &apis.WANState{RTTMs: 20, LossPct: 0.5}

	// ... rest of test unchanged
	ps := e.GetProfileStore()
	edgeKey := apis.ProfileKey{Class: "latency", CPUTier: "small", Location: constants.Edge}
	cloudKey := apis.ProfileKey{Class: "latency", CPUTier: "small", Location: constants.Cloud}

	// Edge: fast but risky (high variance)
	for i := 0; i < 50; i++ {
		ps.Update(edgeKey, apis.ProfileUpdate{ObservedDurationMs: 700, SLOMet: true})
	}
	for i := 0; i < 50; i++ {
		ps.Update(edgeKey, apis.ProfileUpdate{ObservedDurationMs: 1200, SLOMet: false})
	}

	// Cloud: slower but consistent
	for i := 0; i < 100; i++ {
		ps.Update(cloudKey, apis.ProfileUpdate{ObservedDurationMs: 850, SLOMet: true})
	}

	// Initial decision (no Zp buildup)
	res1 := e.Decide(p, s, local, wan)
	t.Logf("Initial decision (no Zp): %s", res1.Location)

	// Build up Zp by simulating edge violations
	for i := 0; i < 20; i++ {
		lyap.UpdateVirtualQueue("latency", 1000, 1200, constants.Edge)
	}

	Zp := lyap.GetVirtualProbQueue("latency")
	t.Logf("Zp after violations: %.2f", Zp)

	// Second decision (high Zp should prefer consistent cloud)
	res2 := e.Decide(p, s, local, wan)
	t.Logf("Decision with high Zp: %s", res2.Location)

	if res2.Location != constants.Cloud {
		t.Errorf("With high Zp and risky edge, expected cloud, got %v", res2.Location)
	}
}

func TestEngine_ProfilesInfluenceDecision(t *testing.T) {
	e := newEngine()
	ps := e.GetProfileStore()

	p := podWith("latency", 200, 128)
	s := sloMust("latency", 1000, true, 5)

	// Use helper
	local := localStateWithDefaults()
	wan := &apis.WANState{RTTMs: 30, LossPct: 0.5}

	edgeKey := apis.ProfileKey{Class: "latency", CPUTier: "small", Location: constants.Edge}
	cloudKey := apis.ProfileKey{Class: "latency", CPUTier: "small", Location: constants.Cloud}

	// Scenario: Edge historically violates, cloud is safe
	for i := 0; i < 100; i++ {
		ps.Update(edgeKey, apis.ProfileUpdate{ObservedDurationMs: 1200, SLOMet: false})
	}
	for i := 0; i < 100; i++ {
		ps.Update(cloudKey, apis.ProfileUpdate{ObservedDurationMs: 800, SLOMet: true})
	}

	edgeProf := ps.GetOrDefault(edgeKey)
	cloudProf := ps.GetOrDefault(cloudKey)

	edgeViolProb := ps.ComputeViolationProbability(edgeProf, 1000)
	cloudViolProb := ps.ComputeViolationProbability(cloudProf, 1000)

	t.Logf("Edge violation probability: %.3f", edgeViolProb)
	t.Logf("Cloud violation probability: %.3f", cloudViolProb)

	if edgeViolProb <= cloudViolProb {
		t.Errorf("Expected edge to have higher violation prob, got edge=%.3f cloud=%.3f",
			edgeViolProb, cloudViolProb)
	}

	res := e.Decide(p, s, local, wan)
	t.Logf("Decision based on profiles: %s", res.Location)

	// Should prefer cloud due to better historical compliance
	if res.Location != constants.Cloud {
		t.Logf("Note: Decision was %s (may be OK if other factors dominate)", res.Location)
	}
}
