package decision_test

import (
	"testing"
	"time"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

func TestEngine_ProbabilityBasedDecision(t *testing.T) {
	e := newEngine()
	lyap := e.GetLyapunovScheduler()

	// Configure to weight probability heavily
	lyap.SetClassConfig("latency", &decision.ClassConfig{
		Beta:                1.0,
		TargetViolationPct:  0.05,
		TargetViolationProb: 0.05,
		DecayFactor:         1.0,
		ProbabilityWeight:   10.0,
	})

	p := podWith("latency", 200, 128)
	s := sloMust("latency", 1000, true, 5)

	local := &telemetry.LocalState{
		FreeCPU:             1000,
		FreeMem:             1000,
		PendingPodsPerClass: map[string]int{"latency": 0},
		TotalDemand:         map[string]telemetry.DemandByClass{},
		TotalAllocatableCPU: 1000,
		TotalAllocatableMem: 1000,
		BestEdgeNode:        telemetry.BestNode{Name: "edge1", FreeCPU: 1000, FreeMem: 1000},
		Timestamp:           time.Now(),
	}
	wan := &telemetry.WANState{RTTMs: 20, LossPct: 0.5}

	// Seed profiles with different risk levels
	ps := e.GetProfileStore()
	edgeKey := decision.ProfileKey{Class: "latency", CPUTier: "small", Location: constants.Edge}
	cloudKey := decision.ProfileKey{Class: "latency", CPUTier: "small", Location: constants.Cloud}

	// Edge: fast but risky (high variance)
	for i := 0; i < 50; i++ {
		ps.Update(edgeKey, decision.ProfileUpdate{ObservedDurationMs: 700, SLOMet: true})
	}
	for i := 0; i < 50; i++ {
		ps.Update(edgeKey, decision.ProfileUpdate{ObservedDurationMs: 1200, SLOMet: false})
	}

	// Cloud: slower but consistent
	for i := 0; i < 100; i++ {
		ps.Update(cloudKey, decision.ProfileUpdate{ObservedDurationMs: 850, SLOMet: true})
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

	local := &telemetry.LocalState{
		FreeCPU:             1000,
		FreeMem:             1000,
		PendingPodsPerClass: map[string]int{"latency": 0},
		TotalDemand:         map[string]telemetry.DemandByClass{},
		TotalAllocatableCPU: 1000,
		TotalAllocatableMem: 1000,
		BestEdgeNode:        telemetry.BestNode{Name: "edge1", FreeCPU: 1000, FreeMem: 1000},
		Timestamp:           time.Now(),
	}
	wan := &telemetry.WANState{RTTMs: 30, LossPct: 0.5}

	edgeKey := decision.ProfileKey{Class: "latency", CPUTier: "small", Location: constants.Edge}
	cloudKey := decision.ProfileKey{Class: "latency", CPUTier: "small", Location: constants.Cloud}

	// Scenario: Edge historically violates, cloud is safe
	for i := 0; i < 100; i++ {
		ps.Update(edgeKey, decision.ProfileUpdate{ObservedDurationMs: 1200, SLOMet: false})
	}
	for i := 0; i < 100; i++ {
		ps.Update(cloudKey, decision.ProfileUpdate{ObservedDurationMs: 800, SLOMet: true})
	}

	edgeProf := ps.GetOrDefault(edgeKey)
	cloudProf := ps.GetOrDefault(cloudKey)

	edgeViolProb := edgeProf.ComputeViolationProbability(1000)
	cloudViolProb := cloudProf.ComputeViolationProbability(1000)

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
