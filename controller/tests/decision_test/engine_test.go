package decision_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/decision"
)

func podWith(class string, cpuM, memMi int64) *corev1.Pod {
	p := testPod(cpuM, memMi, class)
	return p
}

func sloMust(class string, deadline int, offload bool, prio int) *apis.SLO {
	return &apis.SLO{
		Class:          class,
		DeadlineMs:     deadline,
		OffloadAllowed: offload,
		Priority:       prio,
	}
}

func newEngine() *decision.Engine {
	ps := decision.NewProfileStore(fake.NewSimpleClientset(), 100, decision.DefaultHistogramConfig())

	lyap := decision.NewLyapunovScheduler()
	lyap.SetClassConfig("latency", &decision.ClassConfig{Beta: 1.0, TargetViolationProb: 0.05})
	lyap.SetClassConfig("batch", &decision.ClassConfig{Beta: 1.0, TargetViolationProb: 0.2})

	cfg := decision.EngineConfig{
		RTTUnusableMs:           300,
		LossUnusablePct:         10,
		MaxProfileCount:         100,
		ProfileStore:            ps,
		LyapunovScheduler:       lyap,
		CloudCostFactor:         1.0,
		EdgeCostFactor:          0.0,
		EdgePendingPessimismPct: 10,
	}
	return decision.NewEngine(cfg)
}

func TestDecide_WANUnusable_ForcesEdge(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)
	local := localStateWithDefaults()
	wan := &apis.WANState{RTTMs: 400, LossPct: 15}

	res := e.Decide(p, s, local, wan)
	if res.Location != constants.Edge || res.Reason != "wan_unusable" {
		t.Fatalf("want EDGE wan_unusable, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_OffloadDisabled_StaysEdge(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, false, 5)
	local := localStateWithDefaults()
	wan := &apis.WANState{RTTMs: 50, LossPct: 0.1}

	res := e.Decide(p, s, local, wan)
	if res.Location != constants.Edge || res.Reason != "offload_disabled" {
		t.Fatalf("want EDGE offload_disabled, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_CloudFeasibleOnly_WhenEdgeFull(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 500, 256)
	s := sloMust("latency", 500, true, 5)

	local := localStateWithDefaults()
	local.FreeCPU = 100 // Not enough for the 500m request
	local.FreeMem = 100

	wan := &apis.WANState{RTTMs: 10, LossPct: 0.0}

	res := e.Decide(p, s, local, wan)
	if res.Location != constants.Cloud {
		t.Fatalf("expected CLOUD when edge is full, got %v (reason=%s)", res.Location, res.Reason)
	}
	if res.Reason != constants.ReasonCloudFeasibleOnly {
		t.Errorf("expected reason cloud_feasible_only, got %s", res.Reason)
	}
}

func TestDecide_StaleCircuitBreakers(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)

	local := localStateWithDefaults()
	local.IsStale = true
	local.StaleDuration = 6 * time.Second
	local.Timestamp = time.Now().Add(-6 * time.Second)

	wan := &apis.WANState{RTTMs: 50}

	res := e.Decide(p, s, local, wan)
	if res.Location != constants.Cloud || res.Reason != "telemetry_circuit_breaker" {
		t.Fatalf("want CLOUD telemetry_circuit_breaker, got %v %s", res.Location, res.Reason)
	}

	local = localStateWithDefaults() // Reset to fresh state
	wan.IsStale = true
	wan.StaleDuration = 11 * time.Minute

	res2 := e.Decide(p, s, local, wan)
	if res2.Location != constants.Edge || res2.Reason != "wan_circuit_breaker" {
		t.Fatalf("want EDGE wan_circuit_breaker, got %v %s", res2.Location, res2.Reason)
	}
}

func TestDecide_EdgePreferred_WhenFasterAndFeasible(t *testing.T) {
	e := newEngine()
	ps := e.GetProfileStore()
	edgeKey := apis.GetProfileKey(podWith("latency", 200, 128), constants.Edge)
	cloudKey := apis.GetProfileKey(podWith("latency", 200, 128), constants.Cloud)
	ps.Update(edgeKey, apis.ProfileUpdate{ObservedDurationMs: 100})
	ps.Update(cloudKey, apis.ProfileUpdate{ObservedDurationMs: 500})

	p := podWith("latency", 200, 128)
	s := sloMust("latency", 5000, true, 5)
	local := localStateWithDefaults()
	wan := &apis.WANState{RTTMs: 10, LossPct: 0}

	res := e.Decide(p, s, local, wan)

	if res.Location != constants.Edge {
		t.Fatalf("want EDGE, got %v (reason=%s)", res.Location, res.Reason)
	}
	if res.Reason != constants.ReasonEdgePreferred {
		t.Errorf("expected reason edge_preferred, got %s", res.Reason)
	}
}

func TestDecide_LyapunovAdaptsToViolations(t *testing.T) {
	e := newEngine()
	lyap := e.GetLyapunovScheduler()
	ps := e.GetProfileStore()

	p := podWith("latency", 200, 128)
	s := sloMust("latency", 1000, true, 5)
	local := localStateWithDefaults()
	wan := &apis.WANState{RTTMs: 20, LossPct: 0.5}

	edgeKey := apis.GetProfileKey(p, constants.Edge)
	cloudKey := apis.GetProfileKey(p, constants.Cloud)

	// SCENARIO 1: Edge is slightly better than cloud, both are feasible.
	ps.Update(edgeKey, apis.ProfileUpdate{ObservedDurationMs: 900})  // EdgeETA = 900ms
	ps.Update(cloudKey, apis.ProfileUpdate{ObservedDurationMs: 880}) // CloudETA = 880 + 2*20 = 920ms

	// 1. Initial Decision: Z is zero. Both are feasible with no predicted violation.
	// Edge is cheaper and faster, so it must be chosen.
	res1 := e.Decide(p, s, local, wan)
	if res1.Location != constants.Edge {
		t.Fatalf("Initial decision should be edge (cheaper and faster), but got %s", res1.Location)
	}

	// 2. Simulate many SLO violations on the edge. This will increase the Z queue.
	for i := 0; i < 20; i++ {
		lyap.UpdateVirtualQueue("latency", 1000, 1200, constants.Edge) // Actual runs were 1200ms
	}
	afterViolationsZ := lyap.GetVirtualQueue("latency")
	if afterViolationsZ <= 0 {
		t.Fatal("Expected virtual queue to grow significantly after violations")
	}

	// 3. Now, update the edge profile to reflect reality: it's actually slower and violates the SLO.
	ps.Update(edgeKey, apis.ProfileUpdate{ObservedDurationMs: 1200}) // P95 is now > 1000

	// 4. Second Decision: Z is now very high, and the Edge profile correctly predicts a violation.
	// EdgeETA = 1200ms (Violation = 200ms) -> EdgeWeight = 0.0 + Z*200 -> high
	// CloudETA = 920ms (Violation = 0ms)   -> CloudWeight = 1.0 + Z*0 -> 1.0
	// The high edge weight should force a cloud decision.
	res2 := e.Decide(p, s, local, wan)
	if res2.Location != constants.Cloud {
		t.Errorf("Expected CLOUD after many edge violations made the violation penalty high, but got %s", res2.Location)
	}
}

func TestDecide_LowConfidenceForcesCloud(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)

	local := localStateWithDefaults()
	local.IsCompleteSnapshot = false
	local.MeasurementConfidence = 0.3

	wan := &apis.WANState{RTTMs: 50, LossPct: 0.1}

	res := e.Decide(p, s, local, wan)

	if res.Location != constants.Cloud || res.Reason != "low_measurement_confidence" {
		t.Fatalf("Expected cloud with low_measurement_confidence, got %v %s", res.Location, res.Reason)
	}
}

// localStateWithDefaults provides a default healthy, mostly empty local state for tests.
func localStateWithDefaults() *apis.LocalState {
	return &apis.LocalState{
		FreeCPU:                 8000,
		FreeMem:                 8000,
		PendingPodsPerClass:     map[string]int{},
		TotalDemand:             map[string]apis.DemandByClass{},
		TotalAllocatableCPU:     10000,
		TotalAllocatableMem:     10000,
		NonManagedCPU:           1000,
		NonManagedMem:           1000,
		EffectiveAllocatableCPU: 9000,
		EffectiveAllocatableMem: 9000,
		BestEdgeNode: apis.BestNode{
			Name:                    "edge1",
			FreeCPU:                 8000,
			FreeMem:                 8000,
			EffectiveAllocatableCPU: 9000,
			EffectiveAllocatableMem: 9000,
		},
		Timestamp:             time.Now(),
		IsCompleteSnapshot:    true,
		MeasurementConfidence: 1.0,
	}
}
