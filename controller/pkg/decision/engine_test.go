package decision_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

func podWith(class string, cpuM, memMi int64) *corev1.Pod {
	p := testPod(cpuM, memMi, class)
	return p
}

func sloMust(class string, deadline int, offload bool, prio int) *slo.SLO {
	return &slo.SLO{
		Class:          class,
		DeadlineMs:     deadline,
		OffloadAllowed: offload,
		Priority:       prio,
	}
}

func newEngine() *decision.Engine {
	ps := decision.NewProfileStore(fake.NewSimpleClientset(), 100, decision.DefaultHistogramConfig())

	// Initialize Lyapunov scheduler
	lyap := decision.NewLyapunovScheduler(1.0)
	lyap.SetTargetViolationRate("latency", 0.05)
	lyap.SetTargetViolationRate("interactive", 0.05)
	lyap.SetTargetViolationRate("throughput", 0.10)
	lyap.SetTargetViolationRate("streaming", 0.10)
	lyap.SetTargetViolationRate("batch", 0.20)

	cfg := decision.EngineConfig{
		RTTUnusableMs:           300,
		LossUnusablePct:         10,
		LocalityBonus:           50,
		ConfidenceWeight:        30,
		ExplorationRate:         0, // deterministic
		MaxProfileCount:         100,
		ProfileStore:            ps,
		LyapunovScheduler:       lyap,
		CloudCostFactor:         1.0,
		EdgeCostFactor:          0.0,
		CloudMarginOverridePct:  0.15,
		WanStaleConfFactor:      0.8,
		EdgeHeadroomOverridePct: 0.1,
		EdgePendingPessimismPct: 10,
	}
	return decision.NewEngine(cfg)
}

func TestDecide_WANUnusable_ForcesEdge(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)
	local := &telemetry.LocalState{
		PendingPodsPerClass: map[string]int{"latency": 0},
		TotalDemand:         map[string]telemetry.DemandByClass{},
		TotalAllocatableCPU: 1000,
		TotalAllocatableMem: 1000,
		FreeCPU:             1000,
		FreeMem:             1000,
		BestEdgeNode:        telemetry.BestNode{Name: "edge1", FreeCPU: 1000, FreeMem: 1000},
		Timestamp:           time.Now(),
	}
	wan := &telemetry.WANState{RTTMs: 400, LossPct: 15}

	res := e.Decide(p, s, local, wan)
	if res.Location != constants.Edge || res.Reason != "wan_unusable" {
		t.Fatalf("want EDGE wan_unusable, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_OffloadDisabled_StaysEdge(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, false, 5)
	local := &telemetry.LocalState{
		PendingPodsPerClass: map[string]int{"latency": 0},
		TotalDemand:         map[string]telemetry.DemandByClass{},
		TotalAllocatableCPU: 1000,
		TotalAllocatableMem: 1000,
		FreeCPU:             1000,
		FreeMem:             1000,
		BestEdgeNode:        telemetry.BestNode{Name: "edge1", FreeCPU: 1000, FreeMem: 1000},
		Timestamp:           time.Now(),
	}
	wan := &telemetry.WANState{RTTMs: 50, LossPct: 0.1}

	res := e.Decide(p, s, local, wan)
	if res.Location != constants.Edge || res.Reason != "offload_disabled" {
		t.Fatalf("want EDGE offload_disabled, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_CloudFeasibleOnly(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 100, true, 5) // Very tight deadline

	local := &telemetry.LocalState{
		FreeCPU:             1000,
		FreeMem:             1000,
		PendingPodsPerClass: map[string]int{"latency": 10}, // Queue will cause edge to miss deadline
		TotalDemand: map[string]telemetry.DemandByClass{
			"latency": {CPU: 200, Mem: 128},
		},
		TotalAllocatableCPU: 1000,
		TotalAllocatableMem: 1000,
		BestEdgeNode:        telemetry.BestNode{Name: "edge1", FreeCPU: 800, FreeMem: 800},
		Timestamp:           time.Now(),
	}
	// Cloud is fast enough
	wan := &telemetry.WANState{RTTMs: 10, LossPct: 0.0}

	res := e.Decide(p, s, local, wan)
	if res.Location != constants.Cloud {
		t.Fatalf("expected CLOUD, got %v (%s)", res.Location, res.Reason)
	}
}

func TestDecide_StaleCircuitBreakers(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)

	// Local stale >5m => force edge
	local := &telemetry.LocalState{
		PendingPodsPerClass: map[string]int{"latency": 0},
		TotalDemand:         map[string]telemetry.DemandByClass{},
		IsStale:             true,
		StaleDuration:       6 * time.Minute,
		Timestamp:           time.Now().Add(-6 * time.Minute),
	}
	wan := &telemetry.WANState{RTTMs: 50}

	res := e.Decide(p, s, local, wan)
	if res.Location != constants.Edge || res.Reason != "telemetry_circuit_breaker" {
		t.Fatalf("want EDGE telemetry_circuit_breaker, got %v %s", res.Location, res.Reason)
	}

	// WAN stale >10m => force edge with wan_circuit_breaker
	local.IsStale = false
	local.StaleDuration = 0
	local.Timestamp = time.Now()
	wan.IsStale = true
	wan.StaleDuration = 11 * time.Minute
	res2 := e.Decide(p, s, local, wan)
	if res2.Location != constants.Edge || res2.Reason != "wan_circuit_breaker" {
		t.Fatalf("want EDGE wan_circuit_breaker, got %v %s", res2.Location, res2.Reason)
	}
}

func TestDecide_EdgePreferred_BothFeasible(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 5000, true, 5)

	local := &telemetry.LocalState{
		FreeCPU:             800,
		FreeMem:             800,
		PendingPodsPerClass: map[string]int{"latency": 0},
		TotalDemand: map[string]telemetry.DemandByClass{
			"latency": {CPU: 200, Mem: 128},
		},
		TotalAllocatableCPU: 1000,
		TotalAllocatableMem: 1000,
		BestEdgeNode:        telemetry.BestNode{Name: "edge1", FreeCPU: 800, FreeMem: 800},
		Timestamp:           time.Now(),
	}

	wan := &telemetry.WANState{RTTMs: 50, LossPct: 1}

	res := e.Decide(p, s, local, wan)

	// Both should be feasible, edge should be preferred due to locality
	if res.Location != constants.Edge {
		t.Fatalf("want EDGE (both feasible), got %v (reason=%s)", res.Location, res.Reason)
	}
}

func TestDecide_LyapunovAdaptsToViolations(t *testing.T) {
	e := newEngine()
	lyap := e.GetLyapunovScheduler()

	p := podWith("latency", 200, 128)
	s := sloMust("latency", 1000, true, 5)

	local := &telemetry.LocalState{
		FreeCPU:             500,
		FreeMem:             500,
		PendingPodsPerClass: map[string]int{"latency": 5},
		TotalDemand: map[string]telemetry.DemandByClass{
			"latency": {CPU: 500, Mem: 500},
		},
		TotalAllocatableCPU: 1000,
		TotalAllocatableMem: 1000,
		BestEdgeNode:        telemetry.BestNode{Name: "edge1", FreeCPU: 500, FreeMem: 500},
		Timestamp:           time.Now(),
	}

	wan := &telemetry.WANState{RTTMs: 20, LossPct: 0.5}

	// Initial decision
	res1 := e.Decide(p, s, local, wan)
	initialZ := lyap.GetVirtualQueue("latency")

	t.Logf("Initial decision: %s (Z=%.2f)", res1.Location, initialZ)

	// Simulate several edge violations
	for i := 0; i < 5; i++ {
		lyap.UpdateVirtualQueue("latency", 1000, 1500, constants.Edge)
	}

	afterViolationsZ := lyap.GetVirtualQueue("latency")
	t.Logf("After 5 edge violations: Z=%.2f", afterViolationsZ)

	// Virtual queue should have grown
	if afterViolationsZ <= initialZ {
		t.Errorf("Expected virtual queue to grow after violations, got %.2f <= %.2f",
			afterViolationsZ, initialZ)
	}

	// Make another decision - should be more likely to choose cloud
	res2 := e.Decide(p, s, local, wan)
	t.Logf("Decision after violations: %s (Z=%.2f)", res2.Location, afterViolationsZ)

	// Now simulate cloud successes to bring Z back down
	for i := 0; i < 10; i++ {
		lyap.UpdateVirtualQueue("latency", 1000, 800, constants.Cloud)
	}

	finalZ := lyap.GetVirtualQueue("latency")
	t.Logf("After 10 cloud successes: Z=%.2f", finalZ)

	// Virtual queue should decrease
	if finalZ >= afterViolationsZ {
		t.Errorf("Expected virtual queue to decrease after successes, got %.2f >= %.2f",
			finalZ, afterViolationsZ)
	}
}
