package decision_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

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
	cfg := decision.EngineConfig{
		RTTThresholdMs:   100,
		LossThresholdPct: 2,
		RTTUnusableMs:    300,
		LossUnusablePct:  10,
		LocalityBonus:    50,
		ConfidenceWeight: 30,
		ExplorationRate:  0, // deterministic
		MaxProfileCount:  100,
		ProfileStore:     ps,
	}
	return decision.NewEngine(cfg)
}

func TestDecide_WANUnusable_ForcesEdge(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)
	local := &telemetry.LocalState{PendingPodsPerClass: map[string]int{"latency": 0}}
	wan := &telemetry.WANState{RTTMs: 400, LossPct: 15}

	res := e.Decide(p, s, local, wan)
	if res.Location != decision.Edge || res.Reason != "wan_unusable" {
		t.Fatalf("want EDGE wan_unusable, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_OffloadDisabled_StaysEdge(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, false, 5)
	local := &telemetry.LocalState{PendingPodsPerClass: map[string]int{"latency": 0}}
	wan := &telemetry.WANState{RTTMs: 50, LossPct: 0.1}

	res := e.Decide(p, s, local, wan)
	if res.Location != decision.Edge || res.Reason != "offload_disabled" {
		t.Fatalf("want EDGE offload_disabled, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_CloudFeasibleOnly(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 1000, true, 5)

	local := &telemetry.LocalState{
		FreeCPU:             0,
		FreeMem:             0,
		PendingPodsPerClass: map[string]int{"latency": 100}, // large queue to hurt edge P95
		// Best edge node can't fit the pod -> nodeOK=false
		BestEdgeNode: telemetry.BestNode{Name: "edge1", FreeCPU: 0, FreeMem: 0},
	}
	// Keep WAN healthy so cloud is feasible
	wan := &telemetry.WANState{RTTMs: 10, LossPct: 0.0}

	res := e.Decide(p, s, local, wan)
	if res.Location != decision.Cloud {
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
		IsStale:             true,
		StaleDuration:       6 * time.Minute,
	}
	wan := &telemetry.WANState{RTTMs: 50}

	res := e.Decide(p, s, local, wan)
	if res.Location != decision.Edge || res.Reason != "telemetry_circuit_breaker" {
		t.Fatalf("want EDGE telemetry_circuit_breaker, got %v %s", res.Location, res.Reason)
	}

	// WAN stale >10m => force edge with wan_circuit_breaker
	local.IsStale = false
	local.StaleDuration = 0
	wan.IsStale = true
	wan.StaleDuration = 11 * time.Minute
	res2 := e.Decide(p, s, local, wan)
	if res2.Location != decision.Edge || res2.Reason != "wan_circuit_breaker" {
		t.Fatalf("want EDGE wan_circuit_breaker, got %v %s", res2.Location, res2.Reason)
	}
}
