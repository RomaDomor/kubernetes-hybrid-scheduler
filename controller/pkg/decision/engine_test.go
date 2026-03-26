package decision

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
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

func newEngine() *Engine {
	ps := NewProfileStore(fake.NewClientset(), 100, DefaultHistogramConfig())

	lyap := NewLyapunovScheduler()
	lyap.SetClassConfig("latency", &ClassConfig{Beta: 1.0, TargetViolationProb: 0.05})
	lyap.SetClassConfig("batch", &ClassConfig{Beta: 1.0, TargetViolationProb: 0.2})

	cfg := EngineConfig{
		RTTUnusableMs:           300,
		LossUnusablePct:         10,
		MaxProfileCount:         100,
		ProfileStore:            ps,
		LyapunovScheduler:       lyap,
		EdgePendingPessimismPct: 10,
		CostFactors: map[constants.ClusterID]float64{
			constants.LocalCluster: 0.0,
			cloudCluster:           1.0,
		},
	}
	return NewEngine(cfg)
}

// localStateWithDefaults provides a default healthy, mostly empty local state for tests.
func localStateWithDefaults() *apis.ClusterState {
	return &apis.ClusterState{
		ClusterID:               constants.LocalCluster,
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
		BestNode: apis.BestNode{
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

// remoteFromWAN builds a remote ClusterState from WAN metrics for tests.
func remoteFromWAN(wan *apis.WANState) *apis.ClusterState {
	if wan == nil {
		return nil
	}
	ts := wan.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	return &apis.ClusterState{
		ClusterID:               cloudCluster,
		FreeCPU:                 100000,
		FreeMem:                 100000,
		TotalAllocatableCPU:     100000,
		TotalAllocatableMem:     100000,
		EffectiveAllocatableCPU: 100000,
		EffectiveAllocatableMem: 100000,
		BestNode: apis.BestNode{
			Name:                    "cloud-node",
			FreeCPU:                 100000,
			FreeMem:                 100000,
			EffectiveAllocatableCPU: 100000,
			EffectiveAllocatableMem: 100000,
		},
		RTTMs:                 wan.RTTMs,
		LossPct:               wan.LossPct,
		IsStale:               wan.IsStale,
		StaleDuration:         wan.StaleDuration,
		Timestamp:             ts,
		IsCompleteSnapshot:    true,
		MeasurementConfidence: 1.0,
	}
}

// statesMap builds a cluster states map from a local state and optional WAN state.
func statesMap(local *apis.ClusterState, wan *apis.WANState) map[constants.ClusterID]*apis.ClusterState {
	m := make(map[constants.ClusterID]*apis.ClusterState)
	if local != nil {
		m[constants.LocalCluster] = local
	}
	if remote := remoteFromWAN(wan); remote != nil {
		m[cloudCluster] = remote
	}
	return m
}

func TestDecide_NilLocalState_ForcesLocal(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)
	wan := &apis.WANState{RTTMs: 50}

	// No local cluster in the map → safety fallback to local.
	states := statesMap(nil, wan)
	res := e.Decide(p, s, states)
	if res.Location != constants.LocalCluster || res.Reason != constants.ReasonTelemetryCircuitBreaker {
		t.Fatalf("want LOCAL with telemetry_circuit_breaker, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_NoRemoteCluster_ForcesLocal(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)
	local := localStateWithDefaults()

	// No remote cluster in the map → only local candidate.
	states := statesMap(local, nil)
	res := e.Decide(p, s, states)
	if res.Location != constants.LocalCluster {
		t.Fatalf("want LOCAL when no remote clusters, got %v %s", res.Location, res.Reason)
	}
}

// --- Feasibility Logic Tests ---

func TestDecide_InfeasibleResources_Hard_TooLargeForNode(t *testing.T) {
	e := newEngine()
	// Pod requests more CPU than the biggest edge node can offer, even when empty.
	p := podWith("batch", 10000, 128)
	s := sloMust("batch", 5000, true, 5)
	local := localStateWithDefaults() // Best node has 9000m effective CPU
	wan := &apis.WANState{RTTMs: 10}

	res := e.Decide(p, s, statesMap(local, wan))
	if res.Location == constants.LocalCluster {
		t.Fatalf("Expected remote when pod is too big for local, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_InfeasibleResources_Hard_UtilizationLimit(t *testing.T) {
	e := newEngine()
	// Pod fits, but scheduling it would push the cluster's manageable utilization over 90%.
	p := podWith("batch", 8000, 128)
	s := sloMust("batch", 5000, true, 5)
	local := localStateWithDefaults() // Has 9000m effective CPU, 8000m free.
	wan := &apis.WANState{RTTMs: 10}

	// Calculation: (EffectiveAlloc - Free + Request) / EffectiveAlloc
	// (9000 - 8000 + 8000) / 9000 = 9000 / 9000 = 100%, which is > 90%

	res := e.Decide(p, s, statesMap(local, wan))
	if res.Location == constants.LocalCluster {
		t.Fatalf("Expected remote due to utilization limit, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_InfeasibleTime_Edge(t *testing.T) {
	e := newEngine()
	ps := e.GetProfileStore()
	localKey := apis.GetProfileKey(podWith("latency", 200, 128), constants.LocalCluster)
	ps.Update(localKey, apis.ProfileUpdate{ObservedDurationMs: 1200}) // P95 > 1000ms deadline

	p := podWith("latency", 200, 128)
	s := sloMust("latency", 1000, true, 5)
	local := localStateWithDefaults()
	wan := &apis.WANState{RTTMs: 10}

	res := e.Decide(p, s, statesMap(local, wan))
	if res.Location == constants.LocalCluster {
		t.Fatalf("Expected remote when local is time-infeasible, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_InfeasibleResources_Soft(t *testing.T) {
	e := newEngine()
	p := podWith("batch", 2000, 512)
	s := sloMust("batch", 5000, true, 5)

	local := localStateWithDefaults()
	local.FreeCPU = 1000 // Not enough free CPU right now
	wan := &apis.WANState{RTTMs: 10}

	res := e.Decide(p, s, statesMap(local, wan))
	if res.Location == constants.LocalCluster {
		t.Fatalf("Expected remote when local is temporarily full, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_WANUnusable_ForcesLocal(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)
	local := localStateWithDefaults()
	wan := &apis.WANState{RTTMs: 400, LossPct: 15}

	// Remote is skipped due to bad WAN → only local candidate.
	res := e.Decide(p, s, statesMap(local, wan))
	if res.Location != constants.LocalCluster {
		t.Fatalf("want LOCAL when WAN is unusable, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_OffloadDisabled_StaysLocal(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, false, 5)
	local := localStateWithDefaults()
	wan := &apis.WANState{RTTMs: 50, LossPct: 0.1}

	res := e.Decide(p, s, statesMap(local, wan))
	if res.Location != constants.LocalCluster || res.Reason != constants.ReasonOffloadDisabled {
		t.Fatalf("want LOCAL offload_disabled, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_StaleCircuitBreakers(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)

	// Stale local → local is skipped, only remote remains.
	local := localStateWithDefaults()
	local.IsStale = true
	local.StaleDuration = 6 * time.Second
	local.Timestamp = time.Now().Add(-6 * time.Second)

	wan := &apis.WANState{RTTMs: 50}

	res := e.Decide(p, s, statesMap(local, wan))
	if res.Location == constants.LocalCluster {
		t.Fatalf("want remote when local is stale, got %v %s", res.Location, res.Reason)
	}

	// Stale WAN → remote is skipped, only local remains.
	local = localStateWithDefaults()
	staleWAN := &apis.WANState{
		RTTMs:         50,
		IsStale:       true,
		StaleDuration: 11 * time.Minute,
		Timestamp:     time.Now().Add(-11 * time.Minute),
	}

	res2 := e.Decide(p, s, statesMap(local, staleWAN))
	if res2.Location != constants.LocalCluster {
		t.Fatalf("want LOCAL when WAN is stale, got %v %s", res2.Location, res2.Reason)
	}
}

func TestDecide_EdgePreferred_WhenFasterAndFeasible(t *testing.T) {
	e := newEngine()
	ps := e.GetProfileStore()
	localKey := apis.GetProfileKey(podWith("latency", 200, 128), constants.LocalCluster)
	cloudKey := apis.GetProfileKey(podWith("latency", 200, 128), cloudCluster)
	ps.Update(localKey, apis.ProfileUpdate{ObservedDurationMs: 100})
	ps.Update(cloudKey, apis.ProfileUpdate{ObservedDurationMs: 500})

	p := podWith("latency", 200, 128)
	s := sloMust("latency", 5000, true, 5)
	local := localStateWithDefaults()
	wan := &apis.WANState{RTTMs: 10, LossPct: 0}

	res := e.Decide(p, s, statesMap(local, wan))

	if res.Location != constants.LocalCluster {
		t.Fatalf("want LOCAL, got %v (reason=%s)", res.Location, res.Reason)
	}
	if res.Reason != constants.ReasonLocalPreferred {
		t.Errorf("expected reason local_preferred, got %s", res.Reason)
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

	localKey := apis.GetProfileKey(p, constants.LocalCluster)
	cloudKey := apis.GetProfileKey(p, cloudCluster)

	ps.Update(localKey, apis.ProfileUpdate{ObservedDurationMs: 900})
	ps.Update(cloudKey, apis.ProfileUpdate{ObservedDurationMs: 880})

	// 1. Initial Decision: Z is zero. Both are feasible with no predicted violation.
	// Local is cheaper and faster, so it must be chosen.
	res1 := e.Decide(p, s, statesMap(local, wan))
	if res1.Location != constants.LocalCluster {
		t.Fatalf("Initial decision should be local (cheaper and faster), but got %s", res1.Location)
	}

	// 2. Simulate many SLO violations on the local cluster. This will increase the Z queue.
	for i := 0; i < 20; i++ {
		lyap.UpdateVirtualQueue("latency", 1000, 1200, constants.LocalCluster)
	}
	afterViolationsZ := lyap.GetVirtualQueue("latency")
	if afterViolationsZ <= 0 {
		t.Fatal("Expected virtual queue to grow")
	}

	ps.Update(localKey, apis.ProfileUpdate{ObservedDurationMs: 1200})

	// 4. Second Decision: Z is now very high, and the local profile correctly predicts a violation.
	res2 := e.Decide(p, s, statesMap(local, wan))
	if res2.Location == constants.LocalCluster {
		t.Errorf("Expected remote after many local violations, but got %s", res2.Location)
	}
}

func TestDecide_LowConfidenceForcesRemote(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)

	local := localStateWithDefaults()
	local.IsCompleteSnapshot = false
	local.MeasurementConfidence = 0.3

	wan := &apis.WANState{RTTMs: 50, LossPct: 0.1}

	// Local fails safety check (low confidence + latency class) → skipped.
	// Only remote remains.
	res := e.Decide(p, s, statesMap(local, wan))

	if res.Location == constants.LocalCluster {
		t.Fatalf("Expected remote with low measurement confidence, got %v %s", res.Location, res.Reason)
	}
}
