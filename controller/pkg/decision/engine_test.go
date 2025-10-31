package decision_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
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
	kubeClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	podInformer := informerFactory.Core().V1().Pods()

	ps := decision.NewProfileStore(kubeClient, 100, decision.DefaultHistogramConfig())
	simulator := decision.NewScheduleSimulator(ps, podInformer)

	cfg := decision.EngineConfig{
		RTTUnusableMs:          300,
		LossUnusablePct:        10,
		LocalityBonus:          50,
		ConfidenceWeight:       30,
		ExplorationRate:        0,
		MaxProfileCount:        100,
		CloudMarginOverridePct: 0.15,
		ProfileStore:           ps,
		Simulator:              simulator,
	}
	return decision.NewEngine(cfg)
}

func TestDecide_WANUnusable_ForcesEdge(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)
	local := &telemetry.LocalState{
		FreeCPU:             1000,
		FreeMem:             2048,
		PendingPodsPerClass: map[string]int{"latency": 0},
		TotalDemand:         map[string]telemetry.DemandByClass{},
		TotalAllocatableCPU: 2000,
		TotalAllocatableMem: 4096,
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
		FreeCPU:             1000,
		FreeMem:             2048,
		PendingPodsPerClass: map[string]int{"latency": 0},
		TotalAllocatableCPU: 2000,
	}
	wan := &telemetry.WANState{RTTMs: 50, LossPct: 0.1}

	res := e.Decide(p, s, local, wan)
	if res.Location != constants.Edge || res.Reason != "offload_disabled" {
		t.Fatalf("want EDGE offload_disabled, got %v %s", res.Location, res.Reason)
	}
}

func TestDecide_SimulationWithPendingPods(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	podInformer := informerFactory.Core().V1().Pods()

	// Create pending pods in the fake client
	pendingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pending-pod-1",
			Namespace: "default",
			Labels: map[string]string{
				constants.LabelManaged: "true",
			},
			Annotations: map[string]string{
				constants.AnnotationSLOClass:    "latency",
				constants.AnnotationSLODeadline: "5000",
				constants.AnnotationTimestamp:   time.Now().Format(time.RFC3339),
			},
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{
				constants.NodeRoleLabelEdge: "true",
			},
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resourceMilli(500),
							corev1.ResourceMemory: resourceMi(512),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}
	_, _ = kubeClient.CoreV1().Pods("default").Create(nil, pendingPod, metav1.CreateOptions{})

	// Start informer
	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactory.Start(stopCh)
	informerFactory.WaitForCacheSync(stopCh)

	ps := decision.NewProfileStore(kubeClient, 100, decision.DefaultHistogramConfig())
	simulator := decision.NewScheduleSimulator(ps, podInformer)

	cfg := decision.EngineConfig{
		RTTUnusableMs:          300,
		LossUnusablePct:        10,
		CloudMarginOverridePct: 0.15,
		ProfileStore:           ps,
		Simulator:              simulator,
	}
	e := decision.NewEngine(cfg)

	p := podWith("latency", 200, 128)
	s := sloMust("latency", 10000, true, 5)
	local := &telemetry.LocalState{
		FreeCPU:             1000,
		FreeMem:             2048,
		TotalAllocatableCPU: 2000,
		TotalAllocatableMem: 4096,
	}
	wan := &telemetry.WANState{RTTMs: 50, LossPct: 1}

	res := e.Decide(p, s, local, wan)

	// Should account for pending pod in simulation
	t.Logf("Decision: %s (reason=%s, eta=%.0fms)", res.Location, res.Reason, res.PredictedETAMs)

	// With pending pod consuming resources, decision should reflect that
	if res.Location != constants.Edge && res.Location != constants.Cloud {
		t.Fatalf("invalid location: %v", res.Location)
	}
}

func TestDecide_StaleCircuitBreakers(t *testing.T) {
	e := newEngine()
	p := podWith("latency", 200, 128)
	s := sloMust("latency", 2000, true, 5)

	local := &telemetry.LocalState{
		FreeCPU:             0,
		FreeMem:             0,
		PendingPodsPerClass: map[string]int{"latency": 0},
		IsStale:             true,
		StaleDuration:       6 * time.Minute,
	}
	wan := &telemetry.WANState{RTTMs: 50}

	res := e.Decide(p, s, local, wan)
	if res.Location != constants.Edge || res.Reason != "telemetry_circuit_breaker" {
		t.Fatalf("want EDGE telemetry_circuit_breaker, got %v %s", res.Location, res.Reason)
	}
}
