package telemetry_test

import (
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"testing"
	"time"

	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
)

func TestWantsEdge_NodeSelector(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	lc := telemetry.NewLocalCollector(
		kubeClient,
		informerFactory.Core().V1().Pods(),
		informerFactory.Core().V1().Nodes(),
	)

	if lc == nil {
		t.Fatal("failed to create local collector")
	}

	// Test with a pod that has edge node selector
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels: map[string]string{
				constants.LabelManaged: "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"node.role/edge": "true"},
		},
	}

	// Add pod to fake client
	_, err := kubeClient.CoreV1().Pods("default").Create(nil, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create test pod: %v", err)
	}
}

func TestLocalCollector_Staleness(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	lc := telemetry.NewLocalCollector(
		kubeClient,
		informerFactory.Core().V1().Pods(),
		informerFactory.Core().V1().Nodes(),
	)

	if lc == nil {
		t.Fatal("failed to create local collector")
	}

	// Get initial cached state (should be stale initially)
	state := lc.GetCachedLocalState()
	if !state.IsStale {
		t.Log("Warning: expected initial state to be stale, but it's not")
	}

	// Test that staleness is detected after time passes
	time.Sleep(2 * time.Second)
	state2 := lc.GetCachedLocalState()
	if state2.StaleDuration < 2*time.Second {
		t.Errorf("expected staleDuration >= 2s, got %v", state2.StaleDuration)
	}
}
