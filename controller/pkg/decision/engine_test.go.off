package decision

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

func TestDecisionEngine_EdgeFeasible(t *testing.T) {
	engine := NewEngine(Config{
		RTTThresholdMs:   100,
		LossThresholdPct: 2.0,
	})

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"slo.hybrid.io/class": "latency",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
				},
			},
		},
	}

	sloData := &slo.SLO{
		DeadlineMs:     200,
		Class:          "latency",
		Priority:       5,
		OffloadAllowed: true,
	}

	local := &telemetry.LocalState{
		FreeCPU:             1000,
		FreeMem:             2048,
		PendingPodsPerClass: map[string]int{"latency": 0},
	}

	wan := &telemetry.WANState{
		RTTMs:   50,
		LossPct: 0,
	}

	result := engine.Decide(pod, sloData, local, wan)

	if result.Location != Edge {
		t.Errorf("Expected Edge, got %v (reason: %s)", result.Location, result.Reason)
	}
}

func TestDecisionEngine_OffloadDisabled(t *testing.T) {
	engine := NewEngine(Config{})

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{"slo.hybrid.io/class": "batch"},
		},
	}

	sloData := &slo.SLO{
		OffloadAllowed: false,
	}

	result := engine.Decide(pod, sloData, nil, nil)

	if result.Location != Edge || result.Reason != "offload_disabled" {
		t.Errorf("Expected Edge/offload_disabled, got %v/%s", result.Location, result.Reason)
	}
}
