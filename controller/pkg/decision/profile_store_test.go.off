package decision

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestProfileStore_Update(t *testing.T) {
	store := NewProfileStore()

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
							corev1.ResourceCPU: resource.MustParse("100m"),
						},
					},
				},
			},
		},
	}

	key := GetProfileKey(pod, Edge)

	// First update
	store.Update(key, ProfileUpdate{
		ObservedDurationMs: 50,
		QueueWaitMs:        10,
		SLOMet:             true,
	})

	profile := store.GetOrDefault(key)
	if profile.Count != 1 {
		t.Errorf("Expected count=1, got %d", profile.Count)
	}

	// Second update
	store.Update(key, ProfileUpdate{
		ObservedDurationMs: 60,
		QueueWaitMs:        15,
		SLOMet:             true,
	})

	profile = store.GetOrDefault(key)
	if profile.Count != 2 {
		t.Errorf("Expected count=2, got %d", profile.Count)
	}

	// Mean should be moving toward new value
	if profile.MeanDurationMs < 50 || profile.MeanDurationMs > 60 {
		t.Errorf("Unexpected mean: %.2f", profile.MeanDurationMs)
	}
}
