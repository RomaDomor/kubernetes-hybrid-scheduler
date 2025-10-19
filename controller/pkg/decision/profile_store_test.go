package decision_test

import (
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"testing"

	"kubernetes-hybrid-scheduler/controller/pkg/decision"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/fake"
)

func testPod(cpuMillis, memMi int64, class string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "p",
			UID:       uuid.NewUUID(),
			Annotations: map[string]string{
				constants.AnnotationSLOClass: class,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "c",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resourceMilli(cpuMillis),
							corev1.ResourceMemory: resourceMi(memMi),
						},
					},
				},
			},
		},
	}
}

func resourceMilli(m int64) resource.Quantity {
	return *resource.NewMilliQuantity(m, resource.DecimalSI)
}
func resourceMi(mi int64) resource.Quantity {
	return *resource.NewQuantity(mi*1024*1024, resource.BinarySI)
}

func TestProfileKey_TieringAndClass(t *testing.T) {
	p := testPod(250, 128, "latency")
	key := decision.GetProfileKey(p, decision.Edge)
	if key.CPUTier != "small" || key.Class != "latency" || key.Location != decision.Edge {
		t.Fatalf("unexpected key: %+v", key)
	}
	p2 := testPod(2500, 1024, "unknown")
	key2 := decision.GetProfileKey(p2, decision.Cloud)
	if key2.CPUTier != "large" || key2.Class != "batch" {
		t.Fatalf("class normalization failed: %+v", key2)
	}
}

func TestProfileStore_UpdateAndHistogram(t *testing.T) {
	ps := decision.NewProfileStore(fake.NewSimpleClientset(), 100, decision.DefaultHistogramConfig())
	key := decision.ProfileKey{Class: "latency", CPUTier: "small", Location: decision.Edge}

	// Feed some durations
	for _, v := range []float64{40, 45, 50, 55, 60, 100, 150} {
		ps.Update(key, decision.ProfileUpdate{ObservedDurationMs: v, QueueWaitMs: 5, SLOMet: v < 80})
	}
	got := ps.GetOrDefault(key)
	if got.Count == 0 || got.P95DurationMs < 60 {
		t.Fatalf("unexpected stats: count=%d p95=%.1f", got.Count, got.P95DurationMs)
	}
	if got.ConfidenceScore <= 0 {
		t.Fatalf("confidence not growing")
	}
}

func TestProfileStore_LRUEviction(t *testing.T) {
	ps := decision.NewProfileStore(fake.NewSimpleClientset(), 2, decision.DefaultHistogramConfig())
	keys := []decision.ProfileKey{
		{Class: "latency", CPUTier: "small", Location: decision.Edge},
		{Class: "batch", CPUTier: "small", Location: decision.Edge},
		{Class: "throughput", CPUTier: "small", Location: decision.Edge},
	}
	for _, k := range keys {
		ps.Update(k, decision.ProfileUpdate{ObservedDurationMs: 10})
	}
	// Access 2nd to refresh in LRU
	_ = ps.GetOrDefault(keys[1])

	// Add third should evict the first (LRU)
	first := ps.GetOrDefault(keys[0])
	// After eviction, fetching returns default with count==0
	if first.Count > 1 {
		t.Fatalf("expected evicted key to return fresh profile, got count=%d", first.Count)
	}
}

func TestProfileStore_Serialization(t *testing.T) {
	ps := decision.NewProfileStore(fake.NewSimpleClientset(), 10, decision.DefaultHistogramConfig())
	k := decision.ProfileKey{Class: "latency", CPUTier: "small", Location: decision.Edge}
	ps.Update(k, decision.ProfileUpdate{ObservedDurationMs: 42, SLOMet: true})

	// Export and check we have data
	exported := ps.ExportAllProfiles()
	if len(exported) == 0 {
		t.Fatalf("expected at least one profile in export")
	}

	keyStr := k.String()
	profile, ok := exported[keyStr]
	if !ok {
		t.Fatalf("profile %s not found in export", keyStr)
	}

	if profile.Count == 0 {
		t.Fatalf("exported profile has zero count")
	}
}
