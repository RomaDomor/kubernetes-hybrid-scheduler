package decision

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/fake"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

// cloudCluster is the remote cluster ID used across all decision package tests.
const cloudCluster = constants.ClusterID("cloud-1")

func testPod(cpuMillis, memMi int64, class string) *corev1.Pod {
	return testPodWithDeadline(cpuMillis, memMi, class, "120000")
}

func testPodWithDeadline(cpuMillis, memMi int64, class, deadlineMs string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "p",
			UID:       uuid.NewUUID(),
			Annotations: map[string]string{
				constants.AnnotationSLOClass:    class,
				constants.AnnotationSLODeadline: deadlineMs,
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
	p := testPodWithDeadline(250, 128, "latency", "25000")
	key := apis.GetProfileKey(p, constants.LocalCluster)
	if key.CPUTier != "small" || key.Class != "latency" || key.ClusterID != constants.LocalCluster {
		t.Fatalf("unexpected key: %+v", key)
	}
	if key.DeadlineBucket != "tight" {
		t.Fatalf("expected tight deadline bucket, got %q", key.DeadlineBucket)
	}
	p2 := testPod(2500, 1024, "unknown") // default deadline 120000ms → loose
	key2 := apis.GetProfileKey(p2, cloudCluster)
	if key2.CPUTier != "large" || key2.Class != "batch" {
		t.Fatalf("class normalization failed: %+v", key2)
	}
	if key2.DeadlineBucket != "loose" {
		t.Fatalf("expected loose deadline bucket, got %q", key2.DeadlineBucket)
	}
	p3 := testPodWithDeadline(1500, 256, "batch", "50000")
	key3 := apis.GetProfileKey(p3, cloudCluster)
	if key3.DeadlineBucket != "medium" {
		t.Fatalf("expected medium deadline bucket, got %q", key3.DeadlineBucket)
	}
}

func TestProfileStore_UpdateAndHistogram(t *testing.T) {
	ps := NewProfileStore(fake.NewClientset(), 100, DefaultHistogramConfig())
	key := apis.ProfileKey{Class: "latency", CPUTier: "small", ClusterID: constants.LocalCluster}

	// Feed some durations
	for _, v := range []float64{40, 45, 50, 55, 60, 100, 150} {
		ps.Update(key, apis.ProfileUpdate{ObservedDurationMs: v, QueueWaitMs: 5, SLOMet: v < 80})
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
	ps := NewProfileStore(fake.NewClientset(), 2, DefaultHistogramConfig())
	keys := []apis.ProfileKey{
		{Class: "latency", CPUTier: "small", ClusterID: constants.LocalCluster},
		{Class: "batch", CPUTier: "small", ClusterID: constants.LocalCluster},
		{Class: "throughput", CPUTier: "small", ClusterID: constants.LocalCluster},
	}
	for _, k := range keys {
		ps.Update(k, apis.ProfileUpdate{ObservedDurationMs: 10})
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
	ps := NewProfileStore(fake.NewClientset(), 10, DefaultHistogramConfig())
	k := apis.ProfileKey{Class: "latency", CPUTier: "small", ClusterID: constants.LocalCluster}
	ps.Update(k, apis.ProfileUpdate{ObservedDurationMs: 42, SLOMet: true})

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
