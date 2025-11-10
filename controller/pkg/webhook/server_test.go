package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/time/rate"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/decision"
)

type fakeTel struct{}

func (f *fakeTel) GetLocalState(_ context.Context) (*apis.LocalState, error) {
	return &apis.LocalState{
		PendingPodsPerClass:   map[string]int{},
		TotalDemand:           map[string]apis.DemandByClass{},
		TotalAllocatableCPU:   1000,
		TotalAllocatableMem:   1000,
		FreeCPU:               1000,
		FreeMem:               1000,
		IsCompleteSnapshot:    true,
		MeasurementConfidence: 1.0,
	}, nil
}
func (f *fakeTel) GetWANState(_ context.Context) (*apis.WANState, error) {
	return &apis.WANState{RTTMs: 10}, nil
}
func (f *fakeTel) GetCachedLocalState() *apis.LocalState {
	return &apis.LocalState{
		PendingPodsPerClass:   map[string]int{},
		TotalDemand:           map[string]apis.DemandByClass{},
		TotalAllocatableCPU:   1000,
		TotalAllocatableMem:   1000,
		FreeCPU:               1000,
		FreeMem:               1000,
		IsCompleteSnapshot:    true,
		MeasurementConfidence: 1.0,
	}
}
func (f *fakeTel) GetCachedWANState() *apis.WANState { return &apis.WANState{RTTMs: 10} }
func (f *fakeTel) UpdateMetrics()                    {}
func (f *fakeTel) LockForDecision()                  {}
func (f *fakeTel) UnlockForDecision()                {}

type fakeEngine struct{ res apis.Result }

func (f *fakeEngine) Decide(_ *corev1.Pod, _ *apis.SLO, _ *apis.LocalState, _ *apis.WANState) apis.Result {
	return f.res
}
func (f *fakeEngine) GetLyapunovScheduler() *decision.LyapunovScheduler {
	return decision.NewLyapunovScheduler()
}

func TestWebhook_ManagedPodPatched(t *testing.T) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "p1",
			Labels:    map[string]string{constants.LabelManaged: "true"},
			Annotations: map[string]string{
				constants.AnnotationSLODeadline: "1000",
				constants.AnnotationSLOClass:    "latency",
			},
		},
	}
	raw, _ := json.Marshal(pod)
	review := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admission.k8s.io/v1",
			Kind:       "AdmissionReview",
		},
		Request: &admissionv1.AdmissionRequest{
			UID:       "123",
			Kind:      metav1.GroupVersionKind{Kind: "Pod"},
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
	body, _ := json.Marshal(review)

	eng := &fakeEngine{res: apis.Result{
		Location:       constants.Edge,
		PredictedETAMs: 100,
		WanRttMs:       10,
		Reason:         "edge_preferred",
	}}

	s := NewServer(eng, &fakeTel{}, rate.NewLimiter(100, 100), fake.NewSimpleClientset())
	req := httptest.NewRequest(http.MethodPost, "/mutate", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d, body: %s", w.Code, w.Body.String())
	}
	var out admissionv1.AdmissionReview
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if out.Response == nil || !out.Response.Allowed || out.Response.Patch == nil {
		t.Fatalf("expected allowed with patch, got: %+v", out.Response)
	}
}
