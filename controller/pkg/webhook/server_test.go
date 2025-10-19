package webhook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/time/rate"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
	"kubernetes-hybrid-scheduler/controller/pkg/webhook"
)

type fakeTel struct{}

func (f *fakeTel) GetLocalState(_ context.Context) (*telemetry.LocalState, error) {
	return &telemetry.LocalState{PendingPodsPerClass: map[string]int{}}, nil
}
func (f *fakeTel) GetWANState(_ context.Context) (*telemetry.WANState, error) {
	return &telemetry.WANState{RTTMs: 10}, nil
}
func (f *fakeTel) GetCachedLocalState() *telemetry.LocalState {
	return &telemetry.LocalState{PendingPodsPerClass: map[string]int{}}
}
func (f *fakeTel) GetCachedWANState() *telemetry.WANState { return &telemetry.WANState{RTTMs: 10} }
func (f *fakeTel) UpdateMetrics()                         {}

type fakeEngine struct{ res decision.Result }

func (f *fakeEngine) Decide(_ *corev1.Pod, _ *slo.SLO, _ *telemetry.LocalState, _ *telemetry.WANState) decision.Result {
	return f.res
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

	eng := &fakeEngine{res: decision.Result{
		Location:       decision.Edge,
		PredictedETAMs: 100,
		WanRttMs:       10,
		Reason:         "edge_preferred",
	}}

	s := webhook.NewServer(eng, &fakeTel{}, rate.NewLimiter(100, 100), fake.NewSimpleClientset())
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
