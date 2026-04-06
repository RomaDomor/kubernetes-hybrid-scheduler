package webhook

import (
	"bytes"
	"encoding/json"
	"kubernetes-hybrid-scheduler/controller/gen/mocks"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

func makeAdmissionReview(pod *corev1.Pod) admissionv1.AdmissionReview {
	raw, _ := json.Marshal(pod)
	return admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request: &admissionv1.AdmissionRequest{
			UID:       "test-uid",
			Operation: admissionv1.Create,
			Kind:      metav1.GroupVersionKind{Kind: "Pod"},
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
}

func postReview(t *testing.T, srv *Server, pod *corev1.Pod) *admissionv1.AdmissionReview {
	t.Helper()
	review := makeAdmissionReview(pod)
	body, err := json.Marshal(review)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/mutate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp admissionv1.AdmissionReview
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return &resp
}

func TestServeHTTP_NotManaged(t *testing.T) {
	mockDec := mocks.NewDecider(t)
	mockCol := mocks.NewCollector(t)
	limiter := rate.NewLimiter(rate.Limit(100), 200)
	srv := NewServer(mockDec, mockCol, limiter, fake.NewClientset())

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels:    map[string]string{},
		},
	}

	resp := postReview(t, srv, pod)

	assert.True(t, resp.Response.Allowed, "non-managed pod should be allowed")
	assert.Nil(t, resp.Response.Patch, "non-managed pod should have no patch")
	mockDec.AssertNotCalled(t, "Decide")
}

func TestServeHTTP_Managed(t *testing.T) {
	mockDec := mocks.NewDecider(t)
	mockCol := mocks.NewCollector(t)
	limiter := rate.NewLimiter(rate.Limit(100), 200)
	srv := NewServer(mockDec, mockCol, limiter, fake.NewClientset())

	localState := &apis.ClusterState{
		FreeCPU: 4000,
		FreeMem: 8192,
	}

	mockCol.On("LockForDecision").Return()
	mockCol.On("UnlockForDecision").Return()
	mockCol.On("GetAllClusterIDs").Return([]constants.ClusterID{constants.LocalCluster})
	mockCol.On("GetClusterState", mock.Anything, constants.LocalCluster).Return(localState, nil)
	mockCol.On("GetCachedClusterState", constants.LocalCluster).Return(localState).Maybe()

	mockDec.On("Decide", mock.MatchedBy(func(pod *corev1.Pod) bool { return true }), mock.MatchedBy(func(slo *apis.SLO) bool { return true }), mock.Anything).Return(apis.Result{
		Location:       constants.LocalCluster,
		Reason:         constants.ReasonLocalPreferred,
		PredictedETAMs: 100,
		WanRttMs:       0,
	})

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels:    map[string]string{constants.LabelManaged: "true"},
			Annotations: map[string]string{
				constants.AnnotationSLOClass:    "latency",
				constants.AnnotationSLODeadline: "500",
			},
		},
	}

	resp := postReview(t, srv, pod)

	assert.True(t, resp.Response.Allowed)
	assert.NotNil(t, resp.Response.Patch, "managed pod should have a patch")
	mockDec.AssertCalled(t, "Decide", mock.Anything, mock.Anything, mock.Anything)
}

func TestServeHTTP_RateLimited(t *testing.T) {
	mockDec := mocks.NewDecider(t)
	mockCol := mocks.NewCollector(t)
	limiter := rate.NewLimiter(rate.Limit(0), 0) // always limited
	srv := NewServer(mockDec, mockCol, limiter, fake.NewClientset())

	req := httptest.NewRequest(http.MethodPost, "/mutate", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusTooManyRequests, w.Code)
}

func TestServeHTTP_AlreadyDecided(t *testing.T) {
	mockDec := mocks.NewDecider(t)
	mockCol := mocks.NewCollector(t)
	limiter := rate.NewLimiter(rate.Limit(100), 200)
	srv := NewServer(mockDec, mockCol, limiter, fake.NewClientset())

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels:    map[string]string{constants.LabelManaged: "true"},
			Annotations: map[string]string{
				constants.AnnotationDecision: "local",
			},
		},
	}

	resp := postReview(t, srv, pod)

	assert.True(t, resp.Response.Allowed)
	assert.Nil(t, resp.Response.Patch, "already-decided pod should have no patch")
	mockDec.AssertNotCalled(t, "Decide")
}

func TestServeHTTP_InvalidMethod(t *testing.T) {
	mockDec := mocks.NewDecider(t)
	mockCol := mocks.NewCollector(t)
	limiter := rate.NewLimiter(rate.Limit(100), 200)
	srv := NewServer(mockDec, mockCol, limiter, fake.NewClientset())

	req := httptest.NewRequest(http.MethodGet, "/mutate", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestServeHTTP_InvalidBody(t *testing.T) {
	mockDec := mocks.NewDecider(t)
	mockCol := mocks.NewCollector(t)
	limiter := rate.NewLimiter(rate.Limit(100), 200)
	srv := NewServer(mockDec, mockCol, limiter, fake.NewClientset())

	req := httptest.NewRequest(http.MethodPost, "/mutate", bytes.NewReader([]byte("not-json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}
