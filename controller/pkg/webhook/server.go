package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/time/rate"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
)

//go:generate mockery --name=Decider --output=../../gen/mocks
type Decider interface {
	Decide(pod *corev1.Pod, slo *apis.SLO, states map[constants.ClusterID]*apis.ClusterState) apis.Result
}

type Server struct {
	dec        Decider
	tel        telemetry.Collector
	limiter    *rate.Limiter
	kubeClient kubernetes.Interface
}

func NewServer(
	dec Decider,
	tel telemetry.Collector,
	limiter *rate.Limiter,
	kubeClient kubernetes.Interface,
) *Server {
	return &Server{
		dec:        dec,
		tel:        tel,
		limiter:    limiter,
		kubeClient: kubeClient,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() { admissionLatency.Observe(time.Since(start).Seconds()) }()

	if !s.limiter.Allow() {
		klog.Warning("Admission rate limit exceeded")
		admissionRateLimited.Inc()
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "invalid method", http.StatusMethodNotAllowed)
		return
	}

	var review admissionv1.AdmissionReview
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	req := review.Request
	if req == nil {
		writeResponse(w, review, deny(fmt.Errorf("nil request")))
		return
	}

	if req.Kind.Kind != "Pod" || req.Operation != admissionv1.Create {
		writeResponse(w, review, allow())
		return
	}

	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		writeResponse(w, review, deny(err))
		return
	}

	if pod.Labels[constants.LabelManaged] != "true" {
		writeResponse(w, review, allow())
		return
	}

	if pod.Annotations != nil && pod.Annotations[constants.AnnotationDecision] != "" {
		writeResponse(w, review, allow())
		return
	}

	resp := s.processScheduling(r.Context(), &pod)
	writeResponse(w, review, resp)
}

func (s *Server) processScheduling(
	ctx context.Context,
	pod *corev1.Pod,
) *admissionv1.AdmissionResponse {
	sloData, err := slo.ParseSLO(pod)
	if err != nil {
		klog.Warningf("%s: Invalid SLO: %v",
			util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID)), err)
		return allow()
	}

	s.tel.LockForDecision()
	defer s.tel.UnlockForDecision()

	// Collect state for all clusters
	clusterStates := make(map[constants.ClusterID]*apis.ClusterState)
	for _, id := range s.tel.GetAllClusterIDs() {
		ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		state, err := s.tel.GetClusterState(ctx2, id)
		cancel()
		if err != nil {
			klog.Warningf("Failed to get state for cluster %s: %v, using cache", id, err)
			state = s.tel.GetCachedClusterState(id)
		}
		if state != nil {
			clusterStates[id] = state
		}
	}

	result := s.dec.Decide(pod, sloData, clusterStates)

	// Create event
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))
	_, err = s.kubeClient.CoreV1().Events(pod.Namespace).Create(ctx, &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s.%x", pod.Name, time.Now().UnixNano()),
			Namespace: pod.Namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: pod.Namespace,
			Name:      pod.Name,
			UID:       pod.UID,
		},
		Reason:         "SchedulingDecision",
		Message:        fmt.Sprintf("Scheduled to %s: %s (ETA: %.0fms)", result.Location, result.Reason, result.PredictedETAMs),
		Type:           corev1.EventTypeNormal,
		FirstTimestamp: metav1.NewTime(time.Now()),
		LastTimestamp:  metav1.NewTime(time.Now()),
		Count:          1,
	}, metav1.CreateOptions{})
	if err != nil {
		klog.Warningf("%s: Failed to create event", podID)
	}

	klog.Infof("%s: Decision: %s (reason=%s, eta=%.0fms)", podID, result.Location, result.Reason, result.PredictedETAMs)
	return s.buildPatchResponse(pod, result)
}

func writeResponse(w http.ResponseWriter, in admissionv1.AdmissionReview, resp *admissionv1.AdmissionResponse) {
	resp.UID = in.Request.UID
	out := admissionv1.AdmissionReview{TypeMeta: in.TypeMeta, Response: resp}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func allow() *admissionv1.AdmissionResponse { return &admissionv1.AdmissionResponse{Allowed: true} }
func deny(err error) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{Allowed: false, Result: &metav1.Status{Message: err.Error()}}
}
