package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

type Server struct {
	dec *decision.Engine
	tel telemetry.Collector
}

func NewServer(dec *decision.Engine, tel telemetry.Collector) *Server {
	return &Server{dec: dec, tel: tel}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	klog.V(4).Infof("Webhook request from %s", r.RemoteAddr)

	if r.Method != http.MethodPost {
		klog.Warning("Received non-POST request")
		http.Error(w, "invalid method", http.StatusMethodNotAllowed)
		return
	}

	var review admissionv1.AdmissionReview
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		klog.Errorf("Failed to decode admission review: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	req := review.Request
	if req == nil {
		klog.Error("Admission request is nil")
		writeResponse(w, review, deny(fmt.Errorf("nil request")))
		return
	}

	klog.V(3).Infof(
		"Processing %s %s/%s (UID=%s)",
		req.Operation, req.Kind.Kind, req.Name, req.UID)

	// Only handle Pod CREATE operations
	if req.Kind.Kind != "Pod" || req.Operation != admissionv1.Create {
		writeResponse(w, review, allow())
		return
	}

	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		klog.Errorf("Failed to decode pod: %v", err)
		writeResponse(w, review, deny(err))
		return
	}

	// Only process managed pods
	if pod.Labels["scheduling.hybrid.io/managed"] != "true" {
		klog.V(4).Infof("Pod %s/%s not managed, skipping",
			pod.Namespace, pod.Name)
		writeResponse(w, review, allow())
		return
	}

	// Skip if already decided
	if pod.Annotations != nil &&
		pod.Annotations["scheduling.hybrid.io/decision"] != "" {
		klog.V(4).Infof("Pod %s/%s already decided, skipping",
			pod.Namespace, pod.Name)
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
	// Parse SLO
	sloData, err := slo.ParseSLO(pod)
	if err != nil {
		klog.Warningf("Invalid SLO for %s/%s: %v",
			pod.Namespace, pod.Name, err)
		return allow() // Fail open on invalid SLO
	}

	// Get telemetry (with fallback to cached)
	local := s.tel.GetCachedLocalState()
	wan := s.tel.GetCachedWANState()

	// Try fresh data with timeout
	if ls, err := s.tel.GetLocalState(ctx); err == nil {
		local = ls
	}
	if ws, err := s.tel.GetWANState(ctx); err == nil {
		wan = ws
	}

	// Decide
	result := s.dec.Decide(pod, sloData, local, wan)
	klog.Infof("Decision for %s/%s: %s (reason=%s, rtt=%dms)",
		pod.Namespace, pod.Name, result.Location, result.Reason, wan.RTTMs)

	// Build patch
	return s.buildPatchResponse(pod, result)
}

func writeResponse(
	w http.ResponseWriter,
	in admissionv1.AdmissionReview,
	resp *admissionv1.AdmissionResponse,
) {
	resp.UID = in.Request.UID
	out := admissionv1.AdmissionReview{
		TypeMeta: in.TypeMeta,
		Response: resp,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func allow() *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{Allowed: true}
}

func deny(err error) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result:  &metav1.Status{Message: err.Error()},
	}
}
