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

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

type Server struct {
	dec        *decision.Engine
	tel        telemetry.Collector
	limiter    *rate.Limiter
	kubeClient kubernetes.Interface
}

func NewServer(
	dec *decision.Engine,
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
	defer func() {
		admissionLatency.Observe(time.Since(start).Seconds())
	}()

	// Rate limiting
	if !s.limiter.Allow() {
		klog.Warning("Admission rate limit exceeded")
		admissionRateLimited.Inc()
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}

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

	if pod.Labels["scheduling.hybrid.io/managed"] != "true" {
		klog.V(4).Infof("Pod %s/%s not managed, skipping",
			pod.Namespace, pod.Name)
		writeResponse(w, review, allow())
		return
	}

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
		return allow()
	}

	local := s.tel.GetCachedLocalState()
	wan := s.tel.GetCachedWANState()

	// Decide
	result := s.dec.Decide(pod, sloData, local, wan)

	// Record event
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
		Reason: "SchedulingDecision",
		Message: fmt.Sprintf("Scheduled to %s: %s (ETA: %.0fms, WAN RTT: %dms)",
			result.Location, result.Reason, result.PredictedETAMs, result.WanRttMs),
		Type:           corev1.EventTypeNormal,
		FirstTimestamp: metav1.NewTime(time.Now()),
		LastTimestamp:  metav1.NewTime(time.Now()),
		Count:          1,
	}, metav1.CreateOptions{})
	if err != nil {
		klog.Warningf("Failed to create event for pod %s", pod.Name)
	}

	klog.Infof("Decision for %s: %s (reason=%s, rtt=%dms)",
		decision.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID)),
		result.Location, result.Reason, wan.RTTMs)

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
