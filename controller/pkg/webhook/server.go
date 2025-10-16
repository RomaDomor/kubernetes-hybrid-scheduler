package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

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

func (s *Server) buildPatchResponse(
	pod *corev1.Pod,
	res decision.Result,
) *admissionv1.AdmissionResponse {
	patches := []map[string]interface{}{}

	// Ensure annotations exist
	if pod.Annotations == nil {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/metadata/annotations",
			"value": map[string]string{},
		})
	}

	// Add decision annotations
	patches = append(patches,
		addAnnotation("scheduling.hybrid.io/decision",
			string(res.Location)),
		addAnnotation("scheduling.hybrid.io/timestamp",
			time.Now().Format(time.RFC3339)),
		addAnnotation("scheduling.hybrid.io/reason", res.Reason),
	)

	// Ensure nodeSelector exists
	if pod.Spec.NodeSelector == nil {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector",
			"value": map[string]string{},
		})
	}

	// Add location-specific patches
	if res.Location == decision.Edge {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector/node.role~1edge",
			"value": "true",
		})
	} else {
		patches = append(patches,
			map[string]interface{}{
				"op":    "add",
				"path":  "/spec/nodeSelector/node.role~1cloud",
				"value": "true",
			},
			addCloudToleration(len(pod.Spec.Tolerations) == 0),
		)
	}

	patchBytes, _ := json.Marshal(patches)
	pt := admissionv1.PatchTypeJSONPatch

	return &admissionv1.AdmissionResponse{
		Allowed:   true,
		Patch:     patchBytes,
		PatchType: &pt,
	}
}

func addAnnotation(key, val string) map[string]interface{} {
	return map[string]interface{}{
		"op":    "add",
		"path":  "/metadata/annotations/" + escapeJSONPointer(key),
		"value": val,
	}
}

func addCloudToleration(isFirst bool) map[string]interface{} {
	tol := map[string]string{
		"key":      "virtual-node.liqo.io/not-allowed",
		"operator": "Equal",
		"value":    "true",
		"effect":   "NoSchedule",
	}

	path := "/spec/tolerations"
	if !isFirst {
		path += "/-"
	}

	return map[string]interface{}{
		"op":   "add",
		"path": path,
		"value": func() interface{} {
			if isFirst {
				return []map[string]string{tol}
			}
			return tol
		}(),
	}
}

func escapeJSONPointer(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
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
