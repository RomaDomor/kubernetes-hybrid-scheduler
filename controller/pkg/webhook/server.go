package webhook

import (
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
	if req == nil || req.Kind.Kind != "Pod" || req.Operation != admissionv1.Create {
		// Allow by default if not a Pod CREATE
		writeResponse(w, review, allow(nil))
		return
	}

	// Decode Pod
	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		writeResponse(w, review, deny(fmt.Errorf("cannot decode pod: %w", err)))
		return
	}

	// Only handle managed pods
	if pod.Labels["scheduling.example.io/managed"] != "true" {
		writeResponse(w, review, allow(nil))
		return
	}

	// If already decided or has nodeSelector, skip
	if pod.Annotations != nil && pod.Annotations["scheduling.example.io/decision"] != "" {
		writeResponse(w, review, allow(nil))
		return
	}

	// Gather cached telemetry (non-blocking)
	local := s.tel.GetCachedLocalState()
	wan := s.tel.GetCachedWANState()
	if local == nil {
		// Best effort: try once, but with short timeout; otherwise nil-safe defaults
		if ls, err := s.tel.GetLocalState(r.Context()); err == nil {
			local = ls
		}
	}
	if wan == nil {
		if ws, err := s.tel.GetWANState(r.Context()); err == nil {
			wan = ws
		}
	}
	if local == nil {
		local = &telemetry.LocalState{PendingPodsPerClass: map[string]int{}}
	}
	if wan == nil {
		wan = &telemetry.WANState{RTTMs: 999, LossPct: 100}
	}

	// Parse SLO
	sloData, err := slo.ParseSLO(&pod)
	if err != nil {
		// Deny with message? Safer: allow unchanged (fall back to default scheduler)
		klog.Warningf("Invalid SLO for %s/%s: %v", pod.Namespace, pod.Name, err)
		writeResponse(w, review, allow(nil))
		return
	}

	// Decide
	res := s.dec.Decide(&pod, sloData, local, wan)

	// Build JSON patch
	patchOps := make([]map[string]interface{}, 0)
	// Ensure annotations map
	if pod.Annotations == nil {
		patchOps = append(patchOps, map[string]interface{}{
			"op":    "add",
			"path":  "/metadata/annotations",
			"value": map[string]string{},
		})
	}
	patchOps = append(patchOps,
		addAnno("scheduling.example.io/decision", string(res.Location)),
		addAnno("scheduling.example.io/timestamp", time.Now().Format(time.RFC3339)),
		addAnno("scheduling.example.io/reason", res.Reason),
	)

	// nodeSelector
	if pod.Spec.NodeSelector == nil {
		patchOps = append(patchOps, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector",
			"value": map[string]string{},
		})
	}
	if res.Location == decision.Edge {
		patchOps = append(patchOps, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector/node.role~1edge",
			"value": "true",
		})
	} else {
		patchOps = append(patchOps, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector/node.role~1cloud",
			"value": "true",
		})
		// Add toleration for cloud taint
		tol := map[string]string{
			"key":      "virtual-node.liqo.io/not-allowed",
			"operator": "Equal",
			"value":    "true",
			"effect":   "NoSchedule",
		}

		if len(pod.Spec.Tolerations) == 0 {
			patchOps = append(patchOps, map[string]interface{}{
				"op":    "add",
				"path":  "/spec/tolerations",
				"value": []map[string]string{tol},
			})
		} else {
			patchOps = append(patchOps, map[string]interface{}{
				"op":    "add",
				"path":  "/spec/tolerations/-",
				"value": tol,
			})
		}
	}

	patchBytes, _ := json.Marshal(patchOps)
	writeResponse(w, review, &admissionv1.AdmissionResponse{
		Allowed:   true,
		UID:       req.UID,
		Patch:     patchBytes,
		PatchType: func() *admissionv1.PatchType { t := admissionv1.PatchTypeJSONPatch; return &t }(),
	})
}

func addAnno(key, val string) map[string]interface{} {
	// escape / as ~1
	esc := func(s string) string {
		s = strings.ReplaceAll(s, "~", "~0")
		s = strings.ReplaceAll(s, "/", "~1")
		return s
	}
	return map[string]interface{}{
		"op":    "add",
		"path":  "/metadata/annotations/" + esc(key),
		"value": val,
	}
}

func writeResponse(w http.ResponseWriter, in admissionv1.AdmissionReview, resp *admissionv1.AdmissionResponse) {
	out := admissionv1.AdmissionReview{
		TypeMeta: in.TypeMeta,
		Response: resp,
	}
	out.Response.UID = in.Request.UID
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func allow(patch []byte) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{Allowed: true}
}

func deny(err error) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result:  &metav1.Status{Message: err.Error()},
	}
}
