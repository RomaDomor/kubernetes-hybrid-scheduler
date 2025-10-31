package webhook

import (
	"encoding/json"
	"fmt"
	"time"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/util"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
)

func (s *Server) buildPatchResponse(
	pod *corev1.Pod,
	res decision.Result,
) *admissionv1.AdmissionResponse {
	var patches []map[string]interface{}

	if pod.Annotations == nil {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/metadata/annotations",
			"value": map[string]string{},
		})
	}

	patches = append(patches,
		addAnnotation(constants.AnnotationDecision, string(res.Location)),
		addAnnotation(constants.AnnotationPredictedETA, fmt.Sprintf("%.0f", res.PredictedETAMs)),
		addAnnotation(constants.AnnotationTimestamp, time.Now().Format(time.RFC3339)),
		addAnnotation(constants.AnnotationReason, res.Reason),
		addAnnotation(constants.AnnotationWANRtt, fmt.Sprintf("%d", res.WanRttMs)),
	)

	if pod.Spec.NodeSelector == nil {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector",
			"value": map[string]string{},
		})
	}

	if res.Location == constants.Edge {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector/" + util.EscapeJSONPointer(constants.NodeRoleLabelEdge),
			"value": constants.LabelValueTrue,
		})

		// Assign appropriate PriorityClass based on decision reason
		priorityClass := s.getPriorityClassForReason(res.Reason)
		if priorityClass != "" {
			patches = append(patches, map[string]interface{}{
				"op":    "add",
				"path":  "/spec/priorityClassName",
				"value": priorityClass,
			})
		}
	} else {
		patches = append(patches,
			map[string]interface{}{
				"op":    "add",
				"path":  "/spec/nodeSelector/" + util.EscapeJSONPointer(constants.NodeRoleLabelCloud),
				"value": constants.LabelValueTrue,
			},
		)

		// Cloud workloads get lower priority
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/priorityClassName",
			"value": "cloud-workload",
		})
	}

	patchBytes, _ := json.Marshal(patches)
	pt := admissionv1.PatchTypeJSONPatch

	return &admissionv1.AdmissionResponse{
		Allowed:   true,
		Patch:     patchBytes,
		PatchType: &pt,
	}
}

// getPriorityClassForReason maps scheduling decision reasons to PriorityClasses
func (s *Server) getPriorityClassForReason(reason string) string {
	switch reason {
	// High priority: queued pods protecting reserved space
	case "edge_queue_preferred", "edge_queue_marginal":
		return "queued-edge-workload"

	// Medium priority: immediate edge scheduling
	case "edge_feasible_only", "edge_preferred":
		return "edge-preferred"

	// Low priority: best effort
	case "best_effort_edge":
		return "edge-best-effort"

	// No priority assignment for other reasons
	default:
		return ""
	}
}

func addAnnotation(key, val string) map[string]interface{} {
	return map[string]interface{}{
		"op":    "add",
		"path":  "/metadata/annotations/" + util.EscapeJSONPointer(key),
		"value": val,
	}
}

func (s *Server) BuildPatchResponseForTest(pod *corev1.Pod, res decision.Result) *admissionv1.AdmissionResponse {
	return s.buildPatchResponse(pod, res)
}
