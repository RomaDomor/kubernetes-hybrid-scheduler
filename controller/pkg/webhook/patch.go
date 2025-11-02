package webhook

import (
	"encoding/json"
	"fmt"
	"time"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/util"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
)

func (s *Server) buildPatchResponse(
	pod *corev1.Pod,
	res apis.Result,
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

	if pod.Spec.Priority != nil {
		patches = append(patches, map[string]interface{}{
			"op":   "remove",
			"path": "/spec/priority",
		})
	}

	if res.Location == constants.Edge {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector/" + util.EscapeJSONPointer(constants.NodeRoleLabelEdge),
			"value": constants.LabelValueTrue,
		})

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

func (s *Server) getPriorityClassForReason(reason string) string {
	switch reason {
	case "edge_queue_preferred", "edge_queue_marginal":
		return "queued-edge-workload"
	case "edge_feasible_only", "edge_preferred":
		return "edge-preferred"
	case "best_effort_edge":
		return "edge-best-effort"
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

func (s *Server) BuildPatchResponseForTest(pod *corev1.Pod, res apis.Result) *admissionv1.AdmissionResponse {
	return s.buildPatchResponse(pod, res)
}
