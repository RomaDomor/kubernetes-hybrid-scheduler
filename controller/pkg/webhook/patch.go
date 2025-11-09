package webhook

import (
	"encoding/json"
	"fmt"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
)

// buildPatchResponse constructs the JSON patch to mutate the pod spec based on the scheduling decision.
func (s *Server) buildPatchResponse(
	pod *corev1.Pod,
	res apis.Result,
) *admissionv1.AdmissionResponse {
	var patches []map[string]interface{}

	// Ensure annotations map exists
	if pod.Annotations == nil {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/metadata/annotations",
			"value": map[string]string{},
		})
	}

	// Add decision annotations
	patches = append(patches,
		addAnnotation(constants.AnnotationDecision, string(res.Location)),
		addAnnotation(constants.AnnotationPredictedETA, fmt.Sprintf("%.0f", res.PredictedETAMs)),
		addAnnotation(constants.AnnotationTimestamp, time.Now().Format(time.RFC3339)),
		addAnnotation(constants.AnnotationReason, res.Reason),
		addAnnotation(constants.AnnotationWANRtt, fmt.Sprintf("%d", res.WanRttMs)),
	)

	// Ensure nodeSelector map exists
	if pod.Spec.NodeSelector == nil {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector",
			"value": map[string]string{},
		})
	}

	// Remove existing priority to avoid conflicts
	if pod.Spec.Priority != nil {
		patches = append(patches, map[string]interface{}{
			"op":   "remove",
			"path": "/spec/priority",
		})
	}

	// Apply node selector and priority based on location
	if res.Location == constants.Edge {
		patches = append(patches, addNodeSelector(constants.NodeRoleLabelEdge, constants.LabelValueTrue))
		priorityClass := s.getPriorityClassForEdgeReason(res.Reason)
		if priorityClass != "" {
			patches = append(patches, addPriorityClass(priorityClass))
		}
	} else { // Cloud
		patches = append(patches,
			addNodeSelector(constants.NodeRoleLabelCloud, constants.LabelValueTrue),
			addPriorityClass(constants.PriorityClassCloud),
		)
	}

	patchBytes, err := json.Marshal(patches)
	if err != nil {
		// This should not happen with correctly formed patches
		klog.Errorf("Failed to marshal patches: %v", err)
		return deny(err)
	}
	patchType := admissionv1.PatchTypeJSONPatch

	return &admissionv1.AdmissionResponse{
		Allowed:   true,
		Patch:     patchBytes,
		PatchType: &patchType,
	}
}

// getPriorityClassForEdgeReason maps a decision reason to a Kubernetes PriorityClass name for edge workloads.
func (s *Server) getPriorityClassForEdgeReason(reason string) string {
	switch reason {
	// These reasons imply the pod may wait in a queue, so they get a specific priority.
	case "edge_queue_preferred", "edge_queue_marginal":
		return constants.PriorityClassQueuedEdge
	// These are high-priority placements on the edge.
	case constants.ReasonEdgeFeasibleOnly, constants.ReasonEdgePreferred:
		return constants.PriorityClassEdgePref
	// This is for workloads that are placed on the edge without strong guarantees.
	case constants.ReasonEdgeBestEffort:
		return constants.PriorityClassBestEffort
	default:
		// Default to no specific priority class if reason doesn't map.
		return ""
	}
}

// addAnnotation creates a JSON patch operation to add a metadata annotation.
func addAnnotation(key, val string) map[string]interface{} {
	return map[string]interface{}{
		"op":    "add",
		"path":  "/metadata/annotations/" + util.EscapeJSONPointer(key),
		"value": val,
	}
}

// addNodeSelector creates a JSON patch operation to add a node selector.
func addNodeSelector(key, val string) map[string]interface{} {
	return map[string]interface{}{
		"op":    "add",
		"path":  "/spec/nodeSelector/" + util.EscapeJSONPointer(key),
		"value": val,
	}
}

// addPriorityClass creates a JSON patch operation to set the priorityClassName.
func addPriorityClass(name string) map[string]interface{} {
	return map[string]interface{}{
		"op":    "add",
		"path":  "/spec/priorityClassName",
		"value": name,
	}
}

// BuildPatchResponseForTest is a public wrapper for testing purposes.
func (s *Server) BuildPatchResponseForTest(pod *corev1.Pod, res apis.Result) *admissionv1.AdmissionResponse {
	return s.buildPatchResponse(pod, res)
}
