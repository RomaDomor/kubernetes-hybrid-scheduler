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

	// Route to the correct cluster via nodeSelector
	if constants.IsLocal(res.Location) {
		// Local cluster: select edge nodes
		patches = append(patches,
			addNodeSelector(constants.NodeRoleLabelEdge, constants.LabelValueTrue),
		)
		priorityClass := s.getPriorityClassForLocalReason(res.Reason)
		if priorityClass != "" {
			patches = append(patches, addPriorityClass(priorityClass))
		}
	} else {
		// Remote cluster: select the specific virtual node by cluster ID label
		patches = append(patches,
			addNodeSelector(constants.NodeClusterLabel, string(res.Location)),
			addPriorityClass(constants.PriorityClassRemote),
		)
	}

	patchBytes, err := json.Marshal(patches)
	if err != nil {
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

func (s *Server) getPriorityClassForLocalReason(reason string) string {
	switch reason {
	case "local_queue_preferred", "local_queue_marginal":
		return constants.PriorityClassQueuedLocal
	case constants.ReasonLocalFeasibleOnly, constants.ReasonLocalPreferred:
		return constants.PriorityClassLocalPref
	case constants.ReasonLocalBestEffort:
		return constants.PriorityClassBestEffort
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

func addNodeSelector(key, val string) map[string]interface{} {
	return map[string]interface{}{
		"op":    "add",
		"path":  "/spec/nodeSelector/" + util.EscapeJSONPointer(key),
		"value": val,
	}
}

func addPriorityClass(name string) map[string]interface{} {
	return map[string]interface{}{
		"op":    "add",
		"path":  "/spec/priorityClassName",
		"value": name,
	}
}

func (s *Server) BuildPatchResponseForTest(pod *corev1.Pod, res apis.Result) *admissionv1.AdmissionResponse {
	return s.buildPatchResponse(pod, res)
}
