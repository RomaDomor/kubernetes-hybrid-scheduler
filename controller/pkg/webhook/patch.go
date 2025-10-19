package webhook

import (
	"encoding/json"
	"fmt"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
)

func (s *Server) buildPatchResponse(
	pod *corev1.Pod,
	res decision.Result,
) *admissionv1.AdmissionResponse {
	patches := []map[string]interface{}{}

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

	if res.Location == decision.Edge {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector/" + util.EscapeJSONPointer(constants.NodeRoleLabelEdge),
			"value": constants.LabelValueTrue,
		})
	} else {
		patches = append(patches,
			map[string]interface{}{
				"op":    "add",
				"path":  "/spec/nodeSelector/" + util.EscapeJSONPointer(constants.NodeRoleLabelCloud),
				"value": constants.LabelValueTrue,
			},
		)

		//patches = append(patches, s.addCloudTolerationSafe(pod))
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
		"path":  "/metadata/annotations/" + util.EscapeJSONPointer(key),
		"value": val,
	}
}

//func (s *Server) addCloudTolerationSafe(pod *corev1.Pod) map[string]interface{} {
//	targetTol := corev1.Toleration{
//		Key:      "virtual-node.liqo.io/not-allowed",
//		Operator: corev1.TolerationOpEqual,
//		Value:    "true",
//		Effect:   corev1.TaintEffectNoExecute,
//	}
//
//	// Check if toleration already exists
//	for _, tol := range pod.Spec.Tolerations {
//		if tol.Key == targetTol.Key &&
//			tol.Operator == targetTol.Operator &&
//			tol.Value == targetTol.Value &&
//			tol.Effect == targetTol.Effect {
//			// Already exists, return no-op
//			return map[string]interface{}{
//				"op":   "test",
//				"path": "/spec/tolerations",
//			}
//		}
//	}
//
//	// Convert to map for JSON patch
//	tolMap := map[string]string{
//		"key":      targetTol.Key,
//		"operator": string(targetTol.Operator),
//		"value":    targetTol.Value,
//		"effect":   string(targetTol.Effect),
//	}
//
//	if len(pod.Spec.Tolerations) == 0 {
//		// First toleration: replace entire array
//		return map[string]interface{}{
//			"op":    "add",
//			"path":  "/spec/tolerations",
//			"value": []map[string]string{tolMap},
//		}
//	}
//
//	// Append to existing
//	return map[string]interface{}{
//		"op":    "add",
//		"path":  "/spec/tolerations/-",
//		"value": tolMap,
//	}
//}

// Test Helpers
func (s *Server) BuildPatchResponseForTest(pod *corev1.Pod, res decision.Result) *admissionv1.AdmissionResponse {
	return s.buildPatchResponse(pod, res)
}
