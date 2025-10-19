package webhook

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
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
		addAnnotation("scheduling.hybrid.io/decision", string(res.Location)),
		addAnnotation("scheduling.hybrid.io/predictedETAMs", fmt.Sprintf("%.0f", res.PredictedETAMs)),
		addAnnotation("scheduling.hybrid.io/timestamp", time.Now().Format(time.RFC3339)),
		addAnnotation("scheduling.hybrid.io/reason", res.Reason),
		addAnnotation("scheduling.hybrid.io/wanRttMs", fmt.Sprintf("%d", res.WanRttMs)),
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
		)

		patches = append(patches, s.addCloudTolerationSafe(pod))
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

func (s *Server) addCloudTolerationSafe(pod *corev1.Pod) map[string]interface{} {
	targetTol := corev1.Toleration{
		Key:      "virtual-node.liqo.io/not-allowed",
		Operator: corev1.TolerationOpEqual,
		Value:    "true",
		Effect:   corev1.TaintEffectNoExecute,
	}

	// Check if toleration already exists
	for _, tol := range pod.Spec.Tolerations {
		if tol.Key == targetTol.Key &&
			tol.Operator == targetTol.Operator &&
			tol.Value == targetTol.Value &&
			tol.Effect == targetTol.Effect {
			// Already exists, return no-op
			return map[string]interface{}{
				"op":   "test",
				"path": "/spec/tolerations",
			}
		}
	}

	// Convert to map for JSON patch
	tolMap := map[string]string{
		"key":      targetTol.Key,
		"operator": string(targetTol.Operator),
		"value":    targetTol.Value,
		"effect":   string(targetTol.Effect),
	}

	if len(pod.Spec.Tolerations) == 0 {
		// First toleration: replace entire array
		return map[string]interface{}{
			"op":    "add",
			"path":  "/spec/tolerations",
			"value": []map[string]string{tolMap},
		}
	}

	// Append to existing
	return map[string]interface{}{
		"op":    "add",
		"path":  "/spec/tolerations/-",
		"value": tolMap,
	}
}

func escapeJSONPointer(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
}

// Test Helpers
func (s *Server) BuildPatchResponseForTest(pod *corev1.Pod, res decision.Result) *admissionv1.AdmissionResponse {
	return s.buildPatchResponse(pod, res)
}
