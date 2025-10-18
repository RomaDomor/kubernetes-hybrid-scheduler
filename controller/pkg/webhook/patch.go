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

	// Ensure annotations exist
	if pod.Annotations == nil {
		patches = append(patches, map[string]interface{}{
			"op":    "add",
			"path":  "/metadata/annotations",
			"value": map[string]string{},
		})
	}

	// Add decision annotations (include predicted ETA)
	patches = append(patches,
		addAnnotation("scheduling.hybrid.io/decision", string(res.Location)),
		addAnnotation("scheduling.hybrid.io/predictedETAMs", fmt.Sprintf("%.0f", res.PredictedETAMs)),
		addAnnotation("scheduling.hybrid.io/timestamp", time.Now().Format(time.RFC3339)),
		addAnnotation("scheduling.hybrid.io/reason", res.Reason),
		addAnnotation("scheduling.hybrid.io/wanRttMs", fmt.Sprintf("%d", res.WanRttMs)),
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
		"effect":   "NoExecute",
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
