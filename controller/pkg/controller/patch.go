package controller

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
)

func (c *Controller) patchPod(pod *corev1.Pod, result decision.Result) error {
	var patchOps []interface{}

	// Add annotations
	if pod.Annotations == nil {
		patchOps = append(patchOps, map[string]interface{}{
			"op":    "add",
			"path":  "/metadata/annotations",
			"value": map[string]string{},
		})
	}

	patchOps = append(patchOps,
		map[string]interface{}{
			"op":    "add",
			"path":  "/metadata/annotations/" + escapeJSONPointer(DecisionAnnotation),
			"value": string(result.Location),
		},
		map[string]interface{}{
			"op":    "add",
			"path":  "/metadata/annotations/" + escapeJSONPointer(TimestampAnnotation),
			"value": time.Now().Format(time.RFC3339),
		},
		map[string]interface{}{
			"op":    "add",
			"path":  "/metadata/annotations/" + escapeJSONPointer(ReasonAnnotation),
			"value": result.Reason,
		},
	)

	// Add nodeSelector
	if pod.Spec.NodeSelector == nil {
		patchOps = append(patchOps, map[string]interface{}{
			"op":    "add",
			"path":  "/spec/nodeSelector",
			"value": map[string]string{},
		})
	}

	if result.Location == decision.Edge {
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
		toleration := corev1.Toleration{
			Key:      "virtual-node.liqo.io/not-allowed",
			Operator: corev1.TolerationOpEqual,
			Value:    "true",
			Effect:   corev1.TaintEffectNoSchedule,
		}

		if len(pod.Spec.Tolerations) == 0 {
			patchOps = append(patchOps, map[string]interface{}{
				"op":    "add",
				"path":  "/spec/tolerations",
				"value": []corev1.Toleration{toleration},
			})
		} else {
			patchOps = append(patchOps, map[string]interface{}{
				"op":    "add",
				"path":  "/spec/tolerations/-",
				"value": toleration,
			})
		}
	}

	patchBytes, err := json.Marshal(patchOps)
	if err != nil {
		return err
	}

	_, err = c.kubeclientset.CoreV1().Pods(pod.Namespace).Patch(
		context.TODO(),
		pod.Name,
		types.JSONPatchType,
		patchBytes,
		metav1.PatchOptions{},
	)

	return err
}

func escapeJSONPointer(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
}

func (c *Controller) recordEvent(pod *corev1.Pod, eventType, reason, message string) {
	_, err := c.kubeclientset.CoreV1().Events(pod.Namespace).Create(
		context.TODO(),
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: pod.Name + "-",
			},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: pod.Namespace,
				Name:      pod.Name,
				UID:       pod.UID,
			},
			Reason:  reason,
			Message: message,
			Type:    eventType,
			Source: corev1.EventSource{
				Component: "smart-scheduler",
			},
			FirstTimestamp: metav1.Now(),
			LastTimestamp:  metav1.Now(),
		},
		metav1.CreateOptions{},
	)
	if err != nil {
		klog.Warningf("Failed to record event: %v", err)
	}
}
