package webhook

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

func TestBuildPatchResponse_Edge(t *testing.T) {
	s := NewServer(nil, nil, nil, fake.NewSimpleClientset())
	pod := &corev1.Pod{}
	res := apis.Result{Location: constants.Edge, PredictedETAMs: 123, WanRttMs: 10, Reason: "edge_preferred"}

	resp := s.BuildPatchResponseForTest(pod, res)
	if resp.Patch == nil {
		t.Fatalf("patch nil")
	}
	var ops []map[string]interface{}
	_ = json.Unmarshal(resp.Patch, &ops)
	foundNodeSel := false
	for _, op := range ops {
		if op["path"] == "/spec/nodeSelector/node.role~1edge" {
			foundNodeSel = true
		}
	}
	if !foundNodeSel {
		t.Fatalf("edge nodeSelector not added")
	}
}

func TestGetPriorityClassForEdgeReason(t *testing.T) {
	s := NewServer(nil, nil, nil, fake.NewSimpleClientset())
	pod := &corev1.Pod{}

	testCases := []struct {
		reason   string
		expected string
	}{
		{constants.ReasonEdgePreferred, constants.PriorityClassEdgePref},
		{constants.ReasonEdgeFeasibleOnly, constants.PriorityClassEdgePref},
		{"edge_queue_preferred", constants.PriorityClassQueuedEdge},
		{"edge_queue_marginal", constants.PriorityClassQueuedEdge},
		{constants.ReasonEdgeBestEffort, constants.PriorityClassBestEffort},
		{"unknown_reason", ""}, // Default case
	}

	for _, tc := range testCases {
		t.Run(tc.reason, func(t *testing.T) {
			res := apis.Result{Location: constants.Edge, Reason: tc.reason}
			resp := s.BuildPatchResponseForTest(pod, res)

			var ops []map[string]interface{}
			_ = json.Unmarshal(resp.Patch, &ops)

			foundPriority := false
			for _, op := range ops {
				if op["path"] == "/spec/priorityClassName" {
					foundPriority = true
					if val, ok := op["value"].(string); !ok || val != tc.expected {
						t.Errorf("Expected priority class '%s', but got '%s'", tc.expected, val)
					}
				}
			}

			if tc.expected != "" && !foundPriority {
				t.Errorf("Expected to find a patch for priorityClassName, but none was found")
			}
			if tc.expected == "" && foundPriority {
				t.Errorf("Expected no priority class patch, but one was found")
			}
		})
	}
}
