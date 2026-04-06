package webhook

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

func TestBuildPatchResponse_Local(t *testing.T) {
	s := NewServer(nil, nil, nil, fake.NewClientset())
	pod := &corev1.Pod{}
	res := apis.Result{Location: constants.LocalCluster, PredictedETAMs: 123, WanRttMs: 0, Reason: constants.ReasonLocalPreferred}

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
		t.Fatalf("local nodeSelector not added")
	}
}

func TestBuildPatchResponse_Remote(t *testing.T) {
	s := NewServer(nil, nil, nil, fake.NewClientset())
	pod := &corev1.Pod{}
	res := apis.Result{Location: "cloud-1", PredictedETAMs: 200, WanRttMs: 50, Reason: constants.ReasonRemoteFaster}

	resp := s.BuildPatchResponseForTest(pod, res)
	if resp.Patch == nil {
		t.Fatalf("patch nil")
	}
	var ops []map[string]interface{}
	_ = json.Unmarshal(resp.Patch, &ops)

	foundClusterSel := false
	foundPriority := false
	for _, op := range ops {
		if op["path"] == "/spec/nodeSelector/node.cluster~1id" && op["value"] == "cloud-1" {
			foundClusterSel = true
		}
		if op["path"] == "/spec/priorityClassName" && op["value"] == constants.PriorityClassRemote {
			foundPriority = true
		}
	}
	if !foundClusterSel {
		t.Fatalf("remote cluster nodeSelector not added")
	}
	if !foundPriority {
		t.Fatalf("remote priority class not added")
	}
}

func TestGetPriorityClassForLocalReason(t *testing.T) {
	s := NewServer(nil, nil, nil, fake.NewClientset())
	pod := &corev1.Pod{}

	testCases := []struct {
		reason   string
		expected string
	}{
		{constants.ReasonLocalPreferred, constants.PriorityClassLocalPref},
		{constants.ReasonLocalFeasibleOnly, constants.PriorityClassLocalPref},
		{"local_queue_preferred", constants.PriorityClassQueuedLocal},
		{"local_queue_marginal", constants.PriorityClassQueuedLocal},
		{constants.ReasonLocalBestEffort, constants.PriorityClassBestEffort},
		{"unknown_reason", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.reason, func(t *testing.T) {
			res := apis.Result{Location: constants.LocalCluster, Reason: tc.reason}
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
