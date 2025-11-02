package webhook_test

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/webhook"
)

func TestBuildPatchResponse_Edge(t *testing.T) {
	s := webhook.NewServer(nil, nil, nil, fake.NewSimpleClientset())
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
