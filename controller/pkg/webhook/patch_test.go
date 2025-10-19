package webhook_test

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/webhook"
)

func TestBuildPatchResponse_Edge(t *testing.T) {
	s := webhook.NewServer(nil, nil, nil, fake.NewSimpleClientset())
	pod := &corev1.Pod{}
	res := decision.Result{Location: decision.Edge, PredictedETAMs: 123, WanRttMs: 10, Reason: "edge_preferred"}

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

//func TestBuildPatchResponse_CloudAddsToleration(t *testing.T) {
//	s := webhook.NewServer(nil, nil, nil, fake.NewSimpleClientset())
//	pod := &corev1.Pod{}
//	res := decision.Result{Location: decision.Cloud, PredictedETAMs: 50, WanRttMs: 5, Reason: "cloud_faster"}
//
//	resp := s.BuildPatchResponseForTest(pod, res)
//	var ops []map[string]interface{}
//	_ = json.Unmarshal(resp.Patch, &ops)
//
//	hasCloudSelector := false
//	hasTol := false
//	for _, op := range ops {
//		if op["path"] == "/spec/nodeSelector/node.role~1cloud" {
//			hasCloudSelector = true
//		}
//		if op["path"] == "/spec/tolerations" || op["path"] == "/spec/tolerations/-" {
//			hasTol = true
//		}
//	}
//	if !hasCloudSelector || !hasTol {
//		t.Fatalf("cloud selector or toleration missing: sel=%v tol=%v", hasCloudSelector, hasTol)
//	}
//}
