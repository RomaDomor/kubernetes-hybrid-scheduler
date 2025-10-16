package slo

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

type SLO struct {
	DeadlineMs      int
	LatencyTargetMs int
	Class           string
	Priority        int
	OffloadAllowed  bool
}

func ParseSLO(pod *corev1.Pod) (*SLO, error) {
	annotations := pod.Annotations
	if annotations == nil {
		return nil, fmt.Errorf("pod has no annotations")
	}

	slo := &SLO{
		Priority:       5, // default
		OffloadAllowed: true,
	}

	if v, ok := annotations["slo.hybrid.io/deadlineMs"]; ok {
		if val, err := strconv.Atoi(v); err == nil {
			slo.DeadlineMs = val
		}
	}

	if v, ok := annotations["slo.hybrid.io/latencyTargetMs"]; ok {
		if val, err := strconv.Atoi(v); err == nil {
			slo.LatencyTargetMs = val
		}
	}

	if slo.DeadlineMs == 0 && slo.LatencyTargetMs == 0 {
		return nil, fmt.Errorf("at least one of deadlineMs or latencyTargetMs must be set")
	}

	// Use latencyTarget as deadline if deadline not set
	if slo.DeadlineMs == 0 {
		slo.DeadlineMs = slo.LatencyTargetMs
	}

	slo.Class = annotations["slo.hybrid.io/class"]
	if slo.Class == "" {
		return nil, fmt.Errorf("slo.hybrid.io/class annotation required")
	}

	if v, ok := annotations["slo.hybrid.io/priority"]; ok {
		if val, err := strconv.Atoi(v); err == nil {
			slo.Priority = val
		}
	}

	if v, ok := annotations["slo.hybrid.io/offloadAllowed"]; ok {
		slo.OffloadAllowed = v != "false"
	}

	return slo, nil
}
