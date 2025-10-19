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
		Priority:       5,
		OffloadAllowed: true,
	}

	if v, ok := annotations["slo.hybrid.io/deadlineMs"]; ok {
		val, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid deadlineMs: %v", err)
		}
		if val < 1 || val > 3600000 { // 1ms to 1 hour
			return nil, fmt.Errorf("deadlineMs out of range [1-3600000]: %d", val)
		}
		slo.DeadlineMs = val
	}

	if v, ok := annotations["slo.hybrid.io/latencyTargetMs"]; ok {
		val, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid latencyTargetMs: %v", err)
		}
		if val < 1 || val > 3600000 {
			return nil, fmt.Errorf("latencyTargetMs out of range [1-3600000]: %d", val)
		}
		slo.LatencyTargetMs = val
	}

	if slo.DeadlineMs == 0 && slo.LatencyTargetMs == 0 {
		return nil, fmt.Errorf("at least one of deadlineMs or latencyTargetMs must be set")
	}

	if slo.DeadlineMs == 0 {
		slo.DeadlineMs = slo.LatencyTargetMs
	}

	slo.Class = annotations["slo.hybrid.io/class"]
	if slo.Class == "" {
		return nil, fmt.Errorf("slo.hybrid.io/class annotation required")
	}

	// Validate class (whitelist)
	validClasses := map[string]bool{
		"latency":     true,
		"throughput":  true,
		"batch":       true,
		"interactive": true,
		"streaming":   true,
	}
	if !validClasses[slo.Class] {
		return nil, fmt.Errorf("invalid class '%s', must be one of: latency, throughput, batch, interactive, streaming", slo.Class)
	}

	if v, ok := annotations["slo.hybrid.io/priority"]; ok {
		val, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid priority: %v", err)
		}
		if val < 0 || val > 10 {
			return nil, fmt.Errorf("priority out of range [0-10]: %d", val)
		}
		slo.Priority = val
	}

	if v, ok := annotations["slo.hybrid.io/offloadAllowed"]; ok {
		slo.OffloadAllowed = v != "false"
	}

	return slo, nil
}
