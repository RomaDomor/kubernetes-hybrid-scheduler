package slo

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

func TestParseSLO_Success_MinimalWithDeadline(t *testing.T) {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				constants.AnnotationSLODeadline: "2000",
				constants.AnnotationSLOClass:    "latency",
			},
		},
	}
	got, err := ParseSLO(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.DeadlineMs != 2000 {
		t.Errorf("DeadlineMs = %d, want 2000", got.DeadlineMs)
	}
	if got.Priority != 5 || !got.OffloadAllowed {
		t.Errorf("defaults not applied: prio=%d offload=%v", got.Priority, got.OffloadAllowed)
	}
}

func TestParseSLO_UsesLatencyTargetAsDeadline(t *testing.T) {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				constants.AnnotationSLOLatencyTarget: "1500",
				constants.AnnotationSLOClass:         "throughput",
			},
		},
	}
	got, err := ParseSLO(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.DeadlineMs != 1500 {
		t.Errorf("DeadlineMs = %d, want 1500", got.DeadlineMs)
	}
}

func TestParseSLO_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		ann     map[string]string
		wantErr string
	}{
		{
			name:    "no annotations",
			ann:     nil,
			wantErr: "pod has no annotations",
		},
		{
			name: "missing class",
			ann: map[string]string{
				constants.AnnotationSLODeadline: "100",
			},
			wantErr: "class",
		},
		{
			name: "invalid class",
			ann: map[string]string{
				constants.AnnotationSLODeadline: "100",
				constants.AnnotationSLOClass:    "weird",
			},
			wantErr: "invalid class",
		},
		{
			name: "deadline out of range",
			ann: map[string]string{
				constants.AnnotationSLODeadline: "0",
				constants.AnnotationSLOClass:    "batch",
			},
			wantErr: "out of range",
		},
		{
			name: "priority out of range",
			ann: map[string]string{
				constants.AnnotationSLODeadline: "100",
				constants.AnnotationSLOClass:    "batch",
				constants.AnnotationSLOPriority: "11",
			},
			wantErr: "priority out of range",
		},
		{
			name: "need at least one time metric",
			ann: map[string]string{
				constants.AnnotationSLOClass: "latency",
			},
			wantErr: "at least one of deadlineMs or latencyTargetMs",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: tt.ann}}
			_, err := ParseSLO(p)
			if err == nil || (err != nil && !contains(err.Error(), tt.wantErr)) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || (len(sub) > 0 && (contains(s[1:], sub) || (len(s) >= len(sub) && s[:len(sub)] == sub))))
}
