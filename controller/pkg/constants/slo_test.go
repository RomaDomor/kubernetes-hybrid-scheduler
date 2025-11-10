package constants

import "testing"

func TestGetTierForClass(t *testing.T) {
	tests := []struct {
		class string
		want  PriorityTier
	}{
		{"latency", TierHighest},
		{"interactive", TierHighest},
		{"streaming", TierNormal},
		{"throughput", TierNormal},
		{"batch", TierLowest},
		{"unknown-class", TierNormal}, // Test fallback to default
		{"", TierNormal},              // Test empty string fallback
	}

	for _, tt := range tests {
		t.Run(tt.class, func(t *testing.T) {
			if got := GetTierForClass(tt.class); got != tt.want {
				t.Errorf("GetTierForClass(%q) = %v, want %v", tt.class, got, tt.want)
			}
		})
	}
}
