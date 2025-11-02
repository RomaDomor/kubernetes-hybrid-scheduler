package telemetry_test

import (
	"testing"
	"time"

	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

func TestWANProbe_CacheStaleness(t *testing.T) {
	// Use a short TTL for testing
	p := telemetry.NewWANProbe("127.0.0.1", 50*time.Millisecond)

	// Give it a moment to initialize
	time.Sleep(100 * time.Millisecond)

	s := p.GetCachedWANState()
	if s == nil {
		t.Fatal("expected non-nil WAN state")
	}

	// Initial state might be stale due to ping failures to localhost
	t.Logf("Initial state: RTT=%dms, Loss=%.1f%%, IsStale=%v", s.RTTMs, s.LossPct, s.IsStale)

	// Wait for staleness to exceed TTL
	time.Sleep(150 * time.Millisecond)

	s2 := p.GetCachedWANState()
	if !s2.IsStale {
		t.Errorf("expected IsStale=true after TTL expired, got false (staleDuration=%v)", s2.StaleDuration)
	}

	if s2.StaleDuration < 50*time.Millisecond {
		t.Errorf("expected staleDuration >= 50ms, got %v", s2.StaleDuration)
	}
}

func TestWANProbe_DefaultValues(t *testing.T) {
	p := telemetry.NewWANProbe("192.0.2.1", 1*time.Second)
	time.Sleep(100 * time.Millisecond)

	s := p.GetCachedWANState()
	if s == nil {
		t.Fatal("expected non-nil state")
	}

	if !(s.IsStale || s.LossPct >= 50.0 || s.RTTMs >= 0) {
		t.Errorf("unexpected WAN state for unreachable endpoint: %+v", s)
	}

	t.Logf("Unreachable endpoint state: RTT=%dms, Loss=%.1f%%, Stale=%v", s.RTTMs, s.LossPct, s.IsStale)
}
