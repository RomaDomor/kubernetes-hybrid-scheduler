package telemetry

import (
	"context"
	"os/exec"
	"regexp"
	"strconv"
	"time"

	"k8s.io/klog/v2"
)

type WANProbe struct {
	cloudEndpoint string
	cache         *WANState
	cacheTTL      time.Duration
}

func NewWANProbe(endpoint string, ttl time.Duration) *WANProbe {
	return &WANProbe{
		cloudEndpoint: endpoint,
		cacheTTL:      ttl,
		cache: &WANState{
			RTTMs:   999, // pessimistic default
			LossPct: 100,
		},
	}
}

func (w *WANProbe) GetWANState(ctx context.Context) (*WANState, error) {
	// Run ping
	cmd := exec.CommandContext(ctx, "ping", "-c", "3", "-W", "2", w.cloudEndpoint)
	output, err := cmd.CombinedOutput()
	if err != nil {
		klog.Warningf("Ping failed: %v", err)
		return w.cache, err
	}

	// Parse output (Linux format)
	rtt, loss := parsePingOutput(string(output))

	state := &WANState{
		RTTMs:     rtt,
		LossPct:   loss,
		Timestamp: time.Now(),
	}

	w.cache = state
	return state, nil
}

func (w *WANProbe) GetCachedWANState() *WANState {
	if time.Since(w.cache.Timestamp) > w.cacheTTL {
		klog.Warning("WAN cache stale, using pessimistic defaults")
		return &WANState{RTTMs: 999, LossPct: 100}
	}
	return w.cache
}

func parsePingOutput(output string) (rtt int, loss float64) {
	// Example: "rtt min/avg/max/mdev = 12.345/23.456/34.567/5.678 ms"
	rttRegex := regexp.MustCompile(`rtt min/avg/max/mdev = [\d.]+/([\d.]+)/`)
	if match := rttRegex.FindStringSubmatch(output); len(match) > 1 {
		if val, err := strconv.ParseFloat(match[1], 64); err == nil {
			rtt = int(val)
		}
	}

	// Example: "3 packets transmitted, 2 received, 33% packet loss"
	lossRegex := regexp.MustCompile(`([\d.]+)% packet loss`)
	if match := lossRegex.FindStringSubmatch(output); len(match) > 1 {
		loss, _ = strconv.ParseFloat(match[1], 64)
	}

	if rtt == 0 {
		rtt = 999 // fallback
	}

	return rtt, loss
}
