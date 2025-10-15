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
	p := &WANProbe{
		cloudEndpoint: endpoint,
		cacheTTL:      ttl,
		cache: &WANState{
			RTTMs:   999, // pessimistic default
			LossPct: 100,
		},
	}
	go p.startProbeLoop()
	return p
}

// Background loop that refreshes the WAN cache periodically
func (w *WANProbe) startProbeLoop() {
	// Small random initial delay helps avoid sync if multiple replicas run
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		_ = w.refreshWANState(ctx)
		cancel()
	}
}

// refreshWANState performs the ping once and updates the cache
func (w *WANProbe) refreshWANState(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "ping", "-c", "3", "-W", "2", w.cloudEndpoint)
	output, err := cmd.CombinedOutput()
	if err != nil {
		klog.Warningf("Ping failed: %v", err)
		return err
	}
	rtt, loss := parsePingOutput(string(output))
	w.cache = &WANState{
		RTTMs:     rtt,
		LossPct:   loss,
		Timestamp: time.Now(),
	}
	return nil
}

func (w *WANProbe) GetWANState(ctx context.Context) (*WANState, error) {
	if time.Since(w.cache.Timestamp) > w.cacheTTL {
		klog.Warning("WAN cache stale, using pessimistic defaults")
		return &WANState{RTTMs: 999, LossPct: 100}, nil
	}
	return w.cache, nil
}

// GetCachedWANState returns the last cached WAN state without any freshness check.
// Used as a fallback by the controller when active collection fails.
func (w *WANProbe) GetCachedWANState() *WANState {
	if w.cache == nil {
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
