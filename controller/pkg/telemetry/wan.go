package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/go-ping/ping"
	"k8s.io/klog/v2"
)

type WANProbe struct {
	cloudEndpoint string
	cache         *WANState
	cacheMu       sync.RWMutex
	cacheTTL      time.Duration
}

func NewWANProbe(endpoint string, ttl time.Duration) *WANProbe {
	p := &WANProbe{
		cloudEndpoint: endpoint,
		cacheTTL:      ttl,
		cache: &WANState{
			RTTMs:     999,
			LossPct:   100,
			Timestamp: time.Now(),
		},
	}
	// Single initial probe
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = p.refreshWANState(ctx)
	cancel()

	// Background refresh
	go p.startProbeLoop()
	return p
}

// Background loop that refreshes the WAN cache periodically
func (w *WANProbe) startProbeLoop() {
	// Small random initial delay helps avoid sync if multiple replicas run
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = w.refreshWANState(ctx)
		cancel()
	}
}

// refreshWANState performs ICMP pings using go-ping and updates the cache
func (w *WANProbe) refreshWANState(ctx context.Context) error {
	pinger, err := ping.NewPinger(w.cloudEndpoint)
	if err != nil {
		klog.Warningf("NewPinger failed: %v", err)
		return err
	}
	// If running without CAP_NET_RAW, unprivileged mode uses UDP fallback on many platforms.
	// SetPrivileged(true) if you add NET_RAW and want raw ICMP.
	pinger.SetPrivileged(false)
	pinger.Count = 3
	pinger.Timeout = 3 * time.Second
	pinger.Interval = 300 * time.Millisecond

	// Respect context cancellation (optional)
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			pinger.Stop()
		case <-done:
		}
	}()

	if err := pinger.Run(); err != nil {
		klog.Warningf("Ping run failed: %v", err)
		return err
	}
	close(done)

	stats := pinger.Statistics()
	// Average RTT in ms; packet loss in percent
	rtt := int(stats.AvgRtt.Milliseconds())
	loss := float64(stats.PacketLoss)

	w.cacheMu.Lock()
	w.cache = &WANState{
		RTTMs:     rtt,
		LossPct:   loss,
		Timestamp: time.Now(),
	}
	w.cacheMu.Unlock()

	return nil
}

func (w *WANProbe) GetWANState(ctx context.Context) (*WANState, error) {
	w.cacheMu.RLock()
	stale := time.Since(w.cache.Timestamp) > w.cacheTTL
	w.cacheMu.RUnlock()

	if stale {
		klog.Warning("WAN cache stale, using pessimistic defaults")
		return &WANState{RTTMs: 999, LossPct: 100}, nil
	}
	return w.cache, nil
}

// GetCachedWANState returns the last cached WAN state without any freshness check.
// Used as a fallback by the controller when active collection fails.
func (w *WANProbe) GetCachedWANState() *WANState {
	w.cacheMu.RLock()
	defer w.cacheMu.RUnlock()
	if w.cache == nil {
		return &WANState{RTTMs: 999, LossPct: 100}
	}
	return w.cache
}
