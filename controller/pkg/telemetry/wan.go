package telemetry

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/go-ping/ping"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"k8s.io/klog/v2"
)

var (
	wanRTTGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_wan_rtt_milliseconds",
		Help: "WAN round-trip time in milliseconds",
	})
	wanLossGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_wan_packet_loss_percent",
		Help: "WAN packet loss percentage",
	})
	wanStaleGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_wan_stale_seconds",
		Help: "Seconds since last successful WAN probe",
	})
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
			IsStale:   true,
		},
	}

	// Initial probe
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = p.refreshWANState(ctx)
	cancel()

	go p.startProbeLoopWithJitter()
	return p
}

func (p *WANProbe) startProbeLoopWithJitter() {
	// Initial jitter: 0-5 seconds
	time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)

	// 10s base interval with ±2s jitter
	for {
		jitter := time.Duration(rand.Intn(4000)-2000) * time.Millisecond
		time.Sleep(10*time.Second + jitter)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = p.refreshWANState(ctx)
		cancel()
	}
}

func (p *WANProbe) refreshWANState(ctx context.Context) error {
	pinger, err := ping.NewPinger(p.cloudEndpoint)
	if err != nil {
		klog.Warningf("NewPinger failed: %v", err)
		p.markStale()
		return err
	}

	pinger.SetPrivileged(false)
	pinger.Count = 3
	pinger.Timeout = 3 * time.Second
	pinger.Interval = 300 * time.Millisecond

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
		p.markStale()
		close(done)
		return err
	}
	close(done)

	stats := pinger.Statistics()
	rtt := int(stats.AvgRtt.Milliseconds())
	loss := float64(stats.PacketLoss)

	p.cacheMu.Lock()
	p.cache = &WANState{
		RTTMs:         rtt,
		LossPct:       loss,
		Timestamp:     time.Now(),
		IsStale:       false,
		StaleDuration: 0,
	}
	p.cacheMu.Unlock()

	klog.V(5).Infof("WAN probe successful: rtt=%dms loss=%.1f%%", rtt, loss)
	return nil
}

func (p *WANProbe) markStale() {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()

	if p.cache != nil {
		p.cache.IsStale = true
		p.cache.StaleDuration = time.Since(p.cache.Timestamp)
	}
}

func (p *WANProbe) GetWANState(ctx context.Context) (*WANState, error) {
	p.cacheMu.RLock()
	defer p.cacheMu.RUnlock()

	if p.cache == nil {
		return &WANState{RTTMs: 999, LossPct: 100, IsStale: true}, nil
	}

	staleDuration := time.Since(p.cache.Timestamp)

	state := &WANState{
		RTTMs:         p.cache.RTTMs,
		LossPct:       p.cache.LossPct,
		Timestamp:     p.cache.Timestamp,
		IsStale:       staleDuration > p.cacheTTL,
		StaleDuration: staleDuration,
	}

	if state.IsStale {
		klog.V(4).Infof("WAN cache stale by %v, using last known values (rtt=%dms)",
			staleDuration, state.RTTMs)
	}

	return state, nil
}

func (p *WANProbe) GetCachedWANState() *WANState {
	p.cacheMu.RLock()
	defer p.cacheMu.RUnlock()

	if p.cache == nil {
		return &WANState{RTTMs: 999, LossPct: 100, IsStale: true}
	}

	staleDuration := time.Since(p.cache.Timestamp)

	return &WANState{
		RTTMs:         p.cache.RTTMs,
		LossPct:       p.cache.LossPct,
		Timestamp:     p.cache.Timestamp,
		IsStale:       staleDuration > p.cacheTTL,
		StaleDuration: staleDuration,
	}
}

func (p *WANProbe) UpdateMetrics() {
	state := p.GetCachedWANState()
	wanRTTGauge.Set(float64(state.RTTMs))
	wanLossGauge.Set(state.LossPct)
	wanStaleGauge.Set(state.StaleDuration.Seconds())
}
