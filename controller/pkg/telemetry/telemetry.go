package telemetry

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"k8s.io/klog/v2"
)

var (
	queueLambdaGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_queue_lambda_per_ms",
			Help: "Estimated arrival rate per class in arrivals per millisecond",
		},
		[]string{"class"},
	)
)

type ClassStats struct {
	Arrivals    int64
	Completions int64
	WindowStart time.Time
	mu          sync.Mutex
	Window      time.Duration
	minSamples  int64
}

type QueueStatsCollector struct {
	classStats map[string]*ClassStats
	mu         sync.RWMutex
	window     time.Duration
	minSamples int64
}

func NewQueueStatsCollector(window time.Duration, minSamples int64) *QueueStatsCollector {
	return &QueueStatsCollector{
		classStats: make(map[string]*ClassStats),
		window:     window,
		minSamples: minSamples,
	}
}

func (qsc *QueueStatsCollector) getOrCreateClassStats(class string) *ClassStats {
	qsc.mu.Lock()
	defer qsc.mu.Unlock()

	if stats, exists := qsc.classStats[class]; exists {
		return stats
	}

	stats := &ClassStats{
		WindowStart: time.Now(),
		Window:      qsc.window,
		minSamples:  qsc.minSamples,
	}
	qsc.classStats[class] = stats
	return stats
}

func (qsc *QueueStatsCollector) RecordArrival(class string) {
	if class == "" {
		return
	}
	stats := qsc.getOrCreateClassStats(class)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.Arrivals++
	klog.V(5).Infof("Recorded arrival for class %s, total arrivals: %d", class, stats.Arrivals)
}

func (qsc *QueueStatsCollector) RecordCompletion(class string) {
	if class == "" {
		return
	}
	stats := qsc.getOrCreateClassStats(class)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.Completions++
	klog.V(5).Infof("Recorded completion for class %s, total completions: %d", class, stats.Completions)
}

func (qsc *QueueStatsCollector) LambdaForClass(class string) float64 {
	qsc.mu.RLock()
	stats, exists := qsc.classStats[class]
	qsc.mu.RUnlock()

	if !exists {
		return 0
	}

	stats.mu.Lock()
	defer stats.mu.Unlock()

	elapsed := time.Since(stats.WindowStart)

	// Reset window if exceeded (sliding window behavior)
	if elapsed > stats.Window {
		klog.V(4).Infof("Resetting window for class %s after %v", class, elapsed)
		stats.Arrivals = 0
		stats.Completions = 0
		stats.WindowStart = time.Now()
		return 0
	}

	// Need minimum samples for reliable estimate
	if stats.Arrivals < stats.minSamples {
		klog.V(4).Infof("Insufficient samples for class %s: %d < %d", class, stats.Arrivals, stats.minSamples)
		return 0
	}

	if elapsed.Milliseconds() <= 0 {
		return 0
	}

	// λ in arrivals per millisecond
	lambda := float64(stats.Arrivals) / float64(elapsed.Milliseconds())
	return lambda
}

func (qsc *QueueStatsCollector) UpdateMetrics() {
	qsc.mu.RLock()
	defer qsc.mu.RUnlock()

	for class, stats := range qsc.classStats {
		stats.mu.Lock()
		lambda := float64(stats.Arrivals)
		elapsed := time.Since(stats.WindowStart)
		if elapsed.Milliseconds() > 0 && stats.Arrivals >= stats.minSamples {
			lambda = lambda / float64(elapsed.Milliseconds())
		} else {
			lambda = 0
		}
		stats.mu.Unlock()

		queueLambdaGauge.WithLabelValues(class).Set(lambda)
	}
}
