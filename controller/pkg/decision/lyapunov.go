package decision

import (
	"math"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

// LyapunovScheduler implements drift-plus-penalty optimization for SLO guarantees
// Mathematical foundation: Lyapunov optimization theory (Neely 2010)
// Guarantees: Achieves target SLO compliance with bounded virtual queue lengths
type LyapunovScheduler struct {
	mu sync.RWMutex

	// Virtual queues track "debt" of SLO violations per class
	// Z[class] represents accumulated violations that need to be "paid back"
	virtualQueues map[string]float64

	// Beta controls the cost-performance tradeoff
	// Higher beta → prefer cheaper (local) options
	// Lower beta → prioritize SLO compliance
	beta float64

	// Target violation rate per class (e.g., 0.05 = 5% of deadline)
	targetViolationPct map[string]float64

	// Decay factor to prevent unbounded queue growth
	// Applied periodically to forgive old violations
	decayFactor float64
	lastDecay   time.Time

	// Statistics for monitoring
	stats map[string]*LyapunovStats
}

type LyapunovStats struct {
	TotalDecisions  int64
	EdgeDecisions   int64
	CloudDecisions  int64
	ViolationsEdge  int64
	ViolationsCloud int64
	AvgVirtualQueue float64
	MaxVirtualQueue float64
	LastUpdated     time.Time
}

func NewLyapunovScheduler(beta float64) *LyapunovScheduler {
	return &LyapunovScheduler{
		virtualQueues:      make(map[string]float64),
		beta:               beta,
		targetViolationPct: make(map[string]float64),
		decayFactor:        0.95, // 5% decay per hour
		lastDecay:          time.Now(),
		stats:              make(map[string]*LyapunovStats),
	}
}

// SetTargetViolationRate sets the acceptable SLO violation rate for a class
// Example: SetTargetViolationRate("latency", 0.05) means allow 5% deadline slack
func (l *LyapunovScheduler) SetTargetViolationRate(class string, rate float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.targetViolationPct[class] = rate
}

// Decide makes the scheduling decision using Lyapunov drift-plus-penalty
func (l *LyapunovScheduler) Decide(
	class string,
	deadline float64,
	localETA float64,
	cloudETA float64,
	localFeasible bool,
	cloudFeasible bool,
	localCost float64,
	cloudCost float64,
) (location constants.Location, weight float64) {

	l.mu.Lock()
	defer l.mu.Unlock()

	// Periodic decay to prevent unbounded growth
	if time.Since(l.lastDecay) > time.Hour {
		l.applyDecay()
	}

	// Get current virtual queue for this class
	Z := l.virtualQueues[class]

	// Initialize stats if needed
	if l.stats[class] == nil {
		l.stats[class] = &LyapunovStats{LastUpdated: time.Now()}
	}

	// Compute expected violations (penalty for missing deadline)
	localViolation := math.Max(0, localETA-deadline)
	cloudViolation := math.Max(0, cloudETA-deadline)

	// Compute drift-plus-penalty weights
	// Weight = β*Cost + Z*Violation
	// Lower weight is better
	localWeight := l.beta*localCost + Z*localViolation
	cloudWeight := l.beta*cloudCost + Z*cloudViolation

	klog.V(5).Infof(
		"Lyapunov decision: class=%s Z=%.2f deadline=%.0fms | "+
			"Edge: ETA=%.0fms violation=%.0fms cost=%.2f weight=%.2f | "+
			"Cloud: ETA=%.0fms violation=%.0fms cost=%.2f weight=%.2f",
		class, Z, deadline,
		localETA, localViolation, localCost, localWeight,
		cloudETA, cloudViolation, cloudCost, cloudWeight,
	)

	// Hard feasibility constraints override weights
	if !localFeasible && !cloudFeasible {
		// Neither feasible - choose minimum violation
		if localViolation <= cloudViolation {
			l.stats[class].TotalDecisions++
			l.stats[class].EdgeDecisions++
			return constants.Edge, localWeight
		}
		l.stats[class].TotalDecisions++
		l.stats[class].CloudDecisions++
		return constants.Cloud, cloudWeight
	}

	if localFeasible && !cloudFeasible {
		l.stats[class].TotalDecisions++
		l.stats[class].EdgeDecisions++
		return constants.Edge, localWeight
	}

	if cloudFeasible && !localFeasible {
		l.stats[class].TotalDecisions++
		l.stats[class].CloudDecisions++
		return constants.Cloud, cloudWeight
	}

	// Both feasible - choose minimum weight
	if localWeight <= cloudWeight {
		l.stats[class].TotalDecisions++
		l.stats[class].EdgeDecisions++
		return constants.Edge, localWeight
	}

	l.stats[class].TotalDecisions++
	l.stats[class].CloudDecisions++
	return constants.Cloud, cloudWeight
}

// UpdateVirtualQueue updates the virtual queue after observing actual completion
// Call this from the PodObserver after a pod completes
func (l *LyapunovScheduler) UpdateVirtualQueue(
	class string,
	deadline float64,
	actualCompletion float64,
	location constants.Location,
) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Compute actual violation
	actualViolation := math.Max(0, actualCompletion-deadline)

	// Get target violation threshold
	targetViolation := deadline * l.getTargetViolationPct(class)

	// Update virtual queue: Z[t+1] = max(0, Z[t] + violation - target)
	// This ensures long-term average violation <= target
	oldZ := l.virtualQueues[class]
	newZ := math.Max(0, oldZ+actualViolation-targetViolation)
	l.virtualQueues[class] = newZ

	// Update statistics
	stats := l.stats[class]
	if stats == nil {
		stats = &LyapunovStats{LastUpdated: time.Now()}
		l.stats[class] = stats
	}

	if actualViolation > 0 {
		if location == constants.Edge {
			stats.ViolationsEdge++
		} else {
			stats.ViolationsCloud++
		}
	}

	// Update running statistics
	alpha := 0.1 // EMA smoothing factor
	stats.AvgVirtualQueue = alpha*newZ + (1-alpha)*stats.AvgVirtualQueue
	if newZ > stats.MaxVirtualQueue {
		stats.MaxVirtualQueue = newZ
	}
	stats.LastUpdated = time.Now()

	klog.V(4).Infof(
		"Lyapunov virtual queue update: class=%s location=%s "+
			"deadline=%.0fms actual=%.0fms violation=%.0fms target=%.0fms Z: %.2f→%.2f",
		class, location, deadline, actualCompletion, actualViolation, targetViolation,
		oldZ, newZ,
	)
}

// applyDecay reduces virtual queues to prevent unbounded growth
// This "forgives" old violations and adapts to changing conditions
func (l *LyapunovScheduler) applyDecay() {
	for class, Z := range l.virtualQueues {
		l.virtualQueues[class] = Z * l.decayFactor
		klog.V(4).Infof("Applied decay to class %s: Z %.2f → %.2f",
			class, Z, l.virtualQueues[class])
	}
	l.lastDecay = time.Now()
}

func (l *LyapunovScheduler) getTargetViolationPct(class string) float64 {
	if rate, exists := l.targetViolationPct[class]; exists {
		return rate
	}

	// Default target violation rates by class
	switch class {
	case "latency", "interactive":
		return 0.05 // 5% slack
	case "throughput", "streaming":
		return 0.10 // 10% slack
	case "batch":
		return 0.20 // 20% slack
	default:
		return 0.10
	}
}

// GetVirtualQueue returns the current virtual queue length for a class
func (l *LyapunovScheduler) GetVirtualQueue(class string) float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.virtualQueues[class]
}

// GetStats returns statistics for a class
func (l *LyapunovScheduler) GetStats(class string) *LyapunovStats {
	l.mu.RLock()
	defer l.mu.RUnlock()

	stats := l.stats[class]
	if stats == nil {
		return &LyapunovStats{}
	}

	// Return a copy to avoid race conditions
	return &LyapunovStats{
		TotalDecisions:  stats.TotalDecisions,
		EdgeDecisions:   stats.EdgeDecisions,
		CloudDecisions:  stats.CloudDecisions,
		ViolationsEdge:  stats.ViolationsEdge,
		ViolationsCloud: stats.ViolationsCloud,
		AvgVirtualQueue: stats.AvgVirtualQueue,
		MaxVirtualQueue: stats.MaxVirtualQueue,
		LastUpdated:     stats.LastUpdated,
	}
}

// ExportState exports all virtual queues and stats for persistence/debugging
func (l *LyapunovScheduler) ExportState() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	queues := make(map[string]float64)
	for k, v := range l.virtualQueues {
		queues[k] = v
	}

	statsMap := make(map[string]*LyapunovStats)
	for k, v := range l.stats {
		statsMap[k] = &LyapunovStats{
			TotalDecisions:  v.TotalDecisions,
			EdgeDecisions:   v.EdgeDecisions,
			CloudDecisions:  v.CloudDecisions,
			ViolationsEdge:  v.ViolationsEdge,
			ViolationsCloud: v.ViolationsCloud,
			AvgVirtualQueue: v.AvgVirtualQueue,
			MaxVirtualQueue: v.MaxVirtualQueue,
			LastUpdated:     v.LastUpdated,
		}
	}

	return map[string]interface{}{
		"virtual_queues":       queues,
		"stats":                statsMap,
		"beta":                 l.beta,
		"decay_factor":         l.decayFactor,
		"target_violation_pct": l.targetViolationPct,
		"last_decay":           l.lastDecay,
	}
}

// ResetStats resets statistics (useful for testing)
func (l *LyapunovScheduler) ResetStats() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for class := range l.stats {
		l.stats[class] = &LyapunovStats{LastUpdated: time.Now()}
	}
}
