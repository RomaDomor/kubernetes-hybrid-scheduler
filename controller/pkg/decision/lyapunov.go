package decision

import (
	"math"
	"sync"
	"time"

	"k8s.io/klog/v2"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

type LyapunovScheduler struct {
	mu sync.RWMutex

	// Virtual queues: Z_c tracks accumulated magnitude of SLO violations
	// Dynamics: Z_c[t+1] = max(0, Z_c[t] + actualViolation - targetSlack)
	virtualQueues map[string]float64

	// Probability queues: Zp_c tracks accumulated probability of violations
	// Dynamics: Zp_c[t+1] = max(0, Zp_c[t] + 1{violation} - targetProbability)
	virtualProbQueues map[string]float64

	// Per-class configuration
	config map[string]*ClassConfig

	// Decay settings (optional anti-windup mechanism)
	lastDecay time.Time

	// Statistics for monitoring and debugging
	stats map[string]*LyapunovStats
}

// ClassConfig holds per-class Lyapunov parameters
type ClassConfig struct {
	// Beta (V parameter): cost-performance tradeoff
	// Higher beta → prioritize cost reduction
	// Lower beta → prioritize SLO compliance
	Beta float64

	// Target violation slack: percentage of deadline allowed as average slack
	// e.g., 0.05 means allow average positive lateness of 5% of deadline
	TargetViolationPct float64

	// Target violation probability: acceptable fraction of jobs that miss deadline
	// e.g., 0.05 means allow 5% of jobs to violate
	TargetViolationProb float64

	// Decay factor for periodic queue forgiveness (0.95 = 5% decay per interval)
	DecayFactor float64

	// Decay interval (e.g., 1 hour)
	DecayInterval time.Duration

	// Weight for probability queue in decision (relative to magnitude queue)
	ProbabilityWeight float64
}

type LyapunovStats struct {
	TotalDecisions  int64
	EdgeDecisions   int64
	CloudDecisions  int64
	ViolationsEdge  int64
	ViolationsCloud int64

	// Magnitude queue stats
	AvgVirtualQueue float64
	MaxVirtualQueue float64

	// Probability queue stats
	AvgProbQueue float64
	MaxProbQueue float64

	// Violation tracking
	ViolationCount     int64   // Total violations observed
	CompletionCount    int64   // Total completions observed
	ActualViolationPct float64 // Running violation rate

	LastUpdated time.Time
}

// NewLyapunovScheduler creates a new Lyapunov scheduler with default beta.
// For per-class configuration, use SetClassConfig after creation.
func NewLyapunovScheduler() *LyapunovScheduler {
	return &LyapunovScheduler{
		virtualQueues:     make(map[string]float64),
		virtualProbQueues: make(map[string]float64),
		config:            make(map[string]*ClassConfig),
		lastDecay:         time.Now(),
		stats:             make(map[string]*LyapunovStats),
	}
}

// SetClassConfig sets per-class Lyapunov parameters.
// If not set for a class, defaults from NewLyapunovScheduler are used.
func (l *LyapunovScheduler) SetClassConfig(class string, cfg *ClassConfig) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Validate and set defaults
	if cfg.Beta <= 0 {
		cfg.Beta = 1.0
	}
	if cfg.TargetViolationPct <= 0 {
		cfg.TargetViolationPct = 0.05
	}
	if cfg.TargetViolationProb <= 0 {
		cfg.TargetViolationProb = 0.05
	}
	if cfg.DecayFactor <= 0 || cfg.DecayFactor >= 1 {
		cfg.DecayFactor = 0.95
	}
	if cfg.DecayInterval == 0 {
		cfg.DecayInterval = time.Hour
	}
	if cfg.ProbabilityWeight < 0 {
		cfg.ProbabilityWeight = 1.0
	}

	l.config[class] = cfg
}

// SetTargetViolationRate sets both magnitude and probability targets (legacy API).
func (l *LyapunovScheduler) SetTargetViolationRate(class string, rate float64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.config[class] == nil {
		l.config[class] = &ClassConfig{
			Beta:                1.0,
			TargetViolationPct:  rate,
			TargetViolationProb: rate,
			DecayFactor:         0.95,
			DecayInterval:       time.Hour,
			ProbabilityWeight:   1.0,
		}
	} else {
		l.config[class].TargetViolationPct = rate
		l.config[class].TargetViolationProb = rate
	}
}

// getClassConfig returns configuration for a class, with fallback to defaults.
func (l *LyapunovScheduler) getClassConfig(class string) *ClassConfig {
	if cfg, exists := l.config[class]; exists {
		return cfg
	}

	// Default config based on class tier
	beta := 1.0
	targetPct := 0.10
	targetProb := 0.10

	switch class {
	case "latency", "interactive":
		targetPct = 0.05
		targetProb = 0.05
	case "batch":
		targetPct = 0.20
		targetProb = 0.20
	}

	return &ClassConfig{
		Beta:                beta,
		TargetViolationPct:  targetPct,
		TargetViolationProb: targetProb,
		DecayFactor:         0.95,
		DecayInterval:       time.Hour,
		ProbabilityWeight:   1.0,
	}
}

// Decide makes the scheduling decision using drift-plus-penalty optimization.
//
// INPUTS:
//   - class: SLO class (e.g., "latency", "batch")
//   - deadline: job deadline in milliseconds
//   - localETA, cloudETA: predicted completion times
//   - localProfile, cloudProfile: execution profiles with histograms
//   - localFeasible, cloudFeasible: hard feasibility constraints
//   - localCost, cloudCost: resource costs (e.g., energy, monetary)
//
// OUTPUTS:
//   - location: chosen placement (Edge or Cloud)
//   - weight: drift-plus-penalty weight (lower is better)
//
// ALGORITHM:
//  1. Get virtual queues Z_c and Zp_c for class c
//  2. Compute expected violations:
//     - magnitude: max(0, P95_ETA - deadline)
//     - probability: Pr[completion > deadline] from histogram CDF
//  3. Compute weights:
//     - W_local = β*localCost + Z_c*localViolation + Zp_c*localProbability
//     - W_cloud = β*cloudCost + Z_c*cloudViolation + Zp_c*cloudProbability
//  4. Choose location with minimum weight (respecting feasibility)
func (l *LyapunovScheduler) Decide(
	class string,
	deadline float64,
	localETA float64,
	cloudETA float64,
	localProfile *apis.ProfileStats,
	cloudProfile *apis.ProfileStats,
	localFeasible bool,
	cloudFeasible bool,
	localCost float64,
	cloudCost float64,
) (location constants.Location, weight float64) {

	l.mu.Lock()
	defer l.mu.Unlock()

	// Periodic decay
	cfg := l.getClassConfig(class)
	if time.Since(l.lastDecay) > cfg.DecayInterval {
		l.applyDecay()
	}

	// Get virtual queues
	Z := l.virtualQueues[class]
	Zp := l.virtualProbQueues[class]

	// Initialize stats
	if l.stats[class] == nil {
		l.stats[class] = &LyapunovStats{LastUpdated: time.Now()}
	}

	// Compute expected violations (magnitude)
	localViolation := math.Max(0, localETA-deadline)
	cloudViolation := math.Max(0, cloudETA-deadline)

	// Compute violation probabilities from profile histograms
	localProbability := localProfile.ComputeViolationProbability(deadline)
	cloudProbability := cloudProfile.ComputeViolationProbability(deadline)

	// Compute drift-plus-penalty weights
	// Weight = β*Cost + Z*Violation_magnitude + Zp*Violation_probability
	beta := cfg.Beta
	probWeight := cfg.ProbabilityWeight

	localWeight := beta*localCost + Z*localViolation + probWeight*Zp*localProbability
	cloudWeight := beta*cloudCost + Z*cloudViolation + probWeight*Zp*cloudProbability

	klog.V(5).Infof(
		"Lyapunov decision: class=%s Z=%.2f Zp=%.2f deadline=%.0fms β=%.2f | "+
			"Edge: ETA=%.0fms viol=%.0fms prob=%.3f (n=%d conf=%.2f) cost=%.2f weight=%.2f | "+
			"Cloud: ETA=%.0fms viol=%.0fms prob=%.3f (n=%d conf=%.2f) cost=%.2f weight=%.2f",
		class, Z, Zp, deadline, beta,
		localETA, localViolation, localProbability, localProfile.Count, localProfile.ConfidenceScore, localCost, localWeight,
		cloudETA, cloudViolation, cloudProbability, cloudProfile.Count, cloudProfile.ConfidenceScore, cloudCost, cloudWeight,
	)

	// Feasibility-aware decision
	if !localFeasible && !cloudFeasible {
		// Neither feasible - choose minimum total penalty
		localPenalty := localViolation + localProbability*deadline
		cloudPenalty := cloudViolation + cloudProbability*deadline

		if localPenalty < cloudPenalty || (localPenalty == cloudPenalty && localCost < cloudCost) {
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

// UpdateVirtualQueue updates virtual queues after observing actual completion.
//
// VIRTUAL QUEUE DYNAMICS:
//
//  1. Magnitude queue (slack control):
//     Z_c[t+1] = max(0, Z_c[t] + actualViolation - targetSlack)
//     where targetSlack = deadline * targetViolationPct
//
//  2. Probability queue (violation probability control):
//     Zp_c[t+1] = max(0, Zp_c[t] + indicator - targetProbability)
//     where indicator = 1 if actualCompletion > deadline, 0 otherwise
//
// INTERPRETATION:
//   - If average violation exceeds target, Z increases → system prefers lower-violation options
//   - If violation probability exceeds target, Zp increases → system avoids risky options
//   - Queues naturally stabilize when constraints are met on average
func (l *LyapunovScheduler) UpdateVirtualQueue(
	class string,
	deadline float64,
	actualCompletion float64,
	location constants.Location,
) {
	l.mu.Lock()
	defer l.mu.Unlock()

	cfg := l.getClassConfig(class)

	// Compute actual violation (magnitude)
	actualViolation := math.Max(0, actualCompletion-deadline)

	// Compute violation indicator (probability)
	violationIndicator := 0.0
	if actualCompletion > deadline {
		violationIndicator = 1.0
	}

	// Get target thresholds
	targetSlack := deadline * cfg.TargetViolationPct
	targetProb := cfg.TargetViolationProb

	// Update magnitude virtual queue
	oldZ := l.virtualQueues[class]
	newZ := math.Max(0, oldZ+actualViolation-targetSlack)
	l.virtualQueues[class] = newZ

	// Update probability virtual queue
	oldZp := l.virtualProbQueues[class]
	newZp := math.Max(0, oldZp+violationIndicator-targetProb)
	l.virtualProbQueues[class] = newZp

	// Update statistics
	stats := l.stats[class]
	if stats == nil {
		stats = &LyapunovStats{LastUpdated: time.Now()}
		l.stats[class] = stats
	}

	stats.CompletionCount++
	if actualViolation > 0 {
		stats.ViolationCount++
		if location == constants.Edge {
			stats.ViolationsEdge++
		} else {
			stats.ViolationsCloud++
		}
	}

	// Update running violation rate
	stats.ActualViolationPct = float64(stats.ViolationCount) / float64(stats.CompletionCount)

	// Update EMA of queue lengths
	alpha := 0.1
	stats.AvgVirtualQueue = alpha*newZ + (1-alpha)*stats.AvgVirtualQueue
	stats.AvgProbQueue = alpha*newZp + (1-alpha)*stats.AvgProbQueue

	if newZ > stats.MaxVirtualQueue {
		stats.MaxVirtualQueue = newZ
	}
	if newZp > stats.MaxProbQueue {
		stats.MaxProbQueue = newZp
	}

	stats.LastUpdated = time.Now()

	klog.V(4).Infof(
		"Lyapunov queue update: class=%s location=%s "+
			"deadline=%.0fms actual=%.0fms violation=%.0fms indicator=%.0f "+
			"Z: %.2f→%.2f (target_slack=%.0fms) Zp: %.2f→%.2f (target_prob=%.2f) "+
			"violation_rate=%.2f%%",
		class, location, deadline, actualCompletion, actualViolation, violationIndicator,
		oldZ, newZ, targetSlack, oldZp, newZp, targetProb,
		stats.ActualViolationPct*100,
	)
}

// applyDecay reduces virtual queues to prevent unbounded growth.
// This is an optional anti-windup mechanism for non-stationary environments.
//
// WHEN TO USE DECAY:
//   - In environments with regime changes or non-stationary arrivals
//   - To provide "forgiveness" for transient violations
//   - When you want bounded memory of past violations
//
// WHEN NOT TO USE DECAY:
//   - For strict long-term constraint enforcement
//   - In stationary environments where queues will stabilize naturally
//   - When theoretical guarantees without decay are acceptable
func (l *LyapunovScheduler) applyDecay() {
	for class := range l.virtualQueues {
		cfg := l.getClassConfig(class)

		oldZ := l.virtualQueues[class]
		newZ := oldZ * cfg.DecayFactor
		l.virtualQueues[class] = newZ

		oldZp := l.virtualProbQueues[class]
		newZp := oldZp * cfg.DecayFactor
		l.virtualProbQueues[class] = newZp

		if oldZ > 0.01 || oldZp > 0.01 {
			klog.V(4).Infof("Applied decay to class %s: Z %.2f→%.2f Zp %.2f→%.2f",
				class, oldZ, newZ, oldZp, newZp)
		}
	}
	l.lastDecay = time.Now()
}

// GetVirtualQueue returns the current magnitude queue length for a class.
func (l *LyapunovScheduler) GetVirtualQueue(class string) float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.virtualQueues[class]
}

// GetVirtualProbQueue returns the current probability queue length for a class.
func (l *LyapunovScheduler) GetVirtualProbQueue(class string) float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.virtualProbQueues[class]
}

// GetStats returns statistics for a class.
func (l *LyapunovScheduler) GetStats(class string) *LyapunovStats {
	l.mu.RLock()
	defer l.mu.RUnlock()

	stats := l.stats[class]
	if stats == nil {
		return &LyapunovStats{}
	}

	// Return a copy
	return &LyapunovStats{
		TotalDecisions:     stats.TotalDecisions,
		EdgeDecisions:      stats.EdgeDecisions,
		CloudDecisions:     stats.CloudDecisions,
		ViolationsEdge:     stats.ViolationsEdge,
		ViolationsCloud:    stats.ViolationsCloud,
		AvgVirtualQueue:    stats.AvgVirtualQueue,
		MaxVirtualQueue:    stats.MaxVirtualQueue,
		AvgProbQueue:       stats.AvgProbQueue,
		MaxProbQueue:       stats.MaxProbQueue,
		ViolationCount:     stats.ViolationCount,
		CompletionCount:    stats.CompletionCount,
		ActualViolationPct: stats.ActualViolationPct,
		LastUpdated:        stats.LastUpdated,
	}
}

// ExportState exports all virtual queues, configs, and stats for persistence/debugging.
func (l *LyapunovScheduler) ExportState() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	queues := make(map[string]float64)
	for k, v := range l.virtualQueues {
		queues[k] = v
	}

	probQueues := make(map[string]float64)
	for k, v := range l.virtualProbQueues {
		probQueues[k] = v
	}

	configs := make(map[string]interface{})
	for k, v := range l.config {
		configs[k] = map[string]interface{}{
			"beta":                  v.Beta,
			"target_violation_pct":  v.TargetViolationPct,
			"target_violation_prob": v.TargetViolationProb,
			"decay_factor":          v.DecayFactor,
			"decay_interval":        v.DecayInterval.String(),
			"probability_weight":    v.ProbabilityWeight,
		}
	}

	statsMap := make(map[string]*LyapunovStats)
	for k, v := range l.stats {
		statsMap[k] = &LyapunovStats{
			TotalDecisions:     v.TotalDecisions,
			EdgeDecisions:      v.EdgeDecisions,
			CloudDecisions:     v.CloudDecisions,
			ViolationsEdge:     v.ViolationsEdge,
			ViolationsCloud:    v.ViolationsCloud,
			AvgVirtualQueue:    v.AvgVirtualQueue,
			MaxVirtualQueue:    v.MaxVirtualQueue,
			AvgProbQueue:       v.AvgProbQueue,
			MaxProbQueue:       v.MaxProbQueue,
			ViolationCount:     v.ViolationCount,
			CompletionCount:    v.CompletionCount,
			ActualViolationPct: v.ActualViolationPct,
			LastUpdated:        v.LastUpdated,
		}
	}

	return map[string]interface{}{
		"virtual_queues":      queues,
		"virtual_prob_queues": probQueues,
		"class_configs":       configs,
		"stats":               statsMap,
		"last_decay":          l.lastDecay,
	}
}

// ResetStats resets statistics (useful for testing).
func (l *LyapunovScheduler) ResetStats() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for class := range l.stats {
		l.stats[class] = &LyapunovStats{LastUpdated: time.Now()}
	}
}
