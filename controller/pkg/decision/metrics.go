package decision

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	decisionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_decisions_total",
			Help: "Total scheduling decisions by location, reason, and class",
		},
		[]string{"location", "reason", "class"},
	)

	profileCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_profile_samples_total",
			Help: "Number of samples per profile",
		},
		[]string{"class", "tier", "location"},
	)

	profileConfidence = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_profile_confidence",
			Help: "Confidence score (0-1) per profile",
		},
		[]string{"class", "tier", "location"},
	)

	sloComplianceRate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_slo_compliance_rate",
			Help: "SLO compliance rate per profile",
		},
		[]string{"class", "tier", "location"},
	)

	predictionError = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scheduler_prediction_error_ms",
			Help:    "Difference between predicted and actual ETA",
			Buckets: []float64{10, 25, 50, 100, 250, 500, 1000},
		},
		[]string{"class", "tier", "location"},
	)

	// Lyapunov metrics - magnitude queue
	lyapunovVirtualQueue = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_virtual_queue",
			Help: "Current Lyapunov magnitude virtual queue length per class",
		},
		[]string{"class"},
	)

	// Lyapunov metrics - probability queue
	lyapunovProbQueue = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_prob_queue",
			Help: "Current Lyapunov probability virtual queue length per class",
		},
		[]string{"class"},
	)

	// Track violations by class and location for observability
	lyapunovViolationRate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_violation_rate",
			Help: "Observed SLO violation rate per class (0-1)",
		},
		[]string{"class"},
	)

	lyapunovDecisionWeight = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scheduler_decision_weight",
			Help:    "Lyapunov drift-plus-penalty weight for decisions",
			Buckets: []float64{0, 10, 50, 100, 250, 500, 1000, 2500, 5000},
		},
		[]string{"class", "location"},
	)

	// Actual violation rate tracking
	lyapunovActualViolationRate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_actual_violation_rate",
			Help: "Actual observed SLO violation rate per class (0-1)",
		},
		[]string{"class"},
	)
)

func recordDecision(result Result, class string) {
	decisionsTotal.WithLabelValues(
		string(result.Location),
		result.Reason,
		class,
	).Inc()

	lyapunovDecisionWeight.WithLabelValues(
		class,
		string(result.Location),
	).Observe(result.LyapunovWeight)
}

func (ps *ProfileStore) UpdateMetrics() {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for keyStr, profile := range ps.profiles {
		parts := strings.Split(keyStr, "-")
		class, tier, location := "unknown", "unknown", "unknown"
		if len(parts) >= 3 {
			class = parts[0]
			tier = parts[1]
			location = parts[2]
		}

		labels := prometheus.Labels{
			"class":    class,
			"tier":     tier,
			"location": location,
		}

		profileCount.With(labels).Set(float64(profile.Count))
		profileConfidence.With(labels).Set(profile.ConfidenceScore)
		sloComplianceRate.With(labels).Set(profile.SLOComplianceRate)
	}
}

// UpdateLyapunovMetrics updates Lyapunov-specific metrics
func UpdateLyapunovMetrics(lyapunov *LyapunovScheduler) {
	state := lyapunov.ExportState()

	// Magnitude queues
	queues := state["virtual_queues"].(map[string]float64)
	for class, qLen := range queues {
		lyapunovVirtualQueue.WithLabelValues(class).Set(qLen)
	}

	// Probability queues
	probQueues := state["virtual_prob_queues"].(map[string]float64)
	for class, qLen := range probQueues {
		lyapunovProbQueue.WithLabelValues(class).Set(qLen)
	}

	// Stats including actual violation rates
	stats := state["stats"].(map[string]*LyapunovStats)
	for class, st := range stats {
		lyapunovActualViolationRate.WithLabelValues(class).Set(st.ActualViolationPct)
		lyapunovViolationRate.WithLabelValues(class).Set(st.ActualViolationPct)
	}
}

func recordPredictionError(key ProfileKey, errorMs float64) {
	labels := prometheus.Labels{
		"class":    key.Class,
		"tier":     key.CPUTier,
		"location": string(key.Location),
	}
	predictionError.With(labels).Observe(errorMs)
}
