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

	decisionLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "scheduler_decision_duration_seconds",
			Help:    "Time spent making scheduling decisions",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		},
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

	// Lyapunov metrics
	lyapunovVirtualQueue = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_lyapunov_virtual_queue",
			Help: "Current Lyapunov virtual queue length per class",
		},
		[]string{"class"},
	)

	lyapunovViolations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_lyapunov_violations_total",
			Help: "Total SLO violations per class and location",
		},
		[]string{"class", "location"},
	)

	lyapunovDecisionWeight = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scheduler_lyapunov_decision_weight",
			Help:    "Lyapunov drift-plus-penalty weight for decisions",
			Buckets: []float64{0, 10, 50, 100, 250, 500, 1000, 2500, 5000},
		},
		[]string{"class", "location"},
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

// Update Lyapunov metrics
func UpdateLyapunovMetrics(lyapunov *LyapunovScheduler) {
	state := lyapunov.ExportState()

	queues := state["virtual_queues"].(map[string]float64)
	for class, qLen := range queues {
		lyapunovVirtualQueue.WithLabelValues(class).Set(qLen)
	}

	stats := state["stats"].(map[string]*LyapunovStats)
	for class, st := range stats {
		lyapunovViolations.WithLabelValues(class, "edge").Add(float64(st.ViolationsEdge))
		lyapunovViolations.WithLabelValues(class, "cloud").Add(float64(st.ViolationsCloud))
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
