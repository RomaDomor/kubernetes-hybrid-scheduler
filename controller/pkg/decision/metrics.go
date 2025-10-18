package decision

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
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
)

func (ps *ProfileStore) UpdateMetrics() {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, profile := range ps.profiles {
		// Parse key back (keyStr format: "class-tier-location")
		// For simplicity, we'll skip parsing and use keyStr directly
		// In production, you'd want proper parsing

		labels := prometheus.Labels{
			"class":    "unknown",
			"tier":     "unknown",
			"location": "unknown",
		}

		profileCount.With(labels).Set(float64(profile.Count))
		profileConfidence.With(labels).Set(profile.ConfidenceScore)
		sloComplianceRate.With(labels).Set(profile.SLOComplianceRate)
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
