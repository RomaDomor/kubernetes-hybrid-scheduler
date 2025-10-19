package webhook

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	admissionLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "scheduler_admission_duration_seconds",
			Help:    "Time spent processing admission requests",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
	)

	admissionRateLimited = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "scheduler_admission_rate_limited_total",
			Help: "Number of admission requests rejected due to rate limiting",
		},
	)
)
