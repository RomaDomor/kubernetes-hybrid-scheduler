// pkg/decision/config.go
package decision

import (
	"strconv"
	"strings"
	"time"
)

type HistogramBoundsMode string

const (
	BoundsExplicit HistogramBoundsMode = "explicit"
	BoundsLog      HistogramBoundsMode = "log"
)

type HistogramConfig struct {
	// Mode: "explicit" or "log"
	Mode HistogramBoundsMode

	// Explicit bounds: comma-separated ms list
	Explicit []float64

	// Log mode params
	LogStartMs float64 // first bound
	LogFactor  float64 // multiplier per step (e.g., 1.8 or 2.0)
	LogCount   int     // number of bounds
	IncludeInf bool    // whether to append +Inf bucket

	// Sampling/conf
	IngestCapMs    float64       // cap for observed durations
	MinSampleCount int           // minimum samples for reliable p95
	DecayInterval  time.Duration // half-life check interval (e.g., 1h)
}

func ParseFloatSliceCSV(s string) []float64 {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]float64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if f, err := strconv.ParseFloat(p, 64); err == nil {
			out = append(out, f)
		}
	}
	return out
}

// Defaults aligned to your current code
func DefaultHistogramConfig() HistogramConfig {
	return HistogramConfig{
		Mode:           BoundsExplicit,
		Explicit:       []float64{50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 30000, 60000, 120000, 180000, 300000, 450000, 600000, 900000, 1200000, 1500000, 1800000},
		IncludeInf:     true,
		LogStartMs:     50,
		LogFactor:      2.0,
		LogCount:       20,
		IngestCapMs:    30 * 60 * 1000, // 30 min
		MinSampleCount: 10,
		DecayInterval:  time.Hour,
	}
}
