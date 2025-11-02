package v1alpha1

import (
	"fmt"
	"math"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
)

// Result represents the outcome of a scheduling decision.
type Result struct {
	Location       constants.Location
	Reason         string
	PredictedETAMs float64
	WanRttMs       int
	LyapunovWeight float64
}

// ProfileKey uniquely identifies a workload profile.
type ProfileKey struct {
	Class    string
	CPUTier  string
	Location constants.Location
}

func (pk ProfileKey) String() string {
	return fmt.Sprintf("%s-%s-%s", pk.Class, pk.CPUTier, pk.Location)
}

// ProfileStats holds the statistical data for a workload profile.
type ProfileStats struct {
	Count             int               `json:"count"`
	MeanDurationMs    float64           `json:"mean_duration_ms"`
	StdDevDurationMs  float64           `json:"stddev_duration_ms"`
	P95DurationMs     float64           `json:"p95_duration_ms"`
	MeanQueueWaitMs   float64           `json:"mean_queue_wait_ms"`
	SLOComplianceRate float64           `json:"slo_compliance_rate"`
	ConfidenceScore   float64           `json:"confidence_score"`
	LastUpdated       time.Time         `json:"last_updated"`
	DurationHistogram []HistogramBucket `json:"duration_histogram"`
}

// HistogramBucket represents a single bucket in a performance histogram.
type HistogramBucket struct {
	UpperBoundValue float64   `json:"upper_bound_value"`
	UpperBoundStr   string    `json:"upper_bound_str"`
	Count           int       `json:"count"`
	LastDecay       time.Time `json:"last_decay"`
}

func (hb *HistogramBucket) UpperBound() float64 {
	if hb.UpperBoundStr == "Inf" {
		return math.Inf(1)
	}
	return hb.UpperBoundValue
}

func (hb *HistogramBucket) SetUpperBound(val float64) {
	if math.IsInf(val, +1) {
		hb.UpperBoundStr = "Inf"
		hb.UpperBoundValue = 0
	} else {
		hb.UpperBoundStr = ""
		hb.UpperBoundValue = val
	}
}

// ProfileUpdate contains the data from a single completed pod observation.
type ProfileUpdate struct {
	ObservedDurationMs float64
	QueueWaitMs        float64
	SLOMet             bool
}

// GetProfileKey generates a unique key for a pod's profile based on its characteristics.
func GetProfileKey(pod *corev1.Pod, loc constants.Location) ProfileKey {
	cpuMillis := util.GetCPURequest(pod)

	tier := "medium"
	if cpuMillis < 500 {
		tier = "small"
	} else if cpuMillis > 2000 {
		tier = "large"
	}

	class := pod.Annotations[constants.AnnotationSLOClass]
	if class == "" {
		class = "batch"
	}

	class = normalizeClass(class)

	return ProfileKey{
		Class:    class,
		CPUTier:  tier,
		Location: loc,
	}
}

// normalizeClass ensures the pod's SLO class is a known, valid value.
func normalizeClass(class string) string {
	if constants.ValidSLOClasses[class] {
		return class
	}
	klog.V(4).Infof("Unknown class '%s', defaulting to '%s'", class, constants.DefaultSLOClass)
	return constants.DefaultSLOClass
}

// ComputeViolationProbability computes Pr[completion > threshold] from histogram.
//
// ALGORITHM:
//  1. If sufficient samples (≥20): compute from histogram CDF
//  2. If low samples: use normal approximation from mean/stddev
//  3. If no data: return 0.5 (maximum uncertainty)
//
// HISTOGRAM-BASED (primary method):
//   - Compute CDF: F(x) = Σ{buckets ≤ x} count / total
//   - Linear interpolation within buckets
//   - Return tail: 1 - F(threshold)
//
// STATS-BASED (fallback for low samples):
//   - Normal approximation: Pr[X > t] ≈ 1 - Φ((t - μ) / σ)
//   - Conservative for right-skewed distributions
func (ps *ProfileStats) ComputeViolationProbability(threshold float64) float64 {
	if ps == nil {
		// No data: maximum uncertainty
		return 0.5
	}

	// Deterministic case: no variance (handle before histogram check)
	if ps.StdDevDurationMs <= 0 {
		if ps.MeanDurationMs > threshold {
			return 1.0
		}
		return 0.0
	}

	if len(ps.DurationHistogram) == 0 {
		// No histogram: maximum uncertainty
		return 0.5
	}

	// Require minimum samples for histogram-based probability
	const minSamplesForHistogram = 20
	if ps.Count < minSamplesForHistogram {
		// Low sample count: use stats-based estimate
		return ps.computeViolationProbabilityFromStats(threshold)
	}

	// Compute total samples in histogram
	totalCount := 0
	for _, bucket := range ps.DurationHistogram {
		totalCount += bucket.Count
	}

	if totalCount == 0 {
		// Histogram exists but empty (edge case after decay)
		return ps.computeViolationProbabilityFromStats(threshold)
	}

	// Compute CDF: cumulative count up to threshold
	cumulativeCount := 0
	foundBucket := false

	for i, bucket := range ps.DurationHistogram {
		ub := bucket.UpperBound()

		if ub >= threshold {
			// Threshold falls in this bucket: interpolate
			if i == 0 {
				// First bucket: [0, ub]
				if ub <= 0 || math.IsInf(ub, 1) {
					// Can't interpolate: assume half below threshold
					cumulativeCount += bucket.Count / 2
				} else {
					ratio := threshold / ub
					inBucketCount := int(float64(bucket.Count) * ratio)
					cumulativeCount += inBucketCount
				}
			} else {
				// General bucket: [prevUB, ub]
				prevUB := ps.DurationHistogram[i-1].UpperBound()
				bucketRange := ub - prevUB

				if bucketRange <= 0 || math.IsInf(ub, 1) {
					// Can't interpolate (collapsed or +Inf bucket)
					// Assume half below threshold
					cumulativeCount += bucket.Count / 2
				} else {
					// Linear interpolation
					ratio := (threshold - prevUB) / bucketRange
					ratio = math.Max(0, math.Min(1, ratio)) // Clamp to [0,1]
					inBucketCount := int(float64(bucket.Count) * ratio)
					cumulativeCount += inBucketCount
				}
			}
			foundBucket = true
			break
		} else {
			// Entire bucket is below threshold
			cumulativeCount += bucket.Count
		}
	}

	if !foundBucket {
		// Threshold exceeds all finite buckets
		// Check for +Inf bucket with outliers
		if len(ps.DurationHistogram) > 0 {
			lastBucket := ps.DurationHistogram[len(ps.DurationHistogram)-1]
			if math.IsInf(lastBucket.UpperBound(), 1) && lastBucket.Count > 0 {
				// Some samples in +Inf bucket: these definitely violated
				// All other samples didn't violate
				cdf := float64(totalCount-lastBucket.Count) / float64(totalCount)
				tailProb := 1.0 - cdf
				return math.Max(0.001, math.Min(1.0, tailProb))
			}
		}

		// All samples below threshold: use extrapolation
		return ps.extrapolateTailProbability(threshold)
	}

	// Compute tail probability: Pr[X > threshold] = 1 - F(threshold)
	cdf := float64(cumulativeCount) / float64(totalCount)
	tailProb := 1.0 - cdf

	// Clamp to valid range
	return math.Max(0, math.Min(1, tailProb))
}

// computeViolationProbabilityFromStats uses normal approximation when histogram
// has insufficient samples.
func (ps *ProfileStats) computeViolationProbabilityFromStats(threshold float64) float64 {
	// Deterministic case: no variance
	if ps.StdDevDurationMs <= 0 {
		// No variance: deterministic
		if ps.MeanDurationMs > threshold {
			return 1.0
		}
		return 0.0
	}

	// Standard normal approximation: Φ((threshold - mean) / stddev)
	z := (threshold - ps.MeanDurationMs) / ps.StdDevDurationMs

	// Fast approximation of standard normal CDF
	// Φ(z) ≈ 1 / (1 + exp(-1.702 * z)) for z > 0
	// Φ(z) ≈ exp(-1.702 * |z|) / (1 + exp(-1.702 * |z|)) for z < 0

	var cdf float64
	if z >= 0 {
		// Threshold above mean: low violation probability
		cdf = 1.0 / (1.0 + math.Exp(-1.702*z))
	} else {
		// Threshold below mean: high violation probability
		absZ := -z
		cdf = math.Exp(-1.702*absZ) / (1.0 + math.Exp(-1.702*absZ))
	}

	return 1.0 - cdf
}

// extrapolateTailProbability estimates tail probability when threshold exceeds
// all observed samples using exponential tail model.
func (ps *ProfileStats) extrapolateTailProbability(threshold float64) float64 {
	// If threshold well beyond P95, use exponential tail
	if threshold > ps.P95DurationMs && ps.StdDevDurationMs > 0 {
		// P(X > threshold) ≈ P(X > P95) * exp(-(threshold - P95) / stddev)
		// P(X > P95) = 0.05 by definition
		excess := threshold - ps.P95DurationMs
		lambda := 1.0 / ps.StdDevDurationMs
		tailProb := 0.05 * math.Exp(-lambda*excess)
		return math.Max(0.001, tailProb) // At least 0.1% (10× lower than P95 tail)
	}

	// Threshold comfortably exceeds observations: very low probability
	return 0.001 // 0.1%
}
