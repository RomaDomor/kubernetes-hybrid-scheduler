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

// ProbabilityCalculator defines the function signature for a capability that can
// compute a violation probability from profile statistics.
type ProbabilityCalculator func(stats *ProfileStats, threshold float64) float64

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
