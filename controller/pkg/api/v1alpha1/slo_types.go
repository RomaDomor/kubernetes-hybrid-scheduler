package v1alpha1

// SLO holds the parsed Service Level Objective parameters for a pod.
type SLO struct {
	DeadlineMs      int
	LatencyTargetMs int
	Class           string
	Priority        int
	OffloadAllowed  bool
}
