package constants

const (
	LabelManaged           = "scheduling.hybrid.io/managed"
	AnnotationDecision     = "scheduling.hybrid.io/decision"
	AnnotationPredictedETA = "scheduling.hybrid.io/predictedETAMs"
	AnnotationTimestamp    = "scheduling.hybrid.io/timestamp"
	AnnotationReason       = "scheduling.hybrid.io/reason"
	AnnotationWANRtt       = "scheduling.hybrid.io/wanRttMs"
	AnnotationActualStart  = "scheduling.hybrid.io/actualStart"
	AnnotationQueueWait    = "scheduling.hybrid.io/queueWaitMs"

	AnnotationSLOClass          = "slo.hybrid.io/class"
	AnnotationSLODeadline       = "slo.hybrid.io/deadlineMs"
	AnnotationSLOLatencyTarget  = "slo.hybrid.io/latencyTargetMs"
	AnnotationSLOPriority       = "slo.hybrid.io/priority"
	AnnotationSLOOffloadAllowed = "slo.hybrid.io/offloadAllowed"
)
