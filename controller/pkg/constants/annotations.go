package constants

// This file centralizes all annotation and label keys used across the controller.
// Using constants prevents typos and makes refactoring easier.

const (
	// LabelManaged is the label key to identify pods managed by the hybrid scheduler.
	LabelManaged = "scheduling.hybrid.io/managed"

	// AnnotationDecision stores the scheduling decision (edge or cloud).
	AnnotationDecision = "scheduling.hybrid.io/decision"
	// AnnotationPredictedETA stores the predicted ETA in milliseconds.
	AnnotationPredictedETA = "scheduling.hybrid.io/predictedETAMs"
	// AnnotationTimestamp stores the timestamp of the scheduling decision.
	AnnotationTimestamp = "scheduling.hybrid.io/timestamp"
	// AnnotationReason stores the reason for the scheduling decision.
	AnnotationReason = "scheduling.hybrid.io/reason"
	// AnnotationWANRtt stores the WAN RTT at the time of the decision.
	AnnotationWANRtt = "scheduling.hybrid.io/wanRttMs"
	// AnnotationActualStart stores the timestamp when the pod was observed starting.
	AnnotationActualStart = "scheduling.hybrid.io/actualStart"
	// AnnotationQueueWait stores the calculated queue wait time in milliseconds.
	AnnotationQueueWait = "scheduling.hybrid.io/queueWaitMs"

	// AnnotationSLOClass defines the workload class (e.g., latency, batch).
	AnnotationSLOClass = "slo.hybrid.io/class"
	// AnnotationSLODeadline defines the end-to-end deadline in milliseconds.
	AnnotationSLODeadline = "slo.hybrid.io/deadlineMs"
	// AnnotationSLOLatencyTarget is an alias for the deadline.
	AnnotationSLOLatencyTarget = "slo.hybrid.io/latencyTargetMs"
	// AnnotationSLOPriority defines the workload priority (0-10).
	AnnotationSLOPriority = "slo.hybrid.io/priority"
	// AnnotationSLOOffloadAllowed specifies if offloading to the cloud is permitted.
	AnnotationSLOOffloadAllowed = "slo.hybrid.io/offloadAllowed"
)
