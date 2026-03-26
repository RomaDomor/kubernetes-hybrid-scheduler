package constants

const (
	// Local cluster placement reasons
	ReasonLocalFeasibleOnly     = "local_feasible_only"
	ReasonLocalPreferred        = "local_preferred"
	ReasonLocalViolationControl = "local_violation_control"
	ReasonLocalBestEffort       = "local_best_effort"
	ReasonOffloadDisabled       = "offload_disabled"
	ReasonWANUnusable           = "wan_unusable"
	ReasonWANCircuitBreaker     = "wan_circuit_breaker"

	// Remote cluster placement reasons
	ReasonRemoteFeasibleOnly      = "remote_feasible_only"
	ReasonRemoteFaster            = "remote_faster"
	ReasonRemoteViolationControl  = "remote_violation_control"
	ReasonRemoteBestEffort        = "remote_best_effort"
	ReasonTelemetryCircuitBreaker = "telemetry_circuit_breaker"
	ReasonLowMeasurementConf      = "low_measurement_confidence"

	// Infeasibility reasons
	ReasonInfeasibleResourcesHard = "infeasible_resources_hard"
	ReasonInfeasibleResourcesSoft = "infeasible_resources_soft"
	ReasonInfeasibleTime          = "infeasible_time"

	// --- Legacy aliases (keep until all references are migrated) ---
	ReasonEdgeFeasibleOnly      = ReasonLocalFeasibleOnly
	ReasonEdgePreferred         = ReasonLocalPreferred
	ReasonEdgeViolationControl  = ReasonLocalViolationControl
	ReasonEdgeBestEffort        = ReasonLocalBestEffort
	ReasonCloudFeasibleOnly     = ReasonRemoteFeasibleOnly
	ReasonCloudFaster           = ReasonRemoteFaster
	ReasonCloudViolationControl = ReasonRemoteViolationControl
	ReasonCloudBestEffort       = ReasonRemoteBestEffort
)

// Priority class names for Kubernetes
const (
	PriorityClassQueuedLocal = "queued-local-workload"
	PriorityClassLocalPref   = "local-preferred"
	PriorityClassBestEffort  = "best-effort"
	PriorityClassRemote      = "remote-workload"

	// Legacy aliases
	PriorityClassQueuedEdge = PriorityClassQueuedLocal
	PriorityClassEdgePref   = PriorityClassLocalPref
	PriorityClassCloud      = PriorityClassRemote
)
