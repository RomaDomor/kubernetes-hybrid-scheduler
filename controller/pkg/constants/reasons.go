package constants

// This file contains constants for scheduling decision reasons.
const (
	// Edge placement reasons
	ReasonEdgeFeasibleOnly     = "edge_feasible_only"
	ReasonEdgePreferred        = "edge_preferred"
	ReasonEdgeViolationControl = "edge_violation_control"
	ReasonEdgeBestEffort       = "edge_best_effort"
	ReasonOffloadDisabled      = "offload_disabled"
	ReasonWANUnusable          = "wan_unusable"
	ReasonWANCircuitBreaker    = "wan_circuit_breaker"

	// Cloud placement reasons
	ReasonCloudFeasibleOnly       = "cloud_feasible_only"
	ReasonCloudFaster             = "cloud_faster"
	ReasonCloudViolationControl   = "cloud_violation_control"
	ReasonCloudBestEffort         = "cloud_best_effort"
	ReasonTelemetryCircuitBreaker = "telemetry_circuit_breaker"
	ReasonLowMeasurementConf      = "low_measurement_confidence"

	// Infeasibility reasons (internal to decision engine)
	ReasonInfeasibleResourcesHard = "infeasible_resources_hard" // Not enough capacity even if all managed pods finish
	ReasonInfeasibleResourcesSoft = "infeasible_resources_soft" // Temporarily occupied with other managed pods
	ReasonInfeasibleTime          = "infeasible_time"
)

// Priority class names for Kubernetes
const (
	PriorityClassQueuedEdge = "queued-edge-workload"
	PriorityClassEdgePref   = "edge-preferred"
	PriorityClassBestEffort = "edge-best-effort"
	PriorityClassCloud      = "cloud-workload"
)
