package constants

// This file centralizes SLO-related constants, such as valid workload classes.

// ValidSLOClasses defines the whitelist of workload classes the controller understands.
var ValidSLOClasses = map[string]bool{
	"latency":     true,
	"throughput":  true,
	"batch":       true,
	"interactive": true,
	"streaming":   true,
}

// DefaultSLOClass is the class assigned to a workload if its class is unknown or invalid.
const DefaultSLOClass = "batch"
