package constants

// PriorityTier represents the scheduler's internal priority grouping
type PriorityTier int

const (
	TierHighest PriorityTier = 1 // latency, interactive
	TierNormal  PriorityTier = 2 // streaming, throughput
	TierLowest  PriorityTier = 3 // batch
)

// GetTierForClass maps SLO class to priority tier
func GetTierForClass(class string) PriorityTier {
	switch class {
	case "latency", "interactive":
		return TierHighest
	case "streaming", "throughput":
		return TierNormal
	case "batch":
		return TierLowest
	default:
		return TierNormal
	}
}

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
