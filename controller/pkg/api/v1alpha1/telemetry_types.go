package v1alpha1

import (
	"time"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

// ClusterState represents the observed state of a single cluster.
type ClusterState struct {
	ClusterID constants.ClusterID

	// Resources
	FreeCPU                 int64
	FreeMem                 int64
	TotalAllocatableCPU     int64
	TotalAllocatableMem     int64
	EffectiveAllocatableCPU int64
	EffectiveAllocatableMem int64
	NonManagedCPU           int64
	NonManagedMem           int64

	// Best node on this cluster (for local cluster: best edge node)
	BestNode BestNode

	// Pending pressure (only meaningful for local cluster)
	PendingPodsPerClass map[string]int
	TotalDemand         map[string]DemandByClass

	// Network (for remote clusters: WAN metrics; for local: zeros)
	RTTMs   int
	LossPct float64

	// Quality indicators
	IsStale               bool
	StaleDuration         time.Duration
	IsCompleteSnapshot    bool
	MeasurementConfidence float64
	Timestamp             time.Time
}

// IsLocal returns true if this is the local (controller) cluster.
func (cs *ClusterState) IsLocal() bool {
	return constants.IsLocal(cs.ClusterID)
}

// BestNode identifies the node with the most available resources in a cluster.
type BestNode struct {
	Name                    string
	FreeCPU                 int64
	FreeMem                 int64
	EffectiveAllocatableCPU int64
	EffectiveAllocatableMem int64
}

// DemandByClass aggregates the resource demand for a specific SLO class.
type DemandByClass struct {
	CPU int64
	Mem int64
}

// ---- Legacy aliases for gradual migration ----

// LocalState is kept as an alias for code that still references it.
type LocalState = ClusterState

// WANState is now embedded in ClusterState for remote clusters.
// This type is retained for the WAN probe interface.
type WANState struct {
	RTTMs         int
	LossPct       float64
	Timestamp     time.Time
	IsStale       bool
	StaleDuration time.Duration
}
