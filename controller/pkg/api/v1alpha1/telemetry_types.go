package v1alpha1

import "time"

// LocalState represents the observed state of the local (edge) cluster.
type LocalState struct {
	FreeCPU               int64
	FreeMem               int64
	PendingPodsPerClass   map[string]int
	TotalDemand           map[string]DemandByClass
	TotalAllocatableCPU   int64
	TotalAllocatableMem   int64
	Timestamp             time.Time
	BestEdgeNode          BestNode
	IsStale               bool
	StaleDuration         time.Duration
	IsCompleteSnapshot    bool
	MeasurementConfidence float64
}

// BestNode identifies the edge node with the most available resources.
type BestNode struct {
	Name    string
	FreeCPU int64
	FreeMem int64
}

// WANState represents the observed state of the Wide Area Network link.
type WANState struct {
	RTTMs         int
	LossPct       float64
	Timestamp     time.Time
	IsStale       bool
	StaleDuration time.Duration
}

// DemandByClass aggregates the resource demand for a specific SLO class.
type DemandByClass struct {
	CPU int64
	Mem int64
}
