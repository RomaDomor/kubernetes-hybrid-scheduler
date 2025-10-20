package telemetry

import (
	"context"
	"time"
)

type Collector interface {
	GetLocalState(ctx context.Context) (*LocalState, error)
	GetWANState(ctx context.Context) (*WANState, error)
	GetCachedLocalState() *LocalState
	GetCachedWANState() *WANState
	UpdateMetrics()

	LockForDecision()
	UnlockForDecision()
}

type LocalState struct {
	FreeCPU             int64
	FreeMem             int64
	PendingPodsPerClass map[string]int
	Timestamp           time.Time
	BestEdgeNode        BestNode
	IsStale             bool
	StaleDuration       time.Duration
}

type BestNode struct {
	Name    string
	FreeCPU int64
	FreeMem int64
}

type WANState struct {
	RTTMs         int
	LossPct       float64
	Timestamp     time.Time
	IsStale       bool
	StaleDuration time.Duration
}

type CombinedCollector struct {
	local *LocalCollector
	wan   *WANProbe
}

func NewCombinedCollector(local *LocalCollector, wan *WANProbe) *CombinedCollector {
	return &CombinedCollector{local: local, wan: wan}
}

// Local state
func (c *CombinedCollector) GetLocalState(ctx context.Context) (*LocalState, error) {
	return c.local.GetLocalState(ctx)
}

func (c *CombinedCollector) GetCachedLocalState() *LocalState {
	return c.local.GetCachedLocalState()
}

// WAN state
func (c *CombinedCollector) GetWANState(ctx context.Context) (*WANState, error) {
	return c.wan.GetWANState(ctx)
}

func (c *CombinedCollector) GetCachedWANState() *WANState {
	return c.wan.GetCachedWANState()
}

// Metrics
func (c *CombinedCollector) UpdateMetrics() {
	c.local.UpdateMetrics()
	c.wan.UpdateMetrics()
}

// Mutex
func (c *CombinedCollector) LockForDecision() {
	c.local.LockForDecision()
}

func (c *CombinedCollector) UnlockForDecision() {
	c.local.UnlockForDecision()
}
