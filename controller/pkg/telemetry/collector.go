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

func (c *CombinedCollector) GetLocalState(ctx context.Context) (*LocalState, error) {
	return c.local.GetLocalState(ctx)
}

func (c *CombinedCollector) GetWANState(ctx context.Context) (*WANState, error) {
	return c.wan.GetWANState(ctx)
}

func (c *CombinedCollector) GetCachedLocalState() *LocalState {
	return c.local.GetCachedLocalState()
}

func (c *CombinedCollector) GetCachedWANState() *WANState {
	return c.wan.GetCachedWANState()
}

func (c *CombinedCollector) UpdateMetrics() {
	c.local.UpdateMetrics()
	c.wan.UpdateMetrics()
}
