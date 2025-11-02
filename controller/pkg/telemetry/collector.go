package telemetry

import (
	"context"
	"kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
)

type Collector interface {
	GetLocalState(ctx context.Context) (*v1alpha1.LocalState, error)
	GetWANState(ctx context.Context) (*v1alpha1.WANState, error)
	GetCachedLocalState() *v1alpha1.LocalState
	GetCachedWANState() *v1alpha1.WANState
	UpdateMetrics()
	LockForDecision()
	UnlockForDecision()
}

type CombinedCollector struct {
	local *LocalCollector
	wan   *WANProbe
}

func NewCombinedCollector(local *LocalCollector, wan *WANProbe) *CombinedCollector {
	return &CombinedCollector{local: local, wan: wan}
}

func (c *CombinedCollector) GetLocalState(ctx context.Context) (*v1alpha1.LocalState, error) {
	return c.local.GetLocalState(ctx)
}

func (c *CombinedCollector) GetCachedLocalState() *v1alpha1.LocalState {
	return c.local.GetCachedLocalState()
}

func (c *CombinedCollector) GetWANState(ctx context.Context) (*v1alpha1.WANState, error) {
	return c.wan.GetWANState(ctx)
}

func (c *CombinedCollector) GetCachedWANState() *v1alpha1.WANState {
	return c.wan.GetCachedWANState()
}

func (c *CombinedCollector) UpdateMetrics() {
	c.local.UpdateMetrics()
	c.wan.UpdateMetrics()
}

func (c *CombinedCollector) LockForDecision() {
	c.local.LockForDecision()
}

func (c *CombinedCollector) UnlockForDecision() {
	c.local.UnlockForDecision()
}
