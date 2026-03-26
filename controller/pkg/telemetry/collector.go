package telemetry

import (
	"context"

	"kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

// Collector provides cluster state for all clusters in the federation.
//
//go:generate mockery --name=Collector --output=../../gen/mocks
type Collector interface {
	// GetClusterState returns fresh state for a specific cluster.
	GetClusterState(ctx context.Context, id constants.ClusterID) (*v1alpha1.ClusterState, error)

	// GetCachedClusterState returns the last known state.
	GetCachedClusterState(id constants.ClusterID) *v1alpha1.ClusterState

	// GetAllClusterIDs returns the IDs of all known clusters.
	GetAllClusterIDs() []constants.ClusterID

	// UpdateMetrics exports Prometheus metrics for all clusters.
	UpdateMetrics()

	// LockForDecision / UnlockForDecision serialize decision-making.
	LockForDecision()
	UnlockForDecision()
}

// CombinedCollector aggregates local and remote cluster state.
type CombinedCollector struct {
	local      *LocalCollector
	wanProbes  map[constants.ClusterID]*WANProbe
	clusterIDs []constants.ClusterID
}

// NewCombinedCollector creates a collector for one local + N remote clusters.
func NewCombinedCollector(
	local *LocalCollector,
	wanProbes map[constants.ClusterID]*WANProbe,
) *CombinedCollector {
	ids := []constants.ClusterID{constants.LocalCluster}
	for id := range wanProbes {
		ids = append(ids, id)
	}
	return &CombinedCollector{
		local:      local,
		wanProbes:  wanProbes,
		clusterIDs: ids,
	}
}

func (c *CombinedCollector) GetAllClusterIDs() []constants.ClusterID {
	return c.clusterIDs
}

func (c *CombinedCollector) GetClusterState(ctx context.Context, id constants.ClusterID) (*v1alpha1.ClusterState, error) {
	if constants.IsLocal(id) {
		return c.local.GetLocalState(ctx)
	}
	// For remote clusters, build ClusterState from WAN probe
	probe, ok := c.wanProbes[id]
	if !ok {
		return &v1alpha1.ClusterState{
			ClusterID:             id,
			IsStale:               true,
			MeasurementConfidence: 0,
			PendingPodsPerClass:   make(map[string]int),
			TotalDemand:           make(map[string]v1alpha1.DemandByClass),
		}, nil
	}
	wan, err := probe.GetWANState(ctx)
	if err != nil {
		return nil, err
	}
	state := &v1alpha1.ClusterState{
		ClusterID:             id,
		RTTMs:                 wan.RTTMs,
		LossPct:               wan.LossPct,
		IsStale:               wan.IsStale,
		StaleDuration:         wan.StaleDuration,
		Timestamp:             wan.Timestamp,
		IsCompleteSnapshot:    !wan.IsStale,
		MeasurementConfidence: 1.0,
		PendingPodsPerClass:   make(map[string]int),
		TotalDemand:           make(map[string]v1alpha1.DemandByClass),
	}
	c.enrichWithVirtualNodeResources(id, state)
	return state, nil
}

func (c *CombinedCollector) GetCachedClusterState(id constants.ClusterID) *v1alpha1.ClusterState {
	if constants.IsLocal(id) {
		return c.local.GetCachedLocalState()
	}
	probe, ok := c.wanProbes[id]
	if !ok {
		return &v1alpha1.ClusterState{
			ClusterID:           id,
			IsStale:             true,
			PendingPodsPerClass: make(map[string]int),
			TotalDemand:         make(map[string]v1alpha1.DemandByClass),
		}
	}
	wan := probe.GetCachedWANState()
	state := &v1alpha1.ClusterState{
		ClusterID:             id,
		RTTMs:                 wan.RTTMs,
		LossPct:               wan.LossPct,
		IsStale:               wan.IsStale,
		StaleDuration:         wan.StaleDuration,
		Timestamp:             wan.Timestamp,
		IsCompleteSnapshot:    !wan.IsStale,
		MeasurementConfidence: 1.0,
		PendingPodsPerClass:   make(map[string]int),
		TotalDemand:           make(map[string]v1alpha1.DemandByClass),
	}
	c.enrichWithVirtualNodeResources(id, state)
	return state
}

// enrichWithVirtualNodeResources populates resource fields on a remote
// ClusterState from the corresponding Liqo virtual node.
func (c *CombinedCollector) enrichWithVirtualNodeResources(id constants.ClusterID, state *v1alpha1.ClusterState) {
	freeCPU, freeMem, allocCPU, allocMem, found := c.local.GetVirtualNodeResources(id)
	if !found {
		return
	}
	state.FreeCPU = freeCPU
	state.FreeMem = freeMem
	state.TotalAllocatableCPU = allocCPU
	state.TotalAllocatableMem = allocMem
	state.EffectiveAllocatableCPU = allocCPU
	state.EffectiveAllocatableMem = allocMem
	state.BestNode = v1alpha1.BestNode{
		Name:                    string(id),
		FreeCPU:                 freeCPU,
		FreeMem:                 freeMem,
		EffectiveAllocatableCPU: allocCPU,
		EffectiveAllocatableMem: allocMem,
	}
}

func (c *CombinedCollector) UpdateMetrics() {
	c.local.UpdateMetrics()
	for _, probe := range c.wanProbes {
		probe.UpdateMetrics()
	}
}

func (c *CombinedCollector) LockForDecision()   { c.local.LockForDecision() }
func (c *CombinedCollector) UnlockForDecision() { c.local.UnlockForDecision() }
