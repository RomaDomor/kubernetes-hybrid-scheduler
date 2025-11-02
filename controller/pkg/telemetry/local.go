package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	corev1 "k8s.io/api/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

var (
	localFreeCPUGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_edge_free_cpu_millicores",
		Help: "Free CPU on edge nodes in millicores",
	})
	localFreeMemGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_edge_free_memory_mebibytes",
		Help: "Free memory on edge nodes in MiB",
	})
	localStaleGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_local_stale_seconds",
		Help: "Seconds since last successful local telemetry refresh",
	})
	localComputeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "scheduler_local_compute_duration_seconds",
		Help:    "Time to compute local state",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0},
	})
	localIncompleteSnapshots = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_local_incomplete_snapshots_total",
		Help: "Number of incomplete local state snapshots due to timeout",
	})
)

type LocalCollector struct {
	kubeClient       kubernetes.Interface
	cache            *LocalState
	cacheMu          sync.RWMutex
	podLister        corelisters.PodLister
	nodeLister       corelisters.NodeLister
	podIndexer       cache.Indexer
	pessimismPctEdge int64
	decisionMu       sync.Mutex

	nodeCache      map[string]*nodeSnapshot
	nodeCacheMu    sync.RWMutex
	lastFullUpdate time.Time
}

type nodeSnapshot struct {
	allocatableCPU int64
	allocatableMem int64
	lastSeen       time.Time
}

type DemandByClass struct {
	CPU int64
	Mem int64
}

func NewLocalCollector(
	kube kubernetes.Interface,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
	pessimismPctEdge int64,
) *LocalCollector {
	err := podInformer.Informer().AddIndexers(cache.Indexers{
		"nodeName": func(obj interface{}) ([]string, error) {
			pod := obj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return []string{}, nil
			}
			return []string{pod.Spec.NodeName}, nil
		},
		"edgeManaged": func(obj interface{}) ([]string, error) {
			pod := obj.(*corev1.Pod)
			if pod.Labels[constants.LabelManaged] == constants.LabelValueTrue &&
				(pod.Spec.NodeSelector[constants.NodeRoleLabelEdge] == constants.LabelValueTrue || wantsEdge(pod)) {
				return []string{constants.LabelValueTrue}, nil
			}
			return []string{}, nil
		},
	})
	if err != nil {
		klog.Errorf("Failed to add custom indexers for podInformer: %v", err)
		return nil
	}

	lc := &LocalCollector{
		kubeClient:       kube,
		cache:            &LocalState{PendingPodsPerClass: make(map[string]int), TotalDemand: make(map[string]DemandByClass)},
		podLister:        podInformer.Lister(),
		nodeLister:       nodeInformer.Lister(),
		podIndexer:       podInformer.Informer().GetIndexer(),
		pessimismPctEdge: pessimismPctEdge,
		nodeCache:        make(map[string]*nodeSnapshot),
		lastFullUpdate:   time.Now(),
	}

	// Register event handlers for incremental updates
	_, err = podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { lc.triggerIncremental() },
		UpdateFunc: func(old, new interface{}) { lc.triggerIncremental() },
		DeleteFunc: func(obj interface{}) { lc.triggerIncremental() },
	})
	if err != nil {
		klog.Errorf("Failed to add event handler for podInformer: %v", err)
		return nil
	}

	_, err = nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { lc.updateNodeCache(obj.(*corev1.Node)) },
		UpdateFunc: func(old, new interface{}) { lc.updateNodeCache(new.(*corev1.Node)) },
		DeleteFunc: func(obj interface{}) { lc.removeNodeCache(obj.(*corev1.Node).Name) },
	})
	if err != nil {
		klog.Errorf("Failed to add event handler for nodeInformer: %v", err)
		return nil
	}

	return lc
}

func (l *LocalCollector) triggerIncremental() {
	// Debounced: actual update happens in background loop
}

func (l *LocalCollector) updateNodeCache(node *corev1.Node) {
	if node.Labels[constants.NodeRoleLabelEdge] != constants.LabelValueTrue {
		return
	}

	l.nodeCacheMu.Lock()
	defer l.nodeCacheMu.Unlock()

	l.nodeCache[node.Name] = &nodeSnapshot{
		allocatableCPU: node.Status.Allocatable.Cpu().MilliValue(),
		allocatableMem: node.Status.Allocatable.Memory().Value() / (1024 * 1024),
		lastSeen:       time.Now(),
	}
}

func (l *LocalCollector) removeNodeCache(nodeName string) {
	l.nodeCacheMu.Lock()
	defer l.nodeCacheMu.Unlock()
	delete(l.nodeCache, nodeName)
}

// GetLocalState computes state with timeout awareness
func (l *LocalCollector) GetLocalState(ctx context.Context) (*LocalState, error) {
	start := time.Now()
	defer func() {
		localComputeDuration.Observe(time.Since(start).Seconds())
	}()

	// Fast path: use node cache
	l.nodeCacheMu.RLock()
	nodeSnapshots := make(map[string]*nodeSnapshot, len(l.nodeCache))
	for k, v := range l.nodeCache {
		nodeSnapshots[k] = v
	}
	l.nodeCacheMu.RUnlock()

	if len(nodeSnapshots) == 0 {
		st := &LocalState{
			FreeCPU:               0,
			FreeMem:               0,
			PendingPodsPerClass:   make(map[string]int),
			TotalDemand:           make(map[string]DemandByClass),
			TotalAllocatableCPU:   0,
			TotalAllocatableMem:   0,
			Timestamp:             time.Now(),
			BestEdgeNode:          BestNode{},
			IsCompleteSnapshot:    true,
			MeasurementConfidence: 1.0,
		}
		l.setCache(st)
		return st, nil
	}

	// Compute with context deadline awareness
	resultCh := make(chan *LocalState, 1)
	errCh := make(chan error, 1)

	go func() {
		state, err := l.computeState(nodeSnapshots)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- state
	}()

	select {
	case <-ctx.Done():
		// Timeout: return stale cache with low confidence
		klog.V(3).Infof("GetLocalState timed out, returning stale cache")
		localIncompleteSnapshots.Inc()

		cached := l.GetCachedLocalState()
		cached.IsCompleteSnapshot = false
		cached.MeasurementConfidence = 0.3 // Low confidence
		return cached, nil

	case err := <-errCh:
		return l.cache, err

	case state := <-resultCh:
		state.IsCompleteSnapshot = true
		state.MeasurementConfidence = 1.0
		l.setCache(state)
		return state, nil
	}
}

func (l *LocalCollector) computeState(nodeSnapshots map[string]*nodeSnapshot) (*LocalState, error) {
	var totalAllocCPU, totalAllocMem int64
	edgeNodes := make(map[string]bool, len(nodeSnapshots))

	for nodeName, snap := range nodeSnapshots {
		edgeNodes[nodeName] = true
		totalAllocCPU += snap.allocatableCPU
		totalAllocMem += snap.allocatableMem
	}

	// Fast pod lookup by index
	allEdgeBoundPods := make([]*corev1.Pod, 0, 100)
	for nodeName := range edgeNodes {
		nodePodsObjs, err := l.podIndexer.ByIndex("nodeName", nodeName)
		if err != nil {
			continue
		}
		for _, obj := range nodePodsObjs {
			pod := obj.(*corev1.Pod)
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				continue
			}
			allEdgeBoundPods = append(allEdgeBoundPods, pod)
		}
	}

	edgeManagedObjs, _ := l.podIndexer.ByIndex("edgeManaged", constants.LabelValueTrue)
	edgeManagedPods := make([]*corev1.Pod, 0, len(edgeManagedObjs))
	for _, obj := range edgeManagedObjs {
		pod := obj.(*corev1.Pod)
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			edgeManagedPods = append(edgeManagedPods, pod)
		}
	}

	// Deduplicate
	podsMap := make(map[string]*corev1.Pod, len(allEdgeBoundPods)+len(edgeManagedPods))
	for _, p := range allEdgeBoundPods {
		podsMap[string(p.UID)] = p
	}
	for _, p := range edgeManagedPods {
		podsMap[string(p.UID)] = p
	}

	pods := make([]*corev1.Pod, 0, len(podsMap))
	for _, p := range podsMap {
		pods = append(pods, p)
	}

	// Precompute pod requests
	type podReq struct{ cpuM, memMi int64 }
	podReqCache := make(map[*corev1.Pod]podReq, len(pods))
	podsByNode := make(map[string][]*corev1.Pod, len(edgeNodes))

	for _, p := range pods {
		var cpuM, memMi int64
		for _, c := range p.Spec.Containers {
			req := c.Resources.Requests
			cpuM += req.Cpu().MilliValue()
			memMi += req.Memory().Value() / (1024 * 1024)
		}
		podReqCache[p] = podReq{cpuM: cpuM, memMi: memMi}

		if edgeNodes[p.Spec.NodeName] {
			podsByNode[p.Spec.NodeName] = append(podsByNode[p.Spec.NodeName], p)
		}
	}

	// Compute per-node free resources
	var bestName string
	var bestFreeCPU, bestFreeMemMi int64
	var sumFreeCPU, sumFreeMemMi int64

	for nodeName, snap := range nodeSnapshots {
		var resCPU, resMemMi int64
		for _, p := range podsByNode[nodeName] {
			pr := podReqCache[p]
			resCPU += pr.cpuM
			resMemMi += pr.memMi
		}

		freeCPU := snap.allocatableCPU - resCPU
		freeMemMi := snap.allocatableMem - resMemMi

		sumFreeCPU += freeCPU
		sumFreeMemMi += freeMemMi

		if freeCPU > bestFreeCPU || (freeCPU == bestFreeCPU && freeMemMi > bestFreeMemMi) {
			bestFreeCPU = freeCPU
			bestFreeMemMi = freeMemMi
			bestName = nodeName
		}
	}

	// Pending pressure
	var pendingCPU, pendingMemMi int64
	pendingPerClass := make(map[string]int, 8)
	totalDemand := make(map[string]DemandByClass, 8)

	for _, p := range pods {
		if p.Labels[constants.LabelManaged] == constants.LabelValueTrue &&
			p.Status.Phase == corev1.PodPending && p.Spec.NodeName == "" {
			if class := p.Annotations[constants.AnnotationSLOClass]; class != "" {
				pendingPerClass[class]++
			}
		}

		boundToEdge := p.Spec.NodeName != "" && edgeNodes[p.Spec.NodeName]
		unboundEdgeIntended := p.Spec.NodeName == "" && wantsEdge(p)

		if boundToEdge || unboundEdgeIntended {
			pr := podReqCache[p]
			class := p.Annotations[constants.AnnotationSLOClass]
			if class == "" {
				class = constants.DefaultSLOClass
			}

			demand := totalDemand[class]
			demand.CPU += pr.cpuM
			demand.Mem += pr.memMi
			totalDemand[class] = demand

			consume := p.Status.Phase != corev1.PodRunning
			if !consume {
				allReady := true
				for _, cs := range p.Status.ContainerStatuses {
					if !cs.Ready {
						allReady = false
						break
					}
				}
				consume = !allReady
			}
			if !consume {
				continue
			}

			cpuM := pr.cpuM
			memMi := pr.memMi
			if unboundEdgeIntended && l.pessimismPctEdge > 0 {
				cpuM += (cpuM * l.pessimismPctEdge) / 100
				memMi += (memMi * l.pessimismPctEdge) / 100
			}
			pendingCPU += cpuM
			pendingMemMi += memMi
		}
	}

	freeCPU := sumFreeCPU - pendingCPU
	if freeCPU < 0 {
		freeCPU = 0
	}
	freeMemMi := sumFreeMemMi - pendingMemMi
	if freeMemMi < 0 {
		freeMemMi = 0
	}

	return &LocalState{
		FreeCPU:             freeCPU,
		FreeMem:             freeMemMi,
		PendingPodsPerClass: pendingPerClass,
		TotalDemand:         totalDemand,
		TotalAllocatableCPU: totalAllocCPU,
		TotalAllocatableMem: totalAllocMem,
		Timestamp:           time.Now(),
		BestEdgeNode:        BestNode{Name: bestName, FreeCPU: bestFreeCPU, FreeMem: bestFreeMemMi},
		IsStale:             false,
		StaleDuration:       0,
	}, nil
}

func (l *LocalCollector) GetCachedLocalState() *LocalState {
	l.cacheMu.RLock()
	defer l.cacheMu.RUnlock()

	if l.cache == nil {
		return &LocalState{
			PendingPodsPerClass:   make(map[string]int),
			TotalDemand:           make(map[string]DemandByClass),
			IsStale:               true,
			IsCompleteSnapshot:    false,
			MeasurementConfidence: 0.0,
		}
	}

	staleDuration := time.Since(l.cache.Timestamp)
	isStale := staleDuration > 20*time.Second

	// Compute confidence based on age
	confidence := l.cache.MeasurementConfidence
	if staleDuration > 30*time.Second {
		confidence = 0.0
	} else if staleDuration > 10*time.Second {
		confidence *= 1.0 - staleDuration.Seconds()/30.0
	}

	state := *l.cache
	state.IsStale = isStale
	state.StaleDuration = staleDuration
	state.MeasurementConfidence = confidence

	return &state
}

func (l *LocalCollector) UpdateMetrics() {
	state := l.GetCachedLocalState()
	localFreeCPUGauge.Set(float64(state.FreeCPU))
	localFreeMemGauge.Set(float64(state.FreeMem))
	localStaleGauge.Set(state.StaleDuration.Seconds())
}

func (l *LocalCollector) setCache(state *LocalState) {
	l.cacheMu.Lock()
	l.cache = state
	l.cacheMu.Unlock()
}

func wantsEdge(pod *corev1.Pod) bool {
	if pod.Spec.NodeSelector != nil {
		if v, ok := pod.Spec.NodeSelector[constants.NodeRoleLabelEdge]; ok && v == constants.LabelValueTrue {
			return true
		}
	}
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.NodeAffinity != nil {
		req := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if req != nil {
			for _, term := range req.NodeSelectorTerms {
				for _, expr := range term.MatchExpressions {
					if expr.Key != constants.NodeRoleLabelEdge {
						continue
					}
					switch expr.Operator {
					case corev1.NodeSelectorOpIn:
						for _, v := range expr.Values {
							if v == constants.LabelValueTrue {
								return true
							}
						}
					case corev1.NodeSelectorOpExists:
						return true
					default:
					}
				}
			}
		}
	}
	return false
}

func (l *LocalCollector) LockForDecision()   { l.decisionMu.Lock() }
func (l *LocalCollector) UnlockForDecision() { l.decisionMu.Unlock() }

type LocalCollectorForTest struct {
	Cache *LocalState
}

func (l *LocalCollectorForTest) GetCachedLocalState() *LocalState {
	if l.Cache == nil {
		return &LocalState{PendingPodsPerClass: map[string]int{}, TotalDemand: map[string]DemandByClass{}, IsStale: true}
	}
	staleDuration := time.Since(l.Cache.Timestamp)
	state := *l.Cache
	state.IsStale = staleDuration > 60*time.Second
	state.StaleDuration = staleDuration
	return &state
}
