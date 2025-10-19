package telemetry

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

var (
	headroomCPUm     = int64(getEnvInt("HEADROOM_CPU_M", 200))  // 200m
	headroomMemMi    = int64(getEnvInt("HEADROOM_MEM_MI", 256)) // 256Mi
	pessimismPctEdge = int64(getEnvInt("EDGE_PENDING_PESSIMISM_PCT", 10))
)

type LocalCollector struct {
	kubeClient    kubernetes.Interface
	metricsClient metricsv.Interface
	cache         *LocalState
	cacheMu       sync.RWMutex
	podLister     corelisters.PodLister
	nodeLister    corelisters.NodeLister
	podIndexer    cache.Indexer
}

// Add at top:
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
)

func NewLocalCollector(
	kube kubernetes.Interface,
	metrics metricsv.Interface,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
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

	return &LocalCollector{
		kubeClient:    kube,
		metricsClient: metrics,
		cache:         &LocalState{PendingPodsPerClass: make(map[string]int)},
		podLister:     podInformer.Lister(),
		nodeLister:    nodeInformer.Lister(),
		podIndexer:    podInformer.Informer().GetIndexer(),
	}
}

func (l *LocalCollector) GetLocalState(ctx context.Context) (*LocalState, error) {
	edgeSelector := labels.SelectorFromSet(labels.Set{constants.NodeRoleLabelEdge: constants.LabelValueTrue})
	nodes, err := l.nodeLister.List(edgeSelector)
	if err != nil {
		return l.cache, err
	}
	if len(nodes) == 0 {
		// No edge nodes: return a pessimistic snapshot
		st := &LocalState{
			FreeCPU:             0,
			FreeMem:             0,
			PendingPodsPerClass: make(map[string]int),
			Timestamp:           time.Now(),
			BestEdgeNode:        BestNode{},
		}
		l.setCache(st)
		return st, nil
	}

	edgeNodes := make(map[string]*corev1.Node, len(nodes))
	var totalAllocCPU, totalAllocMemMi int64
	for _, n := range nodes {
		edgeNodes[n.Name] = n
		totalAllocCPU += n.Status.Allocatable.Cpu().MilliValue()
		totalAllocMemMi += n.Status.Allocatable.Memory().Value() / (1024 * 1024)
	}

	edgeManagedObjs, err := l.podIndexer.ByIndex("edgeManaged", constants.LabelValueTrue)
	if err != nil {
		klog.V(4).Infof("Index lookup failed, falling back to full list: %v", err)
		edgeManagedObjs = []interface{}{} // Fallback to empty
	}

	// Also get all pods bound to edge nodes (may not be in edgeManaged index)
	allEdgeBoundPods := make([]*corev1.Pod, 0, len(edgeManagedObjs)+100)
	for nodeName := range edgeNodes {
		nodePodsObjs, err := l.podIndexer.ByIndex("nodeName", nodeName)
		if err != nil {
			continue
		}
		for _, obj := range nodePodsObjs {
			pod := obj.(*corev1.Pod)
			// Skip completed
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				continue
			}
			allEdgeBoundPods = append(allEdgeBoundPods, pod)
		}
	}

	// Merge edgeManaged pods (for pending pressure calculation)
	edgeManagedPods := make([]*corev1.Pod, 0, len(edgeManagedObjs))
	for _, obj := range edgeManagedObjs {
		pod := obj.(*corev1.Pod)
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			edgeManagedPods = append(edgeManagedPods, pod)
		}
	}

	// Combine for processing (deduplicate by UID)
	podsMap := make(map[string]*corev1.Pod, len(allEdgeBoundPods)+len(edgeManagedPods))
	for _, p := range allEdgeBoundPods {
		podsMap[string(p.UID)] = p
	}
	for _, p := range edgeManagedPods {
		podsMap[string(p.UID)] = p
	}

	// Convert back to slice for compatibility with existing logic
	pods := make([]*corev1.Pod, 0, len(podsMap))
	for _, p := range podsMap {
		pods = append(pods, p)
	}

	// Index pods by node and precompute per-pod requests
	podsByNode := make(map[string][]*corev1.Pod, len(edgeNodes))
	type podReq struct{ cpuM, memMi int64 }
	podReqCache := make(map[*corev1.Pod]podReq, len(pods))

	for _, p := range pods {
		// Precompute aggregate requests
		var cpuM, memMi int64
		for _, c := range p.Spec.Containers {
			req := c.Resources.Requests
			cpuM += req.Cpu().MilliValue()
			memMi += req.Memory().Value() / (1024 * 1024)
		}
		podReqCache[p] = podReq{cpuM: cpuM, memMi: memMi}

		// Index only edge-resident pods by node
		if _, isEdge := edgeNodes[p.Spec.NodeName]; isEdge {
			podsByNode[p.Spec.NodeName] = append(podsByNode[p.Spec.NodeName], p)
		}
	}

	// Compute best edge node free (per-node requests-based accounting)
	var bestName string
	var bestFreeCPU, bestFreeMemMi int64

	for nodeName, nodeObj := range edgeNodes {
		allocCPU := nodeObj.Status.Allocatable.Cpu().MilliValue()
		allocMemMi := nodeObj.Status.Allocatable.Memory().Value() / (1024 * 1024)

		var resCPU, resMemMi int64
		for _, p := range podsByNode[nodeName] {
			pr := podReqCache[p]
			resCPU += pr.cpuM
			resMemMi += pr.memMi
		}

		freeCPU := allocCPU - resCPU
		freeMemMi := allocMemMi - resMemMi

		if freeCPU > headroomCPUm {
			freeCPU -= headroomCPUm
		} else {
			freeCPU = 0
		}
		if freeMemMi > headroomMemMi {
			freeMemMi -= headroomMemMi
		} else {
			freeMemMi = 0
		}

		// Track the most-free node by CPU (primary), mem as tie-breaker
		if freeCPU > bestFreeCPU || (freeCPU == bestFreeCPU && freeMemMi > bestFreeMemMi) {
			bestFreeCPU = freeCPU
			bestFreeMemMi = freeMemMi
			bestName = nodeName
		}
	}

	// Global used from metrics-server
	// If metrics-server call fails, we still return the best-node snapshot and pending-derived pressure.
	var totalUsedCPU, totalUsedMemMi int64
	if l.metricsClient != nil {
		if nmList, err := l.metricsClient.MetricsV1beta1().NodeMetricses().
			List(ctx, metav1.ListOptions{LabelSelector: constants.NodeRoleLabelEdge + "=" + constants.LabelValueTrue}); err == nil {
			for _, nm := range nmList.Items {
				if _, ok := edgeNodes[nm.Name]; ok {
					totalUsedCPU += nm.Usage.Cpu().MilliValue()
					totalUsedMemMi += nm.Usage.Memory().Value() / (1024 * 1024)
				}
			}
		} else {
			// Do not fail the snapshot on metrics errors
			klog.V(4).Infof("Node metrics fetch failed: %v (using previous global free)", err)
		}
	}

	// Pending pressure and managed per-class counts
	var pendingCPU, pendingMemMi int64
	pendingPerClass := make(map[string]int, 8)

	for _, p := range pods {
		// For managed per-class count (your queue model)
		if p.Labels[constants.LabelManaged] == constants.LabelValueTrue &&
			p.Status.Phase == corev1.PodPending && p.Spec.NodeName == "" {
			if class := p.Annotations[constants.AnnotationSLOClass]; class != "" {
				pendingPerClass[class]++
			}
		}

		// Determine if this pod impacts edge capacity as "pending pressure"
		boundToEdge := p.Spec.NodeName != "" && edgeNodes[p.Spec.NodeName] != nil
		unboundEdgeIntended := p.Spec.NodeName == "" && wantsEdge(p)

		if !(boundToEdge || unboundEdgeIntended) {
			continue
		}

		consume := p.Status.Phase != corev1.PodRunning
		if !consume {
			// Running: consume if any container not ready
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

		pr := podReqCache[p]
		cpuM := pr.cpuM
		memMi := pr.memMi
		if unboundEdgeIntended && pessimismPctEdge > 0 {
			// add small pessimism for unbound edge-intended pods
			cpuM += (cpuM * pessimismPctEdge) / 100
			memMi += (memMi * pessimismPctEdge) / 100
		}
		pendingCPU += cpuM
		pendingMemMi += memMi
	}

	// Global free with headroom (keep for telemetry summary)
	freeCPU := (totalAllocCPU - totalUsedCPU) - pendingCPU
	freeMemMi := (totalAllocMemMi - totalUsedMemMi) - pendingMemMi
	if freeCPU > headroomCPUm {
		freeCPU -= headroomCPUm
	} else {
		freeCPU = 0
	}
	if freeMemMi > headroomMemMi {
		freeMemMi -= headroomMemMi
	} else {
		freeMemMi = 0
	}

	st := &LocalState{
		FreeCPU:             freeCPU,
		FreeMem:             freeMemMi,
		PendingPodsPerClass: pendingPerClass,
		Timestamp:           time.Now(),
		BestEdgeNode:        BestNode{Name: bestName, FreeCPU: bestFreeCPU, FreeMem: bestFreeMemMi},
		IsStale:             false,
		StaleDuration:       0,
	}

	l.setCache(st)
	return st, nil
}

func (l *LocalCollector) GetCachedLocalState() *LocalState {
	l.cacheMu.RLock()
	defer l.cacheMu.RUnlock()

	if l.cache == nil {
		return &LocalState{PendingPodsPerClass: make(map[string]int), IsStale: true}
	}

	staleDuration := time.Since(l.cache.Timestamp)
	isStale := staleDuration > 20*time.Second

	state := *l.cache // Copy
	state.IsStale = isStale
	state.StaleDuration = staleDuration

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

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

// wantsEdge returns true if the pod explicitly targets edge via
// nodeSelector node.role/edge=true or required nodeAffinity.
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
						// ignore NotIn, DoesNotExist, Gt, Lt here
					}
				}
			}
		}
	}
	return false
}

// Test helpers
type LocalCollectorForTest struct {
	Cache *LocalState
}

func (l *LocalCollectorForTest) GetCachedLocalState() *LocalState {
	if l.Cache == nil {
		return &LocalState{PendingPodsPerClass: map[string]int{}, IsStale: true}
	}
	staleDuration := time.Since(l.Cache.Timestamp)
	state := *l.Cache
	state.IsStale = staleDuration > 60*time.Second
	state.StaleDuration = staleDuration
	return &state
}
