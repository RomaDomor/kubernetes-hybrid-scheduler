package telemetry

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

var (
	headroomCPUm  = int64(getEnvInt("HEADROOM_CPU_M", 200))  // 200m default
	headroomMemMi = int64(getEnvInt("HEADROOM_MEM_MI", 256)) // 256Mi default
)

type LocalCollector struct {
	kubeClient    kubernetes.Interface
	metricsClient metricsv.Interface
	cache         *LocalState
	cacheMu       sync.RWMutex // ADD THIS
	podLister     corelisters.PodLister
	nodeLister    corelisters.NodeLister
}

func NewLocalCollector(
	kube kubernetes.Interface,
	metrics metricsv.Interface,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
) *LocalCollector {
	return &LocalCollector{
		kubeClient:    kube,
		metricsClient: metrics,
		cache:         &LocalState{PendingPodsPerClass: make(map[string]int)},
		podLister:     podInformer.Lister(),
		nodeLister:    nodeInformer.Lister(),
	}
}

func (l *LocalCollector) GetLocalState(ctx context.Context) (*LocalState, error) {
	// Get node metrics
	nodeMetrics, err := l.metricsClient.MetricsV1beta1().NodeMetricses().List(ctx, metav1.ListOptions{
		LabelSelector: "node.role/edge=true",
	})
	if err != nil {
		klog.Warningf("Failed to fetch node metrics: %v", err)
		return l.cache, err
	}

	// Use node lister cache for allocatable
	edgeSelector := labels.SelectorFromSet(labels.Set{"node.role/edge": "true"})
	nodes, err := l.nodeLister.List(edgeSelector)
	if err != nil {
		return l.cache, err
	}

	// Build a quick lookup for edge nodes
	edgeNodes := make(map[string]struct{}, len(nodes))
	var totalAllocatableCPU, totalAllocatableMem int64
	for _, node := range nodes {
		edgeNodes[node.Name] = struct{}{}
		totalAllocatableCPU += node.Status.Allocatable.Cpu().MilliValue()
		totalAllocatableMem += node.Status.Allocatable.Memory().Value() / (1024 * 1024)
	}

	// Sum used only for edge nodes
	var totalUsedCPU, totalUsedMem int64
	for _, nm := range nodeMetrics.Items {
		if _, ok := edgeNodes[nm.Name]; !ok {
			continue
		}
		totalUsedCPU += nm.Usage.Cpu().MilliValue()
		totalUsedMem += nm.Usage.Memory().Value() / (1024 * 1024)
	}

	// Count pending managed pods by class (for scheduling model) and
	// include all pending/not-ready pods (managed and non-managed) in capacity pressure
	var pendingCPU, pendingMem int64
	pods, err := l.podLister.List(labels.Everything())
	if err != nil {
		return l.cache, err
	}

	pendingPerClass := make(map[string]int)
	for _, pod := range pods {
		// managed class counts (for your Edge queue model)
		if pod.Labels["scheduling.hybrid.io/managed"] == "true" {
			class := pod.Annotations["slo.hybrid.io/class"]
			if pod.Status.Phase == corev1.PodPending && pod.Spec.NodeName == "" {
				pendingPerClass[class]++
			}
		}

		// Determine if this pod should be counted against edge capacity
		// Case A: already bound to an edge node
		boundToEdge := isEdgeNode(pod.Spec.NodeName, edgeNodes)

		// Case B: unbound but explicitly targets edge via selector/affinity
		unboundEdgeIntended := (pod.Spec.NodeName == "") && wantsEdge(pod)

		// Optional (uncomment to be more conservative):
		// Treat managed pods without explicit selector as edge-intended before binding
		// if pod.Spec.NodeName == "" && pod.Labels["scheduling.hybrid.io/managed"] == "true" {
		// 	unboundEdgeIntended = true
		// }

		// Decide if we consume this pod's requests (Pending or Running-but-not-ready)
		consume := false
		// If pod is not Running, consume
		if pod.Status.Phase != corev1.PodRunning {
			consume = true
		} else {
			// optionally, if Running but containers not ready, still consume
			// This is optional:
			allReady := true
			for _, cs := range pod.Status.ContainerStatuses {
				if !cs.Ready {
					allReady = false
					break
				}
			}
			if !allReady {
				consume = true
			}
		}

		// Only subtract if it impacts edge capacity
		if consume && (boundToEdge || unboundEdgeIntended) {
			// sum requests of all containers
			var podCPU, podMemMi int64
			for _, c := range pod.Spec.Containers {
				req := c.Resources.Requests
				podCPU += req.Cpu().MilliValue()
				podMemMi += req.Memory().Value() / (1024 * 1024)
			}
			// Add small pessimism for unbound edge-intended pods (+10%)
			if unboundEdgeIntended {
				podCPU = podCPU + (podCPU / 10)
				podMemMi = podMemMi + (podMemMi / 10)
			}
			pendingCPU += podCPU
			pendingMem += podMemMi
		}
	}

	freeCPU := (totalAllocatableCPU - totalUsedCPU) - pendingCPU
	freeMem := (totalAllocatableMem - totalUsedMem) - pendingMem
	if freeCPU > headroomCPUm {
		freeCPU -= headroomCPUm
	} else {
		freeCPU = 0
	}
	if freeMem > headroomMemMi {
		freeMem -= headroomMemMi
	} else {
		freeMem = 0
	}

	state := &LocalState{
		FreeCPU:             freeCPU,
		FreeMem:             freeMem,
		PendingPodsPerClass: pendingPerClass,
		Timestamp:           time.Now(),
	}

	l.cacheMu.Lock()
	l.cache = state
	l.cacheMu.Unlock()

	return state, nil
}

// Helpers to determine edge targeting
func isEdgeNode(nodeName string, edgeNodes map[string]struct{}) bool {
	if nodeName == "" {
		return false
	}
	_, ok := edgeNodes[nodeName]
	return ok
}

func wantsEdge(pod *corev1.Pod) bool {
	// nodeSelector
	if pod.Spec.NodeSelector != nil {
		if v, ok := pod.Spec.NodeSelector["node.role/edge"]; ok && v == "true" {
			return true
		}
	}
	// nodeAffinity (requiredDuringSchedulingIgnoredDuringExecution)
	if pod.Spec.Affinity != nil && pod.Spec.Affinity.NodeAffinity != nil {
		req := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if req != nil {
			for _, term := range req.NodeSelectorTerms {
				for _, expr := range term.MatchExpressions {
					if expr.Key == "node.role/edge" {
						switch expr.Operator {
						case corev1.NodeSelectorOpIn:
							for _, v := range expr.Values {
								if v == "true" {
									return true
								}
							}
						case corev1.NodeSelectorOpExists:
							return true
						}
					}
				}
			}
		}
	}
	return false
}
func (l *LocalCollector) GetCachedLocalState() *LocalState {
	l.cacheMu.RLock()
	defer l.cacheMu.RUnlock()
	return l.cache
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}
