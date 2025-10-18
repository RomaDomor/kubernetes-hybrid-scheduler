package telemetry

import (
	"context"
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

	// Count pending pods by class using the informer cache
	var pendingCPU, pendingMem int64
	pods, err := l.podLister.List(labels.SelectorFromSet(labels.Set{
		"scheduling.hybrid.io/managed": "true",
	}))
	if err != nil {
		return l.cache, err
	}

	pendingPerClass := make(map[string]int)
	for _, pod := range pods {
		class := pod.Annotations["slo.hybrid.io/class"]

		// Count pending per class
		if pod.Status.Phase == corev1.PodPending && pod.Spec.NodeName == "" {
			pendingPerClass[class]++
		}

		// Subtract requests from pods that are not Running on edge yet
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

		if consume {
			// If pod is already bound to an edge node, or not bound yet (NodeName empty), we consider it against edge queue
			_, ok := edgeNodes[pod.Spec.NodeName]
			if pod.Spec.NodeName == "" || (pod.Spec.NodeName != "" && ok) {
				if len(pod.Spec.Containers) > 0 {
					req := pod.Spec.Containers[0].Resources.Requests
					pendingCPU += req.Cpu().MilliValue()
					pendingMem += req.Memory().Value() / (1024 * 1024)
				}
			}
		}
	}

	freeCPU := (totalAllocatableCPU - totalUsedCPU) - pendingCPU
	freeMem := (totalAllocatableMem - totalUsedMem) - pendingMem
	if freeCPU < 0 {
		freeCPU = 0
	}
	if freeMem < 0 {
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

func (l *LocalCollector) GetCachedLocalState() *LocalState {
	l.cacheMu.RLock()
	defer l.cacheMu.RUnlock()
	return l.cache
}
