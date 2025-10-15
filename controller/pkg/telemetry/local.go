package telemetry

import (
	"context"
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

	var totalAllocatableCPU, totalUsedCPU int64
	var totalAllocatableMem, totalUsedMem int64

	for _, node := range nodes {
		totalAllocatableCPU += node.Status.Allocatable.Cpu().MilliValue()
		totalAllocatableMem += node.Status.Allocatable.Memory().Value() / (1024 * 1024)
	}

	for _, nm := range nodeMetrics.Items {
		totalUsedCPU += nm.Usage.Cpu().MilliValue()
		totalUsedMem += nm.Usage.Memory().Value() / (1024 * 1024)
	}

	// Count pending pods by class using the informer cache
	// Note: PodLister does not support field selectors; filter in-memory
	pods, err := l.podLister.List(labels.SelectorFromSet(labels.Set{
		"scheduling.example.io/managed": "true",
	}))
	if err != nil {
		return l.cache, err
	}

	pendingPerClass := make(map[string]int)
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodPending && pod.Spec.NodeName == "" {
			class := pod.Annotations["slo.hybrid.io/class"]
			pendingPerClass[class]++
		}
	}

	state := &LocalState{
		FreeCPU:             totalAllocatableCPU - totalUsedCPU,
		FreeMem:             totalAllocatableMem - totalUsedMem,
		PendingPodsPerClass: pendingPerClass,
		Timestamp:           time.Now(),
	}

	l.cache = state
	return state, nil
}

func (l *LocalCollector) GetCachedLocalState() *LocalState {
	return l.cache
}
