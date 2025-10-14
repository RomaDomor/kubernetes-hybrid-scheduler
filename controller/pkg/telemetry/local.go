package telemetry

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

type LocalCollector struct {
	kubeClient    kubernetes.Interface
	metricsClient metricsv.Interface
	cache         *LocalState
}

func NewLocalCollector(kube kubernetes.Interface, metrics metricsv.Interface) *LocalCollector {
	return &LocalCollector{
		kubeClient:    kube,
		metricsClient: metrics,
		cache:         &LocalState{PendingPodsPerClass: make(map[string]int)},
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

	// Get nodes to compute allocatable
	nodes, err := l.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "node.role/edge=true",
	})
	if err != nil {
		return l.cache, err
	}

	var totalAllocatableCPU, totalUsedCPU int64
	var totalAllocatableMem, totalUsedMem int64

	for _, node := range nodes.Items {
		totalAllocatableCPU += node.Status.Allocatable.Cpu().MilliValue()
		totalAllocatableMem += node.Status.Allocatable.Memory().Value() / (1024 * 1024)
	}

	for _, nm := range nodeMetrics.Items {
		totalUsedCPU += nm.Usage.Cpu().MilliValue()
		totalUsedMem += nm.Usage.Memory().Value() / (1024 * 1024)
	}

	// Count pending pods by class
	pods, err := l.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "status.phase=Pending",
		LabelSelector: "scheduling.example.io/managed=true",
	})
	if err != nil {
		return l.cache, err
	}

	pendingPerClass := make(map[string]int)
	for _, pod := range pods.Items {
		class := pod.Annotations["slo.hybrid.io/class"]
		pendingPerClass[class]++
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
