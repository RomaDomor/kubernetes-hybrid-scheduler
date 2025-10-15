package controller

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

const (
	ManagedLabel        = "scheduling.example.io/managed"
	DecisionAnnotation  = "scheduling.example.io/decision"
	TimestampAnnotation = "scheduling.example.io/timestamp"
	ReasonAnnotation    = "scheduling.example.io/reason"
)

type Controller struct {
	kubeclientset kubernetes.Interface

	podsLister  corelisters.PodLister
	podsSynced  cache.InformerSynced
	nodesLister corelisters.NodeLister
	nodesSynced cache.InformerSynced

	workqueue workqueue.RateLimitingInterface

	// Custom components
	decisionEngine *decision.Engine
	telemetry      telemetry.Collector
}

func NewController(
	kubeclientset kubernetes.Interface,
	podInformer coreinformers.PodInformer,
	nodeInformer coreinformers.NodeInformer,
	decisionEngine *decision.Engine,
	telemetryCollector telemetry.Collector,
) *Controller {
	controller := &Controller{
		kubeclientset: kubeclientset,
		podsLister:    podInformer.Lister(),
		podsSynced:    podInformer.Informer().HasSynced,
		nodesLister:   nodeInformer.Lister(),
		nodesSynced:   nodeInformer.Informer().HasSynced,
		workqueue: workqueue.NewNamedRateLimitingQueue(
			workqueue.DefaultControllerRateLimiter(),
			"Pods",
		),
		decisionEngine: decisionEngine,
		telemetry:      telemetryCollector,
	}

	klog.Info("Setting up event handlers")

	// Watch for new or updated Pods
	_, err := podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueuePod,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueuePod(new)
		},
	})

	if err != nil {
		return nil
	}

	return controller
}

func (c *Controller) enqueuePod(obj interface{}) {
	pod := obj.(*corev1.Pod)

	// Filter: only handle managed, pending, unassigned pods
	if pod.Labels[ManagedLabel] != "true" {
		return
	}
	if pod.Status.Phase != corev1.PodPending {
		return
	}
	if pod.Spec.NodeName != "" {
		return
	}
	if _, exists := pod.Annotations[DecisionAnnotation]; exists {
		return // Already processed
	}

	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	klog.Info("Starting smart scheduler controller")

	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.podsSynced, c.nodesSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	if err := c.validateNodeLabels(); err != nil {
		return fmt.Errorf("node validation failed: %w", err)
	}

	go c.startHealthServer()

	klog.Info("Starting workers")
	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Millisecond*200, stopCh) // Your polling interval
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		if err := c.syncHandler(key); err != nil {
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}

		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

func (c *Controller) syncHandler(key string) error {
	start := time.Now()
	defer func() {
		decisionDuration.Observe(time.Since(start).Seconds())
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the Pod
	pod, err := c.podsLister.Pods(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Warningf("Pod '%s' in work queue no longer exists", key)
			return nil
		}
		return err
	}

	// Double-check it still needs scheduling
	if pod.Spec.NodeName != "" || pod.Annotations[DecisionAnnotation] != "" {
		return nil
	}

	// Parse SLO
	sloData, err := slo.ParseSLO(pod)
	if err != nil {
		c.recordEvent(pod, corev1.EventTypeWarning, "InvalidSLO", err.Error())
		return err
	}

	// Gather telemetry
	localState, err := c.telemetry.GetLocalState(context.Background())
	if err != nil {
		klog.Warningf("Failed to get local telemetry, using fallback: %v", err)
		telemetryErrors.WithLabelValues("local").Inc()
		localState = c.telemetry.GetCachedLocalState()
	}

	wanState, err := c.telemetry.GetWANState(context.Background())
	if err != nil {
		klog.Warningf("Failed to get WAN telemetry, using fallback: %v", err)
		telemetryErrors.WithLabelValues("wan").Inc()
		wanState = c.telemetry.GetCachedWANState()
	}

	// Make decision
	result := c.decisionEngine.Decide(pod, sloData, localState, wanState)
	decisionsTotal.WithLabelValues(string(result.Location), result.Reason).Inc()

	// Patch pod
	if err := c.patchPod(pod, result); err != nil {
		return err
	}

	// Log and emit event
	klog.Infof("Scheduled pod %s/%s to %s (reason: %s, RTT: %dms)",
		pod.Namespace, pod.Name, result.Location, result.Reason, wanState.RTTMs)
	c.recordEvent(pod, corev1.EventTypeNormal, "Scheduled",
		fmt.Sprintf("Assigned to %s: %s", result.Location, result.Reason))

	return nil
}

func (c *Controller) validateNodeLabels() error {
	edgeSelector, _ := labels.Parse("node.role/edge=true")
	edgeNodes, err := c.nodesLister.List(edgeSelector)
	if err != nil || len(edgeNodes) == 0 {
		return fmt.Errorf("no edge nodes found with label node.role/edge=true")
	}
	klog.Infof("Found %d edge nodes", len(edgeNodes))

	cloudSelector, _ := labels.Parse("node.role/cloud=true")
	cloudNodes, err := c.nodesLister.List(cloudSelector)
	if err != nil || len(cloudNodes) == 0 {
		klog.Warning("No cloud nodes found; offloading will fail")
	} else {
		klog.Infof("Found %d cloud nodes", len(cloudNodes))
	}
	return nil
}

func (c *Controller) startHealthServer() {
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// If informers are synced, we're ready
		if c.podsSynced() && c.nodesSynced() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe(":8080", nil); err != nil {
		klog.Errorf("Failed to start health server: %s", err)
	}
}

var (
	decisionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_decisions_total",
			Help: "Total scheduling decisions by location and reason",
		},
		[]string{"location", "reason"},
	)
	decisionDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "scheduler_decision_duration_seconds",
			Help:    "Time to compute scheduling decision",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		},
	)
	telemetryErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_telemetry_errors_total",
			Help: "Telemetry collection errors by type",
		},
		[]string{"type"},
	)
)

func init() {
	prometheus.MustRegister(decisionsTotal, decisionDuration, telemetryErrors)
}
