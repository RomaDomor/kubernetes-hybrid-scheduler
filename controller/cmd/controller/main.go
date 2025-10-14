package main

import (
	"flag"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"

	"kubernetes-hybrid-scheduler/controller/pkg/controller"
	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/signals"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
)

var (
	masterURL     string
	kubeconfig    string
	cloudEndpoint string
	rttThreshold  int
	lossThreshold float64
)

func main() {
	klog.InitFlags(nil)
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	flag.StringVar(&masterURL, "master", "", "Kubernetes API server URL")
	flag.StringVar(&cloudEndpoint, "cloud-endpoint", "10.0.1.100", "Cloud endpoint IP for WAN probe")
	flag.IntVar(&rttThreshold, "rtt-threshold", 100, "WAN RTT threshold (ms)")
	flag.Float64Var(&lossThreshold, "loss-threshold", 2.0, "WAN packet loss threshold (%)")
	flag.Parse()

	// Setup signal handler
	stopCh := signals.SetupSignalHandler()

	// Build config
	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	metricsClient, err := metricsv.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building metrics clientset: %s", err.Error())
	}

	// Create informers
	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	podInformer := kubeInformerFactory.Core().V1().Pods()
	nodeInformer := kubeInformerFactory.Core().V1().Nodes()

	// Create telemetry collectors
	localCollector := telemetry.NewLocalCollector(kubeClient, metricsClient)
	wanProbe := telemetry.NewWANProbe(cloudEndpoint, time.Second*60)
	telemetryCollector := telemetry.NewCombinedCollector(localCollector, wanProbe)

	// Create decision engine
	decisionEngine := decision.NewEngine(decision.Config{
		RTTThresholdMs:   rttThreshold,
		LossThresholdPct: lossThreshold,
	})

	// Create controller
	ctrl := controller.NewController(
		kubeClient,
		podInformer,
		nodeInformer,
		decisionEngine,
		telemetryCollector,
	)

	// Start informers
	kubeInformerFactory.Start(stopCh)

	// Run controller
	if err = ctrl.Run(2, stopCh); err != nil {
		klog.Fatalf("Error running controller: %s", err.Error())
	}
}
