package main

import (
	"flag"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/signals"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
	"kubernetes-hybrid-scheduler/controller/pkg/webhook"
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

	// Set defaults from env vars first
	if val := os.Getenv("RTT_THRESHOLD"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			rttThreshold = parsed
		}
	}
	if val := os.Getenv("LOSS_THRESHOLD"); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			lossThreshold = parsed
		}
	}
	if val := os.Getenv("CLOUD_ENDPOINT"); val != "" {
		cloudEndpoint = val
	}

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
	localCollector := telemetry.NewLocalCollector(
		kubeClient,
		metricsClient,
		podInformer,
		nodeInformer,
	)
	wanProbe := telemetry.NewWANProbe(cloudEndpoint, time.Second*60)
	telemetryCollector := telemetry.NewCombinedCollector(localCollector, wanProbe)

	// Create decision engine
	decisionEngine := decision.NewEngine(decision.Config{
		RTTThresholdMs:   rttThreshold,
		LossThresholdPct: lossThreshold,
	})

	// Start informers (needed for webhook to access cached data)
	klog.Info("Starting informers")
	kubeInformerFactory.Start(stopCh)

	// Wait for cache sync
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh,
		podInformer.Informer().HasSynced,
		nodeInformer.Informer().HasSynced,
	); !ok {
		klog.Fatal("Failed to sync caches")
	}
	klog.Info("Caches synced successfully")

	// Setup webhook and health server
	wh := webhook.NewServer(decisionEngine, telemetryCollector)
	mux := http.NewServeMux()
	mux.Handle("/mutate", wh)
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/readyz", readyHandler)
	mux.Handle("/metrics", promhttp.Handler())

	// Start webhook server with TLS
	go func() {
		srv := &http.Server{
			Addr:    ":8443",
			Handler: mux,
		}
		klog.Info("Starting webhook server on :8443")
		if err := srv.ListenAndServeTLS("/certs/tls.crt", "/certs/tls.key"); err != nil {
			klog.Fatalf("webhook server error: %v", err)
		}
	}()

	// Block until signal
	klog.Info("Webhook server running, waiting for requests...")
	<-stopCh
	klog.Info("Shutting down")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ready"))
}
