package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
	config        decision.EngineConfig
)

func main() {
	klog.InitFlags(nil)

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	flag.StringVar(&masterURL, "master", "", "Kubernetes API server URL")
	flag.StringVar(&cloudEndpoint, "cloud-endpoint",
		getEnvOrDefault("CLOUD_ENDPOINT", "10.0.1.100"),
		"Cloud endpoint IP for WAN probe")

	// Configurable thresholds
	flag.IntVar(&config.RTTThresholdMs, "rtt-threshold",
		getEnvInt("RTT_THRESHOLD", 100), "WAN RTT threshold (ms)")
	flag.Float64Var(&config.LossThresholdPct, "loss-threshold",
		getEnvFloat("LOSS_THRESHOLD", 2.0), "WAN packet loss threshold (%)")
	flag.IntVar(&config.RTTUnusableMs, "rtt-unusable",
		getEnvInt("RTT_UNUSABLE", 300), "WAN RTT unusable threshold (ms)")
	flag.Float64Var(&config.LossUnusablePct, "loss-unusable",
		getEnvFloat("LOSS_UNUSABLE", 10.0), "WAN loss unusable threshold (%)")
	flag.Float64Var(&config.LocalityBonus, "locality-bonus",
		getEnvFloat("LOCALITY_BONUS", 50.0), "Edge locality score bonus")
	flag.Float64Var(&config.ConfidenceWeight, "confidence-weight",
		getEnvFloat("CONFIDENCE_WEIGHT", 30.0), "Confidence score weight")
	flag.Float64Var(&config.ExplorationRate, "exploration-rate",
		getEnvFloat("EXPLORATION_RATE", 0.2), "Exploration probability (0-1)")
	flag.IntVar(&config.MaxProfileCount, "max-profiles",
		getEnvInt("MAX_PROFILES", 100), "Maximum profile entries (LRU)")

	flag.Parse()

	stopCh := signals.SetupSignalHandler()

	// Initialize Kubernetes clients
	cfg := buildKubeConfig()
	kubeClient := kubernetes.NewForConfigOrDie(cfg)
	metricsClient := metricsv.NewForConfigOrDie(cfg)

	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building dynamic client: %v", err)
	}

	// Preflight checks
	preflightChecks(kubeClient)

	// Setup informers
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 30*time.Second)
	podInformer := informerFactory.Core().V1().Pods()
	nodeInformer := informerFactory.Core().V1().Nodes()

	// Initialize telemetry collectors
	localCollector := telemetry.NewLocalCollector(kubeClient, metricsClient, podInformer, nodeInformer)
	wanProbe := telemetry.NewWANProbe(cloudEndpoint, 15*time.Second)
	telemetryCollector := telemetry.NewCombinedCollector(localCollector, wanProbe)

	klog.Info("Starting informers...")
	informerFactory.Start(stopCh)

	// Wait for cache sync BEFORE starting servers
	klog.Info("Waiting for informer caches to sync...")
	if !cache.WaitForCacheSync(stopCh,
		podInformer.Informer().HasSynced,
		nodeInformer.Informer().HasSynced) {
		klog.Fatal("Failed to sync informer caches")
	}
	klog.Info("Informer caches synced successfully")

	// Background telemetry refresh loop
	go refreshTelemetryLoop(telemetryCollector, stopCh)

	// Initialize profile store with CRD backend
	profileStore := decision.NewProfileStore(kubeClient, config.MaxProfileCount)
	if err := profileStore.LoadFromCRD(dynClient); err != nil {
		klog.Warningf("Failed to load profiles from CRD (using defaults): %v", err)
	}

	// PodObserver runs as separate goroutine with its own rate limiting
	podObserver := decision.NewPodObserver(kubeClient, profileStore, podInformer)
	go podObserver.Watch(stopCh)

	// Auto-save profiles periodically
	go profileStore.StartAutoSave(dynClient, 5*time.Minute, stopCh)

	// Start metrics updater
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				profileStore.UpdateMetrics()
				telemetryCollector.UpdateMetrics()
			case <-stopCh:
				return
			}
		}
	}()

	// Create decision engine
	config.ProfileStore = profileStore
	decisionEngine := decision.NewEngine(config)

	// Plain HTTP admin server for metrics and health
	adminMux := http.NewServeMux()
	adminMux.Handle("/metrics", promhttp.Handler())
	adminMux.HandleFunc("/healthz", healthHandler)
	adminMux.HandleFunc("/readyz", readyHandlerFactory(
		podInformer.Informer().HasSynced,
		nodeInformer.Informer().HasSynced,
	))
	adminMux.HandleFunc("/debug/telemetry", debugTelemetryHandler(telemetryCollector))
	adminMux.HandleFunc("/debug/profiles", debugProfilesHandler(profileStore))

	adminSrv := &http.Server{
		Addr:         ":8080",
		Handler:      adminMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		klog.Info("Starting admin server on :8080 (HTTP) for /metrics, /healthz, /readyz")
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Fatalf("Admin server failed: %v", err)
		}
	}()

	// Rate limiter
	limiter := rate.NewLimiter(rate.Limit(100), 200) // 100 req/s, burst 200

	// TLS webhook server
	wh := webhook.NewServer(decisionEngine, telemetryCollector, limiter, kubeClient)
	webhookMux := http.NewServeMux()
	webhookMux.Handle("/mutate", wh)

	webhookSrv := &http.Server{
		Addr:         ":8443",
		Handler:      webhookMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		klog.Info("Starting webhook server on :8443 (TLS)")
		if err := webhookSrv.ListenAndServeTLS("/certs/tls.crt", "/certs/tls.key"); err != nil {
			klog.Fatalf("Webhook server failed: %v", err)
		}
	}()

	<-stopCh
	klog.Info("Shutting down gracefully")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = webhookSrv.Shutdown(ctx)
	_ = adminSrv.Shutdown(ctx)
}

func buildKubeConfig() *rest.Config {
	var cfg *rest.Config
	var err error
	if masterURL == "" && kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			klog.Fatalf("Error building in-cluster config: %v", err)
		}
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
		if err != nil {
			klog.Fatalf("Error building kubeconfig: %v", err)
		}
	}
	klog.Infof("Using API host: %s", cfg.Host)
	return cfg
}

func preflightChecks(kubeClient kubernetes.Interface) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		klog.Errorf("Preflight: listing nodes failed: %v", err)
	}
	if _, err := kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		klog.Errorf("Preflight: listing pods failed: %v", err)
	}
}

func refreshTelemetryLoop(tel telemetry.Collector, stopCh <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Initial refresh
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, _ = tel.GetLocalState(ctx)
	_, _ = tel.GetWANState(ctx)
	cancel()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if _, err := tel.GetLocalState(ctx); err != nil {
				klog.V(4).Infof("Background telemetry refresh: %v", err)
			}
			_, _ = tel.GetWANState(ctx)
			cancel()
		case <-stopCh:
			return
		}
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func readyHandlerFactory(syncChecks ...cache.InformerSynced) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for _, check := range syncChecks {
			if !check() {
				http.Error(w, "caches not synced", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	}
}

func debugTelemetryHandler(tel telemetry.Collector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state := struct {
			Local *telemetry.LocalState `json:"local"`
			WAN   *telemetry.WANState   `json:"wan"`
			Time  time.Time             `json:"timestamp"`
		}{
			Local: tel.GetCachedLocalState(),
			WAN:   tel.GetCachedWANState(),
			Time:  time.Now(),
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(state)
	}
}

func debugProfilesHandler(ps *decision.ProfileStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		profiles := ps.ExportAllProfiles()
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(profiles)
	}
}

func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func getEnvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
