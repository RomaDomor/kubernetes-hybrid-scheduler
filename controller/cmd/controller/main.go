// cmd/controller/main.go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
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

	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/signals"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
	"kubernetes-hybrid-scheduler/controller/pkg/webhook"
)

var (
	masterURL     string
	kubeconfig    string
	cloudEndpoint string
	config        decision.EngineConfig
	hcfg          decision.HistogramConfig

	// Helper for histogram bounds mode
	histBoundsModeStr string
	histBoundsCSV     string
	decayIntervalStr  string
)

func main() {
	klog.InitFlags(nil)

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	flag.StringVar(&masterURL, "master", "", "Kubernetes API server URL")
	flag.StringVar(&cloudEndpoint, "cloud-endpoint",
		util.GetEnvOrDefault("CLOUD_ENDPOINT", "10.0.1.100"),
		"Cloud endpoint IP for WAN probe")

	// Engine config
	flag.IntVar(&config.RTTThresholdMs, "rtt-threshold",
		util.GetEnvInt("RTT_THRESHOLD", 100), "WAN RTT threshold (ms)")
	flag.Float64Var(&config.LossThresholdPct, "loss-threshold",
		util.GetEnvFloat("LOSS_THRESHOLD", 2.0), "WAN packet loss threshold (%)")
	flag.IntVar(&config.RTTUnusableMs, "rtt-unusable",
		util.GetEnvInt("RTT_UNUSABLE", 300), "WAN RTT unusable threshold (ms)")
	flag.Float64Var(&config.LossUnusablePct, "loss-unusable",
		util.GetEnvFloat("LOSS_UNUSABLE", 10.0), "WAN loss unusable threshold (%)")
	flag.Float64Var(&config.LocalityBonus, "locality-bonus",
		util.GetEnvFloat("LOCALITY_BONUS", 50.0), "Edge locality score bonus")
	flag.Float64Var(&config.ConfidenceWeight, "confidence-weight",
		util.GetEnvFloat("CONFIDENCE_WEIGHT", 30.0), "Confidence score weight")
	flag.Float64Var(&config.ExplorationRate, "exploration-rate",
		util.GetEnvFloat("EXPLORATION_RATE", 0.2), "Exploration probability (0-1)")
	flag.IntVar(&config.MaxProfileCount, "max-profiles",
		util.GetEnvInt("MAX_PROFILES", 100), "Maximum profile entries (LRU)")
	flag.Float64Var(&config.CloudMarginOverridePct, "cloud-margin-override-pct",
		util.GetEnvFloat("CLOUD_MARGIN_OVERRIDE_PCT", 0.15), "Cloud deadline margin override % (0-1)")
	flag.Float64Var(&config.WanStaleConfFactor, "wan-stale-conf-factor",
		util.GetEnvFloat("WAN_STALE_CONF_FACTOR", 0.8), "WAN stale confidence factor (0-1)")
	flag.Float64Var(&config.EdgeHeadroomOverridePct, "edge-headroom-override-pct",
		util.GetEnvFloat("EDGE_HEADROOM_OVERRIDE_PCT", 0.1), "Edge headroom override % (0-1)")

	// Histogram config - parse directly into hcfg
	flag.StringVar(&histBoundsModeStr, "hist-bounds-mode",
		util.GetEnvOrDefault("HIST_BOUNDS_MODE", "explicit"),
		"Histogram bounds mode: explicit|log")
	flag.StringVar(&histBoundsCSV, "hist-bounds",
		util.GetEnvOrDefault("HIST_BOUNDS", ""),
		"Comma-separated histogram bounds in ms (for explicit mode)")
	flag.Float64Var(&hcfg.LogStartMs, "hist-log-start-ms",
		util.GetEnvFloat("HIST_LOG_START_MS", 50),
		"Log mode: starting bound (ms)")
	flag.Float64Var(&hcfg.LogFactor, "hist-log-factor",
		util.GetEnvFloat("HIST_LOG_FACTOR", 2.0),
		"Log mode: factor per step")
	flag.IntVar(&hcfg.LogCount, "hist-log-count",
		util.GetEnvInt("HIST_LOG_COUNT", 20),
		"Log mode: number of bounds")
	flag.BoolVar(&hcfg.IncludeInf, "hist-include-inf",
		util.GetEnvOrDefault("HIST_INCLUDE_INF", "1") != "0",
		"Append +Inf bucket")
	flag.Float64Var(&hcfg.IngestCapMs, "ingest-cap-ms",
		util.GetEnvFloat("INGEST_CAP_MS", 30*60*1000),
		"Ingest cap in ms")
	flag.IntVar(&hcfg.MinSampleCount, "min-sample-count",
		util.GetEnvInt("MIN_SAMPLE_COUNT", 10),
		"Minimum samples for reliable p95")
	flag.StringVar(&decayIntervalStr, "decay-interval",
		util.GetEnvOrDefault("DECAY_INTERVAL", "1h"),
		"Histogram decay interval (e.g., 30m, 1h)")

	flag.Parse()

	// Post-parse processing for histogram config
	switch histBoundsModeStr {
	case "explicit":
		hcfg.Mode = decision.BoundsExplicit
	case "log":
		hcfg.Mode = decision.BoundsLog
	default:
		klog.Warningf("Unknown hist-bounds-mode %q, defaulting to explicit", histBoundsModeStr)
		hcfg.Mode = decision.BoundsExplicit
	}

	if histBoundsCSV != "" {
		hcfg.Explicit = decision.ParseFloatSliceCSV(histBoundsCSV)
		klog.Infof("Using explicit histogram bounds: %v", hcfg.Explicit)
	} else {
		hcfg.Explicit = decision.DefaultHistogramConfig().Explicit
	}

	if iv, err := time.ParseDuration(decayIntervalStr); err == nil {
		hcfg.DecayInterval = iv
	} else {
		klog.Warningf("Invalid decay-interval %q, using default 1h", decayIntervalStr)
		hcfg.DecayInterval = time.Hour
	}

	klog.Infof("Histogram config: mode=%s includeInf=%v ingestCap=%.0fms minSamples=%d decay=%s",
		hcfg.Mode, hcfg.IncludeInf, hcfg.IngestCapMs, hcfg.MinSampleCount, hcfg.DecayInterval)

	stopCh := signals.SetupSignalHandler()

	// Initialize Kubernetes clients
	cfg := buildKubeConfig()
	kubeClient := kubernetes.NewForConfigOrDie(cfg)

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
	localCollector := telemetry.NewLocalCollector(kubeClient, podInformer, nodeInformer)
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

	// Initialize profile store with histogram config
	profileStore := decision.NewProfileStore(kubeClient, config.MaxProfileCount, hcfg)
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
