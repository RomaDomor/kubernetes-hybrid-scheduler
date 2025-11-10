package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	v1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
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

	// Lyapunov configuration
	lyapunovBeta    float64
	cloudCostFactor float64
	edgeCostFactor  float64

	// Helper for histogram bounds mode
	histBoundsModeStr string
	histBoundsCSV     string
	decayIntervalStr  string
)

// sloClassConfig defines the configuration parameters for each SLO class.
type sloClassConfig struct {
	targetViolationPct  float64
	targetViolationProb float64
	decayFactor         float64
	decayInterval       time.Duration
	probabilityWeight   float64
	betaFactor          float64 // Multiplier for the global lyapunovBeta
}

func main() {
	klog.InitFlags(nil)
	setupFlags()
	flag.Parse()

	if err := postProcessFlags(); err != nil {
		klog.Fatalf("Error processing flags: %v", err)
	}

	klog.Infof("Histogram config: mode=%s includeInf=%v ingestCap=%.0fms minSamples=%d decay=%s",
		hcfg.Mode, hcfg.IncludeInf, hcfg.IngestCapMs, hcfg.MinSampleCount, hcfg.DecayInterval)

	klog.Infof("Lyapunov config: beta=%.2f cloudCost=%.2f edgeCost=%.2f",
		lyapunovBeta, cloudCostFactor, edgeCostFactor)

	stopCh := signals.SetupSignalHandler()

	// Initialize Kubernetes clients
	cfg := buildKubeConfig()
	kubeClient := kubernetes.NewForConfigOrDie(cfg)
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building dynamic client: %v", err)
	}

	// Preflight checks to ensure connectivity
	preflightChecks(kubeClient)

	// Setup informers for watching cluster resources
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 30*time.Second)
	podInformer := informerFactory.Core().V1().Pods()
	nodeInformer := informerFactory.Core().V1().Nodes()

	// Initialize telemetry collectors
	localCollector := telemetry.NewLocalCollector(kubeClient, podInformer, nodeInformer, int64(config.EdgePendingPessimismPct))
	wanProbe := telemetry.NewWANProbe(cloudEndpoint, 15*time.Second)
	telemetryCollector := telemetry.NewCombinedCollector(localCollector, wanProbe)

	klog.Info("Starting informers...")
	informerFactory.Start(stopCh)

	// Wait for cache sync before starting servers
	klog.Info("Waiting for informer caches to sync...")
	if !cache.WaitForCacheSync(stopCh, podInformer.Informer().HasSynced, nodeInformer.Informer().HasSynced) {
		klog.Fatal("Failed to sync informer caches")
	}
	klog.Info("Informer caches synced successfully")

	// Start background telemetry refresh
	go refreshTelemetryLoop(telemetryCollector, stopCh)

	// Initialize profile store with histogram config
	profileStore := decision.NewProfileStore(kubeClient, config.MaxProfileCount, hcfg)
	if err := profileStore.LoadFromCRD(dynClient); err != nil {
		klog.Warningf("Failed to load profiles from CRD (using defaults): %v", err)
	}

	// Initialize and configure the Lyapunov scheduler
	lyapunovScheduler := initializeLyapunovScheduler()
	klog.Info("Lyapunov scheduler initialized with per-class configuration")

	// Create decision engine with all its dependencies
	config.ProfileStore = profileStore
	config.LyapunovScheduler = lyapunovScheduler
	config.CloudCostFactor = cloudCostFactor
	config.EdgeCostFactor = edgeCostFactor
	decisionEngine := decision.NewEngine(config)

	// PodObserver watches for pod completions to update profiles
	podObserver := decision.NewPodObserver(kubeClient, profileStore, podInformer, lyapunovScheduler)
	go podObserver.Watch(stopCh)

	// Auto-save profiles periodically to CRDs
	go profileStore.StartAutoSave(dynClient, 5*time.Minute, stopCh)

	// Start a background process to update Prometheus metrics
	go startMetricsUpdater(profileStore, telemetryCollector, lyapunovScheduler, stopCh)

	// Start the admin server for metrics, health checks, and debug endpoints
	adminSrv := startAdminServer(podInformer, nodeInformer, telemetryCollector, profileStore, lyapunovScheduler)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := adminSrv.Shutdown(ctx); err != nil {
			klog.Errorf("Admin server shutdown failed: %v", err)
		}
	}()

	// Start the main webhook server
	webhookSrv := startWebhookServer(decisionEngine, telemetryCollector, kubeClient)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := webhookSrv.Shutdown(ctx); err != nil {
			klog.Errorf("Webhook server shutdown failed: %v", err)
		}
	}()

	<-stopCh
	klog.Info("Shutting down gracefully")
}

// setupFlags configures all command-line flags.
func setupFlags() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig file. Required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig.")
	flag.StringVar(&cloudEndpoint, "cloud-endpoint", util.GetEnvOrDefault("CLOUD_ENDPOINT", "10.0.1.100"), "Cloud endpoint IP for WAN probe.")

	// Engine config
	flag.IntVar(&config.RTTUnusableMs, "rtt-unusable", util.GetEnvInt("RTT_UNUSABLE", 300), "WAN RTT unusable threshold (ms).")
	flag.Float64Var(&config.LossUnusablePct, "loss-unusable", util.GetEnvFloat("LOSS_UNUSABLE", 10.0), "WAN loss unusable threshold (%).")
	flag.IntVar(&config.MaxProfileCount, "max-profiles", util.GetEnvInt("MAX_PROFILES", 100), "Maximum profile entries (LRU).")
	flag.IntVar(&config.EdgePendingPessimismPct, "edge-pending-pessimism-pct", util.GetEnvInt("EDGE_PENDING_PESSIMISM_PCT", 10), "Edge pending pod pessimism factor % (0-100).")

	// Lyapunov configuration
	flag.Float64Var(&lyapunovBeta, "lyapunov-beta", util.GetEnvFloat("LYAPUNOV_BETA", 1.0), "Lyapunov cost-performance tradeoff (0.5-2.0).")
	flag.Float64Var(&cloudCostFactor, "cloud-cost-factor", util.GetEnvFloat("CLOUD_COST_FACTOR", 1.0), "Relative cost of cloud scheduling.")
	flag.Float64Var(&edgeCostFactor, "edge-cost-factor", util.GetEnvFloat("EDGE_COST_FACTOR", 0.0), "Relative cost of edge scheduling.")

	// Histogram config
	flag.StringVar(&histBoundsModeStr, "hist-bounds-mode", util.GetEnvOrDefault("HIST_BOUNDS_MODE", "explicit"), "Histogram bounds mode: explicit|log.")
	flag.StringVar(&histBoundsCSV, "hist-bounds", util.GetEnvOrDefault("HIST_BOUNDS", ""), "Comma-separated histogram bounds in ms (for explicit mode).")
	flag.Float64Var(&hcfg.LogStartMs, "hist-log-start-ms", util.GetEnvFloat("HIST_LOG_START_MS", 50), "Log mode: starting bound (ms).")
	flag.Float64Var(&hcfg.LogFactor, "hist-log-factor", util.GetEnvFloat("HIST_LOG_FACTOR", 2.0), "Log mode: factor per step.")
	flag.IntVar(&hcfg.LogCount, "hist-log-count", util.GetEnvInt("HIST_LOG_COUNT", 20), "Log mode: number of bounds.")
	flag.BoolVar(&hcfg.IncludeInf, "hist-include-inf", util.GetEnvOrDefault("HIST_INCLUDE_INF", "1") != "0", "Append +Inf bucket.")
	flag.Float64Var(&hcfg.IngestCapMs, "ingest-cap-ms", util.GetEnvFloat("INGEST_CAP_MS", 30*60*1000), "Ingest cap in ms.")
	flag.IntVar(&hcfg.MinSampleCount, "min-sample-count", util.GetEnvInt("MIN_SAMPLE_COUNT", 10), "Minimum samples for reliable p95.")
	flag.StringVar(&decayIntervalStr, "decay-interval", util.GetEnvOrDefault("DECAY_INTERVAL", "1h"), "Histogram decay interval (e.g., 30m, 1h).")
}

// postProcessFlags validates and computes configuration values after parsing.
func postProcessFlags() error {
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

	iv, err := time.ParseDuration(decayIntervalStr)
	if err != nil {
		klog.Warningf("Invalid decay-interval %q, using default 1h. Error: %v", decayIntervalStr, err)
		hcfg.DecayInterval = time.Hour
	} else {
		hcfg.DecayInterval = iv
	}
	return nil
}

// buildKubeConfig creates a Kubernetes REST config, prioritizing in-cluster configuration.
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
			klog.Fatalf("Error building kubeconfig from flags: %v", err)
		}
	}
	klog.Infof("Using Kubernetes API host: %s", cfg.Host)
	return cfg
}

// preflightChecks verifies initial connectivity to the Kubernetes API server.
func preflightChecks(kubeClient kubernetes.Interface) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		klog.Warningf("Preflight check: listing nodes failed (this may be a permissions issue): %v", err)
	}
	if _, err := kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		klog.Warningf("Preflight check: listing pods failed (this may be a permissions issue): %v", err)
	}
	klog.Info("Preflight checks completed")
}

// refreshTelemetryLoop periodically refreshes local and WAN telemetry data.
func refreshTelemetryLoop(tel telemetry.Collector, stopCh <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Initial synchronous refresh
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, _ = tel.GetLocalState(ctx)
	_, _ = tel.GetWANState(ctx)
	cancel()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if _, err := tel.GetLocalState(ctx); err != nil {
				klog.V(4).Infof("Background telemetry refresh failed: %v", err)
			}
			_, _ = tel.GetWANState(ctx)
			cancel()
		case <-stopCh:
			return
		}
	}
}

// initializeLyapunovScheduler creates and configures the Lyapunov scheduler with per-class parameters.
func initializeLyapunovScheduler() *decision.LyapunovScheduler {
	lyapunovScheduler := decision.NewLyapunovScheduler()

	// Define per-class configurations in a map for easy management.
	classConfigs := map[string]sloClassConfig{
		"latency":     {0.05, 0.05, 0.95, time.Hour, 1.0, 1.0},
		"interactive": {0.05, 0.05, 0.95, time.Hour, 1.0, 1.0},
		"throughput":  {0.10, 0.10, 0.95, time.Hour, 0.8, 1.0},
		"streaming":   {0.10, 0.10, 0.95, time.Hour, 0.8, 1.0},
		"batch":       {0.20, 0.20, 0.90, 2 * time.Hour, 0.5, 0.5}, // More cost-sensitive
	}

	for class, cfg := range classConfigs {
		lyapunovScheduler.SetClassConfig(class, &decision.ClassConfig{
			Beta:                lyapunovBeta * cfg.betaFactor,
			TargetViolationPct:  cfg.targetViolationPct,
			TargetViolationProb: cfg.targetViolationProb,
			DecayFactor:         cfg.decayFactor,
			DecayInterval:       cfg.decayInterval,
			ProbabilityWeight:   cfg.probabilityWeight,
		})
	}

	return lyapunovScheduler
}

// startMetricsUpdater runs a loop to periodically update Prometheus metrics.
func startMetricsUpdater(ps *decision.ProfileStore, tel telemetry.Collector, lyap *decision.LyapunovScheduler, stopCh <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ps.UpdateMetrics()
			tel.UpdateMetrics()
			decision.UpdateLyapunovMetrics(lyap)
		case <-stopCh:
			return
		}
	}
}

// startAdminServer initializes and starts the HTTP server for admin endpoints.
func startAdminServer(
	podInformer v1.PodInformer,
	nodeInformer v1.NodeInformer,
	tel telemetry.Collector,
	ps *decision.ProfileStore,
	lyap *decision.LyapunovScheduler,
) *http.Server {
	adminMux := http.NewServeMux()
	adminMux.Handle("/metrics", promhttp.Handler())
	adminMux.HandleFunc("/healthz", healthHandler)
	adminMux.HandleFunc("/readyz", readyHandlerFactory(
		podInformer.Informer().HasSynced,
		nodeInformer.Informer().HasSynced,
	))
	adminMux.HandleFunc("/debug/telemetry", debugTelemetryHandler(tel))
	adminMux.HandleFunc("/debug/profiles", debugProfilesHandler(ps))
	adminMux.HandleFunc("/debug/lyapunov", debugLyapunovHandler(lyap))

	adminSrv := &http.Server{
		Addr:         ":8080",
		Handler:      adminMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		klog.Info("Starting admin server on :8080 (HTTP) for /metrics, /healthz, /readyz, /debug/*")
		if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Fatalf("Admin server failed: %v", err)
		}
	}()
	return adminSrv
}

// startWebhookServer initializes and starts the HTTPS webhook server.
func startWebhookServer(
	decisionEngine *decision.Engine,
	tel telemetry.Collector,
	kubeClient kubernetes.Interface,
) *http.Server {
	limiter := rate.NewLimiter(rate.Limit(100), 200) // 100 req/s, burst 200
	wh := webhook.NewServer(decisionEngine, tel, limiter, kubeClient)
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
		if err := webhookSrv.ListenAndServeTLS("/certs/tls.crt", "/certs/tls.key"); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Fatalf("Webhook server failed: %v", err)
		}
	}()
	return webhookSrv
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
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

// writeJSONResponse is a helper to encode and write JSON responses.
func writeJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		klog.Warningf("Failed to write JSON response: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func debugTelemetryHandler(tel telemetry.Collector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state := struct {
			Local *apis.LocalState `json:"local"`
			WAN   *apis.WANState   `json:"wan"`
			Time  time.Time        `json:"timestamp"`
		}{
			Local: tel.GetCachedLocalState(),
			WAN:   tel.GetCachedWANState(),
			Time:  time.Now(),
		}
		writeJSONResponse(w, state)
	}
}

func debugProfilesHandler(ps *decision.ProfileStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		profiles := ps.ExportAllProfiles()
		writeJSONResponse(w, profiles)
	}
}

func debugLyapunovHandler(lyap *decision.LyapunovScheduler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state := lyap.ExportState()
		writeJSONResponse(w, state)
	}
}
