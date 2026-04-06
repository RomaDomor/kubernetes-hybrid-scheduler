package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"strings"
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

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/decision"
	"kubernetes-hybrid-scheduler/controller/pkg/signals"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
	"kubernetes-hybrid-scheduler/controller/pkg/webhook"
)

var (
	masterURL  string
	kubeconfig string

	// Comma-separated list: "cloud-1=10.0.1.100,cloud-2=10.0.2.100"
	remoteEndpointsCSV string

	config EngineFlags
	hcfg   decision.HistogramConfig

	lyapunovBeta   float64
	costFactorsCSV string // "local=0,cloud-1=1.0,cloud-2=1.5"

	histBoundsModeStr string
	histBoundsCSV     string
	decayIntervalStr  string
)

type EngineFlags struct {
	RTTUnusableMs           int
	LossUnusablePct         float64
	MaxProfileCount         int
	EdgePendingPessimismPct int
}

type sloClassConfig struct {
	targetViolationPct  float64
	targetViolationProb float64
	decayFactor         float64
	decayInterval       time.Duration
	probabilityWeight   float64
	betaFactor          float64
}

func main() {
	klog.InitFlags(nil)
	setupFlags()
	flag.Parse()

	if err := postProcessFlags(); err != nil {
		klog.Fatalf("Error processing flags: %v", err)
	}

	// Parse remote cluster endpoints
	remoteEndpoints := parseEndpoints(remoteEndpointsCSV)
	if len(remoteEndpoints) == 0 {
		klog.Warning("No remote clusters configured. Running in local-only mode.")
	}
	klog.Infof("Configured %d remote cluster(s): %v", len(remoteEndpoints), remoteEndpoints)

	// Parse cost factors
	costFactors := parseCostFactors(costFactorsCSV)
	klog.Infof("Cost factors: %v", costFactors)

	stopCh := signals.SetupSignalHandler()

	cfg := buildKubeConfig()
	kubeClient := kubernetes.NewForConfigOrDie(cfg)
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building dynamic client: %v", err)
	}

	preflightChecks(kubeClient)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 30*time.Second)
	podInformer := informerFactory.Core().V1().Pods()
	nodeInformer := informerFactory.Core().V1().Nodes()

	// Local telemetry
	localCollector := telemetry.NewLocalCollector(kubeClient, podInformer, nodeInformer, int64(config.EdgePendingPessimismPct))

	// WAN probes — one per remote cluster
	wanProbes := make(map[constants.ClusterID]*telemetry.WANProbe, len(remoteEndpoints))
	for clusterID, endpoint := range remoteEndpoints {
		wanProbes[clusterID] = telemetry.NewWANProbe(endpoint, 5*time.Second)
	}

	telemetryCollector := telemetry.NewCombinedCollector(localCollector, wanProbes)

	klog.Info("Starting informers...")
	informerFactory.Start(stopCh)

	klog.Info("Waiting for informer caches to sync...")
	if !cache.WaitForCacheSync(stopCh, podInformer.Informer().HasSynced, nodeInformer.Informer().HasSynced) {
		klog.Fatal("Failed to sync informer caches")
	}
	klog.Info("Informer caches synced")

	go refreshTelemetryLoop(telemetryCollector, stopCh)

	profileStore := decision.NewProfileStore(kubeClient, config.MaxProfileCount, hcfg)
	if err := profileStore.LoadFromCRD(dynClient); err != nil {
		klog.Warningf("Failed to load profiles from CRD: %v", err)
	}

	lyapunovScheduler := initializeLyapunovScheduler()

	engineConfig := decision.EngineConfig{
		RTTUnusableMs:           config.RTTUnusableMs,
		LossUnusablePct:         config.LossUnusablePct,
		MaxProfileCount:         config.MaxProfileCount,
		EdgePendingPessimismPct: config.EdgePendingPessimismPct,
		ProfileStore:            profileStore,
		LyapunovScheduler:       lyapunovScheduler,
		CostFactors:             costFactors,
	}
	decisionEngine := decision.NewEngine(engineConfig)

	podObserver := decision.NewPodObserver(kubeClient, profileStore, podInformer, lyapunovScheduler)
	go podObserver.Watch(stopCh)

	go profileStore.StartAutoSave(dynClient, 5*time.Minute, stopCh)
	go startMetricsUpdater(profileStore, telemetryCollector, lyapunovScheduler, stopCh)

	adminSrv := startAdminServer(podInformer, nodeInformer, telemetryCollector, profileStore, lyapunovScheduler)
	defer shutdownServer(adminSrv, "admin")

	webhookSrv := startWebhookServer(decisionEngine, telemetryCollector, kubeClient)
	defer shutdownServer(webhookSrv, "webhook")

	<-stopCh
	klog.Info("Shutting down gracefully")
}

// parseEndpoints parses "cloud-1=10.0.1.100,cloud-2=10.0.2.100" into a map.
func parseEndpoints(csv string) map[constants.ClusterID]string {
	result := make(map[constants.ClusterID]string)
	if csv == "" {
		return result
	}
	for _, pair := range strings.Split(csv, ",") {
		pair = strings.TrimSpace(pair)
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			klog.Warningf("Invalid endpoint pair: %q", pair)
			continue
		}
		result[constants.ClusterID(strings.TrimSpace(parts[0]))] = strings.TrimSpace(parts[1])
	}
	return result
}

// parseCostFactors parses "local=0,cloud-1=1.0,cloud-2=1.5"
func parseCostFactors(csv string) map[constants.ClusterID]float64 {
	result := make(map[constants.ClusterID]float64)
	if csv == "" {
		return result
	}
	for _, pair := range strings.Split(csv, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) != 2 {
			continue
		}
		f := util.GetEnvFloat(parts[1], 1.0) // reuse parsing
		result[constants.ClusterID(parts[0])] = f
	}
	return result
}

func shutdownServer(srv *http.Server, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		klog.Errorf("%s server shutdown failed: %v", name, err)
	}
}

func setupFlags() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig.")
	flag.StringVar(&masterURL, "master", "", "Kubernetes API server address.")
	flag.StringVar(&remoteEndpointsCSV, "remote-endpoints",
		util.GetEnvOrDefault("REMOTE_ENDPOINTS", ""),
		"Comma-separated remote clusters: cloud-1=10.0.1.100,cloud-2=10.0.2.100")

	flag.IntVar(&config.RTTUnusableMs, "rtt-unusable", util.GetEnvInt("RTT_UNUSABLE", 300), "WAN RTT unusable threshold (ms).")
	flag.Float64Var(&config.LossUnusablePct, "loss-unusable", util.GetEnvFloat("LOSS_UNUSABLE", 10.0), "WAN loss unusable threshold (%).")
	flag.IntVar(&config.MaxProfileCount, "max-profiles", util.GetEnvInt("MAX_PROFILES", 100), "Max profile entries (LRU).")
	flag.IntVar(&config.EdgePendingPessimismPct, "edge-pending-pessimism-pct", util.GetEnvInt("EDGE_PENDING_PESSIMISM_PCT", 10), "Edge pending pessimism %.")

	flag.Float64Var(&lyapunovBeta, "lyapunov-beta", util.GetEnvFloat("LYAPUNOV_BETA", 1.0), "Lyapunov beta.")
	flag.StringVar(&costFactorsCSV, "cost-factors",
		util.GetEnvOrDefault("COST_FACTORS", "local=0"),
		"Cost factors per cluster: local=0,cloud-1=1.0,cloud-2=1.5")

	flag.StringVar(&histBoundsModeStr, "hist-bounds-mode", util.GetEnvOrDefault("HIST_BOUNDS_MODE", "explicit"), "Histogram bounds mode.")
	flag.StringVar(&histBoundsCSV, "hist-bounds", util.GetEnvOrDefault("HIST_BOUNDS", ""), "Histogram bounds CSV.")
	flag.Float64Var(&hcfg.LogStartMs, "hist-log-start-ms", util.GetEnvFloat("HIST_LOG_START_MS", 50), "Log mode start.")
	flag.Float64Var(&hcfg.LogFactor, "hist-log-factor", util.GetEnvFloat("HIST_LOG_FACTOR", 2.0), "Log mode factor.")
	flag.IntVar(&hcfg.LogCount, "hist-log-count", util.GetEnvInt("HIST_LOG_COUNT", 20), "Log mode count.")
	flag.BoolVar(&hcfg.IncludeInf, "hist-include-inf", util.GetEnvOrDefault("HIST_INCLUDE_INF", "1") != "0", "Include +Inf bucket.")
	flag.Float64Var(&hcfg.IngestCapMs, "ingest-cap-ms", util.GetEnvFloat("INGEST_CAP_MS", 30*60*1000), "Ingest cap ms.")
	flag.IntVar(&hcfg.MinSampleCount, "min-sample-count", util.GetEnvInt("MIN_SAMPLE_COUNT", 10), "Min samples for p95.")
	flag.StringVar(&decayIntervalStr, "decay-interval", util.GetEnvOrDefault("DECAY_INTERVAL", "1h"), "Decay interval.")
}

func postProcessFlags() error {
	switch histBoundsModeStr {
	case "explicit":
		hcfg.Mode = decision.BoundsExplicit
	case "log":
		hcfg.Mode = decision.BoundsLog
	default:
		hcfg.Mode = decision.BoundsExplicit
	}

	if histBoundsCSV != "" {
		hcfg.Explicit = decision.ParseFloatSliceCSV(histBoundsCSV)
	} else {
		hcfg.Explicit = decision.DefaultHistogramConfig().Explicit
	}

	iv, err := time.ParseDuration(decayIntervalStr)
	if err != nil {
		hcfg.DecayInterval = time.Hour
	} else {
		hcfg.DecayInterval = iv
	}
	return nil
}

func buildKubeConfig() *rest.Config {
	var cfg *rest.Config
	var err error
	if masterURL == "" && kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	}
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %v", err)
	}
	return cfg
}

func preflightChecks(kubeClient kubernetes.Interface) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		klog.Warningf("Preflight: listing nodes failed: %v", err)
	}
	klog.Info("Preflight checks completed")
}

func refreshTelemetryLoop(tel telemetry.Collector, stopCh <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Initial refresh for all clusters
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	for _, id := range tel.GetAllClusterIDs() {
		_, _ = tel.GetClusterState(ctx, id)
	}
	cancel()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			for _, id := range tel.GetAllClusterIDs() {
				_, _ = tel.GetClusterState(ctx, id)
			}
			cancel()
		case <-stopCh:
			return
		}
	}
}

func initializeLyapunovScheduler() *decision.LyapunovScheduler {
	ls := decision.NewLyapunovScheduler()

	classConfigs := map[string]sloClassConfig{
		"latency":     {0.05, 0.05, 0.95, time.Hour, 1.0, 1.0},
		"interactive": {0.05, 0.05, 0.95, time.Hour, 1.0, 1.0},
		"throughput":  {0.10, 0.10, 0.95, time.Hour, 0.8, 1.0},
		"streaming":   {0.10, 0.10, 0.95, time.Hour, 0.8, 1.0},
		"batch":       {0.20, 0.20, 0.90, 2 * time.Hour, 0.5, 0.5},
	}

	for class, cfg := range classConfigs {
		ls.SetClassConfig(class, &decision.ClassConfig{
			Beta:                lyapunovBeta * cfg.betaFactor,
			TargetViolationPct:  cfg.targetViolationPct,
			TargetViolationProb: cfg.targetViolationProb,
			DecayFactor:         cfg.decayFactor,
			DecayInterval:       cfg.decayInterval,
			ProbabilityWeight:   cfg.probabilityWeight,
		})
	}
	return ls
}

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

func startAdminServer(
	podInformer v1.PodInformer,
	nodeInformer v1.NodeInformer,
	tel telemetry.Collector,
	ps *decision.ProfileStore,
	lyap *decision.LyapunovScheduler,
) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", readyHandlerFactory(
		podInformer.Informer().HasSynced,
		nodeInformer.Informer().HasSynced,
	))
	mux.HandleFunc("/debug/profiles", func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(w, ps.ExportAllProfiles())
	})
	mux.HandleFunc("/debug/lyapunov", func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(w, lyap.ExportState())
	})

	srv := &http.Server{Addr: ":8080", Handler: mux, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second}
	go func() {
		klog.Info("Starting admin server on :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Fatalf("Admin server failed: %v", err)
		}
	}()
	return srv
}

func startWebhookServer(dec *decision.Engine, tel telemetry.Collector, kubeClient kubernetes.Interface) *http.Server {
	limiter := rate.NewLimiter(rate.Limit(100), 200)
	wh := webhook.NewServer(dec, tel, limiter, kubeClient)
	mux := http.NewServeMux()
	mux.Handle("/mutate", wh)

	srv := &http.Server{Addr: ":8443", Handler: mux, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second}
	go func() {
		klog.Info("Starting webhook server on :8443")
		if err := srv.ListenAndServeTLS("/certs/tls.crt", "/certs/tls.key"); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Fatalf("Webhook server failed: %v", err)
		}
	}()
	return srv
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

func writeJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}
