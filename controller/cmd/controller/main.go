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

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	flag.StringVar(&masterURL, "master", "", "Kubernetes API server URL")
	flag.StringVar(&cloudEndpoint, "cloud-endpoint",
		getEnvOrDefault("CLOUD_ENDPOINT", "10.0.1.100"),
		"Cloud endpoint IP for WAN probe")
	flag.IntVar(&rttThreshold, "rtt-threshold",
		getEnvInt("RTT_THRESHOLD", 100),
		"WAN RTT threshold (ms)")
	flag.Float64Var(&lossThreshold, "loss-threshold",
		getEnvFloat("LOSS_THRESHOLD", 2.0),
		"WAN packet loss threshold (%)")
	flag.Parse()

	stopCh := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %v", err)
	}
	kubeClient := kubernetes.NewForConfigOrDie(cfg)
	metricsClient := metricsv.NewForConfigOrDie(cfg)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 30*time.Second)
	podInformer := informerFactory.Core().V1().Pods()
	nodeInformer := informerFactory.Core().V1().Nodes()

	klog.Info("Starting informers")
	informerFactory.Start(stopCh)
	synced := cache.WaitForCacheSync(
		stopCh,
		podInformer.Informer().HasSynced,
		nodeInformer.Informer().HasSynced,
	)
	if !synced {
		klog.Errorf("Cache sync failed. podsSynced=%v nodesSynced=%v",
			podInformer.Informer().HasSynced(),
			nodeInformer.Informer().HasSynced(),
		)
		klog.Fatal("Failed to sync informer caches")
	}
	klog.Info("Informer caches synced")

	localCollector := telemetry.NewLocalCollector(kubeClient, metricsClient, podInformer, nodeInformer)
	wanProbe := telemetry.NewWANProbe(cloudEndpoint, 60*time.Second)
	telemetryCollector := telemetry.NewCombinedCollector(localCollector, wanProbe)

	go refreshTelemetryLoop(telemetryCollector, stopCh)

	decisionEngine := decision.NewEngine(decision.Config{
		RTTThresholdMs:   rttThreshold,
		LossThresholdPct: lossThreshold,
	})

	// TLS webhook server (8443)
	wh := webhook.NewServer(decisionEngine, telemetryCollector)
	webhookMux := http.NewServeMux()
	webhookMux.Handle("/mutate", wh)
	webhookMux.HandleFunc("/healthz", healthHandler)
	webhookMux.HandleFunc("/readyz", readyHandler)

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

	// Plain HTTP admin server (8080) for metrics and debug endpoints
	adminMux := http.NewServeMux()
	adminMux.Handle("/metrics", promhttp.Handler())
	adminMux.HandleFunc("/debug/telemetry", func(w http.ResponseWriter, r *http.Request) {
		// Provide current cached telemetry in JSON
		state := struct {
			Local *telemetry.LocalState `json:"local"`
			WAN   *telemetry.WANState   `json:"wan"`
			Time  time.Time             `json:"timestamp"`
		}{
			Local: telemetryCollector.GetCachedLocalState(),
			WAN:   telemetryCollector.GetCachedWANState(),
			Time:  time.Now(),
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(state)
	})

	adminSrv := &http.Server{
		Addr:         ":8080",
		Handler:      adminMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		klog.Info("Starting admin server on :8080 (HTTP) for /metrics and /debug/telemetry")
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Fatalf("Admin server failed: %v", err)
		}
	}()

	<-stopCh
	klog.Info("Shutting down gracefully")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = webhookSrv.Shutdown(ctx)
	_ = adminSrv.Shutdown(ctx)
}

func refreshTelemetryLoop(tel telemetry.Collector, stopCh <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if _, err := tel.GetLocalState(ctx); err != nil {
				klog.V(4).Infof("Background telemetry refresh: %v", err)
			}
			_, _ = tel.GetWANState(ctx) // refresh WAN too; ignore error (cache will be used)
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

func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
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
