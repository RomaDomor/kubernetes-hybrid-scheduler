package decision

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// ProfileKey uniquely identifies a workload profile bucket
type ProfileKey struct {
	Class    string   // "latency", "throughput", "batch"
	CPUTier  string   // "small", "medium", "large"
	Location Location // Edge or Cloud
}

func (pk ProfileKey) String() string {
	return fmt.Sprintf("%s-%s-%s", pk.Class, pk.CPUTier, pk.Location)
}

// ProfileStats tracks statistical performance data
type ProfileStats struct {
	// Execution time statistics
	Count            int     `json:"count"`
	MeanDurationMs   float64 `json:"mean_duration_ms"`
	StdDevDurationMs float64 `json:"stddev_duration_ms"`
	P95DurationMs    float64 `json:"p95_duration_ms"`

	// Queue wait statistics
	MeanQueueWaitMs float64 `json:"mean_queue_wait_ms"`

	// Success metrics
	SLOComplianceRate float64 `json:"slo_compliance_rate"`

	// Confidence (0-1 based on sample size)
	ConfidenceScore float64   `json:"confidence_score"`
	LastUpdated     time.Time `json:"last_updated"`
}

// ProfileUpdate represents new observed data
type ProfileUpdate struct {
	ObservedDurationMs float64
	QueueWaitMs        float64
	SLOMet             bool
}

// ProfileStore manages learned workload profiles
type ProfileStore struct {
	profiles map[string]*ProfileStats
	mu       sync.RWMutex
	defaults map[string]*ProfileStats // Cold-start defaults
}

func NewProfileStore() *ProfileStore {
	ps := &ProfileStore{
		profiles: make(map[string]*ProfileStats),
		defaults: defaultProfiles(),
	}
	return ps
}

// GetProfileKey determines the bucket for a pod
func GetProfileKey(pod *corev1.Pod, loc Location) ProfileKey {
	cpuMillis := getCPURequest(pod)

	tier := "medium"
	if cpuMillis < 500 {
		tier = "small"
	} else if cpuMillis > 2000 {
		tier = "large"
	}

	class := pod.Annotations["slo.hybrid.io/class"]
	if class == "" {
		class = "batch" // Default fallback
	}

	return ProfileKey{
		Class:    class,
		CPUTier:  tier,
		Location: loc,
	}
}

// GetOrDefault retrieves profile stats or returns conservative defaults
func (ps *ProfileStore) GetOrDefault(key ProfileKey) *ProfileStats {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if profile, exists := ps.profiles[key.String()]; exists {
		return profile
	}

	// Return default based on class and tier
	defaultKey := fmt.Sprintf("%s-%s", key.Class, key.CPUTier)
	if def, exists := ps.defaults[defaultKey]; exists {
		return def
	}

	// Ultimate fallback
	return &ProfileStats{
		MeanDurationMs:    100,
		P95DurationMs:     200,
		ConfidenceScore:   0.0,
		MeanQueueWaitMs:   0,
		SLOComplianceRate: 0.5,
	}
}

// Update applies exponential moving average to statistics
func (ps *ProfileStore) Update(key ProfileKey, update ProfileUpdate) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	keyStr := key.String()
	profile, exists := ps.profiles[keyStr]
	if !exists {
		profile = ps.GetOrDefault(key)
		// Deep copy to avoid modifying defaults
		profileCopy := *profile
		profile = &profileCopy
		ps.profiles[keyStr] = profile
	}

	// Exponential moving average weight (alpha = 0.2 for ~5-sample window)
	alpha := 0.2

	// Update mean duration
	oldMean := profile.MeanDurationMs
	profile.MeanDurationMs = alpha*update.ObservedDurationMs + (1-alpha)*oldMean

	// Update variance (Welford's online algorithm approximation)
	diff := update.ObservedDurationMs - profile.MeanDurationMs
	profile.StdDevDurationMs = math.Sqrt(
		alpha*diff*diff + (1-alpha)*profile.StdDevDurationMs*profile.StdDevDurationMs,
	)

	// Update P95 (approximation: mean + 1.65*stddev)
	profile.P95DurationMs = profile.MeanDurationMs + 1.65*profile.StdDevDurationMs

	// Update queue wait
	profile.MeanQueueWaitMs = alpha*update.QueueWaitMs + (1-alpha)*profile.MeanQueueWaitMs

	// Update SLO compliance rate
	sloMetric := 0.0
	if update.SLOMet {
		sloMetric = 1.0
	}
	profile.SLOComplianceRate = alpha*sloMetric + (1-alpha)*profile.SLOComplianceRate

	// Update confidence (caps at 1.0 after 20 samples)
	profile.Count++
	profile.ConfidenceScore = math.Min(1.0, float64(profile.Count)/20.0)

	profile.LastUpdated = time.Now()

	klog.V(4).Infof("Profile updated: %s count=%d conf=%.2f mean=%.1fms p95=%.1fms slo=%.2f%%",
		keyStr, profile.Count, profile.ConfidenceScore,
		profile.MeanDurationMs, profile.P95DurationMs,
		profile.SLOComplianceRate*100)
}

// SaveToConfigMap persists profiles for restart resilience
func (ps *ProfileStore) SaveToConfigMap(
	clientset kubernetes.Interface,
) error {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	data, err := json.Marshal(ps.profiles)
	if err != nil {
		return fmt.Errorf("marshal profiles: %w", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scheduler-profiles",
			Namespace: "kube-system",
		},
		Data: map[string]string{
			"profiles.json": string(data),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = clientset.CoreV1().ConfigMaps("kube-system").Update(ctx, cm, metav1.UpdateOptions{})
	if err != nil {
		// Try create if update fails
		_, err = clientset.CoreV1().ConfigMaps("kube-system").Create(ctx, cm, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("save configmap: %w", err)
		}
	}

	klog.V(3).Infof("Saved %d profiles to ConfigMap", len(ps.profiles))
	return nil
}

// LoadFromConfigMap restores profiles from persistent storage
func (ps *ProfileStore) LoadFromConfigMap(
	clientset kubernetes.Interface,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cm, err := clientset.CoreV1().ConfigMaps("kube-system").Get(
		ctx, "scheduler-profiles", metav1.GetOptions{},
	)
	if err != nil {
		return fmt.Errorf("load configmap: %w", err)
	}

	data := cm.Data["profiles.json"]
	if data == "" {
		return fmt.Errorf("empty profiles data")
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if err := json.Unmarshal([]byte(data), &ps.profiles); err != nil {
		return fmt.Errorf("unmarshal profiles: %w", err)
	}

	klog.Infof("Loaded %d profiles from ConfigMap", len(ps.profiles))
	return nil
}

// StartAutoSave periodically persists profiles
func (ps *ProfileStore) StartAutoSave(
	clientset kubernetes.Interface,
	interval time.Duration,
	stopCh <-chan struct{},
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := ps.SaveToConfigMap(clientset); err != nil {
				klog.Warningf("Failed to auto-save profiles: %v", err)
			}
		case <-stopCh:
			// Final save before shutdown
			if err := ps.SaveToConfigMap(clientset); err != nil {
				klog.Errorf("Failed to save profiles on shutdown: %v", err)
			}
			return
		}
	}
}

// defaultProfiles returns conservative initial estimates
func defaultProfiles() map[string]*ProfileStats {
	profiles := make(map[string]*ProfileStats)

	classes := []string{"latency", "throughput", "batch"}
	tiers := []string{"small", "medium", "large"}
	estimates := map[string]struct{ mean, p95 float64 }{
		"latency-small":     {50, 80},
		"latency-medium":    {100, 160},
		"latency-large":     {200, 320},
		"throughput-small":  {200, 300},
		"throughput-medium": {400, 600},
		"throughput-large":  {800, 1200},
		"batch-small":       {500, 800},
		"batch-medium":      {1000, 1600},
		"batch-large":       {2000, 3200},
	}

	for _, class := range classes {
		for _, tier := range tiers {
			key := fmt.Sprintf("%s-%s", class, tier)
			est, ok := estimates[key]
			if !ok {
				est = struct{ mean, p95 float64 }{100, 200}
			}

			profiles[key] = &ProfileStats{
				Count:             0,
				MeanDurationMs:    est.mean,
				StdDevDurationMs:  (est.p95 - est.mean) / 1.65,
				P95DurationMs:     est.p95,
				MeanQueueWaitMs:   0,
				SLOComplianceRate: 0.5,
				ConfidenceScore:   0.0,
				LastUpdated:       time.Now(),
			}
		}
	}

	return profiles
}
