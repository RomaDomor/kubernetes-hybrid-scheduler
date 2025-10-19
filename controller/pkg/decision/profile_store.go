package decision

import (
	"container/list"
	"context"
	"fmt"
	constants2 "kubernetes-hybrid-scheduler/controller/pkg/constants"
	"math"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

var workloadProfileGVR = schema.GroupVersionResource{
	Group:    "scheduling.hybrid.io",
	Version:  "v1alpha1",
	Resource: "workloadprofiles",
}

type ProfileKey struct {
	Class    string
	CPUTier  string
	Location Location
}

func (pk ProfileKey) String() string {
	return fmt.Sprintf("%s-%s-%s", pk.Class, pk.CPUTier, pk.Location)
}

// Histogram-based P95 tracking
type ProfileStats struct {
	Count             int       `json:"count"`
	MeanDurationMs    float64   `json:"mean_duration_ms"`
	StdDevDurationMs  float64   `json:"stddev_duration_ms"`
	P95DurationMs     float64   `json:"p95_duration_ms"`
	MeanQueueWaitMs   float64   `json:"mean_queue_wait_ms"`
	SLOComplianceRate float64   `json:"slo_compliance_rate"`
	ConfidenceScore   float64   `json:"confidence_score"`
	LastUpdated       time.Time `json:"last_updated"`

	DurationHistogram []HistogramBucket `json:"duration_histogram"`
}

type HistogramBucket struct {
	UpperBoundValue float64   `json:"upper_bound_value"`
	UpperBoundStr   string    `json:"upper_bound_str"`
	Count           int       `json:"count"`
	LastDecay       time.Time `json:"last_decay"`
}

func (hb *HistogramBucket) UpperBound() float64 {
	if hb.UpperBoundStr == "Inf" {
		return math.Inf(1)
	}
	return hb.UpperBoundValue
}

func (hb *HistogramBucket) SetUpperBound(val float64) {
	if math.IsInf(val, +1) {
		hb.UpperBoundStr = "Inf"
		hb.UpperBoundValue = 0
	} else {
		hb.UpperBoundStr = ""
		hb.UpperBoundValue = val
	}
}

type ProfileUpdate struct {
	ObservedDurationMs float64
	QueueWaitMs        float64
	SLOMet             bool
}

// LRU eviction for bounded growth
type lruEntry struct {
	key   string
	stats *ProfileStats
}

type ProfileStore struct {
	profiles   map[string]*ProfileStats
	lru        *list.List
	lruMap     map[string]*list.Element
	mu         sync.RWMutex
	defaults   map[string]*ProfileStats
	maxEntries int
	kubeClient kubernetes.Interface
	hcfg       HistogramConfig
}

func NewProfileStore(kubeClient kubernetes.Interface, maxEntries int, hcfg HistogramConfig) *ProfileStore {
	ps := &ProfileStore{
		profiles:   make(map[string]*ProfileStats),
		lru:        list.New(),
		lruMap:     make(map[string]*list.Element),
		defaults:   make(map[string]*ProfileStats),
		maxEntries: maxEntries,
		kubeClient: kubeClient,
		hcfg:       hcfg,
	}
	// build defaults with histogram
	for k, v := range defaultProfilesStatic() {
		cp := *v
		cp.DurationHistogram = ps.initHistogram()
		ps.defaults[k] = &cp
	}
	return ps
}

func GetProfileKey(pod *corev1.Pod, loc Location) ProfileKey {
	cpuMillis := getCPURequest(pod)

	tier := "medium"
	if cpuMillis < 500 {
		tier = "small"
	} else if cpuMillis > 2000 {
		tier = "large"
	}

	class := pod.Annotations[constants2.AnnotationSLOClass]
	if class == "" {
		class = "batch"
	}

	// Normalize class to prevent explosion
	class = normalizeClass(class)

	return ProfileKey{
		Class:    class,
		CPUTier:  tier,
		Location: loc,
	}
}

// Whitelist of allowed classes
func normalizeClass(class string) string {
	if constants2.ValidSLOClasses[class] {
		return class
	}

	klog.V(4).Infof("Unknown class '%s', defaulting to '%s'", class, constants2.DefaultSLOClass)
	// Use centralized constant for default
	return constants2.DefaultSLOClass
}

func (ps *ProfileStore) GetOrDefault(key ProfileKey) *ProfileStats {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	keyStr := key.String()

	if profile, exists := ps.profiles[keyStr]; exists {
		return deepCopyProfile(profile)
	}

	defaultKey := fmt.Sprintf("%s-%s", key.Class, key.CPUTier)
	if def, exists := ps.defaults[defaultKey]; exists {
		return deepCopyProfile(def)
	}

	return deepCopyProfile(&ProfileStats{
		MeanDurationMs:    100,
		P95DurationMs:     200,
		ConfidenceScore:   0.0,
		SLOComplianceRate: 0.5,
		DurationHistogram: ps.initHistogram(),
	})
}

func deepCopyProfile(p *ProfileStats) *ProfileStats {
	if p == nil {
		return nil
	}

	pCopy := *p
	pCopy.DurationHistogram = make([]HistogramBucket, len(p.DurationHistogram))
	for i := range p.DurationHistogram {
		pCopy.DurationHistogram[i] = p.DurationHistogram[i]
	}

	return &pCopy
}

func (ps *ProfileStore) Update(key ProfileKey, update ProfileUpdate) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	keyStr := key.String()

	ingestCapMs := ps.hcfg.IngestCapMs
	dur := update.ObservedDurationMs
	if dur < 0 || dur > ingestCapMs {
		klog.V(4).Infof(
			"Discarding outlier duration %.0fms for %s (capped at %.0fms)",
			dur, keyStr, ingestCapMs,
		)
		dur = ingestCapMs
	}

	profile, exists := ps.profiles[keyStr]

	if !exists {
		if len(ps.profiles) >= ps.maxEntries {
			ps.evictLRU()
		}

		profile = ps.getOrDefaultLocked(key)
		ps.profiles[keyStr] = profile
		ps.lruMap[keyStr] = ps.lru.PushFront(&lruEntry{key: keyStr, stats: profile})
	} else {
		if elem, ok := ps.lruMap[keyStr]; ok {
			ps.lru.MoveToFront(elem)
		}
	}

	alpha := 0.2

	oldMean := profile.MeanDurationMs
	profile.MeanDurationMs = alpha*dur + (1-alpha)*oldMean

	diff := dur - profile.MeanDurationMs
	profile.StdDevDurationMs = math.Sqrt(
		alpha*diff*diff + (1-alpha)*profile.StdDevDurationMs*profile.StdDevDurationMs,
	)

	ps.updateHistogram(profile.DurationHistogram, dur)
	profile.P95DurationMs = ps.computeP95FromHistogram(profile.DurationHistogram)

	profile.MeanQueueWaitMs = alpha*update.QueueWaitMs + (1-alpha)*profile.MeanQueueWaitMs

	sloMetric := 0.0
	if update.SLOMet {
		sloMetric = 1.0
	}
	profile.SLOComplianceRate = alpha*sloMetric + (1-alpha)*profile.SLOComplianceRate

	profile.Count++
	profile.ConfidenceScore = math.Min(1.0, float64(profile.Count)/20.0)
	profile.LastUpdated = time.Now()

	klog.V(4).Infof(
		"Profile updated: %s count=%d conf=%.2f mean=%.1fms p95=%.1fms slo=%.2f%%",
		keyStr, profile.Count, profile.ConfidenceScore,
		profile.MeanDurationMs, profile.P95DurationMs,
		profile.SLOComplianceRate*100,
	)
}

// LRU eviction
func (ps *ProfileStore) evictLRU() {
	if ps.lru.Len() == 0 {
		return
	}

	elem := ps.lru.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*lruEntry)
	ps.lru.Remove(elem)
	delete(ps.lruMap, entry.key)
	delete(ps.profiles, entry.key)

	klog.V(3).Infof("Evicted profile %s from LRU cache", entry.key)
}

func (ps *ProfileStore) getOrDefaultLocked(key ProfileKey) *ProfileStats {
	defaultKey := fmt.Sprintf("%s-%s", key.Class, key.CPUTier)
	if def, exists := ps.defaults[defaultKey]; exists {
		pCopy := deepCopyProfile(def)
		pCopy.Count = 0
		pCopy.ConfidenceScore = 0.0
		return pCopy
	}

	return &ProfileStats{
		MeanDurationMs:    100,
		P95DurationMs:     200,
		StdDevDurationMs:  50,
		ConfidenceScore:   0.0,
		SLOComplianceRate: 0.5,
		DurationHistogram: ps.initHistogram(),
	}
}

// Histogram helpers
func (ps *ProfileStore) initHistogram() []HistogramBucket {
	var bounds []float64
	switch ps.hcfg.Mode {
	case BoundsExplicit:
		bounds = append([]float64(nil), ps.hcfg.Explicit...)
	case BoundsLog:
		bounds = make([]float64, 0, ps.hcfg.LogCount)
		v := ps.hcfg.LogStartMs
		for i := 0; i < ps.hcfg.LogCount; i++ {
			bounds = append(bounds, v)
			v *= ps.hcfg.LogFactor
		}
	default:
		bounds = append([]float64(nil), DefaultHistogramConfig().Explicit...)
	}
	if ps.hcfg.IncludeInf {
		bounds = append(bounds, math.Inf(1))
	}
	buckets := make([]HistogramBucket, len(bounds))
	now := time.Now()
	for i, bound := range bounds {
		buckets[i] = HistogramBucket{
			Count:     0,
			LastDecay: now,
		}
		buckets[i].SetUpperBound(bound)
	}
	return buckets
}

func (ps *ProfileStore) updateHistogram(buckets []HistogramBucket, value float64) {
	now := time.Now()

	// Decay every hour (half-life)
	for i := range buckets {
		if now.Sub(buckets[i].LastDecay) > ps.hcfg.DecayInterval {
			buckets[i].Count = buckets[i].Count / 2
			buckets[i].LastDecay = now
		}
	}

	// Use UpperBound() helper to handle Inf properly
	for i := range buckets {
		if value <= buckets[i].UpperBound() {
			buckets[i].Count++
			return
		}
	}

	buckets[len(buckets)-1].Count++
}

func (ps *ProfileStore) computeP95FromHistogram(buckets []HistogramBucket) float64 {
	total := 0
	for _, b := range buckets {
		total += b.Count
	}
	if total == 0 {
		return 150 // Default
	}

	// Require minimum sample size for trustworthy p95
	minSampleCount := ps.hcfg.MinSampleCount
	capMs := ps.lastFiniteUpperBound(buckets)
	const lowSampleFloor = 50.0

	if total < minSampleCount {
		// Try to use max observed bucket with data
		for i := len(buckets) - 1; i >= 0; i-- {
			if buckets[i].Count > 0 {
				ub := buckets[i].UpperBound()
				var prev float64
				if i > 0 {
					prev = buckets[i-1].UpperBound()
				}
				if math.IsInf(ub, +1) {
					if i > 0 {
						ub = math.Min(buckets[i-1].UpperBound()*1.5, capMs)
					} else {
						ub = lowSampleFloor
					}
				}
				// choose midpoint to avoid biasing to ub for sparse data
				if i > 0 {
					return math.Max(lowSampleFloor, (prev+ub)/2)
				}
				return math.Max(lowSampleFloor, ub)
			}
		}

		return lowSampleFloor
	}

	target := int(math.Ceil(0.95 * float64(total)))
	cum := 0

	for i, b := range buckets {
		cum += b.Count
		if cum >= target {
			// Determine bounds for interpolation
			var prev float64
			if i > 0 {
				prev = buckets[i-1].UpperBound()
			} else {
				prev = 0
			}

			ub := buckets[i].UpperBound()

			// If ub is +Inf, use a capped synthetic upper bound
			if math.IsInf(ub, +1) {
				if prev <= 0 {
					return capMs
				}
				// Use min(prev * 2, capMs) as synthetic upper bound
				ub = math.Min(prev*2, capMs)
			}

			// Avoid division by zero
			if b.Count == 0 {
				return math.Min(ub, capMs)
			}

			// Linear interpolation within bucket
			ratio := float64(target-(cum-b.Count)) / float64(b.Count)
			p := prev + ratio*(ub-prev)

			// Clamp to cap
			if p > capMs {
				p = capMs
			}
			if p > ub {
				p = ub
			}

			// Ensure non-decreasing vs previous bound
			if p < prev {
				p = prev
			}
			return p
		}
	}

	return ps.lastFiniteUpperBound(buckets)
}
func (ps *ProfileStore) lastFiniteUpperBound(buckets []HistogramBucket) float64 {
	for i := len(buckets) - 1; i >= 0; i-- {
		ub := buckets[i].UpperBound()
		if !math.IsInf(ub, +1) {
			return ub
		}
	}
	return ps.hcfg.IngestCapMs
}

// CRD-based persistence (clean: no custom JSON, uses sentinel fields)
func (ps *ProfileStore) SaveToCRD(dynClient dynamic.Interface) error {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res := dynClient.Resource(workloadProfileGVR).Namespace("kube-system")
	saved := 0

	for keyStr, profile := range ps.profiles {
		// Convert to unstructured map directly
		profileMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(profile)
		if err != nil {
			klog.V(4).Infof("Convert ProfileStats to map for %s failed: %v", keyStr, err)
			continue
		}

		existing, err := res.Get(ctx, keyStr, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				// Create new object
				obj := &unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "scheduling.hybrid.io/v1alpha1",
						"kind":       "WorkloadProfile",
						"metadata": map[string]interface{}{
							"name":      keyStr,
							"namespace": "kube-system",
						},
						"spec": map[string]interface{}{
							"profile": profileMap,
						},
					},
				}
				if _, err := res.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
					klog.V(4).Infof("Create WorkloadProfile %s failed: %v", keyStr, err)
					continue
				}
				saved++
				continue
			}
			// Other get error
			klog.V(4).Infof("Get WorkloadProfile %s failed: %v", keyStr, err)
			continue
		}

		// Update existing
		obj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "scheduling.hybrid.io/v1alpha1",
				"kind":       "WorkloadProfile",
				"metadata": map[string]interface{}{
					"name":            keyStr,
					"namespace":       "kube-system",
					"resourceVersion": existing.GetResourceVersion(),
				},
				"spec": map[string]interface{}{
					"profile": profileMap,
				},
			},
		}

		if _, err := res.Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
			if errors.IsConflict(err) {
				klog.V(4).Infof("Update conflict for %s, will retry next cycle", keyStr)
				continue
			}
			klog.V(4).Infof("Update WorkloadProfile %s failed: %v", keyStr, err)
			continue
		}
		saved++
	}

	klog.V(3).Infof("Saved %d profiles to CRDs", saved)
	return nil
}

func (ps *ProfileStore) LoadFromCRD(dynClient dynamic.Interface) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dList, err := dynClient.Resource(workloadProfileGVR).Namespace("kube-system").
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list CRDs: %w", err)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, item := range dList.Items {
		keyStr := item.GetName()
		specMap, found, _ := unstructured.NestedMap(item.Object, "spec", "profile")
		if !found {
			continue
		}

		profile := &ProfileStats{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(specMap, profile); err != nil {
			klog.V(4).Infof("Convert specMap to ProfileStats for %s failed: %v", keyStr, err)
			continue
		}

		ps.profiles[keyStr] = profile
		ps.lruMap[keyStr] = ps.lru.PushFront(&lruEntry{key: keyStr, stats: profile})
	}

	klog.Infof("Loaded %d profiles from CRDs", len(ps.profiles))
	return nil
}

func (ps *ProfileStore) StartAutoSave(
	dynClient dynamic.Interface,
	interval time.Duration,
	stopCh <-chan struct{},
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := ps.SaveToCRD(dynClient); err != nil {
				klog.Warningf("Failed to auto-save profiles: %v", err)
			}
		case <-stopCh:
			if err := ps.SaveToCRD(dynClient); err != nil {
				klog.Errorf("Failed to save profiles on shutdown: %v", err)
			}
			return
		}
	}
}

func (ps *ProfileStore) ExportAllProfiles() map[string]*ProfileStats {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	export := make(map[string]*ProfileStats, len(ps.profiles))
	for k, v := range ps.profiles {
		export[k] = deepCopyProfile(v)
	}
	return export
}

func defaultProfilesStatic() map[string]*ProfileStats {
	profiles := make(map[string]*ProfileStats)

	classes := make([]string, 0, len(constants2.ValidSLOClasses))
	for class := range constants2.ValidSLOClasses {
		classes = append(classes, class)
	}
	tiers := []string{"small", "medium", "large"}
	estimates := map[string]struct{ mean, p95 float64 }{
		"latency-small":      {50, 80},
		"latency-medium":     {100, 160},
		"latency-large":      {200, 320},
		"throughput-small":   {200, 300},
		"throughput-medium":  {400, 600},
		"throughput-large":   {800, 1200},
		"batch-small":        {500, 800},
		"batch-medium":       {1000, 1600},
		"batch-large":        {2000, 3200},
		"interactive-small":  {30, 50},
		"interactive-medium": {60, 100},
		"interactive-large":  {120, 200},
		"streaming-small":    {100, 150},
		"streaming-medium":   {200, 300},
		"streaming-large":    {400, 600},
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
				SLOComplianceRate: 0.5,
				ConfidenceScore:   0.0,
				LastUpdated:       time.Now(),
				// histogram will be assigned in NewProfileStore
			}
		}
	}

	return profiles
}
