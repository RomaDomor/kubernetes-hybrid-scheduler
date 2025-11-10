package decision

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	apis "kubernetes-hybrid-scheduler/controller/pkg/api/v1alpha1"
	"kubernetes-hybrid-scheduler/controller/pkg/constants"
)

var workloadProfileGVR = schema.GroupVersionResource{
	Group:    "scheduling.hybrid.io",
	Version:  "v1alpha1",
	Resource: "workloadprofiles",
}

// lruEntry is an entry in the LRU list.
type lruEntry struct {
	key   string
	stats *apis.ProfileStats
}

// ProfileStore manages workload performance profiles with an LRU cache.
type ProfileStore struct {
	profiles   map[string]*apis.ProfileStats
	lru        *list.List
	lruMap     map[string]*list.Element
	mu         sync.RWMutex
	defaults   map[string]*apis.ProfileStats
	maxEntries int
	kubeClient kubernetes.Interface
	hcfg       HistogramConfig
}

// NewProfileStore creates a new ProfileStore.
func NewProfileStore(kubeClient kubernetes.Interface, maxEntries int, hcfg HistogramConfig) *ProfileStore {
	ps := &ProfileStore{
		profiles:   make(map[string]*apis.ProfileStats),
		lru:        list.New(),
		lruMap:     make(map[string]*list.Element),
		defaults:   make(map[string]*apis.ProfileStats),
		maxEntries: maxEntries,
		kubeClient: kubeClient,
		hcfg:       hcfg,
	}
	// Initialize default profiles with empty histograms.
	for k, v := range defaultProfilesStatic() {
		cp := *v
		cp.DurationHistogram = ps.initHistogram()
		ps.defaults[k] = &cp
	}
	return ps
}

// GetOrDefault retrieves a profile by key, returning a default if not found.
func (ps *ProfileStore) GetOrDefault(key apis.ProfileKey) *apis.ProfileStats {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	keyStr := key.String()

	if profile, exists := ps.profiles[keyStr]; exists {
		// Move to front of LRU on access
		if elem, ok := ps.lruMap[keyStr]; ok {
			ps.lru.MoveToFront(elem)
		}
		return deepCopyProfile(profile)
	}

	// Fallback to a default based on class and CPU tier.
	defaultKey := fmt.Sprintf("%s-%s", key.Class, key.CPUTier)
	if def, exists := ps.defaults[defaultKey]; exists {
		return deepCopyProfile(def)
	}

	// Fallback to a generic, uninformative default.
	klog.Warningf("No default profile found for key: %s", keyStr)
	return deepCopyProfile(&apis.ProfileStats{
		MeanDurationMs:    1000,
		P95DurationMs:     2000,
		ConfidenceScore:   0.0,
		SLOComplianceRate: 0.5,
		DurationHistogram: ps.initHistogram(),
	})
}

// deepCopyProfile creates a deep copy of a ProfileStats object.
func deepCopyProfile(p *apis.ProfileStats) *apis.ProfileStats {
	if p == nil {
		return nil
	}
	pCopy := *p
	pCopy.DurationHistogram = make([]apis.HistogramBucket, len(p.DurationHistogram))
	copy(pCopy.DurationHistogram, p.DurationHistogram)
	return &pCopy
}

// Update incorporates a new observation into a workload profile.
func (ps *ProfileStore) Update(key apis.ProfileKey, update apis.ProfileUpdate) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	keyStr := key.String()

	// Cap observed duration to prevent extreme outliers from poisoning the data.
	dur := update.ObservedDurationMs
	if dur < 0 || dur > ps.hcfg.IngestCapMs {
		klog.V(4).Infof(
			"Discarding outlier duration %.0fms for %s (capped at %.0fms)",
			dur, keyStr, ps.hcfg.IngestCapMs,
		)
		dur = ps.hcfg.IngestCapMs
	}

	profile, exists := ps.profiles[keyStr]
	if !exists {
		if len(ps.profiles) >= ps.maxEntries {
			ps.evictLRU()
		}
		profile = ps.getOrDefaultLocked(key) // Use internal locked version
		ps.profiles[keyStr] = profile
		ps.lruMap[keyStr] = ps.lru.PushFront(&lruEntry{key: keyStr, stats: profile})
	} else {
		if elem, ok := ps.lruMap[keyStr]; ok {
			ps.lru.MoveToFront(elem)
		}
	}

	// Update statistics using an exponential moving average (EMA).
	const alpha = 0.2 // Weight for new observations.

	oldMean := profile.MeanDurationMs
	profile.MeanDurationMs = alpha*dur + (1-alpha)*oldMean

	// EMA for variance/stddev
	diff := dur - profile.MeanDurationMs
	profile.StdDevDurationMs = math.Sqrt(alpha*math.Pow(diff, 2) + (1-alpha)*math.Pow(profile.StdDevDurationMs, 2))

	ps.updateHistogram(profile.DurationHistogram, dur)
	profile.P95DurationMs = ps.computeP95FromHistogram(profile.DurationHistogram)

	profile.MeanQueueWaitMs = alpha*update.QueueWaitMs + (1-alpha)*profile.MeanQueueWaitMs

	sloMetric := 0.0
	if update.SLOMet {
		sloMetric = 1.0
	}
	profile.SLOComplianceRate = alpha*sloMetric + (1-alpha)*profile.SLOComplianceRate

	// Update confidence score, capping at 1.0 after 20 samples.
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

// evictLRU removes the least recently used profile from the cache.
func (ps *ProfileStore) evictLRU() {
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

// getOrDefaultLocked is an internal helper that doesn't acquire locks.
func (ps *ProfileStore) getOrDefaultLocked(key apis.ProfileKey) *apis.ProfileStats {
	defaultKey := fmt.Sprintf("%s-%s", key.Class, key.CPUTier)
	if def, exists := ps.defaults[defaultKey]; exists {
		pCopy := deepCopyProfile(def)
		// Reset count and confidence for a new profile based on a default.
		pCopy.Count = 0
		pCopy.ConfidenceScore = 0.0
		return pCopy
	}

	// Should be rare if defaults are comprehensive.
	return &apis.ProfileStats{
		MeanDurationMs:    1000,
		P95DurationMs:     2000,
		StdDevDurationMs:  500,
		ConfidenceScore:   0.0,
		SLOComplianceRate: 0.5,
		DurationHistogram: ps.initHistogram(),
	}
}

// initHistogram creates a new set of histogram buckets based on the store's configuration.
func (ps *ProfileStore) initHistogram() []apis.HistogramBucket {
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
	default: // Fallback to default explicit bounds
		bounds = append([]float64(nil), DefaultHistogramConfig().Explicit...)
	}
	if ps.hcfg.IncludeInf {
		bounds = append(bounds, math.Inf(1))
	}

	buckets := make([]apis.HistogramBucket, len(bounds))
	now := time.Now()
	for i, bound := range bounds {
		buckets[i].SetUpperBound(bound)
		buckets[i].Count = 0
		buckets[i].LastDecay = now
	}
	return buckets
}

// updateHistogram adds a value to the correct bucket and applies decay.
func (ps *ProfileStore) updateHistogram(buckets []apis.HistogramBucket, value float64) {
	now := time.Now()
	// Apply exponential decay to bucket counts to favor recent data.
	for i := range buckets {
		if now.Sub(buckets[i].LastDecay) > ps.hcfg.DecayInterval {
			buckets[i].Count /= 2 // Half-life decay
			buckets[i].LastDecay = now
		}
	}

	// Find the correct bucket and increment its count.
	for i := range buckets {
		if value <= buckets[i].UpperBound() {
			buckets[i].Count++
			return
		}
	}
	// If value is larger than all finite buckets, it goes in the last one (usually +Inf).
	if len(buckets) > 0 {
		buckets[len(buckets)-1].Count++
	}
}

// computeP95FromHistogram estimates the 95th percentile from the histogram data.
func (ps *ProfileStore) computeP95FromHistogram(buckets []apis.HistogramBucket) float64 {
	total := 0
	for _, b := range buckets {
		total += b.Count
	}
	if total == 0 {
		return 150.0 // A reasonable default if no data.
	}

	// If sample count is low, return a more conservative estimate.
	if total < ps.hcfg.MinSampleCount {
		// Find the upper bound of the highest bucket with data.
		for i := len(buckets) - 1; i >= 0; i-- {
			if buckets[i].Count > 0 {
				ub := buckets[i].UpperBound()
				if math.IsInf(ub, +1) {
					// If in +Inf bucket, extrapolate from previous.
					if i > 0 {
						return math.Min(buckets[i-1].UpperBound()*1.5, ps.hcfg.IngestCapMs)
					}
					return ps.hcfg.IngestCapMs
				}
				return ub
			}
		}
		return 50.0 // Fallback if all buckets are empty.
	}

	target := int(math.Ceil(0.95 * float64(total)))
	cum := 0
	for i, b := range buckets {
		cum += b.Count
		if cum >= target {
			// Linear interpolation within the target bucket.
			var prevUB float64
			if i > 0 {
				prevUB = buckets[i-1].UpperBound()
			}
			ub := b.UpperBound()
			if math.IsInf(ub, +1) {
				// Use a synthetic upper bound for the +Inf bucket.
				ub = math.Min(prevUB*2, ps.hcfg.IngestCapMs)
			}

			if b.Count == 0 {
				return math.Min(ub, ps.hcfg.IngestCapMs)
			}

			// Interpolation formula
			ratio := float64(target-(cum-b.Count)) / float64(b.Count)
			p95 := prevUB + ratio*(ub-prevUB)
			return math.Min(p95, ps.hcfg.IngestCapMs)
		}
	}

	return ps.lastFiniteUpperBound(buckets)
}

func (ps *ProfileStore) lastFiniteUpperBound(buckets []apis.HistogramBucket) float64 {
	for i := len(buckets) - 1; i >= 0; i-- {
		ub := buckets[i].UpperBound()
		if !math.IsInf(ub, +1) {
			return ub
		}
	}
	return ps.hcfg.IngestCapMs
}

// SaveToCRD persists all profiles in the cache to WorkloadProfile CRDs.
func (ps *ProfileStore) SaveToCRD(dynClient dynamic.Interface) error {
	ps.mu.RLock()
	profilesToSave := make(map[string]*apis.ProfileStats, len(ps.profiles))
	for k, v := range ps.profiles {
		profilesToSave[k] = v
	}
	ps.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res := dynClient.Resource(workloadProfileGVR).Namespace(constants.KubeSystemNamespace)
	savedCount := 0

	for keyStr, profile := range profilesToSave {
		profileMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(profile)
		if err != nil {
			klog.Warningf("Failed to convert profile %s to unstructured map: %v", keyStr, err)
			continue
		}

		obj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "scheduling.hybrid.io/v1alpha1",
				"kind":       "WorkloadProfile",
				"metadata": map[string]interface{}{
					"name":      keyStr,
					"namespace": constants.KubeSystemNamespace,
				},
				"spec": map[string]interface{}{
					"profile": profileMap,
				},
			},
		}

		existing, err := res.Get(ctx, keyStr, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			// Create new CRD
			if _, createErr := res.Create(ctx, obj, metav1.CreateOptions{}); createErr != nil {
				klog.Warningf("Failed to create WorkloadProfile %s: %v", keyStr, createErr)
			} else {
				savedCount++
			}
		} else if err == nil {
			// Update existing CRD
			obj.Object["metadata"].(map[string]interface{})["resourceVersion"] = existing.GetResourceVersion()
			if _, updateErr := res.Update(ctx, obj, metav1.UpdateOptions{}); updateErr != nil {
				klog.Warningf("Failed to update WorkloadProfile %s: %v", keyStr, updateErr)
			} else {
				savedCount++
			}
		} else {
			klog.Warningf("Failed to get WorkloadProfile %s: %v", keyStr, err)
		}
	}

	klog.V(3).Infof("Saved %d profiles to CRDs", savedCount)
	return nil
}

// LoadFromCRD loads profiles from WorkloadProfile CRDs at startup.
func (ps *ProfileStore) LoadFromCRD(dynClient dynamic.Interface) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	profileList, err := dynClient.Resource(workloadProfileGVR).Namespace(constants.KubeSystemNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list WorkloadProfile CRDs: %w", err)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, item := range profileList.Items {
		keyStr := item.GetName()
		specMap, found, err := unstructured.NestedMap(item.Object, "spec", "profile")
		if !found || err != nil {
			klog.Warningf("Profile data missing or invalid in CRD %s", keyStr)
			continue
		}

		profile := &apis.ProfileStats{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(specMap, profile); err != nil {
			klog.Warningf("Failed to convert unstructured data to ProfileStats for %s: %v", keyStr, err)
			continue
		}

		ps.profiles[keyStr] = profile
		ps.lruMap[keyStr] = ps.lru.PushFront(&lruEntry{key: keyStr, stats: profile})
	}

	klog.Infof("Loaded %d profiles from CRDs", len(ps.profiles))
	return nil
}

// StartAutoSave begins a loop to periodically save profiles.
func (ps *ProfileStore) StartAutoSave(dynClient dynamic.Interface, interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := ps.SaveToCRD(dynClient); err != nil {
				klog.Warningf("Auto-save profiles failed: %v", err)
			}
		case <-stopCh:
			// Perform a final save on shutdown.
			klog.Info("Saving profiles on shutdown...")
			if err := ps.SaveToCRD(dynClient); err != nil {
				klog.Errorf("Failed to save profiles on shutdown: %v", err)
			}
			return
		}
	}
}

// ExportAllProfiles returns a copy of all profiles for debugging.
func (ps *ProfileStore) ExportAllProfiles() map[string]*apis.ProfileStats {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	export := make(map[string]*apis.ProfileStats, len(ps.profiles))
	for k, v := range ps.profiles {
		export[k] = deepCopyProfile(v)
	}
	return export
}

// defaultProfilesStatic provides initial, static default profiles for various workload types.
func defaultProfilesStatic() map[string]*apis.ProfileStats {
	// Estimates for mean and P95 duration in ms for different classes and sizes.
	estimates := map[string]struct{ mean, p95 float64 }{
		"latency-small": {50, 80}, "latency-medium": {100, 160}, "latency-large": {200, 320},
		"throughput-small": {200, 300}, "throughput-medium": {400, 600}, "throughput-large": {800, 1200},
		"batch-small": {500, 800}, "batch-medium": {1000, 1600}, "batch-large": {2000, 3200},
		"interactive-small": {30, 50}, "interactive-medium": {60, 100}, "interactive-large": {120, 200},
		"streaming-small": {100, 150}, "streaming-medium": {200, 300}, "streaming-large": {400, 600},
	}

	profiles := make(map[string]*apis.ProfileStats)
	tiers := []string{"small", "medium", "large"}
	for class := range constants.ValidSLOClasses {
		for _, tier := range tiers {
			key := fmt.Sprintf("%s-%s", class, tier)
			est, ok := estimates[key]
			if !ok {
				est = struct{ mean, p95 float64 }{mean: 500, p95: 1000} // Generic fallback
			}

			profiles[key] = &apis.ProfileStats{
				Count:          0,
				MeanDurationMs: est.mean,
				// Estimate stddev from P95 assuming a normal distribution (P95 ≈ μ + 1.645σ)
				StdDevDurationMs:  (est.p95 - est.mean) / 1.645,
				P95DurationMs:     est.p95,
				SLOComplianceRate: 0.5,
				ConfidenceScore:   0.0,
				LastUpdated:       time.Time{}, // Zero time indicates it's a static default
			}
		}
	}
	return profiles
}

// ComputeViolationProbability computes Pr[completion > threshold] from histogram.
//
// ALGORITHM:
//  1. If sufficient samples (≥20): compute from histogram CDF
//  2. If low samples: use normal approximation from mean/stddev
//  3. If no data: return 0.5 (maximum uncertainty)
//
// HISTOGRAM-BASED (primary method):
//   - Compute CDF: F(x) = Σ{buckets ≤ x} count / total
//   - Linear interpolation within buckets
//   - Return tail: 1 - F(threshold)
//
// STATS-BASED (fallback for low samples):
//   - Normal approximation: Pr[X > t] ≈ 1 - Φ((t - μ) / σ)
//   - Conservative for right-skewed distributions
func (ps *ProfileStore) ComputeViolationProbability(stats *apis.ProfileStats, threshold float64) float64 {
	if stats == nil {
		// No data: maximum uncertainty
		return 0.5
	}

	// Deterministic case: no variance (handle before histogram check)
	if stats.StdDevDurationMs <= 0 {
		if stats.MeanDurationMs > threshold {
			return 1.0
		}
		return 0.0
	}

	if len(stats.DurationHistogram) == 0 {
		// Histogram exists but might be empty after decay. Fall back to stats.
		return ps.computeViolationProbabilityFromStats(stats, threshold)
	}

	// Require minimum samples for histogram-based probability
	if stats.Count < ps.hcfg.MinSampleCount {
		// Low sample count: use stats-based estimate
		return ps.computeViolationProbabilityFromStats(stats, threshold)
	}

	// Compute total samples in histogram
	totalCount := 0
	for _, bucket := range stats.DurationHistogram {
		totalCount += bucket.Count
	}

	if totalCount == 0 {
		// Histogram exists but empty (edge case after decay)
		return ps.computeViolationProbabilityFromStats(stats, threshold)
	}

	// Compute CDF: cumulative count up to threshold
	cumulativeCount := 0
	foundBucket := false

	for i, bucket := range stats.DurationHistogram {
		ub := bucket.UpperBound()

		if ub >= threshold {
			// Threshold falls in this bucket: interpolate
			if i == 0 {
				// First bucket: [0, ub]
				if ub <= 0 || math.IsInf(ub, 1) {
					// Can't interpolate: assume half below threshold
					cumulativeCount += bucket.Count / 2
				} else {
					ratio := threshold / ub
					inBucketCount := int(float64(bucket.Count) * ratio)
					cumulativeCount += inBucketCount
				}
			} else {
				// General bucket: [prevUB, ub]
				prevUB := stats.DurationHistogram[i-1].UpperBound()
				bucketRange := ub - prevUB

				if bucketRange <= 0 || math.IsInf(ub, 1) {
					// Can't interpolate (collapsed or +Inf bucket)
					// Assume half below threshold
					cumulativeCount += bucket.Count / 2
				} else {
					// Linear interpolation
					ratio := (threshold - prevUB) / bucketRange
					ratio = math.Max(0, math.Min(1, ratio)) // Clamp to [0,1]
					inBucketCount := int(float64(bucket.Count) * ratio)
					cumulativeCount += inBucketCount
				}
			}
			foundBucket = true
			break
		} else {
			// Entire bucket is below threshold
			cumulativeCount += bucket.Count
		}
	}

	if !foundBucket {
		// Threshold exceeds all finite buckets
		// Check for +Inf bucket with outliers
		if len(stats.DurationHistogram) > 0 {
			lastBucket := stats.DurationHistogram[len(stats.DurationHistogram)-1]
			if math.IsInf(lastBucket.UpperBound(), 1) && lastBucket.Count > 0 {
				// Some samples in +Inf bucket: these definitely violated
				// All other samples didn't violate
				cdf := float64(totalCount-lastBucket.Count) / float64(totalCount)
				tailProb := 1.0 - cdf
				return math.Max(0.001, math.Min(1.0, tailProb))
			}
		}

		// All samples below threshold: use extrapolation
		return ps.extrapolateTailProbability(stats, threshold)
	}

	// Compute tail probability: Pr[X > threshold] = 1 - F(threshold)
	cdf := float64(cumulativeCount) / float64(totalCount)
	tailProb := 1.0 - cdf

	// Clamp to valid range
	return math.Max(0, math.Min(1, tailProb))
}

// computeViolationProbabilityFromStats uses normal approximation when histogram
// has insufficient samples.
func (ps *ProfileStore) computeViolationProbabilityFromStats(stats *apis.ProfileStats, threshold float64) float64 {
	if stats == nil {
		// No data: maximum uncertainty
		return 0.5
	}

	// Deterministic case: no variance
	if stats.StdDevDurationMs <= 0 {
		if stats.MeanDurationMs > threshold {
			return 1.0
		}
		return 0.0
	}

	// Standard normal approximation: Φ((threshold - mean) / stddev)
	z := (threshold - stats.MeanDurationMs) / stats.StdDevDurationMs

	// Fast approximation of standard normal CDF
	// Φ(z) ≈ 1 / (1 + exp(-1.702 * z)) for z > 0
	// Φ(z) ≈ exp(-1.702 * |z|) / (1 + exp(-1.702 * |z|)) for z < 0

	var cdf float64
	if z >= 0 {
		// Threshold above mean: low violation probability
		cdf = 1.0 / (1.0 + math.Exp(-1.702*z))
	} else {
		// Threshold below mean: high violation probability
		absZ := -z
		cdf = math.Exp(-1.702*absZ) / (1.0 + math.Exp(-1.702*absZ))
	}

	return 1.0 - cdf
}

// extrapolateTailProbability estimates tail probability when threshold exceeds
// all observed samples using exponential tail model.
func (ps *ProfileStore) extrapolateTailProbability(stats *apis.ProfileStats, threshold float64) float64 {
	// If threshold well beyond P95, use exponential tail
	if threshold > stats.P95DurationMs && stats.StdDevDurationMs > 0 {
		// P(X > threshold) ≈ P(X > P95) * exp(-(threshold - P95) / stddev)
		// P(X > P95) = 0.05 by definition
		excess := threshold - stats.P95DurationMs
		lambda := 1.0 / stats.StdDevDurationMs
		tailProb := 0.05 * math.Exp(-lambda*excess)
		return math.Max(0.001, tailProb) // At least 0.1% (10× lower than P95 tail)
	}

	// Threshold comfortably exceeds observations: very low probability
	return 0.001 // 0.1%
}
