package decision

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// PodObserver watches pod lifecycle and extracts ground truth
type PodObserver struct {
	kubeClient kubernetes.Interface
	store      *ProfileStore
}

func NewPodObserver(
	kubeClient kubernetes.Interface,
	profileStore *ProfileStore,
) *PodObserver {
	return &PodObserver{
		kubeClient: kubeClient,
		store:      profileStore,
	}
}

// Watch starts monitoring pod lifecycle events
func (o *PodObserver) Watch(stopCh <-chan struct{}) {
	factory := informers.NewSharedInformerFactory(o.kubeClient, 30*time.Second)
	podInformer := factory.Core().V1().Pods().Informer()

	_, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			pod := newObj.(*corev1.Pod)

			// Only track managed pods with scheduling decision
			if pod.Labels["scheduling.hybrid.io/managed"] != "true" {
				return
			}
			if pod.Annotations["scheduling.hybrid.io/decision"] == "" {
				return
			}

			// Record start time when pod transitions to Running
			if pod.Status.Phase == corev1.PodRunning &&
				pod.Annotations["scheduling.hybrid.io/actualStartMs"] == "" {
				o.recordStart(pod)
			}

			// Record completion when pod finishes
			if pod.Status.Phase == corev1.PodSucceeded ||
				pod.Status.Phase == corev1.PodFailed {
				o.recordCompletion(pod)
			}
		},
	})
	if err != nil {
		klog.Errorf("Failed to start informer: %v", err)
		return
	}

	factory.Start(stopCh)
	factory.WaitForCacheSync(stopCh)
}

func (o *PodObserver) recordStart(pod *corev1.Pod) {
	startTime := time.Now()
	decisionTime, err := parseTime(pod.Annotations["scheduling.hybrid.io/timestamp"])
	if err != nil {
		klog.V(4).Infof("Failed to parse decision timestamp for %s/%s: %v",
			pod.Namespace, pod.Name, err)
		return
	}

	queueWait := startTime.Sub(decisionTime).Milliseconds()

	// Update annotations
	o.annotate(pod, "scheduling.hybrid.io/actualStartMs",
		fmt.Sprintf("%d", startTime.UnixMilli()))
	o.annotate(pod, "scheduling.hybrid.io/queueWaitMs",
		fmt.Sprintf("%d", queueWait))

	klog.V(4).Infof("Pod %s/%s started after %dms queue wait",
		pod.Namespace, pod.Name, queueWait)
}

func (o *PodObserver) recordCompletion(pod *corev1.Pod) {
	// Extract measurements
	decision := Location(pod.Annotations["scheduling.hybrid.io/decision"])
	predictedETA, _ := parseFloat64(pod.Annotations["scheduling.hybrid.io/predictedETAMs"])
	queueWait, _ := parseFloat64(pod.Annotations["scheduling.hybrid.io/queueWaitMs"])
	startTime, err := parseTime(pod.Annotations["scheduling.hybrid.io/actualStartMs"])
	if err != nil {
		klog.V(4).Infof("Pod %s/%s missing start time annotation",
			pod.Namespace, pod.Name)
		return
	}

	// Calculate actual duration
	endTime := time.Now()
	if len(pod.Status.ContainerStatuses) > 0 {
		if state := pod.Status.ContainerStatuses[0].State.Terminated; state != nil {
			endTime = state.FinishedAt.Time
		}
	}
	actualDuration := endTime.Sub(startTime).Milliseconds()

	// Check SLO compliance
	deadlineMs, _ := parseFloat64(pod.Annotations["slo.hybrid.io/deadlineMs"])
	totalTime := float64(queueWait) + float64(actualDuration)
	sloMet := totalTime <= deadlineMs

	// Update profile
	key := GetProfileKey(pod, decision)
	o.store.Update(key, ProfileUpdate{
		ObservedDurationMs: float64(actualDuration),
		QueueWaitMs:        float64(queueWait),
		SLOMet:             sloMet,
	})

	// Record prediction error for metrics
	predErr := math.Abs(float64(actualDuration) - predictedETA)
	recordPredictionError(key, predErr)

	klog.V(3).Infof("Pod %s/%s completed: actual=%dms predicted=%.0fms slo=%v",
		pod.Namespace, pod.Name, actualDuration, predictedETA, sloMet)
}

func (o *PodObserver) annotate(pod *corev1.Pod, key, value string) {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[key] = value

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := o.kubeClient.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		klog.V(4).Infof("Failed to annotate pod %s/%s: %v",
			pod.Namespace, pod.Name, err)
	}
}

// Helpers
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time string")
	}
	return time.Parse(time.RFC3339, s)
}

func parseFloat64(s string) (float64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty float string")
	}
	return strconv.ParseFloat(s, 64)
}
