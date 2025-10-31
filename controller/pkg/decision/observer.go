package decision

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
)

type PodObserver struct {
	kubeClient  kubernetes.Interface
	store       *ProfileStore
	podInformer cache.SharedIndexInformer
	lyapunov    *LyapunovScheduler
}

func NewPodObserver(
	kubeClient kubernetes.Interface,
	profileStore *ProfileStore,
	podInf coreinformers.PodInformer,
	lyapunov *LyapunovScheduler,
) *PodObserver {
	return &PodObserver{
		kubeClient:  kubeClient,
		store:       profileStore,
		podInformer: podInf.Informer(),
		lyapunov:    lyapunov,
	}
}

func (o *PodObserver) Watch(stopCh <-chan struct{}) {
	klog.Info("PodObserver starting")

	_, err := o.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			pod := newObj.(*corev1.Pod)

			if pod.Labels[constants.LabelManaged] != "true" {
				return
			}
			if pod.Annotations[constants.AnnotationDecision] == "" {
				return
			}

			if pod.Status.Phase == corev1.PodRunning &&
				pod.Annotations[constants.AnnotationActualStart] == "" {
				o.recordStart(pod)
			}

			if pod.Status.Phase == corev1.PodSucceeded ||
				pod.Status.Phase == corev1.PodFailed {
				o.recordCompletion(pod)
			}
		},
	})
	if err != nil {
		klog.Errorf("Failed to register PodObserver handlers: %v", err)
		return
	}

	klog.Info("PodObserver handlers registered")
	<-stopCh
}

func (o *PodObserver) recordStart(pod *corev1.Pod) {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))
	startTime := time.Now()

	decisionTime, err := parseTime(pod.Annotations[constants.AnnotationTimestamp])
	if err != nil {
		klog.V(4).Infof("%s: Failed to parse decision timestamp: %v", podID, err)
		return
	}

	queueWait := startTime.Sub(decisionTime).Milliseconds()

	o.annotate(pod, constants.AnnotationActualStart, startTime.Format(time.RFC3339))
	o.annotate(pod, constants.AnnotationQueueWait, fmt.Sprintf("%d", queueWait))

	klog.V(4).Infof("%s: Observed pod start after %dms queue wait", podID, queueWait)
}

func (o *PodObserver) recordCompletion(pod *corev1.Pod) {
	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))
	decision := constants.Location(pod.Annotations[constants.AnnotationDecision])
	predictedETA, _ := parseFloat64(pod.Annotations[constants.AnnotationPredictedETA])
	queueWait, _ := parseFloat64(pod.Annotations[constants.AnnotationQueueWait])
	startTime, err := parseTime(pod.Annotations[constants.AnnotationActualStart])
	if err != nil {
		klog.V(4).Infof("%s: Missing start time annotation: %v", podID, err)
		return
	}

	endTime := time.Now()
	if len(pod.Status.ContainerStatuses) > 0 {
		if state := pod.Status.ContainerStatuses[0].State.Terminated; state != nil {
			endTime = state.FinishedAt.Time
		}
	}
	actualDuration := endTime.Sub(startTime).Milliseconds()

	deadlineMs, _ := parseFloat64(pod.Annotations[constants.AnnotationSLODeadline])
	totalTime := float64(queueWait) + float64(actualDuration)
	sloMet := true
	if deadlineMs != 0 {
		sloMet = totalTime <= deadlineMs
	}

	// Get SLO class
	class := pod.Annotations[constants.AnnotationSLOClass]
	if class == "" {
		class = constants.DefaultSLOClass
	}

	// Update profile store
	key := GetProfileKey(pod, decision)
	o.store.Update(key, ProfileUpdate{
		ObservedDurationMs: float64(actualDuration),
		QueueWaitMs:        float64(queueWait),
		SLOMet:             sloMet,
	})

	predErr := math.Abs(float64(actualDuration) - predictedETA)
	recordPredictionError(key, predErr)

	// Update Lyapunov virtual queue
	if o.lyapunov != nil && deadlineMs > 0 {
		o.lyapunov.UpdateVirtualQueue(
			class,
			deadlineMs,
			totalTime,
			decision,
		)
	}

	klog.V(3).Infof("%s: Observed pod completion: actual=%dms predicted=%.0fms slo=%v class=%s",
		podID, actualDuration, predictedETA, sloMet, class)
}

func (o *PodObserver) annotate(pod *corev1.Pod, key, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var ops []map[string]interface{}
	if pod.Annotations == nil {
		ops = append(ops, map[string]interface{}{
			"op":    "add",
			"path":  "/metadata/annotations",
			"value": map[string]string{},
		})
	}
	ops = append(ops, map[string]interface{}{
		"op":    "add",
		"path":  "/metadata/annotations/" + util.EscapeJSONPointer(key),
		"value": value,
	})
	patchBytes, _ := json.Marshal(ops)

	for i := 0; i < 3; i++ {
		_, err := o.kubeClient.CoreV1().Pods(pod.Namespace).Patch(
			ctx,
			pod.Name,
			types.JSONPatchType,
			patchBytes,
			metav1.PatchOptions{},
		)
		if err == nil {
			return
		}
		if i < 2 {
			time.Sleep(100 * time.Millisecond)
			klog.V(5).Infof("Retrying pod annotation patch (attempt %d/3): %v", i+1, err)
		}
	}

	klog.V(4).Infof("%s: Failed to patch pod after retries",
		util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID)))
}

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
