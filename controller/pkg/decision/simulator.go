package decision

import (
	"math"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	coreinformers "k8s.io/client-go/informers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"

	"kubernetes-hybrid-scheduler/controller/pkg/constants"
	"kubernetes-hybrid-scheduler/controller/pkg/slo"
	"kubernetes-hybrid-scheduler/controller/pkg/telemetry"
	"kubernetes-hybrid-scheduler/controller/pkg/util"
)

// Job represents a workload to be scheduled
type Job struct {
	UID         types.UID
	Deadline    float64 // Absolute deadline in ms from now
	ExecTime    float64 // Expected execution time in ms
	ArrivalTime float64 // Time when job arrived (for simulation)
	Priority    constants.PriorityTier
	CPUMillis   int64
	MemMi       int64
	Class       string
	PodName     string // For debugging
}

// ScheduleSimulator simulates EDF scheduling on the edge cluster
type ScheduleSimulator struct {
	profiles  *ProfileStore
	podLister corelisters.PodLister
}

func NewScheduleSimulator(profiles *ProfileStore, podInformer coreinformers.PodInformer) *ScheduleSimulator {
	return &ScheduleSimulator{
		profiles:  profiles,
		podLister: podInformer.Lister(),
	}
}

// SimulateLocalCompletion simulates when a pod would complete if scheduled locally
// Returns: (completion time in ms, is feasible)
func (s *ScheduleSimulator) SimulateLocalCompletion(
	pod *corev1.Pod,
	sloData *slo.SLO,
	local *telemetry.LocalState,
) (completionTime float64, feasible bool) {

	podID := util.PodID(pod.Namespace, pod.Name, pod.GenerateName, string(pod.UID))

	// Build queue from actual pending pods
	queue, err := s.buildQueue()
	if err != nil {
		klog.Warningf("%s: Failed to build queue from actual pods: %v", podID, err)
		return math.Inf(1), false
	}

	// Add current pod as new job
	profile := s.profiles.GetOrDefault(GetProfileKey(pod, constants.Edge))
	newJob := Job{
		UID:         pod.UID,
		Deadline:    float64(sloData.DeadlineMs),
		ExecTime:    profile.P95DurationMs,
		ArrivalTime: 0, // arriving now
		Priority:    constants.GetTierForClass(sloData.Class),
		CPUMillis:   util.GetCPURequest(pod),
		MemMi:       util.GetMemRequestMi(pod),
		Class:       sloData.Class,
		PodName:     util.PodID(pod.Namespace, pod.Name, pod.GenerateName, ""),
	}

	queue = append(queue, newJob)

	// Sort by EDF (earliest deadline first), with priority as tie-breaker
	sort.Slice(queue, func(i, j int) bool {
		if math.Abs(queue[i].Deadline-queue[j].Deadline) < 1.0 { // Within 1ms
			return queue[i].Priority < queue[j].Priority
		}
		return queue[i].Deadline < queue[j].Deadline
	})

	klog.V(5).Infof("%s: Simulating EDF queue with %d jobs (target at position %d)",
		podID, len(queue), s.findJobPosition(queue, pod.UID))

	// Simulate execution with bin-packing
	completion, ok := s.simulateExecution(queue, local, pod.UID)

	if !ok {
		klog.V(4).Infof("%s: Simulation failed - pod cannot be scheduled", podID)
		return math.Inf(1), false
	}

	klog.V(4).Infof("%s: Simulated completion time: %.0fms", podID, completion)
	return completion, true
}

// buildQueue constructs queue from real pending pods in the cluster
func (s *ScheduleSimulator) buildQueue() ([]Job, error) {
	// Get all pending pods that target edge
	selector := labels.Set{constants.LabelManaged: constants.LabelValueTrue}.AsSelector()
	allManagedPods, err := s.podLister.List(selector)
	if err != nil {
		return nil, err
	}

	queue := make([]Job, 0, len(allManagedPods))
	now := time.Now()

	for _, p := range allManagedPods {
		// Only include pending pods that want edge
		if p.Status.Phase != corev1.PodPending {
			continue
		}
		if p.Spec.NodeName != "" {
			continue // Already bound
		}

		// Check if pod targets edge
		wantsEdge := false
		if p.Spec.NodeSelector != nil {
			if v, ok := p.Spec.NodeSelector[constants.NodeRoleLabelEdge]; ok && v == constants.LabelValueTrue {
				wantsEdge = true
			}
		}
		if !wantsEdge {
			continue // Not targeting edge
		}

		// Parse SLO
		podSLO, err := slo.ParseSLO(p)
		if err != nil {
			klog.V(5).Infof("Skipping pod %s/%s in queue: invalid SLO: %v",
				p.Namespace, p.Name, err)
			continue
		}

		// Calculate remaining deadline
		decisionTimeStr := p.Annotations[constants.AnnotationTimestamp]
		var decisionTime time.Time
		if decisionTimeStr != "" {
			decisionTime, err = time.Parse(time.RFC3339, decisionTimeStr)
			if err != nil {
				decisionTime = p.CreationTimestamp.Time
			}
		} else {
			decisionTime = p.CreationTimestamp.Time
		}

		elapsed := now.Sub(decisionTime).Milliseconds()
		remainingDeadline := float64(podSLO.DeadlineMs) - float64(elapsed)
		if remainingDeadline < 0 {
			remainingDeadline = 0 // Already missed deadline, but still in queue
		}

		// Get execution time estimate
		profile := s.profiles.GetOrDefault(GetProfileKey(p, constants.Edge))

		job := Job{
			UID:         p.UID,
			Deadline:    remainingDeadline,
			ExecTime:    profile.P95DurationMs,
			ArrivalTime: 0, // All jobs are "already arrived" from simulation perspective
			Priority:    constants.GetTierForClass(podSLO.Class),
			CPUMillis:   util.GetCPURequest(p),
			MemMi:       util.GetMemRequestMi(p),
			Class:       podSLO.Class,
			PodName:     util.PodID(p.Namespace, p.Name, p.GenerateName, ""),
		}

		queue = append(queue, job)
	}

	klog.V(5).Infof("Built queue from %d actual pending pods", len(queue))
	return queue, nil
}

func (s *ScheduleSimulator) findJobPosition(queue []Job, uid types.UID) int {
	for i, job := range queue {
		if job.UID == uid {
			return i
		}
	}
	return -1
}

// simulateExecution performs bin-packing simulation with time advancement
func (s *ScheduleSimulator) simulateExecution(
	queue []Job,
	local *telemetry.LocalState,
	targetUID types.UID,
) (float64, bool) {

	// Track resource availability over time
	type ResourceState struct {
		FreeCPU int64
		FreeMem int64
	}

	currentState := ResourceState{
		FreeCPU: local.FreeCPU,
		FreeMem: local.FreeMem,
	}

	// Track running jobs: UID -> end time
	running := make(map[types.UID]float64)

	// Track completions
	completions := make(map[types.UID]float64)

	currentTime := 0.0
	maxSimulationTime := 3600000.0 // 1 hour cap

	// Create a copy of queue for consumption
	remainingQueue := make([]Job, len(queue))
	copy(remainingQueue, queue)

	for len(remainingQueue) > 0 && currentTime < maxSimulationTime {
		// Process any completions at current time
		for uid, endTime := range running {
			if endTime <= currentTime {
				// Find the job to reclaim resources
				var completedJob *Job
				for i := range queue {
					if queue[i].UID == uid {
						completedJob = &queue[i]
						break
					}
				}

				if completedJob != nil {
					currentState.FreeCPU += completedJob.CPUMillis
					currentState.FreeMem += completedJob.MemMi
					klog.V(6).Infof("Sim t=%.0f: Job %s completed, freed CPU=%dm MEM=%dMi",
						currentTime, completedJob.PodName, completedJob.CPUMillis, completedJob.MemMi)
				}

				delete(running, uid)
			}
		}

		// Try to schedule next job(s) from queue
		scheduled := false

		for i := 0; i < len(remainingQueue); i++ {
			job := remainingQueue[i]

			// Check resource availability
			if currentState.FreeCPU >= job.CPUMillis && currentState.FreeMem >= job.MemMi {
				// Schedule this job
				startTime := currentTime
				endTime := startTime + job.ExecTime

				// Consume resources
				currentState.FreeCPU -= job.CPUMillis
				currentState.FreeMem -= job.MemMi

				// Track completion
				completions[job.UID] = endTime
				running[job.UID] = endTime

				klog.V(6).Infof("Sim t=%.0f: Scheduled %s (deadline=%.0f, exec=%.0f) -> completes at t=%.0f",
					startTime, job.PodName, job.Deadline, job.ExecTime, endTime)

				// Remove from queue
				remainingQueue = append(remainingQueue[:i], remainingQueue[i+1:]...)
				scheduled = true
				i-- // Adjust index after removal
			}
		}

		if !scheduled {
			// No job can start - advance time to next completion
			nextEvent := math.Inf(1)

			for _, endTime := range running {
				if endTime > currentTime && endTime < nextEvent {
					nextEvent = endTime
				}
			}

			if math.IsInf(nextEvent, 1) {
				// Deadlock: jobs remain but none can fit and none are running
				klog.V(4).Infof("Simulation deadlock at t=%.0f: %d jobs remain, none can fit (need CPU=%dm but have %dm)",
					currentTime, len(remainingQueue),
					remainingQueue[0].CPUMillis, currentState.FreeCPU)
				return math.Inf(1), false
			}

			currentTime = nextEvent
		}
	}

	// Check if target pod completed
	completion, found := completions[targetUID]
	if !found {
		klog.V(4).Infof("Target pod UID=%s never scheduled in simulation", targetUID)
		return math.Inf(1), false
	}

	return completion, true
}
