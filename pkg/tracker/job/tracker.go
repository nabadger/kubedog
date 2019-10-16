package job

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"

	"github.com/flant/kubedog/pkg/tracker"
	"github.com/flant/kubedog/pkg/tracker/debug"
	"github.com/flant/kubedog/pkg/tracker/event"
	"github.com/flant/kubedog/pkg/tracker/pod"
)

type FailedReport struct {
	FailedReason string
	JobStatus    JobStatus
}

type PodErrorReport struct {
	PodError  pod.PodError
	JobStatus JobStatus
}

type PodAddedReport struct {
	PodName   string
	JobStatus JobStatus
}

type Tracker struct {
	tracker.Tracker

	Added     chan JobStatus
	Succeeded chan JobStatus
	Failed    chan FailedReport
	Status    chan JobStatus

	EventMsg    chan string
	AddedPod    chan PodAddedReport
	PodLogChunk chan *pod.PodLogChunk
	PodError    chan PodErrorReport

	State            tracker.TrackerState
	TrackedPodsNames []string

	lastObject       *batchv1.Job
	statusGeneration uint64
	failedReason     string
	podStatuses      map[string]pod.PodStatus

	objectAdded    chan *batchv1.Job
	objectModified chan *batchv1.Job
	objectDeleted  chan *batchv1.Job
	objectFailed   chan string
	errors         chan error

	// Relay events from subordinate pods to the main Track goroutine.
	// Map data by pod name.
	podStatusesRelay        chan map[string]pod.PodStatus
	podContainerErrorsRelay chan map[string]pod.ContainerErrorReport
	donePodsRelay           chan map[string]pod.PodStatus
}

func NewTracker(ctx context.Context, name, namespace string, kube kubernetes.Interface) *Tracker {
	return &Tracker{
		Tracker: tracker.Tracker{
			Kube:             kube,
			Namespace:        namespace,
			FullResourceName: fmt.Sprintf("job/%s", name),
			ResourceName:     name,
			Context:          ctx,
		},

		Added:     make(chan JobStatus, 1),
		Succeeded: make(chan JobStatus, 0),
		Failed:    make(chan FailedReport, 0),
		Status:    make(chan JobStatus, 100),

		EventMsg:    make(chan string, 1),
		AddedPod:    make(chan PodAddedReport, 10),
		PodLogChunk: make(chan *pod.PodLogChunk, 1000),
		PodError:    make(chan PodErrorReport, 0),

		podStatuses: make(map[string]pod.PodStatus),

		State: tracker.Initial,

		objectAdded:    make(chan *batchv1.Job, 0),
		objectModified: make(chan *batchv1.Job, 0),
		objectDeleted:  make(chan *batchv1.Job, 0),
		objectFailed:   make(chan string, 1),
		errors:         make(chan error, 0),

		podStatusesRelay:        make(chan map[string]pod.PodStatus, 10),
		podContainerErrorsRelay: make(chan map[string]pod.ContainerErrorReport, 10),
		donePodsRelay:           make(chan map[string]pod.PodStatus, 10),
	}
}

func (job *Tracker) Track() error {
	var err error
	var podsTrackersIsRunning bool

	err = job.runInformer()
	if err != nil {
		return err
	}

	for {
		select {
		case object := <-job.objectAdded:
			job.runEventsInformer()

			if !podsTrackersIsRunning {
				if err := job.runPodsTrackers(object); err != nil {
					return fmt.Errorf("unable to track job %s pods: %s", job.ResourceName, err)
				}
			}

			if err := job.handleJobState(object); err != nil {
				return err
			}

		case object := <-job.objectModified:
			if err := job.handleJobState(object); err != nil {
				return err
			}

		case reason := <-job.objectFailed:
			job.State = tracker.ResourceFailed
			job.failedReason = reason

			var status JobStatus
			if job.lastObject != nil {
				job.statusGeneration++
				status = NewJobStatus(job.lastObject, job.statusGeneration, (job.State == tracker.ResourceFailed), job.failedReason, job.podStatuses, job.TrackedPodsNames)
			} else {
				status = JobStatus{IsFailed: true, FailedReason: reason}
			}
			job.Failed <- FailedReport{JobStatus: status, FailedReason: status.FailedReason}

		case <-job.objectDeleted:
			job.lastObject = nil
			job.State = tracker.ResourceDeleted
			job.failedReason = "resource deleted"
			status := JobStatus{IsFailed: true, FailedReason: job.failedReason}
			job.Failed <- FailedReport{JobStatus: status, FailedReason: status.FailedReason}
			// TODO (longterm): This is not a fail, object may disappear then appear again.
			// TODO (longterm): At this level tracker should allow that situation and still continue tracking.

		case donePods := <-job.donePodsRelay:
			trackedPodsNames := make([]string, 0)

		trackedPodsIteration:
			for _, name := range job.TrackedPodsNames {
				for donePodName, status := range donePods {
					if name == donePodName {
						// This Pod is no more tracked,
						// but we need to update final
						// Pod's status
						if _, hasKey := job.podStatuses[name]; hasKey {
							job.podStatuses[name] = status
						}
						continue trackedPodsIteration
					}
				}

				trackedPodsNames = append(trackedPodsNames, name)
			}
			job.TrackedPodsNames = trackedPodsNames

			if err := job.handleJobState(job.lastObject); err != nil {
				return err
			}

		case podStatuses := <-job.podStatusesRelay:
			for podName, podStatus := range podStatuses {
				job.podStatuses[podName] = podStatus
			}
			if job.lastObject != nil {
				job.statusGeneration++
				job.Status <- NewJobStatus(job.lastObject, job.statusGeneration, (job.State == tracker.ResourceFailed), job.failedReason, job.podStatuses, job.TrackedPodsNames)
			}

		case podContainerErrors := <-job.podContainerErrorsRelay:
			for podName, containerError := range podContainerErrors {
				job.podStatuses[podName] = containerError.PodStatus
			}
			if job.lastObject != nil {
				job.statusGeneration++
				status := NewJobStatus(job.lastObject, job.statusGeneration, (job.State == tracker.ResourceFailed), job.failedReason, job.podStatuses, job.TrackedPodsNames)

				for podName, containerError := range podContainerErrors {
					job.PodError <- PodErrorReport{
						PodError: pod.PodError{
							ContainerError: containerError.ContainerError,
							PodName:        podName,
						},
						JobStatus: status,
					}
				}
			}

		case <-job.Context.Done():
			return job.Context.Err()
		case err := <-job.errors:
			return err
		}
	}
}

func (job *Tracker) runInformer() error {
	tweakListOptions := func(options metav1.ListOptions) metav1.ListOptions {
		options.FieldSelector = fields.OneTermEqualSelector("metadata.name", job.ResourceName).String()
		return options
	}
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return job.Kube.BatchV1().Jobs(job.Namespace).List(tweakListOptions(options))
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return job.Kube.BatchV1().Jobs(job.Namespace).Watch(tweakListOptions(options))
		},
	}

	go func() {
		_, err := watchtools.UntilWithSync(job.Context, lw, &batchv1.Job{}, nil, func(e watch.Event) (bool, error) {
			if debug.Debug() {
				fmt.Printf("Job `%s` informer event: %#v\n", job.ResourceName, e.Type)
			}

			var object *batchv1.Job

			if e.Type != watch.Error {
				var ok bool
				object, ok = e.Object.(*batchv1.Job)
				if !ok {
					return true, fmt.Errorf("expected %s to be a *batchv1.Job, got %T", job.ResourceName, e.Object)
				}
			}

			if e.Type == watch.Added {
				job.objectAdded <- object
			} else if e.Type == watch.Modified {
				job.objectModified <- object
			} else if e.Type == watch.Deleted {
				job.objectDeleted <- object
			}

			return false, nil
		})

		if err != nil {
			job.errors <- err
		}

		if debug.Debug() {
			fmt.Printf("Job `%s` informer done\n", job.ResourceName)
		}
	}()

	return nil
}

func (job *Tracker) handleJobState(object *batchv1.Job) error {
	job.lastObject = object
	job.statusGeneration++

	status := NewJobStatus(object, job.statusGeneration, job.State == tracker.ResourceFailed, job.failedReason, job.podStatuses, job.TrackedPodsNames)

	switch job.State {
	case tracker.Initial:
		if status.IsFailed {
			job.State = tracker.ResourceFailed
			job.Failed <- FailedReport{JobStatus: status, FailedReason: status.FailedReason}
		} else if status.IsSucceeded {
			job.State = tracker.ResourceSucceeded
			job.Succeeded <- status
		} else {
			job.State = tracker.ResourceAdded
			job.Added <- status
		}
	case tracker.ResourceAdded:
	case tracker.ResourceFailed:
		if status.IsFailed {
			job.State = tracker.ResourceFailed
			job.Failed <- FailedReport{JobStatus: status, FailedReason: status.FailedReason}
		} else if status.IsSucceeded {
			job.State = tracker.ResourceSucceeded
			job.Succeeded <- status
		} else {
			job.Status <- status
		}
	case tracker.ResourceSucceeded:
		job.Status <- status
	}

	return nil
}

func (job *Tracker) runPodsTrackers(object *batchv1.Job) error {
	// FIXME: use PodsInformer to track new pods

	selector, err := metav1.LabelSelectorAsSelector(object.Spec.Selector)
	if err != nil {
		return err
	}

	list, err := job.Kube.CoreV1().Pods(job.Namespace).List(metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return err
	}
	for _, item := range list.Items {
		err := job.runPodTracker(item.Name)
		if err != nil {
			return err
		}
	}

	tweakListOptions := func(options metav1.ListOptions) metav1.ListOptions {
		options.LabelSelector = selector.String()
		return options
	}
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return job.Kube.CoreV1().Pods(job.Namespace).List(tweakListOptions(options))
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return job.Kube.CoreV1().Pods(job.Namespace).Watch(tweakListOptions(options))
		},
	}

	go func() {
		_, err = watchtools.UntilWithSync(job.Context, lw, &corev1.Pod{}, nil, func(e watch.Event) (bool, error) {
			if debug.Debug() {
				fmt.Printf("Job `%s` pods informer event: %#v\n", job.ResourceName, e.Type)
			}

			object, ok := e.Object.(*corev1.Pod)
			if !ok {
				return true, fmt.Errorf("expected %s to be a *corev1.Pod, got %T", job.ResourceName, e.Object)
			}

			if e.Type == watch.Added {
				for _, podName := range job.TrackedPodsNames {
					if podName == object.Name {
						// Already under tracking
						return false, nil
					}
				}

				err := job.runPodTracker(object.Name)
				if err != nil {
					return true, err
				}
			}

			return false, nil
		})

		if err != nil {
			job.errors <- err
		}

		if debug.Debug() {
			fmt.Printf("Job `%s` pods informer done\n", job.ResourceName)
		}
	}()

	return nil
}

func (job *Tracker) runPodTracker(podName string) error {
	errorChan := make(chan error, 0)
	doneChan := make(chan struct{}, 0)

	podTracker := pod.NewTracker(job.Context, podName, job.Namespace, job.Kube)
	job.TrackedPodsNames = append(job.TrackedPodsNames, podName)

	job.statusGeneration++
	status := NewJobStatus(job.lastObject, job.statusGeneration, (job.State == tracker.ResourceFailed), job.failedReason, job.podStatuses, job.TrackedPodsNames)
	job.AddedPod <- PodAddedReport{
		PodName:   podTracker.ResourceName,
		JobStatus: status,
	}

	go func() {
		if debug.Debug() {
			fmt.Printf("Starting Job's `%s` Pod `%s` tracker\n", job.ResourceName, podTracker.ResourceName)
		}

		err := podTracker.Start()
		if err != nil {
			errorChan <- err
		} else {
			doneChan <- struct{}{}
		}

		if debug.Debug() {
			fmt.Printf("Done Job's `%s` Pod `%s` tracker done\n", job.ResourceName, podTracker.ResourceName)
		}
	}()

	go func() {
		for {
			select {
			case status := <-podTracker.Added:
				job.podStatusesRelay <- map[string]pod.PodStatus{podTracker.ResourceName: status}
			case status := <-podTracker.Succeeded:
				job.podStatusesRelay <- map[string]pod.PodStatus{podTracker.ResourceName: status}
			case status := <-podTracker.Ready:
				job.podStatusesRelay <- map[string]pod.PodStatus{podTracker.ResourceName: status}
			case report := <-podTracker.Failed:
				job.podStatusesRelay <- map[string]pod.PodStatus{podTracker.ResourceName: report.PodStatus}
			case status := <-podTracker.Status:
				job.podStatusesRelay <- map[string]pod.PodStatus{podTracker.ResourceName: status}

			case msg := <-podTracker.EventMsg:
				job.EventMsg <- fmt.Sprintf("po/%s %s", podTracker.ResourceName, msg)
			case chunk := <-podTracker.ContainerLogChunk:
				podChunk := &pod.PodLogChunk{ContainerLogChunk: chunk, PodName: podTracker.ResourceName}
				job.PodLogChunk <- podChunk
			case report := <-podTracker.ContainerError:
				job.podContainerErrorsRelay <- map[string]pod.ContainerErrorReport{podTracker.ResourceName: report}

			case err := <-errorChan:
				job.errors <- err
				return
			case <-doneChan:
				job.donePodsRelay <- map[string]pod.PodStatus{podTracker.ResourceName: podTracker.LastStatus}
				return
			}
		}
	}()

	return nil
}

// runEventsInformer watch for DaemonSet events
func (job *Tracker) runEventsInformer() {
	if job.lastObject == nil {
		return
	}

	eventInformer := event.NewEventInformer(&job.Tracker, job.lastObject)
	eventInformer.WithChannels(job.EventMsg, job.objectFailed, job.errors)
	eventInformer.Run()

	return
}
