package pod

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"

	"github.com/flant/kubedog/pkg/display"
	"github.com/flant/kubedog/pkg/tracker"
	"github.com/flant/kubedog/pkg/tracker/debug"
	"github.com/flant/kubedog/pkg/tracker/event"
)

type ContainerError struct {
	Message       string
	ContainerName string
}

type ContainerLogChunk struct {
	ContainerName string
	LogLines      []display.LogLine
}

type PodLogChunk struct {
	*ContainerLogChunk
	PodName string
}

type PodError struct {
	ContainerError
	PodName string
}

type FailedReport struct {
	FailedReason string
	PodStatus    PodStatus
}

type ContainerErrorReport struct {
	ContainerError
	PodStatus PodStatus
}

type Tracker struct {
	tracker.Tracker

	statusGeneration uint64

	Added     chan PodStatus
	Succeeded chan PodStatus
	Ready     chan PodStatus
	Failed    chan FailedReport
	Status    chan PodStatus

	EventMsg          chan string
	ContainerLogChunk chan *ContainerLogChunk
	ContainerError    chan ContainerErrorReport

	// LastStatus struct is needed for the Job tracker.
	// LastStatus contains latest known and actual resource status.
	LastStatus PodStatus

	State                           tracker.TrackerState
	ContainerTrackerStates          map[string]tracker.TrackerState
	ProcessedContainerLogTimestamps map[string]time.Time
	TrackedContainers               []string
	LogsFromTime                    time.Time

	lastObject   *corev1.Pod
	failedReason string

	objectAdded    chan *corev1.Pod
	objectModified chan *corev1.Pod
	objectDeleted  chan *corev1.Pod
	objectFailed   chan string

	containerDone chan string
	errors        chan error
}

func NewTracker(ctx context.Context, name, namespace string, kube kubernetes.Interface) *Tracker {
	return &Tracker{
		Tracker: tracker.Tracker{
			Kube:             kube,
			Namespace:        namespace,
			FullResourceName: fmt.Sprintf("po/%s", name),
			ResourceName:     name,
			Context:          ctx,
		},

		Added:     make(chan PodStatus, 1),
		Succeeded: make(chan PodStatus, 0),
		Ready:     make(chan PodStatus, 0),
		Failed:    make(chan FailedReport, 0),
		Status:    make(chan PodStatus, 100),

		EventMsg:          make(chan string, 1),
		ContainerError:    make(chan ContainerErrorReport, 0),
		ContainerLogChunk: make(chan *ContainerLogChunk, 1000),

		State:                           tracker.Initial,
		ContainerTrackerStates:          make(map[string]tracker.TrackerState),
		ProcessedContainerLogTimestamps: make(map[string]time.Time),
		TrackedContainers:               make([]string, 0),
		LogsFromTime:                    time.Time{},

		objectAdded:    make(chan *corev1.Pod, 0),
		objectModified: make(chan *corev1.Pod, 0),
		objectDeleted:  make(chan *corev1.Pod, 0),
		objectFailed:   make(chan string, 1),
		errors:         make(chan error, 0),
		containerDone:  make(chan string, 10),
	}
}

func (pod *Tracker) Start() error {
	var containersTrackersIsRunning bool

	err := pod.runInformer()
	if err != nil {
		return err
	}

	for {
		select {
		case object := <-pod.objectAdded:
			pod.runEventsInformer()

			if !containersTrackersIsRunning {
				if err := pod.runContainersTrackers(object); err != nil {
					return fmt.Errorf("unable to track pod %s containers: %s", pod.ResourceName, err)
				}
				containersTrackersIsRunning = true
			}

			if err := pod.handlePodState(object); err != nil {
				return err
			}

		case object := <-pod.objectModified:
			if err := pod.handlePodState(object); err != nil {
				return err
			}

		case <-pod.objectDeleted:
			pod.lastObject = nil
			pod.State = tracker.ResourceDeleted
			pod.failedReason = "resource deleted"
			pod.LastStatus = PodStatus{IsFailed: true, FailedReason: pod.failedReason}

			keys := []string{}
			for k := range pod.ContainerTrackerStates {
				keys = append(keys, k)
			}
			for _, k := range keys {
				pod.ContainerTrackerStates[k] = tracker.ContainerTrackerDone
			}

			pod.Failed <- FailedReport{PodStatus: pod.LastStatus, FailedReason: pod.LastStatus.FailedReason}
			// TODO (longterm): This is not a fail, object may disappear then appear again.
			// TODO (longterm): At this level tracker should allow that situation and still continue tracking.

		case reason := <-pod.objectFailed:
			pod.State = tracker.ResourceFailed
			pod.failedReason = reason

			var status PodStatus
			if pod.lastObject != nil {
				pod.statusGeneration++
				status = NewPodStatus(pod.lastObject, pod.statusGeneration, pod.TrackedContainers, pod.State == tracker.ResourceFailed, pod.failedReason)
			} else {
				status = PodStatus{IsFailed: true, FailedReason: reason}
			}

			pod.LastStatus = status
			pod.Failed <- FailedReport{PodStatus: status, FailedReason: reason}

		case containerName := <-pod.containerDone:
			trackedContainers := make([]string, 0)
			for _, name := range pod.TrackedContainers {
				if name != containerName {
					trackedContainers = append(trackedContainers, name)
				}
			}
			pod.TrackedContainers = trackedContainers

			if err := pod.handlePodState(pod.lastObject); err != nil {
				return err
			}

		case <-pod.Context.Done():
			return pod.Context.Err()
		case err := <-pod.errors:
			return err
		}
	}
}

func (pod *Tracker) handlePodState(object *corev1.Pod) error {
	pod.lastObject = object
	pod.statusGeneration++

	status := NewPodStatus(object, pod.statusGeneration, pod.TrackedContainers, pod.State == tracker.ResourceFailed, pod.failedReason)
	pod.LastStatus = status

	if err := pod.handleContainersState(object); err != nil {
		return fmt.Errorf("unable to handle pod containers state: %s", err)
	}

	for containerName, msg := range status.ContainersErrors {
		pod.ContainerError <- ContainerErrorReport{
			ContainerError: ContainerError{
				ContainerName: containerName,
				Message:       msg,
			},
			PodStatus: status,
		}
	}

	switch pod.State {
	case tracker.Initial:
		if status.IsFailed {
			pod.State = tracker.ResourceFailed
			pod.Failed <- FailedReport{PodStatus: status, FailedReason: status.FailedReason}
		} else if status.IsSucceeded {
			pod.State = tracker.ResourceSucceeded
			pod.Succeeded <- status
		} else if status.IsReady {
			pod.State = tracker.ResourceReady
			pod.Ready <- status
		} else {
			pod.State = tracker.ResourceAdded
			pod.Added <- status
		}
	case tracker.ResourceAdded:
	case tracker.ResourceFailed:
		if status.IsFailed {
			pod.State = tracker.ResourceFailed
			pod.Failed <- FailedReport{PodStatus: status, FailedReason: status.FailedReason}
		} else if status.IsSucceeded {
			pod.State = tracker.ResourceSucceeded
			pod.Succeeded <- status
		} else if status.IsReady {
			pod.State = tracker.ResourceReady
			pod.Ready <- status
		} else {
			pod.Status <- status
		}
	case tracker.ResourceSucceeded:
		pod.Status <- status
	case tracker.ResourceReady:
		if status.IsFailed {
			pod.State = tracker.ResourceFailed
			pod.Failed <- FailedReport{PodStatus: status, FailedReason: status.FailedReason}
		} else if status.IsSucceeded {
			pod.State = tracker.ResourceSucceeded
			pod.Succeeded <- status
		} else {
			pod.Status <- status
		}
	}

	return nil
}

func (pod *Tracker) handleContainersState(object *corev1.Pod) error {
	allContainerStatuses := make([]corev1.ContainerStatus, 0)
	for _, cs := range object.Status.InitContainerStatuses {
		allContainerStatuses = append(allContainerStatuses, cs)
	}
	for _, cs := range object.Status.ContainerStatuses {
		allContainerStatuses = append(allContainerStatuses, cs)
	}

	for _, cs := range allContainerStatuses {
		oldState := pod.ContainerTrackerStates[cs.Name]

		if cs.State.Running != nil || cs.State.Terminated != nil {
			pod.ContainerTrackerStates[cs.Name] = tracker.FollowingContainerLogs
		}

		if debug.Debug() {
			if oldState != pod.ContainerTrackerStates[cs.Name] {
				fmt.Printf("pod/%s container/%s state changed %#v -> %#v\n", pod.ResourceName, cs.Name, oldState, pod.ContainerTrackerStates[cs.Name])
			}
		}
	}

	return nil
}

func (pod *Tracker) followContainerLogs(containerName string) error {
	logOpts := &corev1.PodLogOptions{
		Container:  containerName,
		Timestamps: true,
		Follow:     true,
	}
	if !pod.LogsFromTime.IsZero() {
		logOpts.SinceTime = &metav1.Time{
			Time: pod.LogsFromTime,
		}
	}
	req := pod.Kube.CoreV1().
		Pods(pod.Namespace).
		GetLogs(pod.ResourceName, logOpts)

	readCloser, err := req.Stream()
	if err != nil {
		return err
	}
	defer readCloser.Close()

	chunkBuf := make([]byte, 1024*64)
	lineBuf := make([]byte, 0, 1024*4)

	for {
		n, err := readCloser.Read(chunkBuf)

		if n > 0 {
			chunkLines := make([]display.LogLine, 0)
			for i := 0; i < n; i++ {
				bt := chunkBuf[i]

				if bt == '\n' {
					line := string(lineBuf)
					lineBuf = lineBuf[:0]

					lineParts := strings.SplitN(line, " ", 2)
					if len(lineParts) == 2 {
						chunkLines = append(chunkLines, display.LogLine{Timestamp: lineParts[0], Message: lineParts[1]})
					}

					continue
				}

				lineBuf = append(lineBuf, bt)
			}

			pod.ContainerLogChunk <- &ContainerLogChunk{
				ContainerName: containerName,
				LogLines:      chunkLines,
			}
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		select {
		case <-pod.Context.Done():
			return pod.Context.Err()
		default:
		}
	}

	return nil
}

func (pod *Tracker) trackContainer(containerName string) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			state := pod.ContainerTrackerStates[containerName]

			switch state {
			case tracker.FollowingContainerLogs:
				err := pod.followContainerLogs(containerName)
				if err != nil {
					if debug.Debug() {
						fmt.Fprintf(os.Stderr, "Pod `%s` Container `%s` logs streaming error: %s\n", pod.ResourceName, containerName, err)
					}
				}
				return nil
			case tracker.Initial:
			case tracker.ContainerTrackerDone:
				return nil
			default:
				return fmt.Errorf("unknown Pod's `%s` Container `%s` tracker state `%s`", pod.ResourceName, containerName, state)
			}

		case <-pod.Context.Done():
			return pod.Context.Err()
		}
	}
}

func (pod *Tracker) runContainersTrackers(object *corev1.Pod) error {
	allContainersNames := make([]string, 0)
	for _, containerConf := range object.Spec.InitContainers {
		allContainersNames = append(allContainersNames, containerConf.Name)
	}
	for _, containerConf := range object.Spec.Containers {
		allContainersNames = append(allContainersNames, containerConf.Name)
	}
	for i := range allContainersNames {
		containerName := allContainersNames[i]

		pod.ContainerTrackerStates[containerName] = tracker.Initial
		pod.TrackedContainers = append(pod.TrackedContainers, containerName)

		go func() {
			if debug.Debug() {
				fmt.Printf("Starting to track Pod's `%s` container `%s`\n", pod.ResourceName, containerName)
			}

			err := pod.trackContainer(containerName)
			if err != nil {
				pod.errors <- err
			}

			if debug.Debug() {
				fmt.Printf("Done tracking Pod's `%s` container `%s`\n", pod.ResourceName, containerName)
			}

			pod.containerDone <- containerName
		}()
	}

	return nil
}

func (pod *Tracker) runInformer() error {
	tweakListOptions := func(options metav1.ListOptions) metav1.ListOptions {
		options.FieldSelector = fields.OneTermEqualSelector("metadata.name", pod.ResourceName).String()
		return options
	}
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return pod.Kube.CoreV1().Pods(pod.Namespace).List(tweakListOptions(options))
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return pod.Kube.CoreV1().Pods(pod.Namespace).Watch(tweakListOptions(options))
		},
	}

	go func() {
		_, err := watchtools.UntilWithSync(pod.Context, lw, &corev1.Pod{}, nil, func(e watch.Event) (bool, error) {
			if debug.Debug() {
				fmt.Printf("Pod `%s` informer event: %#v\n", pod.ResourceName, e.Type)
			}

			var object *corev1.Pod

			if e.Type != watch.Error {
				var ok bool
				object, ok = e.Object.(*corev1.Pod)
				if !ok {
					return true, fmt.Errorf("TRACK POD EVENT %s expect *corev1.Pod object, got %T", pod.ResourceName, e.Object)
				}
			}

			if e.Type == watch.Added {
				pod.objectAdded <- object
			} else if e.Type == watch.Modified {
				pod.objectModified <- object
			} else if e.Type == watch.Deleted {
				pod.objectDeleted <- object
			} else if e.Type == watch.Error {
				pod.errors <- fmt.Errorf("Pod %s error: %v", pod.ResourceName, e.Object)
			}

			return false, nil
		})

		if err != nil {
			pod.errors <- err
		}

		if debug.Debug() {
			fmt.Printf("Pod `%s` informer done\n", pod.ResourceName)
		}
	}()

	return nil
}

// runEventsInformer watch for DaemonSet events
func (pod *Tracker) runEventsInformer() {
	if pod.lastObject == nil {
		return
	}

	eventInformer := event.NewEventInformer(&pod.Tracker, pod.lastObject)
	eventInformer.WithChannels(pod.EventMsg, pod.objectFailed, pod.errors)
	eventInformer.Run()

	return
}
