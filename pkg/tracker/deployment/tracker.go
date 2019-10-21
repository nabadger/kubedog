package deployment

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/flant/kubedog/pkg/tracker"
	"github.com/flant/kubedog/pkg/tracker/debug"
	"github.com/flant/kubedog/pkg/tracker/event"
	"github.com/flant/kubedog/pkg/tracker/pod"
	"github.com/flant/kubedog/pkg/tracker/replicaset"
	"github.com/flant/kubedog/pkg/utils"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watchtools "k8s.io/client-go/tools/watch"
)

type ReplicaSetAddedReport struct {
	ReplicaSet       replicaset.ReplicaSet
	DeploymentStatus DeploymentStatus
}

type PodAddedReport struct {
	ReplicaSetPod    replicaset.ReplicaSetPod
	DeploymentStatus DeploymentStatus
}

type PodErrorReport struct {
	ReplicaSetPodError replicaset.ReplicaSetPodError
	DeploymentStatus   DeploymentStatus
}

type Tracker struct {
	tracker.Tracker
	LogsFromTime time.Time

	State                 tracker.TrackerState
	Conditions            []string
	FinalDeploymentStatus appsv1.DeploymentStatus
	NewReplicaSetName     string
	CurrentReady          bool

	knownReplicaSets map[string]*appsv1.ReplicaSet
	lastObject       *appsv1.Deployment
	failedReason     string
	statusGeneration uint64
	podStatuses      map[string]pod.PodStatus
	rsNameByPod      map[string]string

	TrackedPodsNames []string

	Added  chan DeploymentStatus
	Ready  chan DeploymentStatus
	Failed chan DeploymentStatus
	Status chan DeploymentStatus

	EventMsg        chan string
	AddedReplicaSet chan ReplicaSetAddedReport
	AddedPod        chan PodAddedReport
	PodLogChunk     chan *replicaset.ReplicaSetPodLogChunk
	PodError        chan PodErrorReport

	resourceAdded      chan *appsv1.Deployment
	resourceModified   chan *appsv1.Deployment
	resourceDeleted    chan *appsv1.Deployment
	resourceFailed     chan string
	replicaSetAdded    chan *appsv1.ReplicaSet
	replicaSetModified chan *appsv1.ReplicaSet
	replicaSetDeleted  chan *appsv1.ReplicaSet
	errors             chan error

	podAddedRelay           chan *corev1.Pod
	podStatusesRelay        chan map[string]pod.PodStatus
	podLogChunksRelay       chan map[string]*pod.ContainerLogChunk
	podContainerErrorsRelay chan map[string]pod.ContainerErrorReport
	donePodsRelay           chan map[string]pod.PodStatus
}

func NewTracker(ctx context.Context, name, namespace string, kube kubernetes.Interface, opts tracker.Options) *Tracker {
	return &Tracker{
		Tracker: tracker.Tracker{
			Kube:             kube,
			Namespace:        namespace,
			FullResourceName: fmt.Sprintf("deploy/%s", name),
			ResourceName:     name,
			Context:          ctx,
		},

		LogsFromTime: opts.LogsFromTime,

		Added:  make(chan DeploymentStatus, 1),
		Ready:  make(chan DeploymentStatus, 0),
		Failed: make(chan DeploymentStatus, 0),
		Status: make(chan DeploymentStatus, 100),

		EventMsg:        make(chan string, 1),
		AddedReplicaSet: make(chan ReplicaSetAddedReport, 10),
		AddedPod:        make(chan PodAddedReport, 10),
		PodLogChunk:     make(chan *replicaset.ReplicaSetPodLogChunk, 1000),
		PodError:        make(chan PodErrorReport, 0),

		knownReplicaSets: make(map[string]*appsv1.ReplicaSet),
		podStatuses:      make(map[string]pod.PodStatus),
		rsNameByPod:      make(map[string]string),

		TrackedPodsNames: make([]string, 0),

		errors:             make(chan error, 0),
		resourceAdded:      make(chan *appsv1.Deployment, 1),
		resourceModified:   make(chan *appsv1.Deployment, 1),
		resourceDeleted:    make(chan *appsv1.Deployment, 1),
		resourceFailed:     make(chan string, 1),
		replicaSetAdded:    make(chan *appsv1.ReplicaSet, 1),
		replicaSetModified: make(chan *appsv1.ReplicaSet, 1),
		replicaSetDeleted:  make(chan *appsv1.ReplicaSet, 1),

		podAddedRelay:           make(chan *corev1.Pod, 1),
		podStatusesRelay:        make(chan map[string]pod.PodStatus, 10),
		podLogChunksRelay:       make(chan map[string]*pod.ContainerLogChunk, 10),
		podContainerErrorsRelay: make(chan map[string]pod.ContainerErrorReport, 10),
		donePodsRelay:           make(chan map[string]pod.PodStatus, 10),
	}
}

// Track starts tracking of deployment rollout process.
// watch only for one deployment resource with name d.ResourceName within the namespace with name d.Namespace
// Watcher can wait for namespace creation and then for deployment creation
// watcher receives added event if deployment is started
// watch is infinite by default
// there is option StopOnAvailable — if true, watcher stops after deployment has available status
// you can define custom stop triggers using custom implementation of ControllerFeed.
func (d *Tracker) Track() (err error) {
	if debug.Debug() {
		fmt.Printf("> DeploymentTracker.Track()\n")
	}

	d.runDeploymentInformer()

	for {
		select {
		case object := <-d.resourceAdded:
			d.runReplicaSetsInformer()
			d.runPodsInformer()
			d.runEventsInformer(object)

			if err := d.handleDeploymentState(object); err != nil {
				if debug.Debug() {
					fmt.Printf("handle deployment state error: %v", err)
				}
				return err
			}

		case object := <-d.resourceModified:
			if err := d.handleDeploymentState(object); err != nil {
				return err
			}

		case <-d.resourceDeleted:
			d.lastObject = nil
			d.State = tracker.ResourceDeleted
			d.failedReason = "resource deleted"
			d.Failed <- DeploymentStatus{IsFailed: true, FailedReason: d.failedReason}
			// TODO (longterm): This is not a fail, object may disappear then appear again.
			// TODO (longterm): At this level tracker should allow that situation and still continue tracking.

		case reason := <-d.resourceFailed:
			d.State = tracker.ResourceFailed
			d.failedReason = reason

			var status DeploymentStatus
			if d.lastObject != nil {
				d.statusGeneration++
				newPodsNames, err := d.getNewPodsNames()
				if err != nil {
					return err
				}
				status = NewDeploymentStatus(d.lastObject, d.statusGeneration, (d.State == tracker.ResourceFailed), d.failedReason, d.podStatuses, newPodsNames)
			} else {
				status = DeploymentStatus{IsFailed: true, FailedReason: reason}
			}
			d.Failed <- status

		case rs := <-d.replicaSetAdded:
			d.knownReplicaSets[rs.Name] = rs

			rsNew, err := utils.IsReplicaSetNew(d.lastObject, d.knownReplicaSets, rs.Name)
			if err != nil {
				return err
			}

			if d.lastObject != nil {
				d.statusGeneration++
				newPodsNames, err := d.getNewPodsNames()
				if err != nil {
					return err
				}
				status := NewDeploymentStatus(d.lastObject, d.statusGeneration, (d.State == tracker.ResourceFailed), d.failedReason, d.podStatuses, newPodsNames)

				d.AddedReplicaSet <- ReplicaSetAddedReport{
					ReplicaSet: replicaset.ReplicaSet{
						Name:  rs.Name,
						IsNew: rsNew,
					},
					DeploymentStatus: status,
				}
			}

		case rs := <-d.replicaSetModified:
			d.knownReplicaSets[rs.Name] = rs

		case rs := <-d.replicaSetDeleted:
			delete(d.knownReplicaSets, rs.Name)

		case pod := <-d.podAddedRelay:
			rsName := utils.GetPodReplicaSetName(pod)
			d.rsNameByPod[pod.Name] = rsName

			if d.lastObject != nil {
				d.statusGeneration++
				newPodsNames, err := d.getNewPodsNames()
				if err != nil {
					return err
				}
				rsNew, err := utils.IsReplicaSetNew(d.lastObject, d.knownReplicaSets, rsName)
				if err != nil {
					return err
				}
				status := NewDeploymentStatus(d.lastObject, d.statusGeneration, (d.State == tracker.ResourceFailed), d.failedReason, d.podStatuses, newPodsNames)

				d.AddedPod <- PodAddedReport{
					ReplicaSetPod: replicaset.ReplicaSetPod{
						Name: pod.Name,
						ReplicaSet: replicaset.ReplicaSet{
							Name:  rsName,
							IsNew: rsNew,
						},
					},
					DeploymentStatus: status,
				}
			}

			if err := d.runPodTracker(pod.Name, rsName); err != nil {
				return err
			}

		case donePods := <-d.donePodsRelay:
			trackedPodsNames := make([]string, 0)

		trackedPodsIteration:
			for _, name := range d.TrackedPodsNames {
				for donePodName, status := range donePods {
					if name == donePodName {
						// This Pod is no more tracked,
						// but we need to update final
						// Pod's status
						if _, hasKey := d.podStatuses[name]; hasKey {
							d.podStatuses[name] = status
						}
						continue trackedPodsIteration
					}
				}

				trackedPodsNames = append(trackedPodsNames, name)
			}
			d.TrackedPodsNames = trackedPodsNames

			if err := d.handleDeploymentState(d.lastObject); err != nil {
				return err
			}

		case podStatuses := <-d.podStatusesRelay:
			for podName, podStatus := range podStatuses {
				d.podStatuses[podName] = podStatus
			}
			if d.lastObject != nil {
				d.statusGeneration++
				newPodsNames, err := d.getNewPodsNames()
				if err != nil {
					return err
				}
				d.Status <- NewDeploymentStatus(d.lastObject, d.statusGeneration, (d.State == "Failed"), d.failedReason, d.podStatuses, newPodsNames)
			}

		case podLogChunks := <-d.podLogChunksRelay:
			for podName, chunk := range podLogChunks {
				rsName, hasKey := d.rsNameByPod[podName]
				if !hasKey {
					continue
				}

				rsNew, err := utils.IsReplicaSetNew(d.lastObject, d.knownReplicaSets, rsName)
				if err != nil {
					return err
				}

				rsChunk := &replicaset.ReplicaSetPodLogChunk{
					PodLogChunk: &pod.PodLogChunk{
						ContainerLogChunk: chunk,
						PodName:           podName,
					},
					ReplicaSet: replicaset.ReplicaSet{
						Name:  rsName,
						IsNew: rsNew,
					},
				}
				d.PodLogChunk <- rsChunk
			}

		case podContainerErrors := <-d.podContainerErrorsRelay:
			for podName, containerError := range podContainerErrors {
				d.podStatuses[podName] = containerError.PodStatus
			}
			if d.lastObject != nil {
				d.statusGeneration++
				newPodsNames, err := d.getNewPodsNames()
				if err != nil {
					return err
				}
				status := NewDeploymentStatus(d.lastObject, d.statusGeneration, (d.State == "Failed"), d.failedReason, d.podStatuses, newPodsNames)

				for podName, containerError := range podContainerErrors {
					rsName, hasKey := d.rsNameByPod[podName]
					if !hasKey {
						continue
					}

					rsNew, err := utils.IsReplicaSetNew(d.lastObject, d.knownReplicaSets, rsName)
					if err != nil {
						return err
					}

					d.PodError <- PodErrorReport{
						ReplicaSetPodError: replicaset.ReplicaSetPodError{
							PodError: pod.PodError{
								ContainerError: containerError.ContainerError,
								PodName:        podName,
							},
							ReplicaSet: replicaset.ReplicaSet{
								Name:  rsName,
								IsNew: rsNew,
							},
						},
						DeploymentStatus: status,
					}
				}

			}

		case <-d.Context.Done():
			return d.Context.Err()
		case err := <-d.errors:
			return err
		}
	}

	return err
}

func (d *Tracker) getNewPodsNames() ([]string, error) {
	res := []string{}

	for podName, _ := range d.podStatuses {
		if rsName, hasKey := d.rsNameByPod[podName]; hasKey {
			rsNew, err := utils.IsReplicaSetNew(d.lastObject, d.knownReplicaSets, rsName)
			if err != nil {
				return nil, err
			}
			if rsNew {
				res = append(res, podName)
			}
		}
	}

	return res, nil
}

// runDeploymentInformer watch for deployment events
func (d *Tracker) runDeploymentInformer() {
	client := d.Kube

	tweakListOptions := func(options metav1.ListOptions) metav1.ListOptions {
		options.FieldSelector = fields.OneTermEqualSelector("metadata.name", d.ResourceName).String()
		return options
	}
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return client.AppsV1().Deployments(d.Namespace).List(tweakListOptions(options))
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return client.AppsV1().Deployments(d.Namespace).Watch(tweakListOptions(options))
		},
	}

	go func() {
		_, err := watchtools.UntilWithSync(d.Context, lw, &appsv1.Deployment{}, nil, func(e watch.Event) (bool, error) {
			if debug.Debug() {
				fmt.Printf("    deploy/%s event: %#v\n", d.ResourceName, e.Type)
			}

			var object *appsv1.Deployment

			if e.Type != watch.Error {
				var ok bool
				object, ok = e.Object.(*appsv1.Deployment)
				if !ok {
					return true, fmt.Errorf("expected %s to be a *extension.Deployment, got %T", d.ResourceName, e.Object)
				}
			}

			switch e.Type {
			case watch.Added:
				d.resourceAdded <- object
			case watch.Modified:
				d.resourceModified <- object
			case watch.Deleted:
				d.resourceDeleted <- object
			case watch.Error:
				err := fmt.Errorf("deployment error: %v", e.Object)
				//d.errors <- err
				return true, err
			}

			return false, nil
		})

		if err != nil {
			d.errors <- err
		}

		if debug.Debug() {
			fmt.Printf("      deploy/%s informer DONE\n", d.ResourceName)
		}
	}()

	return
}

// runReplicaSetsInformer watch for deployment events
func (d *Tracker) runReplicaSetsInformer() {
	if d.lastObject == nil {
		// This shouldn't happen!
		// TODO add error
		return
	}

	rsInformer := replicaset.NewReplicaSetInformer(&d.Tracker, utils.ControllerAccessor(d.lastObject))
	rsInformer.WithChannels(d.replicaSetAdded, d.replicaSetModified, d.replicaSetDeleted, d.errors)
	rsInformer.Run()

	return
}

// runDeploymentInformer watch for deployment events
func (d *Tracker) runPodsInformer() {
	if d.lastObject == nil {
		// This shouldn't happen!
		// TODO add error
		return
	}

	podsInformer := pod.NewPodsInformer(&d.Tracker, utils.ControllerAccessor(d.lastObject))
	podsInformer.WithChannels(d.podAddedRelay, d.errors)
	podsInformer.Run()

	return
}

func (d *Tracker) runPodTracker(podName, rsName string) error {
	errorChan := make(chan error, 0)
	doneChan := make(chan struct{}, 0)

	podTracker := pod.NewTracker(d.Context, podName, d.Namespace, d.Kube)
	if !d.LogsFromTime.IsZero() {
		podTracker.LogsFromTime = d.LogsFromTime
	}
	d.TrackedPodsNames = append(d.TrackedPodsNames, podName)

	go func() {
		if debug.Debug() {
			fmt.Printf("Starting Deployment's `%s` Pod `%s` tracker. pod state: %v\n", d.ResourceName, podTracker.ResourceName, podTracker.State)
		}

		err := podTracker.Start()
		if err != nil {
			errorChan <- err
		} else {
			doneChan <- struct{}{}
		}

		if debug.Debug() {
			fmt.Printf("Done Deployment's `%s` Pod `%s` tracker\n", d.ResourceName, podTracker.ResourceName)
		}
	}()

	go func() {
		for {
			select {
			case status := <-podTracker.Added:
				d.podStatusesRelay <- map[string]pod.PodStatus{podTracker.ResourceName: status}
			case status := <-podTracker.Succeeded:
				d.podStatusesRelay <- map[string]pod.PodStatus{podTracker.ResourceName: status}
			case report := <-podTracker.Failed:
				d.podStatusesRelay <- map[string]pod.PodStatus{podTracker.ResourceName: report.PodStatus}
			case status := <-podTracker.Ready:
				d.podStatusesRelay <- map[string]pod.PodStatus{podTracker.ResourceName: status}
			case status := <-podTracker.Status:
				d.podStatusesRelay <- map[string]pod.PodStatus{podTracker.ResourceName: status}

			case msg := <-podTracker.EventMsg:
				d.EventMsg <- fmt.Sprintf("po/%s %s", podTracker.ResourceName, msg)
			case chunk := <-podTracker.ContainerLogChunk:
				d.podLogChunksRelay <- map[string]*pod.ContainerLogChunk{podTracker.ResourceName: chunk}
			case report := <-podTracker.ContainerError:
				d.podContainerErrorsRelay <- map[string]pod.ContainerErrorReport{podTracker.ResourceName: report}

			case err := <-errorChan:
				d.errors <- err
				return
			case <-doneChan:
				d.donePodsRelay <- map[string]pod.PodStatus{podTracker.ResourceName: podTracker.LastStatus}
				return
			}
		}
	}()

	return nil
}

// FIXME: states
func (d *Tracker) handleDeploymentState(object *appsv1.Deployment) error {
	if debug.Debug() {
		fmt.Printf("%s\n%s\n",
			getDeploymentStatus(d.Kube, d.lastObject, object),
			getReplicaSetsStatus(d.Kube, object))
	}

	if debug.Debug() {
		evList, err := utils.ListEventsForObject(d.Kube, object)
		if err != nil {
			fmt.Fprintf(os.Stderr, "DEBUG: ERROR fetching list of events for deploy/%s in %s: %s\n", object.Name, object.Namespace, err)
			return nil
		}
		utils.DescribeEvents(evList)
	}

	prevReady := false
	if d.lastObject != nil {
		prevReady = d.CurrentReady
	}
	d.lastObject = object

	d.statusGeneration++

	newPodsNames, err := d.getNewPodsNames()
	if err != nil {
		return err
	}
	status := NewDeploymentStatus(object, d.statusGeneration, (d.State == tracker.ResourceFailed), d.failedReason, d.podStatuses, newPodsNames)

	d.CurrentReady = status.IsReady

	switch d.State {
	case tracker.Initial:
		d.State = tracker.ResourceAdded
		d.Added <- status
	default:
		if prevReady == false && d.CurrentReady == true {
			d.FinalDeploymentStatus = object.Status
			d.Ready <- status
		} else {
			d.Status <- status
		}
	}

	return nil
}

// runEventsInformer watch for Deployment events
func (d *Tracker) runEventsInformer(resource interface{}) {
	//if d.lastObject == nil {
	//	return
	//}

	eventInformer := event.NewEventInformer(&d.Tracker, resource)
	eventInformer.WithChannels(d.EventMsg, d.resourceFailed, d.errors)
	eventInformer.Run()

	return
}
