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

	Added  chan DeploymentStatus
	Ready  chan DeploymentStatus
	Failed chan DeploymentStatus

	EventMsg        chan string
	AddedReplicaSet chan replicaset.ReplicaSet
	AddedPod        chan replicaset.ReplicaSetPod
	PodLogChunk     chan *replicaset.ReplicaSetPodLogChunk
	PodError        chan replicaset.ReplicaSetPodError
	StatusReport    chan DeploymentStatus

	resourceAdded         chan *appsv1.Deployment
	resourceModified      chan *appsv1.Deployment
	resourceDeleted       chan *appsv1.Deployment
	resourceFailed        chan string
	replicaSetAdded       chan *appsv1.ReplicaSet
	replicaSetModified    chan *appsv1.ReplicaSet
	replicaSetDeleted     chan *appsv1.ReplicaSet
	podAdded              chan *corev1.Pod
	podDone               chan string
	errors                chan error
	podStatusesReport     chan map[string]pod.PodStatus
	replicaSetPodLogChunk chan *replicaset.ReplicaSetPodLogChunk
	replicaSetPodError    chan replicaset.ReplicaSetPodError

	TrackedPods []string
}

func NewTracker(ctx context.Context, name, namespace string, kube kubernetes.Interface, opts tracker.Options) *Tracker {
	if debug.Debug() {
		fmt.Printf("> deployment.NewTracker\n")
	}
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

		EventMsg:        make(chan string, 1),
		AddedReplicaSet: make(chan replicaset.ReplicaSet, 10),
		AddedPod:        make(chan replicaset.ReplicaSetPod, 10),
		PodLogChunk:     make(chan *replicaset.ReplicaSetPodLogChunk, 1000),
		PodError:        make(chan replicaset.ReplicaSetPodError, 0),
		StatusReport:    make(chan DeploymentStatus, 100),
		//PodReady:        make(chan bool, 1),

		knownReplicaSets: make(map[string]*appsv1.ReplicaSet),
		podStatuses:      make(map[string]pod.PodStatus),
		rsNameByPod:      make(map[string]string),

		TrackedPods: make([]string, 0),

		//PodError: make(chan PodError, 0),
		resourceAdded:         make(chan *appsv1.Deployment, 1),
		resourceModified:      make(chan *appsv1.Deployment, 1),
		resourceDeleted:       make(chan *appsv1.Deployment, 1),
		resourceFailed:        make(chan string, 1),
		replicaSetAdded:       make(chan *appsv1.ReplicaSet, 1),
		replicaSetModified:    make(chan *appsv1.ReplicaSet, 1),
		replicaSetDeleted:     make(chan *appsv1.ReplicaSet, 1),
		podAdded:              make(chan *corev1.Pod, 1),
		podDone:               make(chan string, 1),
		errors:                make(chan error, 0),
		podStatusesReport:     make(chan map[string]pod.PodStatus),
		replicaSetPodLogChunk: make(chan *replicaset.ReplicaSetPodLogChunk, 1000),
		replicaSetPodError:    make(chan replicaset.ReplicaSetPodError, 1),
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
			if debug.Debug() {
				fmt.Printf("rs/%s added\n", rs.Name)
			}

			d.knownReplicaSets[rs.Name] = rs

			rsNew, err := utils.IsReplicaSetNew(d.lastObject, d.knownReplicaSets, rs.Name)
			if err != nil {
				return err
			}

			d.AddedReplicaSet <- replicaset.ReplicaSet{
				Name:  rs.Name,
				IsNew: rsNew,
			}

		case rs := <-d.replicaSetModified:
			if debug.Debug() {
				fmt.Printf("rs/%s modified\n", rs.Name)
			}

			d.knownReplicaSets[rs.Name] = rs

		case rs := <-d.replicaSetDeleted:
			delete(d.knownReplicaSets, rs.Name)

		case pod := <-d.podAdded:
			if debug.Debug() {
				fmt.Printf("po/%s added\n", pod.Name)
			}

			rsName := utils.GetPodReplicaSetName(pod)
			d.rsNameByPod[pod.Name] = rsName

			rsNew, err := utils.IsReplicaSetNew(d.lastObject, d.knownReplicaSets, rsName)
			if err != nil {
				return err
			}

			rsPod := replicaset.ReplicaSetPod{
				Name: pod.Name,
				ReplicaSet: replicaset.ReplicaSet{
					Name:  rsName,
					IsNew: rsNew,
				},
			}

			d.AddedPod <- rsPod

			err = d.runPodTracker(pod.Name, rsName)
			if err != nil {
				return err
			}

		case podName := <-d.podDone:
			trackedPods := make([]string, 0)
			for _, name := range d.TrackedPods {
				if name != podName {
					trackedPods = append(trackedPods, name)
				}
			}
			d.TrackedPods = trackedPods

		case podStatuses := <-d.podStatusesReport:
			for podName, podStatus := range podStatuses {
				d.podStatuses[podName] = podStatus
			}
			if d.lastObject != nil {
				d.statusGeneration++
				newPodsNames, err := d.getNewPodsNames()
				if err != nil {
					return err
				}
				d.StatusReport <- NewDeploymentStatus(d.lastObject, d.statusGeneration, (d.State == "Failed"), d.failedReason, d.podStatuses, newPodsNames)
			}

		case rsChunk := <-d.replicaSetPodLogChunk:
			rsNew, err := utils.IsReplicaSetNew(d.lastObject, d.knownReplicaSets, rsChunk.ReplicaSet.Name)
			if err != nil {
				return err
			}
			rsChunk.ReplicaSet.IsNew = rsNew
			d.PodLogChunk <- rsChunk

		case rsPodError := <-d.replicaSetPodError:
			rsNew, err := utils.IsReplicaSetNew(d.lastObject, d.knownReplicaSets, rsPodError.ReplicaSet.Name)
			if err != nil {
				return err
			}
			rsPodError.ReplicaSet.IsNew = rsNew
			d.PodError <- rsPodError

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
	podsInformer.WithChannels(d.podAdded, d.errors)
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
	d.TrackedPods = append(d.TrackedPods, podName)

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
			case chunk := <-podTracker.ContainerLogChunk:
				rsChunk := &replicaset.ReplicaSetPodLogChunk{
					PodLogChunk: &pod.PodLogChunk{
						ContainerLogChunk: chunk,
						PodName:           podTracker.ResourceName,
					},
					ReplicaSet: replicaset.ReplicaSet{
						Name: rsName,
					},
				}
				d.replicaSetPodLogChunk <- rsChunk

			case containerError := <-podTracker.ContainerError:
				podError := replicaset.ReplicaSetPodError{
					PodError: pod.PodError{
						ContainerError: containerError,
						PodName:        podTracker.ResourceName,
					},
					ReplicaSet: replicaset.ReplicaSet{
						Name: rsName,
					},
				}
				d.replicaSetPodError <- podError

			case msg := <-podTracker.EventMsg:
				d.EventMsg <- fmt.Sprintf("po/%s %s", podTracker.ResourceName, msg)
			case <-podTracker.Added:
			case <-podTracker.Succeeded:
			case <-podTracker.Failed:
			case <-podTracker.Ready:
			case podStatus := <-podTracker.StatusReport:
				d.podStatusesReport <- map[string]pod.PodStatus{podTracker.ResourceName: podStatus}
			case err := <-errorChan:
				d.errors <- err
				return
			case <-doneChan:
				d.podDone <- podTracker.ResourceName
				return
			}
		}
	}()

	return nil
}

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
			return err
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
			d.StatusReport <- status
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
