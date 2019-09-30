package multitrack

import (
	"fmt"
	"strings"

	"github.com/flant/kubedog/pkg/tracker/replicaset"
	"github.com/flant/kubedog/pkg/tracker/statefulset"
	"k8s.io/client-go/kubernetes"
)

func (mt *multitracker) TrackStatefulSet(kube kubernetes.Interface, spec MultitrackSpec, opts MultitrackOptions) error {
	feed := statefulset.NewFeed()

	feed.OnAdded(func(status statefulset.StatefulSetStatus) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.statefulsetAdded(spec, feed, status)
	})
	feed.OnReady(func(status statefulset.StatefulSetStatus) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.statefulsetReady(spec, feed, status)
	})
	feed.OnFailed(func(status statefulset.StatefulSetStatus) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.statefulsetFailed(spec, feed, status)
	})
	feed.OnEventMsg(func(msg string) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.statefulsetEventMsg(spec, feed, msg)
	})
	feed.OnAddedReplicaSet(func(rs replicaset.ReplicaSet) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.statefulsetAddedReplicaSet(spec, feed, rs)
	})
	feed.OnAddedPod(func(pod replicaset.ReplicaSetPod) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.statefulsetAddedPod(spec, feed, pod)
	})
	feed.OnPodError(func(podError replicaset.ReplicaSetPodError) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.statefulsetPodError(spec, feed, podError)
	})
	feed.OnPodLogChunk(func(chunk *replicaset.ReplicaSetPodLogChunk) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.statefulsetPodLogChunk(spec, feed, chunk)
	})
	feed.OnStatusReport(func(status statefulset.StatefulSetStatus) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.statefulsetStatusReport(spec, feed, status)
	})

	return feed.Track(spec.ResourceName, spec.Namespace, kube, opts.Options)
}

func (mt *multitracker) statefulsetAdded(spec MultitrackSpec, feed statefulset.Feed, status statefulset.StatefulSetStatus) error {
	mt.StatefulSetsStatuses[spec.ResourceName] = status

	if status.IsReady {
		mt.displayResourceTrackerMessageF("sts", spec, "appears to be READY")

		return mt.handleResourceReadyCondition(mt.TrackingStatefulSets, spec)
	}

	mt.displayResourceTrackerMessageF("sts", spec, "added")

	return nil
}

func (mt *multitracker) statefulsetReady(spec MultitrackSpec, feed statefulset.Feed, status statefulset.StatefulSetStatus) error {
	mt.StatefulSetsStatuses[spec.ResourceName] = status

	mt.displayResourceTrackerMessageF("sts", spec, "become READY")

	return mt.handleResourceReadyCondition(mt.TrackingStatefulSets, spec)
}

func (mt *multitracker) isPostOperationCouldNotBeCompletedError(reason string) bool {
	return strings.Index(reason, "The POST operation against Pod could not be completed at this time, please try again.") != -1
}

func (mt *multitracker) handlePostOperationCouldNotBeCompleted(spec MultitrackSpec, reason string) error {
	mt.displayResourceTrackerMessageF("sts", spec, "WARNING: %s", reason)
	return nil
}

func (mt *multitracker) statefulsetFailed(spec MultitrackSpec, feed statefulset.Feed, status statefulset.StatefulSetStatus) error {
	mt.StatefulSetsStatuses[spec.ResourceName] = status

	if mt.isPostOperationCouldNotBeCompletedError(status.FailedReason) {
		return mt.handlePostOperationCouldNotBeCompleted(spec, status.FailedReason)
	}

	mt.displayResourceErrorF("sts", spec, "%s", status.FailedReason)
	return mt.handleResourceFailure(mt.TrackingStatefulSets, "sts", spec, status.FailedReason)
}

func (mt *multitracker) statefulsetEventMsg(spec MultitrackSpec, feed statefulset.Feed, msg string) error {
	mt.displayResourceEventF("sts", spec, "%s", msg)
	return nil
}

func (mt *multitracker) statefulsetAddedReplicaSet(spec MultitrackSpec, feed statefulset.Feed, rs replicaset.ReplicaSet) error {
	mt.displayResourceTrackerMessageF("sts", spec, "rs/%s added", rs.Name)
	return nil
}

func (mt *multitracker) statefulsetAddedPod(spec MultitrackSpec, feed statefulset.Feed, pod replicaset.ReplicaSetPod) error {
	mt.displayResourceTrackerMessageF("sts", spec, "po/%s added", pod.Name)
	return nil
}

func (mt *multitracker) statefulsetPodError(spec MultitrackSpec, feed statefulset.Feed, podError replicaset.ReplicaSetPodError) error {
	reason := fmt.Sprintf("po/%s container/%s: %s", podError.PodName, podError.ContainerName, podError.Message)

	mt.displayResourceErrorF("sts", spec, "%s", reason)

	return mt.handleResourceFailure(mt.TrackingStatefulSets, "sts", spec, reason)
}

func (mt *multitracker) statefulsetPodLogChunk(spec MultitrackSpec, feed statefulset.Feed, chunk *replicaset.ReplicaSetPodLogChunk) error {
	controllerStatus := feed.GetStatus()
	if podStatus, hasKey := controllerStatus.Pods[chunk.PodName]; hasKey {
		if podStatus.IsReady {
			return nil
		}
	}

	mt.displayResourceLogChunk("sts", spec, podContainerLogChunkHeader(chunk.PodName, chunk.ContainerLogChunk), chunk.ContainerLogChunk)
	return nil
}

func (mt *multitracker) statefulsetStatusReport(spec MultitrackSpec, feed statefulset.Feed, status statefulset.StatefulSetStatus) error {
	mt.StatefulSetsStatuses[spec.ResourceName] = status
	return nil
}
