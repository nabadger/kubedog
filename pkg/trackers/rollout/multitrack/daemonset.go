package multitrack

import (
	"fmt"

	"github.com/flant/kubedog/pkg/tracker/daemonset"
	"github.com/flant/kubedog/pkg/tracker/replicaset"
	"k8s.io/client-go/kubernetes"
)

func (mt *multitracker) TrackDaemonSet(kube kubernetes.Interface, spec MultitrackSpec, opts MultitrackOptions) error {
	feed := daemonset.NewFeed()

	feed.OnAdded(func(status daemonset.DaemonSetStatus) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.daemonsetAdded(spec, feed, status)
	})
	feed.OnReady(func(status daemonset.DaemonSetStatus) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.daemonsetReady(spec, feed, status)
	})
	feed.OnFailed(func(status daemonset.DaemonSetStatus) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.daemonsetFailed(spec, feed, status)
	})
	feed.OnEventMsg(func(msg string) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.daemonsetEventMsg(spec, feed, msg)
	})
	feed.OnAddedReplicaSet(func(rs replicaset.ReplicaSet) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.daemonsetAddedReplicaSet(spec, feed, rs)
	})
	feed.OnAddedPod(func(pod replicaset.ReplicaSetPod) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.daemonsetAddedPod(spec, feed, pod)
	})
	feed.OnPodError(func(podError replicaset.ReplicaSetPodError) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.daemonsetPodError(spec, feed, podError)
	})
	feed.OnPodLogChunk(func(chunk *replicaset.ReplicaSetPodLogChunk) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.daemonsetPodLogChunk(spec, feed, chunk)
	})
	feed.OnStatusReport(func(status daemonset.DaemonSetStatus) error {
		mt.mux.Lock()
		defer mt.mux.Unlock()
		return mt.daemonsetStatusReport(spec, feed, status)
	})

	return feed.Track(spec.ResourceName, spec.Namespace, kube, opts.Options)
}

func (mt *multitracker) daemonsetAdded(spec MultitrackSpec, feed daemonset.Feed, status daemonset.DaemonSetStatus) error {
	mt.DaemonSetsStatuses[spec.ResourceName] = status

	if status.IsReady {
		mt.displayResourceTrackerMessageF("ds", spec, "appears to be READY")

		return mt.handleResourceReadyCondition(mt.TrackingDaemonSets, spec)
	}

	mt.displayResourceTrackerMessageF("ds", spec, "added")

	return nil
}

func (mt *multitracker) daemonsetReady(spec MultitrackSpec, feed daemonset.Feed, status daemonset.DaemonSetStatus) error {
	mt.DaemonSetsStatuses[spec.ResourceName] = status

	mt.displayResourceTrackerMessageF("ds", spec, "become READY")

	return mt.handleResourceReadyCondition(mt.TrackingDaemonSets, spec)
}

func (mt *multitracker) daemonsetFailed(spec MultitrackSpec, feed daemonset.Feed, status daemonset.DaemonSetStatus) error {
	mt.DaemonSetsStatuses[spec.ResourceName] = status

	mt.displayResourceErrorF("ds", spec, "%s", status.FailedReason)

	return mt.handleResourceFailure(mt.TrackingDaemonSets, "ds", spec, status.FailedReason)
}

func (mt *multitracker) daemonsetEventMsg(spec MultitrackSpec, feed daemonset.Feed, msg string) error {
	mt.displayResourceEventF("ds", spec, "%s", msg)
	return nil
}

func (mt *multitracker) daemonsetAddedReplicaSet(spec MultitrackSpec, feed daemonset.Feed, rs replicaset.ReplicaSet) error {
	mt.displayResourceTrackerMessageF("ds", spec, "rs/%s added", rs.Name)
	return nil
}

func (mt *multitracker) daemonsetAddedPod(spec MultitrackSpec, feed daemonset.Feed, pod replicaset.ReplicaSetPod) error {
	mt.displayResourceTrackerMessageF("ds", spec, "po/%s added", pod.Name)
	return nil
}

func (mt *multitracker) daemonsetPodError(spec MultitrackSpec, feed daemonset.Feed, podError replicaset.ReplicaSetPodError) error {
	reason := fmt.Sprintf("po/%s container/%s: %s", podError.PodName, podError.ContainerName, podError.Message)

	mt.displayResourceErrorF("ds", spec, "%s", reason)

	return mt.handleResourceFailure(mt.TrackingDaemonSets, "ds", spec, reason)
}

func (mt *multitracker) daemonsetPodLogChunk(spec MultitrackSpec, feed daemonset.Feed, chunk *replicaset.ReplicaSetPodLogChunk) error {
	controllerStatus := feed.GetStatus()
	if podStatus, hasKey := controllerStatus.Pods[chunk.PodName]; hasKey {
		if podStatus.IsReady {
			return nil
		}
	}

	mt.displayResourceLogChunk("ds", spec, podContainerLogChunkHeader(chunk.PodName, chunk.ContainerLogChunk), chunk.ContainerLogChunk)
	return nil
}

func (mt *multitracker) daemonsetStatusReport(spec MultitrackSpec, feed daemonset.Feed, status daemonset.DaemonSetStatus) error {
	mt.DaemonSetsStatuses[spec.ResourceName] = status
	return nil
}
