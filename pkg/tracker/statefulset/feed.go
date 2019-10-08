package statefulset

import (
	"context"
	"fmt"
	"sync"

	"github.com/flant/kubedog/pkg/tracker"
	"github.com/flant/kubedog/pkg/tracker/controller"
	"github.com/flant/kubedog/pkg/tracker/debug"
	"k8s.io/client-go/kubernetes"

	watchtools "k8s.io/client-go/tools/watch"
)

type Feed interface {
	controller.ControllerFeed

	OnStatusReport(func(StatefulSetStatus) error)

	GetStatus() StatefulSetStatus
	Track(name, namespace string, kube kubernetes.Interface, opts tracker.Options) error
}

func NewFeed() Feed {
	return &feed{}
}

type feed struct {
	controller.CommonControllerFeed

	OnStatusReportFunc func(StatefulSetStatus) error

	statusMux sync.Mutex
	status    StatefulSetStatus
}

func (f *feed) OnStatusReport(function func(StatefulSetStatus) error) {
	f.OnStatusReportFunc = function
}

func (f *feed) Track(name, namespace string, kube kubernetes.Interface, opts tracker.Options) error {
	errorChan := make(chan error, 0)
	doneChan := make(chan bool, 0)

	parentContext := opts.ParentContext
	if parentContext == nil {
		parentContext = context.Background()
	}
	ctx, cancel := watchtools.ContextWithOptionalTimeout(parentContext, opts.Timeout)
	defer cancel()

	stsTracker := NewTracker(ctx, name, namespace, kube, opts)

	go func() {
		if debug.Debug() {
			fmt.Printf("  goroutine: start statefulset/%s tracker\n", name)
		}
		err := stsTracker.Track()
		if err != nil {
			errorChan <- err
		} else {
			doneChan <- true
		}
	}()

	if debug.Debug() {
		fmt.Printf("  statefulset/%s: for-select stsTracker channels\n", name)
	}

	for {
		select {
		case status := <-stsTracker.Added:
			f.setStatus(status)

			if f.OnAddedFunc != nil {
				err := f.OnAddedFunc(status.IsReady)
				if err == tracker.StopTrack {
					return nil
				}
				if err != nil {
					return err
				}
			}

		case status := <-stsTracker.Ready:
			f.setStatus(status)

			if f.OnReadyFunc != nil {
				err := f.OnReadyFunc()
				if err == tracker.StopTrack {
					return nil
				}
				if err != nil {
					return err
				}
			}

		case status := <-stsTracker.Failed:
			f.setStatus(status)

			if f.OnFailedFunc != nil {
				err := f.OnFailedFunc(status.FailedReason)
				if err == tracker.StopTrack {
					return nil
				}
				if err != nil {
					return err
				}
			}

		case msg := <-stsTracker.EventMsg:
			if debug.Debug() {
				fmt.Printf("    statefulset/%s event: %s\n", name, msg)
			}

			if f.OnEventMsgFunc != nil {
				err := f.OnEventMsgFunc(msg)
				if err == tracker.StopTrack {
					return nil
				}
				if err != nil {
					return err
				}
			}

		case rsPod := <-stsTracker.AddedPod:
			if debug.Debug() {
				fmt.Printf("    statefulset/%s got new pod `%s`\n", stsTracker.ResourceName, rsPod.Name)
			}

			if f.OnAddedPodFunc != nil {
				err := f.OnAddedPodFunc(rsPod)
				if err == tracker.StopTrack {
					return nil
				}
				if err != nil {
					return err
				}

			}

		case chunk := <-stsTracker.PodLogChunk:
			if debug.Debug() {
				fmt.Printf("    statefulset/%s pod `%s` log chunk\n", stsTracker.ResourceName, chunk.PodName)
				for _, line := range chunk.LogLines {
					fmt.Printf("po/%s [%s] %s\n", chunk.PodName, line.Timestamp, line.Message)
				}
			}

			if f.OnPodLogChunkFunc != nil {
				err := f.OnPodLogChunkFunc(chunk)
				if err == tracker.StopTrack {
					return nil
				}
				if err != nil {
					return err
				}
			}

		case podError := <-stsTracker.PodError:
			if debug.Debug() {
				fmt.Printf("    statefulset/%s pod error: %s\n", stsTracker.ResourceName, podError.Message)
			}

			if f.OnPodErrorFunc != nil {
				err := f.OnPodErrorFunc(podError)
				if err == tracker.StopTrack {
					return nil
				}
				if err != nil {
					return err
				}
			}

		case status := <-stsTracker.StatusReport:
			f.setStatus(status)

			if f.OnStatusReportFunc != nil {
				err := f.OnStatusReportFunc(status)
				if err == tracker.StopTrack {
					return nil
				}
				if err != nil {
					return err
				}
			}

		case err := <-errorChan:
			return err
		case <-doneChan:
			return nil
		}
	}
}

func (f *feed) setStatus(status StatefulSetStatus) {
	f.statusMux.Lock()
	defer f.statusMux.Unlock()
	f.status = status
}

func (f *feed) GetStatus() StatefulSetStatus {
	f.statusMux.Lock()
	defer f.statusMux.Unlock()
	return f.status
}
