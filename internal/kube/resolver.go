package kube

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/grepplabs/vectap/internal/targets"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type Resolver struct {
	client kubernetes.Interface
}

const observeRetryDelay = time.Second

func NewResolver(client kubernetes.Interface) *Resolver {
	return &Resolver{client: client}
}

func NewResolverFromConfig(kubeConfigPath, kubeContext string) (*Resolver, error) {
	cfg, err := LoadConfig(kubeConfigPath, kubeContext)
	if err != nil {
		return nil, err
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}

	return NewResolver(client), nil
}

func (r *Resolver) Resolve(ctx context.Context, opts targets.ResolveOptions) ([]targets.Target, error) {
	selector, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("parse label selector: %w", err)
	}

	pods, err := r.client.CoreV1().Pods(opts.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	out := make([]targets.Target, 0, len(pods.Items))
	for _, pod := range pods.Items {
		target, ok := targetFromPod(pod, selector, opts.RemotePort)
		if ok {
			out = append(out, target)
		}
	}

	return sortTargets(out), nil
}

func (r *Resolver) Observe(ctx context.Context, opts targets.ResolveOptions) (<-chan []targets.Target, <-chan error) {
	out := make(chan []targets.Target)
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)

		selector, err := labels.Parse(opts.LabelSelector)
		if err != nil {
			sendObserveError(ctx, errCh, fmt.Errorf("parse label selector: %w", err))
			return
		}

		var last []targets.Target
		for r.observeCycle(ctx, opts, selector, out, errCh, &last) {
		}
	}()

	return out, errCh
}

func (r *Resolver) observeCycle(
	ctx context.Context,
	opts targets.ResolveOptions,
	selector labels.Selector,
	out chan<- []targets.Target,
	errCh chan<- error,
	last *[]targets.Target,
) bool {
	podList, knownPods, err := r.listKnownPods(ctx, opts.Namespace)
	if err != nil {
		return retryObserveLoop(ctx, errCh, fmt.Errorf("list pods: %w", err))
	}

	if !emitSnapshot(ctx, out, snapshotTargets(knownPods, selector, opts.RemotePort), last) {
		return false
	}

	watcher, err := r.client.CoreV1().Pods(opts.Namespace).Watch(ctx, metav1.ListOptions{
		ResourceVersion:     podList.ResourceVersion,
		AllowWatchBookmarks: true,
	})
	if err != nil {
		return retryObserveLoop(ctx, errCh, fmt.Errorf("watch pods: %w", err))
	}
	defer watcher.Stop()

	return watchUntilRelistOrStop(ctx, watcher, knownPods, selector, opts.RemotePort, out, errCh, last)
}

func (r *Resolver) listKnownPods(ctx context.Context, namespace string) (*v1.PodList, map[string]*v1.Pod, error) {
	podList, err := r.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, err
	}

	knownPods := make(map[string]*v1.Pod, len(podList.Items))
	for i := range podList.Items {
		pod := podList.Items[i].DeepCopy()
		knownPods[pod.Namespace+"/"+pod.Name] = pod
	}
	return podList, knownPods, nil
}

func watchUntilRelistOrStop(
	ctx context.Context,
	watcher watch.Interface,
	knownPods map[string]*v1.Pod,
	selector labels.Selector,
	remotePort int,
	out chan<- []targets.Target,
	errCh chan<- error,
	last *[]targets.Target,
) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return true
			}
			relist, stop := applyObserveEvent(ctx, event, knownPods, selector, remotePort, out, errCh, last)
			if stop {
				return false
			}
			if relist {
				return true
			}
		}
	}
}

func applyObserveEvent(
	ctx context.Context,
	event watch.Event,
	knownPods map[string]*v1.Pod,
	selector labels.Selector,
	remotePort int,
	out chan<- []targets.Target,
	errCh chan<- error,
	last *[]targets.Target,
) (bool, bool) {
	next, changed, err := applyWatchEvent(knownPods, event, selector, remotePort)
	if err != nil {
		sendObserveError(ctx, errCh, err)
		return true, false
	}
	if changed && !emitSnapshot(ctx, out, next, last) {
		return false, true
	}
	return false, false
}

func retryObserveLoop(ctx context.Context, errCh chan<- error, err error) bool {
	return sendObserveError(ctx, errCh, err) && waitObserveRetry(ctx)
}

func isReady(conds []v1.PodCondition) bool {
	for _, cond := range conds {
		if cond.Type == v1.PodReady {
			return cond.Status == v1.ConditionTrue
		}
	}
	return false
}

func copyLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func targetFromPod(pod v1.Pod, selector labels.Selector, remotePort int) (targets.Target, bool) {
	if !selector.Matches(labels.Set(pod.Labels)) {
		return targets.Target{}, false
	}
	if pod.Status.Phase != v1.PodRunning {
		return targets.Target{}, false
	}
	if !isReady(pod.Status.Conditions) {
		return targets.Target{}, false
	}

	return targets.Target{
		ID:         pod.Namespace + "/" + pod.Name,
		Namespace:  pod.Namespace,
		PodName:    pod.Name,
		PodIP:      pod.Status.PodIP,
		RemotePort: remotePort,
		Labels:     copyLabels(pod.Labels),
	}, true
}

func sortTargets(out []targets.Target) []targets.Target {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].PodName < out[j].PodName
	})
	return out
}

func snapshotTargets(knownPods map[string]*v1.Pod, selector labels.Selector, remotePort int) []targets.Target {
	out := make([]targets.Target, 0, len(knownPods))
	for _, pod := range knownPods {
		target, ok := targetFromPod(*pod, selector, remotePort)
		if ok {
			out = append(out, target)
		}
	}
	return sortTargets(out)
}

func emitSnapshot(ctx context.Context, out chan<- []targets.Target, current []targets.Target, last *[]targets.Target) bool {
	if reflect.DeepEqual(current, *last) {
		return true
	}

	snapshot := append([]targets.Target(nil), current...)
	select {
	case <-ctx.Done():
		return false
	case out <- snapshot:
		*last = snapshot
		return true
	}
}

func applyWatchEvent(knownPods map[string]*v1.Pod, event watch.Event, selector labels.Selector, remotePort int) ([]targets.Target, bool, error) {
	switch event.Type {
	case watch.Bookmark:
		return nil, false, nil
	case watch.Error:
		return nil, false, fmt.Errorf("watch pods: %v", event.Object)
	case watch.Added, watch.Modified:
		pod, ok := event.Object.(*v1.Pod)
		if !ok {
			return nil, false, fmt.Errorf("watch pods: unexpected object type %T", event.Object)
		}
		knownPods[pod.Namespace+"/"+pod.Name] = pod.DeepCopy()
	case watch.Deleted:
		pod, ok := event.Object.(*v1.Pod)
		if !ok {
			return nil, false, fmt.Errorf("watch pods: unexpected object type %T", event.Object)
		}
		delete(knownPods, pod.Namespace+"/"+pod.Name)
	default:
		return nil, false, nil
	}

	return snapshotTargets(knownPods, selector, remotePort), true, nil
}

func sendObserveError(ctx context.Context, errCh chan<- error, err error) bool {
	select {
	case <-ctx.Done():
		return false
	case errCh <- err:
		return true
	default:
		return true
	}
}

func waitObserveRetry(ctx context.Context) bool {
	timer := time.NewTimer(observeRetryDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
