package kube

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/grepplabs/vectap/internal/targets"
)

func TestResolverResolveFiltersRunningAndReady(t *testing.T) {
	client := fake.NewClientset(
		pod("obs", "vector-ready", map[string]string{"app": "vector"}, "10.0.0.1", v1.PodRunning, true),
		pod("obs", "vector-pending", map[string]string{"app": "vector"}, "10.0.0.2", v1.PodPending, true),
		pod("obs", "vector-not-ready", map[string]string{"app": "vector"}, "10.0.0.3", v1.PodRunning, false),
	)

	resolver := NewResolver(client)
	resolved, err := resolver.Resolve(t.Context(), targets.ResolveOptions{
		Namespace:     "obs",
		LabelSelector: "app=vector",
		RemotePort:    8686,
	})
	require.NoError(t, err)
	require.Len(t, resolved, 1)

	got := resolved[0]
	require.Equal(t, "obs/vector-ready", got.ID)
	require.Equal(t, 8686, got.RemotePort)
	require.Equal(t, "10.0.0.1", got.PodIP)
	require.Equal(t, "vector", got.Labels["app"])
}

func TestResolverResolveSortsTargetsByNamespaceAndName(t *testing.T) {
	client := fake.NewClientset(
		pod("obs", "vector-b", map[string]string{"app": "vector"}, "10.0.0.2", v1.PodRunning, true),
		pod("obs", "vector-a", map[string]string{"app": "vector"}, "10.0.0.1", v1.PodRunning, true),
	)

	resolver := NewResolver(client)
	resolved, err := resolver.Resolve(t.Context(), targets.ResolveOptions{
		Namespace:     "obs",
		LabelSelector: "app=vector",
		RemotePort:    8686,
	})
	require.NoError(t, err)
	require.Len(t, resolved, 2)
	require.Equal(t, []string{"vector-a", "vector-b"}, []string{resolved[0].PodName, resolved[1].PodName})
}

func TestResolverObserveTracksPodAddUpdateAndDelete(t *testing.T) {
	client := fake.NewClientset(
		pod("obs", "vector-a", map[string]string{"app": "vector"}, "10.0.0.1", v1.PodRunning, true),
		pod("obs", "other", map[string]string{"app": "other"}, "10.0.0.2", v1.PodRunning, true),
	)

	resolver := NewResolver(client)
	snapshots, errs := resolver.Observe(t.Context(), targets.ResolveOptions{
		Namespace:     "obs",
		LabelSelector: "app=vector",
		RemotePort:    8686,
	})

	requireSnapshotPods(t, snapshots, "vector-a")

	_, err := client.CoreV1().Pods("obs").Create(t.Context(), pod("obs", "vector-b", map[string]string{"app": "vector"}, "10.0.0.3", v1.PodRunning, true), metav1.CreateOptions{})
	require.NoError(t, err)
	requireSnapshotPods(t, snapshots, "vector-a", "vector-b")

	updated := pod("obs", "vector-a", map[string]string{"app": "other"}, "10.0.0.1", v1.PodRunning, true)
	_, err = client.CoreV1().Pods("obs").Update(t.Context(), updated, metav1.UpdateOptions{})
	require.NoError(t, err)
	requireSnapshotPods(t, snapshots, "vector-b")

	err = client.CoreV1().Pods("obs").Delete(t.Context(), "vector-b", metav1.DeleteOptions{})
	require.NoError(t, err)
	requireSnapshotPods(t, snapshots)

	select {
	case err := <-errs:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestResolverObserveFiltersReadinessChanges(t *testing.T) {
	client := fake.NewClientset(
		pod("obs", "vector-a", map[string]string{"app": "vector"}, "10.0.0.1", v1.PodRunning, false),
	)

	resolver := NewResolver(client)
	snapshots, _ := resolver.Observe(t.Context(), targets.ResolveOptions{
		Namespace:     "obs",
		LabelSelector: "app=vector",
		RemotePort:    8686,
	})

	requireSnapshotPods(t, snapshots)

	ready := pod("obs", "vector-a", map[string]string{"app": "vector"}, "10.0.0.1", v1.PodRunning, true)
	_, err := client.CoreV1().Pods("obs").Update(t.Context(), ready, metav1.UpdateOptions{})
	require.NoError(t, err)
	requireSnapshotPods(t, snapshots, "vector-a")
}

func TestResolverObserveInvalidSelectorReturnsErrorAndClosesChannels(t *testing.T) {
	client := fake.NewClientset()
	resolver := NewResolver(client)

	snapshots, errs := resolver.Observe(t.Context(), targets.ResolveOptions{
		Namespace:     "obs",
		LabelSelector: "app in (",
		RemotePort:    8686,
	})

	select {
	case err, ok := <-errs:
		require.True(t, ok, "expected parse error before channel close")
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "parse label selector"))
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for parse error")
	}

	requireChannelClosedSnapshots(t, snapshots)
	requireChannelClosedErrors(t, errs)
}

func TestResolverObserveSkipsDuplicateSnapshotForUnchangedTargets(t *testing.T) {
	client := fake.NewClientset(
		pod("obs", "vector-a", map[string]string{"app": "vector"}, "10.0.0.1", v1.PodRunning, true),
	)

	resolver := NewResolver(client)
	snapshots, _ := resolver.Observe(t.Context(), targets.ResolveOptions{
		Namespace:     "obs",
		LabelSelector: "app=vector",
		RemotePort:    8686,
	})

	requireSnapshotPods(t, snapshots, "vector-a")

	// Update with effectively identical target state; Observe should not emit a duplicate snapshot.
	_, err := client.CoreV1().Pods("obs").Update(t.Context(), pod("obs", "vector-a", map[string]string{"app": "vector"}, "10.0.0.1", v1.PodRunning, true), metav1.UpdateOptions{})
	require.NoError(t, err)
	requireNoSnapshotWithin(t, snapshots, 150*time.Millisecond)

	_, err = client.CoreV1().Pods("obs").Create(t.Context(), pod("obs", "vector-b", map[string]string{"app": "vector"}, "10.0.0.2", v1.PodRunning, true), metav1.CreateOptions{})
	require.NoError(t, err)
	requireSnapshotPods(t, snapshots, "vector-a", "vector-b")
}

func TestResolverObserveClosesChannelsOnContextCancel(t *testing.T) {
	client := fake.NewClientset(
		pod("obs", "vector-a", map[string]string{"app": "vector"}, "10.0.0.1", v1.PodRunning, true),
	)

	resolver := NewResolver(client)
	ctx, cancel := context.WithCancel(context.Background())
	snapshots, errs := resolver.Observe(ctx, targets.ResolveOptions{
		Namespace:     "obs",
		LabelSelector: "app=vector",
		RemotePort:    8686,
	})

	requireSnapshotPods(t, snapshots, "vector-a")
	cancel()

	requireChannelClosedSnapshots(t, snapshots)
	requireChannelClosedErrors(t, errs)
}

func TestResolverListKnownPodsReturnsDeepCopies(t *testing.T) {
	client := fake.NewClientset(
		pod("obs", "vector-a", map[string]string{"app": "vector"}, "10.0.0.1", v1.PodRunning, true),
	)
	resolver := NewResolver(client)

	_, known, err := resolver.listKnownPods(t.Context(), "obs")
	require.NoError(t, err)
	require.Contains(t, known, "obs/vector-a")

	known["obs/vector-a"].Labels["app"] = "mutated"

	got, err := client.CoreV1().Pods("obs").Get(t.Context(), "vector-a", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "vector", got.Labels["app"])
}

func TestApplyObserveEventWatchErrorRequestsRelist(t *testing.T) {
	selector, err := labels.Parse("app=vector")
	require.NoError(t, err)

	knownPods := map[string]*v1.Pod{}
	out := make(chan []targets.Target, 1)
	errCh := make(chan error, 1)
	var last []targets.Target

	relist, stop := applyObserveEvent(
		t.Context(),
		watch.Event{Type: watch.Error, Object: &metav1.Status{Message: "boom"}},
		knownPods,
		selector,
		8686,
		out,
		errCh,
		&last,
	)
	require.True(t, relist)
	require.False(t, stop)

	select {
	case got := <-errCh:
		require.Error(t, got)
		require.Contains(t, got.Error(), "watch pods:")
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for watch error")
	}
}

func TestWatchUntilRelistOrStopReturnsRelistWhenWatchCloses(t *testing.T) {
	selector, err := labels.Parse("app=vector")
	require.NoError(t, err)

	knownPods := map[string]*v1.Pod{}
	out := make(chan []targets.Target, 1)
	errCh := make(chan error, 1)
	var last []targets.Target

	fw := watch.NewFake()
	resultCh := make(chan bool, 1)
	go func() {
		resultCh <- watchUntilRelistOrStop(t.Context(), fw, knownPods, selector, 8686, out, errCh, &last)
	}()

	fw.Add(pod("obs", "vector-a", map[string]string{"app": "vector"}, "10.0.0.1", v1.PodRunning, true))
	requireSnapshotPods(t, out, "vector-a")

	fw.Stop()

	select {
	case relist := <-resultCh:
		require.True(t, relist)
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for watch loop result")
	}
}

func requireSnapshotPods(t *testing.T, snapshots <-chan []targets.Target, want ...string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	select {
	case got, ok := <-snapshots:
		require.True(t, ok, "expected snapshot channel to remain open")
		names := make([]string, 0, len(got))
		for _, target := range got {
			names = append(names, target.PodName)
		}
		if len(want) == 0 {
			want = []string{}
		}
		require.Equal(t, want, names)
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for snapshot")
	}
}

func requireNoSnapshotWithin(t *testing.T, snapshots <-chan []targets.Target, d time.Duration) {
	t.Helper()

	select {
	case got, ok := <-snapshots:
		require.FailNowf(t, "unexpected snapshot", "got=%v open=%v", got, ok)
	case <-time.After(d):
	}
}

func requireChannelClosedSnapshots(t *testing.T, snapshots <-chan []targets.Target) {
	t.Helper()

	select {
	case _, ok := <-snapshots:
		require.False(t, ok, "expected snapshots channel to be closed")
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for snapshots channel close")
	}
}

func requireChannelClosedErrors(t *testing.T, errs <-chan error) {
	t.Helper()

	select {
	case _, ok := <-errs:
		require.False(t, ok, "expected errors channel to be closed")
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for errors channel close")
	}
}

func pod(namespace, name string, labels map[string]string, ip string, phase v1.PodPhase, ready bool) *v1.Pod {
	readyStatus := v1.ConditionFalse
	if ready {
		readyStatus = v1.ConditionTrue
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    labels,
		},
		Status: v1.PodStatus{
			Phase: phase,
			PodIP: ip,
			Conditions: []v1.PodCondition{
				{Type: v1.PodReady, Status: readyStatus},
			},
		},
	}
}
