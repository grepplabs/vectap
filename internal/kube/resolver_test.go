package kube

import (
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
