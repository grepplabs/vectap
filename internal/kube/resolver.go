package kube

import (
	"context"
	"fmt"
	"sort"

	"github.com/grepplabs/vectap/internal/targets"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Resolver struct {
	client kubernetes.Interface
}

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
	pods, err := r.client.CoreV1().Pods(opts.Namespace).List(ctx, metav1.ListOptions{LabelSelector: opts.LabelSelector})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	out := make([]targets.Target, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Status.Phase != v1.PodRunning {
			continue
		}
		if !isReady(pod.Status.Conditions) {
			continue
		}

		out = append(out, targets.Target{
			ID:         pod.Namespace + "/" + pod.Name,
			Namespace:  pod.Namespace,
			PodName:    pod.Name,
			PodIP:      pod.Status.PodIP,
			RemotePort: opts.RemotePort,
			Labels:     copyLabels(pod.Labels),
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].PodName < out[j].PodName
	})

	return out, nil
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
