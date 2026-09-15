package kube

import (
	"errors"
	"fmt"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func LoadConfig(kubeConfigPath, kubeContext string) (*rest.Config, error) {
	cfg, kubeErr := loadKubeConfig(kubeConfigPath, kubeContext)
	if kubeErr == nil {
		return cfg, nil
	}

	cfg, inClusterErr := rest.InClusterConfig()
	if inClusterErr == nil {
		return cfg, nil
	}

	if errors.Is(inClusterErr, rest.ErrNotInCluster) {
		return nil, fmt.Errorf("load kubeconfig: %w", kubeErr)
	}

	return nil, fmt.Errorf("load kubernetes config: %w", errors.Join(kubeErr, inClusterErr))
}

func loadKubeConfig(kubeConfigPath, kubeContext string) (*rest.Config, error) {
	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeConfigPath != "" {
		loader.ExplicitPath = kubeConfigPath
	}

	overrides := &clientcmd.ConfigOverrides{}
	if kubeContext != "" {
		overrides.CurrentContext = kubeContext
	}

	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
}
