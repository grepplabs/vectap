package kube

import (
	"fmt"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func LoadConfig(kubeConfigPath, kubeContext string) (*rest.Config, error) {
	if cfg, err := loadKubeConfig(kubeConfigPath, kubeContext); err == nil {
		return cfg, nil
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubernetes config: %w", err)
	}
	return cfg, nil
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
