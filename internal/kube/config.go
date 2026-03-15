package kube

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
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
	if kubeConfigPath == "" {
		kubeConfigPath = os.Getenv("KUBECONFIG")
	}
	if kubeConfigPath == "" {
		home := homedir.HomeDir()
		if home == "" {
			return nil, errors.New("home directory not found")
		}
		kubeConfigPath = filepath.Join(home, ".kube", "config")
	}

	loader := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeConfigPath}
	overrides := &clientcmd.ConfigOverrides{}
	if kubeContext != "" {
		overrides.CurrentContext = kubeContext
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
	if err != nil {
		return nil, err
	}

	return cfg, nil
}
