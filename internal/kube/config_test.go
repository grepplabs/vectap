package kube

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/tools/clientcmd"
)

func TestLoadKubeConfigMultiPathKubeconfig(t *testing.T) {
	dir := isolate(t)
	first := writeKubeconfig(t, dir, "one.yaml", "prod", "https://prod.example:6443")
	second := writeKubeconfig(t, dir, "two.yaml", "kind-dev", "https://127.0.0.1:6443")
	t.Setenv("KUBECONFIG", first+string(filepath.ListSeparator)+second)

	cfg, err := loadKubeConfig("", "kind-dev")
	if err != nil {
		t.Fatalf("loadKubeConfig: %v", err)
	}
	if want := "https://127.0.0.1:6443"; cfg.Host != want {
		t.Errorf("host = %q, want %q", cfg.Host, want)
	}
}

func TestLoadKubeConfigExplicitPathOverridesEnv(t *testing.T) {
	dir := isolate(t)
	env := writeKubeconfig(t, dir, "env.yaml", "prod", "https://prod.example:6443")
	flag := writeKubeconfig(t, dir, "flag.yaml", "kind-dev", "https://127.0.0.1:6443")
	t.Setenv("KUBECONFIG", env)

	cfg, err := loadKubeConfig(flag, "")
	if err != nil {
		t.Fatalf("loadKubeConfig: %v", err)
	}
	if want := "https://127.0.0.1:6443"; cfg.Host != want {
		t.Errorf("host = %q, want %q", cfg.Host, want)
	}
}

func TestLoadConfigReportsKubeconfigError(t *testing.T) {
	dir := isolate(t)
	missing := filepath.Join(dir, "does-not-exist.yaml")

	_, err := LoadConfig(missing, "")
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "KUBERNETES_SERVICE_HOST") {
		t.Errorf("in-cluster error leaked to the caller: %v", err)
	}
	if !strings.Contains(err.Error(), "does-not-exist.yaml") {
		t.Errorf("kubeconfig error not surfaced: %v", err)
	}
}

func TestLoadConfigSucceedsFromHomeFallback(t *testing.T) {
	dir := isolate(t)
	kubeDir := filepath.Join(dir, ".kube")
	if err := os.MkdirAll(kubeDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeKubeconfig(t, kubeDir, "config", "kind-dev", "https://127.0.0.1:6443")
	overrideHomeFile(t, filepath.Join(kubeDir, "config"))

	cfg, err := LoadConfig("", "")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if want := "https://127.0.0.1:6443"; cfg.Host != want {
		t.Errorf("host = %q, want %q", cfg.Host, want)
	}
}

func writeKubeconfig(t *testing.T, dir, name, contextName, server string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	content := `apiVersion: v1
kind: Config
clusters:
- name: ` + contextName + `
  cluster:
    server: ` + server + `
contexts:
- name: ` + contextName + `
  context:
    cluster: ` + contextName + `
    user: ` + contextName + `
current-context: ` + contextName + `
users:
- name: ` + contextName + `
  user: {}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

// isolate keeps the real kubeconfig and any in-cluster environment out of the test.
func isolate(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	t.Setenv("KUBECONFIG", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	return dir
}

func overrideHomeFile(t *testing.T, path string) {
	t.Helper()

	previous := clientcmd.RecommendedHomeFile
	clientcmd.RecommendedHomeFile = path
	t.Cleanup(func() { clientcmd.RecommendedHomeFile = previous })
}
