package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

const realConfigYAML = `
defaults:
  type: kubernetes
  cluster:
    kubeconfig: ""
    context: ""
  discovery:
    namespace: default
    selector: app.kubernetes.io/name=vector
  transport:
    vector_port: 8686
    interval: 500
    limit: 100
  include_meta: true
  format: text

sources:
  - name: local
    type: direct
    enabled: true
    apply_defaults: false
    endpoint:
      url: http://127.0.0.1:8686/graphql
    outputs_of:
      - source.my_logs
    local_filters:
      - +component.kind:sink
  - name: eu-prod
    type: kubernetes
    enabled: true
    cluster:
      kubeconfig: /tmp/kubeconfig
      context: prod-eu
    discovery:
      namespace: telemetry-routing-engine
      selector: app=vector
    transport:
      vector_port: 8687
      interval: 750
      limit: 250
`

func newKoanfTestCommand(t *testing.T, configPath, namespace string) *cobra.Command {
	t.Helper()

	cmd := &cobra.Command{Use: "tap"}
	cmd.Flags().String("config", "", "")
	cmd.Flags().String("namespace", "", "")
	err := cmd.Flags().Set("config", configPath)
	require.NoError(t, err)
	if namespace != "" {
		err = cmd.Flags().Set("namespace", namespace)
		require.NoError(t, err)
	}

	return cmd
}

func TestLoadKoanfLoadsYAMLConfigFromTempFile(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "vectap.yaml")
	err := os.WriteFile(configPath, []byte("namespace: from-config\nformat: yaml\n"), 0o600)
	require.NoError(t, err)

	cmd := newKoanfTestCommand(t, configPath, "")
	k, err := loadKoanf(cmd)
	require.NoError(t, err)
	require.Equal(t, "from-config", k.String("namespace"))
	require.Equal(t, "yaml", k.String("format"))
}

func TestLoadKoanfPrecedenceFileEnvFlag(t *testing.T) {
	t.Setenv("VECTAP_NAMESPACE", "from-env")

	configPath := filepath.Join(t.TempDir(), "vectap.yaml")
	err := os.WriteFile(configPath, []byte("namespace: from-config\n"), 0o600)
	require.NoError(t, err)

	cmd := newKoanfTestCommand(t, configPath, "from-flag")
	k, err := loadKoanf(cmd)
	require.NoError(t, err)
	require.Equal(t, "from-flag", k.String("namespace"))
}

func TestLoadKoanfLoadsHardcodedRealConfigFixture(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "vectap.yaml")
	err := os.WriteFile(configPath, []byte(realConfigYAML), 0o600)
	require.NoError(t, err)

	cmd := newKoanfTestCommand(t, configPath, "")
	k, err := loadKoanf(cmd)
	require.NoError(t, err)

	require.Equal(t, "kubernetes", k.String("defaults.type"))
	require.Equal(t, "", k.String("defaults.cluster.kubeconfig"))
	require.Equal(t, "", k.String("defaults.cluster.context"))
	require.Equal(t, "default", k.String("defaults.discovery.namespace"))
	require.Equal(t, "app.kubernetes.io/name=vector", k.String("defaults.discovery.selector"))
	require.Equal(t, 8686, k.Int("defaults.transport.vector_port"))
	require.Equal(t, 500, k.Int("defaults.transport.interval"))
	require.Equal(t, 100, k.Int("defaults.transport.limit"))
	require.True(t, k.Bool("defaults.include_meta"))
	require.Equal(t, "text", k.String("defaults.format"))

	var sources []sourceFile
	err = k.Unmarshal("sources", &sources)
	require.NoError(t, err)
	require.Len(t, sources, 2)
	require.Equal(t, "local", sources[0].Name)
	require.Equal(t, "direct", sources[0].Type)
	require.NotNil(t, sources[0].Enabled)
	require.True(t, *sources[0].Enabled)
	require.NotNil(t, sources[0].ApplyDefaults)
	require.False(t, *sources[0].ApplyDefaults)
	require.Equal(t, "http://127.0.0.1:8686/graphql", sources[0].Endpoint.URL)
	require.Equal(t, []string{"source.my_logs"}, sources[0].OutputsOf)
	require.Equal(t, []string{"+component.kind:sink"}, sources[0].LocalFilter)
	require.Equal(t, "eu-prod", sources[1].Name)
	require.Equal(t, "kubernetes", sources[1].Type)
	require.NotNil(t, sources[1].Enabled)
	require.True(t, *sources[1].Enabled)
	require.Equal(t, "/tmp/kubeconfig", sources[1].Cluster.KubeConfig)
	require.Equal(t, "prod-eu", sources[1].Cluster.Context)
	require.Equal(t, "telemetry-routing-engine", sources[1].Discovery.Namespace)
	require.Equal(t, "app=vector", sources[1].Discovery.Selector)
	require.NotNil(t, sources[1].Transport.VectorPort)
	require.Equal(t, 8687, *sources[1].Transport.VectorPort)
	require.NotNil(t, sources[1].Transport.Interval)
	require.Equal(t, 750, *sources[1].Transport.Interval)
	require.NotNil(t, sources[1].Transport.Limit)
	require.Equal(t, 250, *sources[1].Transport.Limit)
}
