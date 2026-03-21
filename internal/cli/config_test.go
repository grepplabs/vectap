package cli

import (
	"testing"
	"time"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/ptr"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestTapConfigFromViperDefaultsFallback(t *testing.T) {
	v := viper.New()
	v.Set("defaults.type", "kubernetes")
	v.Set("defaults.direct_url", "http://10.0.0.1:8686/graphql")
	v.Set("defaults.discovery.namespace", "obs")
	v.Set("defaults.discovery.selector", "app=vector")
	v.Set("defaults.cluster.kubeconfig", "/tmp/kubeconfig")
	v.Set("defaults.cluster.context", "prod-eu")
	v.Set("defaults.format", "json")
	v.Set("defaults.transport.vector_port", 9777)
	v.Set("defaults.transport.interval", 750)
	v.Set("defaults.transport.limit", 250)
	v.Set("duration", "45s")
	v.Set("defaults.include_meta", false)
	v.Set("defaults.outputs_of", []string{"*"})
	v.Set("defaults.inputs_of", []string{"sink.default"})
	v.Set("defaults.local_filters", []string{"+component.kind:sink"})
	v.Set("sources", []map[string]any{
		{"name": "eu", "type": "kubernetes"},
	})

	cfg, err := tapConfigFromViper(v, nil)
	require.NoError(t, err)
	require.Equal(t, runconfig.SourceTypeKubernetes, cfg.Type)
	require.Equal(t, []string{"http://10.0.0.1:8686/graphql"}, cfg.DirectURLs)
	require.Equal(t, "obs", cfg.Namespace)
	require.Equal(t, "app=vector", cfg.LabelSelector)
	require.Equal(t, "/tmp/kubeconfig", cfg.KubeConfigPath)
	require.Equal(t, "prod-eu", cfg.KubeContext)
	require.Equal(t, runconfig.FormatJSON, cfg.Format)
	require.Equal(t, 9777, cfg.VectorPort)
	require.Equal(t, 750, cfg.Interval)
	require.Equal(t, 250, cfg.Limit)
	require.Equal(t, 45*time.Second, cfg.Duration)
	require.False(t, cfg.IncludeMeta)
	require.Equal(t, []string{"*"}, cfg.OutputsOf)
	require.Equal(t, []string{"sink.default"}, cfg.InputsOf)
	require.Equal(t, []string{"sink"}, cfg.LocalFilters.ComponentKind.IncludeGlob)
}

func TestTapConfigFromViperCLIOverridesDefaults(t *testing.T) {
	v := viper.New()
	v.Set("defaults.type", "direct")
	v.Set("defaults.direct_url", "http://127.0.0.1:8686/graphql")
	v.Set("defaults.discovery.namespace", "default")
	v.Set("defaults.format", "json")
	v.Set("defaults.include_meta", false)
	v.Set("defaults.transport.vector_port", 9777)
	v.Set("defaults.transport.interval", 750)
	v.Set("defaults.transport.limit", 250)
	v.Set("format", "text")
	v.Set("include-meta", true)
	v.Set("vector-port", 8686)
	v.Set("interval", 500)
	v.Set("limit", 100)
	v.Set("duration", "30s")

	flagSet := func(name string) bool {
		switch name {
		case "format", "include-meta", "vector-port", "interval", "limit", "duration":
			return true
		default:
			return false
		}
	}

	cfg, err := tapConfigFromViper(v, flagSet)
	require.NoError(t, err)
	require.Equal(t, runconfig.FormatText, cfg.Format)
	require.True(t, cfg.IncludeMeta)
	require.Equal(t, 8686, cfg.VectorPort)
	require.Equal(t, 500, cfg.Interval)
	require.Equal(t, 100, cfg.Limit)
	require.Equal(t, 30*time.Second, cfg.Duration)
}

func TestLoadSourceConfigsSourceOverrides(t *testing.T) {
	v := viper.New()
	defs := defaultsFile{}
	defs.Type = runconfig.SourceTypeKubernetes
	defs.DirectURL = "http://127.0.0.1:8686/graphql"
	defs.Discovery.Namespace = "default"
	defs.Discovery.Selector = "app.kubernetes.io/name=vector"
	defs.Transport.VectorPort = ptr.To(8686)
	defs.Transport.Interval = ptr.To(500)
	defs.Transport.Limit = ptr.To(100)
	defs.Cluster.Context = "default-ctx"

	v.Set("sources", []map[string]any{
		{
			"name":         "kube-a",
			"type":         "kubernetes",
			"format":       "text",
			"include_meta": false,
			"outputs_of":   []string{"kube-only-*"},
			"inputs_of":    []string{"sink-a"},
			"local_filters": []string{
				"+component.kind:sink",
				"-component.kind:source",
			},
			"transport": map[string]any{
				"vector_port": 9777,
				"interval":    750,
				"limit":       250,
			},
		},
		{
			"name": "direct-a",
			"type": "direct",
			"endpoint": map[string]any{
				"url": "http://10.0.0.2:8686/graphql",
			},
		},
	})

	sources, err := loadSourceConfigs(defs, v, runconfig.FormatJSON, true)
	require.NoError(t, err)
	require.Len(t, sources, 2)

	require.Equal(t, "kube-a", sources[0].Name)
	require.Equal(t, runconfig.FormatText, sources[0].Format)
	require.False(t, sources[0].IncludeMeta)
	require.Equal(t, 9777, sources[0].VectorPort)
	require.Equal(t, 750, sources[0].Interval)
	require.Equal(t, 250, sources[0].Limit)
	require.Equal(t, []string{"kube-only-*"}, sources[0].OutputsOf)
	require.Equal(t, []string{"sink-a"}, sources[0].InputsOf)
	require.Equal(t, []string{"sink"}, sources[0].LocalFilters.ComponentKind.IncludeGlob)
	require.Equal(t, []string{"source"}, sources[0].LocalFilters.ComponentKind.ExcludeGlob)
	require.True(t, sources[0].ApplyDefaults)

	require.Equal(t, "direct-a", sources[1].Name)
	require.Equal(t, runconfig.FormatJSON, sources[1].Format)
	require.True(t, sources[1].IncludeMeta)
	require.Equal(t, []string{"http://10.0.0.2:8686/graphql"}, sources[1].DirectURLs)
	require.Equal(t, 500, sources[1].Interval)
	require.Equal(t, 100, sources[1].Limit)
	require.True(t, sources[1].ApplyDefaults)
}

func TestLoadSourceConfigsPerSourceFieldsAndApplyDefaultsToggle(t *testing.T) {
	v := viper.New()
	defs := defaultsFile{}
	defs.Type = runconfig.SourceTypeDirect
	defs.DirectURL = runconfig.DefaultDirectURL
	defs.Discovery.Namespace = runconfig.DefaultNamespace
	defs.Discovery.Selector = runconfig.DefaultSelector
	defs.Transport.VectorPort = ptr.To(runconfig.DefaultVectorPort)
	defs.Transport.Interval = ptr.To(runconfig.DefaultTapInterval)
	defs.Transport.Limit = ptr.To(runconfig.DefaultTapLimit)

	v.Set("sources", []map[string]any{
		{
			"name":           "src-a",
			"type":           "direct",
			"apply_defaults": false,
			"outputs_of":     []string{"src-a-out"},
			"inputs_of":      []string{"src-a-in"},
			"local_filters":  []string{"+component.type:aws_*"},
		},
	})

	sources, err := loadSourceConfigs(defs, v, runconfig.FormatText, true)
	require.NoError(t, err)
	require.Len(t, sources, 1)
	require.False(t, sources[0].ApplyDefaults)
	require.Equal(t, []string{"src-a-out"}, sources[0].OutputsOf)
	require.Equal(t, []string{"src-a-in"}, sources[0].InputsOf)
	require.Equal(t, []string{"aws_*"}, sources[0].LocalFilters.ComponentType.IncludeGlob)
}

func TestLoadSourceConfigsInvalidSourceLocalFilter(t *testing.T) {
	v := viper.New()
	defs := defaultsFile{}
	defs.Type = runconfig.SourceTypeDirect
	defs.DirectURL = runconfig.DefaultDirectURL
	defs.Discovery.Namespace = runconfig.DefaultNamespace
	defs.Discovery.Selector = runconfig.DefaultSelector
	defs.Transport.VectorPort = ptr.To(runconfig.DefaultVectorPort)
	defs.Transport.Interval = ptr.To(runconfig.DefaultTapInterval)
	defs.Transport.Limit = ptr.To(runconfig.DefaultTapLimit)

	v.Set("sources", []map[string]any{
		{
			"name":          "src-a",
			"type":          "direct",
			"local_filters": []string{"+unknown.field:x"},
		},
	})

	_, err := loadSourceConfigs(defs, v, runconfig.FormatText, true)
	require.EqualError(t, err, `source "src-a": invalid local-filter "+unknown.field:x": unsupported field "unknown.field"`)
}

func TestResolveHelpersUseDefaultsWhenNoCLIConfigEnv(t *testing.T) {
	v := viper.New()
	none := func(string) bool { return false }

	require.Equal(t, "json", resolveString(v, none, "format", "json"))
	require.Equal(t, []string{"http://127.0.0.1:8686/graphql"}, resolveStringSlice(v, none, "direct-url", "http://127.0.0.1:8686/graphql"))
	require.Equal(t, 9999, resolveInt(v, none, "vector-port", ptr.To(9999)))
	require.Equal(t, 500, resolveInt(v, none, "interval", ptr.To(500)))
	require.Equal(t, 100, resolveInt(v, none, "limit", ptr.To(100)))
	require.Equal(t, 30*time.Second, resolveDuration(v, none, "duration", "30s"))
	require.True(t, resolveBool(v, none, "include-meta", ptr.To(true)))
}

func TestComponentsConfigFromViperDefaultsFallback(t *testing.T) {
	v := viper.New()
	v.Set("defaults.type", "kubernetes")
	v.Set("defaults.direct_url", "http://10.0.0.1:8686/graphql")
	v.Set("defaults.discovery.namespace", "obs")
	v.Set("defaults.discovery.selector", "app=vector")
	v.Set("defaults.cluster.kubeconfig", "/tmp/kubeconfig")
	v.Set("defaults.cluster.context", "prod-eu")
	v.Set("defaults.format", "yaml")
	v.Set("defaults.transport.vector_port", 9777)
	v.Set("defaults.include_meta", false)
	v.Set("sources", []map[string]any{
		{"name": "eu", "type": "kubernetes"},
	})

	cfg, err := componentsConfigFromViper(v, nil)
	require.NoError(t, err)
	require.Equal(t, runconfig.SourceTypeKubernetes, cfg.Type)
	require.Equal(t, []string{"http://10.0.0.1:8686/graphql"}, cfg.DirectURLs)
	require.Equal(t, "obs", cfg.Namespace)
	require.Equal(t, "app=vector", cfg.LabelSelector)
	require.Equal(t, "/tmp/kubeconfig", cfg.KubeConfigPath)
	require.Equal(t, "prod-eu", cfg.KubeContext)
	require.Equal(t, runconfig.FormatYAML, cfg.Format)
	require.Equal(t, 9777, cfg.VectorPort)
	require.False(t, cfg.IncludeMeta)
}

func TestLoadComponentsSourceConfigsSourceOverrides(t *testing.T) {
	v := viper.New()
	defs := defaultsFile{}
	defs.Type = runconfig.SourceTypeKubernetes
	defs.DirectURL = "http://127.0.0.1:8686/graphql"
	defs.Discovery.Namespace = "default"
	defs.Discovery.Selector = "app.kubernetes.io/name=vector"
	defs.Transport.VectorPort = ptr.To(8686)
	defs.Cluster.Context = "default-ctx"

	v.Set("sources", []map[string]any{
		{
			"name":         "kube-a",
			"type":         "kubernetes",
			"format":       "text",
			"include_meta": false,
			"transport": map[string]any{
				"vector_port": 9777,
			},
		},
		{
			"name": "direct-a",
			"type": "direct",
			"endpoint": map[string]any{
				"url": "http://10.0.0.2:8686/graphql",
			},
		},
	})

	sources, err := loadComponentsSourceConfigs(defs, v, runconfig.FormatYAML, true)
	require.NoError(t, err)
	require.Len(t, sources, 2)

	require.Equal(t, "kube-a", sources[0].Name)
	require.Equal(t, runconfig.FormatText, sources[0].Format)
	require.False(t, sources[0].IncludeMeta)
	require.Equal(t, 9777, sources[0].VectorPort)

	require.Equal(t, "direct-a", sources[1].Name)
	require.Equal(t, runconfig.FormatYAML, sources[1].Format)
	require.True(t, sources[1].IncludeMeta)
	require.Equal(t, []string{"http://10.0.0.2:8686/graphql"}, sources[1].DirectURLs)
}
