package cli

import (
	"testing"
	"time"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/vectorapi"
	"github.com/knadh/koanf/v2"
	"github.com/stretchr/testify/require"
)

func setK(k *koanf.Koanf, key string, value any) {
	err := k.Set(key, value)
	if err != nil {
		panic(err)
	}
}

func TestTapConfigFromKoanfDefaultAPIResolution(t *testing.T) {
	v := koanf.New(".")
	setK(v, "defaults.type", runconfig.SourceTypeDirect)
	setK(v, "defaults.direct_url", runconfig.DefaultDirectURL)
	setK(v, "defaults.discovery.namespace", runconfig.DefaultNamespace)
	setK(v, "defaults.format", runconfig.FormatText)
	setK(v, "defaults.transport.vector_port", runconfig.DefaultVectorPort)
	setK(v, "defaults.transport.interval", runconfig.DefaultTapInterval)
	setK(v, "defaults.transport.limit", runconfig.DefaultTapLimit)

	cfg, err := tapConfigFromKoanf(v)
	require.NoError(t, err)
	require.Equal(t, string(runconfig.VectorDefaultAPI), cfg.API)
}

func TestTapConfigFromKoanfCLIOverridesDefaultAPI(t *testing.T) {
	v := koanf.New(".")
	setK(v, "defaults.type", runconfig.SourceTypeDirect)
	setK(v, "defaults.api", string(runconfig.VectorAPIGraphQL))
	setK(v, "defaults.direct_url", runconfig.DefaultDirectURL)
	setK(v, "defaults.discovery.namespace", runconfig.DefaultNamespace)
	setK(v, "defaults.format", runconfig.FormatText)
	setK(v, "defaults.transport.vector_port", runconfig.DefaultVectorPort)
	setK(v, "defaults.transport.interval", runconfig.DefaultTapInterval)
	setK(v, "defaults.transport.limit", runconfig.DefaultTapLimit)
	setK(v, "api", string(runconfig.VectorAPIGrpc))

	cfg, err := tapConfigFromKoanf(v)
	require.NoError(t, err)
	require.Equal(t, string(runconfig.VectorAPIGrpc), cfg.API)
}

func TestTapConfigFromKoanfPerSourceAPIOverridesTopLevel(t *testing.T) {
	v := koanf.New(".")
	setK(v, "defaults.type", runconfig.SourceTypeDirect)
	setK(v, "defaults.api", string(runconfig.VectorAPIGraphQL))
	setK(v, "defaults.direct_url", runconfig.DefaultDirectURL)
	setK(v, "defaults.discovery.namespace", runconfig.DefaultNamespace)
	setK(v, "defaults.format", runconfig.FormatText)
	setK(v, "defaults.transport.vector_port", runconfig.DefaultVectorPort)
	setK(v, "defaults.transport.interval", runconfig.DefaultTapInterval)
	setK(v, "defaults.transport.limit", runconfig.DefaultTapLimit)
	setK(v, "api", string(runconfig.VectorAPIGrpc))
	setK(v, "all-sources", true)
	setK(v, "sources", []map[string]any{
		{
			"name": "src-a",
			"type": runconfig.SourceTypeDirect,
			"api":  string(runconfig.VectorAPIGraphQL),
			"endpoint": map[string]any{
				"url": runconfig.DefaultDirectURL,
			},
		},
	})

	cfg, err := tapConfigFromKoanf(v)
	require.NoError(t, err)
	require.Equal(t, string(runconfig.VectorAPIGrpc), cfg.API)
	require.Len(t, cfg.Sources, 1)
	require.Equal(t, string(runconfig.VectorAPIGraphQL), cfg.Sources[0].API)
}

func TestTapConfigFromKoanfRejectsInvalidAPI(t *testing.T) {
	v := koanf.New(".")
	setK(v, "defaults.type", runconfig.SourceTypeDirect)
	setK(v, "defaults.direct_url", runconfig.DefaultDirectURL)
	setK(v, "defaults.discovery.namespace", runconfig.DefaultNamespace)
	setK(v, "defaults.format", runconfig.FormatText)
	setK(v, "defaults.transport.vector_port", runconfig.DefaultVectorPort)
	setK(v, "defaults.transport.interval", runconfig.DefaultTapInterval)
	setK(v, "defaults.transport.limit", runconfig.DefaultTapLimit)
	setK(v, "api", "bad-api")

	_, err := tapConfigFromKoanf(v)
	require.EqualError(t, err, `unsupported api "bad-api"`)
}

func TestTapConfigFromKoanfDefaultsFallback(t *testing.T) {
	v := koanf.New(".")
	setK(v, "defaults.type", "kubernetes")
	setK(v, "defaults.direct_url", "http://10.0.0.1:8686/graphql")
	setK(v, "defaults.discovery.namespace", "obs")
	setK(v, "defaults.discovery.selector", "app=vector")
	setK(v, "defaults.cluster.kubeconfig", "/tmp/kubeconfig")
	setK(v, "defaults.cluster.context", "prod-eu")
	setK(v, "defaults.format", "json")
	setK(v, "defaults.transport.vector_port", 9777)
	setK(v, "defaults.transport.interval", 750)
	setK(v, "defaults.transport.limit", 250)
	setK(v, "duration", "45s")
	setK(v, "defaults.include_meta", false)
	setK(v, "defaults.outputs_of", []string{"*"})
	setK(v, "defaults.inputs_of", []string{"sink.default"})
	setK(v, "defaults.local_filters", []string{"+component.kind:sink"})
	setK(v, "sources", []map[string]any{
		{"name": "eu", "type": "kubernetes"},
	})

	cfg, err := tapConfigFromKoanf(v)
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
	require.Equal(t, []string{vectorapi.EventKindLog}, cfg.EventKinds)
	require.Equal(t, []string{"sink"}, cfg.LocalFilters.ComponentKind.IncludeGlob)
}

func TestTapConfigFromKoanfCLIOverridesDefaults(t *testing.T) {
	v := koanf.New(".")
	setK(v, "defaults.type", "direct")
	setK(v, "defaults.direct_url", "http://127.0.0.1:8686")
	setK(v, "defaults.discovery.namespace", "default")
	setK(v, "defaults.format", "json")
	setK(v, "defaults.include_meta", false)
	setK(v, "defaults.transport.vector_port", 9777)
	setK(v, "defaults.transport.interval", 750)
	setK(v, "defaults.transport.limit", 250)
	setK(v, "format", "text")
	setK(v, "include-meta", true)
	setK(v, "vector-port", 8686)
	setK(v, "interval", 500)
	setK(v, "limit", 100)
	setK(v, "duration", "30s")
	setK(v, "raw-format", true)

	cfg, err := tapConfigFromKoanf(v)
	require.NoError(t, err)
	require.Equal(t, runconfig.FormatText, cfg.Format)
	require.True(t, cfg.IncludeMeta)
	require.Equal(t, 8686, cfg.VectorPort)
	require.Equal(t, 500, cfg.Interval)
	require.Equal(t, 100, cfg.Limit)
	require.Equal(t, 30*time.Second, cfg.Duration)
	require.Equal(t, []string{vectorapi.EventKindLog}, cfg.EventKinds)
	require.True(t, cfg.RawFormat)
}

func TestTapConfigFromKoanfEventKinds(t *testing.T) {
	v := koanf.New(".")
	setK(v, "defaults.type", runconfig.SourceTypeDirect)
	setK(v, "defaults.direct_url", runconfig.DefaultDirectURL)
	setK(v, "defaults.discovery.namespace", runconfig.DefaultNamespace)
	setK(v, "defaults.format", runconfig.FormatText)
	setK(v, "defaults.transport.vector_port", runconfig.DefaultVectorPort)
	setK(v, "defaults.transport.interval", runconfig.DefaultTapInterval)
	setK(v, "defaults.transport.limit", runconfig.DefaultTapLimit)
	setK(v, "event-kind", []string{"metric", "trace,log"})

	cfg, err := tapConfigFromKoanf(v)
	require.NoError(t, err)
	require.Equal(t, []string{"metric", "trace", "log"}, cfg.EventKinds)
}

func TestLoadSourceConfigsSourceOverrides(t *testing.T) {
	v := koanf.New(".")
	defs := defaultsFile{}
	defs.Type = runconfig.SourceTypeKubernetes
	defs.DirectURL = "http://127.0.0.1:8686"
	defs.Discovery.Namespace = "default"
	defs.Discovery.Selector = "app.kubernetes.io/name=vector"
	defs.Transport.VectorPort = new(8686)
	defs.Transport.Interval = new(500)
	defs.Transport.Limit = new(100)
	defs.Cluster.Context = "default-ctx"

	setK(v, "sources", []map[string]any{
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

	sources, err := loadSourceConfigs(defs, v, runconfig.FormatJSON, true, string(runconfig.VectorDefaultAPI))
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
	v := koanf.New(".")
	defs := defaultsFile{}
	defs.Type = runconfig.SourceTypeDirect
	defs.DirectURL = runconfig.DefaultDirectURL
	defs.Discovery.Namespace = runconfig.DefaultNamespace
	defs.Discovery.Selector = runconfig.DefaultSelector
	defs.Transport.VectorPort = new(runconfig.DefaultVectorPort)
	defs.Transport.Interval = new(runconfig.DefaultTapInterval)
	defs.Transport.Limit = new(runconfig.DefaultTapLimit)

	setK(v, "sources", []map[string]any{
		{
			"name":           "src-a",
			"type":           "direct",
			"apply_defaults": false,
			"outputs_of":     []string{"src-a-out"},
			"inputs_of":      []string{"src-a-in"},
			"local_filters":  []string{"+component.type:aws_*"},
		},
	})

	sources, err := loadSourceConfigs(defs, v, runconfig.FormatText, true, string(runconfig.VectorDefaultAPI))
	require.NoError(t, err)
	require.Len(t, sources, 1)
	require.False(t, sources[0].ApplyDefaults)
	require.Equal(t, []string{"src-a-out"}, sources[0].OutputsOf)
	require.Equal(t, []string{"src-a-in"}, sources[0].InputsOf)
	require.Equal(t, []string{"aws_*"}, sources[0].LocalFilters.ComponentType.IncludeGlob)
}

func TestLoadSourceConfigsInvalidSourceLocalFilter(t *testing.T) {
	v := koanf.New(".")
	defs := defaultsFile{}
	defs.Type = runconfig.SourceTypeDirect
	defs.DirectURL = runconfig.DefaultDirectURL
	defs.Discovery.Namespace = runconfig.DefaultNamespace
	defs.Discovery.Selector = runconfig.DefaultSelector
	defs.Transport.VectorPort = new(runconfig.DefaultVectorPort)
	defs.Transport.Interval = new(runconfig.DefaultTapInterval)
	defs.Transport.Limit = new(runconfig.DefaultTapLimit)

	setK(v, "sources", []map[string]any{
		{
			"name":          "src-a",
			"type":          "direct",
			"local_filters": []string{"+unknown.field:x"},
		},
	})

	_, err := loadSourceConfigs(defs, v, runconfig.FormatText, true, string(runconfig.VectorDefaultAPI))
	require.EqualError(t, err, `source "src-a": invalid local-filter "+unknown.field:x": unsupported field "unknown.field"`)
}

func TestResolveHelpersUseDefaultsWhenNoInput(t *testing.T) {
	v := koanf.New(".")

	require.Equal(t, "json", resolveString(v, "format", "json"))
	require.Equal(t, []string{"http://127.0.0.1:8686"}, resolveStringSlice(v, "direct-url", "http://127.0.0.1:8686"))
	require.Equal(t, 9999, resolveInt(v, "vector-port", new(9999)))
	require.Equal(t, 500, resolveInt(v, "interval", new(500)))
	require.Equal(t, 100, resolveInt(v, "limit", new(100)))
	require.Equal(t, 30*time.Second, resolveDuration(v, "duration", "30s"))
	require.True(t, resolveBool(v, "include-meta", new(true)))
}

func TestComponentsConfigFromKoanfDefaultsFallback(t *testing.T) {
	v := koanf.New(".")
	setK(v, "defaults.type", "kubernetes")
	setK(v, "defaults.direct_url", "http://10.0.0.1:8686/graphql")
	setK(v, "defaults.discovery.namespace", "obs")
	setK(v, "defaults.discovery.selector", "app=vector")
	setK(v, "defaults.cluster.kubeconfig", "/tmp/kubeconfig")
	setK(v, "defaults.cluster.context", "prod-eu")
	setK(v, "defaults.format", "yaml")
	setK(v, "defaults.transport.vector_port", 9777)
	setK(v, "defaults.include_meta", false)
	setK(v, "sources", []map[string]any{
		{"name": "eu", "type": "kubernetes"},
	})

	cfg, err := componentsConfigFromKoanf(v)
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
	v := koanf.New(".")
	defs := defaultsFile{}
	defs.Type = runconfig.SourceTypeKubernetes
	defs.DirectURL = "http://127.0.0.1:8686"
	defs.Discovery.Namespace = "default"
	defs.Discovery.Selector = "app.kubernetes.io/name=vector"
	defs.Transport.VectorPort = new(8686)
	defs.Cluster.Context = "default-ctx"

	setK(v, "sources", []map[string]any{
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

	sources, err := loadComponentsSourceConfigs(defs, v, runconfig.FormatYAML, true, string(runconfig.VectorDefaultAPI))
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
