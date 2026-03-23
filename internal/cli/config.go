package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	components "github.com/grepplabs/vectap/internal/app/components"
	"github.com/grepplabs/vectap/internal/app/runconfig"
	tap "github.com/grepplabs/vectap/internal/app/tap"
	topology "github.com/grepplabs/vectap/internal/app/topology"
	"github.com/grepplabs/vectap/internal/ptr"
	"github.com/spf13/viper"
)

func tapConfigFromViper(v *viper.Viper, cliFlagSet cliFlagSetFunc) (tap.Config, error) {
	if cliFlagSet == nil {
		cliFlagSet = func(string) bool { return false }
	}

	defs, err := loadDefaults(v)
	if err != nil {
		return tap.Config{}, err
	}

	topFormat := resolveString(v, cliFlagSet, "format", defs.Format)
	topIncludeMeta := resolveBool(v, cliFlagSet, "include-meta", defs.IncludeMeta)

	sources, err := loadSourceConfigs(defs, v, topFormat, topIncludeMeta)
	if err != nil {
		return tap.Config{}, err
	}

	cfg := tap.Config{
		BaseConfig: runconfig.BaseConfig{
			Type:            resolveString(v, cliFlagSet, "type", defs.Type),
			DirectURLs:      resolveStringSlice(v, cliFlagSet, "direct-url", defs.DirectURL),
			SelectedSources: getList(v, "source"),
			AllSources:      v.GetBool("all-sources"),
			Namespace:       resolveString(v, cliFlagSet, "namespace", defs.Discovery.Namespace),
			LabelSelector:   resolveString(v, cliFlagSet, "selector", defs.Discovery.Selector),
			KubeConfigPath:  resolveString(v, cliFlagSet, "kubeconfig", defs.Cluster.KubeConfig),
			KubeContext:     resolveString(v, cliFlagSet, "context", defs.Cluster.Context),
			Format:          topFormat,
			VectorPort:      resolveInt(v, cliFlagSet, "vector-port", defs.Transport.VectorPort),
			IncludeMeta:     topIncludeMeta,
		},
		Sources: sources,
		TapScopeConfig: tap.TapScopeConfig{
			OutputsOf: resolveStringSliceList(v, cliFlagSet, "outputs-of", defs.OutputsOf),
			InputsOf:  resolveStringSliceList(v, cliFlagSet, "inputs-of", defs.InputsOf),
			Interval:  resolveInt(v, cliFlagSet, "interval", defs.Transport.Interval),
			Limit:     resolveInt(v, cliFlagSet, "limit", defs.Transport.Limit),
			Duration:  resolveDuration(v, cliFlagSet, "duration", ""),
		},
		LocalFilters: tap.LocalFilters{},
		NoColor:      v.GetBool("no-color"),
	}
	localFilters, err := parseLocalFilters(resolveStringSliceList(v, cliFlagSet, "local-filter", defs.LocalFilters))
	if err != nil {
		return tap.Config{}, err
	}
	cfg.LocalFilters = localFilters
	return cfg, cfg.Validate()
}

func componentsConfigFromViper(v *viper.Viper, cliFlagSet cliFlagSetFunc) (components.Config, error) {
	if cliFlagSet == nil {
		cliFlagSet = func(string) bool { return false }
	}

	defs, err := loadDefaults(v)
	if err != nil {
		return components.Config{}, err
	}

	topFormat := resolveString(v, cliFlagSet, "format", defs.Format)
	topIncludeMeta := resolveBool(v, cliFlagSet, "include-meta", defs.IncludeMeta)
	sources, err := loadComponentsSourceConfigs(defs, v, topFormat, topIncludeMeta)
	if err != nil {
		return components.Config{}, err
	}

	cfg := components.Config{
		BaseConfig: runconfig.BaseConfig{
			Type:            resolveString(v, cliFlagSet, "type", defs.Type),
			DirectURLs:      resolveStringSlice(v, cliFlagSet, "direct-url", defs.DirectURL),
			SelectedSources: getList(v, "source"),
			AllSources:      v.GetBool("all-sources"),
			Namespace:       resolveString(v, cliFlagSet, "namespace", defs.Discovery.Namespace),
			LabelSelector:   resolveString(v, cliFlagSet, "selector", defs.Discovery.Selector),
			KubeConfigPath:  resolveString(v, cliFlagSet, "kubeconfig", defs.Cluster.KubeConfig),
			KubeContext:     resolveString(v, cliFlagSet, "context", defs.Cluster.Context),
			Format:          topFormat,
			VectorPort:      resolveInt(v, cliFlagSet, "vector-port", defs.Transport.VectorPort),
			IncludeMeta:     topIncludeMeta,
		},
		Sources: sources,
	}
	return cfg, cfg.Validate()
}

func topologyConfigFromViper(v *viper.Viper, cliFlagSet cliFlagSetFunc) (topology.Config, error) {
	if cliFlagSet == nil {
		cliFlagSet = func(string) bool { return false }
	}

	defs, err := loadDefaults(v)
	if err != nil {
		return topology.Config{}, err
	}

	topFormat := resolveString(v, cliFlagSet, "format", defs.Format)
	topIncludeMeta := resolveBool(v, cliFlagSet, "include-meta", defs.IncludeMeta)
	sources, err := loadTopologySourceConfigs(defs, v, topFormat, topIncludeMeta)
	if err != nil {
		return topology.Config{}, err
	}

	cfg := topology.Config{
		BaseConfig: runconfig.BaseConfig{
			Type:            resolveString(v, cliFlagSet, "type", defs.Type),
			DirectURLs:      resolveStringSlice(v, cliFlagSet, "direct-url", defs.DirectURL),
			SelectedSources: getList(v, "source"),
			AllSources:      v.GetBool("all-sources"),
			Namespace:       resolveString(v, cliFlagSet, "namespace", defs.Discovery.Namespace),
			LabelSelector:   resolveString(v, cliFlagSet, "selector", defs.Discovery.Selector),
			KubeConfigPath:  resolveString(v, cliFlagSet, "kubeconfig", defs.Cluster.KubeConfig),
			KubeContext:     resolveString(v, cliFlagSet, "context", defs.Cluster.Context),
			Format:          topFormat,
			VectorPort:      resolveInt(v, cliFlagSet, "vector-port", defs.Transport.VectorPort),
			IncludeMeta:     topIncludeMeta,
		},
		View:     resolveString(v, cliFlagSet, "view", topology.ViewTable),
		Orphaned: resolveBool(v, cliFlagSet, "orphaned", ptr.To(false)),
		Sources:  sources,
	}
	return cfg, cfg.Validate()
}

type defaultsFile struct {
	Type         string   `mapstructure:"type"`
	DirectURL    string   `mapstructure:"direct_url"`
	Format       string   `mapstructure:"format"`
	OutputsOf    []string `mapstructure:"outputs_of"`
	InputsOf     []string `mapstructure:"inputs_of"`
	LocalFilters []string `mapstructure:"local_filters"`
	Cluster      struct {
		KubeConfig string `mapstructure:"kubeconfig"`
		Context    string `mapstructure:"context"`
	} `mapstructure:"cluster"`
	Discovery struct {
		Namespace string `mapstructure:"namespace"`
		Selector  string `mapstructure:"selector"`
	} `mapstructure:"discovery"`
	Transport struct {
		VectorPort *int `mapstructure:"vector_port"`
		Interval   *int `mapstructure:"interval"`
		Limit      *int `mapstructure:"limit"`
	} `mapstructure:"transport"`
	IncludeMeta *bool `mapstructure:"include_meta"`
}

type sourceFile struct {
	Name          string   `mapstructure:"name"`
	Type          string   `mapstructure:"type"`
	Enabled       *bool    `mapstructure:"enabled"`
	Format        string   `mapstructure:"format"`
	IncludeMeta   *bool    `mapstructure:"include_meta"`
	OutputsOf     []string `mapstructure:"outputs_of"`
	InputsOf      []string `mapstructure:"inputs_of"`
	LocalFilter   []string `mapstructure:"local_filters"`
	ApplyDefaults *bool    `mapstructure:"apply_defaults"`
	Cluster       struct {
		KubeConfig string `mapstructure:"kubeconfig"`
		Context    string `mapstructure:"context"`
	} `mapstructure:"cluster"`
	Discovery struct {
		Namespace string `mapstructure:"namespace"`
		Selector  string `mapstructure:"selector"`
	} `mapstructure:"discovery"`
	Transport struct {
		VectorPort *int `mapstructure:"vector_port"`
		Interval   *int `mapstructure:"interval"`
		Limit      *int `mapstructure:"limit"`
	} `mapstructure:"transport"`
	Endpoint struct {
		URL string `mapstructure:"url"`
	} `mapstructure:"endpoint"`
}

func loadDefaults(v *viper.Viper) (defaultsFile, error) {
	var defs defaultsFile
	if err := v.UnmarshalKey("defaults", &defs); err != nil {
		return defaultsFile{}, fmt.Errorf("decode defaults: %w", err)
	}
	return defs, nil
}

//nolint:funlen
func loadSourceConfigs(defs defaultsFile, v *viper.Viper, defaultFormat string, sourceDefaultIncludeMeta bool) ([]tap.SourceConfig, error) {
	defaultType := ptr.Default(defs.Type, runconfig.SourceTypeDirect)
	fallbackDirectURL := ptr.Default(defs.DirectURL, runconfig.DefaultDirectURL)
	defaultFormat = ptr.Default(defaultFormat, runconfig.FormatText)
	fallbackNamespace := ptr.Default(defs.Discovery.Namespace, runconfig.DefaultNamespace)
	fallbackSelector := ptr.Default(defs.Discovery.Selector, runconfig.DefaultSelector)
	fallbackVectorPort := ptr.Default(ptr.Deref(defs.Transport.VectorPort, 0), runconfig.DefaultVectorPort)
	fallbackInterval := ptr.Default(ptr.Deref(defs.Transport.Interval, 0), runconfig.DefaultTapInterval)
	fallbackLimit := ptr.Default(ptr.Deref(defs.Transport.Limit, 0), runconfig.DefaultTapLimit)

	var srcFiles []sourceFile
	if err := v.UnmarshalKey("sources", &srcFiles); err != nil {
		return nil, fmt.Errorf("decode sources: %w", err)
	}
	out := make([]tap.SourceConfig, 0, len(srcFiles))
	for _, s := range srcFiles {
		sourceType := ptr.Default(s.Type, defaultType)
		enabled := ptr.Deref(s.Enabled, true)
		format := ptr.Default(s.Format, defaultFormat)
		includeMeta := ptr.Deref(s.IncludeMeta, sourceDefaultIncludeMeta)
		outputsOf := splitCSVSlice(append([]string{}, s.OutputsOf...))
		inputsOf := splitCSVSlice(append([]string{}, s.InputsOf...))
		localFilterRules := splitCSVSlice(append([]string{}, s.LocalFilter...))
		localFilters, err := parseLocalFilters(localFilterRules)
		if err != nil {
			return nil, fmt.Errorf("source %q: %w", s.Name, err)
		}
		applyDefaults := ptr.Deref(s.ApplyDefaults, true)
		namespace := ptr.Default(s.Discovery.Namespace, fallbackNamespace)
		selector := ptr.Default(s.Discovery.Selector, fallbackSelector)
		vectorPort := ptr.Default(ptr.Deref(s.Transport.VectorPort, 0), fallbackVectorPort)
		interval := ptr.Default(ptr.Deref(s.Transport.Interval, 0), fallbackInterval)
		limit := ptr.Default(ptr.Deref(s.Transport.Limit, 0), fallbackLimit)
		kubeconfig := ptr.Default(s.Cluster.KubeConfig, defs.Cluster.KubeConfig)
		kubeContext := ptr.Default(s.Cluster.Context, defs.Cluster.Context)

		var directURLs []string
		switch sourceType {
		case runconfig.SourceTypeDirect:
			url := ptr.Default(s.Endpoint.URL, fallbackDirectURL)
			directURLs = []string{url}
		case runconfig.SourceTypeKubernetes:
			directURLs = nil
		default:
			// Validation will report unsupported type.
		}

		out = append(out, tap.SourceConfig{
			BaseSourceConfig: runconfig.BaseSourceConfig{
				Name:           s.Name,
				Type:           sourceType,
				Enabled:        enabled,
				Format:         format,
				DirectURLs:     directURLs,
				Namespace:      namespace,
				LabelSelector:  selector,
				KubeConfigPath: kubeconfig,
				KubeContext:    kubeContext,
				VectorPort:     vectorPort,
				IncludeMeta:    includeMeta,
			},
			TapScopeConfig: tap.TapScopeConfig{
				OutputsOf: outputsOf,
				InputsOf:  inputsOf,
				Interval:  interval,
				Limit:     limit,
			},
			LocalFilters:  localFilters,
			ApplyDefaults: applyDefaults,
		})
	}
	return out, nil
}

func loadComponentsSourceConfigs(defs defaultsFile, v *viper.Viper, defaultFormat string, sourceDefaultIncludeMeta bool) ([]components.SourceConfig, error) {
	defaultType := ptr.Default(defs.Type, runconfig.SourceTypeDirect)
	fallbackDirectURL := ptr.Default(defs.DirectURL, runconfig.DefaultDirectURL)
	defaultFormat = ptr.Default(defaultFormat, runconfig.FormatText)
	fallbackNamespace := ptr.Default(defs.Discovery.Namespace, runconfig.DefaultNamespace)
	fallbackSelector := ptr.Default(defs.Discovery.Selector, runconfig.DefaultSelector)
	fallbackVectorPort := ptr.Default(ptr.Deref(defs.Transport.VectorPort, 0), runconfig.DefaultVectorPort)

	var srcFiles []sourceFile
	if err := v.UnmarshalKey("sources", &srcFiles); err != nil {
		return nil, fmt.Errorf("decode sources: %w", err)
	}
	out := make([]components.SourceConfig, 0, len(srcFiles))
	for _, s := range srcFiles {
		sourceType := ptr.Default(s.Type, defaultType)
		enabled := ptr.Deref(s.Enabled, true)
		format := ptr.Default(s.Format, defaultFormat)
		includeMeta := ptr.Deref(s.IncludeMeta, sourceDefaultIncludeMeta)
		namespace := ptr.Default(s.Discovery.Namespace, fallbackNamespace)
		selector := ptr.Default(s.Discovery.Selector, fallbackSelector)
		vectorPort := ptr.Default(ptr.Deref(s.Transport.VectorPort, 0), fallbackVectorPort)
		kubeconfig := ptr.Default(s.Cluster.KubeConfig, defs.Cluster.KubeConfig)
		kubeContext := ptr.Default(s.Cluster.Context, defs.Cluster.Context)

		var directURLs []string
		switch sourceType {
		case runconfig.SourceTypeDirect:
			url := ptr.Default(s.Endpoint.URL, fallbackDirectURL)
			directURLs = []string{url}
		case runconfig.SourceTypeKubernetes:
			directURLs = nil
		}

		out = append(out, components.SourceConfig{
			BaseSourceConfig: runconfig.BaseSourceConfig{
				Name:           s.Name,
				Type:           sourceType,
				Enabled:        enabled,
				Format:         format,
				DirectURLs:     directURLs,
				Namespace:      namespace,
				LabelSelector:  selector,
				KubeConfigPath: kubeconfig,
				KubeContext:    kubeContext,
				VectorPort:     vectorPort,
				IncludeMeta:    includeMeta,
			},
		})
	}
	return out, nil
}

func loadTopologySourceConfigs(defs defaultsFile, v *viper.Viper, defaultFormat string, sourceDefaultIncludeMeta bool) ([]topology.SourceConfig, error) {
	defaultType := ptr.Default(defs.Type, runconfig.SourceTypeDirect)
	fallbackDirectURL := ptr.Default(defs.DirectURL, runconfig.DefaultDirectURL)
	defaultFormat = ptr.Default(defaultFormat, runconfig.FormatText)
	fallbackNamespace := ptr.Default(defs.Discovery.Namespace, runconfig.DefaultNamespace)
	fallbackSelector := ptr.Default(defs.Discovery.Selector, runconfig.DefaultSelector)
	fallbackVectorPort := ptr.Default(ptr.Deref(defs.Transport.VectorPort, 0), runconfig.DefaultVectorPort)

	var srcFiles []sourceFile
	if err := v.UnmarshalKey("sources", &srcFiles); err != nil {
		return nil, fmt.Errorf("decode sources: %w", err)
	}
	out := make([]topology.SourceConfig, 0, len(srcFiles))
	for _, s := range srcFiles {
		sourceType := ptr.Default(s.Type, defaultType)
		enabled := ptr.Deref(s.Enabled, true)
		format := ptr.Default(s.Format, defaultFormat)
		includeMeta := ptr.Deref(s.IncludeMeta, sourceDefaultIncludeMeta)
		namespace := ptr.Default(s.Discovery.Namespace, fallbackNamespace)
		selector := ptr.Default(s.Discovery.Selector, fallbackSelector)
		vectorPort := ptr.Default(ptr.Deref(s.Transport.VectorPort, 0), fallbackVectorPort)
		kubeconfig := ptr.Default(s.Cluster.KubeConfig, defs.Cluster.KubeConfig)
		kubeContext := ptr.Default(s.Cluster.Context, defs.Cluster.Context)

		var directURLs []string
		switch sourceType {
		case runconfig.SourceTypeDirect:
			url := ptr.Default(s.Endpoint.URL, fallbackDirectURL)
			directURLs = []string{url}
		case runconfig.SourceTypeKubernetes:
			directURLs = nil
		default:
			// Validation will report unsupported type.
		}

		out = append(out, topology.SourceConfig{
			BaseSourceConfig: runconfig.BaseSourceConfig{
				Name:           s.Name,
				Type:           sourceType,
				Enabled:        enabled,
				Format:         format,
				DirectURLs:     directURLs,
				Namespace:      namespace,
				LabelSelector:  selector,
				KubeConfigPath: kubeconfig,
				KubeContext:    kubeContext,
				VectorPort:     vectorPort,
				IncludeMeta:    includeMeta,
			},
		})
	}
	return out, nil
}

func resolveString(v *viper.Viper, cliFlagSet cliFlagSetFunc, key, defaultsValue string) string {
	if cliFlagSet(key) || v.InConfig(key) || envSetForKey(key) {
		return v.GetString(key)
	}
	if defaultsValue != "" {
		return defaultsValue
	}
	return v.GetString(key)
}

//nolint:unparam
func resolveStringSlice(v *viper.Viper, cliFlagSet cliFlagSetFunc, key, defaultsValue string) []string {
	if cliFlagSet(key) || v.InConfig(key) || envSetForKey(key) {
		return getList(v, key)
	}
	if defaultsValue != "" {
		return []string{defaultsValue}
	}
	return getList(v, key)
}

func resolveStringSliceList(v *viper.Viper, cliFlagSet cliFlagSetFunc, key string, defaultsValue []string) []string {
	if cliFlagSet(key) || v.InConfig(key) || envSetForKey(key) {
		return getList(v, key)
	}
	if len(defaultsValue) > 0 {
		return splitCSVSlice(append([]string{}, defaultsValue...))
	}
	return getList(v, key)
}

func resolveInt(v *viper.Viper, cliFlagSet cliFlagSetFunc, key string, defaultsValue *int) int {
	if cliFlagSet(key) || v.InConfig(key) || envSetForKey(key) {
		return v.GetInt(key)
	}
	if defaultsValue != nil {
		return *defaultsValue
	}
	return v.GetInt(key)
}

func resolveBool(v *viper.Viper, cliFlagSet cliFlagSetFunc, key string, defaultsValue *bool) bool {
	if cliFlagSet(key) || v.InConfig(key) || envSetForKey(key) {
		return v.GetBool(key)
	}
	if defaultsValue != nil {
		return *defaultsValue
	}
	return v.GetBool(key)
}

func resolveDuration(v *viper.Viper, cliFlagSet cliFlagSetFunc, key string, defaultsValue string) time.Duration {
	if cliFlagSet(key) || v.InConfig(key) || envSetForKey(key) {
		d, err := parseDurationValue(v.Get(key))
		if err == nil {
			return d
		}
		return 0
	}
	if defaultsValue != "" {
		d, err := parseDurationString(defaultsValue)
		if err == nil {
			return d
		}
		return 0
	}
	d, err := parseDurationValue(v.Get(key))
	if err == nil {
		return d
	}
	return 0
}

func parseDurationValue(value any) (time.Duration, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case time.Duration:
		return v, nil
	case string:
		return parseDurationString(v)
	default:
		return time.ParseDuration(fmt.Sprint(value))
	}
}

func parseDurationString(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", value, err)
	}
	return d, nil
}

func envSetForKey(key string) bool {
	name := "VECTAP_" + strings.ToUpper(strings.ReplaceAll(key, "-", "_"))
	_, ok := os.LookupEnv(name)
	return ok
}

func getList(v *viper.Viper, key string) []string {
	return splitCSVSlice(v.GetStringSlice(key))
}

func splitCSVSlice(in []string) []string {
	out := make([]string, 0, len(in))
	for _, item := range in {
		for _, p := range strings.Split(item, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}
