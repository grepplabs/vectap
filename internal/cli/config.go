package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/grepplabs/vectap/internal/app/components"
	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/app/tap"
	"github.com/grepplabs/vectap/internal/app/topology"
	"github.com/grepplabs/vectap/internal/app/vectorcmd"
	"github.com/grepplabs/vectap/internal/ptr"
	"github.com/grepplabs/vectap/internal/vectorapi"
	"github.com/knadh/koanf/v2"
)

func tapConfigFromKoanf(v *koanf.Koanf) (tap.Config, error) {
	defs, err := loadDefaults(v)
	if err != nil {
		return tap.Config{}, err
	}

	defaultInterval := ptr.Default(ptr.Deref(defs.Transport.Interval, 0), runconfig.DefaultTapInterval)
	defaultLimit := ptr.Default(ptr.Deref(defs.Transport.Limit, 0), runconfig.DefaultTapLimit)
	baseConfig := baseConfigFromKoanf(v, defs)
	sources, err := loadSourceConfigs(defs, v, baseConfig.Format, baseConfig.IncludeMeta, baseConfig.API)
	if err != nil {
		return tap.Config{}, err
	}

	cfg := tap.Config{
		BaseConfig: baseConfig,
		Sources:    sources,
		TapScopeConfig: tap.TapScopeConfig{
			OutputsOf:  resolveStringSliceList(v, "outputs-of", defs.OutputsOf),
			InputsOf:   resolveStringSliceList(v, "inputs-of", defs.InputsOf),
			EventKinds: resolveEventKinds(v, "event-kind", defs.EventKinds),
			RawFormat:  resolveBool(v, "raw-format", defs.RawFormat),
			Interval:   resolveInt(v, "interval", new(defaultInterval)),
			Limit:      resolveInt(v, "limit", new(defaultLimit)),
			Duration:   resolveDuration(v, "duration", ""),
		},
		LocalFilters: tap.LocalFilters{},
		NoColor:      v.Bool("no-color"),
	}
	localFilters, err := parseLocalFilters(resolveStringSliceList(v, "local-filter", defs.LocalFilters))
	if err != nil {
		return tap.Config{}, err
	}
	cfg.LocalFilters = localFilters
	return cfg, cfg.Validate()
}

func componentsConfigFromKoanf(v *koanf.Koanf) (components.Config, error) {
	defs, err := loadDefaults(v)
	if err != nil {
		return components.Config{}, err
	}
	baseConfig := baseConfigFromKoanf(v, defs)
	sources, err := loadComponentsSourceConfigs(defs, v, baseConfig.Format, baseConfig.IncludeMeta, baseConfig.API)
	if err != nil {
		return components.Config{}, err
	}

	cfg := components.Config{
		BaseConfig: baseConfig,
		Sources:    sources,
	}
	return cfg, cfg.Validate()
}

func topologyConfigFromKoanf(v *koanf.Koanf) (topology.Config, error) {
	defs, err := loadDefaults(v)
	if err != nil {
		return topology.Config{}, err
	}

	baseConfig := baseConfigFromKoanf(v, defs)
	sources, err := loadTopologySourceConfigs(defs, v, baseConfig.Format, baseConfig.IncludeMeta, baseConfig.API)
	if err != nil {
		return topology.Config{}, err
	}

	cfg := topology.Config{
		BaseConfig: baseConfig,
		View:       resolveString(v, "view", topology.ViewTable),
		Orphaned:   resolveBool(v, "orphaned", new(false)),
		Sources:    sources,
	}
	return cfg, cfg.Validate()
}

func vectorConfigFromKoanf(v *koanf.Koanf, mode string, extraArgs []string) (vectorcmd.Config, error) {
	defs, err := loadDefaults(v)
	if err != nil {
		return vectorcmd.Config{}, err
	}

	baseConfig := baseConfigFromKoanf(v, defs)
	baseConfig.API = string(runconfig.VectorDefaultAPI)
	baseConfig.Format = runconfig.FormatText
	baseConfig.IncludeMeta = runconfig.DefaultIncludeMeta
	sources, err := loadVectorSourceConfigs(defs, v, baseConfig.Format, baseConfig.IncludeMeta, baseConfig.API)
	if err != nil {
		return vectorcmd.Config{}, err
	}

	cfg := vectorcmd.Config{
		BaseConfig:   baseConfig,
		Mode:         mode,
		VectorBin:    resolveString(v, "vector-bin", "vector"),
		TapPrefix:    resolveBool(v, "tap-prefix", new(true)),
		TapColor:     resolveBool(v, "tap-color", new(true)),
		TerminalCmd:  resolveString(v, "terminal-cmd", ""),
		TerminalHold: resolveBool(v, "terminal-hold", new(false)),
		ExtraArgs:    append([]string{}, extraArgs...),
		Sources:      sources,
	}
	return cfg, cfg.Validate()
}

func baseConfigFromKoanf(v *koanf.Koanf, defs defaultsFile) runconfig.BaseConfig {
	defaultType := ptr.Default(defs.Type, runconfig.SourceTypeDirect)
	defaultDirectURL := ptr.Default(defs.DirectURL, runconfig.DefaultDirectURL)
	defaultNamespace := ptr.Default(defs.Discovery.Namespace, runconfig.DefaultNamespace)
	defaultSelector := ptr.Default(defs.Discovery.Selector, runconfig.DefaultSelector)
	defaultFormat := ptr.Default(defs.Format, runconfig.FormatText)
	defaultVectorPort := ptr.Deref(defs.Transport.VectorPort, runconfig.DefaultVectorPort)
	defaultIncludeMeta := ptr.Deref(defs.IncludeMeta, runconfig.DefaultIncludeMeta)

	return runconfig.BaseConfig{
		Type:            resolveString(v, "type", defaultType),
		API:             resolveString(v, "api", defs.API),
		DirectURLs:      resolveStringSlice(v, "direct-url", defaultDirectURL),
		SelectedSources: getList(v, "source"),
		AllSources:      v.Bool("all-sources"),
		Namespace:       resolveString(v, "namespace", defaultNamespace),
		LabelSelector:   resolveString(v, "selector", defaultSelector),
		KubeConfigPath:  resolveString(v, "kubeconfig", defs.Cluster.KubeConfig),
		KubeContext:     resolveString(v, "context", defs.Cluster.Context),
		Format:          resolveString(v, "format", defaultFormat),
		VectorPort:      resolveInt(v, "vector-port", new(defaultVectorPort)),
		IncludeMeta:     resolveBool(v, "include-meta", new(defaultIncludeMeta)),
	}
}

type defaultsFile struct {
	Type         string   `koanf:"type"`
	API          string   `koanf:"api"`
	DirectURL    string   `koanf:"direct_url"`
	Format       string   `koanf:"format"`
	OutputsOf    []string `koanf:"outputs_of"`
	InputsOf     []string `koanf:"inputs_of"`
	EventKinds   []string `koanf:"event_kinds"`
	RawFormat    *bool    `koanf:"raw_format"`
	LocalFilters []string `koanf:"local_filters"`
	Cluster      struct {
		KubeConfig string `koanf:"kubeconfig"`
		Context    string `koanf:"context"`
	} `koanf:"cluster"`
	Discovery struct {
		Namespace string `koanf:"namespace"`
		Selector  string `koanf:"selector"`
	} `koanf:"discovery"`
	Transport struct {
		VectorPort *int `koanf:"vector_port"`
		Interval   *int `koanf:"interval"`
		Limit      *int `koanf:"limit"`
	} `koanf:"transport"`
	IncludeMeta *bool `koanf:"include_meta"`
}

type sourceFile struct {
	Name          string   `koanf:"name"`
	Type          string   `koanf:"type"`
	API           string   `koanf:"api"`
	Enabled       *bool    `koanf:"enabled"`
	Format        string   `koanf:"format"`
	IncludeMeta   *bool    `koanf:"include_meta"`
	OutputsOf     []string `koanf:"outputs_of"`
	InputsOf      []string `koanf:"inputs_of"`
	EventKinds    []string `koanf:"event_kinds"`
	RawFormat     *bool    `koanf:"raw_format"`
	LocalFilter   []string `koanf:"local_filters"`
	ApplyDefaults *bool    `koanf:"apply_defaults"`
	Cluster       struct {
		KubeConfig string `koanf:"kubeconfig"`
		Context    string `koanf:"context"`
	} `koanf:"cluster"`
	Discovery struct {
		Namespace string `koanf:"namespace"`
		Selector  string `koanf:"selector"`
	} `koanf:"discovery"`
	Transport struct {
		VectorPort *int `koanf:"vector_port"`
		Interval   *int `koanf:"interval"`
		Limit      *int `koanf:"limit"`
	} `koanf:"transport"`
	Endpoint struct {
		URL string `koanf:"url"`
	} `koanf:"endpoint"`
}

func loadDefaults(v *koanf.Koanf) (defaultsFile, error) {
	var defs defaultsFile
	if err := v.Unmarshal("defaults", &defs); err != nil {
		return defaultsFile{}, fmt.Errorf("decode defaults: %w", err)
	}
	defs.API = ptr.Default(defs.API, string(runconfig.VectorDefaultAPI))
	return defs, nil
}

//nolint:funlen
func loadSourceConfigs(defs defaultsFile, v *koanf.Koanf, defaultFormat string, sourceDefaultIncludeMeta bool, defaultAPI string) ([]tap.SourceConfig, error) {
	defaultType := ptr.Default(defs.Type, runconfig.SourceTypeDirect)
	fallbackDirectURL := ptr.Default(defs.DirectURL, runconfig.DefaultDirectURL)
	defaultFormat = ptr.Default(defaultFormat, runconfig.FormatText)
	fallbackNamespace := ptr.Default(defs.Discovery.Namespace, runconfig.DefaultNamespace)
	fallbackSelector := ptr.Default(defs.Discovery.Selector, runconfig.DefaultSelector)
	fallbackVectorPort := ptr.Default(ptr.Deref(defs.Transport.VectorPort, 0), runconfig.DefaultVectorPort)
	fallbackInterval := ptr.Default(ptr.Deref(defs.Transport.Interval, 0), runconfig.DefaultTapInterval)
	fallbackLimit := ptr.Default(ptr.Deref(defs.Transport.Limit, 0), runconfig.DefaultTapLimit)

	var srcFiles []sourceFile
	if err := v.Unmarshal("sources", &srcFiles); err != nil {
		return nil, fmt.Errorf("decode sources: %w", err)
	}
	out := make([]tap.SourceConfig, 0, len(srcFiles))
	for _, s := range srcFiles {
		sourceType := ptr.Default(s.Type, defaultType)
		sourceAPI := ptr.Default(s.API, defaultAPI)
		enabled := ptr.Deref(s.Enabled, true)
		format := ptr.Default(s.Format, defaultFormat)
		includeMeta := ptr.Deref(s.IncludeMeta, sourceDefaultIncludeMeta)
		outputsOf := splitCSVSlice(append([]string{}, s.OutputsOf...))
		inputsOf := splitCSVSlice(append([]string{}, s.InputsOf...))
		eventKinds := normalizeEventKinds(s.EventKinds, false)
		rawFormat := ptr.Deref(s.RawFormat, ptr.Deref(defs.RawFormat, false))
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
				API:            sourceAPI,
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
				OutputsOf:  outputsOf,
				InputsOf:   inputsOf,
				EventKinds: eventKinds,
				RawFormat:  rawFormat,
				Interval:   interval,
				Limit:      limit,
			},
			LocalFilters:  localFilters,
			ApplyDefaults: applyDefaults,
		})
	}
	return out, nil
}

func loadComponentsSourceConfigs(defs defaultsFile, v *koanf.Koanf, defaultFormat string, sourceDefaultIncludeMeta bool, defaultAPI string) ([]components.SourceConfig, error) {
	defaultType := ptr.Default(defs.Type, runconfig.SourceTypeDirect)
	fallbackDirectURL := ptr.Default(defs.DirectURL, runconfig.DefaultDirectURL)
	defaultFormat = ptr.Default(defaultFormat, runconfig.FormatText)
	fallbackNamespace := ptr.Default(defs.Discovery.Namespace, runconfig.DefaultNamespace)
	fallbackSelector := ptr.Default(defs.Discovery.Selector, runconfig.DefaultSelector)
	fallbackVectorPort := ptr.Default(ptr.Deref(defs.Transport.VectorPort, 0), runconfig.DefaultVectorPort)

	var srcFiles []sourceFile
	if err := v.Unmarshal("sources", &srcFiles); err != nil {
		return nil, fmt.Errorf("decode sources: %w", err)
	}
	out := make([]components.SourceConfig, 0, len(srcFiles))
	for _, s := range srcFiles {
		sourceType := ptr.Default(s.Type, defaultType)
		sourceAPI := ptr.Default(s.API, defaultAPI)
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
				API:            sourceAPI,
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

func loadTopologySourceConfigs(defs defaultsFile, v *koanf.Koanf, defaultFormat string, sourceDefaultIncludeMeta bool, defaultAPI string) ([]topology.SourceConfig, error) {
	defaultType := ptr.Default(defs.Type, runconfig.SourceTypeDirect)
	fallbackDirectURL := ptr.Default(defs.DirectURL, runconfig.DefaultDirectURL)
	defaultFormat = ptr.Default(defaultFormat, runconfig.FormatText)
	fallbackNamespace := ptr.Default(defs.Discovery.Namespace, runconfig.DefaultNamespace)
	fallbackSelector := ptr.Default(defs.Discovery.Selector, runconfig.DefaultSelector)
	fallbackVectorPort := ptr.Default(ptr.Deref(defs.Transport.VectorPort, 0), runconfig.DefaultVectorPort)

	var srcFiles []sourceFile
	if err := v.Unmarshal("sources", &srcFiles); err != nil {
		return nil, fmt.Errorf("decode sources: %w", err)
	}
	out := make([]topology.SourceConfig, 0, len(srcFiles))
	for _, s := range srcFiles {
		sourceType := ptr.Default(s.Type, defaultType)
		sourceAPI := ptr.Default(s.API, defaultAPI)
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
				API:            sourceAPI,
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

func loadVectorSourceConfigs(defs defaultsFile, v *koanf.Koanf, defaultFormat string, sourceDefaultIncludeMeta bool, defaultAPI string) ([]vectorcmd.SourceConfig, error) {
	componentSources, err := loadComponentsSourceConfigs(defs, v, defaultFormat, sourceDefaultIncludeMeta, defaultAPI)
	if err != nil {
		return nil, err
	}

	out := make([]vectorcmd.SourceConfig, 0, len(componentSources))
	for _, s := range componentSources {
		out = append(out, vectorcmd.SourceConfig{BaseSourceConfig: s.BaseSourceConfig})
	}
	return out, nil
}

func resolveString(v *koanf.Koanf, key, defaultsValue string) string {
	if v.Exists(key) {
		return v.String(key)
	}
	if defaultsValue != "" {
		return defaultsValue
	}
	return v.String(key)
}

//nolint:unparam
func resolveStringSlice(v *koanf.Koanf, key, defaultsValue string) []string {
	if v.Exists(key) {
		return getList(v, key)
	}
	if defaultsValue != "" {
		return []string{defaultsValue}
	}
	return getList(v, key)
}

func resolveStringSliceList(v *koanf.Koanf, key string, defaultsValue []string) []string {
	if v.Exists(key) {
		return getList(v, key)
	}
	if len(defaultsValue) > 0 {
		return splitCSVSlice(append([]string{}, defaultsValue...))
	}
	return getList(v, key)
}

func resolveEventKinds(v *koanf.Koanf, key string, defaultsValue []string) []string {
	var kinds []string
	if v.Exists(key) {
		kinds = getList(v, key)
	} else {
		kinds = append([]string{}, defaultsValue...)
	}
	return normalizeEventKinds(kinds, true)
}

func normalizeEventKinds(kinds []string, defaultLog bool) []string {
	raw := splitCSVSlice(append([]string{}, kinds...))
	if len(raw) == 0 && defaultLog {
		return []string{vectorapi.EventKindLog}
	}
	seen := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	for _, kind := range raw {
		normalized := strings.ToLower(strings.TrimSpace(kind))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if len(out) == 0 && defaultLog {
		return []string{vectorapi.EventKindLog}
	}
	return out
}

func resolveInt(v *koanf.Koanf, key string, defaultsValue *int) int {
	if v.Exists(key) {
		return v.Int(key)
	}
	if defaultsValue != nil {
		return *defaultsValue
	}
	return v.Int(key)
}

func resolveBool(v *koanf.Koanf, key string, defaultsValue *bool) bool {
	if v.Exists(key) {
		return v.Bool(key)
	}
	if defaultsValue != nil {
		return *defaultsValue
	}
	return v.Bool(key)
}

func resolveDuration(v *koanf.Koanf, key string, defaultsValue string) time.Duration {
	if v.Exists(key) {
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

func getList(v *koanf.Koanf, key string) []string {
	raw := v.Get(key)
	switch vv := raw.(type) {
	case nil:
		return nil
	case string:
		return splitCSVSlice([]string{vv})
	case []string:
		return splitCSVSlice(vv)
	case []any:
		out := make([]string, 0, len(vv))
		for _, item := range vv {
			out = append(out, fmt.Sprint(item))
		}
		return splitCSVSlice(out)
	default:
		return splitCSVSlice([]string{fmt.Sprint(raw)})
	}
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
