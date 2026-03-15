package tap

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/filter"
	"github.com/grepplabs/vectap/internal/forward"
	"github.com/grepplabs/vectap/internal/kube"
	"github.com/grepplabs/vectap/internal/output"
	"github.com/grepplabs/vectap/internal/render"
	"github.com/grepplabs/vectap/internal/stream"
	"github.com/grepplabs/vectap/internal/targets"
	"github.com/grepplabs/vectap/internal/vectorapi"
)

type Runner struct {
	client vectorapi.Client
}

type eventFilter struct {
	componentType *filter.ValueMatcher
	componentKind *filter.ValueMatcher
	name          *filter.ValueMatcher
	tag           *filter.TagFieldsMatcher
}

func NewDefaultRunner() *Runner {
	return &Runner{
		client: vectorapi.NewGraphQLWSClient(),
	}
}

func (r *Runner) Tap(ctx context.Context, cfg Config) error {
	return r.runTap(ctx, cfg)
}

// nolint:cyclop
func (r *Runner) runTap(ctx context.Context, cfg Config) error {
	runCfgs, err := expandRunConfigs(cfg)
	if err != nil {
		return err
	}

	mux := stream.NewMux()
	renderer, err := newRenderer(cfg, runCfgs)
	if err != nil {
		return err
	}

	evFilter, err := newEventFilter(cfg.LocalFilters)
	if err != nil {
		return err
	}

	if err := r.addRunConfigStreams(ctx, runCfgs, mux); err != nil {
		return err
	}

	mux.CloseWhenDone()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-mux.Errors():
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, "stream error:", err)
			}
		case ev, ok := <-mux.Events():
			if !ok {
				return nil
			}
			if !evFilter.Allow(ev) {
				continue
			}
			if err := renderer.Render(ev); err != nil {
				return err
			}
		}
	}
}

func (r *Runner) addRunConfigStreams(ctx context.Context, runCfgs []Config, mux *stream.Mux) error {
	for _, cfg := range runCfgs {
		if err := r.addSourceStreams(ctx, cfg, mux); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) addSourceStreams(ctx context.Context, cfg Config, mux *stream.Mux) error {
	switch cfg.Type {
	case runconfig.SourceTypeDirect:
		return r.addDirectSourceStreams(ctx, cfg, mux)
	case runconfig.SourceTypeKubernetes:
		return r.addKubernetesSourceStreams(ctx, cfg, mux)
	default:
		return fmt.Errorf("unsupported type %q", cfg.Type)
	}
}

func (r *Runner) addDirectSourceStreams(ctx context.Context, cfg Config, mux *stream.Mux) error {
	for i, endpointURL := range cfg.DirectURLs {
		target := directTarget(i, endpointURL)
		r.addTapStream(ctx, cfg, mux, endpointURL, target)
	}
	return nil
}

func (r *Runner) addKubernetesSourceStreams(ctx context.Context, cfg Config, mux *stream.Mux) error {
	resolver, err := kube.NewResolverFromConfig(cfg.KubeConfigPath, cfg.KubeContext)
	if err != nil {
		return err
	}

	fwd, err := forward.NewManagerFromConfig(cfg.KubeConfigPath, cfg.KubeContext)
	if err != nil {
		return err
	}

	ts, err := resolver.Resolve(ctx, targets.ResolveOptions{
		Namespace:     cfg.Namespace,
		LabelSelector: cfg.LabelSelector,
		RemotePort:    cfg.VectorPort,
	})
	if err != nil {
		return err
	}
	if len(ts) == 0 {
		return errors.New("no matching targets found")
	}

	for _, target := range ts {
		session, err := fwd.Start(ctx, target)
		if err != nil {
			return fmt.Errorf("start port-forward for %s: %w", target.ID, err)
		}
		r.addTapStream(ctx, cfg, mux, session.EndpointURL, target)
	}

	return nil
}

func (r *Runner) addTapStream(ctx context.Context, cfg Config, mux *stream.Mux, endpointURL string, target targets.Target) {
	events, errs := r.client.Tap(ctx, endpointURL, newTapRequest(cfg, target))
	outCh := make(chan output.Event)
	go func() {
		defer close(outCh)
		for ev := range events {
			outCh <- withSourcePrefix(ev.ToOutput(target), cfg.SourceName)
		}
	}()
	mux.Add(target.ID, outCh, errs)
}

func newTapRequest(cfg Config, target targets.Target) vectorapi.TapRequest {
	return vectorapi.TapRequest{
		OutputsOf:   append([]string{}, cfg.OutputsOf...),
		InputsOf:    append([]string{}, cfg.InputsOf...),
		Interval:    cfg.Interval,
		Limit:       cfg.Limit,
		IncludeMeta: cfg.IncludeMeta,
		Target:      target,
	}
}

func newEventFilter(filters LocalFilters) (*eventFilter, error) {
	componentType, err := newValueMatcher(filters.ComponentType)
	if err != nil {
		return nil, err
	}
	componentKind, err := newValueMatcher(filters.ComponentKind)
	if err != nil {
		return nil, err
	}
	name, err := newValueMatcher(filters.Name)
	if err != nil {
		return nil, err
	}
	tag, err := filter.NewTagFieldsMatcher(
		newTagFieldRules(filters.TagComponentID),
		newTagFieldRules(filters.TagComponentKind),
		newTagFieldRules(filters.TagComponentType),
		newTagFieldRules(filters.TagHost),
	)
	if err != nil {
		return nil, err
	}
	return &eventFilter{
		componentType: componentType,
		componentKind: componentKind,
		name:          name,
		tag:           tag,
	}, nil
}

func newValueMatcher(rules LocalFilterRules) (*filter.ValueMatcher, error) {
	return filter.NewValueMatcher(filter.ValueRules{
		IncludeGlob: rules.IncludeGlob,
		ExcludeGlob: rules.ExcludeGlob,
		IncludeRE:   rules.IncludeRE,
		ExcludeRE:   rules.ExcludeRE,
	})
}

func newTagFieldRules(rules LocalFilterRules) filter.TagFieldRules {
	return filter.TagFieldRules{
		IncludeGlob: rules.IncludeGlob,
		ExcludeGlob: rules.ExcludeGlob,
		IncludeRE:   rules.IncludeRE,
		ExcludeRE:   rules.ExcludeRE,
	}
}

func (f *eventFilter) Allow(ev output.Event) bool {
	if f.componentType.HasRules() && !f.componentType.Allow(ev.Meta["component_type"]) {
		return false
	}

	componentKind := ev.Kind
	if componentKind == "" {
		componentKind = ev.Meta["component_kind"]
	}
	if f.componentKind.HasRules() && !f.componentKind.Allow(componentKind) {
		return false
	}

	if f.name.HasRules() && !f.name.Allow(filter.ExtractPayloadName(ev.Message)) {
		return false
	}

	return !f.tag.HasRules() || f.tag.Allow(ev)
}

func expandRunConfigs(cfg Config) ([]Config, error) {
	if !cfg.UsesConfiguredSources() {
		return []Config{cfg}, nil
	}
	selected, err := runconfig.Select(
		cfg.AllSources,
		cfg.SelectedSources,
		cfg.Sources,
		func(s SourceConfig) string { return s.Name },
		func(s SourceConfig) bool { return s.Enabled },
	)
	if err != nil {
		return nil, err
	}

	out := make([]Config, 0, len(selected))
	for _, s := range selected {
		sc := cfg
		sc.Type = s.Type
		sc.DirectURLs = append([]string{}, s.DirectURLs...)
		sc.Namespace = s.Namespace
		sc.LabelSelector = s.LabelSelector
		sc.KubeConfigPath = s.KubeConfigPath
		sc.KubeContext = s.KubeContext
		sc.VectorPort = s.VectorPort
		sc.Interval = s.Interval
		sc.Limit = s.Limit
		sc.IncludeMeta = s.IncludeMeta
		sc.Format = s.Format
		sc.SourceName = s.Name
		sc.Sources = nil
		sc.SelectedSources = nil
		sc.AllSources = false
		out = append(out, sc)
	}
	return out, nil
}

func withSourcePrefix(ev output.Event, sourceName string) output.Event {
	if sourceName == "" {
		return ev
	}
	ev.Namespace = sourceName + "/" + ev.Namespace
	return ev
}

func directTarget(i int, endpointURL string) targets.Target {
	host := fmt.Sprintf("direct-%d", i+1)
	if u, err := url.Parse(endpointURL); err == nil && u.Host != "" {
		host = u.Host
	}
	safe := strings.NewReplacer("/", "_", ":", "_", ".", "_").Replace(host)
	return targets.Target{
		ID:        "direct/" + safe,
		Namespace: "direct",
		PodName:   host,
	}
}

func includeMetaForRender(cfg Config, runCfgs []Config) bool {
	// When running configured sources, honor the resolved per-source include_meta
	// values so defaults/source config can disable metadata output.
	if cfg.UsesConfiguredSources() {
		for _, rcfg := range runCfgs {
			if rcfg.IncludeMeta {
				return true
			}
		}
		return false
	}

	if cfg.IncludeMeta {
		return true
	}
	for _, rcfg := range runCfgs {
		if rcfg.IncludeMeta {
			return true
		}
	}
	return false
}

func formatForRender(cfg Config, runCfgs []Config) (string, error) {
	// For configured sources, render format comes from selected source config(s)
	// unless --format was explicitly used before expansion.
	if cfg.UsesConfiguredSources() {
		if len(runCfgs) == 0 {
			return cfg.Format, nil
		}
		format := runCfgs[0].Format
		if format == "" {
			format = runconfig.FormatText
		}
		for i := 1; i < len(runCfgs); i++ {
			candidate := runCfgs[i].Format
			if candidate == "" {
				candidate = runconfig.FormatText
			}
			if candidate != format {
				return "", fmt.Errorf("selected sources use different formats (%q and %q); set a single --format value", format, candidate)
			}
		}
		return format, nil
	}
	if cfg.Format == "" {
		return runconfig.FormatText, nil
	}
	return cfg.Format, nil
}

func newRenderer(cfg Config, runCfgs []Config) (render.Renderer, error) {
	renderCfg := cfg
	renderCfg.IncludeMeta = includeMetaForRender(cfg, runCfgs)

	format, err := formatForRender(cfg, runCfgs)
	if err != nil {
		return nil, err
	}
	renderCfg.Format = format

	return makeRenderer(renderCfg)
}

func makeRenderer(cfg Config) (render.Renderer, error) {
	switch cfg.Format {
	case runconfig.FormatText:
		return render.NewTextRenderer(os.Stdout, !cfg.NoColor, cfg.IncludeMeta), nil
	case runconfig.FormatJSON:
		return render.NewJSONRenderer(os.Stdout, cfg.IncludeMeta), nil
	case runconfig.FormatYAML:
		return render.NewYAMLRenderer(os.Stdout, cfg.IncludeMeta), nil
	default:
		return nil, fmt.Errorf("unknown renderer format %q", cfg.Format)
	}
}
