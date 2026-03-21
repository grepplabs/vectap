package tap

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

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

type kubeTargetObserver interface {
	Observe(ctx context.Context, opts targets.ResolveOptions) (<-chan []targets.Target, <-chan error)
}

type kubeTargetRuntime struct {
	cancel context.CancelFunc
}

var (
	newKubeResolverFromConfig = func(kubeConfigPath, kubeContext string) (kubeTargetObserver, error) {
		return kube.NewResolverFromConfig(kubeConfigPath, kubeContext)
	}
	newForwardManagerFromConfig = func(kubeConfigPath, kubeContext string) (forward.Manager, error) {
		return forward.NewManagerFromConfig(kubeConfigPath, kubeContext)
	}
	kubernetesReconcileInterval = time.Second
)

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
		if _, err := r.addTapStream(ctx, cfg, mux, endpointURL, target); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) addKubernetesSourceStreams(ctx context.Context, cfg Config, mux *stream.Mux) error {
	if _, err := newEventFilter(cfg.LocalFilters); err != nil {
		return err
	}

	resolver, err := newKubeResolverFromConfig(cfg.KubeConfigPath, cfg.KubeContext)
	if err != nil {
		return err
	}

	fwd, err := newForwardManagerFromConfig(cfg.KubeConfigPath, cfg.KubeContext)
	if err != nil {
		return err
	}

	snapshots, observeErrs := resolver.Observe(ctx, targets.ResolveOptions{
		Namespace:     cfg.Namespace,
		LabelSelector: cfg.LabelSelector,
		RemotePort:    cfg.VectorPort,
	})

	keepAliveEvents := make(chan output.Event)
	runtimeErrs := make(chan error, 16)
	mux.Add(cfg.SourceName+"/kubernetes", keepAliveEvents, runtimeErrs)
	close(keepAliveEvents)

	go r.runKubernetesSource(ctx, cfg, mux, fwd, snapshots, observeErrs, runtimeErrs)

	return nil
}

//nolint:cyclop
func (r *Runner) runKubernetesSource(
	ctx context.Context,
	cfg Config,
	mux *stream.Mux,
	fwd forward.Manager,
	snapshots <-chan []targets.Target,
	observeErrs <-chan error,
	runtimeErrs chan<- error,
) {
	defer close(runtimeErrs)

	active := make(map[string]kubeTargetRuntime)
	desired := make(map[string]targets.Target)
	finishedTargets := make(chan string, 64)
	reconcileTicker := time.NewTicker(kubernetesReconcileInterval)
	defer reconcileTicker.Stop()

	defer func() {
		for _, runtime := range active {
			runtime.cancel()
		}
	}()

	for snapshots != nil || observeErrs != nil {
		select {
		case <-ctx.Done():
			return
		case <-reconcileTicker.C:
			r.reconcileTapTargets(ctx, cfg, mux, fwd, active, desired, runtimeErrs, finishedTargets)
		case targetID := <-finishedTargets:
			if _, ok := active[targetID]; ok {
				delete(active, targetID)
				r.reconcileTapTargets(ctx, cfg, mux, fwd, active, desired, runtimeErrs, finishedTargets)
			}
		case err, ok := <-observeErrs:
			if !ok {
				observeErrs = nil
				continue
			}
			sendAsyncError(runtimeErrs, err)
		case ts, ok := <-snapshots:
			if !ok {
				snapshots = nil
				continue
			}
			desired = make(map[string]targets.Target, len(ts))
			for _, target := range ts {
				desired[target.ID] = target
			}
			r.reconcileTapTargets(ctx, cfg, mux, fwd, active, desired, runtimeErrs, finishedTargets)
		}
	}
}

func (r *Runner) reconcileTapTargets(
	ctx context.Context,
	cfg Config,
	mux *stream.Mux,
	fwd forward.Manager,
	active map[string]kubeTargetRuntime,
	desired map[string]targets.Target,
	runtimeErrs chan<- error,
	finishedTargets chan<- string,
) {
	for id, runtime := range active {
		if _, ok := desired[id]; ok {
			continue
		}
		runtime.cancel()
		delete(active, id)
	}

	for _, target := range desired {
		if _, ok := active[target.ID]; ok {
			continue
		}

		targetCtx, cancel := context.WithCancel(ctx)
		session, err := fwd.Start(targetCtx, target)
		if err != nil {
			cancel()
			sendAsyncError(runtimeErrs, fmt.Errorf("start port-forward for %s: %w", target.ID, err))
			continue
		}
		tapDone, err := r.addTapStream(targetCtx, cfg, mux, session.EndpointURL, target)
		if err != nil {
			cancel()
			sendAsyncError(runtimeErrs, err)
			continue
		}
		active[target.ID] = kubeTargetRuntime{cancel: cancel}

		go func(targetID string, streamDone <-chan struct{}, streamCtx context.Context) {
			<-streamDone
			if streamCtx.Err() != nil {
				return
			}
			select {
			case finishedTargets <- targetID:
			default:
			}
		}(target.ID, tapDone, targetCtx)
	}
}

func (r *Runner) addTapStream(ctx context.Context, cfg Config, mux *stream.Mux, endpointURL string, target targets.Target) (<-chan struct{}, error) {
	evFilter, err := newEventFilter(cfg.LocalFilters)
	if err != nil {
		return nil, err
	}
	events, errs := r.client.Tap(ctx, endpointURL, newTapRequest(cfg, target))
	outCh := make(chan output.Event)
	outErrs := make(chan error)
	streamDone := make(chan struct{})
	doneEvents := make(chan struct{})
	doneErrs := make(chan struct{})

	go func() {
		defer close(outCh)
		defer close(doneEvents)
		for ev := range events {
			out := withSourcePrefix(ev.ToOutput(target), cfg.SourceName)
			if evFilter.Allow(out) {
				outCh <- out
			}
		}
	}()
	go func() {
		defer close(outErrs)
		defer close(doneErrs)
		for err := range errs {
			outErrs <- err
		}
	}()
	go func() {
		defer close(streamDone)
		<-doneEvents
		<-doneErrs
	}()
	mux.Add(target.ID, outCh, outErrs)
	return streamDone, nil
}

func sendAsyncError(errCh chan<- error, err error) {
	if err == nil {
		return
	}
	select {
	case errCh <- err:
	default:
	}
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
		if s.ApplyDefaults {
			sc.OutputsOf = append(append([]string{}, cfg.OutputsOf...), s.OutputsOf...)
			sc.InputsOf = append(append([]string{}, cfg.InputsOf...), s.InputsOf...)
			sc.LocalFilters = mergeLocalFilters(cfg.LocalFilters, s.LocalFilters)
		} else {
			sc.OutputsOf = append([]string{}, s.OutputsOf...)
			sc.InputsOf = append([]string{}, s.InputsOf...)
			sc.LocalFilters = cloneLocalFilters(s.LocalFilters)
		}
		sc.Sources = nil
		sc.SelectedSources = nil
		sc.AllSources = false
		out = append(out, sc)
	}
	return out, nil
}

func mergeLocalFilters(base, extra LocalFilters) LocalFilters {
	out := cloneLocalFilters(base)
	out.ComponentType = mergeLocalFilterRules(out.ComponentType, extra.ComponentType)
	out.ComponentKind = mergeLocalFilterRules(out.ComponentKind, extra.ComponentKind)
	out.Name = mergeLocalFilterRules(out.Name, extra.Name)
	out.TagComponentID = mergeLocalFilterRules(out.TagComponentID, extra.TagComponentID)
	out.TagComponentKind = mergeLocalFilterRules(out.TagComponentKind, extra.TagComponentKind)
	out.TagComponentType = mergeLocalFilterRules(out.TagComponentType, extra.TagComponentType)
	out.TagHost = mergeLocalFilterRules(out.TagHost, extra.TagHost)
	return out
}

func cloneLocalFilters(in LocalFilters) LocalFilters {
	return LocalFilters{
		ComponentType:    cloneLocalFilterRules(in.ComponentType),
		ComponentKind:    cloneLocalFilterRules(in.ComponentKind),
		Name:             cloneLocalFilterRules(in.Name),
		TagComponentID:   cloneLocalFilterRules(in.TagComponentID),
		TagComponentKind: cloneLocalFilterRules(in.TagComponentKind),
		TagComponentType: cloneLocalFilterRules(in.TagComponentType),
		TagHost:          cloneLocalFilterRules(in.TagHost),
	}
}

func mergeLocalFilterRules(base, extra LocalFilterRules) LocalFilterRules {
	out := cloneLocalFilterRules(base)
	out.IncludeGlob = append(out.IncludeGlob, extra.IncludeGlob...)
	out.ExcludeGlob = append(out.ExcludeGlob, extra.ExcludeGlob...)
	out.IncludeRE = append(out.IncludeRE, extra.IncludeRE...)
	out.ExcludeRE = append(out.ExcludeRE, extra.ExcludeRE...)
	return out
}

func cloneLocalFilterRules(in LocalFilterRules) LocalFilterRules {
	return LocalFilterRules{
		IncludeGlob: append([]string{}, in.IncludeGlob...),
		ExcludeGlob: append([]string{}, in.ExcludeGlob...),
		IncludeRE:   append([]string{}, in.IncludeRE...),
		ExcludeRE:   append([]string{}, in.ExcludeRE...),
	}
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
