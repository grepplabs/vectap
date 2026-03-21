package tap

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/forward"
	"github.com/grepplabs/vectap/internal/stream"
	"github.com/grepplabs/vectap/internal/targets"
	"github.com/grepplabs/vectap/internal/vectorapi"
	"github.com/stretchr/testify/require"
)

func testTapConfig(common runconfig.BaseConfig, apply ...func(*Config)) Config {
	cfg := Config{BaseConfig: common}
	for _, fn := range apply {
		fn(&cfg)
	}
	return cfg
}

func TestNewDefaultRunnerCreatesRunnerWithoutEagerKubeInit(t *testing.T) {
	t.Setenv("KUBECONFIG", "/path/that/does/not/exist")

	r := NewDefaultRunner()
	require.NotNil(t, r)
}

func TestIncludeMetaForRender(t *testing.T) {
	t.Run("uses top-level include meta", func(t *testing.T) {
		got := includeMetaForRender(testTapConfig(runconfig.BaseConfig{IncludeMeta: true}), nil)
		require.True(t, got)
	})

	t.Run("uses source include meta when top-level is false", func(t *testing.T) {
		got := includeMetaForRender(
			testTapConfig(runconfig.BaseConfig{IncludeMeta: false}),
			[]Config{
				testTapConfig(runconfig.BaseConfig{IncludeMeta: false}),
				testTapConfig(runconfig.BaseConfig{IncludeMeta: true}),
			},
		)
		require.True(t, got)
	})

	t.Run("stays false when disabled everywhere", func(t *testing.T) {
		got := includeMetaForRender(
			testTapConfig(runconfig.BaseConfig{IncludeMeta: false}),
			[]Config{testTapConfig(runconfig.BaseConfig{IncludeMeta: false})},
		)
		require.False(t, got)
	})

	t.Run("honors source configs when running configured sources", func(t *testing.T) {
		got := includeMetaForRender(
			testTapConfig(runconfig.BaseConfig{IncludeMeta: true, AllSources: true}),
			[]Config{
				testTapConfig(runconfig.BaseConfig{IncludeMeta: false}),
				testTapConfig(runconfig.BaseConfig{IncludeMeta: false}),
			},
		)
		require.False(t, got)
	})
}

func TestFormatForRender(t *testing.T) {
	t.Run("uses top-level format for direct run", func(t *testing.T) {
		got, err := formatForRender(
			testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatJSON}),
			[]Config{testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatJSON})},
		)
		require.NoError(t, err)
		require.Equal(t, runconfig.FormatJSON, got)
	})

	t.Run("uses selected source format", func(t *testing.T) {
		got, err := formatForRender(
			testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatText, SelectedSources: []string{"a"}}),
			[]Config{testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatJSON})},
		)
		require.NoError(t, err)
		require.Equal(t, runconfig.FormatJSON, got)
	})

	t.Run("rejects mixed source formats", func(t *testing.T) {
		_, err := formatForRender(
			testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatText, AllSources: true}),
			[]Config{
				testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatJSON}),
				testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatText}),
			},
		)
		require.Error(t, err)
	})
}

func TestExpandRunConfigsSourceDefaultsApplied(t *testing.T) {
	cfg := testTapConfig(
		runconfig.BaseConfig{
			AllSources: true,
		},
		func(c *Config) {
			c.OutputsOf = []string{"global-out"}
			c.InputsOf = []string{"global-in"}
			c.LocalFilters = LocalFilters{
				ComponentKind: LocalFilterRules{IncludeGlob: []string{"sink"}},
			}
			c.Sources = []SourceConfig{
				{
					BaseSourceConfig: runconfig.BaseSourceConfig{
						Name:       "src-a",
						Type:       runconfig.SourceTypeDirect,
						Enabled:    true,
						DirectURLs: []string{runconfig.DefaultDirectURL},
						Format:     runconfig.FormatText,
						Namespace:  runconfig.DefaultNamespace,
						VectorPort: runconfig.DefaultVectorPort,
					},
					LocalFilters:  LocalFilters{ComponentType: LocalFilterRules{IncludeGlob: []string{"aws_*"}}},
					ApplyDefaults: true,
					TapScopeConfig: TapScopeConfig{
						OutputsOf: []string{"source-out"},
						InputsOf:  []string{"source-in"},
						Interval:  runconfig.DefaultTapInterval,
						Limit:     runconfig.DefaultTapLimit,
					},
				},
			}
		},
	)

	runCfgs, err := expandRunConfigs(cfg)
	require.NoError(t, err)
	require.Len(t, runCfgs, 1)
	require.Equal(t, []string{"global-out", "source-out"}, runCfgs[0].OutputsOf)
	require.Equal(t, []string{"global-in", "source-in"}, runCfgs[0].InputsOf)
	require.Equal(t, []string{"sink"}, runCfgs[0].LocalFilters.ComponentKind.IncludeGlob)
	require.Equal(t, []string{"aws_*"}, runCfgs[0].LocalFilters.ComponentType.IncludeGlob)
}

func TestExpandRunConfigsSourceDefaultsDisabled(t *testing.T) {
	cfg := testTapConfig(
		runconfig.BaseConfig{
			AllSources: true,
		},
		func(c *Config) {
			c.OutputsOf = []string{"global-out"}
			c.InputsOf = []string{"global-in"}
			c.LocalFilters = LocalFilters{
				ComponentKind: LocalFilterRules{IncludeGlob: []string{"sink"}},
			}
			c.Sources = []SourceConfig{
				{
					BaseSourceConfig: runconfig.BaseSourceConfig{
						Name:       "src-a",
						Type:       runconfig.SourceTypeDirect,
						Enabled:    true,
						DirectURLs: []string{runconfig.DefaultDirectURL},
						Format:     runconfig.FormatText,
						Namespace:  runconfig.DefaultNamespace,
						VectorPort: runconfig.DefaultVectorPort,
					},
					LocalFilters:  LocalFilters{ComponentType: LocalFilterRules{IncludeGlob: []string{"aws_*"}}},
					ApplyDefaults: false,
					TapScopeConfig: TapScopeConfig{
						OutputsOf: []string{"source-out"},
						InputsOf:  []string{"source-in"},
						Interval:  runconfig.DefaultTapInterval,
						Limit:     runconfig.DefaultTapLimit,
					},
				},
			}
		},
	)

	runCfgs, err := expandRunConfigs(cfg)
	require.NoError(t, err)
	require.Len(t, runCfgs, 1)
	require.Equal(t, []string{"source-out"}, runCfgs[0].OutputsOf)
	require.Equal(t, []string{"source-in"}, runCfgs[0].InputsOf)
	require.Empty(t, runCfgs[0].LocalFilters.ComponentKind.IncludeGlob)
	require.Equal(t, []string{"aws_*"}, runCfgs[0].LocalFilters.ComponentType.IncludeGlob)
}

func TestAddKubernetesSourceStreamsReconcilesDynamicTargets(t *testing.T) {
	originalResolverFactory := newKubeResolverFromConfig
	originalForwardFactory := newForwardManagerFromConfig
	originalReconcileInterval := kubernetesReconcileInterval
	t.Cleanup(func() {
		newKubeResolverFromConfig = originalResolverFactory
		newForwardManagerFromConfig = originalForwardFactory
		kubernetesReconcileInterval = originalReconcileInterval
	})
	kubernetesReconcileInterval = 10 * time.Millisecond

	snapshots := make(chan []targets.Target, 4)
	observeErrs := make(chan error)
	newKubeResolverFromConfig = func(_, _ string) (kubeTargetObserver, error) {
		return fakeKubeObserver{snapshots: snapshots, errs: observeErrs}, nil
	}

	fwd := &fakeForwardManager{
		sessions: map[string]*forward.Session{
			"obs/vector-a": {TargetID: "obs/vector-a", EndpointURL: "http://127.0.0.1:21001/graphql"},
			"obs/vector-b": {TargetID: "obs/vector-b", EndpointURL: "http://127.0.0.1:21002/graphql"},
		},
	}
	newForwardManagerFromConfig = func(_, _ string) (forward.Manager, error) {
		return fwd, nil
	}

	client := &fakeTapClient{
		eventsByEndpoint: map[string][]vectorapi.TapEvent{
			"http://127.0.0.1:21001/graphql": {{ComponentID: "a", Message: "from-a"}},
			"http://127.0.0.1:21002/graphql": {{ComponentID: "b", Message: "from-b"}},
		},
	}
	r := &Runner{client: client}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	mux := stream.NewMux()
	cfg := testTapConfig(runconfig.BaseConfig{
		Type:          runconfig.SourceTypeKubernetes,
		Namespace:     "obs",
		LabelSelector: "app=vector",
		VectorPort:    8686,
	})

	require.NoError(t, r.addKubernetesSourceStreams(ctx, cfg, mux))
	mux.CloseWhenDone()

	snapshots <- []targets.Target{{ID: "obs/vector-a", Namespace: "obs", PodName: "vector-a", RemotePort: 8686}}
	require.Equal(t, "from-a", readMuxEventMessage(t, mux))

	snapshots <- []targets.Target{
		{ID: "obs/vector-a", Namespace: "obs", PodName: "vector-a", RemotePort: 8686},
		{ID: "obs/vector-b", Namespace: "obs", PodName: "vector-b", RemotePort: 8686},
	}
	require.Equal(t, "from-b", readMuxEventMessage(t, mux))

	snapshots <- []targets.Target{{ID: "obs/vector-b", Namespace: "obs", PodName: "vector-b", RemotePort: 8686}}
	require.Eventually(t, func() bool {
		fwd.mu.Lock()
		defer fwd.mu.Unlock()
		return fwd.cancelled["obs/vector-a"]
	}, time.Second, 10*time.Millisecond)
}

func TestAddKubernetesSourceStreamsRetriesFailedStartWithoutNewSnapshot(t *testing.T) {
	originalResolverFactory := newKubeResolverFromConfig
	originalForwardFactory := newForwardManagerFromConfig
	originalReconcileInterval := kubernetesReconcileInterval
	t.Cleanup(func() {
		newKubeResolverFromConfig = originalResolverFactory
		newForwardManagerFromConfig = originalForwardFactory
		kubernetesReconcileInterval = originalReconcileInterval
	})
	kubernetesReconcileInterval = 10 * time.Millisecond

	snapshots := make(chan []targets.Target, 1)
	observeErrs := make(chan error)
	newKubeResolverFromConfig = func(_, _ string) (kubeTargetObserver, error) {
		return fakeKubeObserver{snapshots: snapshots, errs: observeErrs}, nil
	}

	fwd := &fakeForwardManager{
		sessions: map[string]*forward.Session{
			"obs/vector-a": {TargetID: "obs/vector-a", EndpointURL: "http://127.0.0.1:21001/graphql"},
		},
		startFailuresLeft: map[string]int{
			"obs/vector-a": 1,
		},
	}
	newForwardManagerFromConfig = func(_, _ string) (forward.Manager, error) {
		return fwd, nil
	}

	client := &fakeTapClient{
		eventsByEndpoint: map[string][]vectorapi.TapEvent{
			"http://127.0.0.1:21001/graphql": {{ComponentID: "a", Message: "from-a"}},
		},
	}
	r := &Runner{client: client}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	mux := stream.NewMux()
	cfg := testTapConfig(runconfig.BaseConfig{
		Type:          runconfig.SourceTypeKubernetes,
		Namespace:     "obs",
		LabelSelector: "app=vector",
		VectorPort:    8686,
	})

	require.NoError(t, r.addKubernetesSourceStreams(ctx, cfg, mux))
	mux.CloseWhenDone()

	snapshots <- []targets.Target{{ID: "obs/vector-a", Namespace: "obs", PodName: "vector-a", RemotePort: 8686}}
	require.Equal(t, "from-a", readMuxEventMessage(t, mux))

	require.Eventually(t, func() bool {
		return fwd.startCount("obs/vector-a") >= 2
	}, time.Second, 10*time.Millisecond)
}

func TestAddKubernetesSourceStreamsRestartsTargetAfterTapEndsUnexpectedly(t *testing.T) {
	originalResolverFactory := newKubeResolverFromConfig
	originalForwardFactory := newForwardManagerFromConfig
	originalReconcileInterval := kubernetesReconcileInterval
	t.Cleanup(func() {
		newKubeResolverFromConfig = originalResolverFactory
		newForwardManagerFromConfig = originalForwardFactory
		kubernetesReconcileInterval = originalReconcileInterval
	})
	kubernetesReconcileInterval = 10 * time.Millisecond

	snapshots := make(chan []targets.Target, 1)
	observeErrs := make(chan error)
	newKubeResolverFromConfig = func(_, _ string) (kubeTargetObserver, error) {
		return fakeKubeObserver{snapshots: snapshots, errs: observeErrs}, nil
	}

	fwd := &fakeForwardManager{
		sessions: map[string]*forward.Session{
			"obs/vector-a": {TargetID: "obs/vector-a", EndpointURL: "http://127.0.0.1:21001/graphql"},
		},
	}
	newForwardManagerFromConfig = func(_, _ string) (forward.Manager, error) {
		return fwd, nil
	}

	client := &fakeFlakyTapClient{
		tapPlans: map[string][]tapPlan{
			"http://127.0.0.1:21001/graphql": {
				{events: []vectorapi.TapEvent{{Message: "first"}}},
				{events: []vectorapi.TapEvent{{Message: "second"}}},
			},
		},
	}
	r := &Runner{client: client}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	mux := stream.NewMux()
	cfg := testTapConfig(runconfig.BaseConfig{
		Type:          runconfig.SourceTypeKubernetes,
		Namespace:     "obs",
		LabelSelector: "app=vector",
		VectorPort:    8686,
	})

	require.NoError(t, r.addKubernetesSourceStreams(ctx, cfg, mux))
	mux.CloseWhenDone()

	snapshots <- []targets.Target{{ID: "obs/vector-a", Namespace: "obs", PodName: "vector-a", RemotePort: 8686}}
	require.Equal(t, "first", readMuxEventMessage(t, mux))
	require.Equal(t, "second", readMuxEventMessage(t, mux))

	require.Eventually(t, func() bool {
		return fwd.startCount("obs/vector-a") >= 2
	}, time.Second, 10*time.Millisecond)
}

type fakeKubeObserver struct {
	snapshots <-chan []targets.Target
	errs      <-chan error
}

func (f fakeKubeObserver) Observe(_ context.Context, _ targets.ResolveOptions) (<-chan []targets.Target, <-chan error) {
	return f.snapshots, f.errs
}

type fakeForwardManager struct {
	sessions          map[string]*forward.Session
	startFailuresLeft map[string]int
	mu                sync.Mutex
	cancelled         map[string]bool
	startCalls        map[string]int
}

func (f *fakeForwardManager) Start(ctx context.Context, target targets.Target) (*forward.Session, error) {
	f.mu.Lock()
	if f.cancelled == nil {
		f.cancelled = make(map[string]bool)
	}
	if f.startCalls == nil {
		f.startCalls = make(map[string]int)
	}
	f.startCalls[target.ID]++
	if f.startFailuresLeft[target.ID] > 0 {
		f.startFailuresLeft[target.ID]--
		f.mu.Unlock()
		return nil, fmt.Errorf("temporary start failure for %s", target.ID)
	}
	f.mu.Unlock()
	go func() {
		<-ctx.Done()
		f.mu.Lock()
		f.cancelled[target.ID] = true
		f.mu.Unlock()
	}()
	return f.sessions[target.ID], nil
}

func (f *fakeForwardManager) startCount(targetID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.startCalls[targetID]
}

type fakeTapClient struct {
	eventsByEndpoint map[string][]vectorapi.TapEvent
}

func (f *fakeTapClient) Tap(ctx context.Context, endpointURL string, _ vectorapi.TapRequest) (<-chan vectorapi.TapEvent, <-chan error) {
	events := make(chan vectorapi.TapEvent, len(f.eventsByEndpoint[endpointURL]))
	errs := make(chan error)
	for _, event := range f.eventsByEndpoint[endpointURL] {
		events <- event
	}
	close(events)

	go func() {
		<-ctx.Done()
		close(errs)
	}()

	return events, errs
}

func (f *fakeTapClient) Components(context.Context, string, vectorapi.ComponentsRequest) ([]vectorapi.Component, error) {
	return nil, nil
}

type tapPlan struct {
	events []vectorapi.TapEvent
	errs   []error
}

type fakeFlakyTapClient struct {
	mu       sync.Mutex
	tapPlans map[string][]tapPlan
}

func (f *fakeFlakyTapClient) Tap(_ context.Context, endpointURL string, _ vectorapi.TapRequest) (<-chan vectorapi.TapEvent, <-chan error) {
	f.mu.Lock()
	plans := f.tapPlans[endpointURL]
	plan := tapPlan{}
	if len(plans) > 0 {
		plan = plans[0]
		f.tapPlans[endpointURL] = plans[1:]
	}
	f.mu.Unlock()

	events := make(chan vectorapi.TapEvent, len(plan.events))
	for _, event := range plan.events {
		events <- event
	}
	close(events)

	errs := make(chan error, len(plan.errs))
	for _, err := range plan.errs {
		errs <- err
	}
	close(errs)

	return events, errs
}

func (f *fakeFlakyTapClient) Components(context.Context, string, vectorapi.ComponentsRequest) ([]vectorapi.Component, error) {
	return nil, nil
}

func readMuxEventMessage(t *testing.T, mux *stream.Mux) string {
	t.Helper()

	select {
	case ev := <-mux.Events():
		return ev.Message
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for mux event")
		return ""
	}
}
