package cli

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	components "github.com/grepplabs/vectap/internal/app/components"
	tap "github.com/grepplabs/vectap/internal/app/tap"
	"github.com/stretchr/testify/require"
)

type captureRunner struct {
	tapCfgs        []tap.Config
	componentsCfgs []components.Config
}

func (r *captureRunner) Tap(_ context.Context, cfg tap.Config) error {
	r.tapCfgs = append(r.tapCfgs, cfg)
	return nil
}

func (r *captureRunner) Components(_ context.Context, cfg components.Config) error {
	r.componentsCfgs = append(r.componentsCfgs, cfg)
	return nil
}

func newRunnerWithCapture(r *captureRunner) newRunnerFunc {
	return func() appRunner { return r }
}

func runTapCommand(t *testing.T, args ...string) tap.Config {
	t.Helper()

	r := &captureRunner{}
	var stderr bytes.Buffer
	exitCode := execute(args, &stderr, newRunnerWithCapture(r))
	require.Zero(t, exitCode, "stderr=%q", stderr.String())
	require.Len(t, r.tapCfgs, 1)
	return r.tapCfgs[0]
}

func runTapCommandExpectError(t *testing.T, args ...string) {
	t.Helper()

	r := &captureRunner{}
	var stderr bytes.Buffer
	exitCode := execute(args, &stderr, newRunnerWithCapture(r))
	require.NotZero(t, exitCode, "expected failure")
	require.Empty(t, r.tapCfgs, "runner should not be invoked on validation error")
}

func runComponentsCommand(t *testing.T, args ...string) components.Config {
	t.Helper()

	r := &captureRunner{}
	var stderr bytes.Buffer
	exitCode := execute(args, &stderr, newRunnerWithCapture(r))
	require.Zero(t, exitCode, "stderr=%q", stderr.String())
	require.Len(t, r.componentsCfgs, 1)
	return r.componentsCfgs[0]
}

func requireFilterRules(t *testing.T, got tap.LocalFilterRules, includeGlob, excludeGlob, includeRE, excludeRE []string) {
	t.Helper()
	require.Equal(t, includeGlob, got.IncludeGlob)
	require.Equal(t, excludeGlob, got.ExcludeGlob)
	require.Equal(t, includeRE, got.IncludeRE)
	require.Equal(t, excludeRE, got.ExcludeRE)
}

func requireTapLocalFilters(t *testing.T, got tap.LocalFilters) {
	t.Helper()
	requireFilterRules(t, got.ComponentType, []string{"prometheus_*"}, []string{"console"}, []string{"^aws_.*$"}, []string{"^console$"})
	requireFilterRules(t, got.ComponentKind, []string{"sink"}, []string{"source"}, []string{"^sink$"}, []string{"^source$"})
	requireFilterRules(t, got.Name, []string{"component_*"}, []string{"adaptive_*"}, []string{"^component_.*"}, []string{"^adaptive_.*"})
	requireFilterRules(t, got.TagComponentID, []string{"destination-*"}, []string{"debug-*"}, []string{"^dest-.*$"}, []string{"^prom_.*$"})
	requireFilterRules(t, got.TagComponentKind, []string{"sink"}, []string{"source"}, []string{"^sink$"}, []string{"^source$"})
	requireFilterRules(t, got.TagComponentType, []string{"aws_*"}, []string{"console"}, []string{"^aws_.*$"}, []string{"^console$"})
	requireFilterRules(t, got.TagHost, []string{"vector-*"}, []string{"vector-9"}, []string{"^vector-[0-9]+$"}, []string{"^vector-9$"})
}

func TestTapDefaults(t *testing.T) {
	cfg := runTapCommand(t, "tap")

	require.Equal(t, "default", cfg.Namespace)
	require.Equal(t, "app.kubernetes.io/name=vector", cfg.LabelSelector)
	require.Equal(t, "direct", cfg.Type)
	require.Equal(t, []string{"http://127.0.0.1:8686/graphql"}, cfg.DirectURLs)
	require.Equal(t, "text", cfg.Format)
	require.Equal(t, 8686, cfg.VectorPort)
	require.Equal(t, 500, cfg.Interval)
	require.Equal(t, 100, cfg.Limit)
	require.Zero(t, cfg.Duration)
	require.False(t, cfg.NoColor)
	require.True(t, cfg.IncludeMeta)
	require.Empty(t, cfg.OutputsOf)
	require.Empty(t, cfg.InputsOf)
	require.Equal(t, tap.LocalFilters{}, cfg.LocalFilters)
}

func TestTapExplicitFlags(t *testing.T) {
	cfg := runTapCommand(t, "tap", "--type", "kubernetes", "--direct-url", "http://127.0.0.1:9999/graphql", "-n", "obs", "-l", "app=vector,role=agg", "--kubeconfig", "/tmp/kubeconfig", "--context", "kind-dev", "--outputs-of", "a, b", "--inputs-of", "x, y", "--local-filter", "+component.type:prometheus_*", "--local-filter", "-component.type:console", "--local-filter", "+re:component.type:^aws_.*$", "--local-filter", "-re:component.type:^console$", "--local-filter", "+component.kind:sink", "--local-filter", "-component.kind:source", "--local-filter", "+re:component.kind:^sink$", "--local-filter", "-re:component.kind:^source$", "--local-filter", "+name:component_*", "--local-filter", "-name:adaptive_*", "--local-filter", "+re:name:^component_.*", "--local-filter", "-re:name:^adaptive_.*", "--local-filter", "+tags.component_id:destination-*", "--local-filter", "-tags.component_id:debug-*", "--local-filter", "+re:tags.component_id:^dest-.*$", "--local-filter", "-re:tags.component_id:^prom_.*$", "--local-filter", "+tags.component_kind:sink", "--local-filter", "-tags.component_kind:source", "--local-filter", "+re:tags.component_kind:^sink$", "--local-filter", "-re:tags.component_kind:^source$", "--local-filter", "+tags.component_type:aws_*", "--local-filter", "-tags.component_type:console", "--local-filter", "+re:tags.component_type:^aws_.*$", "--local-filter", "-re:tags.component_type:^console$", "--local-filter", "+tags.host:vector-*", "--local-filter", "-tags.host:vector-9", "--local-filter", "+re:tags.host:^vector-[0-9]+$", "--local-filter", "-re:tags.host:^vector-9$", "--format", "json", "--no-color", "--vector-port", "9999", "--interval", "750", "--limit", "250", "--duration", "45s", "--include-meta")

	require.Equal(t, "obs", cfg.Namespace)
	require.Equal(t, "app=vector,role=agg", cfg.LabelSelector)
	require.Equal(t, "kubernetes", cfg.Type)
	require.Equal(t, []string{"http://127.0.0.1:9999/graphql"}, cfg.DirectURLs)
	require.Equal(t, "/tmp/kubeconfig", cfg.KubeConfigPath)
	require.Equal(t, "kind-dev", cfg.KubeContext)
	require.Equal(t, []string{"a", "b"}, cfg.OutputsOf)
	require.Equal(t, []string{"x", "y"}, cfg.InputsOf)
	requireFilterRules(t, cfg.LocalFilters.ComponentType, []string{"prometheus_*"}, []string{"console"}, []string{"^aws_.*$"}, []string{"^console$"})
	requireFilterRules(t, cfg.LocalFilters.ComponentKind, []string{"sink"}, []string{"source"}, []string{"^sink$"}, []string{"^source$"})
	requireFilterRules(t, cfg.LocalFilters.Name, []string{"component_*"}, []string{"adaptive_*"}, []string{"^component_.*"}, []string{"^adaptive_.*"})
	requireFilterRules(t, cfg.LocalFilters.TagComponentID, []string{"destination-*"}, []string{"debug-*"}, []string{"^dest-.*$"}, []string{"^prom_.*$"})
	requireFilterRules(t, cfg.LocalFilters.TagComponentKind, []string{"sink"}, []string{"source"}, []string{"^sink$"}, []string{"^source$"})
	requireFilterRules(t, cfg.LocalFilters.TagComponentType, []string{"aws_*"}, []string{"console"}, []string{"^aws_.*$"}, []string{"^console$"})
	requireFilterRules(t, cfg.LocalFilters.TagHost, []string{"vector-*"}, []string{"vector-9"}, []string{"^vector-[0-9]+$"}, []string{"^vector-9$"})
	require.Equal(t, "json", cfg.Format)
	require.True(t, cfg.NoColor)
	require.Equal(t, 9999, cfg.VectorPort)
	require.Equal(t, 750, cfg.Interval)
	require.Equal(t, 250, cfg.Limit)
	require.Equal(t, 45*time.Second, cfg.Duration)
	require.True(t, cfg.IncludeMeta)
}

func TestComponentsYAMLFormat(t *testing.T) {
	cfg := runComponentsCommand(t, "components", "--format", "yaml")
	require.Equal(t, "yaml", cfg.Format)
	require.Equal(t, "direct", cfg.Type)
	require.Equal(t, []string{"http://127.0.0.1:8686/graphql"}, cfg.DirectURLs)
}

func TestComponentsIncludeMetaFlag(t *testing.T) {
	cfg := runComponentsCommand(t, "components", "--include-meta=false")
	require.False(t, cfg.IncludeMeta)
}

func TestTapValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "invalid format", args: []string{"tap", "--format", "xml"}},
		{name: "invalid type", args: []string{"tap", "--type", "yaml"}},
		{name: "invalid port", args: []string{"tap", "--vector-port", "70000"}},
		{name: "invalid component regex", args: []string{"tap", "--component-re", "["}},
		{name: "invalid include component regex", args: []string{"tap", "--component-re", "["}},
		{name: "invalid exclude component regex", args: []string{"tap", "--exclude-component-re", "["}},
		{name: "invalid include component-id regex", args: []string{"tap", "--component-re", "["}},
		{name: "invalid exclude component-id regex", args: []string{"tap", "--exclude-component-re", "["}},
		{name: "invalid local filter regex", args: []string{"tap", "--local-filter", "+re:component.type:["}},
		{name: "invalid local filter field", args: []string{"tap", "--local-filter", "+unknown.field:x"}},
		{name: "removed include-tag-component alias", args: []string{"tap", "--include-tag-component", "^destination-.*"}},
		{name: "removed exclude-tag-component alias", args: []string{"tap", "--exclude-tag-component", "^prom_.*$"}},
		{name: "removed include-component alias", args: []string{"tap", "--include-component", "x"}},
		{name: "removed include-component-re alias", args: []string{"tap", "--include-component-re", "^x$"}},
		{name: "removed include-component-id alias", args: []string{"tap", "--include-component-id", "x*"}},
		{name: "removed exclude-component-id alias", args: []string{"tap", "--exclude-component-id", "x*"}},
		{name: "removed include-component-id-re alias", args: []string{"tap", "--include-component-id-re", "^x$"}},
		{name: "removed exclude-component-id-re alias", args: []string{"tap", "--exclude-component-id-re", "^x$"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runTapCommandExpectError(t, tc.args...)
		})
	}
}

func TestTapEnvironmentBindings(t *testing.T) {
	t.Setenv("VECTAP_NAMESPACE", "env-ns")
	t.Setenv("VECTAP_SELECTOR", "app=env")
	t.Setenv("VECTAP_TYPE", "kubernetes")
	t.Setenv("VECTAP_DIRECT_URL", "http://127.0.0.1:7777/graphql")
	t.Setenv("VECTAP_KUBECONFIG", "/env/kubeconfig")
	t.Setenv("VECTAP_CONTEXT", "env-context")
	t.Setenv("VECTAP_VECTOR_PORT", "7777")
	t.Setenv("VECTAP_INTERVAL", "650")
	t.Setenv("VECTAP_LIMIT", "150")
	t.Setenv("VECTAP_DURATION", "2m30s")
	t.Setenv("VECTAP_NO_COLOR", "true")
	t.Setenv("VECTAP_LOCAL_FILTER", "+component.type:prometheus_*,-component.type:console,+re:component.type:^aws_.*$,-re:component.type:^console$,+component.kind:sink,-component.kind:source,+re:component.kind:^sink$,-re:component.kind:^source$,+name:component_*,-name:adaptive_*,+re:name:^component_.*,-re:name:^adaptive_.*,+tags.component_id:destination-*,-tags.component_id:debug-*,+re:tags.component_id:^dest-.*$,-re:tags.component_id:^prom_.*$,+tags.component_kind:sink,-tags.component_kind:source,+re:tags.component_kind:^sink$,-re:tags.component_kind:^source$,+tags.component_type:aws_*,-tags.component_type:console,+re:tags.component_type:^aws_.*$,-re:tags.component_type:^console$,+tags.host:vector-*,-tags.host:vector-9,+re:tags.host:^vector-[0-9]+$,-re:tags.host:^vector-9$")

	cfg := runTapCommand(t, "tap")
	require.Equal(t, "env-ns", cfg.Namespace)
	require.Equal(t, "app=env", cfg.LabelSelector)
	require.Equal(t, "kubernetes", cfg.Type)
	require.Equal(t, []string{"http://127.0.0.1:7777/graphql"}, cfg.DirectURLs)
	require.Equal(t, "/env/kubeconfig", cfg.KubeConfigPath)
	require.Equal(t, "env-context", cfg.KubeContext)
	require.Equal(t, 7777, cfg.VectorPort)
	require.Equal(t, 650, cfg.Interval)
	require.Equal(t, 150, cfg.Limit)
	require.Equal(t, 150*time.Second, cfg.Duration)
	require.True(t, cfg.NoColor)
	requireTapLocalFilters(t, cfg.LocalFilters)
}

func TestTapFlagOverridesEnvironment(t *testing.T) {
	t.Setenv("VECTAP_NAMESPACE", "env-ns")
	t.Setenv("VECTAP_VECTOR_PORT", "7777")
	t.Setenv("VECTAP_INTERVAL", "650")
	t.Setenv("VECTAP_LIMIT", "150")
	t.Setenv("VECTAP_DURATION", "2m30s")

	cfg := runTapCommand(t, "tap", "--namespace", "flag-ns", "--vector-port", "8888", "--interval", "700", "--limit", "200", "--duration", "30s")
	require.Equal(t, "flag-ns", cfg.Namespace)
	require.Equal(t, 8888, cfg.VectorPort)
	require.Equal(t, 700, cfg.Interval)
	require.Equal(t, 200, cfg.Limit)
	require.Equal(t, 30*time.Second, cfg.Duration)
}

func TestTapRepeatableFlagsAccumulate(t *testing.T) {
	cfg := runTapCommand(t,
		"tap",
		"--outputs-of", "a",
		"--outputs-of", "b,c",
		"--outputs-of", "d,e",
		"--inputs-of", "x",
		"--inputs-of", "y,z",
	)
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, cfg.OutputsOf)
	require.Equal(t, []string{"x", "y", "z"}, cfg.InputsOf)
}

func TestTapOutputsOfFromEnvironment(t *testing.T) {
	t.Setenv("VECTAP_OUTPUTS_OF", "a,b")

	cfg := runTapCommand(t, "tap")
	require.Equal(t, []string{"a", "b"}, cfg.OutputsOf)
}

func TestTapLocalFilter(t *testing.T) {
	cfg := runTapCommand(t,
		"tap",
		"--local-filter", "+component.type:prometheus_*",
		"--local-filter", "-component.kind:source",
		"--local-filter", "+re:name:^component_.*$",
	)
	require.Equal(t, []string{"prometheus_*"}, cfg.LocalFilters.ComponentType.IncludeGlob)
	require.Equal(t, []string{"source"}, cfg.LocalFilters.ComponentKind.ExcludeGlob)
	require.Equal(t, []string{"^component_.*$"}, cfg.LocalFilters.Name.IncludeRE)
}

func TestTapLoadsSourcesFromConfigFile(t *testing.T) {
	cfgFile, err := os.CreateTemp(t.TempDir(), "vectap-*.yaml")
	require.NoError(t, err)
	defer cfgFile.Close()

	_, err = cfgFile.WriteString(`
defaults:
  type: kubernetes
  format: json
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
duration: 1m
sources:
  - name: local-direct
    type: direct
    format: text
    endpoint:
      url: http://127.0.0.1:8686/graphql
  - name: eu-prod
    type: kubernetes
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
`)
	require.NoError(t, err)

	cfg := runTapCommand(t, "--config", cfgFile.Name(), "tap", "--source", "eu-prod")
	require.Equal(t, []string{"eu-prod"}, cfg.SelectedSources)
	require.False(t, cfg.AllSources)
	require.Equal(t, "json", cfg.Format)
	require.Len(t, cfg.Sources, 2)
	require.Equal(t, "local-direct", cfg.Sources[0].Name)
	require.Equal(t, "direct", cfg.Sources[0].Type)
	require.Equal(t, []string{"http://127.0.0.1:8686/graphql"}, cfg.Sources[0].DirectURLs)
	require.Equal(t, "text", cfg.Sources[0].Format)
	require.Equal(t, "eu-prod", cfg.Sources[1].Name)
	require.Equal(t, "kubernetes", cfg.Sources[1].Type)
	require.Equal(t, "/tmp/kubeconfig", cfg.Sources[1].KubeConfigPath)
	require.Equal(t, "prod-eu", cfg.Sources[1].KubeContext)
	require.Equal(t, "telemetry-routing-engine", cfg.Sources[1].Namespace)
	require.Equal(t, "app=vector", cfg.Sources[1].LabelSelector)
	require.Equal(t, 8687, cfg.Sources[1].VectorPort)
	require.Equal(t, 750, cfg.Sources[1].Interval)
	require.Equal(t, 250, cfg.Sources[1].Limit)
	require.Equal(t, "json", cfg.Sources[1].Format)
	require.Equal(t, time.Minute, cfg.Duration)
}

func TestTapAllSourcesFlag(t *testing.T) {
	cfg := runTapCommand(t, "tap", "--all-sources")
	require.True(t, cfg.AllSources)
}
