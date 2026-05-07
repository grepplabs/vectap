package vectorcmd

import (
	"context"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/forward"
	"github.com/grepplabs/vectap/internal/targets"
	"github.com/stretchr/testify/require"
)

type fakeResolver struct {
	targets []targets.Target
	err     error
}

func (f fakeResolver) Resolve(_ context.Context, _ targets.ResolveOptions) ([]targets.Target, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.targets, nil
}

type fakeForwardManager struct {
	sessions map[string]*forward.Session
}

func (f fakeForwardManager) Start(_ context.Context, target targets.Target) (*forward.Session, error) {
	return f.sessions[target.ID], nil
}

func TestNormalizeVectorURL(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "http://127.0.0.1:8686/graphql", want: "http://127.0.0.1:8686"},
		{in: "http://127.0.0.1:8686", want: "http://127.0.0.1:8686"},
		{in: "127.0.0.1:8686", want: "127.0.0.1:8686"},
	}
	for _, tt := range tests {
		got, err := normalizeVectorURL(tt.in)
		require.NoError(t, err)
		require.Equal(t, tt.want, got)
	}
}

func TestRunnerVectorRunsAllDirectInvocations(t *testing.T) {
	var got [][]string
	var mu sync.Mutex
	r := &Runner{deps: runnerDeps{
		lookPath: func(_ string) (string, error) { return "/usr/bin/vector", nil },
		newResolver: func(_, _ string) (kubeResolver, error) {
			return fakeResolver{}, nil
		},
		newForward: func(_, _ string) (forward.Manager, error) {
			return fakeForwardManager{}, nil
		},
		newCommand: func(ctx context.Context, name string, args ...string) *exec.Cmd {
			copied := append([]string{name}, args...)
			mu.Lock()
			got = append(got, copied)
			mu.Unlock()
			return exec.CommandContext(ctx, "true")
		},
	}}

	cfg := Config{
		BaseConfig: runconfig.BaseConfig{
			Type:       runconfig.SourceTypeDirect,
			DirectURLs: []string{"http://127.0.0.1:8686/graphql", "http://127.0.0.1:8787"},
			Namespace:  runconfig.DefaultNamespace,
			Format:     runconfig.FormatText,
			VectorPort: runconfig.DefaultVectorPort,
		},
		Mode:      ModeTap,
		VectorBin: "vector",
		ExtraArgs: []string{"--interval", "200"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	require.NoError(t, r.Vector(ctx, cfg))
	require.Len(t, got, 2)

	wantA := []string{"vector", "tap", "--url", "http://127.0.0.1:8686", "--interval", "200"}
	wantB := []string{"vector", "tap", "--url", "http://127.0.0.1:8787", "--interval", "200"}
	require.ElementsMatch(t, [][]string{wantA, wantB}, got)
}

func TestRunnerVectorTopUsesCustomTerminalCmd(t *testing.T) {
	var got [][]string
	var mu sync.Mutex
	r := &Runner{deps: runnerDeps{
		lookPath: func(file string) (string, error) { return "/usr/bin/" + file, nil },
		newResolver: func(_, _ string) (kubeResolver, error) {
			return fakeResolver{}, nil
		},
		newForward: func(_, _ string) (forward.Manager, error) {
			return fakeForwardManager{}, nil
		},
		newCommand: func(ctx context.Context, name string, args ...string) *exec.Cmd {
			copied := append([]string{name}, args...)
			mu.Lock()
			got = append(got, copied)
			mu.Unlock()
			return exec.CommandContext(ctx, "true")
		},
	}}

	cfg := Config{
		BaseConfig: runconfig.BaseConfig{
			Type:       runconfig.SourceTypeDirect,
			DirectURLs: []string{"http://127.0.0.1:8686"},
			Namespace:  runconfig.DefaultNamespace,
			Format:     runconfig.FormatText,
			VectorPort: runconfig.DefaultVectorPort,
		},
		Mode:         ModeTop,
		VectorBin:    "vector",
		TerminalCmd:  "xterm -e",
		TerminalHold: true,
		ExtraArgs:    []string{"--human-metrics"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	require.NoError(t, r.Vector(ctx, cfg))
	require.Len(t, got, 1)
	require.Len(t, got[0], 5)
	require.Equal(t, []string{"xterm", "-e", "bash", "-lc"}, got[0][:4])
	cmdline := got[0][4]
	require.Contains(t, cmdline, "echo $$ > ")
	require.NotContains(t, cmdline, "& _vectap_child=$!")
	require.Contains(t, cmdline, "printf '\\033]0;%s\\007' 'vectap top: default direct/1';")
	require.Contains(t, cmdline, "'vector' 'top' '--url' 'http://127.0.0.1:8686' '--human-metrics'")
	require.Contains(t, cmdline, "exec \"${SHELL:-/bin/sh}\" -l")
	require.NotContains(t, cmdline, "exit ${_vectap_status}")
}

func TestRunnerVectorTapTerminalsUsesCustomTerminalCmd(t *testing.T) {
	var got [][]string
	var mu sync.Mutex
	r := &Runner{deps: runnerDeps{
		lookPath: func(file string) (string, error) { return "/usr/bin/" + file, nil },
		newResolver: func(_, _ string) (kubeResolver, error) {
			return fakeResolver{}, nil
		},
		newForward: func(_, _ string) (forward.Manager, error) {
			return fakeForwardManager{}, nil
		},
		newCommand: func(ctx context.Context, name string, args ...string) *exec.Cmd {
			copied := append([]string{name}, args...)
			mu.Lock()
			got = append(got, copied)
			mu.Unlock()
			return exec.CommandContext(ctx, "true")
		},
	}}

	cfg := Config{
		BaseConfig: runconfig.BaseConfig{
			Type:       runconfig.SourceTypeDirect,
			DirectURLs: []string{"http://127.0.0.1:8686"},
			Namespace:  runconfig.DefaultNamespace,
			Format:     runconfig.FormatText,
			VectorPort: runconfig.DefaultVectorPort,
		},
		Mode:         ModeTap,
		TapLayout:    TapLayoutTerminals,
		VectorBin:    "vector",
		TerminalCmd:  "xterm -e",
		TerminalHold: true,
		ExtraArgs:    []string{"--interval", "200"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	require.NoError(t, r.Vector(ctx, cfg))
	require.Len(t, got, 1)
	cmdline := got[0][4]
	require.Contains(t, cmdline, "printf '\\033]0;%s\\007' 'vectap tap: default direct/1';")
	require.Contains(t, cmdline, "'vector' 'tap' '--url' 'http://127.0.0.1:8686' '--interval' '200'")
	require.Contains(t, cmdline, "vector tap exited with status %s")
}

func TestTerminalCommandLineWithoutHoldExecsVectorInForeground(t *testing.T) {
	cmdline := terminalCommandLine("vector", []string{"top", "--url", "http://127.0.0.1:8686"}, false, "/tmp/vectap.pid", "vectap top")

	require.Contains(t, cmdline, "printf '\\033]0;%s\\007' 'vectap top';")
	require.Contains(t, cmdline, "echo $$ > '/tmp/vectap.pid'; exec 'vector' 'top' '--url' 'http://127.0.0.1:8686'")
}

func TestTerminalCommandLineWithHoldKeepsTerminalOpenAfterFailures(t *testing.T) {
	cmdline := terminalCommandLine("vector", []string{"top", "--url", "http://127.0.0.1:8686"}, true, "/tmp/vectap.pid", "vectap top")

	require.Contains(t, cmdline, "printf '\\033]0;%s\\007' 'vectap top';")
	require.Contains(t, cmdline, "echo $$ > '/tmp/vectap.pid'; 'vector' 'top' '--url' 'http://127.0.0.1:8686';")
	require.Contains(t, cmdline, "_vectap_status=$?")
	require.Contains(t, cmdline, "vector top exited with status %s")
	require.Contains(t, cmdline, "exec \"${SHELL:-/bin/sh}\" -l")
}
