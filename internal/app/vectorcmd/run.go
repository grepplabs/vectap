package vectorcmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/forward"
	"github.com/grepplabs/vectap/internal/kube"
	"github.com/grepplabs/vectap/internal/targets"
)

type kubeResolver interface {
	Resolve(ctx context.Context, opts targets.ResolveOptions) ([]targets.Target, error)
}

type runnerDeps struct {
	newResolver func(kubeConfigPath, kubeContext string) (kubeResolver, error)
	newForward  func(kubeConfigPath, kubeContext string) (forward.Manager, error)
	lookPath    func(file string) (string, error)
	newCommand  func(ctx context.Context, name string, args ...string) *exec.Cmd
}

type Runner struct {
	deps runnerDeps
}

func NewDefaultRunner() *Runner {
	return &Runner{
		deps: runnerDeps{
			newResolver: func(kubeConfigPath, kubeContext string) (kubeResolver, error) {
				return kube.NewResolverFromConfig(kubeConfigPath, kubeContext)
			},
			newForward: func(kubeConfigPath, kubeContext string) (forward.Manager, error) {
				return forward.NewManagerFromConfig(kubeConfigPath, kubeContext)
			},
			lookPath:   exec.LookPath,
			newCommand: exec.CommandContext,
		},
	}
}

func (r *Runner) Vector(ctx context.Context, cfg Config) error {
	if _, err := r.deps.lookPath(cfg.VectorBin); err != nil {
		return fmt.Errorf("vector binary %q not found in PATH: %w", cfg.VectorBin, err)
	}

	runCfgs, err := expandRunConfigs(cfg)
	if err != nil {
		return err
	}

	invocations, err := r.buildInvocations(ctx, runCfgs)
	if err != nil {
		return err
	}
	if len(invocations) == 0 {
		return errors.New("no targets resolved")
	}

	if cfg.Mode == ModeTop || (cfg.Mode == ModeTap && cfg.TapLayout == TapLayoutTerminals) {
		return r.runInSeparateTerminals(ctx, cfg, invocations)
	}

	return r.runInCurrentTerminal(ctx, cfg, invocations)
}

//nolint:funlen
func (r *Runner) runInCurrentTerminal(ctx context.Context, cfg Config, invocations []invocation) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(invocations))
	var outMu sync.Mutex

	for i, inv := range invocations {
		wg.Add(1)
		go func(inv invocation, idx int) {
			defer wg.Done()

			//nolint:prealloc
			args := []string{cfg.Mode, "--url", inv.endpointURL}
			args = append(args, cfg.ExtraArgs...)
			cmd := r.deps.newCommand(ctx, cfg.VectorBin, args...)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				errCh <- fmt.Errorf("vector %s stdout pipe failed source=%q target=%q: %w", cfg.Mode, inv.sourceName, inv.targetID, err)
				return
			}
			stderr, err := cmd.StderrPipe()
			if err != nil {
				errCh <- fmt.Errorf("vector %s stderr pipe failed source=%q target=%q: %w", cfg.Mode, inv.sourceName, inv.targetID, err)
				return
			}

			_, _ = fmt.Fprintf(os.Stderr, "info: starting vector %s source=%q target=%q url=%s\n", cfg.Mode, inv.sourceName, inv.targetID, inv.endpointURL)
			if err := cmd.Start(); err != nil {
				errCh <- fmt.Errorf("vector %s start failed source=%q target=%q url=%s: %w", cfg.Mode, inv.sourceName, inv.targetID, inv.endpointURL, err)
				return
			}

			prefix := formatTapPrefix(inv, idx, cfg.TapPrefix, cfg.TapColor)
			var streamWG sync.WaitGroup
			streamWG.Add(2)
			go func() {
				defer streamWG.Done()
				streamPrefixed(stdout, os.Stdout, prefix, &outMu)
			}()
			go func() {
				defer streamWG.Done()
				streamPrefixed(stderr, os.Stderr, prefix, &outMu)
			}()

			waitErr := cmd.Wait()
			streamWG.Wait()
			if waitErr != nil {
				errCh <- fmt.Errorf("vector %s failed source=%q target=%q url=%s: %w", cfg.Mode, inv.sourceName, inv.targetID, inv.endpointURL, waitErr)
			}
		}(inv, i)
	}

	wg.Wait()
	close(errCh)

	var errs []string
	for err := range errCh {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func streamPrefixed(src io.Reader, dst io.Writer, prefix string, outMu *sync.Mutex) {
	scanner := bufio.NewScanner(src)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		outMu.Lock()
		_, _ = fmt.Fprintf(dst, "%s%s\n", prefix, scanner.Text())
		outMu.Unlock()
	}
}

func formatTapPrefix(inv invocation, idx int, enabled, color bool) string {
	if !enabled {
		return ""
	}
	label := fmt.Sprintf("[%s|%s] ", inv.sourceName, inv.targetID)
	if !color {
		return label
	}
	colors := []int{31, 32, 33, 34, 36, 91, 92, 93, 94, 96}
	c := colors[idx%len(colors)]
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", c, label)
}

func (r *Runner) runInSeparateTerminals(ctx context.Context, cfg Config, invocations []invocation) error {
	pidDir, err := os.MkdirTemp("", "vectap-top-pids-")
	if err != nil {
		return fmt.Errorf("create pid dir: %w", err)
	}
	defer os.RemoveAll(pidDir) //nolint:errcheck

	pidFiles := make([]string, 0, len(invocations))
	for i, inv := range invocations {
		vectorArgs := append([]string{cfg.Mode, "--url", inv.endpointURL}, cfg.ExtraArgs...)
		pidFile := fmt.Sprintf("%s/%d.pid", pidDir, i)
		title := fmt.Sprintf("vectap %s: %s %s", cfg.Mode, inv.sourceName, inv.targetID)
		cmd, err := r.newTerminalCommand(ctx, cfg, vectorArgs, pidFile, title)
		if err != nil {
			return fmt.Errorf("create terminal command for source=%q target=%q: %w", inv.sourceName, inv.targetID, err)
		}

		_, _ = fmt.Fprintf(os.Stderr, "info: starting vector %s source=%q target=%q url=%s\n", cfg.Mode, inv.sourceName, inv.targetID, inv.endpointURL)
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start vector %s source=%q target=%q url=%s: %w", cfg.Mode, inv.sourceName, inv.targetID, inv.endpointURL, err)
		}
		pidFiles = append(pidFiles, pidFile)
	}
	_, _ = fmt.Fprintf(os.Stderr, "info: vector %s terminals started; keeping port-forwards active until interrupted (Ctrl+C)\n", cfg.Mode)
	if !cfg.TerminalHold {
		if r.waitUntilAllExited(ctx, pidFiles) {
			_, _ = fmt.Fprintf(os.Stderr, "info: all vector %s processes exited; stopping port-forwards\n", cfg.Mode)
			return nil
		}
	}
	<-ctx.Done()
	r.killTrackedPIDs(pidFiles)
	return nil
}

func (r *Runner) waitUntilAllExited(ctx context.Context, pidFiles []string) bool {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			pids := readSpawnedPIDs(pidFiles)
			if len(pids) < len(pidFiles) {
				continue
			}
			allExited := true
			for _, pid := range pids {
				if isProcessAlive(pid) {
					allExited = false
					break
				}
			}
			if allExited {
				return true
			}
		}
	}
}

func (r *Runner) killTrackedPIDs(pidFiles []string) {
	for _, pid := range readSpawnedPIDs(pidFiles) {
		if p, err := os.FindProcess(pid); err == nil {
			_ = p.Kill()
		}
	}
}

func isProcessAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil
}

func (r *Runner) newTerminalCommand(ctx context.Context, cfg Config, vectorArgs []string, pidFile, title string) (*exec.Cmd, error) {
	cmdline := terminalCommandLine(cfg.VectorBin, vectorArgs, cfg.TerminalHold, pidFile, title)

	if cfg.TerminalCmd != "" {
		return r.customTerminalCommand(ctx, cfg.TerminalCmd, cmdline)
	}
	if runtime.GOOS != "linux" {
		return nil, fmt.Errorf("vector top terminal spawning is supported only on linux (current: %s); use vector top directly", runtime.GOOS)
	}
	return r.defaultLinuxTerminalCommand(ctx, cmdline)
}

func (r *Runner) customTerminalCommand(ctx context.Context, terminalCmd, cmdline string) (*exec.Cmd, error) {
	fields := strings.Fields(terminalCmd)
	if len(fields) == 0 {
		return nil, errors.New("terminal-cmd must not be empty")
	}
	if _, err := r.deps.lookPath(fields[0]); err != nil {
		return nil, fmt.Errorf("terminal command %q not found in PATH: %w", fields[0], err)
	}
	args := append(append([]string{}, fields[1:]...), "bash", "-lc", cmdline)
	return r.deps.newCommand(ctx, fields[0], args...), nil
}

func (r *Runner) defaultLinuxTerminalCommand(ctx context.Context, cmdline string) (*exec.Cmd, error) {
	type terminalTemplate struct {
		bin    string
		prefix []string
	}
	templates := []terminalTemplate{
		{bin: "x-terminal-emulator", prefix: []string{"-e"}},
		{bin: "gnome-terminal", prefix: []string{"--"}},
		{bin: "konsole", prefix: []string{"-e"}},
		{bin: "xfce4-terminal", prefix: []string{"-x"}},
		{bin: "xterm", prefix: []string{"-e"}},
		{bin: "alacritty", prefix: []string{"-e"}},
		{bin: "wezterm", prefix: []string{"start", "--"}},
	}
	for _, t := range templates {
		if _, err := r.deps.lookPath(t.bin); err == nil {
			args := append(append([]string{}, t.prefix...), "bash", "-lc", cmdline)
			return r.deps.newCommand(ctx, t.bin, args...), nil
		}
	}
	return nil, errors.New("no supported terminal emulator found (tried: x-terminal-emulator, gnome-terminal, konsole, xfce4-terminal, xterm, alacritty, wezterm); set --terminal-cmd to override")
}

func terminalCommandLine(vectorBin string, vectorArgs []string, hold bool, pidFile, title string) string {
	parts := append([]string{vectorBin}, vectorArgs...)
	cmdline := shellJoin(parts)
	modeLabel := "vector"
	if len(vectorArgs) > 0 && vectorArgs[0] != "" {
		modeLabel = "vector " + vectorArgs[0]
	}
	setTitle := "printf '\\033]0;%s\\007' " + shellQuote(title) + "; "
	if hold {
		return setTitle + "echo $$ > " + shellQuote(pidFile) + "; " + cmdline + "; _vectap_status=$?; if [ ${_vectap_status} -ne 0 ]; then printf '\\n" + modeLabel + " exited with status %s\\n' \"${_vectap_status}\"; fi; exec \"${SHELL:-/bin/sh}\" -l"
	}
	return setTitle + "echo $$ > " + shellQuote(pidFile) + "; exec " + cmdline
}

func readSpawnedPIDs(pidFiles []string) []int {
	out := make([]int, 0, len(pidFiles))
	for _, pidFile := range pidFiles {
		//nolint:gosec
		raw, err := os.ReadFile(pidFile)
		if err != nil {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil || pid <= 0 {
			continue
		}
		out = append(out, pid)
	}
	return out
}

func shellJoin(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, a := range args {
		quoted = append(quoted, shellQuote(a))
	}
	return strings.Join(quoted, " ")
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

type invocation struct {
	sourceName  string
	targetID    string
	endpointURL string
}

func (r *Runner) buildInvocations(ctx context.Context, cfgs []Config) ([]invocation, error) {
	out := make([]invocation, 0)
	for _, cfg := range cfgs {
		inv, err := r.sourceInvocations(ctx, cfg)
		if err != nil {
			return nil, err
		}
		out = append(out, inv...)
	}
	return out, nil
}

func (r *Runner) sourceInvocations(ctx context.Context, cfg Config) ([]invocation, error) {
	sourceName := sourceNameOrDefault(cfg.SourceName)

	switch cfg.Type {
	case runconfig.SourceTypeDirect:
		return directSourceInvocations(sourceName, cfg.DirectURLs)
	case runconfig.SourceTypeKubernetes:
		return r.kubernetesSourceInvocations(ctx, cfg, sourceName)
	default:
		return nil, fmt.Errorf("unsupported type %q", cfg.Type)
	}
}

func sourceNameOrDefault(sourceName string) string {
	if sourceName == "" {
		return "default"
	}
	return sourceName
}

func directSourceInvocations(sourceName string, directURLs []string) ([]invocation, error) {
	out := make([]invocation, 0, len(directURLs))
	for i, endpointURL := range directURLs {
		normalized, err := normalizeVectorURL(endpointURL)
		if err != nil {
			return nil, fmt.Errorf("source %q direct url %q: %w", sourceName, endpointURL, err)
		}
		out = append(out, invocation{
			sourceName:  sourceName,
			targetID:    fmt.Sprintf("direct/%d", i+1),
			endpointURL: normalized,
		})
	}
	return out, nil
}

func (r *Runner) kubernetesSourceInvocations(ctx context.Context, cfg Config, sourceName string) ([]invocation, error) {
	resolver, err := r.deps.newResolver(cfg.KubeConfigPath, cfg.KubeContext)
	if err != nil {
		return nil, err
	}
	fwd, err := r.deps.newForward(cfg.KubeConfigPath, cfg.KubeContext)
	if err != nil {
		return nil, err
	}

	ts, err := resolver.Resolve(ctx, targets.ResolveOptions{
		Namespace:     cfg.Namespace,
		LabelSelector: cfg.LabelSelector,
		RemotePort:    cfg.VectorPort,
	})
	if err != nil {
		return nil, err
	}
	if len(ts) == 0 {
		return nil, fmt.Errorf("source %q: no matching targets found", sourceName)
	}

	out := make([]invocation, 0, len(ts))
	for _, t := range ts {
		session, err := fwd.Start(ctx, t)
		if err != nil {
			return nil, fmt.Errorf("source %q: start port-forward for %s: %w", sourceName, t.ID, err)
		}
		normalized, err := normalizeVectorURL(session.EndpointURL)
		if err != nil {
			return nil, fmt.Errorf("source %q target %q: %w", sourceName, t.ID, err)
		}
		out = append(out, invocation{sourceName: sourceName, targetID: t.ID, endpointURL: normalized})
	}
	return out, nil
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
		sc.API = s.API
		sc.DirectURLs = append([]string{}, s.DirectURLs...)
		sc.Namespace = s.Namespace
		sc.LabelSelector = s.LabelSelector
		sc.KubeConfigPath = s.KubeConfigPath
		sc.KubeContext = s.KubeContext
		sc.VectorPort = s.VectorPort
		sc.Format = s.Format
		sc.IncludeMeta = s.IncludeMeta
		sc.SourceName = s.Name
		sc.Sources = nil
		sc.SelectedSources = nil
		sc.AllSources = false
		out = append(out, sc)
	}
	return out, nil
}

func normalizeVectorURL(raw string) (string, error) {
	if !strings.Contains(raw, "://") {
		return strings.TrimSuffix(raw, "/graphql"), nil
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", errors.New("missing host")
	}
	u.Path = ""
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}
