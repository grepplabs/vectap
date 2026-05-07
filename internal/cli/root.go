package cli

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/grepplabs/vectap/internal/app/components"
	"github.com/grepplabs/vectap/internal/app/tap"
	"github.com/grepplabs/vectap/internal/app/topology"
	"github.com/grepplabs/vectap/internal/app/vectorcmd"
	"github.com/spf13/cobra"
)

type appRunner interface {
	Tap(ctx context.Context, cfg tap.Config) error
	Components(ctx context.Context, cfg components.Config) error
	Topology(ctx context.Context, cfg topology.Config) error
	Vector(ctx context.Context, cfg vectorcmd.Config) error
}

type newRunnerFunc func() appRunner

func Execute() int {
	return execute(os.Args[1:], os.Stderr, defaultRunner)
}

func execute(args []string, stderr io.Writer, newRunner newRunnerFunc) int {
	rootCmd := newRootCmd(newRunner)
	rootCmd.SetArgs(args)
	rootCmd.SetErr(stderr)

	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

func defaultRunner() appRunner {
	return &defaultAppRunner{
		tapRunner:        tap.NewDefaultRunner(),
		componentsRunner: components.NewDefaultRunner(),
		topologyRunner:   topology.NewDefaultRunner(),
		vectorRunner:     vectorcmd.NewDefaultRunner(),
	}
}

type defaultAppRunner struct {
	tapRunner        *tap.Runner
	componentsRunner *components.Runner
	topologyRunner   *topology.Runner
	vectorRunner     *vectorcmd.Runner
}

func (r *defaultAppRunner) Tap(ctx context.Context, cfg tap.Config) error {
	return r.tapRunner.Tap(ctx, cfg)
}

func (r *defaultAppRunner) Components(ctx context.Context, cfg components.Config) error {
	return r.componentsRunner.Components(ctx, cfg)
}

func (r *defaultAppRunner) Topology(ctx context.Context, cfg topology.Config) error {
	return r.topologyRunner.Topology(ctx, cfg)
}

func (r *defaultAppRunner) Vector(ctx context.Context, cfg vectorcmd.Config) error {
	return r.vectorRunner.Vector(ctx, cfg)
}

func newRootCmd(newRunner newRunnerFunc) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "vectap",
		Short:         "Aggregate Vector tap streams across Kubernetes and direct sources",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			k, err := loadKoanf(cmd)
			if err != nil {
				return err
			}
			cmd.SetContext(withKoanf(cmd.Context(), k))
			return nil
		},
	}
	cmd.PersistentFlags().String("config", "", "path to config file")

	cmd.AddCommand(newComponentsCmd(newRunner))
	cmd.AddCommand(newTopologyCmd(newRunner))
	cmd.AddCommand(newTapCmd(newRunner))
	cmd.AddCommand(newVectorCmd(newRunner))
	cmd.AddCommand(newVersionCmd())

	return cmd
}
