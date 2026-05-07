package cli

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/app/vectorcmd"
	"github.com/spf13/cobra"
)

func newVectorCmd(newRunner newRunnerFunc) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "vector",
		Short: "Run vector top/tap against resolved endpoints",
	}

	cmd.AddCommand(newVectorTapCmd(newRunner))
	cmd.AddCommand(newVectorTopCmd(newRunner))
	return cmd
}

func newVectorTapCmd(newRunner newRunnerFunc) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tap [-- <vector tap args...>]",
		Short: "Run vector tap for all resolved endpoints in parallel",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := vectorConfigFromCommand(cmd, vectorcmd.ModeTap, args)
			if err != nil {
				return err
			}
			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()
			return newRunner().Vector(ctx, cfg)
		},
	}
	addVectorBaseFlags(cmd)
	cmd.Flags().Bool("tap-prefix", true, "prefix each vector tap line with source/target")
	cmd.Flags().Bool("tap-color", true, "colorize vector tap prefixes")
	cmd.Flags().String("tap-layout", vectorcmd.TapLayoutMerged, "tap output layout: merged|terminals")
	return cmd
}

func newVectorTopCmd(newRunner newRunnerFunc) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "top [-- <vector top args...>]",
		Short: "Run vector top for all resolved endpoints in parallel",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := vectorConfigFromCommand(cmd, vectorcmd.ModeTop, args)
			if err != nil {
				return err
			}
			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()
			return newRunner().Vector(ctx, cfg)
		},
	}
	addVectorBaseFlags(cmd)
	return cmd
}

func addVectorBaseFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("namespace", "n", runconfig.DefaultNamespace, "kubernetes namespace")
	cmd.Flags().StringP("selector", "l", runconfig.DefaultSelector, "label selector")
	cmd.Flags().String("type", runconfig.SourceTypeDirect, "source type: direct|kubernetes")
	cmd.Flags().StringSlice("direct-url", []string{runconfig.DefaultDirectURL}, "direct Vector API endpoint URL(s) (repeatable and/or comma-separated)")
	cmd.Flags().StringSlice("source", nil, "source names from config to run (repeatable and/or comma-separated)")
	cmd.Flags().Bool("all-sources", false, "run all enabled sources from config")
	cmd.Flags().String("kubeconfig", "", "path to kubeconfig file")
	cmd.Flags().String("context", "", "kubernetes context name")
	cmd.Flags().Int("vector-port", runconfig.DefaultVectorPort, "vector API port")
	cmd.Flags().String("vector-bin", "vector", "vector executable path")
	cmd.Flags().String("terminal-cmd", "", "terminal launcher command prefix for vector top (for example: \"gnome-terminal --\", \"xterm -e\", \"open -a Terminal\")")
	cmd.Flags().Bool("terminal-hold", false, "keep terminal open after vector top exits")
}

func vectorConfigFromCommand(cmd *cobra.Command, mode string, extraArgs []string) (vectorcmd.Config, error) {
	k, err := koanfFromContext(cmd.Context())
	if err != nil {
		return vectorcmd.Config{}, err
	}
	cfg, err := vectorConfigFromKoanf(k, mode, extraArgs)
	if err != nil {
		return vectorcmd.Config{}, err
	}
	return cfg, nil
}
