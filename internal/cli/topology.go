package cli

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	topology "github.com/grepplabs/vectap/internal/app/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newTopologyCmd(v *viper.Viper, newRunner newRunnerFunc) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topology",
		Short: "Show Vector topology (inputs/transforms/outputs)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := topologyConfigFromViper(v, cliFlagSetFromContext(cmd.Context()))
			if err != nil {
				return err
			}
			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()
			return newRunner().Topology(ctx, cfg)
		},
	}

	cmd.Flags().StringP("namespace", "n", runconfig.DefaultNamespace, "kubernetes namespace")
	cmd.Flags().StringP("selector", "l", runconfig.DefaultSelector, "label selector")
	cmd.Flags().String("type", runconfig.SourceTypeDirect, "source type: direct|kubernetes")
	cmd.Flags().StringSlice("direct-url", []string{runconfig.DefaultDirectURL}, "direct Vector GraphQL endpoint URL(s) (repeatable and/or comma-separated)")
	cmd.Flags().StringSlice("source", nil, "source names from config to run (repeatable and/or comma-separated)")
	cmd.Flags().Bool("all-sources", false, "run all enabled sources from config")
	cmd.Flags().String("kubeconfig", "", "path to kubeconfig file")
	cmd.Flags().String("context", "", "kubernetes context name")
	cmd.Flags().String("format", runconfig.FormatText, "output format: text|json|yaml")
	cmd.Flags().String("view", topology.ViewTable, "text view: table|edges|tree")
	cmd.Flags().Bool("orphaned", false, "show only orphaned components (no parents and no children); supported only with --view table")
	cmd.Flags().Int("vector-port", runconfig.DefaultVectorPort, "vector API port")
	cmd.Flags().Bool("include-meta", runconfig.DefaultIncludeMeta, "include metadata in output")

	return cmd
}
