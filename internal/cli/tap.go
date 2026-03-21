package cli

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newTapCmd(v *viper.Viper, newRunner newRunnerFunc) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tap",
		Short: "Tap events from Vector instances",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := tapConfigFromViper(v, cliFlagSetFromContext(cmd.Context()))
			if err != nil {
				return err
			}
			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()
			return newRunner().Tap(ctx, cfg)
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
	cmd.Flags().StringSlice("outputs-of", nil, "components (sources, transforms) IDs whose outputs to observe (glob patterns; repeatable and/or comma-separated)")
	cmd.Flags().StringSlice("inputs-of", nil, "components (transforms, sinks) IDs whose inputs to observe (glob patterns; repeatable and/or comma-separated)")
	cmd.Flags().StringSlice("local-filter", nil, "local filter rule: <+|-><field>:<glob> or <+|->re:<field>:<regex> (field: component.type|component.kind|name|tags.component_id|tags.component_kind|tags.component_type|tags.host)")
	cmd.Flags().String("format", runconfig.FormatText, "output format: text|json|yaml")
	cmd.Flags().Bool("no-color", false, "disable colored output")
	cmd.Flags().Int("vector-port", runconfig.DefaultVectorPort, "vector API port")
	cmd.Flags().Int("interval", runconfig.DefaultTapInterval, "sampling interval in milliseconds")
	cmd.Flags().Int("limit", runconfig.DefaultTapLimit, "maximum number of events per interval")
	cmd.Flags().DurationP("duration", "d", 0, "sampling duration in Go format (for example 30s, 5m, 1h15m); exits automatically when elapsed")
	cmd.Flags().Bool("include-meta", runconfig.DefaultIncludeMeta, "include metadata in output")

	return cmd
}
