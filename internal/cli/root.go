package cli

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	components "github.com/grepplabs/vectap/internal/app/components"
	tap "github.com/grepplabs/vectap/internal/app/tap"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type appRunner interface {
	Tap(ctx context.Context, cfg tap.Config) error
	Components(ctx context.Context, cfg components.Config) error
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
	}
}

type defaultAppRunner struct {
	tapRunner        *tap.Runner
	componentsRunner *components.Runner
}

func (r *defaultAppRunner) Tap(ctx context.Context, cfg tap.Config) error {
	return r.tapRunner.Tap(ctx, cfg)
}

func (r *defaultAppRunner) Components(ctx context.Context, cfg components.Config) error {
	return r.componentsRunner.Components(ctx, cfg)
}

func newRootCmd(newRunner newRunnerFunc) *cobra.Command {
	v := viper.New()
	v.SetEnvPrefix("VECTAP")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	cmd := &cobra.Command{
		Use:           "vectap",
		Short:         "Aggregate Vector tap streams across Kubernetes and direct sources",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			setChangedFlagsInContext(cmd)
			if err := bindActiveCommandFlags(v, cmd); err != nil {
				return err
			}

			cfgPath, err := cmd.Flags().GetString("config")
			if err != nil {
				return err
			}
			if cfgPath == "" {
				cfgPath = v.GetString("config")
			}
			if cfgPath != "" {
				v.SetConfigFile(cfgPath)
				if err := v.ReadInConfig(); err != nil {
					return fmt.Errorf("failed to read config file %s: %w", cfgPath, err)
				}
			}
			applyViperToCommand(v, cmd)
			return nil
		},
	}
	cmd.PersistentFlags().String("config", "", "path to config file")

	cmd.AddCommand(newComponentsCmd(v, newRunner))
	cmd.AddCommand(newTapCmd(v, newRunner))
	cmd.AddCommand(newVersionCmd())

	if err := bindCommandTreeFlags(v, cmd); err != nil {
		panic(err)
	}
	return cmd
}

func setChangedFlagsInContext(cmd *cobra.Command) {
	changed := map[string]bool{}
	cmd.InheritedFlags().Visit(func(f *pflag.Flag) { changed[f.Name] = true })
	cmd.Flags().Visit(func(f *pflag.Flag) { changed[f.Name] = true })
	cmd.SetContext(withCLIChangedFlags(cmd.Context(), changed))
}

func applyViperToCommand(v *viper.Viper, cmd *cobra.Command) {
	applyViperToFlagSet(v, cmd.InheritedFlags())
	applyViperToFlagSet(v, cmd.Flags())
}

//nolint:cyclop
func applyViperToFlagSet(v *viper.Viper, fs *pflag.FlagSet) {
	if fs == nil {
		return
	}
	fs.VisitAll(func(f *pflag.Flag) {
		if f.Changed || !v.IsSet(f.Name) {
			return
		}

		switch f.Value.Type() {
		case "stringSlice":
			items, ok := toStringSlice(v.Get(f.Name))
			if !ok {
				return
			}
			if err := fs.Set(f.Name, strings.Join(items, ",")); err != nil {
				log.Fatalf("unable to set flag %q from viper: %v", f.Name, err)
			}
			return
		case "bool":
			if err := fs.Set(f.Name, strconv.FormatBool(v.GetBool(f.Name))); err != nil {
				log.Fatalf("unable to set flag %q from viper: %v", f.Name, err)
			}
			return
		case "int":
			if err := fs.Set(f.Name, strconv.Itoa(v.GetInt(f.Name))); err != nil {
				log.Fatalf("unable to set flag %q from viper: %v", f.Name, err)
			}
			return
		}

		if err := fs.Set(f.Name, fmt.Sprint(v.Get(f.Name))); err != nil {
			log.Fatalf("unable to set flag %q from viper: %v", f.Name, err)
		}
	})
}

func toStringSlice(v any) ([]string, bool) {
	switch vv := v.(type) {
	case []string:
		return vv, true
	case []any:
		out := make([]string, 0, len(vv))
		for _, item := range vv {
			s, ok := item.(string)
			if !ok {
				return nil, false
			}
			out = append(out, s)
		}
		return out, true
	default:
		return nil, false
	}
}
