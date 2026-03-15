package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type cliChangedFlagsContextKey struct{}

type cliFlagSetFunc func(name string) bool

func withCLIChangedFlags(ctx context.Context, flags map[string]bool) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, cliChangedFlagsContextKey{}, flags)
}

func cliFlagSetFromContext(ctx context.Context) cliFlagSetFunc {
	if ctx == nil {
		return func(string) bool { return false }
	}
	flags, _ := ctx.Value(cliChangedFlagsContextKey{}).(map[string]bool)
	if len(flags) == 0 {
		return func(string) bool { return false }
	}
	return func(name string) bool { return flags[name] }
}

func bindCommandTreeFlags(v *viper.Viper, cmd *cobra.Command) error {
	applyBindToFlagSet(v, cmd.PersistentFlags())
	applyBindToFlagSet(v, cmd.Flags())

	for _, sub := range cmd.Commands() {
		if err := bindCommandTreeFlags(v, sub); err != nil {
			return err
		}
	}
	return nil
}

func bindActiveCommandFlags(v *viper.Viper, cmd *cobra.Command) error {
	bindFlagSet := func(fs *pflag.FlagSet) error {
		if fs == nil {
			return nil
		}
		var bindErr error
		fs.VisitAll(func(f *pflag.Flag) {
			if bindErr != nil {
				return
			}
			if err := v.BindPFlag(f.Name, f); err != nil {
				bindErr = fmt.Errorf("bind active flag %q: %w", f.Name, err)
			}
		})
		return bindErr
	}

	if err := bindFlagSet(cmd.InheritedFlags()); err != nil {
		return err
	}
	return bindFlagSet(cmd.Flags())
}

func applyBindToFlagSet(v *viper.Viper, fs *pflag.FlagSet) {
	if fs == nil {
		return
	}
	fs.VisitAll(func(f *pflag.Flag) {
		if err := v.BindPFlag(f.Name, f); err != nil {
			panic(fmt.Errorf("bind flag %q: %w", f.Name, err))
		}
	})
}
