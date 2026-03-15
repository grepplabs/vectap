package cli

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/spf13/cobra"
)

const (
	Version = "0.0.0-dev"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print vectap version",
		RunE: func(_ *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(os.Stdout, "vectap (version: %s)\n", GetBuildVersion())
			return err
		},
	}
}

func GetBuildVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return Version
}
