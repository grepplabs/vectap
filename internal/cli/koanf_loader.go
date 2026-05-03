package cli

import (
	"fmt"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func loadKoanf(cmd *cobra.Command) (*koanf.Koanf, error) {
	k := koanf.New(".")

	cfgPath, err := cmd.Flags().GetString("config")
	if err != nil {
		return nil, err
	}
	if cfgPath != "" {
		parser, err := parserForFile(cfgPath)
		if err != nil {
			return nil, err
		}
		if err := k.Load(file.Provider(cfgPath), parser); err != nil {
			return nil, fmt.Errorf("failed to read config file %s: %w", cfgPath, err)
		}
	}

	if err := k.Load(env.Provider(".", env.Opt{
		Prefix: "VECTAP_",
		TransformFunc: func(k, v string) (string, any) {
			key := strings.ToLower(strings.TrimPrefix(k, "VECTAP_"))
			return strings.ReplaceAll(key, "_", "-"), v
		},
	}), nil); err != nil {
		return nil, err
	}

	if err := k.Load(posflag.ProviderWithFlag(cmd.Flags(), ".", k, func(f *pflag.Flag) (string, interface{}) {
		if !f.Changed {
			return "", nil
		}
		return f.Name, posflag.FlagVal(cmd.Flags(), f)
	}), nil); err != nil {
		return nil, err
	}
	if err := k.Load(posflag.ProviderWithFlag(cmd.InheritedFlags(), ".", k, func(f *pflag.Flag) (string, interface{}) {
		if !f.Changed {
			return "", nil
		}
		return f.Name, posflag.FlagVal(cmd.InheritedFlags(), f)
	}), nil); err != nil {
		return nil, err
	}

	return k, nil
}

func parserForFile(path string) (koanf.Parser, error) {
	ext := strings.ToLower(path)
	switch {
	case strings.HasSuffix(ext, ".yaml"), strings.HasSuffix(ext, ".yml"):
		return yaml.Parser(), nil
	default:
		return nil, fmt.Errorf("unsupported config file extension for %q", path)
	}
}
