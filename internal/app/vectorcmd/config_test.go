package vectorcmd

import (
	"testing"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/stretchr/testify/require"
)

func TestConfigValidateSuccess(t *testing.T) {
	cfg := Config{
		BaseConfig: runconfig.BaseConfig{
			Type:       runconfig.SourceTypeDirect,
			DirectURLs: []string{runconfig.DefaultDirectURL},
			Namespace:  runconfig.DefaultNamespace,
			Format:     runconfig.FormatText,
			VectorPort: runconfig.DefaultVectorPort,
		},
		Mode:      ModeTap,
		TapLayout: TapLayoutMerged,
		VectorBin: "vector",
	}
	require.NoError(t, cfg.Validate())
}

func TestConfigValidateErrors(t *testing.T) {
	cfg := Config{
		BaseConfig: runconfig.BaseConfig{
			Type:       runconfig.SourceTypeDirect,
			DirectURLs: []string{runconfig.DefaultDirectURL},
			Namespace:  runconfig.DefaultNamespace,
			Format:     runconfig.FormatText,
			VectorPort: runconfig.DefaultVectorPort,
		},
		Mode:      "invalid",
		VectorBin: "vector",
	}
	require.Error(t, cfg.Validate())

	cfg.Mode = ModeTop
	cfg.VectorBin = ""
	require.Error(t, cfg.Validate())

	cfg.Mode = ModeTap
	cfg.VectorBin = "vector"
	cfg.TapLayout = "bad"
	require.Error(t, cfg.Validate())
}
