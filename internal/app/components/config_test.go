package components

import (
	"testing"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/stretchr/testify/require"
)

func testConfig(common runconfig.BaseConfig) Config {
	return Config{BaseConfig: common}
}

func testSource(common runconfig.BaseSourceConfig) SourceConfig {
	return SourceConfig{BaseSourceConfig: common}
}

func TestConfigValidateSuccess(t *testing.T) {
	for _, format := range []string{runconfig.FormatText, runconfig.FormatJSON, runconfig.FormatYAML} {
		cfg := testConfig(runconfig.BaseConfig{
			Type:       runconfig.SourceTypeDirect,
			DirectURLs: []string{runconfig.DefaultDirectURL},
			Namespace:  runconfig.DefaultNamespace,
			Format:     format,
			VectorPort: runconfig.DefaultVectorPort,
		})
		require.NoError(t, cfg.Validate())
	}
}

func TestSourceConfigValidateErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  SourceConfig
	}{
		{
			name: "missing name",
			cfg: testSource(runconfig.BaseSourceConfig{
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				VectorPort: runconfig.DefaultVectorPort,
			}),
		},
		{
			name: "invalid type",
			cfg: testSource(runconfig.BaseSourceConfig{
				Name:       "src-a",
				Type:       "invalid",
				DirectURLs: []string{runconfig.DefaultDirectURL},
				VectorPort: runconfig.DefaultVectorPort,
			}),
		},
		{
			name: "direct without url",
			cfg: testSource(runconfig.BaseSourceConfig{
				Name:       "src-a",
				Type:       runconfig.SourceTypeDirect,
				VectorPort: runconfig.DefaultVectorPort,
			}),
		},
		{
			name: "invalid format",
			cfg: testSource(runconfig.BaseSourceConfig{
				Name:       "src-a",
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				Format:     "xml",
				VectorPort: runconfig.DefaultVectorPort,
			}),
		},
		{
			name: "invalid vector port",
			cfg: testSource(runconfig.BaseSourceConfig{
				Name:       "src-a",
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				VectorPort: 0,
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.cfg.Validate())
		})
	}
}
