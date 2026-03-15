package tap

import (
	"testing"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/stretchr/testify/require"
)

func tapConfig(common runconfig.BaseConfig, interval, limit int, sources ...SourceConfig) Config {
	return Config{
		BaseConfig: common,
		Sources:    sources,
		Interval:   interval,
		Limit:      limit,
	}
}

func sourceConfig(common runconfig.BaseSourceConfig, interval, limit int) SourceConfig {
	return SourceConfig{
		BaseSourceConfig: common,
		Interval:         interval,
		Limit:            limit,
	}
}

func TestConfigValidateSuccess(t *testing.T) {
	for _, format := range []string{runconfig.FormatText, runconfig.FormatJSON, runconfig.FormatYAML} {
		cfg := tapConfig(runconfig.BaseConfig{
			Type:       runconfig.SourceTypeDirect,
			DirectURLs: []string{runconfig.DefaultDirectURL},
			Namespace:  runconfig.DefaultNamespace,
			Format:     format,
			VectorPort: runconfig.DefaultVectorPort,
		}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit)
		require.NoError(t, cfg.Validate())
	}
}

func TestConfigValidateErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "invalid type",
			cfg: tapConfig(runconfig.BaseConfig{
				Type:       "invalid",
				DirectURLs: []string{runconfig.DefaultDirectURL},
				Namespace:  runconfig.DefaultNamespace,
				Format:     runconfig.FormatText,
				VectorPort: runconfig.DefaultVectorPort,
			}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
		},
		{
			name: "direct without url",
			cfg: tapConfig(runconfig.BaseConfig{
				Type:       runconfig.SourceTypeDirect,
				Namespace:  runconfig.DefaultNamespace,
				Format:     runconfig.FormatText,
				VectorPort: runconfig.DefaultVectorPort,
			}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
		},
		{
			name: "missing namespace",
			cfg: tapConfig(runconfig.BaseConfig{
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				Format:     runconfig.FormatText,
				VectorPort: runconfig.DefaultVectorPort,
			}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
		},
		{
			name: "invalid format",
			cfg: tapConfig(runconfig.BaseConfig{
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				Namespace:  runconfig.DefaultNamespace,
				Format:     "xml",
				VectorPort: runconfig.DefaultVectorPort,
			}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
		},
		{
			name: "invalid vector port",
			cfg: tapConfig(runconfig.BaseConfig{
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				Namespace:  runconfig.DefaultNamespace,
				Format:     runconfig.FormatText,
				VectorPort: 70000,
			}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
		},
		{
			name: "invalid interval",
			cfg: tapConfig(runconfig.BaseConfig{
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				Namespace:  runconfig.DefaultNamespace,
				Format:     runconfig.FormatText,
				VectorPort: runconfig.DefaultVectorPort,
			}, 0, runconfig.DefaultTapLimit),
		},
		{
			name: "invalid limit",
			cfg: tapConfig(runconfig.BaseConfig{
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				Namespace:  runconfig.DefaultNamespace,
				Format:     runconfig.FormatText,
				VectorPort: runconfig.DefaultVectorPort,
			}, runconfig.DefaultTapInterval, 0),
		},
		{
			name: "source without name",
			cfg: tapConfig(
				runconfig.BaseConfig{
					Type:       runconfig.SourceTypeDirect,
					DirectURLs: []string{runconfig.DefaultDirectURL},
					Namespace:  runconfig.DefaultNamespace,
					Format:     runconfig.FormatText,
					VectorPort: runconfig.DefaultVectorPort,
				},
				runconfig.DefaultTapInterval,
				runconfig.DefaultTapLimit,
				sourceConfig(runconfig.BaseSourceConfig{
					Type:       runconfig.SourceTypeDirect,
					DirectURLs: []string{runconfig.DefaultDirectURL},
					VectorPort: runconfig.DefaultVectorPort,
				}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
			),
		},
		{
			name: "source direct without url",
			cfg: tapConfig(
				runconfig.BaseConfig{
					Type:       runconfig.SourceTypeDirect,
					DirectURLs: []string{runconfig.DefaultDirectURL},
					Namespace:  runconfig.DefaultNamespace,
					Format:     runconfig.FormatText,
					VectorPort: runconfig.DefaultVectorPort,
				},
				runconfig.DefaultTapInterval,
				runconfig.DefaultTapLimit,
				sourceConfig(runconfig.BaseSourceConfig{
					Name:       "src-a",
					Type:       runconfig.SourceTypeDirect,
					VectorPort: runconfig.DefaultVectorPort,
				}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
			),
		},
		{
			name: "source invalid vector port",
			cfg: tapConfig(
				runconfig.BaseConfig{
					Type:       runconfig.SourceTypeDirect,
					DirectURLs: []string{runconfig.DefaultDirectURL},
					Namespace:  runconfig.DefaultNamespace,
					Format:     runconfig.FormatText,
					VectorPort: runconfig.DefaultVectorPort,
				},
				runconfig.DefaultTapInterval,
				runconfig.DefaultTapLimit,
				sourceConfig(runconfig.BaseSourceConfig{
					Name:       "src-a",
					Type:       runconfig.SourceTypeDirect,
					DirectURLs: []string{runconfig.DefaultDirectURL},
					VectorPort: 0,
				}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
			),
		},
		{
			name: "source invalid interval",
			cfg: tapConfig(
				runconfig.BaseConfig{
					Type:       runconfig.SourceTypeDirect,
					DirectURLs: []string{runconfig.DefaultDirectURL},
					Namespace:  runconfig.DefaultNamespace,
					Format:     runconfig.FormatText,
					VectorPort: runconfig.DefaultVectorPort,
				},
				runconfig.DefaultTapInterval,
				runconfig.DefaultTapLimit,
				sourceConfig(runconfig.BaseSourceConfig{
					Name:       "src-a",
					Type:       runconfig.SourceTypeDirect,
					DirectURLs: []string{runconfig.DefaultDirectURL},
					VectorPort: runconfig.DefaultVectorPort,
				}, 0, runconfig.DefaultTapLimit),
			),
		},
		{
			name: "source invalid limit",
			cfg: tapConfig(
				runconfig.BaseConfig{
					Type:       runconfig.SourceTypeDirect,
					DirectURLs: []string{runconfig.DefaultDirectURL},
					Namespace:  runconfig.DefaultNamespace,
					Format:     runconfig.FormatText,
					VectorPort: runconfig.DefaultVectorPort,
				},
				runconfig.DefaultTapInterval,
				runconfig.DefaultTapLimit,
				sourceConfig(runconfig.BaseSourceConfig{
					Name:       "src-a",
					Type:       runconfig.SourceTypeDirect,
					DirectURLs: []string{runconfig.DefaultDirectURL},
					VectorPort: runconfig.DefaultVectorPort,
				}, runconfig.DefaultTapInterval, 0),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.cfg.Validate())
		})
	}
}

func TestSourceConfigValidateErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  SourceConfig
	}{
		{
			name: "missing name",
			cfg: sourceConfig(runconfig.BaseSourceConfig{
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				VectorPort: runconfig.DefaultVectorPort,
			}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
		},
		{
			name: "invalid type",
			cfg: sourceConfig(runconfig.BaseSourceConfig{
				Name:       "src-a",
				Type:       "invalid",
				DirectURLs: []string{runconfig.DefaultDirectURL},
				VectorPort: runconfig.DefaultVectorPort,
			}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
		},
		{
			name: "direct without url",
			cfg: sourceConfig(runconfig.BaseSourceConfig{
				Name:       "src-a",
				Type:       runconfig.SourceTypeDirect,
				VectorPort: runconfig.DefaultVectorPort,
			}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
		},
		{
			name: "invalid vector port",
			cfg: sourceConfig(runconfig.BaseSourceConfig{
				Name:       "src-a",
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				VectorPort: 0,
			}, runconfig.DefaultTapInterval, runconfig.DefaultTapLimit),
		},
		{
			name: "invalid interval",
			cfg: sourceConfig(runconfig.BaseSourceConfig{
				Name:       "src-a",
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				VectorPort: runconfig.DefaultVectorPort,
			}, 0, runconfig.DefaultTapLimit),
		},
		{
			name: "invalid limit",
			cfg: sourceConfig(runconfig.BaseSourceConfig{
				Name:       "src-a",
				Type:       runconfig.SourceTypeDirect,
				DirectURLs: []string{runconfig.DefaultDirectURL},
				VectorPort: runconfig.DefaultVectorPort,
			}, runconfig.DefaultTapInterval, 0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.cfg.Validate())
		})
	}
}

func TestLocalFiltersValidateError(t *testing.T) {
	err := LocalFilters{
		Name: LocalFilterRules{
			IncludeRE: []string{"("},
		},
	}.Validate()

	require.Error(t, err)
	require.Contains(t, err.Error(), `invalid local-filter include name regex "("`)
}
