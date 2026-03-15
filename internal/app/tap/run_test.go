package tap

import (
	"testing"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/stretchr/testify/require"
)

func testTapConfig(common runconfig.BaseConfig, apply ...func(*Config)) Config {
	cfg := Config{BaseConfig: common}
	for _, fn := range apply {
		fn(&cfg)
	}
	return cfg
}

func TestNewDefaultRunnerCreatesRunnerWithoutEagerKubeInit(t *testing.T) {
	t.Setenv("KUBECONFIG", "/path/that/does/not/exist")

	r := NewDefaultRunner()
	require.NotNil(t, r)
}

func TestIncludeMetaForRender(t *testing.T) {
	t.Run("uses top-level include meta", func(t *testing.T) {
		got := includeMetaForRender(testTapConfig(runconfig.BaseConfig{IncludeMeta: true}), nil)
		require.True(t, got)
	})

	t.Run("uses source include meta when top-level is false", func(t *testing.T) {
		got := includeMetaForRender(
			testTapConfig(runconfig.BaseConfig{IncludeMeta: false}),
			[]Config{
				testTapConfig(runconfig.BaseConfig{IncludeMeta: false}),
				testTapConfig(runconfig.BaseConfig{IncludeMeta: true}),
			},
		)
		require.True(t, got)
	})

	t.Run("stays false when disabled everywhere", func(t *testing.T) {
		got := includeMetaForRender(
			testTapConfig(runconfig.BaseConfig{IncludeMeta: false}),
			[]Config{testTapConfig(runconfig.BaseConfig{IncludeMeta: false})},
		)
		require.False(t, got)
	})

	t.Run("honors source configs when running configured sources", func(t *testing.T) {
		got := includeMetaForRender(
			testTapConfig(runconfig.BaseConfig{IncludeMeta: true, AllSources: true}),
			[]Config{
				testTapConfig(runconfig.BaseConfig{IncludeMeta: false}),
				testTapConfig(runconfig.BaseConfig{IncludeMeta: false}),
			},
		)
		require.False(t, got)
	})
}

func TestFormatForRender(t *testing.T) {
	t.Run("uses top-level format for direct run", func(t *testing.T) {
		got, err := formatForRender(
			testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatJSON}),
			[]Config{testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatJSON})},
		)
		require.NoError(t, err)
		require.Equal(t, runconfig.FormatJSON, got)
	})

	t.Run("uses selected source format", func(t *testing.T) {
		got, err := formatForRender(
			testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatText, SelectedSources: []string{"a"}}),
			[]Config{testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatJSON})},
		)
		require.NoError(t, err)
		require.Equal(t, runconfig.FormatJSON, got)
	})

	t.Run("rejects mixed source formats", func(t *testing.T) {
		_, err := formatForRender(
			testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatText, AllSources: true}),
			[]Config{
				testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatJSON}),
				testTapConfig(runconfig.BaseConfig{Format: runconfig.FormatText}),
			},
		)
		require.Error(t, err)
	})
}
