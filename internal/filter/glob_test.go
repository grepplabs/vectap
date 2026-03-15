package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByGlobPatternsEmptyMatchesEverything(t *testing.T) {
	match := ByGlobPatterns(nil)

	require.True(t, match("any-value"))
	require.True(t, match(""))
}

func TestByGlobPatternsExactAndGlob(t *testing.T) {
	match := ByGlobPatterns([]string{"debug-view", "prom_*"})

	require.True(t, match("debug-view"))
	require.True(t, match("prom_exporter"))
	require.False(t, match("destination-1"))
}

func TestByGlobPatternsInvalidGlobFallsBackToLiteral(t *testing.T) {
	match := ByGlobPatterns([]string{"["})

	require.True(t, match("["))
	require.False(t, match("prom_exporter"))
}

func TestByGlobPatternsInvalidAndValidPatterns(t *testing.T) {
	match := ByGlobPatterns([]string{"[", "destination-*"})

	require.True(t, match("destination-1"))
	require.True(t, match("["))
	require.False(t, match("internal_metrics"))
}
