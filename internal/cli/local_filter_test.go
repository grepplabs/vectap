package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseLocalFilters(t *testing.T) {
	filters, err := parseLocalFilters([]string{
		"component.type:prometheus_*",
		"-component.kind:source",
		"+re:name:^component_.*$",
		"-re:tags.component_id:^debug-.*$",
		"tags.host:vector-*",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"prometheus_*"}, filters.ComponentType.IncludeGlob)
	require.Equal(t, []string{"source"}, filters.ComponentKind.ExcludeGlob)
	require.Equal(t, []string{"^component_.*$"}, filters.Name.IncludeRE)
	require.Equal(t, []string{"^debug-.*$"}, filters.TagComponentID.ExcludeRE)
	require.Equal(t, []string{"vector-*"}, filters.TagHost.IncludeGlob)
}

func TestParseLocalFiltersErrors(t *testing.T) {
	tests := []string{
		"+component.type",
		"+re:component.type",
		"+unknown.field:x",
		"+component.type:",
	}
	for _, tc := range tests {
		_, err := parseLocalFilters([]string{tc})
		require.Error(t, err, tc)
	}
}

func TestParseLocalFiltersDefaultsToInclude(t *testing.T) {
	filters, err := parseLocalFilters([]string{"component.type:new"})
	require.NoError(t, err)
	require.Equal(t, []string{"new"}, filters.ComponentType.IncludeGlob)
}
