package runconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testSource struct {
	Name    string
	Enabled bool
}

func TestSelectAllSources(t *testing.T) {
	sources := []testSource{
		{Name: "a", Enabled: true},
		{Name: "b", Enabled: false},
		{Name: "c", Enabled: true},
	}

	got, err := Select(true, nil, sources, func(s testSource) string { return s.Name }, func(s testSource) bool { return s.Enabled })
	require.NoError(t, err)
	require.Equal(t, []testSource{{Name: "a", Enabled: true}, {Name: "c", Enabled: true}}, got)
}

func TestSelectByNameOrder(t *testing.T) {
	sources := []testSource{
		{Name: "a", Enabled: true},
		{Name: "b", Enabled: true},
		{Name: "c", Enabled: true},
	}

	got, err := Select(false, []string{"c", "a"}, sources, func(s testSource) string { return s.Name }, func(s testSource) bool { return s.Enabled })
	require.NoError(t, err)
	require.Equal(t, []testSource{{Name: "c", Enabled: true}, {Name: "a", Enabled: true}}, got)
}

func TestSelectUnknownSource(t *testing.T) {
	sources := []testSource{{Name: "a", Enabled: true}}

	_, err := Select(false, []string{"missing"}, sources, func(s testSource) string { return s.Name }, func(s testSource) bool { return s.Enabled })
	require.EqualError(t, err, `unknown source "missing"`)
}

func TestSelectNoSourcesConfigured(t *testing.T) {
	_, err := Select(true, nil, nil, func(s testSource) string { return s.Name }, func(s testSource) bool { return s.Enabled })
	require.EqualError(t, err, "no sources configured")
}

func TestSelectNoSourcesSelected(t *testing.T) {
	sources := []testSource{
		{Name: "a", Enabled: false},
	}

	_, err := Select(true, nil, sources, func(s testSource) string { return s.Name }, func(s testSource) bool { return s.Enabled })
	require.EqualError(t, err, "no sources selected")
}
