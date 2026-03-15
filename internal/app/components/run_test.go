package components

import (
	"bytes"
	"testing"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/stretchr/testify/require"
)

func TestRenderUsesTabwriterForImplicitTextFormat(t *testing.T) {
	var buf bytes.Buffer
	err := render(&buf, runconfig.FormatText, true, []ListedComponent{
		{Namespace: "ns-a", PodName: "vector-0", ComponentID: "generate_syslog", ComponentKind: "source", ComponentType: "demo_logs"},
		{Namespace: "ns-a", PodName: "vector-0", ComponentID: "remap_syslog", ComponentKind: "transform", ComponentType: "remap"},
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "TARGET")
	require.Contains(t, buf.String(), "COMPONENT_ID")
	require.Contains(t, buf.String(), "KIND")
	require.Contains(t, buf.String(), "TYPE")
	require.Contains(t, buf.String(), "ns-a/vector-0  generate_syslog")
	require.Contains(t, buf.String(), "ns-a/vector-0  remap_syslog")
	require.Contains(t, buf.String(), "source")
	require.Contains(t, buf.String(), "transform")
}

func TestRenderUsesTabwriterForTextFormat(t *testing.T) {
	var buf bytes.Buffer
	err := render(&buf, runconfig.FormatText, true, []ListedComponent{
		{Namespace: "ns-a", PodName: "vector-0", ComponentID: "generate_syslog", ComponentKind: "source", ComponentType: "demo_logs"},
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "TARGET")
	require.Contains(t, buf.String(), "ns-a/vector-0  generate_syslog")
}

func TestRenderOmitsMetaWhenDisabled(t *testing.T) {
	var buf bytes.Buffer
	err := render(&buf, runconfig.FormatText, false, []ListedComponent{
		{Namespace: "ns-a", PodName: "vector-0", ComponentID: "generate_syslog", ComponentKind: "source", ComponentType: "demo_logs"},
	})
	require.NoError(t, err)
	require.NotContains(t, buf.String(), "TARGET")
	require.Contains(t, buf.String(), "COMPONENT_ID")
	require.Contains(t, buf.String(), "generate_syslog")
	require.NotContains(t, buf.String(), "ns-a/vector-0")
}

func TestStripMeta(t *testing.T) {
	out := stripMeta([]ListedComponent{
		{TargetID: "t-1", Namespace: "ns-a", PodName: "vector-0", ComponentID: "generate_syslog", ComponentKind: "source", ComponentType: "demo_logs"},
	}, false)
	require.Len(t, out, 1)
	require.Empty(t, out[0].TargetID)
	require.Empty(t, out[0].Namespace)
	require.Empty(t, out[0].PodName)
	require.Equal(t, "generate_syslog", out[0].ComponentID)
}
