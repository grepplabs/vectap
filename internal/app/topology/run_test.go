package topology

import (
	"bytes"
	"testing"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/vectorapi"
	"github.com/stretchr/testify/require"
)

func TestRenderUsesTabwriterForTextFormat(t *testing.T) {
	var buf bytes.Buffer
	err := render(&buf, runconfig.FormatText, ViewTable, true, []ListedTopology{
		{
			Namespace:     "ns-a",
			PodName:       "vector-0",
			ComponentID:   "generate_syslog",
			ComponentKind: "source",
			ComponentType: "demo_logs",
			Children:      []string{"remap_syslog"},
			Outputs:       []string{"default"},
		},
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "TARGET")
	require.Contains(t, buf.String(), "ns-a/vector-0  generate_syslog")
}

func TestRenderOmitsMetaWhenDisabled(t *testing.T) {
	var buf bytes.Buffer
	err := render(&buf, runconfig.FormatText, ViewTable, false, []ListedTopology{
		{
			Namespace:     "ns-a",
			PodName:       "vector-0",
			ComponentID:   "generate_syslog",
			ComponentKind: "source",
			ComponentType: "demo_logs",
		},
	})
	require.NoError(t, err)
	require.NotContains(t, buf.String(), "TARGET")
	require.Contains(t, buf.String(), "COMPONENT_ID")
	require.Contains(t, buf.String(), "generate_syslog")
	require.NotContains(t, buf.String(), "ns-a/vector-0")
}

func TestRenderEdgesView(t *testing.T) {
	f := func(v float64) *float64 { return &v }

	var buf bytes.Buffer
	err := render(&buf, runconfig.FormatText, ViewEdges, false, []ListedTopology{
		{
			ComponentID:     "source.in",
			ComponentKind:   "source",
			ComponentType:   "opentelemetry",
			SentBytesTotal:  f(900),
			SentEventsTotal: f(123),
			Transforms:      []string{"filter.auth", "route.metrics"},
			Children:        []string{"filter.auth", "route.metrics"},
		},
		{
			ComponentID:         "filter.auth",
			ComponentKind:       "transform",
			ComponentType:       "filter",
			ReceivedBytesTotal:  f(800),
			ReceivedEventsTotal: f(120),
			Sinks:               []string{"sink.elasticsearch"},
			Children:            []string{"sink.elasticsearch"},
		},
		{
			ComponentID:   "route.metrics",
			ComponentKind: "transform",
			ComponentType: "exclusive_route",
			Sinks:         []string{"sink.prometheus"},
			Children:      []string{"sink.prometheus"},
		},
		{ComponentID: "sink.elasticsearch", ComponentKind: "sink", ComponentType: "elasticsearch"},
		{ComponentID: "sink.prometheus", ComponentKind: "sink", ComponentType: "prometheus_exporter"},
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "SRC")
	require.Contains(t, buf.String(), "SRC_TYPE")
	require.Contains(t, buf.String(), "SRC_SENT_EVENTS")
	require.Contains(t, buf.String(), "SRC_SENT_BYTES")
	require.Contains(t, buf.String(), "DST_KIND")
	require.Contains(t, buf.String(), "DST_TYPE")
	require.Contains(t, buf.String(), "DST_RECV_EVENTS")
	require.Contains(t, buf.String(), "DST_RECV_BYTES")
	require.Contains(t, buf.String(), "source.in")
	require.Contains(t, buf.String(), "filter.auth")
	require.Contains(t, buf.String(), "filter")
	require.Contains(t, buf.String(), "123")
	require.Contains(t, buf.String(), "120")
	require.Contains(t, buf.String(), "900")
	require.Contains(t, buf.String(), "800")
	require.Contains(t, buf.String(), "sink.elasticsearch")
	require.Contains(t, buf.String(), "transform")
	require.Contains(t, buf.String(), "sink")
}

func TestRenderTreeView(t *testing.T) {
	f := func(v float64) *float64 { return &v }

	var buf bytes.Buffer
	err := render(&buf, runconfig.FormatText, ViewTree, false, []ListedTopology{
		{ComponentID: "source.in", SentBytesTotal: f(500), SentEventsTotal: f(50), Children: []string{"filter.auth", "route.metrics"}},
		{ComponentID: "filter.auth", ReceivedBytesTotal: f(450), ReceivedEventsTotal: f(45), SentEventsTotal: f(42), Children: []string{"sink.elasticsearch"}},
		{ComponentID: "route.metrics", Children: []string{"sink.prometheus"}},
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "source.in")
	require.Contains(t, buf.String(), "[recv_events=- sent_events=50 recv_bytes=- sent_bytes=500]")
	require.Contains(t, buf.String(), "|- filter.auth")
	require.Contains(t, buf.String(), "[recv_events=45 sent_events=42 recv_bytes=450 sent_bytes=-]")
	require.Contains(t, buf.String(), "`- route.metrics")
	require.Contains(t, buf.String(), "sink.elasticsearch")
}

func TestStripMeta(t *testing.T) {
	out := stripMeta([]ListedTopology{
		{
			TargetID:      "t-1",
			Namespace:     "ns-a",
			PodName:       "vector-0",
			ComponentID:   "generate_syslog",
			ComponentKind: "source",
			ComponentType: "demo_logs",
		},
	}, false)
	require.Len(t, out, 1)
	require.Empty(t, out[0].TargetID)
	require.Empty(t, out[0].Namespace)
	require.Empty(t, out[0].PodName)
	require.Equal(t, "generate_syslog", out[0].ComponentID)
}

func TestToListedTopologyInfersReverseLinks(t *testing.T) {
	listed := toListedTopology(
		directTarget(0, "http://127.0.0.1:8686/graphql"),
		"",
		[]vectorapi.TopologyComponent{
			{
				ComponentID:   "otlp_in",
				ComponentKind: "source",
				ComponentType: "opentelemetry",
			},
			{
				ComponentID:   "attributor_logs",
				ComponentKind: "transform",
				ComponentType: "remap",
				Sources: []vectorapi.TopologyComponentRef{
					{ComponentID: "otlp_in", ComponentType: "opentelemetry"},
				},
			},
		},
	)

	require.Len(t, listed, 2)
	var src, tr *ListedTopology
	for i := range listed {
		switch listed[i].ComponentID {
		case "otlp_in":
			src = &listed[i]
		case "attributor_logs":
			tr = &listed[i]
		}
	}
	require.NotNil(t, src)
	require.NotNil(t, tr)
	require.Contains(t, src.Children, "attributor_logs")
	require.Contains(t, tr.Parents, "otlp_in")
}

func TestOnlyOrphaned(t *testing.T) {
	in := []ListedTopology{
		{ComponentID: "a"},
		{ComponentID: "b", Parents: []string{"a"}},
		{ComponentID: "c", Children: []string{"d"}},
	}
	got := onlyOrphaned(in)
	require.Len(t, got, 1)
	require.Equal(t, "a", got[0].ComponentID)
}
