package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractPayloadName(t *testing.T) {
	require.Equal(t, "component_received_events_count", ExtractPayloadName(`{"name":"component_received_events_count"}`))
	require.Equal(t, "component_sent_bytes_total", ExtractPayloadName(`{"message":{"fields":{"name":"component_sent_bytes_total"}}}`))
	require.Empty(t, ExtractPayloadName(`{"x":"y"}`))
	require.Empty(t, ExtractPayloadName(`{`))
	require.Empty(t, ExtractPayloadName(""))
}
