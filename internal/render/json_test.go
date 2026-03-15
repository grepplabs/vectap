package render

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/grepplabs/vectap/internal/output"
	"github.com/stretchr/testify/require"
)

func TestJSONRendererRenderWithoutMeta(t *testing.T) {
	var b bytes.Buffer
	r := NewJSONRenderer(&b, false)

	err := r.Render(output.Event{
		ComponentID: "source.logs",
		Kind:        "source",
		Message:     `{"message":"hello","count":1}`,
		Meta: map[string]string{
			"component_type": "file",
		},
	})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(b.Bytes(), &got))
	require.Equal(t, "hello", got["message"])
	require.EqualValues(t, 1, got["count"])
	_, hasComponentID := got["component_id"]
	require.False(t, hasComponentID)
}

func TestJSONRendererRenderWithMeta(t *testing.T) {
	var b bytes.Buffer
	r := NewJSONRenderer(&b, true)

	err := r.Render(output.Event{
		ComponentID: "source.logs",
		Kind:        "source",
		Message:     `{"message":"hello"}`,
		Meta: map[string]string{
			"component_type": "file",
		},
	})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(b.Bytes(), &got))
	require.Equal(t, "source.logs", got["component_id"])
	require.Equal(t, "source", got["kind"])
	meta, ok := got["meta"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "file", meta["component_type"])
}
