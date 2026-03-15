package render

import (
	"bytes"
	"testing"

	"github.com/grepplabs/vectap/internal/output"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestYAMLRendererRenderWithoutMeta(t *testing.T) {
	var b bytes.Buffer
	r := NewYAMLRenderer(&b, false)

	err := r.Render(output.Event{
		ComponentID: "source.logs",
		Kind:        "source",
		Message:     `{"message":"hello","count":1}`,
		Meta: map[string]string{
			"component_type": "file",
		},
	})
	require.NoError(t, err)
	require.Contains(t, b.String(), "---\n")

	var got map[string]any
	require.NoError(t, yaml.Unmarshal(bytes.TrimPrefix(b.Bytes(), []byte("---\n")), &got))
	require.Equal(t, "hello", got["message"])
	require.Equal(t, 1, got["count"])
	_, hasComponentID := got["component_id"]
	require.False(t, hasComponentID)
}

func TestYAMLRendererRenderWithMeta(t *testing.T) {
	var b bytes.Buffer
	r := NewYAMLRenderer(&b, true)

	err := r.Render(output.Event{
		ComponentID: "source.logs",
		Kind:        "source",
		Message:     `{"message":"hello"}`,
		Raw:         []byte(`{"raw":"hello"}`),
		Meta: map[string]string{
			"component_type": "file",
		},
	})
	require.NoError(t, err)
	require.Contains(t, b.String(), "---\n")

	var got map[string]any
	require.NoError(t, yaml.Unmarshal(bytes.TrimPrefix(b.Bytes(), []byte("---\n")), &got))
	require.Equal(t, "source.logs", got["component_id"])
	require.Equal(t, "source", got["kind"])
	require.Equal(t, `{"raw":"hello"}`, got["raw"])
	meta, ok := got["meta"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "file", meta["component_type"])
}

func TestYAMLRendererSeparatesDocuments(t *testing.T) {
	var b bytes.Buffer
	r := NewYAMLRenderer(&b, false)

	require.NoError(t, r.Render(output.Event{Message: `{"message":"one"}`}))
	require.NoError(t, r.Render(output.Event{Message: `{"message":"two"}`}))

	require.Equal(t, 2, bytes.Count(b.Bytes(), []byte("---\n")))
}
