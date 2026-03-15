package render

import (
	"bytes"
	"testing"

	"github.com/grepplabs/vectap/internal/output"
	"github.com/stretchr/testify/require"
)

func TestTextRendererRenderWithoutMeta(t *testing.T) {
	var b bytes.Buffer
	r := NewTextRenderer(&b, false, false)

	err := r.Render(output.Event{
		Namespace:   "ns-a",
		PodName:     "pod-a",
		ComponentID: "source.logs",
		Message:     `{"message":"hello"}`,
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"message":"hello"}`+"\n", b.String())
}

func TestTextRendererRenderWithMeta(t *testing.T) {
	var b bytes.Buffer
	r := NewTextRenderer(&b, false, true)

	err := r.Render(output.Event{
		Namespace:   "ns-a",
		PodName:     "pod-a",
		ComponentID: "source.logs",
		Message:     `{"message":"hello"}`,
	})
	require.NoError(t, err)
	require.Equal(t, `ns-a/pod-a source.logs {"message":"hello"}`+"\n", b.String())
}
