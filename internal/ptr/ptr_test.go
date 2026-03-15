package ptr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTo(t *testing.T) {
	require.Equal(t, "value", *To("value"))
}

func TestDefault(t *testing.T) {
	require.Equal(t, "fallback", Default("", "fallback"))
	require.Equal(t, "value", Default("value", "fallback"))
}

func TestDeref(t *testing.T) {
	require.Equal(t, true, Deref[bool](nil, true))

	value := false
	require.Equal(t, false, Deref(&value, true))
}
