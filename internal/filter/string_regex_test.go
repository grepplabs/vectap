package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringRegexMatcher(t *testing.T) {
	m, err := NewStringRegexMatcher([]string{"^dest-.*"}, []string{"^dest-drop$"})
	require.NoError(t, err)

	require.True(t, m.MatchInclude("dest-1"))
	require.False(t, m.MatchInclude("prom"))
	require.True(t, m.MatchExclude("dest-drop"))
	require.False(t, m.MatchExclude("dest-1"))
}

func TestStringRegexMatcherInvalid(t *testing.T) {
	_, err := NewStringRegexMatcher([]string{"["}, nil)
	require.Error(t, err)
	_, err = NewStringRegexMatcher(nil, []string{"["})
	require.Error(t, err)
}
