package filter

import (
	"testing"

	"github.com/grepplabs/vectap/internal/output"
	"github.com/stretchr/testify/require"
)

func TestTagFieldsMatcherAllowNoRules(t *testing.T) {
	m, err := NewTagFieldsMatcher(TagFieldRules{}, TagFieldRules{}, TagFieldRules{}, TagFieldRules{})
	require.NoError(t, err)
	require.True(t, m.Allow(output.Event{Message: `{"tags":{"component_id":"dest-1"}}`}))
}

func TestTagFieldsMatcherComponentIDRules(t *testing.T) {
	m, err := NewTagFieldsMatcher(
		TagFieldRules{IncludeGlob: []string{"dest-*"}, ExcludeRE: []string{"^dest-drop$"}},
		TagFieldRules{},
		TagFieldRules{},
		TagFieldRules{},
	)
	require.NoError(t, err)
	require.True(t, m.Allow(output.Event{Message: `{"tags":{"component_id":"dest-1"}}`}))
	require.False(t, m.Allow(output.Event{Message: `{"tags":{"component_id":"dest-drop"}}`}))
}

func TestTagFieldsMatcherComponentKindTypeHostRules(t *testing.T) {
	m, err := NewTagFieldsMatcher(
		TagFieldRules{},
		TagFieldRules{IncludeRE: []string{"^sink$"}},
		TagFieldRules{IncludeGlob: []string{"aws_*"}},
		TagFieldRules{ExcludeGlob: []string{"vector-1"}},
	)
	require.NoError(t, err)

	ev := output.Event{Message: `{"tags":{"component_kind":"sink","component_type":"aws_s3","host":"vector-0"}}`}
	require.True(t, m.Allow(ev))
	blockedHost := output.Event{Message: `{"tags":{"component_kind":"sink","component_type":"aws_s3","host":"vector-1"}}`}
	require.False(t, m.Allow(blockedHost))
}

func TestTagFieldsMatcherInvalidRegex(t *testing.T) {
	_, err := NewTagFieldsMatcher(TagFieldRules{IncludeRE: []string{"["}}, TagFieldRules{}, TagFieldRules{}, TagFieldRules{})
	require.Error(t, err)
}
