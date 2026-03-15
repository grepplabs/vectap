package filter

import (
	"encoding/json"
	"fmt"

	"github.com/grepplabs/vectap/internal/output"
)

type TagFieldRules struct {
	IncludeGlob []string
	ExcludeGlob []string
	IncludeRE   []string
	ExcludeRE   []string
}

type TagFieldsMatcher struct {
	componentID   tagValueMatcher
	componentKind tagValueMatcher
	componentType tagValueMatcher
	host          tagValueMatcher
}

type tagValueMatcher struct {
	m *ValueMatcher
}

func newTagValueMatcher(r TagFieldRules) (tagValueMatcher, error) {
	m, err := NewValueMatcher(ValueRules(r))
	if err != nil {
		return tagValueMatcher{}, err
	}
	return tagValueMatcher{m: m}, nil
}

func (m tagValueMatcher) allow(value string) bool {
	return m.m.Allow(value)
}

func NewTagFieldsMatcher(componentID, componentKind, componentType, host TagFieldRules) (*TagFieldsMatcher, error) {
	idMatcher, err := newTagValueMatcher(componentID)
	if err != nil {
		return nil, fmt.Errorf("component_id: %w", err)
	}
	kindMatcher, err := newTagValueMatcher(componentKind)
	if err != nil {
		return nil, fmt.Errorf("component_kind: %w", err)
	}
	typeMatcher, err := newTagValueMatcher(componentType)
	if err != nil {
		return nil, fmt.Errorf("component_type: %w", err)
	}
	hostMatcher, err := newTagValueMatcher(host)
	if err != nil {
		return nil, fmt.Errorf("host: %w", err)
	}

	return &TagFieldsMatcher{
		componentID:   idMatcher,
		componentKind: kindMatcher,
		componentType: typeMatcher,
		host:          hostMatcher,
	}, nil
}

func (m *TagFieldsMatcher) Allow(ev output.Event) bool {
	tags := extractPayloadTags(ev.Message)
	if !m.componentID.allow(tags["component_id"]) {
		return false
	}
	if !m.componentKind.allow(tags["component_kind"]) {
		return false
	}
	if !m.componentType.allow(tags["component_type"]) {
		return false
	}
	if !m.host.allow(tags["host"]) {
		return false
	}
	return true
}

func (m *TagFieldsMatcher) HasRules() bool {
	return m.componentID.m.HasRules() ||
		m.componentKind.m.HasRules() ||
		m.componentType.m.HasRules() ||
		m.host.m.HasRules()
}

func extractPayloadTags(message string) map[string]string {
	if message == "" {
		return map[string]string{}
	}

	var payload struct {
		Tags map[string]any `json:"tags"`
	}
	if err := json.Unmarshal([]byte(message), &payload); err != nil || payload.Tags == nil {
		return map[string]string{}
	}

	out := make(map[string]string, len(payload.Tags))
	for k, v := range payload.Tags {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}
