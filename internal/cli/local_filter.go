package cli

import (
	"fmt"
	"strings"

	tap "github.com/grepplabs/vectap/internal/app/tap"
)

//nolint:cyclop
func parseLocalFilters(filters []string) (tap.LocalFilters, error) {
	out := tap.LocalFilters{}
	for _, raw := range filters {
		rule := strings.TrimSpace(raw)
		if rule == "" {
			continue
		}
		op := byte('+')
		body := rule
		if rule[0] == '+' || rule[0] == '-' {
			op = rule[0]
			body = strings.TrimSpace(rule[1:])
			if body == "" {
				return tap.LocalFilters{}, fmt.Errorf("invalid local-filter %q: missing rule body", raw)
			}
		}

		isRegex := strings.HasPrefix(body, "re:")
		if isRegex {
			body = strings.TrimPrefix(body, "re:")
		}
		parts := strings.SplitN(body, ":", 2)
		if len(parts) != 2 {
			return tap.LocalFilters{}, fmt.Errorf("invalid local-filter %q: expected '<op><field>:<pattern>' or '<op>re:<field>:<regex>'", raw)
		}
		field := strings.TrimSpace(parts[0])
		pattern := strings.TrimSpace(parts[1])
		if field == "" || pattern == "" {
			return tap.LocalFilters{}, fmt.Errorf("invalid local-filter %q: field and pattern are required", raw)
		}

		target, ok := localFilterTarget(&out, field)
		if !ok {
			return tap.LocalFilters{}, fmt.Errorf("invalid local-filter %q: unsupported field %q", raw, field)
		}
		addLocalFilterPattern(target, op == '+', isRegex, pattern)
	}
	return out, nil
}

func localFilterTarget(filters *tap.LocalFilters, field string) (*tap.LocalFilterRules, bool) {
	switch field {
	case "component.type":
		return &filters.ComponentType, true
	case "component.kind":
		return &filters.ComponentKind, true
	case "name":
		return &filters.Name, true
	case "tags.component_id":
		return &filters.TagComponentID, true
	case "tags.component_kind":
		return &filters.TagComponentKind, true
	case "tags.component_type":
		return &filters.TagComponentType, true
	case "tags.host":
		return &filters.TagHost, true
	default:
		return nil, false
	}
}

func addLocalFilterPattern(target *tap.LocalFilterRules, include, isRegex bool, pattern string) {
	switch {
	case include && isRegex:
		target.IncludeRE = append(target.IncludeRE, pattern)
	case include:
		target.IncludeGlob = append(target.IncludeGlob, pattern)
	case isRegex:
		target.ExcludeRE = append(target.ExcludeRE, pattern)
	default:
		target.ExcludeGlob = append(target.ExcludeGlob, pattern)
	}
}
