package filter

import (
	"path"
	"strings"
)

// ByGlobPatterns returns a matcher for glob patterns.
// Empty patterns mean "match everything".
func ByGlobPatterns(patterns []string) func(string) bool {
	if len(patterns) == 0 {
		return func(string) bool { return true }
	}

	exact := make(map[string]struct{}, len(patterns))
	globs := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if strings.ContainsAny(p, "*?[") {
			globs = append(globs, p)
			continue
		}
		exact[p] = struct{}{}
	}

	return func(value string) bool {
		if _, ok := exact[value]; ok {
			return true
		}

		for _, g := range globs {
			matched, err := path.Match(g, value)
			if err != nil {
				// Invalid glob pattern: fall back to literal matching behavior.
				if g == value {
					return true
				}
				continue
			}
			if matched {
				return true
			}
		}

		return false
	}
}
