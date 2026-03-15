package filter

type ValueRules struct {
	IncludeGlob []string
	ExcludeGlob []string
	IncludeRE   []string
	ExcludeRE   []string
}

type ValueMatcher struct {
	includeGlob      func(string) bool
	excludeGlob      func(string) bool
	includeGlobCount int
	excludeGlobCount int
	regex            *StringRegexMatcher
}

func NewValueMatcher(r ValueRules) (*ValueMatcher, error) {
	re, err := NewStringRegexMatcher(r.IncludeRE, r.ExcludeRE)
	if err != nil {
		return nil, err
	}
	return &ValueMatcher{
		includeGlob:      ByGlobPatterns(r.IncludeGlob),
		excludeGlob:      ByGlobPatterns(r.ExcludeGlob),
		includeGlobCount: len(r.IncludeGlob),
		excludeGlobCount: len(r.ExcludeGlob),
		regex:            re,
	}, nil
}

func (m *ValueMatcher) Allow(value string) bool {
	hasInclude := m.includeGlobCount > 0 || m.regex.HasInclude()
	if hasInclude {
		included := (m.includeGlobCount > 0 && m.includeGlob(value)) || m.regex.MatchInclude(value)
		if value == "" || !included {
			return false
		}
	}

	if (m.excludeGlobCount > 0 && m.excludeGlob(value)) || m.regex.MatchExclude(value) {
		return false
	}
	return true
}

func (m *ValueMatcher) HasRules() bool {
	return m.includeGlobCount > 0 || m.excludeGlobCount > 0 || m.regex.HasInclude() || m.regex.HasExclude()
}
