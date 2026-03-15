package filter

import (
	"fmt"
	"regexp"
)

type StringRegexMatcher struct {
	include []*regexp.Regexp
	exclude []*regexp.Regexp
}

func NewStringRegexMatcher(includePatterns, excludePatterns []string) (*StringRegexMatcher, error) {
	include, err := compileRegexps(includePatterns)
	if err != nil {
		return nil, fmt.Errorf("compile include regex: %w", err)
	}
	exclude, err := compileRegexps(excludePatterns)
	if err != nil {
		return nil, fmt.Errorf("compile exclude regex: %w", err)
	}
	return &StringRegexMatcher{include: include, exclude: exclude}, nil
}

func (m *StringRegexMatcher) MatchInclude(value string) bool {
	if len(m.include) == 0 {
		return false
	}
	return matchesAnyRegexp(m.include, value)
}

func (m *StringRegexMatcher) MatchExclude(value string) bool {
	if len(m.exclude) == 0 {
		return false
	}
	return matchesAnyRegexp(m.exclude, value)
}

func (m *StringRegexMatcher) HasInclude() bool {
	return len(m.include) > 0
}

func (m *StringRegexMatcher) HasExclude() bool {
	return len(m.exclude) > 0
}
