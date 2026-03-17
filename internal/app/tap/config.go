package tap

import (
	"fmt"
	"regexp"

	"github.com/grepplabs/vectap/internal/app/runconfig"
)

type Config struct {
	runconfig.BaseConfig
	Sources []SourceConfig
	TapScopeConfig
	LocalFilters LocalFilters
	NoColor      bool
}

type TapScopeConfig struct {
	OutputsOf []string
	InputsOf  []string
	Interval  int
	Limit     int
}

type LocalFilters struct {
	ComponentType    LocalFilterRules
	ComponentKind    LocalFilterRules
	Name             LocalFilterRules
	TagComponentID   LocalFilterRules
	TagComponentKind LocalFilterRules
	TagComponentType LocalFilterRules
	TagHost          LocalFilterRules
}

type LocalFilterRules struct {
	IncludeGlob []string
	ExcludeGlob []string
	IncludeRE   []string
	ExcludeRE   []string
}

type SourceConfig struct {
	runconfig.BaseSourceConfig
	TapScopeConfig
	LocalFilters  LocalFilters
	ApplyDefaults bool
}

func (c Config) Validate() error {
	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}
	if err := runconfig.ValidatePositive("", "interval", c.Interval); err != nil {
		return err
	}
	if err := runconfig.ValidatePositive("", "limit", c.Limit); err != nil {
		return err
	}
	for _, s := range c.Sources {
		if err := s.Validate(); err != nil {
			return err
		}
	}
	if err := c.LocalFilters.Validate(); err != nil {
		return err
	}
	return nil
}

func (s SourceConfig) Validate() error {
	if err := s.BaseSourceConfig.Validate(); err != nil {
		return err
	}
	if err := s.LocalFilters.Validate(); err != nil {
		return fmt.Errorf("source %q: %w", s.Name, err)
	}
	if err := runconfig.ValidatePositive(s.Name, "interval", s.Interval); err != nil {
		return err
	}
	if err := runconfig.ValidatePositive(s.Name, "limit", s.Limit); err != nil {
		return err
	}
	return nil
}

func (f LocalFilters) Validate() error {
	ruleChecks := map[string]LocalFilterRules{
		"component.type":      f.ComponentType,
		"component.kind":      f.ComponentKind,
		"name":                f.Name,
		"tags.component_id":   f.TagComponentID,
		"tags.component_kind": f.TagComponentKind,
		"tags.component_type": f.TagComponentType,
		"tags.host":           f.TagHost,
	}
	for field, rules := range ruleChecks {
		if err := rules.Validate("local-filter include " + field); err != nil {
			return err
		}
		if err := rules.validateExclude("local-filter exclude " + field); err != nil {
			return err
		}
	}
	return nil
}

func (r LocalFilterRules) Validate(kind string) error {
	return validateRegexes(kind, r.IncludeRE)
}

func (r LocalFilterRules) validateExclude(kind string) error {
	return validateRegexes(kind, r.ExcludeRE)
}

func validateRegexes(kind string, patterns []string) error {
	for _, p := range patterns {
		if _, err := regexp.Compile(p); err != nil {
			return fmt.Errorf("invalid %s regex %q: %w", kind, p, err)
		}
	}
	return nil
}
