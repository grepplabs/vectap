package components

import (
	"github.com/grepplabs/vectap/internal/app/runconfig"
)

type Config struct {
	runconfig.BaseConfig
	Sources []SourceConfig
}

type SourceConfig struct {
	runconfig.BaseSourceConfig
}

func (c Config) Validate() error {
	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}
	for _, s := range c.Sources {
		if err := s.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (s SourceConfig) Validate() error {
	if err := s.BaseSourceConfig.Validate(); err != nil {
		return err
	}
	return nil
}
