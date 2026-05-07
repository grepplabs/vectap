package vectorcmd

import (
	"errors"

	"github.com/grepplabs/vectap/internal/app/runconfig"
)

const (
	ModeTap = "tap"
	ModeTop = "top"
)

type Config struct {
	runconfig.BaseConfig
	Mode         string
	VectorBin    string
	TapPrefix    bool
	TapColor     bool
	TerminalCmd  string
	TerminalHold bool
	ExtraArgs    []string
	Sources      []SourceConfig
}

type SourceConfig struct {
	runconfig.BaseSourceConfig
}

func (c Config) Validate() error {
	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}
	if err := runconfig.ValidateAllowed("", "mode", c.Mode, false, ModeTap, ModeTop); err != nil {
		return err
	}
	if c.VectorBin == "" {
		return errors.New("vector-bin is required")
	}
	for _, s := range c.Sources {
		if err := s.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (s SourceConfig) Validate() error {
	return s.BaseSourceConfig.Validate()
}
