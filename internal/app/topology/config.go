package topology

import (
	"errors"

	"github.com/grepplabs/vectap/internal/app/runconfig"
)

const (
	ViewTable = "table"
	ViewEdges = "edges"
	ViewTree  = "tree"
)

type Config struct {
	runconfig.BaseConfig
	View     string
	Orphaned bool
	Sources  []SourceConfig
}

type SourceConfig struct {
	runconfig.BaseSourceConfig
}

func (c Config) Validate() error {
	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}
	if err := runconfig.ValidateAllowed("", "view", c.View, false, ViewTable, ViewEdges, ViewTree); err != nil {
		return err
	}
	if c.Orphaned && c.View != ViewTable {
		return errors.New("--orphaned is supported only with --view table")
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
