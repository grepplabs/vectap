package runconfig

import "errors"

type BaseConfig struct {
	Type            string
	DirectURLs      []string
	SourceName      string
	SelectedSources []string
	AllSources      bool
	Namespace       string
	LabelSelector   string
	KubeConfigPath  string
	KubeContext     string
	Format          string
	VectorPort      int
	IncludeMeta     bool
}

type BaseSourceConfig struct {
	Name           string
	Type           string
	Enabled        bool
	Format         string
	DirectURLs     []string
	Namespace      string
	LabelSelector  string
	KubeConfigPath string
	KubeContext    string
	VectorPort     int
	IncludeMeta    bool
}

func (cfg BaseConfig) UsesConfiguredSources() bool {
	return cfg.AllSources || len(cfg.SelectedSources) > 0
}

//nolint:cyclop
func (cfg BaseConfig) Validate() error {
	if err := ValidateAllowed("", "type", cfg.Type, false, SourceTypeDirect, SourceTypeKubernetes); err != nil {
		return err
	}
	if err := ValidateDirectURLs("", cfg.Type, SourceTypeDirect, cfg.DirectURLs); err != nil {
		return err
	}
	if cfg.Namespace == "" {
		return errors.New("namespace is required")
	}
	if err := ValidateAllowed("", "format", cfg.Format, false, FormatText, FormatJSON, FormatYAML); err != nil {
		return err
	}
	if err := ValidateRange("", "vector-port", cfg.VectorPort, 1, 65535); err != nil {
		return err
	}
	return nil
}

//nolint:cyclop
func (cfg BaseSourceConfig) Validate() error {
	if cfg.Name == "" {
		return errors.New("source name is required")
	}
	if err := ValidateAllowed(cfg.Name, "type", cfg.Type, false, SourceTypeDirect, SourceTypeKubernetes); err != nil {
		return err
	}
	if err := ValidateDirectURLs(cfg.Name, cfg.Type, SourceTypeDirect, cfg.DirectURLs); err != nil {
		return err
	}
	if err := ValidateAllowed(cfg.Name, "format", cfg.Format, false, FormatText, FormatJSON, FormatYAML); err != nil {
		return err
	}
	if err := ValidateRange(cfg.Name, "vector-port", cfg.VectorPort, 1, 65535); err != nil {
		return err
	}
	return nil
}
