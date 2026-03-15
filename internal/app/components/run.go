package components

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/forward"
	"github.com/grepplabs/vectap/internal/kube"
	"github.com/grepplabs/vectap/internal/targets"
	"github.com/grepplabs/vectap/internal/vectorapi"
	"gopkg.in/yaml.v3"
)

type Runner struct {
	client vectorapi.Client
}

//nolint:tagliatelle
type ListedComponent struct {
	TargetID      string `json:"target_id" yaml:"target_id"`
	Namespace     string `json:"namespace" yaml:"namespace"`
	PodName       string `json:"pod_name" yaml:"pod_name"`
	ComponentID   string `json:"component_id" yaml:"component_id"`
	ComponentKind string `json:"component_kind" yaml:"component_kind"`
	ComponentType string `json:"component_type" yaml:"component_type"`
}

func NewDefaultRunner() *Runner {
	return &Runner{client: vectorapi.NewGraphQLWSClient()}
}

func NewRunner(client vectorapi.Client) *Runner {
	return &Runner{client: client}
}

func (r *Runner) Components(ctx context.Context, cfg Config) error {
	runCfgs, err := expandRunConfigs(cfg)
	if err != nil {
		return err
	}

	effectiveFormat, err := formatForRender(cfg, runCfgs)
	if err != nil {
		return err
	}
	includeMeta := includeMetaForRender(cfg, runCfgs)

	var listed []ListedComponent
	for _, rcfg := range runCfgs {
		items, err := r.listSourceComponents(ctx, rcfg)
		if err != nil {
			return err
		}
		listed = append(listed, items...)
	}

	sort.Slice(listed, func(i, j int) bool {
		if listed[i].Namespace != listed[j].Namespace {
			return listed[i].Namespace < listed[j].Namespace
		}
		if listed[i].PodName != listed[j].PodName {
			return listed[i].PodName < listed[j].PodName
		}
		if listed[i].ComponentKind != listed[j].ComponentKind {
			return listed[i].ComponentKind < listed[j].ComponentKind
		}
		if listed[i].ComponentType != listed[j].ComponentType {
			return listed[i].ComponentType < listed[j].ComponentType
		}
		return listed[i].ComponentID < listed[j].ComponentID
	})

	return render(os.Stdout, effectiveFormat, includeMeta, listed)
}

// nolint:cyclop
func (r *Runner) listSourceComponents(ctx context.Context, cfg Config) ([]ListedComponent, error) {
	switch cfg.Type {
	case runconfig.SourceTypeDirect:
		var out []ListedComponent
		for i, endpointURL := range cfg.DirectURLs {
			target := directTarget(i, endpointURL)
			components, err := r.client.Components(ctx, endpointURL, vectorapi.ComponentsRequest{})
			if err != nil {
				return nil, err
			}
			out = append(out, toListedComponents(target, cfg.SourceName, components)...)
		}
		return out, nil
	case runconfig.SourceTypeKubernetes:
		resolver, err := kube.NewResolverFromConfig(cfg.KubeConfigPath, cfg.KubeContext)
		if err != nil {
			return nil, err
		}
		fwd, err := forward.NewManagerFromConfig(cfg.KubeConfigPath, cfg.KubeContext)
		if err != nil {
			return nil, err
		}

		ts, err := resolver.Resolve(ctx, targets.ResolveOptions{
			Namespace:     cfg.Namespace,
			LabelSelector: cfg.LabelSelector,
			RemotePort:    cfg.VectorPort,
		})
		if err != nil {
			return nil, err
		}
		if len(ts) == 0 {
			return nil, errors.New("no matching targets found")
		}

		var out []ListedComponent
		for _, t := range ts {
			session, err := fwd.Start(ctx, t)
			if err != nil {
				return nil, fmt.Errorf("start port-forward for %s: %w", t.ID, err)
			}
			components, err := r.client.Components(ctx, session.EndpointURL, vectorapi.ComponentsRequest{})
			if err != nil {
				return nil, err
			}
			out = append(out, toListedComponents(t, cfg.SourceName, components)...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported type %q", cfg.Type)
	}
}

func expandRunConfigs(cfg Config) ([]Config, error) {
	if !cfg.UsesConfiguredSources() {
		return []Config{cfg}, nil
	}
	selected, err := runconfig.Select(
		cfg.AllSources,
		cfg.SelectedSources,
		cfg.Sources,
		func(s SourceConfig) string { return s.Name },
		func(s SourceConfig) bool { return s.Enabled },
	)
	if err != nil {
		return nil, err
	}

	out := make([]Config, 0, len(selected))
	for _, s := range selected {
		sc := cfg
		sc.Type = s.Type
		sc.DirectURLs = append([]string{}, s.DirectURLs...)
		sc.Namespace = s.Namespace
		sc.LabelSelector = s.LabelSelector
		sc.KubeConfigPath = s.KubeConfigPath
		sc.KubeContext = s.KubeContext
		sc.VectorPort = s.VectorPort
		sc.Format = s.Format
		sc.IncludeMeta = s.IncludeMeta
		sc.SourceName = s.Name
		sc.Sources = nil
		sc.SelectedSources = nil
		sc.AllSources = false
		out = append(out, sc)
	}
	return out, nil
}

func formatForRender(cfg Config, runCfgs []Config) (string, error) {
	if cfg.UsesConfiguredSources() {
		if len(runCfgs) == 0 {
			return cfg.Format, nil
		}
		format := runCfgs[0].Format
		if format == "" {
			format = runconfig.FormatText
		}
		for i := 1; i < len(runCfgs); i++ {
			candidate := runCfgs[i].Format
			if candidate == "" {
				candidate = runconfig.FormatText
			}
			if candidate != format {
				return "", fmt.Errorf("selected sources use different formats (%q and %q); set a single --format value", format, candidate)
			}
		}
		return format, nil
	}
	if cfg.Format == "" {
		return runconfig.FormatText, nil
	}
	return cfg.Format, nil
}

func toListedComponents(target targets.Target, sourceName string, components []vectorapi.Component) []ListedComponent {
	out := make([]ListedComponent, 0, len(components))
	namespace := target.Namespace
	if sourceName != "" {
		namespace = sourceName + "/" + namespace
	}
	for _, c := range components {
		out = append(out, ListedComponent{
			TargetID:      target.ID,
			Namespace:     namespace,
			PodName:       target.PodName,
			ComponentID:   c.ComponentID,
			ComponentKind: c.ComponentKind,
			ComponentType: c.ComponentType,
		})
	}
	return out
}

func includeMetaForRender(cfg Config, runCfgs []Config) bool {
	if cfg.UsesConfiguredSources() {
		for _, rcfg := range runCfgs {
			if rcfg.IncludeMeta {
				return true
			}
		}
		return false
	}
	return cfg.IncludeMeta
}

// nolint:cyclop
func render(w io.Writer, format string, includeMeta bool, listed []ListedComponent) error {
	switch format {
	case runconfig.FormatText:
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		header := "COMPONENT_ID\tKIND\tTYPE"
		if includeMeta {
			header = "TARGET\tCOMPONENT_ID\tKIND\tTYPE"
		}
		if _, err := fmt.Fprintln(tw, header); err != nil {
			return err
		}
		for _, item := range listed {
			if includeMeta {
				if _, err := fmt.Fprintf(tw, "%s/%s\t%s\t%s\t%s\n", item.Namespace, item.PodName, item.ComponentID, item.ComponentKind, item.ComponentType); err != nil {
					return err
				}
				continue
			}
			if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", item.ComponentID, item.ComponentKind, item.ComponentType); err != nil {
				return err
			}
		}
		return tw.Flush()
	case runconfig.FormatJSON:
		enc := json.NewEncoder(w)
		return enc.Encode(stripMeta(listed, includeMeta))
	case runconfig.FormatYAML:
		enc := yaml.NewEncoder(w)
		defer enc.Close() //nolint:errcheck
		return enc.Encode(stripMeta(listed, includeMeta))
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

//nolint:tagliatelle
type componentView struct {
	TargetID      string `json:"target_id,omitempty" yaml:"target_id,omitempty"`
	Namespace     string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	PodName       string `json:"pod_name,omitempty" yaml:"pod_name,omitempty"`
	ComponentID   string `json:"component_id" yaml:"component_id"`
	ComponentKind string `json:"component_kind" yaml:"component_kind"`
	ComponentType string `json:"component_type" yaml:"component_type"`
}

func stripMeta(in []ListedComponent, includeMeta bool) []componentView {
	out := make([]componentView, 0, len(in))
	for _, item := range in {
		v := componentView{
			ComponentID:   item.ComponentID,
			ComponentKind: item.ComponentKind,
			ComponentType: item.ComponentType,
		}
		if includeMeta {
			v.TargetID = item.TargetID
			v.Namespace = item.Namespace
			v.PodName = item.PodName
		}
		out = append(out, v)
	}
	return out
}

func directTarget(i int, endpointURL string) targets.Target {
	host := fmt.Sprintf("direct-%d", i+1)
	if u, err := url.Parse(endpointURL); err == nil && u.Host != "" {
		host = u.Host
	}
	safe := strings.NewReplacer("/", "_", ":", "_", ".", "_").Replace(host)
	return targets.Target{
		ID:        "direct/" + safe,
		Namespace: "direct",
		PodName:   host,
	}
}
