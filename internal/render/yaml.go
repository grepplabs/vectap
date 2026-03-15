package render

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/grepplabs/vectap/internal/output"
	"gopkg.in/yaml.v3"
)

type YAMLRenderer struct {
	w           io.Writer
	includeMeta bool
	mu          sync.Mutex
}

func NewYAMLRenderer(w io.Writer, includeMeta bool) *YAMLRenderer {
	return &YAMLRenderer{w: w, includeMeta: includeMeta}
}

func (r *YAMLRenderer) Render(ev output.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	eventPayload := any(ev.Message)
	if err := json.Unmarshal([]byte(ev.Message), &eventPayload); err != nil {
		eventPayload = ev.Message
	}

	enc := yaml.NewEncoder(r.w)
	defer enc.Close() //nolint:errcheck
	if _, err := io.WriteString(r.w, "---\n"); err != nil {
		return err
	}
	if !r.includeMeta {
		return enc.Encode(eventPayload)
	}
	return enc.Encode(yamlEvent(ev))
}

//nolint:tagliatelle
type yamlEventView struct {
	TargetID    string            `yaml:"target_id"`
	Namespace   string            `yaml:"namespace"`
	PodName     string            `yaml:"pod_name"`
	ComponentID string            `yaml:"component_id"`
	Kind        string            `yaml:"kind"`
	Timestamp   any               `yaml:"timestamp"`
	Message     string            `yaml:"message"`
	Raw         string            `yaml:"raw,omitempty"`
	Meta        map[string]string `yaml:"meta,omitempty"`
}

func yamlEvent(ev output.Event) yamlEventView {
	view := yamlEventView{
		TargetID:    ev.TargetID,
		Namespace:   ev.Namespace,
		PodName:     ev.PodName,
		ComponentID: ev.ComponentID,
		Kind:        ev.Kind,
		Timestamp:   ev.Timestamp,
		Message:     ev.Message,
		Meta:        ev.Meta,
	}
	if len(ev.Raw) > 0 {
		view.Raw = string(ev.Raw)
	}
	return view
}
