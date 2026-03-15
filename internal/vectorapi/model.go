package vectorapi

import (
	"time"

	"github.com/grepplabs/vectap/internal/output"
	"github.com/grepplabs/vectap/internal/targets"
)

type TapEvent struct {
	ComponentID string
	Kind        string
	Timestamp   time.Time
	Message     string
	Raw         []byte
	Meta        map[string]string
}

//nolint:tagliatelle
type Component struct {
	ComponentID   string `json:"component_id"`
	ComponentKind string `json:"component_kind"`
	ComponentType string `json:"component_type"`
}

func (e TapEvent) ToOutput(t targets.Target) output.Event {
	return output.Event{
		TargetID:    t.ID,
		Namespace:   t.Namespace,
		PodName:     t.PodName,
		ComponentID: e.ComponentID,
		Kind:        e.Kind,
		Timestamp:   e.Timestamp,
		Message:     e.Message,
		Raw:         e.Raw,
		Meta:        e.Meta,
	}
}
