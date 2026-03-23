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

//nolint:tagliatelle
type TopologyComponentRef struct {
	ComponentID   string `json:"component_id"`
	ComponentType string `json:"component_type"`
}

//nolint:tagliatelle
type TopologyOutput struct {
	OutputID string `json:"output_id"`
}

//nolint:tagliatelle
type TopologyComponent struct {
	ComponentID         string                 `json:"component_id"`
	ComponentKind       string                 `json:"component_kind"`
	ComponentType       string                 `json:"component_type"`
	ReceivedBytesTotal  *float64               `json:"received_bytes_total,omitempty"`
	SentBytesTotal      *float64               `json:"sent_bytes_total,omitempty"`
	ReceivedEventsTotal *float64               `json:"received_events_total,omitempty"`
	SentEventsTotal     *float64               `json:"sent_events_total,omitempty"`
	Sources             []TopologyComponentRef `json:"sources,omitempty"`
	Transforms          []TopologyComponentRef `json:"transforms,omitempty"`
	Sinks               []TopologyComponentRef `json:"sinks,omitempty"`
	Outputs             []TopologyOutput       `json:"outputs,omitempty"`
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
