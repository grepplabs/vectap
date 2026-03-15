package output

import "time"

//nolint:tagliatelle
type Event struct {
	TargetID    string            `json:"target_id" yaml:"target_id"`
	Namespace   string            `json:"namespace" yaml:"namespace"`
	PodName     string            `json:"pod_name" yaml:"pod_name"`
	ComponentID string            `json:"component_id" yaml:"component_id"`
	Kind        string            `json:"kind" yaml:"kind"`
	Timestamp   time.Time         `json:"timestamp" yaml:"timestamp"`
	Message     string            `json:"message" yaml:"message"`
	Raw         []byte            `json:"raw,omitempty" yaml:"raw,omitempty"`
	Meta        map[string]string `json:"meta,omitempty" yaml:"meta,omitempty"`
}
