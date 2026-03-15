package render

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/grepplabs/vectap/internal/output"
)

type JSONRenderer struct {
	w           io.Writer
	includeMeta bool
	mu          sync.Mutex
}

func NewJSONRenderer(w io.Writer, includeMeta bool) *JSONRenderer {
	return &JSONRenderer{w: w, includeMeta: includeMeta}
}

func (r *JSONRenderer) Render(ev output.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	eventPayload := any(ev.Message)
	if err := json.Unmarshal([]byte(ev.Message), &eventPayload); err != nil {
		eventPayload = ev.Message
	}

	enc := json.NewEncoder(r.w)
	if !r.includeMeta {
		return enc.Encode(eventPayload)
	}
	return enc.Encode(ev)
}
