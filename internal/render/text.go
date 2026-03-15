package render

import (
	"fmt"
	"io"
	"sync"

	"github.com/grepplabs/vectap/internal/output"
)

type TextRenderer struct {
	w           io.Writer
	useColor    bool
	includeMeta bool
	mu          sync.Mutex
	colors      map[string]string
	next        int
}

var palette = []string{
	"\033[31m", // red
	"\033[32m", // green
	"\033[33m", // yellow
	"\033[34m", // blue
	"\033[35m", // magenta
	"\033[36m", // cyan
}

const reset = "\033[0m"

func NewTextRenderer(w io.Writer, useColor bool, includeMeta bool) *TextRenderer {
	return &TextRenderer{
		w:           w,
		useColor:    useColor,
		includeMeta: includeMeta,
		colors:      make(map[string]string),
	}
}

func (r *TextRenderer) Render(ev output.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.includeMeta {
		_, err := fmt.Fprintln(r.w, ev.Message)
		return err
	}

	prefix := fmt.Sprintf("%s/%s %s", ev.Namespace, ev.PodName, ev.ComponentID)
	if r.useColor {
		prefix = r.colorFor(ev.TargetID) + prefix + reset
	}

	_, err := fmt.Fprintf(r.w, "%s %s\n", prefix, ev.Message)
	return err
}

func (r *TextRenderer) colorFor(key string) string {
	if c, ok := r.colors[key]; ok {
		return c
	}
	c := palette[r.next%len(palette)]
	r.colors[key] = c
	r.next++
	return c
}
