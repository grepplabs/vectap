package render

import "github.com/grepplabs/vectap/internal/output"

type Renderer interface {
	Render(ev output.Event) error
}
