package vectorapi

import (
	"context"

	"github.com/grepplabs/vectap/internal/targets"
)

type TapRequest struct {
	OutputsOf   []string
	InputsOf    []string
	Interval    int
	Limit       int
	IncludeMeta bool
	Target      targets.Target
}

type ComponentsRequest struct{}

type Client interface {
	Tap(ctx context.Context, endpointURL string, req TapRequest) (<-chan TapEvent, <-chan error)
	Components(ctx context.Context, endpointURL string, req ComponentsRequest) ([]Component, error)
}
