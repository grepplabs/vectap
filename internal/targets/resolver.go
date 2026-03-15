package targets

import "context"

type ResolveOptions struct {
	Namespace     string
	LabelSelector string
	RemotePort    int
}

type Resolver interface {
	Resolve(ctx context.Context, opts ResolveOptions) ([]Target, error)
}
