package vectorapi

import (
	"fmt"

	"github.com/grepplabs/vectap/internal/app/runconfig"
)

func NewClient(api string) (Client, error) {
	switch api {
	case string(runconfig.VectorAPIGraphQL):
		return NewGraphQLWSClient(), nil
	case string(runconfig.VectorAPIGrpc):
		return NewGRPCClient(), nil
	default:
		return nil, fmt.Errorf("unsupported vector api %q", api)
	}
}
