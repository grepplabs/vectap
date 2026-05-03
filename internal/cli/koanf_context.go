package cli

import (
	"context"
	"errors"

	"github.com/knadh/koanf/v2"
)

type koanfContextKey struct{}

func withKoanf(ctx context.Context, k *koanf.Koanf) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, koanfContextKey{}, k)
}

func koanfFromContext(ctx context.Context) (*koanf.Koanf, error) {
	if ctx == nil {
		return nil, errors.New("internal error: missing command context")
	}
	k, _ := ctx.Value(koanfContextKey{}).(*koanf.Koanf)
	if k == nil {
		return nil, errors.New("internal error: configuration not initialized")
	}
	return k, nil
}
