package keysplit

import (
	"context"
	"sync"
)

// ReadWithCDC reads a read's streams in splits by rest, on as many lanes as
// it has connections, limit, and with cdc, if any, by cdc on one of them:
// after the rest with one, since a continuous cdc read never ends.
func ReadWithCDC(ctx context.Context, limit int, rest func(ctx context.Context, lanes int) error, cdc func(context.Context) error) error {
	if cdc == nil {
		return rest(ctx, limit)
	}
	if limit == 1 {
		if err := rest(ctx, 1); err != nil {
			return err
		}
		return cdc(ctx)
	}
	return Alongside(ctx, func(ctx context.Context) error { return rest(ctx, limit-1) }, cdc)
}

// Alongside runs rest and cdc together, and stops both at the first to fail.
func Alongside(ctx context.Context, rest, cdc func(context.Context) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := rest(ctx); err != nil {
			cancel(err)
		}
	})
	if err := cdc(ctx); err != nil {
		cancel(err)
	}
	wg.Wait()
	return context.Cause(ctx)
}
