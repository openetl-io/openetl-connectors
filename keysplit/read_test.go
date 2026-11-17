package keysplit

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestReadWithCDC(t *testing.T) {
	var ran []string
	rest := func(_ context.Context, lanes int) error { ran = append(ran, fmt.Sprint("rest ", lanes)); return nil }
	cdc := func(context.Context) error { ran = append(ran, "cdc"); return nil }
	if err := ReadWithCDC(t.Context(), 1, rest, cdc); err != nil || !slices.Equal(ran, []string{"rest 1", "cdc"}) {
		t.Errorf("one connection: %v, ran %v", err, ran)
	}
	ran = nil
	if err := ReadWithCDC(t.Context(), 3, rest, nil); err != nil || !slices.Equal(ran, []string{"rest 3"}) {
		t.Errorf("no cdc: %v, ran %v", err, ran)
	}
	// The rest failing stops a cdc read that would go on.
	boom := errors.New("boom")
	failing := func(context.Context, int) error { return boom }
	waits := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	if err := ReadWithCDC(t.Context(), 3, failing, waits); !errors.Is(err, boom) {
		t.Errorf("alongside: %v, want boom", err)
	}
}
