package docstore

import (
	"context"
	"fmt"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// Marker is a stream's entry in its pipeline's fence: the epoch that last
// applied it, and while an overwrite read that ended in it takes its
// target's place, the read.
type Marker struct {
	Stream     string `bson:"s"`
	Generation uint64 `bson:"g"`
	N          uint64 `bson:"n"`
	Swap       string `bson:"swap,omitempty"`
	Target     string `bson:"t,omitempty"`
}

func (m Marker) Is(e destination.Epoch) bool { return m.Generation == e.Generation && m.N == e.N }

// Mark is the marker of stream applied in e; an ended overwrite's swaps
// its read in as target.
func Mark(e destination.Epoch, stream string, ended bool, read, target string) Marker {
	m := Marker{Stream: stream, Generation: e.Generation, N: e.N}
	if ended {
		m.Swap, m.Target = read, target
	}
	return m
}

// Store is how a destination applies an epoch's staged streams, S, and
// keeps their markers.
type Store[S any] interface {
	// Markers are the pipeline's, by stream.
	Markers(ctx context.Context) (map[string]Marker, error)
	// SetMarkers replaces the streams' markers, all or none, unless a newer
	// generation has raised the fence.
	SetMarkers(ctx context.Context, ms []Marker) error
	// Mark is the marker of s applied in e.
	Mark(s S, e destination.Epoch) Marker
	// Apply applies s's staging, and returns the rows its dedupe dropped.
	Apply(ctx context.Context, s S) (uint64, error)
	// Swap makes m's read, if it has one, its target.
	Swap(ctx context.Context, m Marker) error
	// DropStaging drops s's staging; what's gone already isn't an error.
	DropStaging(ctx context.Context, s S) error
	// Staging counts s's staging that's there, and that's gone.
	Staging(ctx context.Context, s S) (present, missing int, err error)
}

// Settle applies each stream whose marker isn't the epoch's yet: with
// ScopePipeline, every stream's rows and then their markers together, with
// ScopeStream, stream by stream.
func Settle[S any](ctx context.Context, st Store[S], scope destination.Scope, e destination.Epoch, streams []S) (destination.Committed, error) {
	done := destination.Committed{Collapsed: map[string]uint64{}}
	markers, err := st.Markers(ctx)
	if err != nil {
		return done, err
	}
	var todo []S
	for _, s := range streams {
		if m, ok := markers[st.Mark(s, e).Stream]; ok && m.Is(e) {
			// Applied by an earlier call, which may have stopped before
			// its swap or its staging's drop.
			if err := st.Swap(ctx, m); err != nil {
				return done, err
			}
			if err := st.DropStaging(ctx, s); err != nil {
				return done, err
			}
			continue
		}
		todo = append(todo, s)
	}
	groups := [][]S{todo}
	if scope == destination.ScopeStream {
		groups = groups[:0]
		for _, s := range todo {
			groups = append(groups, []S{s})
		}
	}
	for _, g := range groups {
		if len(g) == 0 {
			continue
		}
		var ms []Marker
		for _, s := range g {
			n, err := st.Apply(ctx, s)
			if err != nil {
				return done, err
			}
			m := st.Mark(s, e)
			if n > 0 {
				done.Collapsed[m.Stream] = n
			}
			ms = append(ms, m)
		}
		if err := st.SetMarkers(ctx, ms); err != nil {
			return done, err
		}
		for i, s := range g {
			if err := st.Swap(ctx, ms[i]); err != nil {
				return done, err
			}
			if err := st.DropStaging(ctx, s); err != nil {
				return done, err
			}
		}
	}
	return done, nil
}

// SettleRecorded settles a recorded epoch that may be applied, partly or
// whole, or whose staging may be gone, for the destination name's Recover.
func SettleRecorded[S any](ctx context.Context, name string, st Store[S], scope destination.Scope, e destination.Epoch, streams []S) (destination.Recovery, error) {
	markers, err := st.Markers(ctx)
	if err != nil {
		return 0, err
	}
	present, missing, swaps := 0, 0, false
	for _, s := range streams {
		if m, ok := markers[st.Mark(s, e).Stream]; ok && m.Is(e) {
			swaps = swaps || m.Swap != ""
			continue
		}
		p, m, err := st.Staging(ctx, s)
		if err != nil {
			return 0, err
		}
		if p+m == 0 {
			// It staged nothing in the epoch.
			p = 1
		}
		present, missing = present+p, missing+m
	}
	switch {
	case present == 0 && missing == 0:
		if _, err := Settle(ctx, st, scope, e, streams); err != nil {
			return 0, err
		}
		if swaps {
			return destination.Redone, nil
		}
		return destination.Applied, nil
	case missing == 0:
		_, err := Settle(ctx, st, scope, e, streams)
		return destination.Redone, err
	case present == 0:
		return destination.Lost, nil
	}
	return 0, fmt.Errorf("%s: epoch %d.%d's staging is partly gone, %d of %d parts", name, e.Generation, e.N, missing, present+missing)
}
