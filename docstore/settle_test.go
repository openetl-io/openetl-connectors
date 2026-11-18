package docstore

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// store is a Store of streams by name: staged has the streams whose staging
// is there, gone those whose staging was lost.
type store struct {
	markers      map[string]Marker
	staged, gone map[string]bool
	applied      []string
	// The next DropStaging calls that fail.
	failDrops int
}

func newStore(staged ...string) *store {
	s := &store{markers: map[string]Marker{}, staged: map[string]bool{}, gone: map[string]bool{}}
	for _, n := range staged {
		s.staged[n] = true
	}
	return s
}

func (s *store) Markers(context.Context) (map[string]Marker, error) {
	return maps.Clone(s.markers), nil
}

func (s *store) SetMarkers(_ context.Context, ms []Marker) error {
	for _, m := range ms {
		s.markers[m.Stream] = m
	}
	return nil
}

func (s *store) Mark(name string, e destination.Epoch) Marker { return Mark(e, name, false, "", "") }

func (s *store) Apply(_ context.Context, name string) (uint64, error) {
	s.applied = append(s.applied, name)
	return 0, nil
}

func (s *store) Swap(context.Context, Marker) error { return nil }

func (s *store) DropStaging(_ context.Context, name string) error {
	if s.failDrops > 0 {
		s.failDrops--
		return errors.New("drop failed")
	}
	delete(s.staged, name)
	return nil
}

func (s *store) Staging(_ context.Context, name string) (present, missing int, err error) {
	switch {
	case s.staged[name]:
		return 1, 0, nil
	case s.gone[name]:
		return 0, 1, nil
	}
	return 0, 0, nil
}

var epoch = destination.Epoch{Generation: 1, N: 3}

func TestSettleDropsTheStagingOfAStreamAppliedBefore(t *testing.T) {
	for _, scope := range []destination.Scope{destination.ScopePipeline, destination.ScopeStream} {
		st := newStore("a", "b")
		st.failDrops = 1
		if _, err := Settle(t.Context(), st, scope, epoch, []string{"a", "b"}); err == nil {
			t.Fatal("settled with a drop failing")
		}
		if _, err := Settle(t.Context(), st, scope, epoch, []string{"a", "b"}); err != nil {
			t.Fatal(err)
		}
		if len(st.staged) > 0 {
			t.Errorf("scope %d: staging left: %v", scope, slices.Sorted(maps.Keys(st.staged)))
		}
		if slices.Sort(st.applied); !slices.Equal(st.applied, []string{"a", "b"}) {
			t.Errorf("scope %d: applied %v, want each stream once", scope, st.applied)
		}
	}
}
