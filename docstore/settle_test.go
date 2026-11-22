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
	// A newer generation raises the fence after Markers' first call.
	newer bool
	reads int
}

func newStore(staged ...string) *store {
	s := &store{markers: map[string]Marker{}, staged: map[string]bool{}, gone: map[string]bool{}}
	for _, n := range staged {
		s.staged[n] = true
	}
	return s
}

func (s *store) Markers(context.Context) (map[string]Marker, error) {
	if s.reads++; s.newer && s.reads > 1 {
		return nil, destination.ErrFenced
	}
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

func TestSettleRecordedIsLostOnlyWhenNothingApplied(t *testing.T) {
	st := newStore()
	st.gone["a"], st.gone["b"] = true, true
	if r, err := SettleRecorded(t.Context(), "db", st, destination.ScopeStream, epoch, []string{"a", "b"}); err != nil || r != destination.Lost {
		t.Errorf("with every stream's staging gone, recovery is %v, %v; want lost", r, err)
	}
	st = newStore()
	st.markers["a"] = st.Mark("a", epoch)
	st.gone["b"] = true
	if r, err := SettleRecorded(t.Context(), "db", st, destination.ScopeStream, epoch, []string{"a", "b"}); err == nil {
		t.Errorf("with a applied and b's staging gone, recovery is %v", r)
	}
	if len(st.applied) > 0 {
		t.Errorf("applied %v", st.applied)
	}
}

// A stream that staged nothing in the epoch, as one whose read only ended
// in it, doesn't make the rest's gone staging partly gone.
func TestSettleRecordedIsLostWithAStreamThatStagedNothing(t *testing.T) {
	st := newStore()
	st.gone["b"] = true
	if r, err := SettleRecorded(t.Context(), "db", st, destination.ScopePipeline, epoch, []string{"a", "b"}); err != nil || r != destination.Lost {
		t.Errorf("with a's staging empty and b's gone, recovery is %v, %v; want lost", r, err)
	}
	if len(st.applied) > 0 {
		t.Errorf("applied %v", st.applied)
	}
	// With the other's staging there, both apply.
	st = newStore("b")
	if r, err := SettleRecorded(t.Context(), "db", st, destination.ScopePipeline, epoch, []string{"a", "b"}); err != nil || r != destination.Redone || len(st.applied) != 2 {
		t.Errorf("with a's staging empty and b's there, recovery is %v, %v, applied %v; want both redone", r, err, st.applied)
	}
	// Alone, it applies.
	st = newStore()
	if r, err := SettleRecorded(t.Context(), "db", st, destination.ScopePipeline, epoch, []string{"a"}); err != nil || r != destination.Redone || len(st.applied) != 1 {
		t.Errorf("with a's staging empty, recovery is %v, %v, applied %v; want a redone", r, err, st.applied)
	}
}

func TestSettleRecordedIsLostOnlyBehindItsOwnFence(t *testing.T) {
	st := newStore()
	st.gone["a"], st.newer = true, true
	if r, err := SettleRecorded(t.Context(), "db", st, destination.ScopePipeline, epoch, []string{"a"}); !errors.Is(err, destination.ErrFenced) {
		t.Errorf("with a newer generation's fence raised meanwhile, recovery is %v, %v; want fenced", r, err)
	}
}
