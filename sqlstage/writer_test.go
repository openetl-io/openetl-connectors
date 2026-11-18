package sqlstage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// fakeSQL has no marker and no staging; its fence is raised past by a newer
// generation after Fence's first call when newer is set.
type fakeSQL struct {
	fences int
	newer  bool
}

func (f *fakeSQL) Stage(context.Context, map[uint64]map[string]*Table, string, destination.Epoch, string, string, []string, arrow.RecordBatch) (*Table, error) {
	return nil, errors.New("not staged")
}
func (f *fakeSQL) Apply(context.Context, Staged, []Stream, map[string]uint64) error { return nil }
func (f *fakeSQL) Fence(context.Context, uint64) error {
	f.fences++
	if f.newer && f.fences > 1 {
		return destination.ErrFenced
	}
	return nil
}
func (f *fakeSQL) Unapplied(_ context.Context, _ Staged, streams []Stream) ([]Stream, error) {
	return streams, nil
}
func (f *fakeSQL) Exists(context.Context, string) (bool, error) { return false, nil }
func (f *fakeSQL) DropStaging(context.Context, []Stream)        {}
func (f *fakeSQL) TrimMarkers(context.Context, uint64)          {}
func (f *fakeSQL) DropStale(context.Context, uint64)            {}
func (f *fakeSQL) DropRemovedReads(context.Context)             {}

func TestRecoverIsLostOnlyBehindItsOwnFence(t *testing.T) {
	desc, _ := json.Marshal(Staged{Generation: 1, Epoch: 2, Streams: []Stream{{Name: "users", Table: "users", Staging: "s1"}}})
	e := destination.Epoch{Generation: 1, N: 2}
	for _, newer := range []bool{false, true} {
		p := NewEpochs[*Table]()
		p.Dialect = Dialect[*Table]{Name: "db", SQL: &fakeSQL{newer: newer}}
		r, err := p.Recover(t.Context(), 2, e, []destination.Staged{desc})
		switch {
		case !newer && (err != nil || r != destination.Lost):
			t.Errorf("with its staging gone, recovery is %v, %v; want lost", r, err)
		case newer && !errors.Is(err, destination.ErrFenced):
			t.Errorf("with a newer generation's fence raised meanwhile, recovery is %v, %v; want fenced", r, err)
		}
	}
}
