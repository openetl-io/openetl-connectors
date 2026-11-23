package sqlserver

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A read is at its until where it's at the until's position, in the same
// part of its snapshot or poll, however the until spells when that position
// was the latest: it completes there, with the until's bytes. A state at the
// until's position but outside its snapshot isn't at it.
func TestCDCReachesUntilByPosition(t *testing.T) {
	at := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	b, err := newBatcher(memory.DefaultAllocator, []sourceColumn{{name: "id", typ: "int"}}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer b.release()
	r := &cdcReader{m: &tracking{}}
	for _, c := range []struct {
		name    string
		st      cdcState
		until   string
		reached bool
	}{
		{"at in another offset", cdcState{Method: methodTracking, Position: "9", At: &at},
			`{"position":"9","at":"2026-11-23T11:00:00+01:00","method":"change_tracking"}`, true},
		{"at the state hasn't", cdcState{Method: methodTracking, Position: "9"},
			`{"method":"change_tracking","position":"9","at":"2026-11-23T10:00:00Z"}`, true},
		{"in the snapshot", cdcState{Method: methodTracking, Position: "9", At: &at},
			`{"method":"change_tracking","position":"9","at":"2026-11-23T10:00:00Z","snapshot":{"after":["3"]}}`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := &connectortest.Recorder{}
			ct := &cdcTable{t: table{name: "t"}, b: b, out: rec.Split("t", source.WholeStream), st: c.st, until: []byte(c.until), stop: &cdcState{}}
			if err := json.Unmarshal(ct.until, ct.stop); err != nil {
				t.Fatal(err)
			}
			if ct.resumed, err = json.Marshal(c.st); err != nil {
				t.Fatal(err)
			}
			_, err := r.step(t.Context(), ct)
			ev := rec.Snapshot()
			if !c.reached {
				if err == nil || ct.done || len(ev) > 0 {
					t.Errorf("a state at the until's position outside its snapshot: %v, done %v, emitted %+v", err, ct.done, ev)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(ev) != 2 || !ev[0].Resume || string(ev[0].State) != c.until || !ev[1].Complete || string(ev[1].State) != c.until || !ct.done {
				t.Errorf("at the until %s: emitted %+v, done %v", c.until, ev, ct.done)
			}
		})
	}
}
