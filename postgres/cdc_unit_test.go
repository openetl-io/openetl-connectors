package postgres

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pglogrepl"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A column the run didn't declare fails the read, retryably, key or not:
// a read that left it out would send rows without its values, which the
// next attempt, declaring it, wouldn't send again.
func TestUndeclared(t *testing.T) {
	users := table{name: "users", cols: []sourceColumn{{name: "a"}, {name: "k1"}, {name: "new"}, {name: "k2"}}, key: []int{3, 1}}
	if err := undeclared(users, []string{"a", "k1", "new", "k2", "dropped"}); err != nil {
		t.Errorf("every column declared: %v", err)
	}
	if err := undeclared(users, nil); err != nil {
		t.Errorf("a request that doesn't say: %v", err)
	}
	for _, declared := range [][]string{{"a", "k1", "k2"}, {"a", "k1", "new"}} {
		if err := undeclared(users, declared); !isTransient(err) {
			t.Errorf("declared %v: %v", declared, err)
		}
	}
}

// An update's value that the log left out, as an unchanged TOAST value, is
// named as unchanged; with the whole old row, as replica identity full
// sends it, it's filled from that.
func TestAnUnsentValueIsFilledFromAWholeOldRow(t *testing.T) {
	rel := &relation{
		stream: &cdcStream{t: table{name: "users", cols: []sourceColumn{{name: "id"}, {name: "name"}, {name: "doc"}}}},
		at:     []int{0, 1, 2},
		conv:   make([]func([]byte) []byte, 3),
		names:  []string{"id", "name", "doc"},
	}
	col := func(kind byte, v string) *pglogrepl.TupleDataColumn {
		return &pglogrepl.TupleDataColumn{DataType: kind, Data: []byte(v)}
	}
	update := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		col(pglogrepl.TupleDataTypeText, "3"), col(pglogrepl.TupleDataTypeText, "renamed"), col(pglogrepl.TupleDataTypeToast, ""),
	}}
	vals, unchanged := rel.values(update, nil)
	if vals[2] != nil || !slices.Equal(unchanged, []string{"doc"}) {
		t.Errorf("without the old row: doc %q, unchanged %v", vals[2], unchanged)
	}
	old := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		col(pglogrepl.TupleDataTypeText, "3"), col(pglogrepl.TupleDataTypeText, "before"), col(pglogrepl.TupleDataTypeText, "big"),
	}}
	vals, unchanged = rel.values(update, old)
	if string(vals[1]) != "renamed" || string(vals[2]) != "big" || len(unchanged) != 0 {
		t.Errorf("with the whole old row: %q, unchanged %v", vals, unchanged)
	}
}

// events records what a CDC read emits: "STREAM:ids" for a batch, "STREAM@STATE"
// and "STREAM complete STATE".
type events struct{ log []string }

func (e *events) Rows(stream string, _ []json.RawMessage) error {
	return fmt.Errorf("unexpected raw rows on %s", stream)
}

func (e *events) Records(stream string, b arrow.RecordBatch) error {
	defer b.Release()
	ids := b.Column(0).(*array.String)
	var vals []string
	for i := range ids.Len() {
		vals = append(vals, ids.Value(i))
	}
	e.log = append(e.log, stream+":"+strings.Join(vals, ","))
	return nil
}

func (e *events) ResumePoint(stream string, state []byte) error {
	e.log = append(e.log, stream+"@"+string(state))
	return nil
}

func (e *events) Complete(stream string, next []byte) error {
	e.log = append(e.log, stream+" complete "+string(next))
	return nil
}

func (e *events) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(e, stream, id)
}

// logMessage spells a pgoutput message: Begin, Insert of one text column, or
// Commit.
func logMessage(kind byte, fields ...any) []byte {
	b := []byte{kind}
	for _, f := range fields {
		switch v := f.(type) {
		case uint8:
			b = append(b, v)
		case uint32:
			b = binary.BigEndian.AppendUint32(b, v)
		case pglogrepl.LSN:
			b = binary.BigEndian.AppendUint64(b, uint64(v))
		case string:
			b = append(b, 'N')
			b = binary.BigEndian.AppendUint16(b, 1)
			b = append(b, pglogrepl.TupleDataTypeText)
			b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
			b = append(b, v...)
		}
	}
	return b
}

// A bounded CDC read asked to stop at a position emits the changes of the
// transactions that committed before it, none of those after, and completes
// there with the state it was given, after a resume point with it if that
// was an until, though the log has passed the position.
func TestCDCStopsAtAPosition(t *testing.T) {
	stop := func(until bool, stopAt pglogrepl.LSN) []string {
		t.Helper()
		st := &cdcStream{t: table{name: "users", cols: []sourceColumn{{name: "id"}}, key: []int{0}}, from: 100, conv: []func([]byte) []byte{nil}}
		st.b = newChangeBatcher(memory.DefaultAllocator, st.t.cols)
		defer st.b.release()
		state, _ := json.Marshal(cdcState{Slot: "s", LSN: lsn(stopAt)})
		var err error
		if until {
			err = st.stopAt("s", state, nil)
		} else {
			err = st.stopAt("s", nil, state)
		}
		if err != nil || st.stop != stopAt {
			t.Fatalf("stops at %v (%v), not at %v", st.stop, err, stopAt)
		}
		out := &events{}
		r := &cdcReader{s: &Source{cfg: SourceConfig{BatchBytes: 1 << 20}}, out: out, slot: "s", streams: []*cdcStream{st}, pos: 100,
			rels: map[uint32]*relation{7: {stream: st, at: []int{0}, key: []int{0}, conv: []func([]byte) []byte{nil}, names: []string{"id"}}}}
		txn := func(xid uint32, commit, end pglogrepl.LSN, ids ...string) {
			t.Helper()
			msgs := [][]byte{logMessage('B', commit, pglogrepl.LSN(0), xid)}
			for _, id := range ids {
				msgs = append(msgs, logMessage('I', uint32(7), id))
			}
			msgs = append(msgs, logMessage('C', uint8(0), commit, end, pglogrepl.LSN(0)))
			for _, m := range msgs {
				if err := r.message(t.Context(), m); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.stopped(); err != nil {
				t.Fatal(err)
			}
			if st.done {
				return
			}
			if err := r.resumeStream(st); err != nil {
				t.Fatal(err)
			}
		}
		txn(1, 90, 120, "0")
		txn(2, 150, 200, "1", "2")
		txn(3, stopAt, stopAt+50, "3")
		txn(4, stopAt+100, stopAt+150, "4")
		if err := r.complete(); err != nil {
			t.Fatal(err)
		}
		return out.log
	}
	at := func(l pglogrepl.LSN) string {
		b, _ := json.Marshal(cdcState{Slot: "s", LSN: lsn(l)})
		return string(b)
	}

	// Only the transactions that committed in [100, 300) have changes.
	got := stop(true, 300)
	want := []string{"users@" + at(120), "users:1,2", "users@" + at(200), "users@" + at(300), "users complete " + at(300)}
	if !slices.Equal(got, want) {
		t.Errorf("until 300:\n got %q\nwant %q", got, want)
	}
	got = stop(false, 300)
	want = []string{"users@" + at(120), "users:1,2", "users@" + at(200), "users complete " + at(300)}
	if !slices.Equal(got, want) {
		t.Errorf("complete at 300:\n got %q\nwant %q", got, want)
	}
	// Where the stop isn't a transaction's end, but a keepalive's, as a
	// quiet log's resume point is.
	got = stop(false, 250)
	want = []string{"users@" + at(120), "users:1,2", "users@" + at(200), "users complete " + at(250)}
	if !slices.Equal(got, want) {
		t.Errorf("complete at 250:\n got %q\nwant %q", got, want)
	}

	// A stream with a snapshot left to read ignores where it's asked to
	// stop, as do a continuous read, which never sets one, and a state
	// with a snapshot of its own.
	st := &cdcStream{t: table{name: "users"}, ranges: []*snapshotRange{{}}}
	state, _ := json.Marshal(cdcState{Slot: "s", LSN: 300})
	if err := st.stopAt("s", state, nil); err != nil || st.stop != 0 {
		t.Errorf("a stream mid-snapshot stops at %v (%v)", st.stop, err)
	}
	st = &cdcStream{t: table{name: "users"}}
	state, _ = json.Marshal(cdcState{Slot: "s", LSN: 300, Snapshot: &snapshotCursor{After: []string{"7"}}})
	if err := st.stopAt("s", state, nil); err != nil || st.stop != 0 {
		t.Errorf("a stop mid-snapshot stops at %v (%v)", st.stop, err)
	}
	if err := st.stopAt("s", []byte("{"), nil); err == nil {
		t.Error("a broken until")
	}
}

// A position in another slot's log says nothing of this one's: a read asked
// to stop at one fails with POSITION_LOST, as one resumed from it does.
func TestCDCStopsOnlyAtAPositionOfItsSlot(t *testing.T) {
	state, _ := json.Marshal(cdcState{Slot: "other", LSN: 300})
	for _, until := range []bool{true, false} {
		st := &cdcStream{t: table{name: "users"}}
		var err error
		if until {
			err = st.stopAt("s", state, nil)
		} else {
			err = st.stopAt("s", nil, state)
		}
		var coded interface{ ErrorCode() protocolv1.ErrorCode }
		if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST || st.stop != 0 {
			t.Errorf("until %v: stops at %v (%v), want POSITION_LOST", until, st.stop, err)
		}
	}
}

// A read of the slot that ends after the next one started, as a read's end
// can when the next read took the slot from it, leaves the next read the
// one Committed acknowledges on; and the next read takes what the one it
// replaced was acknowledged.
func TestALateReadEndLeavesTheNextReadsReader(t *testing.T) {
	s := &Source{}
	committed := func(at string) {
		t.Helper()
		if err := s.Committed(t.Context(), map[string][]byte{"users": []byte(`{"slot":"s","lsn":"` + at + `"}`)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	old, next := &cdcReader{slot: "s"}, &cdcReader{slot: "s"}
	unsetOld := s.setReader(old)
	committed("0/10")
	unsetNext := s.setReader(next)
	if got := pglogrepl.LSN(next.acked.Load()); got != 0x10 {
		t.Errorf("the next read starts acknowledged up to %s, want 0/10", got)
	}
	unsetOld()
	if s.reader != next {
		t.Fatalf("the earlier read's end unset the next read's reader: %p, want %p", s.reader, next)
	}
	committed("0/20")
	if got := pglogrepl.LSN(next.acked.Load()); got != 0x20 {
		t.Errorf("the next read was acknowledged up to %s, want 0/20", got)
	}
	if got := pglogrepl.LSN(old.acked.Load()); got != 0x10 {
		t.Errorf("the earlier read was acknowledged up to %s after it was replaced", got)
	}
	unsetNext()
	if s.reader != nil {
		t.Errorf("a read's end left its reader: %p", s.reader)
	}

	// A read of another slot takes nothing from the one it replaces.
	other := &cdcReader{slot: "t"}
	s.setReader(next)
	s.setReader(other)
	if got := other.acked.Load(); got != 0 {
		t.Errorf("a read of another slot took %s", pglogrepl.LSN(got))
	}
}
