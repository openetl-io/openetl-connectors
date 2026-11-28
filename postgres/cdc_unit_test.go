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
			if err := r.resumeStream(t.Context(), st); err != nil {
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

// Integer keys are placed in Go against each range: behind its position or
// in a range read already, among a held chunk's keys, or ahead of what any
// chunk has read; a key's first column alone says which range it's in.
func TestPlaceIntegerKeys(t *testing.T) {
	s := func(v string) *string { return &v }
	single := &cdcStream{t: table{name: "t", cols: []sourceColumn{{name: "id", oid: 23}}, key: []int{0}}, ints: true}
	// (-, 100] at 50 with a chunk to 70 held; (100, 200] not started;
	// (200, 300] read; (300, -) at 350 with a chunk to its end held.
	single.ranges = []*snapshotRange{
		{keyRange: keyRange{Hi: s("100")}, After: []string{"50"}},
		{keyRange: keyRange{Lo: s("100"), Hi: s("200")}},
		{keyRange: keyRange{Lo: s("300")}, After: []string{"350"}},
	}
	first := &chunk{stream: single, rng: single.ranges[0], full: true, last: []string{"70"}}
	last := &chunk{stream: single, rng: single.ranges[2]}
	composite := &cdcStream{t: table{name: "u", cols: []sourceColumn{{name: "a", oid: 20}, {name: "b", oid: 21}}, key: []int{0, 1}}, ints: true}
	composite.ranges = []*snapshotRange{
		{keyRange: keyRange{Hi: s("10")}, After: []string{"5", "7"}},
		{keyRange: keyRange{Lo: s("10")}},
	}
	mid := &chunk{stream: composite, rng: composite.ranges[0], full: true, last: []string{"8", "2"}}
	r := &cdcReader{streams: []*cdcStream{single, composite}, chunks: []*chunk{first, last, mid}}
	for _, c := range []struct {
		st   *cdcStream
		keys map[string]placed
	}{
		{single, map[string]placed{
			"-5": {behind: true}, "50": {behind: true}, "51": {chunk: first}, "70": {chunk: first}, "71": {}, "100": {},
			"101": {}, "200": {}, "201": {behind: true}, "300": {behind: true}, "350": {behind: true}, "351": {chunk: last}, "99999": {chunk: last},
		}},
		{composite, map[string]placed{
			"4\x001000": {behind: true}, "5\x007": {behind: true}, "5\x008": {chunk: mid}, "8\x002": {chunk: mid}, "8\x003": {},
			"10\x00999": {}, "11\x00-1": {},
		}},
	} {
		var keys []string
		for k := range c.keys {
			keys = append(keys, k)
		}
		got, err := r.place(t.Context(), c.st, keys)
		if err != nil {
			t.Fatal(err)
		}
		for i, k := range keys {
			if got[i] != c.keys[k] {
				t.Errorf("%s: %q placed %+v, want %+v", c.st.t.name, strings.Split(k, "\x00"), got[i], c.keys[k])
			}
		}
	}
	if _, err := r.place(t.Context(), single, []string{"x"}); err == nil {
		t.Error("a key that isn't an integer was placed")
	}
}

// A change to a key the snapshot has read is emitted; one to a key no chunk
// has read waits for the chunk that will, and so its transaction must be
// visible to that chunk's snapshot. A change to a held chunk's key that its
// snapshot saw is in the chunk's row; one it didn't see follows the chunk's
// row of the key, if it has one, once the chunk is emitted.
func TestApplyChangesAgainstAHeldChunk(t *testing.T) {
	st := &cdcStream{t: table{name: "users", cols: []sourceColumn{{name: "id", oid: 23}}, key: []int{0}}, ints: true, conv: []func([]byte) []byte{nil}}
	st.b = newChangeBatcher(memory.DefaultAllocator, st.t.cols)
	defer st.b.release()
	st.ranges = []*snapshotRange{{After: []string{"2"}}}
	// It sees 101, finished, and not 105, running, or 120, after it.
	c := &chunk{stream: st, rng: st.ranges[0], snap: pgSnapshot{xmin: 100, xmax: 110, xip: map[uint64]bool{105: true}},
		rows: map[string][][]byte{"4": {[]byte("4")}, "6": {[]byte("6")}}, keys: []string{"4", "6"}, last: []string{"20"}, full: true, lsn: 1000}
	out := &events{}
	r := &cdcReader{s: &Source{cfg: SourceConfig{BatchBytes: 1 << 20}}, out: out, streams: []*cdcStream{st}, chunks: []*chunk{c}}
	in := func(xid uint32, commit pglogrepl.LSN, ids ...string) *txn {
		t.Helper()
		r.txn = &txn{xid: xid, commit: commit}
		for _, id := range ids {
			if err := r.read(t.Context(), st, id, source.OpInsert, [][]byte{[]byte(id)}, nil); err != nil {
				t.Fatal(err)
			}
		}
		x := r.txn
		r.txn = nil
		return x
	}
	seen := in(101, 200, "4", "1")
	running := in(105, 300, "6", "8", "30")
	after := in(120, 400, "6", "2")
	if err := st.b.flush("users", out); err != nil {
		t.Fatal(err)
	}
	if want := []string{"users:1,2"}; !slices.Equal(out.log, want) {
		t.Errorf("emitted %q, want only the changes behind the snapshot, %q", out.log, want)
	}
	var after6 []string
	for _, v := range c.after {
		after6 = append(after6, v.op+" "+string(v.vals[0])+" "+v.seq.BigInt().String())
	}
	if want := []string{"insert 6 300000000000000000000", "insert 6 300000000000000000001", "insert 8 300000000000000000002", "insert 6 400000000000000000001"}; !slices.Equal(after6, want) {
		t.Errorf("the chunk emits after its rows %q, want %q", after6, want)
	}
	if _, ok := c.rows["6"]; ok {
		t.Error("the chunk kept its row of 6, which goes before the change its snapshot didn't see")
	}
	if !slices.Equal(r.unseen, []*txn{running, after}) || !seen.seen || seen.listed {
		t.Errorf("transactions a chunk must see: %v", r.unseen)
	}
	if !slices.Equal(running.ahead, []*cdcStream{st}) || !slices.Equal(running.held, []*chunk{c}) || len(after.ahead) != 0 || !slices.Equal(after.held, []*chunk{c}) {
		t.Errorf("105 is owed to %v and %v, 120 to %v and %v", running.ahead, running.held, after.ahead, after.held)
	}
	// Once the chunk passes, a read resumed after it reads its keys from the
	// log: only the key no chunk has read is owed.
	c.done = true
	if !owed(running, st) || owed(after, st) || owed(after, nil) {
		t.Errorf("owed once the chunk passed: 105 %v, 120 %v", owed(running, st), owed(after, st))
	}
	st.ranges = nil
	if owed(running, nil) {
		t.Error("owed once the snapshot is read")
	}
}

// A change held with a chunk keeps its values though the log's next message
// reuses the buffer the change was read from, as pgx's does.
func TestHeldChangesOutliveTheLogsBuffer(t *testing.T) {
	st := &cdcStream{t: table{name: "users", cols: []sourceColumn{{name: "id", oid: 23}}, key: []int{0}}, ints: true, conv: []func([]byte) []byte{nil}}
	st.b = newChangeBatcher(memory.DefaultAllocator, st.t.cols)
	defer st.b.release()
	st.ranges = []*snapshotRange{{}}
	c := &chunk{stream: st, rng: st.ranges[0], snap: pgSnapshot{xmin: 100, xmax: 110, xip: map[uint64]bool{}}, rows: map[string][][]byte{}, lsn: 1 << 40}
	r := &cdcReader{s: &Source{cfg: SourceConfig{BatchBytes: 1 << 20}}, out: &events{}, streams: []*cdcStream{st}, chunks: []*chunk{c},
		rels: map[uint32]*relation{7: {stream: st, at: []int{0}, key: []int{0}, conv: []func([]byte) []byte{nil}, names: []string{"id"}}}}
	buf := make([]byte, 64)
	for _, m := range [][]byte{
		logMessage('B', pglogrepl.LSN(300), pglogrepl.LSN(0), uint32(120)),
		logMessage('I', uint32(7), "6"),
		logMessage('I', uint32(7), "8"),
		logMessage('C', uint8(0), pglogrepl.LSN(300), pglogrepl.LSN(350), pglogrepl.LSN(0)),
	} {
		n := copy(buf, m)
		if err := r.message(t.Context(), buf[:n]); err != nil {
			t.Fatal(err)
		}
		for i := range buf {
			buf[i] = 'x'
		}
	}
	var held []string
	for _, v := range c.after {
		held = append(held, string(v.vals[0]))
	}
	if !slices.Equal(held, []string{"6", "8"}) {
		t.Errorf("the chunk holds %q", held)
	}
}

// A chunk passes at the begin of a transaction that committed at or after
// its position, before that transaction's changes: they're behind it then,
// and the chunk holds none of them.
func TestAChunkPassesBeforeATransactionAfterIt(t *testing.T) {
	st := &cdcStream{t: table{name: "users", cols: []sourceColumn{{name: "id", oid: 23}}, key: []int{0}}, ints: true, conv: []func([]byte) []byte{nil}}
	st.b = newChangeBatcher(memory.DefaultAllocator, st.t.cols)
	defer st.b.release()
	st.ranges = []*snapshotRange{{}}
	c := &chunk{stream: st, rng: st.ranges[0], snap: pgSnapshot{xmin: 100, xmax: 110, xip: map[uint64]bool{}}, rows: map[string][][]byte{"4": {[]byte("4")}}, keys: []string{"4"}, lsn: 1000}
	out := &events{}
	r := &cdcReader{s: &Source{cfg: SourceConfig{BatchBytes: 1 << 20}}, out: out, slot: "s", streams: []*cdcStream{st}, chunks: []*chunk{c}, pos: 500,
		rels: map[uint32]*relation{7: {stream: st, at: []int{0}, key: []int{0}, conv: []func([]byte) []byte{nil}, names: []string{"id"}}}}
	for _, m := range [][]byte{
		logMessage('B', pglogrepl.LSN(1000), pglogrepl.LSN(0), uint32(120)),
		logMessage('I', uint32(7), "6"),
		logMessage('I', uint32(7), "8"),
		logMessage('C', uint8(0), pglogrepl.LSN(1000), pglogrepl.LSN(1100), pglogrepl.LSN(0)),
	} {
		if err := r.message(t.Context(), m); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.b.flush("users", out); err != nil {
		t.Fatal(err)
	}
	state, _ := json.Marshal(cdcState{Slot: "s", LSN: 500})
	if want := []string{"users:4", "users@" + string(state), "users:6,8"}; !slices.Equal(out.log, want) {
		t.Errorf("emitted %q, want %q", out.log, want)
	}
	if len(c.after) != 0 || len(r.chunks) != 0 || len(st.ranges) != 0 {
		t.Errorf("the chunk holds %d changes; %d chunks held, %d ranges left", len(c.after), len(r.chunks), len(st.ranges))
	}
}

// A transaction new snapshots don't see in time fails the read for good
// when the read's own walsender is a synchronous standby it waits for, which
// confirms only what the pipeline has checkpointed, and retryably otherwise,
// naming the walsender where it's among a quorum.
func TestUnseenTransactionsErrors(t *testing.T) {
	for _, c := range []struct {
		state, app string
		code       protocolv1.ErrorCode
		names      bool
	}{
		{"sync", "syncme", protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, true},
		{"sync", "", protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, true},
		{"quorum", "q", protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, true},
		{"potential", "p", protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, false},
		{"async", "", protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, false},
		{"", "", protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, false},
	} {
		err := unseenError(42, c.state, c.app)
		var coded interface{ ErrorCode() protocolv1.ErrorCode }
		if !errors.As(err, &coded) || coded.ErrorCode() != c.code {
			t.Errorf("%s: %v, want %v", c.state, err, c.code)
		}
		if named := strings.Contains(err.Error(), "synchronous_standby_names") && strings.Contains(err.Error(), fmt.Sprintf("application_name %q", c.app)); named != c.names || !strings.Contains(err.Error(), "transaction 42") {
			t.Errorf("%s: %v", c.state, err)
		}
	}
}
