package postgres

import (
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/openetl-io/openetl-connectors/keysplit"
)

func TestCutRanges(t *testing.T) {
	bounds := make([]string, 101)
	for i := range bounds {
		bounds[i] = fmt.Sprint(i * 10)
	}
	cuts := func(r []keyRange) []string {
		var out []string
		for i, k := range r {
			if k.Hi == nil {
				if i != len(r)-1 {
					t.Errorf("range %d of %d has no end", i, len(r))
				}
				continue
			}
			out = append(out, *k.Hi)
			if r[i+1].Lo != k.Hi {
				t.Errorf("range %d doesn't start where %d ends", i+1, i)
			}
		}
		if len(r) > 0 && r[0].Lo != nil {
			t.Errorf("the first range starts at %s", *r[0].Lo)
		}
		return out
	}
	for _, c := range []struct {
		name   string
		bounds []string
		tuples float64
		rows   int
		want   []string
	}{
		{name: "few rows", bounds: bounds, tuples: 100, rows: 100},
		{name: "one chunk and a bit", bounds: bounds, tuples: 101, rows: 100, want: []string{"500"}},
		{name: "four", bounds: bounds, tuples: 400, rows: 100, want: []string{"250", "500", "750"}},
		{name: "no statistics", tuples: 1e9, rows: 100},
		{name: "never analyzed", bounds: bounds, tuples: -1, rows: 100},
		{name: "one bucket", bounds: []string{"a", "b"}, tuples: 1e9, rows: 100},
		{name: "all the buckets", bounds: []string{"a", "b", "c", "d"}, tuples: 1e9, rows: 100, want: []string{"b", "c"}},
		{name: "repeated bounds", bounds: []string{"a", "a", "a", "b", "b", "c", "d"}, tuples: 1e9, rows: 100, want: []string{"b", "c"}},
	} {
		if got := cuts(cutRanges(c.bounds, c.tuples, c.rows)); !slices.Equal(got, c.want) {
			t.Errorf("%s: cuts %q, want %q", c.name, got, c.want)
		}
	}
	if got := cutRanges(bounds, 1e12, 100); len(got) != keysplit.MaxRanges {
		t.Errorf("%d ranges at most, want %d", len(got), keysplit.MaxRanges)
	}
}

func TestIntRemainder(t *testing.T) {
	text := func(s string) *string { return &s }
	for _, c := range []struct {
		name      string
		r         keyRange
		pos       *string
		single    bool
		low, high string
	}{
		{name: "nothing known", single: true},
		{name: "bounds", r: keyRange{Lo: text("5"), Hi: text("9")}, single: true, low: "6", high: "9"},
		{name: "a key read", r: keyRange{Lo: text("5"), Hi: text("9")}, pos: text("7"), single: true, low: "8", high: "9"},
		{name: "more keys of its first value", r: keyRange{Lo: text("5"), Hi: text("9")}, pos: text("7"), low: "7", high: "9"},
		{name: "past int8", r: keyRange{Lo: text("9223372036854775807"), Hi: text("-9223372036854775808")}, single: true, low: "9223372036854775808", high: "-9223372036854775808"},
	} {
		low, high, err := intRemainder(c.r, c.pos, c.single)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		lowText, highText := "", ""
		if low != nil {
			lowText = low.String()
		}
		if high != nil {
			highText = high.String()
		}
		if lowText != c.low || highText != c.high {
			t.Errorf("%s: [%s, %s], want [%s, %s]", c.name, lowText, highText, c.low, c.high)
		}
	}
	if _, _, err := intRemainder(keyRange{Hi: text("x")}, nil, true); err == nil {
		t.Error("read x as an integer")
	}
}

// A resplit of an (int, text) key, asked at any position of a range, leaves
// the rows after it to the two parts, each to one: the rest of the
// position's first value stays below, and a range with no other value left
// isn't divided.
func TestResplitACompositeKeysRemainder(t *testing.T) {
	type row struct {
		a int
		b string
	}
	var rows []row
	for a := range 10 {
		for _, b := range []string{"x", "y", "z"} {
			rows = append(rows, row{a, b})
		}
	}
	atoi := func(s *string) int {
		n, err := strconv.Atoi(*s)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	// What a read of st reads, by its where and afterKey.
	reads := func(st sourceState, x row) bool {
		if st.Lo != nil && x.a <= atoi(st.Lo) || st.Hi != nil && x.a > atoi(st.Hi) {
			return false
		}
		if st.After == nil {
			return true
		}
		at := atoi(&st.After[0])
		return x.a > at || x.a == at && x.b > st.After[1]
	}
	tbl := table{name: "t", cols: []sourceColumn{{name: "a", oid: pgtype.Int4OID}, {name: "b", oid: pgtype.TextOID}}, order: []int{0, 1}}
	seven := "7"
	for _, lo := range []string{"", "2"} {
		r := keyRange{Hi: &seven}
		if lo != "" {
			r.Lo = &lo
		}
		for _, p := range rows {
			if !reads(sourceState{keyRange: r}, p) {
				continue
			}
			st := sourceState{After: []string{strconv.Itoa(p.a), p.b}, keyRange: r}
			var want []row
			for _, x := range rows {
				if reads(st, x) {
					want = append(want, x)
				}
			}
			var reading keysplit.Reading
			reading.Asked.Store(true)
			added, err := (&Source{}).resplit(t.Context(), nil, tbl, &reading, &st, false, st.position(), false)
			if err != nil {
				t.Fatal(err)
			}
			parts := []sourceState{st}
			for _, sp := range added {
				var upper sourceState
				if err := json.Unmarshal(sp.State, &upper); err != nil {
					t.Fatal(err)
				}
				parts = append(parts, upper)
			}
			var got []row
			for _, part := range parts {
				for _, x := range rows {
					if reads(part, x) {
						got = append(got, x)
					}
				}
			}
			slices.SortFunc(got, func(x, y row) int { return cmp.Or(cmp.Compare(x.a, y.a), strings.Compare(x.b, y.b)) })
			if !slices.Equal(got, want) {
				t.Errorf("(%s, 7] after %v: the parts read %v, want %v", lo, p, got, want)
			}
			divides := p.a < 7
			if (len(added) == 1) != divides || reading.Over.Load() == divides {
				t.Errorf("(%s, 7] after %v: added %d, over %v", lo, p, len(added), reading.Over.Load())
			}
		}
	}
}

// A state without a range is spelled as it was before ranges, and one with
// a range keeps it.
func TestStatesSpelling(t *testing.T) {
	hi := "9"
	for _, c := range []struct {
		st   sourceState
		want string
	}{
		{sourceState{After: []string{"1"}}, `{"after":["1"]}`},
		{sourceState{Done: true}, `{"done":true}`},
		{sourceState{After: []string{"1"}, keyRange: keyRange{Hi: &hi}}, `{"after":["1"],"hi":"9"}`},
	} {
		b, err := json.Marshal(c.st)
		if err != nil || string(b) != c.want {
			t.Errorf("%s, %v, want %s", b, err, c.want)
		}
		var back sourceState
		if err := json.Unmarshal(b, &back); err != nil {
			t.Error(err)
		}
		if again, _ := json.Marshal(back); string(again) != c.want {
			t.Errorf("read back as %s", again)
		}
	}
	for _, c := range []struct {
		st   cdcStream
		want string
	}{
		{cdcStream{}, `null`},
		{cdcStream{ranges: []*snapshotRange{{}}}, `{}`},
		{cdcStream{ranges: []*snapshotRange{{After: []string{"1"}}}}, `{"after":["1"]}`},
		{cdcStream{ranges: []*snapshotRange{{keyRange: keyRange{Lo: &hi}}}}, `{"ranges":[{"lo":"9"}]}`},
		{cdcStream{ranges: []*snapshotRange{{keyRange: keyRange{Hi: &hi}, After: []string{"1"}}, {keyRange: keyRange{Lo: &hi}}}}, `{"ranges":[{"hi":"9","after":["1"]},{"lo":"9"}]}`},
	} {
		b, err := json.Marshal(c.st.cursor())
		if err != nil || string(b) != c.want {
			t.Errorf("%s, %v, want %s", b, err, c.want)
		}
	}
}

// Each new chunk loses the keys of the transactions its own snapshot didn't
// see. A transaction goes once every chunk's snapshot sees it from then on.
func TestReconcileChunks(t *testing.T) {
	st, other := &cdcStream{}, &cdcStream{}
	newChunk := func(s *cdcStream, snap pgSnapshot, keys ...string) *chunk {
		c := &chunk{stream: s, snap: snap, rows: map[string][][]byte{}, keys: keys, lsn: pglogrepl.LSN(1)}
		for _, k := range keys {
			c.rows[k] = nil
		}
		return c
	}
	// Transaction 105 is running in a's snapshot, finished in b's; 101 is
	// finished in both, though above a's xmin; 90 is below both.
	a := newChunk(st, pgSnapshot{xmin: 100, xmax: 110, xip: map[uint64]bool{105: true}}, "a1", "a2")
	b := newChunk(st, pgSnapshot{xmin: 103, xmax: 112, xip: map[uint64]bool{}}, "b1", "b2")
	c := newChunk(other, pgSnapshot{xmin: 100, xmax: 110, xip: map[uint64]bool{105: true}}, "a1")
	x105 := &txn{xid: 105, keys: map[*cdcStream][]string{st: {"a1", "b1"}}}
	x101 := &txn{xid: 101, keys: map[*cdcStream][]string{st: {"a2", "b2"}}}
	x90 := &txn{xid: 90, keys: map[*cdcStream][]string{st: {"a2", "b2"}}}
	r := &cdcReader{recent: []*txn{x90, x101, x105}}
	r.reconcile([]*chunk{a, b, c})
	if _, ok := a.rows["a1"]; ok {
		t.Error("a kept the key of a transaction its snapshot didn't see")
	}
	if _, ok := a.rows["a2"]; !ok {
		t.Error("a lost a key of transactions its snapshot saw")
	}
	if _, ok := b.rows["b1"]; !ok {
		t.Error("b lost a key of a transaction its snapshot saw")
	}
	if _, ok := c.rows["a1"]; !ok {
		t.Error("c, of another stream, lost a key the transaction changed in the first")
	}
	if !slices.Equal(r.recent, []*txn{x101, x105}) {
		t.Errorf("kept %d transactions, want those some chunk's xmin doesn't pass", len(r.recent))
	}
}

func TestSnapshotFullIDs(t *testing.T) {
	// full's ID, by signed arithmetic.
	want := func(xmax uint64, xid uint32) uint64 {
		return uint64(int64(xmax) + int64(int32(xid-uint32(xmax))))
	}
	for _, xmax := range []uint64{0, 3, 1<<31 - 1, 1 << 31, 1<<32 - 1, 1 << 32, 5<<32 + 7, 1<<63 + 9, math.MaxUint64} {
		for _, xid := range []uint32{0, 1, 2, 7, 1<<31 - 1, 1 << 31, 1<<31 + 1, math.MaxUint32, uint32(xmax), uint32(xmax) + 1, uint32(xmax) - 1} {
			if got := (pgSnapshot{xmax: xmax}).full(xid); got != want(xmax, xid) {
				t.Errorf("xmax %d, xid %d: %d, want %d", xmax, xid, got, want(xmax, xid))
			}
		}
	}
	if got := (pgSnapshot{xmax: 5<<32 + 10}).full(4); got != 5<<32+4 {
		t.Errorf("an ID just behind xmax: %d", got)
	}
	if got := (pgSnapshot{xmax: 5<<32 + 10}).full(math.MaxUint32); got != 5<<32-1 {
		t.Errorf("an ID behind xmax in the epoch before: %d", got)
	}
	if got := (pgSnapshot{xmax: 5<<32 - 2}).full(3); got != 5<<32+3 {
		t.Errorf("an ID ahead of xmax in the next epoch: %d", got)
	}
}

// A state with a range or a position holds only in the order it was made
// in, a split not yet started included.
func TestSameOrder(t *testing.T) {
	lo := "50"
	cols := []sourceColumn{{name: "a"}, {name: "b"}}
	byB := table{name: "t", cols: cols, order: []int{1}}
	byKey := table{name: "t", cols: cols, key: []int{0}, order: []int{0}}
	unordered := table{name: "t", cols: cols}
	for _, c := range []struct {
		t    table
		st   sourceState
		lost bool
	}{
		{byB, sourceState{}, false},
		{byB, sourceState{Key: []string{"a"}}, false},
		{byB, sourceState{keyRange: keyRange{Lo: &lo}, Key: []string{"b"}}, false},
		{byB, sourceState{After: []string{"1"}, Key: []string{"b"}}, false},
		{byB, sourceState{keyRange: keyRange{Lo: &lo}}, true},
		{byB, sourceState{keyRange: keyRange{Hi: &lo}, Key: []string{"a"}}, true},
		{byB, sourceState{After: []string{"1"}, Key: []string{"a"}}, true},
		{byKey, sourceState{keyRange: keyRange{Lo: &lo}}, false},
		{byKey, sourceState{keyRange: keyRange{Lo: &lo}, Key: []string{"b"}}, true},
		{unordered, sourceState{keyRange: keyRange{Lo: &lo}, Key: []string{"b"}}, true},
		// A table that lost its primary key, read in its order, as states
		// from 0.1.0 were, which name no key.
		{unordered, sourceState{After: []string{"1"}}, true},
		{unordered, sourceState{keyRange: keyRange{Lo: &lo}}, true},
		{unordered, sourceState{keyRange: keyRange{Hi: &lo}, After: []string{"1"}}, true},
		{unordered, sourceState{}, false},
		{unordered, sourceState{Done: true}, false},
		{byKey, sourceState{After: []string{"1"}}, false},
		// A state of a read without an order, which a table read in one now
		// takes as ended.
		{byKey, sourceState{Done: true}, false},
		{byB, sourceState{Done: true}, false},
	} {
		if err := sameOrder(c.t, c.st); (err != nil) != c.lost {
			b, _ := json.Marshal(c.st)
			t.Errorf("order %v, state %s: %v", c.t.orderKey(), b, err)
		}
	}
}
