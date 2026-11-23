//go:build integration

package postgres

import (
	"slices"
	"testing"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// readIncremental reads stream incrementally by cursor from state.
func readIncremental(t *testing.T, s *Source, stream, cursor, state string) (*capture, error) {
	t.Helper()
	cs := source.ConfiguredStream{Name: stream, SyncMode: source.Incremental, Cursor: cursor}
	if state != "" {
		cs.State = []byte(state)
	}
	c := &capture{}
	t.Cleanup(c.release)
	return c, s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c)
}

func mustReadIncremental(t *testing.T, s *Source, stream, cursor, state string) *capture {
	t.Helper()
	c, err := readIncremental(t, s, stream, cursor, state)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSourceDeclaresIncremental(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.stamped (id int PRIMARY KEY, at timestamptz)")
	e.exec("CREATE TABLE $schema.blobs (b bytea)")
	cat, err := e.source("").Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	modes := map[string][]source.SyncMode{}
	for _, s := range cat.Streams {
		modes[s.Name] = s.SyncModes
	}
	if got := modes["stamped"]; !slices.Equal(got, []source.SyncMode{source.FullRefresh, source.Incremental, source.CDC}) {
		t.Errorf("stamped reads with %v", got)
	}
	if got := modes["blobs"]; !slices.Equal(got, []source.SyncMode{source.FullRefresh}) {
		t.Errorf("blobs, with no column to order by, reads with %v", got)
	}
}

// A read goes up to the cursor's highest value when it starts, in chunks
// ordered by (cursor, key), each a resume point; it completes with that
// value as the next read's floor, which reads its rows again.
func TestSourceReadsIncrementally(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, seq bigint)")
	e.exec("INSERT INTO $schema.t VALUES (1, 10), (2, 20), (3, 20), (4, 30), (5, NULL)")
	s := e.source(`, "chunkRows": 2`)

	c := mustReadIncremental(t, s, "t", "seq", "")
	want := []string{
		"t:2 rows", `t@{"after":["20","2"],"until":"30"}`, "t:2 rows", `t@{"after":["30","4"],"until":"30"}`,
		`t@{"after":["30","4"],"until":"30"}`, `t complete{"since":"30"}`,
	}
	if !slices.Equal(c.log, want) {
		t.Errorf("emitted %q\nwant %q", c.log, want)
	}

	c = mustReadIncremental(t, s, "t", "seq", `{"after":["20","2"],"until":"30"}`)
	if got := c.column("t", "id"); !slices.Equal(got, []string{"3", "4"}) {
		t.Errorf("resumed after (20, 2): %v", got)
	}

	e.exec("INSERT INTO $schema.t VALUES (6, 40), (7, 25)")
	e.exec("UPDATE $schema.t SET seq = 50 WHERE id = 1")
	c = mustReadIncremental(t, s, "t", "seq", `{"since":"30"}`)
	if got := c.column("t", "id"); !slices.Equal(got, []string{"4", "6", "1"}) {
		t.Errorf("from 30: %v, want the row at 30 again, then 40 and 50; 25 is below the floor", got)
	}
	if last := c.log[len(c.log)-1]; last != `t complete{"since":"50"}` {
		t.Errorf("completed with %s", last)
	}
}

func TestSourceReadsAnEmptyTableIncrementally(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, at date)")
	s := e.source("")
	if c := mustReadIncremental(t, s, "t", "at", ""); !slices.Equal(c.log, []string{"t complete{}"}) {
		t.Errorf("an empty table: %q", c.log)
	}
	if c := mustReadIncremental(t, s, "t", "at", `{"since":"2026-11-04"}`); !slices.Equal(c.log, []string{`t complete{"since":"2026-11-04"}`}) {
		t.Errorf("a table emptied since: %q", c.log)
	}
}

// Cursor values pass through state as text, and back as the column's type.
func TestSourceIncrementalCursorTypes(t *testing.T) {
	e := newSourceEnv(t)
	e.exec(`CREATE TABLE $schema.t (id int PRIMARY KEY, tz timestamptz, ts timestamp, d date, n numeric(10, 3), s text, v varchar(5))`)
	e.exec(`INSERT INTO $schema.t VALUES
		(1, '2026-11-04 10:00:00.5+05', '2026-11-04 10:00:00.5', '2026-11-04', 1.5, 'a', 'a'),
		(2, '2026-11-04 10:00:00.25Z', '2026-11-04 10:00:01', '2026-11-05', 2.25, 'b', 'b')`)
	s := e.source("")
	for _, col := range []string{"tz", "ts", "d", "n", "s", "v"} {
		c := mustReadIncremental(t, s, "t", col, "")
		next := c.log[len(c.log)-1]
		c = mustReadIncremental(t, s, "t", col, next[len("t complete"):])
		if got := c.column("t", "id"); !slices.Equal(got, []string{"2"}) {
			t.Errorf("%s: from %s, read %v, want the row at the floor", col, next, got)
		}
	}
}

func TestSourceReadsATableWithoutAKeyIncrementally(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (seq int, v text)")
	e.exec("INSERT INTO $schema.t VALUES (1, 'a'), (2, 'b'), (2, 'c')")
	s := e.source(`, "chunkRows": 1`)
	c := mustReadIncremental(t, s, "t", "seq", `{"since":"2"}`)
	if want := []string{"t:2 rows", `t@{"done":true,"since":"2","until":"2"}`, `t complete{"since":"2"}`}; !slices.Equal(c.log, want) {
		t.Errorf("emitted %q, want %q", c.log, want)
	}
	c = mustReadIncremental(t, s, "t", "seq", `{"done":true,"since":"2","until":"2"}`)
	if want := []string{`t complete{"since":"2"}`}; !slices.Equal(c.log, want) {
		t.Errorf("after done: %q", c.log)
	}
}

func TestSourceRefusesBadCursors(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, b boolean)")
	s := e.source("")
	for _, cursor := range []string{"", "missing", "b"} {
		_, err := readIncremental(t, s, "t", cursor, "")
		if code := codeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID {
			t.Errorf("cursor %q: %v (%v), want CONFIG_INVALID", cursor, err, code)
		}
	}
}

// A read given where an incremental read completed reads up to that floor,
// not to the cursor's highest value now, and completes there; one given the
// completion of a read that found no rows reads none.
func TestSourceIncrementalCompletesWhereItIsAsked(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, seq bigint)")
	e.exec("INSERT INTO $schema.t VALUES (1, 10), (2, 20), (3, 30)")
	s := e.source(`, "chunkRows": 2`)
	if c := mustReadIncremental(t, s, "t", "seq", `{"since":"20"}`); c.log[len(c.log)-1] != `t complete{"since":"30"}` {
		t.Fatalf("the first read ended with %q", c.log)
	}
	e.exec("INSERT INTO $schema.t VALUES (4, 40)")
	read := func(state, completeAt string) *capture {
		t.Helper()
		cs := source.ConfiguredStream{Name: "t", SyncMode: source.Incremental, Cursor: "seq", CompleteAt: []byte(completeAt)}
		if state != "" {
			cs.State = []byte(state)
		}
		c := &capture{}
		t.Cleanup(c.release)
		if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	c := read(`{"since":"20"}`, `{"since":"30"}`)
	if got := c.column("t", "id"); !slices.Equal(got, []string{"2", "3"}) {
		t.Errorf("a read to 30 read %v", got)
	}
	if last := c.log[len(c.log)-1]; last != `t complete{"since":"30"}` {
		t.Errorf("a read to 30 ended with %s", last)
	}
	c = read(`{"after":["20","2"],"until":"30"}`, `{"since":"30"}`)
	if got := c.column("t", "id"); !slices.Equal(got, []string{"3"}) || c.log[len(c.log)-1] != `t complete{"since":"30"}` {
		t.Errorf("a read to 30 from a resume point read %v, and ended with %q", got, c.log)
	}
	if c := read("", "{}"); !slices.Equal(c.log, []string{"t complete{}"}) {
		t.Errorf("a read to where an empty table's read completed: %q", c.log)
	}
}

// A cursor's NaN and infinities are never read, as its nulls aren't, and
// don't hold the next read's floor above the values written after them,
// whole or in splits. A floor at one, as earlier versions took, reads from
// the start.
func TestIncrementalSkipsNonFiniteCursors(t *testing.T) {
	e := newSourceEnv(t)
	s := e.source(`, "chunkRows": 10`)
	for typ, c := range map[string]struct{ rows, later, floor, stuck string }{
		"numeric":     {"(1, 1), (2, 'NaN'), (3, 'Infinity'), (4, '-Infinity'), (5, NULL)", "(6, 7)", "1", "NaN"},
		"date":        {"(1, '2020-01-01'), (2, 'infinity'), (3, '-infinity')", "(6, '2021-01-01')", "2020-01-01", "infinity"},
		"timestamp":   {"(1, '2020-01-01'), (2, 'infinity'), (3, '-infinity')", "(6, '2021-01-01')", "2020-01-01 00:00:00", "infinity"},
		"timestamptz": {"(1, '2020-01-01Z'), (2, 'infinity'), (3, '-infinity')", "(6, '2021-01-01Z')", "2020-01-01 00:00:00+00", "infinity"},
	} {
		e.exec("CREATE TABLE $schema." + typ + "s (id int PRIMARY KEY, v " + typ + ")")
		e.exec("INSERT INTO $schema." + typ + "s VALUES " + c.rows)
		first := mustReadIncremental(t, s, typ+"s", "v", "")
		if got, state := first.column(typ+"s", "id"), first.last(typ+"s"); !slices.Equal(got, []string{"1"}) || state != `{"since":"`+c.floor+`"}` {
			t.Errorf("%s: read %q, then %s", typ, got, state)
			continue
		}
		e.exec("INSERT INTO $schema." + typ + "s VALUES " + c.later)
		if got := mustReadIncremental(t, s, typ+"s", "v", first.last(typ+"s")).column(typ+"s", "id"); !slices.Equal(got, []string{"1", "6"}) {
			t.Errorf("%s: the next read read %q", typ, got)
		}
		if got := mustReadIncremental(t, s, typ+"s", "v", `{"since":"`+c.stuck+`"}`).column(typ+"s", "id"); !slices.Equal(got, []string{"1", "6"}) {
			t.Errorf("%s: a read from %s read %q", typ, c.stuck, got)
		}
	}

	e.exec("CREATE TABLE $schema.many (id int PRIMARY KEY, v numeric)")
	e.exec("INSERT INTO $schema.many SELECT i, i FROM generate_series(1, 200) i")
	e.exec("INSERT INTO $schema.many VALUES (201, 'NaN'), (202, 'Infinity')")
	e.exec("ANALYZE $schema.many")
	splits := e.mustSplits(s, source.ConfiguredStream{Name: "many", SyncMode: source.Incremental, Cursor: "v"}, nil)
	if len(splits) < 2 {
		t.Fatalf("%d splits", len(splits))
	}
	for _, sp := range splits {
		if st := parseState(t, string(sp.State)); st.Until == nil || *st.Until != "200" {
			t.Errorf("split %q's state %s", sp.ID, sp.State)
		}
	}
}

// readIncrementalUntil reads stream incrementally by cursor from state,
// asked to end at until.
func readIncrementalUntil(t *testing.T, s *Source, stream, cursor, state, until string) (*capture, error) {
	t.Helper()
	cs := source.ConfiguredStream{Name: stream, SyncMode: source.Incremental, Cursor: cursor, Until: []byte(until)}
	if state != "" {
		cs.State = []byte(state)
	}
	c := &capture{}
	t.Cleanup(c.release)
	return c, s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c)
}

// A read given a resume point an incremental read sent goes up to that
// read's highest cursor value, not the column's highest now, emits no row
// past the resume point's, though its chunks end elsewhere, then that resume
// point, byte for byte, and completes with it: as a redo of part of an epoch
// asks, from where the epoch started, a completion's state, a split's or
// none, in states as 0.1.0 wrote them, which name no key.
func TestSourceIncrementalStopsWhereItIsAsked(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, seq bigint)")
	e.exec("INSERT INTO $schema.t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
	e.exec("CREATE TABLE $schema.k (seq int, v text)")
	e.exec("INSERT INTO $schema.k VALUES (1, 'a'), (2, 'b')")
	first := mustReadIncremental(t, e.source(`, "chunkRows": 2`), "t", "seq", `{"since":"10"}`)
	until := `{"after":["20","2"],"since":"10","until":"40"}`
	if !slices.Contains(first.log, "t@"+until) {
		t.Fatalf("the first read emitted %q", first.log)
	}
	keyless := mustReadIncremental(t, e.source(""), "k", "seq", `{"since":"1"}`)
	if !slices.Contains(keyless.log, `k@{"done":true,"since":"1","until":"2"}`) {
		t.Fatalf("the first read of k emitted %q", keyless.log)
	}
	// A row past the resume point and under the read's highest value, and
	// one above it, which raises the column's highest.
	e.exec("INSERT INTO $schema.t VALUES (5, 35), (6, 50)")
	e.exec("INSERT INTO $schema.k VALUES (3, 'c')")

	for _, c := range []struct {
		name, stream, chunkRows, state, until string
		ids                                   []string
	}{
		{"whole, from a completion", "t", "2", `{"since":"10"}`, until, []string{"1", "2"}},
		{"in chunks that end past it", "t", "3", `{"since":"10"}`, until, []string{"1", "2"}},
		{"from no state", "t", "3", "", `{"after":["20","2"],"until":"40"}`, []string{"1", "2"}},
		{"from a resume point", "t", "2", `{"after":["10","1"],"since":"10","until":"40"}`, `{"after":["30","3"],"since":"10","until":"40"}`, []string{"2", "3"}},
		{"a split's", "t", "3", `{"since":"10","until":"40","hi":"4"}`, `{"after":["20","2"],"since":"10","until":"40","hi":"4"}`, []string{"1", "2"}},
		{"where no row was read", "t", "3", `{"since":"100"}`, `{"since":"100","until":"40"}`, nil},
		{"without a read order", "k", "3", `{"since":"1"}`, `{"done":true,"since":"1","until":"2"}`, []string{"1", "2"}},
	} {
		got, err := readIncrementalUntil(t, e.source(`, "chunkRows": `+c.chunkRows), c.stream, "seq", c.state, c.until)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		col := "id"
		if c.stream == "k" {
			col = "seq"
		}
		n := len(got.log)
		if ids := got.column(c.stream, col); !slices.Equal(ids, c.ids) || n < 2 || got.log[n-2] != c.stream+"@"+c.until || got.log[n-1] != c.stream+" complete"+c.until {
			t.Errorf("%s: read %v, emitted %q", c.name, ids, got.log)
		}
	}

	// An until in another order than the table's, or of another width, isn't
	// a position of this read.
	if _, err := readIncrementalUntil(t, e.source(""), "t", "seq", `{"since":"10"}`, `{"after":["20","2"],"key":["seq"],"since":"10","until":"40"}`); codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("an until in another order: %v", err)
	}
	if _, err := readIncrementalUntil(t, e.source(""), "t", "seq", `{"since":"10"}`, `{"after":["20"],"since":"10","until":"40"}`); err == nil {
		t.Error("an until with one column of two")
	}
}

// A read given the resume point of a read without a read order, sent once
// it had read every row up to its highest cursor value, reads those rows
// again though the table has a primary key since, then sends that resume
// point and completes with it: the resume point holds after its rows, not
// before them.
func TestSourceIncrementalReadsAgainAReadWithoutAnOrder(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.k (seq int, v text)")
	e.exec("INSERT INTO $schema.k VALUES (1, 'a'), (2, 'b')")
	until := `{"done":true,"until":"2"}`
	if first := mustReadIncremental(t, e.source(""), "k", "seq", ""); !slices.Contains(first.log, "k@"+until) {
		t.Fatalf("the first read emitted %q", first.log)
	}
	e.exec("ALTER TABLE $schema.k ADD PRIMARY KEY (seq)")
	e.exec("INSERT INTO $schema.k VALUES (3, 'c')")
	got, err := readIncrementalUntil(t, e.source(`, "chunkRows": 1`), "k", "seq", "", until)
	if err != nil {
		t.Fatal(err)
	}
	n := len(got.log)
	if seqs := got.column("k", "seq"); !slices.Equal(seqs, []string{"1", "2"}) || n < 2 || got.log[n-2] != "k@"+until || got.log[n-1] != "k complete"+until {
		t.Errorf("read %v, emitted %q", seqs, got.log)
	}
}
