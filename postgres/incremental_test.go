//go:build integration

package postgres

import (
	"slices"
	"testing"

	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
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
