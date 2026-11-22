//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// A table without a primary key is read in the order of a unique index whose
// columns are all NOT NULL, in chunks, and resumed after its last chunk; its
// catalog entry still has no key, and no cdc.
func TestSourceReadsInAUniqueIndexsOrder(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (a int NOT NULL, b text NOT NULL, c int, v text)")
	e.exec("INSERT INTO $schema.t SELECT i / 2, 'b' || i % 2, i, 'v' || i FROM generate_series(1, 10) i")
	// Indexes that aren't a read order: on a nullable column, partial, on an
	// expression, and one with more columns than another.
	e.exec("CREATE UNIQUE INDEX t_c ON $schema.t (c)")
	e.exec("CREATE UNIQUE INDEX t_partial ON $schema.t (a) WHERE a > 100")
	e.exec("CREATE UNIQUE INDEX t_expr ON $schema.t (lower(b), a)")
	e.exec("CREATE UNIQUE INDEX t_wide ON $schema.t (a, b, v) WHERE true IS NOT NULL")
	e.exec("CREATE UNIQUE INDEX t_ab ON $schema.t (a, b) INCLUDE (c)")
	e.exec("ALTER TABLE $schema.t ALTER COLUMN v SET NOT NULL")
	e.exec("CREATE UNIQUE INDEX t_bav ON $schema.t (b, a, v)")
	s := e.source(`, "chunkRows": 4`)

	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if cs := cat.Streams[0]; len(cs.PrimaryKey) != 0 || cs.SplitsByKey || slices.Contains(cs.SyncModes, source.CDC) || cs.Replay != source.Exact {
		t.Errorf("catalog %+v", cs)
	}
	c := read(t, s, map[string]string{"t": ""})
	want := []string{
		"t:4 rows", `t@{"after":["2","b0"],"key":["a","b"]}`, "t:4 rows", `t@{"after":["4","b0"],"key":["a","b"]}`,
		"t:2 rows", `t@{"after":["5","b0"],"key":["a","b"]}`, "t complete",
	}
	if !slices.Equal(c.log, want) {
		t.Errorf("emitted %q\nwant %q", c.log, want)
	}
	c = read(t, s, map[string]string{"t": `{"after":["2","b0"],"key":["a","b"]}`})
	if got := c.column("t", "c"); !slices.Equal(got, []string{"5", "6", "7", "8", "9", "10"}) {
		t.Errorf("resumed after (2, b0): %v", got)
	}
	// A state from before the table had an order, which had ended.
	c = read(t, s, map[string]string{"t": `{"done":true}`})
	if !slices.Equal(c.log, []string{"t complete"}) {
		t.Errorf("after done: %q", c.log)
	}

	// Incrementally, in (cursor, index) order, and to an end.
	inc, err := readIncremental(t, s, "t", "a", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := inc.column("t", "c"); len(got) != 10 || !strings.Contains(strings.Join(inc.log, " "), `"key":["a","b"]`) {
		t.Fatalf("incremental: %v, %q", got, inc.log)
	}

	// Another index in its place: a read in the old order starts again.
	e.exec("DROP INDEX $schema.t_ab")
	cs := source.ConfiguredStream{Name: "t", State: []byte(`{"after":["2","b0"],"key":["a","b"]}`)}
	err = s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, &capture{})
	if code := codeOf(err); code.String() != "ERROR_CODE_POSITION_LOST" {
		t.Errorf("resumed in another order: %v (%v)", err, code)
	}
	c = read(t, s, map[string]string{"t": ""})
	if !strings.Contains(strings.Join(c.log, " "), `"key":["b","a","v"]`) {
		t.Errorf("read in %q, want the order of t_bav", c.log)
	}

	// Without such an index, one query.
	e.exec("DROP INDEX $schema.t_bav")
	c = read(t, s, map[string]string{"t": ""})
	if want := []string{"t:10 rows", `t@{"done":true}`, "t complete"}; !slices.Equal(c.log, want) {
		t.Errorf("emitted %q, want %q", c.log, want)
	}
}

// A read resumed after its table lost its primary key, with no unique index
// in its place, fails with POSITION_LOST rather than read every row again in
// one query, in full or incrementally; while the key is there, a state
// written as 0.1.0 wrote it, naming no key, resumes.
func TestSourceReadOfATableThatLostItsKey(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, seq int)")
	e.exec("INSERT INTO $schema.t SELECT i, i FROM generate_series(1, 6) i")
	s := e.source(`, "chunkRows": 2`)
	full, inc := `{"after":["2"]}`, `{"after":["2","2"],"until":"6"}`
	if got := read(t, s, map[string]string{"t": full}).column("t", "id"); !slices.Equal(got, []string{"3", "4", "5", "6"}) {
		t.Fatalf("resumed after 2: %v", got)
	}
	if got := mustReadIncremental(t, s, "t", "seq", inc).column("t", "id"); !slices.Equal(got, []string{"3", "4", "5", "6"}) {
		t.Fatalf("resumed incrementally after 2: %v", got)
	}

	e.exec("ALTER TABLE $schema.t DROP CONSTRAINT t_pkey")
	cs := source.ConfiguredStream{Name: "t", State: []byte(full)}
	c := &capture{}
	t.Cleanup(c.release)
	err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c)
	if code := codeOf(err); code.String() != "ERROR_CODE_POSITION_LOST" {
		t.Errorf("a full refresh resumed without a key: %v (%v), emitted %q", err, code, c.log)
	}
	c, err = readIncremental(t, s, "t", "seq", inc)
	if code := codeOf(err); code.String() != "ERROR_CODE_POSITION_LOST" {
		t.Errorf("an incremental read resumed without a key: %v (%v), emitted %q", err, code, c.log)
	}
}

// A table's splits hold the order they were cut in from the start: once
// another index takes its place, a split not yet read fails with
// POSITION_LOST rather than read its range of another column.
func TestSplitsKeepTheirOrder(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (a int NOT NULL, b int NOT NULL)")
	e.exec("INSERT INTO $schema.t SELECT i, 2001 - i FROM generate_series(1, 2000) i")
	e.exec("CREATE UNIQUE INDEX t_a ON $schema.t (a)")
	e.exec("ANALYZE $schema.t")
	s := e.source(`, "chunkRows": 200`)
	splits, err := s.Splits(t.Context(), source.ConfiguredStream{Name: "t", SyncMode: source.FullRefresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) < 2 {
		t.Fatalf("%d splits", len(splits))
	}
	for _, sp := range splits {
		if !strings.Contains(string(sp.State), `"key":["a"]`) {
			t.Errorf("split %s: %s", sp.ID, sp.State)
		}
	}
	e.exec("DROP INDEX $schema.t_a")
	e.exec("CREATE UNIQUE INDEX t_b ON $schema.t (b)")
	cs := source.ConfiguredStream{Name: "t", Splits: splits[1:2]}
	err = s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, &splitCapture{Col: "a"})
	if code := codeOf(err); code.String() != "ERROR_CODE_POSITION_LOST" {
		t.Errorf("a split read in another order: %v (%v)", err, code)
	}
}

// A unique index in another collation than its column's isn't a read order:
// the read compares in the column's, in which two of its keys can be equal.
func TestSourceReadOrderKeepsTheColumnsCollation(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (k text NOT NULL)")
	e.exec("INSERT INTO $schema.t SELECT 'k' || i FROM generate_series(1, 6) i")
	e.exec(`CREATE UNIQUE INDEX t_c ON $schema.t (k COLLATE "C")`)
	s := e.source(`, "chunkRows": 4`)
	c := read(t, s, map[string]string{"t": ""})
	if want := []string{"t:6 rows", `t@{"done":true}`, "t complete"}; !slices.Equal(c.log, want) {
		t.Errorf("emitted %q, want %q", c.log, want)
	}
	e.exec("CREATE UNIQUE INDEX t_k ON $schema.t (k)")
	c = read(t, s, map[string]string{"t": ""})
	if !strings.Contains(strings.Join(c.log, " "), `"key":["k"]`) {
		t.Errorf("read in %q, want the order of t_k", c.log)
	}
}

// A unique index of a table others inherit from isn't a read order: the
// read reads their rows too, whose keys the index doesn't cover. A
// partitioned table's is, as it covers every partition.
func TestSourceReadOrderOfAnInheritedTable(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.p (id int NOT NULL, v text)")
	e.exec("CREATE UNIQUE INDEX ON $schema.p (id)")
	e.exec("CREATE TABLE $schema.c () INHERITS ($schema.p)")
	e.exec("INSERT INTO $schema.p VALUES (1, 'parent')")
	e.exec("INSERT INTO $schema.c VALUES (1, 'child')")
	e.exec("CREATE TABLE $schema.parts (id int NOT NULL, v text) PARTITION BY RANGE (id)")
	e.exec("CREATE TABLE $schema.parts_1 PARTITION OF $schema.parts FOR VALUES FROM (0) TO (10)")
	e.exec("CREATE UNIQUE INDEX ON $schema.parts (id)")
	e.exec("INSERT INTO $schema.parts VALUES (1, 'a'), (2, 'b')")
	s := e.source(`, "chunkRows": 1`)
	c := read(t, s, map[string]string{"p": ""})
	if want := []string{"p:2 rows", `p@{"done":true}`, "p complete"}; !slices.Equal(c.log, want) {
		t.Errorf("emitted %q, want %q", c.log, want)
	}
	c = read(t, s, map[string]string{"parts": ""})
	if got := c.column("parts", "id"); !slices.Equal(got, []string{"1", "2"}) || !strings.Contains(strings.Join(c.log, " "), `"key":["id"]`) {
		t.Errorf("parts: %v, %q", got, c.log)
	}
}

// Views are read in one query, and incrementally. A materialized view's
// columns are never NOT NULL, so even one with a unique index has no read
// order.
func TestSourceReadsViews(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, at int NOT NULL)")
	e.exec("INSERT INTO $schema.t SELECT i, i % 4 FROM generate_series(1, 10) i")
	e.exec("CREATE VIEW $schema.v AS SELECT id, at, id * 2 AS twice FROM $schema.t")
	e.exec("CREATE MATERIALIZED VIEW $schema.m AS SELECT id, at FROM $schema.t")
	e.exec("CREATE UNIQUE INDEX ON $schema.m (id)")
	s := e.source(`, "chunkRows": 4`)
	c := read(t, s, map[string]string{"v": "", "m": ""})
	if got := c.column("v", "twice"); len(got) != 10 {
		t.Errorf("v: %v", got)
	}
	if got := c.column("m", "id"); len(got) != 10 || !slices.Contains(c.log, `m@{"done":true}`) {
		t.Errorf("m: %v, %q", got, c.log)
	}
	// A view doesn't say how many dimensions its arrays have: they're text.
	e.exec("CREATE VIEW $schema.arr AS SELECT id, ARRAY[id, at] AS pair FROM $schema.t WHERE id = 1")
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, cs := range cat.Streams {
		if f, ok := cs.Schema.FieldsByName("pair"); cs.Name == "arr" && (!ok || f[0].Type.String() != "utf8") {
			t.Errorf("arr.pair is declared %v", f)
		}
	}
	if got := read(t, s, map[string]string{"arr": ""}).column("arr", "pair"); !slices.Equal(got, []string{"{1,1}"}) {
		t.Errorf("arr.pair: %q", got)
	}
	for _, name := range []string{"v", "m"} {
		c, err := readIncremental(t, s, name, "at", `{"since":"2"}`)
		if err != nil {
			t.Fatal(err)
		}
		if got := c.column(name, "id"); len(got) != 5 {
			t.Errorf("%s from at 2: %v", name, got)
		}
	}
}

// Read in a unique index's order, a chunk at a time and restarted part way,
// under inserts, updates and deletes, and updates that move a row's index
// key: no index key is read twice, and every row the writers never touch is
// read exactly once.
func TestSourceReadsInAUniqueIndexsOrderUnderWrites(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (k int NOT NULL, part text NOT NULL, v text)")
	e.exec("CREATE UNIQUE INDEX ON $schema.t (part, k)")
	e.exec("INSERT INTO $schema.t SELECT i, 'p' || i % 3, 'v' || i FROM generate_series(1, 6000) i")
	s := e.source(`, "chunkRows": 97`)

	var writers sync.WaitGroup
	defer writers.Wait()
	for w := range 4 {
		writers.Go(func() {
			conn, err := pgx.Connect(context.Background(), dsn())
			if err != nil {
				t.Error(err)
				return
			}
			defer func() { _ = conn.Close(context.Background()) }()
			table := pgx.Identifier{e.schema, "t"}.Sanitize()
			rng := mrand.New(mrand.NewPCG(uint64(w), 7))
			for range 1500 {
				// Writers touch only even k, and k they add above 6000.
				k := 2 * (rng.IntN(3000) + 1)
				var q string
				switch rng.IntN(4) {
				case 0:
					q = fmt.Sprintf("DELETE FROM %s WHERE k = %d", table, k)
				case 1:
					q = fmt.Sprintf("INSERT INTO %s VALUES (%d, 'p%d', 'new')", table, 6000+2*rng.IntN(3000)+2, rng.IntN(3))
				case 2:
					// Moves the row to another place in the order.
					q = fmt.Sprintf("UPDATE %s SET k = %d, part = 'p%d' WHERE k = %d", table, 6000+2*rng.IntN(3000)+2, rng.IntN(3), k)
				default:
					q = fmt.Sprintf("UPDATE %s SET v = 'w%d' WHERE k = %d", table, w, k)
				}
				if _, err := conn.Exec(context.Background(), q); err != nil {
					if pe, ok := errors.AsType[*pgconn.PgError](err); ok && (pe.Code == "23505" || pe.Code == "40P01") {
						continue
					}
					t.Error(err)
					return
				}
			}
		})
	}

	// Stop at the 20th resume point, and go on from it.
	rctx, stop := context.WithCancel(t.Context())
	n := 0
	first := &cutCapture{capture: &capture{}, stream: "t", cancel: stop, at: func(string) bool { n++; return n == 20 }}
	t.Cleanup(first.release)
	cs := source.ConfiguredStream{Name: "t"}
	if err := s.Read(rctx, source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("the first read: %v", err)
	}
	state := first.checkpointed()
	rest := read(t, s, map[string]string{"t": state})

	seen := map[string]int{}
	untouched := map[string]int{}
	for _, c := range []*capture{first.capture, rest} {
		ks, parts := c.column("t", "k"), c.column("t", "part")
		for i := range ks {
			seen[parts[i]+"/"+ks[i]]++
			var k int
			_, _ = fmt.Sscan(ks[i], &k)
			if k%2 == 1 && k <= 6000 {
				untouched[ks[i]]++
			}
		}
	}
	for key, n := range seen {
		if n > 1 {
			t.Errorf("read %s %d times", key, n)
		}
	}
	if len(untouched) != 3000 {
		t.Errorf("read %d of the 3000 rows the writers never touch", len(untouched))
	}
	for k, n := range untouched {
		if n != 1 {
			t.Errorf("read k %s %d times", k, n)
		}
	}
}

// Split reads of a whole stream emit through the cutCapture too.
func (c *cutCapture) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(c, stream, id)
}

// A table without a primary key but with a unique index is split into
// ranges of its first column, which together read every row once.
func TestSplitsByAUniqueIndex(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int NOT NULL UNIQUE, n int)")
	e.exec("INSERT INTO $schema.t SELECT i, i FROM generate_series(1, 2000) i")
	e.exec("ANALYZE $schema.t")
	s := e.source(`, "chunkRows": 200`)
	splits, err := s.Splits(t.Context(), source.ConfiguredStream{Name: "t", SyncMode: source.FullRefresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) < 5 {
		t.Fatalf("%d splits", len(splits))
	}
	c := &splitCapture{Col: "id"}
	cs := source.ConfiguredStream{Name: "t", Splits: splits}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 3}, c); err != nil {
		t.Fatal(err)
	}
	got := c.Values()
	slices.Sort(got)
	if len(slices.Compact(got)) != 2000 || len(c.Values()) != 2000 {
		t.Errorf("read %d rows, %d distinct", len(c.Values()), len(slices.Compact(got)))
	}
}
