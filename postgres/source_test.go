//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// sourceEnv is a fresh schema in the test database.
type sourceEnv struct {
	t      *testing.T
	conn   *pgx.Conn
	schema string
}

func newSourceEnv(t *testing.T) *sourceEnv {
	t.Helper()
	schema := "s_" + strings.ToLower(rand.Text())[:10]
	conn, err := pgx.Connect(t.Context(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		_ = conn.Close(ctx)
	})
	e := &sourceEnv{t: t, conn: conn, schema: schema}
	e.exec("CREATE SCHEMA $schema")
	return e
}

func (e *sourceEnv) exec(sql string) {
	e.t.Helper()
	if _, err := e.conn.Exec(e.t.Context(), strings.ReplaceAll(sql, "$schema", pgx.Identifier{e.schema}.Sanitize())); err != nil {
		e.t.Fatalf("%s: %v", sql, err)
	}
}

func (e *sourceEnv) source(extra string) *Source {
	e.t.Helper()
	cfg := fmt.Sprintf(`{"dsn": %q, "schema": %q%s}`, dsn(), e.schema, extra)
	s, err := NewSource([]byte(cfg))
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

// capture records what a source emits, in order.
type capture struct {
	mu       sync.Mutex
	log      []string
	batches  map[string][]arrow.RecordBatch
	progress []source.Progress
}

func (c *capture) Rows(stream string, _ []json.RawMessage) error {
	return fmt.Errorf("unexpected raw rows on %s", stream)
}

func (c *capture) Records(stream string, b arrow.RecordBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.batches == nil {
		c.batches = map[string][]arrow.RecordBatch{}
	}
	c.batches[stream] = append(c.batches[stream], b)
	c.log = append(c.log, fmt.Sprintf("%s:%d rows", stream, b.NumRows()))
	return nil
}

func (c *capture) ResumePoint(stream string, state []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = append(c.log, fmt.Sprintf("%s@%s", stream, state))
	return nil
}

func (c *capture) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(c, stream, id)
}

func (c *capture) Complete(stream string, next []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = append(c.log, fmt.Sprintf("%s complete%s", stream, next))
	return nil
}

func (c *capture) Progress(p source.Progress) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.progress = append(c.progress, p)
	return nil
}

func (c *capture) reports() []source.Progress {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.progress)
}

func (c *capture) release() {
	for _, bs := range c.batches {
		for _, b := range bs {
			b.Release()
		}
	}
}

// column returns a stream's values in one column, as Arrow prints them.
func (c *capture) column(stream, name string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, b := range c.batches[stream] {
		idx := b.Schema().FieldIndices(name)
		if len(idx) != 1 {
			return nil
		}
		col := b.Column(idx[0])
		for i := range col.Len() {
			if col.IsNull(i) {
				out = append(out, array.NullValueStr)
				continue
			}
			out = append(out, col.ValueStr(i))
		}
	}
	return out
}

func read(t *testing.T, s *Source, streams map[string]string) *capture {
	t.Helper()
	var req source.ReadRequest
	names := make([]string, 0, len(streams))
	for name := range streams {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		cs := source.ConfiguredStream{Name: name}
		if st := streams[name]; st != "" {
			cs.State = []byte(st)
		}
		req.Streams = append(req.Streams, cs)
	}
	c := &capture{}
	t.Cleanup(c.release)
	if err := s.Read(t.Context(), req, c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSourceDiscovers(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.keyed (b text, a int, v text, PRIMARY KEY (a, b))")
	e.exec("CREATE TABLE $schema.loose (v text, w text NOT NULL)")
	e.exec("CREATE TABLE $schema.parted (id int PRIMARY KEY) PARTITION BY RANGE (id)")
	e.exec("CREATE TABLE $schema.parted_1 PARTITION OF $schema.parted FOR VALUES FROM (0) TO (100)")
	e.exec("CREATE VIEW $schema.view AS SELECT 1 AS x")
	e.exec("CREATE MATERIALIZED VIEW $schema.matview AS SELECT 1 AS x, 'a' AS y")
	e.exec("CREATE UNIQUE INDEX ON $schema.matview (x)")

	cat, err := e.source("").Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]source.CatalogStream{}
	for _, s := range cat.Streams {
		got[s.Name] = s
	}
	if len(got) != 5 {
		t.Errorf("streams %v, want keyed, loose, parted, view and matview", cat.Streams)
	}
	// Nothing in the catalog says a column orders new and changed rows, as an
	// incremental read needs: a pipeline names its cursor.
	for _, s := range got {
		if s.CursorField != "" {
			t.Errorf("%s has a default cursor %q", s.Name, s.CursorField)
		}
	}
	// Views are read in full or incrementally, never with cdc.
	for _, name := range []string{"view", "matview"} {
		s := got[name]
		if len(s.PrimaryKey) != 0 || s.SplitsByKey || !slices.Equal(s.SyncModes, []source.SyncMode{source.FullRefresh, source.Incremental}) {
			t.Errorf("%s: %+v", name, s)
		}
	}
	if s := got["keyed"]; !slices.Equal(s.PrimaryKey, []string{"a", "b"}) || s.Replay != source.Exact {
		t.Errorf("keyed: %+v", s)
	}
	if s := got["loose"]; len(s.PrimaryKey) != 0 || s.Replay != source.Exact {
		t.Errorf("loose: %+v", s)
	}
	if s := got["parted"]; !slices.Equal(s.PrimaryKey, []string{"id"}) {
		t.Errorf("parted: %+v", s)
	}
	// A column is nullable unless it's NOT NULL, and in a table CDC can
	// read, also in the key: a delete's row has nulls in the rest.
	for stream, want := range map[string]string{"keyed": "b=false a=false v=true", "loose": "v=true w=false"} {
		var nullable []string
		for _, f := range got[stream].Schema.Fields() {
			nullable = append(nullable, fmt.Sprintf("%s=%v", f.Name, f.Nullable))
		}
		if strings.Join(nullable, " ") != want {
			t.Errorf("%s's nullability %v, want %s", stream, nullable, want)
		}
	}
	e.exec("CREATE TABLE $schema.tracked (id int PRIMARY KEY, v text NOT NULL)")
	cat, err = e.source(`, "tables": ["tracked"]`).Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if f := cat.Streams[0].Schema.Field(1); !f.Nullable {
		t.Errorf("a NOT NULL column CDC can read: %v", f)
	}

	cat, err = e.source(`, "tables": ["loose"]`).Discover(t.Context())
	if err != nil || len(cat.Streams) != 1 || cat.Streams[0].Name != "loose" {
		t.Errorf("with tables: %v, %v", cat.Streams, err)
	}
	if _, err := e.source(`, "tables": ["missing"]`).Discover(t.Context()); err == nil {
		t.Error("discovered a missing table")
	}
}

func TestSourceReadsTypes(t *testing.T) {
	e := newSourceEnv(t)
	e.exec(`CREATE TABLE $schema.typed (
		id bigint PRIMARY KEY, flag boolean, small smallint, medium integer, f4 real, f8 double precision,
		name text, code char(3), label varchar(10), raw bytea, day date, at_time time,
		ts timestamp, tstz timestamptz, uid uuid, doc json, docb jsonb, amount numeric(12, 2),
		any_num numeric, span interval, tags text[])`)
	e.exec(`INSERT INTO $schema.typed VALUES
		(1, true, -2, 3, 1.5, 2.25, 'héllo', 'ab', 'xy', '\x0102', '2026-10-31', '12:34:56.789',
		 '2026-10-31 12:00:00.123456', '2026-10-31 12:00:00+05:30', '0b0e1f2a-3c4d-4e5f-8a9b-0c1d2e3f4a5b',
		 '{"a": [1, 2]}', '{"b": true}', -12345.67, 1.000000000000000000000000000000000000001, '1 day 2 hours', '{x,y}'),
		(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)

	c := read(t, e.source(""), map[string]string{"typed": ""})
	b := c.batches["typed"]
	if len(b) != 1 {
		t.Fatalf("%d batches", len(b))
	}
	types := map[string]string{}
	for _, f := range b[0].Schema().Fields() {
		types[f.Name] = f.Type.String()
	}
	want := map[string]string{
		"id": "int64", "flag": "bool", "small": "int16", "medium": "int32", "f4": "float32", "f8": "float64",
		"name": "utf8", "code": "utf8", "label": "utf8", "raw": "binary", "day": "date32", "at_time": "time64[us]",
		"ts": "timestamp[us]", "tstz": "timestamp[us, tz=UTC]", "uid": "extension<arrow.uuid>",
		"doc": `extension<arrow.json[storage_type=utf8]>`, "docb": `extension<arrow.json[storage_type=utf8]>`,
		"amount": "utf8", "any_num": "utf8", "span": "month_day_nano_interval", "tags": "list<item: utf8, nullable>",
	}
	for name, typ := range want {
		if types[name] != typ {
			t.Errorf("%s is %s, want %s", name, types[name], typ)
		}
	}

	row := map[string]string{}
	for name := range want {
		vals := c.column("typed", name)
		if len(vals) != 2 {
			t.Fatalf("%s has %d values", name, len(vals))
		}
		row[name] = vals[0]
		if name != "id" && vals[1] != array.NullValueStr {
			t.Errorf("%s in the null row is %s", name, vals[1])
		}
	}
	for name, v := range map[string]string{
		"id": "1", "flag": "true", "small": "-2", "medium": "3", "f4": "1.5", "f8": "2.25", "name": "héllo",
		"code": "ab", "label": "xy", "day": "2026-10-31", "at_time": "12:34:56.789000",
		"ts": "2026-10-31T12:00:00.123456", "tstz": "2026-10-31T06:30:00Z",
		"uid": "0b0e1f2a-3c4d-4e5f-8a9b-0c1d2e3f4a5b", "doc": `{"a": [1, 2]}`, "docb": `{"b": true}`,
		"amount": "-12345.67", "any_num": "1.000000000000000000000000000000000000001", "span": `{"months":0,"days":1,"nanoseconds":7200000000000}`, "tags": `["x","y"]`,
	} {
		if row[name] != v {
			t.Errorf("%s = %q, want %q", name, row[name], v)
		}
	}
}

// A column whose type changed since the run's Discover, as a bytea altered
// to text, fails the read retryably before it reads: its text would land as
// other bytes. A read that declares the types as they are now reads it,
// extension types among them.
func TestSourceFailsOnATypeChangedSinceDiscover(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.files (id int PRIMARY KEY, uid uuid, doc jsonb, body bytea)")
	e.exec(`INSERT INTO $schema.files VALUES (1, '0b0e1f2a-3c4d-4e5f-8a9b-0c1d2e3f4a5b', '{"a": 1}', '\x6869')`)
	s := e.source("")
	declared := func() *arrow.Schema {
		cat, err := s.Discover(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(cat.Streams, func(cs source.CatalogStream) bool { return cs.Name == "files" })
		if i < 0 {
			t.Fatalf("no files in %v", cat.Streams)
		}
		fields, _ := source.SplitDeclared(cat.Streams[i].Schema)
		return fields
	}
	read := func(sc *arrow.Schema) (*capture, error) {
		c := &capture{}
		t.Cleanup(c.release)
		cs := source.ConfiguredStream{Name: "files", SyncMode: source.FullRefresh, Columns: []string{"id", "uid", "doc", "body"}, Schema: sc}
		return c, s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c)
	}
	was := declared()
	if _, err := read(was); err != nil {
		t.Fatalf("a read of the types as declared: %v", err)
	}
	e.exec("ALTER TABLE $schema.files ALTER COLUMN body TYPE text USING convert_from(body, 'UTF8')")
	c, err := read(was)
	if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT || !strings.Contains(err.Error(), `"body"`) {
		t.Fatalf("a read of body changed to text: %v", err)
	}
	if got := c.column("files", "id"); got != nil {
		t.Errorf("the read sent rows: %q", got)
	}
	c, err = read(declared())
	if err != nil {
		t.Fatal(err)
	}
	if got := c.column("files", "body"); !slices.Equal(got, []string{"hi"}) {
		t.Errorf("body %q, want hi", got)
	}
}

func TestSourceDeclaresNumericsAndPassesInfinities(t *testing.T) {
	e := newSourceEnv(t)
	e.exec(`CREATE TABLE $schema.edges (id int PRIMARY KEY, amount numeric(12, 2), any_num numeric, huge numeric(50, 2),
		day date, ts timestamp, tstz timestamptz, n oid)`)
	e.exec(`INSERT INTO $schema.edges VALUES
		(1, 'NaN', 'NaN', 1, 'infinity', 'infinity', '-infinity', 7),
		(2, 1.5, 0.0000000001, 2, '-infinity', '-infinity', 'infinity', NULL)`)

	src := e.source("")
	cat, err := src.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]string{}
	for _, f := range cat.Streams[0].Schema.Fields() {
		declared[f.Name] = f.Type.String()
	}
	for name, want := range map[string]string{
		"amount": "decimal(12, 2)", "any_num": "decimal(38, 9)", "huge": "decimal(38, 9)", "n": "int64", "day": "date32",
	} {
		if declared[name] != want {
			t.Errorf("%s is declared %s, want %s", name, declared[name], want)
		}
	}

	c := read(t, src, map[string]string{"edges": ""})
	for name, want := range map[string][]string{
		"amount":  {"NaN", "1.50"},
		"any_num": {"NaN", "0.0000000001"},
		"n":       {"7", array.NullValueStr},
	} {
		if got := c.column("edges", name); !slices.Equal(got, want) {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	b := c.batches["edges"][0]
	day := b.Column(b.Schema().FieldIndices("day")[0]).(*array.Date32)
	if day.Value(0) != math.MaxInt32 || day.Value(1) != math.MinInt32 {
		t.Errorf("day = %v, want the largest and smallest Date32", day)
	}
	for name, want := range map[string][]arrow.Timestamp{"ts": {math.MaxInt64, math.MinInt64}, "tstz": {math.MinInt64, math.MaxInt64}} {
		ts := b.Column(b.Schema().FieldIndices(name)[0]).(*array.Timestamp)
		if got := ts.Values(); !slices.Equal(got, want) {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestSourceChunksAndResumesExactly(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v text)")
	e.exec("INSERT INTO $schema.t SELECT i, 'v' || i FROM generate_series(1, 10) i")
	s := e.source(`, "chunkRows": 3`)

	c := read(t, s, map[string]string{"t": ""})
	want := []string{
		"t:3 rows", `t@{"after":["3"]}`, "t:3 rows", `t@{"after":["6"]}`, "t:3 rows", `t@{"after":["9"]}`,
		"t:1 rows", `t@{"after":["10"]}`, "t complete",
	}
	if !slices.Equal(c.log, want) {
		t.Errorf("emitted %q\nwant %q", c.log, want)
	}

	c = read(t, s, map[string]string{"t": `{"after":["6"]}`})
	if got := c.column("t", "id"); !slices.Equal(got, []string{"7", "8", "9", "10"}) {
		t.Errorf("resumed after 6: %v", got)
	}
	c = read(t, s, map[string]string{"t": `{"after":["10"]}`})
	if !slices.Equal(c.log, []string{`t@{"after":["10"]}`, "t complete"}) {
		t.Errorf("resumed at the end: %q", c.log)
	}
}

func TestSourceResumesOnACompositeKey(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (a int, b text, PRIMARY KEY (a, b))")
	e.exec("INSERT INTO $schema.t VALUES (1, 'x'), (1, 'y'), (2, 'a'), (2, 'b'), (3, 'a')")
	s := e.source(`, "chunkRows": 2`)
	c := read(t, s, map[string]string{"t": `{"after":["1","y"]}`})
	got := slices.Collect(func(yield func(string) bool) {
		as, bs := c.column("t", "a"), c.column("t", "b")
		for i := range as {
			if !yield(as[i] + bs[i]) {
				return
			}
		}
	})
	if !slices.Equal(got, []string{"2a", "2b", "3a"}) {
		t.Errorf("resumed after (1, y): %v", got)
	}
}

func TestSourceReadsATableWithoutAKeyOnce(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (v int)")
	e.exec("INSERT INTO $schema.t SELECT generate_series(1, 5)")
	s := e.source(`, "chunkRows": 2`)
	c := read(t, s, map[string]string{"t": ""})
	if want := []string{"t:5 rows", `t@{"done":true}`, "t complete"}; !slices.Equal(c.log, want) {
		t.Errorf("emitted %q, want %q", c.log, want)
	}
	c = read(t, s, map[string]string{"t": `{"done":true}`})
	if want := []string{"t complete"}; !slices.Equal(c.log, want) {
		t.Errorf("after done: %q", c.log)
	}
}

func TestSourceSplitsBatchesByBytes(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v text)")
	e.exec("INSERT INTO $schema.t SELECT i, repeat('x', 100) FROM generate_series(1, 50) i")
	c := read(t, e.source(`, "batchBytes": 1000`), map[string]string{"t": ""})
	n := len(c.batches["t"])
	if n < 5 {
		t.Errorf("%d batches, want one per ~1000 bytes", n)
	}
	if got := len(c.column("t", "id")); got != 50 {
		t.Errorf("%d rows", got)
	}
}

// Intervals arrive as intervals, and one-dimensional arrays of booleans,
// integers, floats and strings as lists, in full, incrementally and with
// cdc; arrays declared with more dimensions, and of other types, as text.
func TestSourceReadsArraysAndIntervals(t *testing.T) {
	e := newSourceEnv(t)
	e.exec(`CREATE TABLE $schema.a (id int PRIMARY KEY, iv interval, b bool[], i2 int2[], i4 int[], i8 bigint[],
		f4 real[], f8 float8[], tx text[], vc varchar(5)[], ch char(3)[], grid int[][], nums numeric[], days date[])`)
	e.exec(`INSERT INTO $schema.a VALUES
		(1, '1 year 2 mons -3 days 04:05:06.789', '{t,f,NULL}', '{-32768,32767}', '{1,NULL,3}', '{9223372036854775807}',
		 '{1.5,NaN}', '{-Infinity,1e300,0.1}', '{"a,b","q\"u","NULL",NULL,"","back\\slash"," sp "}', '{xy}', '{ab}',
		 '{{1,2},{3,4}}', '{1.5}', '{2026-01-01}'),
		(2, '-00:00:00.000001', '{}', '[0:1]={5,6}', NULL, '{}', NULL, NULL, '{}', NULL, NULL, NULL, NULL, NULL),
		(3, '0', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)
	want := map[string][]string{
		"iv": {`{"months":14,"days":-3,"nanoseconds":14706789000000}`, `{"months":0,"days":0,"nanoseconds":-1000}`, `{"months":0,"days":0,"nanoseconds":0}`},
		"b":  {`[true,false,null]`, `[]`, array.NullValueStr},
		"i2": {`[-32768,32767]`, `[5,6]`, array.NullValueStr},
		"i4": {`[1,null,3]`, array.NullValueStr, array.NullValueStr},
		"i8": {`[9223372036854775807]`, `[]`, array.NullValueStr},
		"f4": {`[1.5,"NaN"]`, array.NullValueStr, array.NullValueStr},
		"f8": {`["-Inf",1e+300,0.1]`, array.NullValueStr, array.NullValueStr},
		"tx": {`["a,b","q\"u","NULL",null,"","back\\slash"," sp "]`, `[]`, array.NullValueStr},
		"vc": {`["xy"]`, array.NullValueStr, array.NullValueStr},
		"ch": {`["ab"]`, array.NullValueStr, array.NullValueStr},
		// As Postgres spells them.
		"grid": {"{{1,2},{3,4}}", array.NullValueStr, array.NullValueStr},
		"nums": {"{1.5}", array.NullValueStr, array.NullValueStr},
		"days": {"{2026-01-01}", array.NullValueStr, array.NullValueStr},
	}
	check := func(t *testing.T, what string, col func(name string) []string) {
		t.Helper()
		for name, w := range want {
			if got := col(name); !slices.Equal(got, w) {
				t.Errorf("%s: %s = %q, want %q", what, name, got, w)
			}
		}
	}
	s := e.source("")
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]string{}
	for _, f := range cat.Streams[0].Schema.Fields() {
		declared[f.Name] = f.Type.String()
	}
	for name, typ := range map[string]string{"iv": "month_day_nano_interval", "i4": "list<item: int32, nullable>", "f4": "list<item: float32, nullable>", "tx": "list<item: utf8, nullable>", "grid": "utf8", "nums": "utf8"} {
		if declared[name] != typ {
			t.Errorf("%s is declared %s, want %s", name, declared[name], typ)
		}
	}
	c := read(t, s, map[string]string{"a": ""})
	check(t, "full", func(name string) []string { return c.column("a", name) })
	c, err = readIncremental(t, s, "a", "id", "")
	if err != nil {
		t.Fatal(err)
	}
	check(t, "incremental", func(name string) []string { return c.column("a", name) })

	cs, pipeline := e.cdcSource("")
	c = readCDC(t, cs, pipeline, map[string]string{"a": ""})
	check(t, "cdc snapshot", func(name string) []string { return c.column("a", name) })
	state := c.last("a")
	e.exec("UPDATE $schema.a SET id = id + 10")
	c = readCDC(t, cs, pipeline, map[string]string{"a": state})
	check(t, "cdc", func(name string) []string {
		var out []string
		for i, v := range c.column("a", name) {
			// After each key's delete.
			if i%2 == 1 {
				out = append(out, v)
			}
		}
		return out
	})
}

// A value with nowhere to go in its column's type fails the read, saying
// what it is: an array of more dimensions than a list holds, and an interval
// past the engine's.
func TestSourceRefusesArraysAndIntervalsItCantHold(t *testing.T) {
	for value, column := range map[string]string{
		"'{{1,2},{3,4}}'::int[]":      "int[]",
		"'100000000 hours'::interval": "interval",
		"'infinity'::interval":        "interval",
	} {
		e := newSourceEnv(t)
		e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v " + column + ")")
		e.exec("INSERT INTO $schema.t VALUES (1, " + value + ")")
		err := e.source("").Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t"}}}, &capture{})
		if code := codeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID || !strings.Contains(err.Error(), `"v"`) {
			t.Errorf("%s: %v (%v)", value, err, code)
		}
	}
}
