//go:build integration

package clickhouse

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// collector keeps what a read emits.
type collector struct {
	mu   sync.Mutex
	rows map[string][]map[string]any
	// Each row's batch's schema.
	rowSchemas map[string][]*arrow.Schema
	schemas    map[string][]*arrow.Schema
	resumes    map[string][][]byte
	complete   map[string][]byte
	added      []source.Split
	// By stream and split: where each split completed.
	completed map[string]map[string][]byte
	// Called at each resume point.
	onResume func(stream, split string)
}

func newCollector() *collector {
	return &collector{rows: map[string][]map[string]any{}, rowSchemas: map[string][]*arrow.Schema{}, schemas: map[string][]*arrow.Schema{}, resumes: map[string][][]byte{}, complete: map[string][]byte{}, completed: map[string]map[string][]byte{}}
}

func (c *collector) Rows(string, []json.RawMessage) error { return fmt.Errorf("no raw rows") }

func (c *collector) Records(stream string, b arrow.RecordBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer b.Release()
	c.schemas[stream] = append(c.schemas[stream], b.Schema())
	for i := range int(b.NumRows()) {
		row := map[string]any{}
		for j, f := range b.Schema().Fields() {
			col := b.Column(j)
			if col.IsNull(i) {
				row[f.Name] = nil
				continue
			}
			row[f.Name] = col.GetOneForMarshal(i)
		}
		c.rows[stream] = append(c.rows[stream], row)
		c.rowSchemas[stream] = append(c.rowSchemas[stream], b.Schema())
	}
	return nil
}

func (c *collector) ResumePoint(stream string, state []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumes[stream] = append(c.resumes[stream], state)
	return nil
}

func (c *collector) Complete(stream string, next []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.complete[stream] = next
	return nil
}

func (c *collector) Split(stream, id string) source.SplitEmitter {
	return splitCollector{c, stream, id}
}

// ended is where each split of a stream completed.
func (c *collector) ended(stream string) []source.Split {
	var out []source.Split
	for id, next := range c.completed[stream] {
		out = append(out, source.Split{ID: id, State: next})
	}
	return out
}

type splitCollector struct {
	c          *collector
	stream, id string
}

func (s splitCollector) Rows(r []json.RawMessage) error    { return s.c.Rows(s.stream, r) }
func (s splitCollector) Records(b arrow.RecordBatch) error { return s.c.Records(s.stream, b) }
func (s splitCollector) Complete(next []byte, added ...source.Split) error {
	s.c.mu.Lock()
	s.c.added = append(s.c.added, added...)
	if s.c.completed[s.stream] == nil {
		s.c.completed[s.stream] = map[string][]byte{}
	}
	s.c.completed[s.stream][s.id] = next
	s.c.mu.Unlock()
	return s.c.Complete(s.stream, next)
}
func (s splitCollector) ResumePoint(state []byte, added ...source.Split) error {
	s.c.mu.Lock()
	s.c.added = append(s.c.added, added...)
	hook := s.c.onResume
	s.c.mu.Unlock()
	if hook != nil {
		hook(s.stream, s.id)
	}
	return s.c.ResumePoint(s.stream, state)
}

func newTestSource(t *testing.T, db, extra string) *Source {
	t.Helper()
	cfg := fmt.Sprintf(`{"addr": %q, "user": "openetl", "password": %q, "database": %q%s}`, testAddr(), testPassword, db, extra)
	s, err := NewSource([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSourceTypes(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec(kindsTable)
	e.exec(kindsRows)
	s := newTestSource(t, db, "")
	c := newCollector()
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "kinds", SyncMode: source.FullRefresh}}}, c); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`{"a":[1,null,3],"aa":[["a"],[]],"adt9":["2262-04-11 23:47:16.854775807"],"at":["-01:00:00"],"au8":[0,255],"b":true,"bf":1.5,"d":"12345678901234.5678","d16":"2149-06-06","d32":"1900-01-01","d76":"123456789012345678901234567890123456789012345678901234567890.1234567891","dt":"2026-11-03T01:00:00Z","dt3":"1900-01-01T00:00:00.001Z","dt9":"2262-04-11 23:47:16.854775807Z","e":"ok","f32":1.5,"f64":"NaN","fs":"YWIAAA==","i128":"-170141183460469231731687303715884105728","i256":"-57896044618658097711785492504343953926634992332820282019728792003956564819968","i8":-128,"id":1,"ip4":"1.2.3.4","ip6":"::1","j":{"a":[1,{}]},"lc":"x","m":{"x":1,"y":2},"pt":[1.5,-2],"s":"naïve","t":"-999h59m59s","t6":"23:59:59.999999","t9":"23:59:59.123456","tu":[-1,"z"],"tup":{"n":1,"s":null},"u":"61f0c404-5cb3-11e7-907b-a6006ad3dba0","u16":65535,"u256":"115792089237316195423570985008687907853269984665640564039457584007913129639935","u32":4294967295,"u64":"18446744073709551615","u8":255,"v":42}`,
		`{"a":[],"aa":[],"adt9":[],"at":[],"au8":[],"b":false,"bf":0,"d":"-0.0001","d16":"1970-01-01","d32":null,"d76":null,"dt":"1970-01-01T00:00:00Z","dt3":null,"dt9":"1900-01-01 00:00:00.000000000Z","e":"sad","f32":"-Inf","f64":null,"fs":"AAAAAA==","i128":"170141183460469231731687303715884105727","i256":"57896044618658097711785492504343953926634992332820282019728792003956564819967","i8":0,"id":2,"ip4":"0.0.0.0","ip6":"::","j":{},"lc":null,"m":{},"pt":[0,0],"s":null,"t":null,"t6":"00:00:00","t9":"00:00:00","tu":[0,""],"tup":{"n":0,"s":"s"},"u":null,"u16":null,"u256":null,"u32":0,"u64":"0","u8":0,"v":"text"}`,
		`{"a":[null],"aa":[[""]],"adt9":["1970-01-01 00:00:00.000000001"],"at":["12:00:00"],"au8":[1],"b":true,"bf":0,"d":"0","d16":"2149-06-06","d32":"2299-12-31","d76":"1","dt":"2106-02-07T06:28:15Z","dt3":"2299-12-31T23:59:59.999Z","dt9":"1970-01-01 00:00:00.000000000Z","e":"ok","f32":0,"f64":1e+308,"fs":"YWJjZA==","i128":"1","i256":"1","i8":1,"id":3,"ip4":"255.255.255.255","ip6":"ffff::1","j":{"n":1},"lc":"","m":{"":0},"pt":[0,0],"s":"","t":"12:00:00","t6":"00:00:00.000001","t9":"00:00:00","tu":[127,"x"],"tup":{"n":2147483647,"s":""},"u":"00000000-0000-0000-0000-000000000000","u16":1,"u256":"1","u32":1,"u64":"1","u8":1,"v":null}`,
	}
	// A time of day past a day's comes as text, in a batch of its own.
	text := [][]string{{"t"}, nil, nil}
	rows := c.rows["kinds"]
	if len(rows) != len(want) {
		t.Fatalf("read %d rows, not %d", len(rows), len(want))
	}
	for i, r := range rows {
		b, _ := json.Marshal(r)
		if string(b) != want[i] {
			t.Errorf("row %d:\n got %s\nwant %s", i+1, b, want[i])
		}
		var strs []string
		for _, f := range c.rowSchemas["kinds"][i].Fields() {
			if f.Type.ID() == arrow.STRING && f.Name == "t" {
				strs = append(strs, f.Name)
			}
		}
		slices.Sort(strs)
		if !slices.Equal(strs, text[i]) {
			t.Errorf("row %d's columns as text: %v, not %v", i+1, strs, text[i])
		}
	}
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]string{}
	for _, f := range cat.Streams[slices.IndexFunc(cat.Streams, func(cs source.CatalogStream) bool { return cs.Name == "kinds" })].Schema.Fields() {
		declared[f.Name] = f.Type.String()
	}
	// Types the engine takes: UInt64 as decimal(20, 0), wider integers and
	// decimals as their text, timestamps in microseconds in UTC, and maps and
	// tuples as JSON.
	for col, typ := range map[string]string{
		"i8": "int16", "u64": "decimal(20, 0)", "i128": "utf8", "u256": "utf8", "d": "decimal(18, 4)", "d76": "utf8", "fs": "binary",
		"lc": "utf8", "e": "utf8", "u": "extension<arrow.uuid>", "dt": "timestamp[us, tz=UTC]", "dt3": "timestamp[us, tz=UTC]",
		"dt9": "timestamp[us, tz=UTC]", "d16": "date32", "d32": "date32", "t": "time64[us]", "t6": "time64[us]", "a": "list<item: int64, nullable>",
		"m": "extension<arrow.json[storage_type=utf8]>", "tup": "extension<arrow.json[storage_type=utf8]>", "j": "extension<arrow.json[storage_type=utf8]>",
		"v": "extension<arrow.json[storage_type=utf8]>", "pt": "extension<arrow.json[storage_type=utf8]>", "bf": "float32",
		"u8": "int16", "u16": "int32", "u32": "int64", "au8": "list<item: int16, nullable>", "at": "extension<arrow.json[storage_type=utf8]>",
		"t9": "time64[us]", "adt9": "extension<arrow.json[storage_type=utf8]>",
	} {
		if declared[col] != typ {
			t.Errorf("%s is declared %s, not %s", col, declared[col], typ)
		}
	}
	// A DateTime64 of 9 digits comes as text, which the engine parses.
	last := c.schemas["kinds"][len(c.schemas["kinds"])-1]
	if f, _ := last.FieldsByName("dt9"); len(f) != 1 || f[0].Type.ID() != arrow.STRING {
		t.Errorf("dt9 comes as %v, not text", f)
	}
}

// ids reads the column id of each row a read emitted for stream.
func (c *collector) ids(stream string) []int64 {
	var out []int64
	for _, r := range c.rows[stream] {
		out = append(out, r["id"].(int64))
	}
	slices.Sort(out)
	return out
}

func span(from, to int64) []int64 {
	var out []int64
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// A table read in splits, from its primary index's granules, and in splits
// added by Resplit, yields each row once.
func TestSourceSplits(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.big (id Int64, g String, at DateTime) ENGINE = MergeTree ORDER BY (id, g) SETTINGS index_granularity = 64")
	e.exec("INSERT INTO $db.big SELECT number + 1, toString(number % 3), toDateTime('2026-01-01 00:00:00') + number FROM numbers(5000)")
	s := newTestSource(t, db, `, "chunkRows": 300`)
	cs := source.ConfiguredStream{Name: "big", SyncMode: source.FullRefresh}
	splits, err := s.Splits(t.Context(), cs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) < 2 {
		t.Fatalf("%d splits of 5000 rows in chunks of 300", len(splits))
	}
	cs.Splits = splits
	c := newCollector()
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 4}, c); err != nil {
		t.Fatal(err)
	}
	if got := c.ids("big"); !slices.Equal(got, span(1, 5000)) {
		t.Errorf("%d splits read %d rows, not ids 1 to 5000 once", len(splits), len(got))
	}

	// Asked at its first resume point, the whole stream's read splits its
	// remainder, and the split it adds is read too.
	c = newCollector()
	var once sync.Once
	c.onResume = func(stream, split string) {
		once.Do(func() {
			if ok, err := s.Resplit(t.Context(), stream, split); !ok || err != nil {
				t.Errorf("Resplit: %v, %v", ok, err)
			}
		})
	}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "big", SyncMode: source.FullRefresh}}, Lanes: 2}, c); err != nil {
		t.Fatal(err)
	}
	if len(c.added) == 0 {
		t.Error("Resplit added no split")
	}
	if got := c.ids("big"); !slices.Equal(got, span(1, 5000)) {
		t.Errorf("a resplit read read %d rows, not ids 1 to 5000 once", len(got))
	}

	// An incremental read in splits, and the next from where they ended.
	cs = source.ConfiguredStream{Name: "big", SyncMode: source.Incremental, Cursor: "at"}
	if cs.Splits, err = s.Splits(t.Context(), cs, nil); err != nil || len(cs.Splits) < 2 {
		t.Fatalf("incremental splits: %d, %v", len(cs.Splits), err)
	}
	ic := newCollector()
	ended := readEnded(t, s, cs, ic)
	if got := ic.ids("big"); !slices.Equal(got, span(1, 5000)) {
		t.Errorf("incremental splits read %d rows, not ids 1 to 5000 once", len(got))
	}
	e.exec("INSERT INTO $db.big SELECT number + 5001, 'n', toDateTime('2026-02-01 00:00:00') + number FROM numbers(10)")
	if cs.Splits, err = s.Splits(t.Context(), cs, ended); err != nil {
		t.Fatal(err)
	}
	ic = newCollector()
	readEnded(t, s, cs, ic)
	// The floor's rows come again, with the new ones.
	if got := ic.ids("big"); !slices.Equal(got, span(5000, 5010)) {
		t.Errorf("the next incremental read read %v, not ids 5000 to 5010", got)
	}
}

// readEnded reads a stream in its splits, and returns where they ended.
func readEnded(t *testing.T, s *Source, cs source.ConfiguredStream, c *collector) []source.Split {
	t.Helper()
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 3}, c); err != nil {
		t.Fatal(err)
	}
	return c.ended(cs.Name)
}

// A ReplacingMergeTree is read with FINAL, one row per key, in splits too,
// and declares its sorting key as its primary key; a plain MergeTree
// declares none, and a table without a sorting key is read in one query.
func TestSourceFinal(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.r (id UInt32, v String, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY id SETTINGS index_granularity = 32")
	e.exec("INSERT INTO $db.r SELECT number + 1, 'old', 1 FROM numbers(2000)")
	e.exec("INSERT INTO $db.r SELECT number + 1, 'new', 2 FROM numbers(1000)")
	e.exec("CREATE TABLE $db.plain (id Int64, k Nullable(String)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1")
	e.exec("INSERT INTO $db.plain SELECT number + 1, if(number % 2 = 0, NULL, 'x') FROM numbers(50)")
	s := newTestSource(t, db, `, "chunkRows": 100`)
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, cs := range cat.Streams {
		switch cs.Name {
		case "r":
			if !slices.Equal(cs.PrimaryKey, []string{"id"}) || !cs.SplitsByKey {
				t.Errorf("r: primary key %v, splits by key %v", cs.PrimaryKey, cs.SplitsByKey)
			}
		case "plain":
			if cs.PrimaryKey != nil || cs.SplitsByKey {
				t.Errorf("plain: primary key %v, splits by key %v", cs.PrimaryKey, cs.SplitsByKey)
			}
		}
		if !slices.Equal(cs.SyncModes, []source.SyncMode{source.FullRefresh, source.Incremental}) {
			t.Errorf("%s's sync modes: %v", cs.Name, cs.SyncModes)
		}
	}
	cs := source.ConfiguredStream{Name: "r", SyncMode: source.FullRefresh}
	if cs.Splits, err = s.Splits(t.Context(), cs, nil); err != nil || len(cs.Splits) < 2 {
		t.Fatalf("r's splits: %d, %v", len(cs.Splits), err)
	}
	c := newCollector()
	err = s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs, {Name: "plain", SyncMode: source.FullRefresh}}, Lanes: 3}, c)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.ids("r"); !slices.Equal(got, span(1, 2000)) {
		t.Errorf("r read %d rows", len(got))
	}
	for _, r := range c.rows["r"] {
		if want := map[bool]string{true: "new", false: "old"}[r["id"].(int64) <= 1000]; r["v"] != want {
			t.Fatalf("r's id %v is %v, not %s", r["id"], r["v"], want)
		}
	}
	if got := c.ids("plain"); !slices.Equal(got, span(1, 50)) || len(c.resumes["plain"]) != 1 {
		t.Errorf("plain read %d rows with %d resume points, not 50 with 1", len(got), len(c.resumes["plain"]))
	}

	err = s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "r", SyncMode: source.CDC}}}, newCollector())
	if err == nil || !strings.Contains(err.Error(), "no change log") {
		t.Errorf("a cdc read: %v", err)
	}
}

// A DateTime outside UTC resumes exactly across a fall-back hour, whose
// local times come twice, as a key and as a cursor.
func TestSourceZonedKey(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.z (at DateTime('Europe/Berlin'), id Int64) ENGINE = MergeTree ORDER BY at")
	e.exec("INSERT INTO $db.z SELECT toDateTime('2026-10-25 00:00:00', 'UTC') + number * 60, number + 1 FROM numbers(300)")
	s := newTestSource(t, db, `, "chunkRows": 7`)
	for _, cs := range []source.ConfiguredStream{{Name: "z", SyncMode: source.FullRefresh}, {Name: "z", SyncMode: source.Incremental, Cursor: "at"}} {
		c := newCollector()
		if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c); err != nil {
			t.Fatal(err)
		}
		if got := c.ids("z"); !slices.Equal(got, span(1, 300)) {
			t.Errorf("%v read %d rows, not ids 1 to 300 once", cs.SyncMode, len(got))
		}
	}
}

// A view is a stream with no key, read in one query, by a cursor too; a
// String that isn't UTF-8 arrives with U+FFFD in place of each bad byte.
func TestSourceViewsAndText(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.base (id Int64, s String) ENGINE = MergeTree ORDER BY id")
	e.exec("INSERT INTO $db.base VALUES (1, 'naïve'), (2, unhex('61FF62')), (3, '')")
	e.exec("CREATE VIEW $db.vw AS SELECT id, s FROM $db.base WHERE id > 1")
	s := newTestSource(t, db, `, "chunkRows": 1`)
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(cat.Streams, func(cs source.CatalogStream) bool { return cs.Name == "vw" })
	if i < 0 {
		t.Fatalf("no stream vw in %v", cat.Streams)
	}
	if vw := cat.Streams[i]; vw.PrimaryKey != nil || vw.SplitsByKey {
		t.Errorf("vw: primary key %v, splits by key %v", vw.PrimaryKey, vw.SplitsByKey)
	}
	for _, cs := range []source.ConfiguredStream{{Name: "vw", SyncMode: source.FullRefresh}, {Name: "vw", SyncMode: source.Incremental, Cursor: "id"}} {
		c := newCollector()
		if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c); err != nil {
			t.Fatal(err)
		}
		if got := c.ids("vw"); !slices.Equal(got, span(2, 3)) {
			t.Errorf("%v read ids %v, not 2 and 3", cs.SyncMode, got)
		}
		if cs.SyncMode == source.FullRefresh && len(c.resumes["vw"]) != 1 {
			t.Errorf("vw read with %d resume points, not 1", len(c.resumes["vw"]))
		}
	}
	c := newCollector()
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "base", SyncMode: source.FullRefresh}}}, c); err != nil {
		t.Fatal(err)
	}
	var got []any
	for _, r := range c.rows["base"] {
		got = append(got, r["s"])
	}
	if want := []any{"naïve", "a�b", ""}; !slices.Equal(got, want) {
		t.Errorf("base's s: %q, not %q", got, want)
	}
}

// An incremental read by a Nullable cursor reads in chunks, resuming after
// each, and leaves out the rows whose cursor is NULL.
func TestSourceNullableCursor(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.n (id Int64, at Nullable(Int64)) ENGINE = MergeTree ORDER BY id")
	e.exec("INSERT INTO $db.n SELECT number + 1, if(number % 10 = 9, NULL, intDiv(number, 3)) FROM numbers(100)")
	s := newTestSource(t, db, `, "chunkRows": 7`)
	c := newCollector()
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "n", SyncMode: source.Incremental, Cursor: "at"}}}, c); err != nil {
		t.Fatal(err)
	}
	var want []int64
	for id := int64(1); id <= 100; id++ {
		if id%10 != 0 {
			want = append(want, id)
		}
	}
	if got := c.ids("n"); !slices.Equal(got, want) {
		t.Errorf("read ids %v, not those whose cursor isn't NULL, once", got)
	}
	if len(c.resumes["n"]) < 2 {
		t.Errorf("read with %d resume points, not one a chunk", len(c.resumes["n"]))
	}
}

// A SummingMergeTree's incremental splits read up to the highest cursor of
// the rows FINAL gives, so a key whose unmerged parts sum past each of
// theirs is read.
func TestSourceFinalSplitsCursor(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.s (id UInt32, at Int64) ENGINE = SummingMergeTree ORDER BY id SETTINGS index_granularity = 32")
	e.exec("SYSTEM STOP MERGES $db.s")
	e.exec("INSERT INTO $db.s SELECT number + 1, 10 FROM numbers(1000)")
	e.exec("INSERT INTO $db.s SELECT number + 1, 20 FROM numbers(1000)")
	s := newTestSource(t, db, `, "chunkRows": 100`)
	cs := source.ConfiguredStream{Name: "s", SyncMode: source.Incremental, Cursor: "at"}
	var err error
	if cs.Splits, err = s.Splits(t.Context(), cs, nil); err != nil || len(cs.Splits) < 2 {
		t.Fatalf("splits: %d, %v", len(cs.Splits), err)
	}
	c := newCollector()
	readEnded(t, s, cs, c)
	if got := c.ids("s"); !slices.Equal(got, span(1, 1000)) {
		t.Errorf("read %d rows, not ids 1 to 1000 once", len(got))
	}
}

// A DateTime64 in an array arrives as its instant, past 2262 too, as a
// column's does.
func TestSourceDateTimeArrays(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.ad (id Int64, a Array(DateTime64(6, 'UTC')), an Array(Array(Nullable(DateTime64(3, 'Asia/Tokyo'))))) ENGINE = MergeTree ORDER BY id")
	e.exec("INSERT INTO $db.ad VALUES (1, ['2290-01-01 00:00:00.123456', '1900-01-01 00:00:00'], [[NULL, '2299-12-31 23:59:59.999']])")
	s := newTestSource(t, db, "")
	c := newCollector()
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "ad", SyncMode: source.FullRefresh}}}, c); err != nil {
		t.Fatal(err)
	}
	if len(c.rows["ad"]) != 1 {
		t.Fatalf("read %d rows", len(c.rows["ad"]))
	}
	b, _ := json.Marshal(c.rows["ad"][0])
	if want := `{"a":["2290-01-01T00:00:00.123456Z","1900-01-01T00:00:00Z"],"an":[[null,"2299-12-31T14:59:59.999Z"]],"id":1}`; string(b) != want {
		t.Errorf("read %s, not %s", b, want)
	}
}

// A sorting key's float column, which can hold NaN, isn't one a read orders
// and resumes by: NaN sorts last and compares false with every value, so a
// chunk after one with a NaN, or with the value before it, left it out.
// Rows whose float cursor is NaN aren't read, and a cursor all NaN reads
// later rows once they come.
func TestSourceNaN(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.fk (id Int64, f Float64) ENGINE = MergeTree ORDER BY f")
	e.exec("CREATE TABLE $db.gk (id Int64, g Int64, f Float64) ENGINE = MergeTree ORDER BY (g, f)")
	e.exec("INSERT INTO $db.fk SELECT number + 1, if(number % 4 = 3, nan, number) FROM numbers(40)")
	e.exec("INSERT INTO $db.gk SELECT number + 1, intDiv(number, 4), if(number % 4 = 3, nan, number) FROM numbers(40)")
	e.exec("CREATE TABLE $db.fc (id Int64, f Float64) ENGINE = MergeTree ORDER BY id")
	e.exec("INSERT INTO $db.fc VALUES (1, nan), (2, nan)")
	s := newTestSource(t, db, `, "chunkRows": 3`)
	for _, name := range []string{"fk", "gk"} {
		c := newCollector()
		if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: name, SyncMode: source.FullRefresh}}}, c); err != nil {
			t.Fatal(err)
		}
		if got := c.ids(name); !slices.Equal(got, span(1, 40)) {
			t.Errorf("%s read ids %v, not 1 to 40 once", name, got)
		}
	}
	cs := source.ConfiguredStream{Name: "fc", SyncMode: source.Incremental, Cursor: "f"}
	c := newCollector()
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c); err != nil {
		t.Fatal(err)
	}
	if got := c.ids("fc"); len(got) != 0 {
		t.Errorf("read ids %v whose cursor is NaN", got)
	}
	e.exec("INSERT INTO $db.fc VALUES (3, 1.5), (4, nan)")
	cs.State = c.complete["fc"]
	c = newCollector()
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c); err != nil {
		t.Fatal(err)
	}
	if got := c.ids("fc"); !slices.Equal(got, []int64{3}) {
		t.Errorf("the next read read ids %v, not 3", got)
	}
}

// A chunk reads about its own rows, not the table's rest, nor a group of a
// key's first column from its start: the read's rows read are about the
// table's, not its square's over 2. The server reads by blocks of 65,409
// rows, so a smaller table, or chunk, couldn't tell.
func TestSourceChunksSkipGranules(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.k (g Int64, id Int64) ENGINE = MergeTree ORDER BY (g, id)")
	e.exec("INSERT INTO $db.k SELECT intDiv(number, 250000), number + 1 FROM numbers(1000000)")
	s := newTestSource(t, db, `, "chunkRows": 100000`)
	var read atomic.Uint64
	ctx := ch.Context(t.Context(), ch.WithProgress(func(p *ch.Progress) { read.Add(p.Rows) }))
	c := newCollector()
	if err := s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "k", SyncMode: source.FullRefresh}}}, c); err != nil {
		t.Fatal(err)
	}
	if got := c.ids("k"); !slices.Equal(got, span(1, 1000000)) {
		t.Fatalf("read %d rows, not ids 1 to 1000000 once", len(got))
	}
	if n := read.Load(); n < 1000000 || n > 3500000 {
		t.Errorf("the read's 10 chunks read %d rows of a table of 1000000", n)
	}
}
