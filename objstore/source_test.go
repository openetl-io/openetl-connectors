package objstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/openetl-io/openetl-connectors/objstore/internal/testfiles"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestSourceFile(t *testing.T) {
	sourceTests(t, func(t *testing.T) map[string]any { return map[string]any{"url": "file://" + t.TempDir()} })
}

// sourceTests runs the source's tests against the stores newStore makes,
// each test's empty.
func sourceTests(t *testing.T, newStore func(*testing.T) map[string]any) {
	for name, test := range map[string]func(*testing.T, map[string]any){
		"Formats":              testSourceFormats,
		"ParquetTypes":         testParquetTypes,
		"ParquetJSON":          testParquetJSON,
		"CSVOptions":           testCSVOptions,
		"IncrementalBoundary":  testIncrementalBoundary,
		"IncrementalRewritten": testIncrementalRewritten,
		"Splits":               testSourceSplits,
		"ResumeWithinFiles":    testResumeWithinFiles,
		"NewColumn":            testNewColumn,
		"TypesAcrossFiles":     testTypesAcrossFiles,
		"Avro":                 testAvro,
		"Resplit":              testResplit,
		"Preflight":            testSourcePreflight,
		"Versions":             testStoreVersions,
	} {
		t.Run(name, func(t *testing.T) { test(t, newStore(t)) })
	}
}

// put writes files to the store.
func put(t *testing.T, cfg map[string]any, files map[string][]byte) {
	t.Helper()
	b, _ := json.Marshal(cfg)
	d, err := testDestination(b)
	if err != nil {
		t.Fatal(err)
	}
	testfiles.Put(t, d.bucket, files)
}

func newSource(t *testing.T, cfg map[string]any, streams ...map[string]any) *Source {
	t.Helper()
	c := maps.Clone(cfg)
	c["streams"] = streams
	b, _ := json.Marshal(c)
	s, err := testSource(b)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// emitted is what a read sent, by stream and split.
type emitted struct {
	mu      sync.Mutex
	rows    map[string][]map[string]any
	batches map[string][]arrow.RecordBatch
	resumes map[string][][]byte
	done    map[string][]byte
	ended   map[string]bool
	// Called after each resume point, without the lock.
	onResume func(at string)
	// What Complete returns.
	completeErr error
}

func (e *emitted) Rows(stream string, rows []json.RawMessage) error {
	return e.Split(stream, source.WholeStream).Rows(rows)
}

func (e *emitted) Records(stream string, b arrow.RecordBatch) error {
	return e.Split(stream, source.WholeStream).Records(b)
}

func (e *emitted) ResumePoint(stream string, state []byte) error {
	return e.Split(stream, source.WholeStream).ResumePoint(state)
}

func (e *emitted) Complete(stream string, next []byte) error {
	return e.Split(stream, source.WholeStream).Complete(next)
}

func (e *emitted) Split(stream, id string) source.SplitEmitter {
	return splitOut{e, stream + "/" + id}
}

type splitOut struct {
	e  *emitted
	at string
}

func (s splitOut) Rows(rows []json.RawMessage) error {
	s.e.mu.Lock()
	defer s.e.mu.Unlock()
	for _, r := range rows {
		var m map[string]any
		d := json.NewDecoder(bytes.NewReader(r))
		d.UseNumber()
		if err := d.Decode(&m); err != nil {
			return err
		}
		s.e.rows[s.at] = append(s.e.rows[s.at], m)
	}
	return nil
}

func (s splitOut) Records(b arrow.RecordBatch) error {
	s.e.mu.Lock()
	defer s.e.mu.Unlock()
	s.e.batches[s.at] = append(s.e.batches[s.at], b)
	for i := range int(b.NumRows()) {
		m := map[string]any{}
		for c, f := range b.Schema().Fields() {
			if !b.Column(c).IsNull(i) {
				m[f.Name] = b.Column(c).ValueStr(i)
			}
		}
		s.e.rows[s.at] = append(s.e.rows[s.at], m)
	}
	return nil
}

func (s splitOut) ResumePoint(state []byte, added ...source.Split) error {
	s.e.mu.Lock()
	s.e.resumes[s.at] = append(s.e.resumes[s.at], state)
	s.e.mu.Unlock()
	if s.e.onResume != nil {
		s.e.onResume(s.at)
	}
	return nil
}

func (s splitOut) Complete(next []byte, added ...source.Split) error {
	s.e.mu.Lock()
	defer s.e.mu.Unlock()
	s.e.done[s.at], s.e.ended[s.at] = next, true
	return s.e.completeErr
}

// read reads a stream whole, from state, and returns what it sent.
func read(t *testing.T, s *Source, stream string, mode source.SyncMode, state []byte) (*emitted, error) {
	t.Helper()
	return readStreams(t, s, source.ConfiguredStream{Name: stream, SyncMode: mode, State: state})
}

func readStreams(t *testing.T, s *Source, streams ...source.ConfiguredStream) (*emitted, error) {
	t.Helper()
	return readWith(t, s, nil, streams...)
}

func readWith(t *testing.T, s *Source, onResume func(at string), streams ...source.ConfiguredStream) (*emitted, error) {
	t.Helper()
	e := &emitted{onResume: onResume, rows: map[string][]map[string]any{}, batches: map[string][]arrow.RecordBatch{}, resumes: map[string][][]byte{}, done: map[string][]byte{}, ended: map[string]bool{}}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	err := s.Read(ctx, source.ReadRequest{Streams: streams, Lanes: 4}, e)
	return e, err
}

func mustRead(t *testing.T, s *Source, stream string, mode source.SyncMode, state []byte) *emitted {
	t.Helper()
	e, err := read(t, s, stream, mode, state)
	if err != nil {
		t.Fatal(err)
	}
	if !e.ended[stream+"/"] {
		t.Fatalf("%s didn't complete", stream)
	}
	return e
}

// column is a column's values, in the order read, "" for null.
func values(rows []map[string]any, name string) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		if v, ok := r[name]; ok && v != nil {
			out[i] = fmt.Sprint(v)
		}
	}
	return out
}

var itemsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func testSourceFormats(t *testing.T, cfg map[string]any) {
	csvText := "id,name,at,price,ok\n1,a,2026-01-02T03:04:05Z,1.50,true\n2,,2026-01-02T03:04:06Z,2.25,false\n3,\"\",2026-01-02T03:04:07Z,3,true\n"
	jsonText := `{"id":1,"name":"a","tags":["x"],"n":1.5}` + "\n\n" + `{"id":2,"name":null,"tags":[],"n":2}` + "\n"
	put(t, cfg, map[string][]byte{
		"p/a.parquet":    parquetOf(t, itemsSchema, `[{"id":1,"name":"a"},{"id":2,"name":null}]`, 1),
		"p/b.parquet":    parquetOf(t, itemsSchema, `[{"id":3,"name":"c"}]`, 10),
		"p/skip.csv":     []byte("not,this\n"),
		"c/one.csv.gz":   gz(t, csvText),
		"c/two.csv.zst":  zst(t, csvText),
		"c/three.csv.sz": sz(t, csvText),
		"j/one.jsonl":    []byte(jsonText),
		"j/two.jsonl.gz": gz(t, jsonText),
	})
	s := newSource(t, cfg,
		map[string]any{"name": "p", "glob": "p/*.parquet", "format": "parquet"},
		map[string]any{"name": "c", "glob": "c/**", "format": "csv"},
		map[string]any{"name": "j", "glob": "j/*.{jsonl,jsonl.gz}", "format": "jsonl"},
	)
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]string{}
	for _, cs := range cat.Streams {
		if !slices.Equal(cs.PrimaryKey, []string{fileColumn, lineColumn}) || cs.CursorField != modifiedColumn || cs.Replay != source.Superset {
			t.Errorf("%s: key %v, cursor %q, replay %v", cs.Name, cs.PrimaryKey, cs.CursorField, cs.Replay)
		}
		for _, f := range cs.Schema.Fields() {
			types[cs.Name+"."+f.Name] = f.Type.String()
			if f.Metadata.FindKey("ARROW:extension:name") >= 0 {
				types[cs.Name+"."+f.Name] = "json"
			}
		}
	}
	for k, want := range map[string]string{
		"p.id": "int64", "p.name": "utf8", "p._modified": "timestamp[us, tz=UTC]", "p._line": "int64",
		"c.id": "int64", "c.name": "utf8", "c.at": "timestamp[us, tz=UTC]", "c.price": "float64", "c.ok": "bool",
		"j.id": "int64", "j.name": "utf8", "j.tags": "json", "j.n": "float64",
	} {
		if types[k] != want {
			t.Errorf("%s: %s, want %s", k, types[k], want)
		}
	}

	p := mustRead(t, s, "p", source.FullRefresh, nil).rows["p/"]
	if got := values(p, "id"); !slices.Equal(got, []string{"1", "2", "3"}) {
		t.Errorf("parquet ids %v", got)
	}
	if got := values(p, "name"); !slices.Equal(got, []string{"a", "", "c"}) {
		t.Errorf("parquet names %v", got)
	}
	if got := values(p, lineColumn); !slices.Equal(got, []string{"1", "2", "1"}) {
		t.Errorf("parquet lines %v", got)
	}
	if !strings.HasSuffix(values(p, fileColumn)[0], "p/a.parquet") {
		t.Errorf("parquet file %v", values(p, fileColumn)[0])
	}

	c := mustRead(t, s, "c", source.FullRefresh, nil).rows["c/"]
	if len(c) != 9 {
		t.Fatalf("csv: %d rows", len(c))
	}
	// One file's: an unquoted empty value is null, a quoted one empty.
	for i, want := range []any{"a", nil, ""} {
		if v, ok := c[i]["name"]; want == nil && ok || want != nil && v != want {
			t.Errorf("csv name %d: %v", i, v)
		}
	}

	j := mustRead(t, s, "j", source.FullRefresh, nil).rows["j/"]
	if got := values(j, "id"); !slices.Equal(got, []string{"1", "2", "1", "2"}) {
		t.Errorf("jsonl ids %v", got)
	}
	if got := values(j, lineColumn); !slices.Equal(got, []string{"1", "2", "1", "2"}) {
		t.Errorf("jsonl lines %v", got)
	}
}

// storedParquet is a Parquet file of one JSON value and a null in each of
// sc's columns, with sc stored in it, as the S3 destination stores it.
func storedParquet(t *testing.T, sc *arrow.Schema) []byte {
	t.Helper()
	rb := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer rb.Release()
	for i, f := range sc.Fields() {
		v := `"{\"a\":1}"`
		if f.Type.ID() == arrow.LIST {
			v = "[" + v + "]"
		}
		if err := rb.Field(i).UnmarshalJSON([]byte("[" + v + ", null]")); err != nil {
			t.Fatal(err)
		}
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()
	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(sc, &buf, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// A column of Parquet's JSON type is read as JSON, whatever the extension
// type its stored Arrow schema gives it; a file arrow-go can't read fails
// the read, naming it, and doesn't crash the source.
func testParquetJSON(t *testing.T, cfg map[string]any) {
	js, _ := extensions.NewJSONType(arrow.BinaryTypes.String)
	large, _ := extensions.NewJSONType(arrow.BinaryTypes.LargeString)
	put(t, cfg, map[string][]byte{
		"j/a.parquet": storedParquet(t, arrow.NewSchema([]arrow.Field{{Name: "j", Type: js, Nullable: true}, {Name: "jl", Type: large, Nullable: true}}, nil)),
		"n/a.parquet": storedParquet(t, arrow.NewSchema([]arrow.Field{{Name: "l", Type: arrow.ListOf(large), Nullable: true}}, nil)),
	})
	s := newSource(t, cfg, map[string]any{"name": "j", "glob": "j/*", "format": "parquet"}, map[string]any{"name": "n", "glob": "n/*", "format": "parquet"})
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range cat.Streams[0].Schema.Fields()[:2] {
		if ext, _ := f.Metadata.GetValue("ARROW:extension:name"); f.Type.ID() != arrow.STRING || ext != "arrow.json" {
			t.Errorf("%s: discovered %s %v, want JSON", f.Name, f.Type, f.Metadata)
		}
	}
	e := mustRead(t, s, "j", source.FullRefresh, nil)
	checkBatches(t, cat, e)
	b := e.batches["j/"][0]
	for _, c := range []int{0, 1} {
		if a := b.Column(c); a.ValueStr(0) != `{"a":1}` || !a.IsNull(1) {
			t.Errorf("%s: read %v", b.ColumnName(c), a)
		}
	}
	if _, err := read(t, s, "n", source.FullRefresh, nil); err == nil || !strings.Contains(err.Error(), "n/a.parquet") || !strings.Contains(err.Error(), `"l"`) {
		t.Errorf("a list of large_utf8 JSON: %v", err)
	}
}

func testParquetTypes(t *testing.T, cfg map[string]any) {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "ms", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "ns", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
		{Name: "d", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true},
		{Name: "big", Type: &arrow.Decimal256Type{Precision: 50, Scale: 0}, Nullable: true},
		{Name: "t32", Type: arrow.FixedWidthTypes.Time32ms, Nullable: true},
		{Name: "s", Type: arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32}), Nullable: true},
		{Name: "l", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		{Name: "dict", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}, Nullable: true},
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
		{Name: "u16", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
		{Name: "f16", Type: arrow.FixedWidthTypes.Float16, Nullable: true},
		{Name: "ls", Type: arrow.BinaryTypes.LargeString, Nullable: true},
		{Name: "fsb", Type: &arrow.FixedSizeBinaryType{ByteWidth: 2}, Nullable: true},
		{Name: "d64", Type: arrow.FixedWidthTypes.Date64, Nullable: true},
		{Name: "d256", Type: &arrow.Decimal256Type{Precision: 20, Scale: 2}, Nullable: true},
		{Name: "sec", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: "+02:00"}, Nullable: true},
	}, nil)
	rows := `[{"i8":-128,"u32":4294967295,"u64":18446744073709551615,"ms":"2026-01-02T03:04:05.678Z","ns":"1969-12-31T23:59:59.999999999","d":"-12345678.90","big":"12345678901234567890123456789012345678901234567890","t32":"23:59:59.999","s":{"a":1},"l":[1,2],"dict":"x",
	"u8":255,"u16":65535,"f16":1.5,"ls":"large","fsb":"AQI=","d64":"2026-01-02","d256":"-123456789012345678.90","sec":"2026-01-02T05:04:05+02:00"},
	{}]`
	put(t, cfg, map[string][]byte{"t/a.parquet": parquetOf(t, sc, rows, 10)})
	s := newSource(t, cfg, map[string]any{"name": "t", "glob": "t/*", "format": "parquet"})
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, f := range cat.Streams[0].Schema.Fields() {
		got[f.Name] = f.Type.String()
	}
	for k, want := range map[string]string{
		"i8": "int64", "u32": "int64", "u64": "decimal(20, 0)", "ms": "timestamp[us, tz=UTC]", "ns": "timestamp[us]",
		"d": "decimal(10, 2)", "big": "utf8", "t32": "time64[us]", "s": "utf8", "l": "list<element: int64, nullable>", "dict": "utf8",
		"u8": "int64", "u16": "int64", "f16": "float64", "ls": "utf8", "fsb": "binary", "d64": "date32", "d256": "decimal(20, 2)",
		"sec": "timestamp[us, tz=UTC]",
	} {
		if got[k] != want {
			t.Errorf("%s: discovered %s, want %s", k, got[k], want)
		}
	}
	e := mustRead(t, s, "t", source.FullRefresh, nil)
	b := e.batches["t/"][0]
	for i, f := range b.Schema().Fields() {
		want := cat.Streams[0].Schema.Field(i)
		if !arrow.TypeEqual(f.Type, want.Type) {
			t.Errorf("%s: read %s, discovered %s", f.Name, f.Type, want.Type)
		}
	}
	r := e.rows["t/"]
	for k, want := range map[string]string{
		"i8": "-128", "u32": "4294967295", "u64": "18446744073709551615", "d": "-12345678.9",
		"big": "12345678901234567890123456789012345678901234567890", "s": `{"a":1}`, "l": "[1,2]", "dict": "x",
		"ms": "2026-01-02T03:04:05.678Z", "ns": "1969-12-31T23:59:59.999999",
		"u8": "255", "u16": "65535", "f16": "1.5", "ls": "large", "fsb": "AQI=", "d64": "2026-01-02", "d256": "-123456789012345678.9",
		"sec": "2026-01-02T03:04:05Z",
	} {
		if got := fmt.Sprint(r[0][k]); got != want {
			t.Errorf("%s: %s, want %s", k, got, want)
		}
		if _, ok := r[1][k]; ok {
			t.Errorf("%s: null read as %v", k, r[1][k])
		}
	}
}

func testCSVOptions(t *testing.T, cfg map[string]any) {
	// windows-1252, semicolons, single quotes, a preamble line, no header,
	// NULL as null, and a field across lines.
	text := "generated by a tool\n1;'caf\xe9';NULL\n2;'two\nlines';'NULL'\n3;'it''s';\n"
	put(t, cfg, map[string][]byte{"o/a.txt": []byte(text)})
	s := newSource(t, cfg, map[string]any{"name": "o", "glob": "o/*.txt", "format": "csv",
		"csv": map[string]any{"delimiter": ";", "quote": "'", "header": false, "columnNames": []string{"id", "name", "note"},
			"skipRows": 1, "encoding": "windows-1252", "nullValues": []string{"NULL", ""}},
		"columns": map[string]string{"id": "integer", "note": "string"}})
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if f, _ := cat.Streams[0].Schema.FieldsByName("id"); f[0].Type.ID() != arrow.INT64 {
		t.Errorf("id: %s", f[0].Type)
	}
	r := mustRead(t, s, "o", source.FullRefresh, nil).rows["o/"]
	if got := values(r, "name"); !slices.Equal(got, []string{"café", "two\nlines", "it's"}) {
		t.Errorf("names %q", got)
	}
	for i, want := range []any{nil, "NULL", nil} {
		if v, ok := r[i]["note"]; want == nil && ok || want != nil && v != want {
			t.Errorf("note %d: %v", i, v)
		}
	}
}

// lines are the values of _file and _line a read sent, sorted.
func lines(e *emitted) []string {
	var out []string
	for _, rows := range e.rows {
		for _, r := range rows {
			f := fmt.Sprint(r[fileColumn])
			out = append(out, f[strings.LastIndex(f, "/")+1:]+":"+fmt.Sprint(r[lineColumn]))
		}
	}
	slices.Sort(out)
	return out
}

// A file last modified at the instant of one read before is read the next
// time, and one older than the lookback isn't.
func testIncrementalBoundary(t *testing.T, cfg map[string]any) {
	stream := map[string]any{"name": "i", "glob": "i/*.jsonl", "format": "jsonl"}
	put(t, cfg, map[string][]byte{"i/b.jsonl": []byte(`{"id":1}`)})
	s := newSource(t, cfg, stream)
	first := mustRead(t, s, "i", source.Incremental, nil)
	if got := lines(first); !slices.Equal(got, []string{"b.jsonl:1"}) {
		t.Fatalf("first: %v", got)
	}
	// a.jsonl sorts before b.jsonl, and may share its modification time
	// where the store keeps them to the second.
	put(t, cfg, map[string][]byte{"i/a.jsonl": []byte(`{"id":2}`), "i/c.jsonl": []byte(`{"id":3}`)})
	second := mustRead(t, s, "i", source.Incremental, first.done["i/"])
	if got := lines(second); !slices.Equal(got, []string{"a.jsonl:1", "c.jsonl:1"}) {
		t.Fatalf("second: %v", got)
	}
	third := mustRead(t, s, "i", source.Incremental, second.done["i/"])
	if got := lines(third); len(got) > 0 {
		t.Fatalf("third: %v", got)
	}
	// Past the lookback: the floor is after every file.
	var next readState
	_ = json.Unmarshal(third.done["i/"], &next)
	next.Floor, next.Read = time.Now().Add(time.Hour).UnixNano(), nil
	late, _ := json.Marshal(next)
	if got := lines(mustRead(t, s, "i", source.Incremental, late)); len(got) > 0 {
		t.Fatalf("below the floor: %v", got)
	}
}

// A file's version, where the store gives one, tells an overwrite in the
// same second; a state of before versions matches by time alone, and one
// without them is as it was.
func TestStateVersions(t *testing.T) {
	at := time.Unix(1000, 0)
	var st readState
	for i := range 2*maxRead + 1 {
		st.finish(object{Key: fmt.Sprint(i), Modified: at.Add(time.Duration(i)), Version: "v1"}, true)
	}
	if len(st.Versions) != len(st.Read) {
		t.Errorf("%d versions of %d files", len(st.Versions), len(st.Read))
	}
	last := fmt.Sprint(2 * maxRead)
	next := st.next(time.Minute)
	if len(next.Versions) != len(next.Read) || !next.done(object{Key: last, Modified: at.Add(2 * maxRead), Version: "v1"}, true) ||
		next.done(object{Key: last, Modified: at.Add(2 * maxRead), Version: "v2"}, true) {
		t.Errorf("an overwrite in the same second: %d versions of %d files", len(next.Versions), len(next.Read))
	}
	var old readState
	if err := json.Unmarshal([]byte(`{"read":{"a":1000000000000}}`), &old); err != nil {
		t.Fatal(err)
	}
	if !old.done(object{Key: "a", Modified: at, Version: "v2"}, true) {
		t.Error("a state of before versions reads its files again")
	}
	var plain readState
	plain.finish(object{Key: "a", Modified: at}, true)
	if b, _ := json.Marshal(plain.next(time.Minute)); string(b) != `{"floor":940000000000,"read":{"a":1000000000000}}` {
		t.Errorf("a state without versions: %s", b)
	}
}

// The state reads a file at the instant of the newest it read, which a
// cursor alone would skip, and none below its floor.
func TestStateBoundary(t *testing.T) {
	at := time.Unix(1000, 0)
	b := object{Key: "b", Modified: at}
	var st readState
	st.finish(b, true)
	next := st.next(time.Minute)
	for _, c := range []struct {
		o    object
		done bool
	}{
		{b, true},
		{object{Key: "a", Modified: at}, false},
		{object{Key: "b", Modified: at.Add(time.Second)}, false},
		{object{Key: "c", Modified: at.Add(-time.Minute)}, false},
		{object{Key: "c", Modified: at.Add(-time.Minute - 1)}, true},
	} {
		if got := next.done(c.o, true); got != c.done {
			t.Errorf("%s at %v: done %v", c.o.Key, c.o.Modified, got)
		}
	}
	if (&readState{}).done(object{Key: "epoch", Modified: time.Unix(0, 0)}, true) {
		t.Error("a fresh state has read a file of the Unix epoch")
	}
	var full readState
	full.finish(object{Key: "m"}, false)
	if !full.done(object{Key: "a"}, false) || full.done(object{Key: "n"}, false) {
		t.Error("a full refresh's state")
	}
	many := readState{Read: map[string]int64{}}
	for i := range maxRead + 10 {
		many.Read[fmt.Sprint(i)] = int64(i)
	}
	if n := many.next(time.Hour); len(n.Read) != maxRead || n.Floor != 10 {
		t.Errorf("past %d files: %d kept, floor %d", maxRead, len(n.Read), n.Floor)
	}
	// A read keeps its state's size in bounds as it goes.
	var growing readState
	for i := range 2*maxRead + 1 {
		growing.finish(object{Key: fmt.Sprint(i), Modified: time.Unix(0, int64(i))}, true)
	}
	if len(growing.Read) != maxRead || growing.Floor != maxRead+1 || growing.Read[fmt.Sprint(2*maxRead)] == 0 {
		t.Errorf("past %d files read: %d kept, floor %d", 2*maxRead, len(growing.Read), growing.Floor)
	}
}

// A read of many files sends a resume point every so many past 1,000, and
// restarts from any of them, or stops at it, without losing a record.
func TestManyFiles(t *testing.T) {
	cfg := map[string]any{"url": "file://" + t.TempDir()}
	files := map[string][]byte{}
	for i := range 1150 {
		files[fmt.Sprintf("m/%04d.jsonl", i)] = fmt.Appendf(nil, "{\"id\":%d}\n", i)
	}
	put(t, cfg, files)
	s := newSource(t, cfg, map[string]any{"name": "m", "glob": "m/*", "format": "jsonl"})
	whole := mustRead(t, s, "m", source.Incremental, nil)
	all := lines(whole)
	points := whole.resumes["m/"]
	if len(all) != 1150 || len(points) != 1001 {
		t.Fatalf("%d records, %d resume points", len(all), len(points))
	}
	for _, p := range [][]byte{points[0], points[999], points[1000]} {
		rest := mustRead(t, s, "m", source.Incremental, p)
		before, err := readStreams(t, s, source.ConfiguredStream{Name: "m", SyncMode: source.Incremental, Until: p})
		if err != nil || !bytes.Equal(before.done["m/"], p) {
			t.Fatalf("until a resume point: %v", err)
		}
		got := append(lines(rest), lines(before)...)
		slices.Sort(got)
		if !slices.Equal(got, all) {
			t.Fatalf("%d and %d records, of %d", len(lines(before)), len(lines(rest)), len(all))
		}
	}
}

// A read up to a resume point within a file fails when its completion
// does.
func TestUntilWithinFile(t *testing.T) {
	cfg := map[string]any{"url": "file://" + t.TempDir()}
	var data []byte
	for i := range batchRecords + 1 {
		data = fmt.Appendf(data, "{\"id\":%d}\n", i)
	}
	put(t, cfg, map[string][]byte{"u/a.jsonl": data})
	s := newSource(t, cfg, map[string]any{"name": "u", "glob": "u/*", "format": "jsonl"})
	within := mustRead(t, s, "u", source.FullRefresh, nil).resumes["u/"][0]
	refused := errors.New("refused")
	e := &emitted{completeErr: refused, rows: map[string][]map[string]any{}, batches: map[string][]arrow.RecordBatch{}, resumes: map[string][][]byte{}, done: map[string][]byte{}, ended: map[string]bool{}}
	err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "u", SyncMode: source.FullRefresh, Until: within}}, Lanes: 1}, e)
	if !errors.Is(err, refused) || !bytes.Equal(e.done["u/"], within) {
		t.Fatalf("completed at %s: %v", e.done["u/"], err)
	}
}

// A read up to a resume point stops where the first read sent it, by file
// and position, though files were written since, and completes there. A file
// it read records of up to the until, that its start hadn't, which is gone or
// written again since fails it with POSITION_LOST: it can't send those
// records again.
func TestUntilPastChangedFiles(t *testing.T) {
	var big []byte
	for i := range batchRecords + 1 {
		big = fmt.Appendf(big, "{\"id\":%d}\n", i)
	}
	at := time.Now().Add(-time.Hour).Truncate(time.Second)
	for _, c := range []struct {
		name string
		mode source.SyncMode
		// The first read's resume points the read again starts at, 1-based,
		// 0 for its beginning, and stops at: after a.jsonl, within
		// b.jsonl, or after it.
		from, until int
		// a.jsonl or b.jsonl, deleted, or written again, of 3 records, at
		// rewritten, a second after b.jsonl; or a file of 3 records written
		// at added, between a.jsonl and b.jsonl.
		deleted, rewritten, added string
		lost                      bool
		// The records read again, of each file.
		want map[string]int
	}{
		{name: "after a file read before it was deleted", mode: source.Incremental, until: 2, deleted: "a.jsonl", lost: true},
		{name: "within a file, one read before it deleted", mode: source.Incremental, until: 1, deleted: "a.jsonl", lost: true},
		{name: "after the deleted file itself", mode: source.Incremental, until: 0, deleted: "a.jsonl", lost: true},
		{name: "within its own file deleted", mode: source.Incremental, until: 1, deleted: "b.jsonl", lost: true},
		{name: "after a file read before it was written again", mode: source.Incremental, until: 2, rewritten: "a.jsonl", lost: true},
		{name: "within its own file written again", mode: source.FullRefresh, until: 1, rewritten: "b.jsonl", lost: true},
		{name: "after its own last file, deleted", mode: source.FullRefresh, until: 2, deleted: "b.jsonl", lost: true},
		{name: "after a file new since, before it", mode: source.Incremental, until: 2, added: "n.jsonl", want: map[string]int{"a.jsonl": 1, "n.jsonl": 3, "b.jsonl": batchRecords + 1}},
		{name: "from after a file deleted since", mode: source.Incremental, from: 1, until: 2, deleted: "a.jsonl", want: map[string]int{"b.jsonl": batchRecords + 1}},
		{name: "before a file deleted since", mode: source.Incremental, until: 0, deleted: "b.jsonl", want: map[string]int{"a.jsonl": 1}},
		{name: "from within a file", mode: source.FullRefresh, from: 2, until: 2, want: map[string]int{"b.jsonl": 1}},
		{name: "from within a file written again since", mode: source.FullRefresh, from: 2, until: 2, rewritten: "b.jsonl", lost: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := map[string]any{"url": "file://" + dir}
			put(t, cfg, map[string][]byte{"x/a.jsonl": []byte("{\"id\":1}\n"), "x/b.jsonl": big})
			touch := func(key string, to time.Time) {
				if err := os.Chtimes(filepath.Join(dir, key), to, to); err != nil {
					t.Fatal(err)
				}
			}
			touch("x/a.jsonl", at)
			touch("x/b.jsonl", at.Add(time.Second))
			s := newSource(t, cfg, map[string]any{"name": "x", "glob": "x/*", "format": "jsonl"})
			points := mustRead(t, s, "x", c.mode, nil).resumes["x/"]
			if len(points) != 3 {
				t.Fatalf("%d resume points", len(points))
			}
			if c.deleted != "" {
				if err := mustOpenBucket(t, s).Delete(t.Context(), "x/"+c.deleted); err != nil {
					t.Fatal(err)
				}
			}
			three := []byte("{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n")
			if c.rewritten != "" {
				put(t, cfg, map[string][]byte{"x/" + c.rewritten: three})
				touch("x/"+c.rewritten, at.Add(2*time.Second))
			}
			if c.added != "" {
				put(t, cfg, map[string][]byte{"x/" + c.added: three})
				touch("x/"+c.added, at.Add(time.Second/2))
			}
			var from []byte
			if c.from > 0 {
				from = points[c.from-1]
			}
			until := points[c.until]
			again, err := readStreams(t, s, source.ConfiguredStream{Name: "x", SyncMode: c.mode, State: from, Until: until})
			if c.lost {
				var coded interface{ ErrorCode() protocolv1.ErrorCode }
				if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST || len(again.rows["x/"]) > 0 || again.ended["x/"] {
					t.Fatalf("%v isn't POSITION_LOST before any record, the read sent %d and completed %v", err, len(again.rows["x/"]), again.ended["x/"])
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !again.ended["x/"] || !bytes.Equal(again.done["x/"], until) {
				t.Fatalf("a read up to %s completed at %s", until, again.done["x/"])
			}
			if resumes := again.resumes["x/"]; len(resumes) == 0 || !bytes.Equal(resumes[len(resumes)-1], until) {
				t.Errorf("its last resume point isn't its until")
			}
			got := map[string]int{}
			for _, l := range lines(again) {
				f, _, _ := strings.Cut(l, ":")
				got[f]++
			}
			if !maps.Equal(got, c.want) {
				t.Errorf("read again %v, not %v", got, c.want)
			}
		})
	}
}

// A decimal OpenETL can't hold, of more than 38 digits or with a scale
// outside 0 to its precision, is read as text.
func TestDecimalBounds(t *testing.T) {
	for _, c := range []struct {
		t    arrow.DataType
		want arrow.DataType
	}{
		{&arrow.Decimal128Type{Precision: 38, Scale: 38}, &arrow.Decimal128Type{Precision: 38, Scale: 38}},
		{&arrow.Decimal128Type{Precision: 10, Scale: -2}, arrow.BinaryTypes.String},
		{&arrow.Decimal128Type{Precision: 5, Scale: 6}, arrow.BinaryTypes.String},
		{&arrow.Decimal32Type{Precision: 5, Scale: -1}, arrow.BinaryTypes.String},
		{&arrow.Decimal64Type{Precision: 15, Scale: 3}, &arrow.Decimal128Type{Precision: 15, Scale: 3}},
		{&arrow.Decimal256Type{Precision: 39, Scale: 0}, arrow.BinaryTypes.String},
		{&arrow.Decimal256Type{Precision: 20, Scale: -3}, arrow.BinaryTypes.String},
	} {
		if got, _ := logical(arrow.Field{Name: "d", Type: c.t}); !arrow.TypeEqual(got.Type, c.want) {
			t.Errorf("%s: %s, want %s", c.t, got.Type, c.want)
		}
	}
}

// A stream whose config can't be read as it says is refused.
func TestStreamRefused(t *testing.T) {
	for name, sc := range map[string]map[string]any{
		"columnNames twice":        {"name": "s", "glob": "*", "format": "csv", "csv": map[string]any{"header": false, "columnNames": []string{"x", "y", "x"}}},
		"a column the source adds": {"name": "s", "glob": "*", "format": "parquet", "columns": map[string]string{"_file": "string"}},
	} {
		b, _ := json.Marshal(map[string]any{"url": "file://" + t.TempDir(), "streams": []any{sc}})
		if _, err := testSource(b); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestGlob(t *testing.T) {
	for _, c := range []struct {
		glob, prefix string
		match, not   []string
	}{
		{"data/*.csv", "data/", []string{"data/a.csv"}, []string{"data/x/a.csv", "data/a.csvx"}},
		{"data/**/*.csv", "data/", []string{"data/a.csv", "data/x/y/a.csv"}, []string{"other/a.csv"}},
		{"logs/2026-??/{a,b}.jsonl", "logs/2026-", []string{"logs/2026-01/a.jsonl"}, []string{"logs/2026-01/c.jsonl"}},
		{"x/[!.]*", "x/", []string{"x/a"}, []string{"x/.hidden"}},
		{"a.b+c", "a.b+c", []string{"a.b+c"}, []string{"aXb+c"}},
		{"café/*.csv", "café/", []string{"café/a.csv"}, []string{"cafe/a.csv"}},
	} {
		re, prefix, err := globRegexp(c.glob)
		if err != nil || prefix != c.prefix {
			t.Errorf("%s: prefix %q, %v", c.glob, prefix, err)
			continue
		}
		for _, k := range c.match {
			if !re.MatchString(k) {
				t.Errorf("%s doesn't match %s", c.glob, k)
			}
		}
		for _, k := range c.not {
			if re.MatchString(k) {
				t.Errorf("%s matches %s", c.glob, k)
			}
		}
	}
}

// A store that gives files' versions gives a reader the version it listed,
// for a file uploaded in parts too, and a file written again at once, at its
// size, is read again.
func testStoreVersions(t *testing.T, cfg map[string]any) {
	put(t, cfg, map[string][]byte{"w/a.jsonl": []byte("{\"id\":1}\n"), "w/parts": bytes.Repeat([]byte("x"), 10<<20)})
	s := newSource(t, cfg, map[string]any{"name": "w", "glob": "w/*", "format": "jsonl"}, map[string]any{"name": "a", "glob": "w/a.jsonl", "format": "jsonl"})
	if s.streams["w"].version == nil {
		t.Skip("the store gives no versions")
	}
	b, err := s.store.open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	files, err := s.streams["w"].list(t.Context(), b, wholeRange, false)
	if err != nil || len(files) != 2 {
		t.Fatal(files, err)
	}
	for _, o := range files {
		r, err := open(t.Context(), b, o, o.Size-1)
		if err != nil {
			t.Errorf("%s: %v", o.Key, err)
			continue
		}
		if read := o.version(r.As); o.Version == "" || read != o.Version {
			t.Errorf("%s: listed version %q, read %q", o.Key, o.Version, read)
		}
		_ = r.Close()
	}
	first := mustRead(t, s, "a", source.Incremental, nil)
	put(t, cfg, map[string][]byte{"w/a.jsonl": []byte("{\"id\":2}\n")})
	if got := values(mustRead(t, s, "a", source.Incremental, first.done["a/"]).rows["a/"], "id"); !slices.Equal(got, []string{"2"}) {
		t.Errorf("written again at once: %v", got)
	}
}

// A file written again is read again, whole.
func testIncrementalRewritten(t *testing.T, cfg map[string]any) {
	put(t, cfg, map[string][]byte{"r/a.jsonl": []byte("{\"id\":1}\n")})
	s := newSource(t, cfg, map[string]any{"name": "r", "glob": "r/*", "format": "jsonl"})
	first := mustRead(t, s, "r", source.Incremental, nil)
	// Stores keep modification times to the second, or finer.
	time.Sleep(1100 * time.Millisecond)
	put(t, cfg, map[string][]byte{"r/a.jsonl": []byte("{\"id\":1}\n{\"id\":2}\n")})
	if got := lines(mustRead(t, s, "r", source.Incremental, first.done["r/"])); !slices.Equal(got, []string{"a.jsonl:1", "a.jsonl:2"}) {
		t.Fatalf("rewritten: %v", got)
	}
}

func testSourceSplits(t *testing.T, cfg map[string]any) {
	files := map[string][]byte{}
	for i := range 20 {
		files[fmt.Sprintf("s/%02d.jsonl", i)] = fmt.Appendf(nil, "{\"id\":%d}\n", i)
	}
	put(t, cfg, files)
	s := newSource(t, cfg, map[string]any{"name": "s", "glob": "s/*", "format": "jsonl"})
	cs := source.ConfiguredStream{Name: "s", SyncMode: source.Incremental}
	splits, err := s.Splits(t.Context(), cs, nil)
	if err != nil || len(splits) != splitCount {
		t.Fatal(splits, err)
	}
	cs.Splits = splits
	e, err := readStreams(t, s, cs)
	if err != nil {
		t.Fatal(err)
	}
	if got := lines(e); len(got) != 20 || len(slices.Compact(got)) != 20 {
		t.Fatalf("splits read %v", got)
	}
	var ended []source.Split
	for _, sp := range splits {
		if !e.ended["s/"+sp.ID] {
			t.Fatalf("%s didn't complete", sp.ID)
		}
		ended = append(ended, source.Split{ID: sp.ID, State: e.done["s/"+sp.ID]})
	}
	// A split's read stops at its until.
	for _, sp := range splits {
		points := e.resumes["s/"+sp.ID]
		if len(points) < 2 {
			continue
		}
		cs := cs
		cs.Splits = []source.Split{{ID: sp.ID, Until: points[0]}}
		part, err := readStreams(t, s, cs)
		if err != nil || !bytes.Equal(part.done["s/"+sp.ID], points[0]) || len(part.rows["s/"+sp.ID]) != 1 {
			t.Fatalf("%s until %s: ended %s with %d rows, %v", sp.ID, points[0], part.done["s/"+sp.ID], len(part.rows["s/"+sp.ID]), err)
		}
		cs.Splits = []source.Split{{ID: sp.ID, State: points[0]}}
		rest, err := readStreams(t, s, cs)
		if err != nil || len(rest.rows["s/"+sp.ID]) != len(e.rows["s/"+sp.ID])-1 {
			t.Fatalf("%s from %s: %d rows of %d, %v", sp.ID, points[0], len(rest.rows["s/"+sp.ID]), len(e.rows["s/"+sp.ID]), err)
		}
		break
	}
	put(t, cfg, map[string][]byte{"s/new.jsonl": []byte("{\"id\":99}\n")})
	if cs.Splits, err = s.Splits(t.Context(), cs, ended); err != nil {
		t.Fatal(err)
	}
	e, err = readStreams(t, s, cs)
	if err != nil {
		t.Fatal(err)
	}
	if got := lines(e); !slices.Equal(got, []string{"new.jsonl:1"}) {
		t.Fatalf("the next read in splits read %v", got)
	}
}

// A read restarted from a resume point in a file goes on from there, and
// one with an until stops at it.
func testResumeWithinFiles(t *testing.T, cfg map[string]any) {
	var ndjson, csvText strings.Builder
	csvText.WriteString("id\n")
	n := batchRecords*2 + 5
	for i := range n {
		fmt.Fprintf(&ndjson, "{\"id\":%d}\n", i)
		fmt.Fprintf(&csvText, "%d\n", i)
	}
	var pq strings.Builder
	pq.WriteString("[")
	for i := range 30 {
		if i > 0 {
			pq.WriteString(",")
		}
		fmt.Fprintf(&pq, `{"id":%d}`, i)
	}
	pq.WriteString("]")
	put(t, cfg, map[string][]byte{
		"w/a.jsonl": []byte(ndjson.String()), "w/b.jsonl.gz": gz(t, ndjson.String()),
		"w/c.csv": []byte(csvText.String()), "w/d.csv.zst": zst(t, csvText.String()),
		"w/e.parquet": parquetOf(t, itemsSchema, pq.String(), 10),
		"w/f.avro":    avroOf(t, 0, n, 1000),
	})
	for _, c := range []struct{ glob, format string }{{"w/*.jsonl*", "jsonl"}, {"w/*.csv*", "csv"}, {"w/*.parquet", "parquet"}, {"w/*.avro", "avro"}} {
		t.Run(c.format, func(t *testing.T) {
			s := newSource(t, cfg, map[string]any{"name": "w", "glob": c.glob, "format": c.format})
			whole := mustRead(t, s, "w", source.FullRefresh, nil)
			all := lines(whole)
			points := whole.resumes["w/"]
			if len(points) < 3 {
				t.Fatalf("%d resume points", len(points))
			}
			for _, p := range points[:len(points)-1] {
				var st readState
				_ = json.Unmarshal(p, &st)
				rest := mustRead(t, s, "w", source.FullRefresh, p)
				before, err := readStreams(t, s, source.ConfiguredStream{Name: "w", SyncMode: source.FullRefresh, Until: p})
				if err != nil || !before.ended["w/"] || !bytes.Equal(before.done["w/"], p) {
					t.Fatalf("a read until %s ended with %s: %v", p, before.done["w/"], err)
				}
				got := append(lines(rest), lines(before)...)
				slices.Sort(got)
				if !slices.Equal(got, all) {
					t.Fatalf("from %s: %d and %d records, of %d", p, len(lines(before)), len(lines(rest)), len(all))
				}
			}
		})
	}
}

// A column a read's request doesn't declare fails it, retryably.
func testNewColumn(t *testing.T, cfg map[string]any) {
	put(t, cfg, map[string][]byte{
		"n/a.csv": []byte("id\n1\n"), "n/b.csv": []byte("id,extra\n2,x\n"),
		"n/a.parquet": parquetOf(t, itemsSchema, `[{"id":1}]`, 10),
		"n/a.jsonl":   []byte("{\"id\":1,\"extra\":2}\n"),
	})
	for _, format := range []string{formatCSV, formatParquet, formatJSONL} {
		s := newSource(t, cfg, map[string]any{"name": "n", "glob": "n/*." + format, "format": format})
		_, err := readStreams(t, s, source.ConfiguredStream{Name: "n", SyncMode: source.FullRefresh, Columns: []string{"id", fileColumn, lineColumn, modifiedColumn}})
		var coded interface{ ErrorCode() protocolv1.ErrorCode }
		if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
			t.Errorf("%s: a column the request doesn't declare, and the read returned %v", format, err)
		}
	}
}
