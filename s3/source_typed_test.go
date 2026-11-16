package s3

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
	"gocloud.dev/blob"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// checkBatches fails unless every batch's column has its declared type, or
// is text, which OpenETL parses to it.
func checkBatches(t *testing.T, cat source.Catalog, e *emitted) {
	t.Helper()
	for _, cs := range cat.Streams {
		for at, batches := range e.batches {
			if !strings.HasPrefix(at, cs.Name+"/") {
				continue
			}
			for _, b := range batches {
				for _, f := range b.Schema().Fields() {
					d, ok := cs.Schema.FieldsByName(f.Name)
					if !ok {
						t.Errorf("%s: undeclared column %q", cs.Name, f.Name)
						continue
					}
					if !arrow.TypeEqual(f.Type, d[0].Type) && f.Type.ID() != arrow.STRING {
						t.Errorf("%s: column %q is %s, declared %s", cs.Name, f.Name, f.Type, d[0].Type)
					}
				}
			}
		}
	}
}

// A column whose type differs between a stream's Parquet files is declared
// with a type every file's values fit, and read as it, or as text.
func testTypesAcrossFiles(t *testing.T, cfg map[string]any) {
	older := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "f", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "d", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "day", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "nday", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	newer := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "f", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "d", Type: &arrow.Decimal128Type{Precision: 20, Scale: 4}, Nullable: true},
		{Name: "s", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "day", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "nday", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: true},
		{Name: "id", Type: extensions.NewUUIDType(), Nullable: true},
	}, nil)
	put(t, cfg, map[string][]byte{
		"x/a.parquet": parquetOf(t, older, `[{"i":-2147483648,"f":1.5,"d":"12345678.91","s":"text","day":"2026-01-02","nday":"2026-01-02","id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8"},{}]`, 10),
		"x/b.parquet": parquetOf(t, newer, `[{"i":9223372036854775807,"f":0.1,"d":"1234567890123456.7891","s":42,"day":"2026-01-02T03:04:05.678Z","nday":"2026-01-02T03:04:05.678","id":"6ba7b810-9dad-11d1-80b4-00c04fd430c9"}]`, 10),
	})
	s := newSource(t, cfg, map[string]any{"name": "x", "glob": "x/*", "format": "parquet"})
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, f := range cat.Streams[0].Schema.Fields() {
		got[f.Name] = f.Type.String()
	}
	for k, want := range map[string]string{
		"i": "int64", "f": "float64", "d": "decimal(20, 4)", "s": "utf8", "day": "timestamp[us, tz=UTC]",
		"nday": "timestamp[us]", "id": "utf8",
	} {
		if got[k] != want {
			t.Errorf("%s: declared %s, want %s", k, got[k], want)
		}
	}
	e := mustRead(t, s, "x", source.FullRefresh, nil)
	checkBatches(t, cat, e)
	r := e.rows["x/"]
	for k, want := range map[string][]string{
		"i":   {"-2147483648", "", "9223372036854775807"},
		"f":   {"1.5", "", "0.1"},
		"d":   {"12345678.91", "", "1234567890123456.7891"},
		"s":   {"text", "", "42"},
		"day": {"2026-01-02T00:00:00Z", "", "2026-01-02T03:04:05.678Z"},
		// Text OpenETL parses: a naive timestamp has no offset.
		"nday": {"2026-01-02T00:00:00", "", "2026-01-02T03:04:05.678"},
		"id":   {"6ba7b810-9dad-11d1-80b4-00c04fd430c8", "", "6ba7b810-9dad-11d1-80b4-00c04fd430c9"},
	} {
		if got := values(r, k); !slices.Equal(got, want) {
			t.Errorf("%s: %q, want %q", k, got, want)
		}
	}
	for _, b := range e.batches["x/"] {
		for _, f := range b.Schema().Fields() {
			if text := f.Type.ID() == arrow.STRING; (f.Name == "d" || f.Name == "day" || f.Name == "nday") != text && f.Name != "s" && f.Name != "id" && f.Name != fileColumn {
				t.Errorf("%s: sent as %s", f.Name, f.Type)
			}
		}
	}

	// A type the config declares is sent as text.
	ds := newSource(t, cfg, map[string]any{"name": "x", "glob": "x/*", "format": "parquet", "columns": map[string]any{"i": "decimal(30,1)"}})
	dcat, err := ds.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	de := mustRead(t, ds, "x", source.FullRefresh, nil)
	checkBatches(t, dcat, de)
	if f, _ := dcat.Streams[0].Schema.FieldsByName("i"); f[0].Type.String() != "decimal(30, 1)" {
		t.Errorf("declared i: %s", f[0].Type)
	}
	if got := values(de.rows["x/"], "i"); !slices.Equal(got, []string{"-2147483648", "", "9223372036854775807"}) {
		t.Errorf("declared i: %q", got)
	}

	// A file written after the read took the stream's columns, with a type
	// they don't have, fails the read for the next attempt to read.
	typed := &typedColumns{}
	b := mustOpenBucket(t, s)
	if _, err := typed.of(t.Context(), b, s.streams["x"]); err != nil {
		t.Fatal(err)
	}
	other := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	put(t, cfg, map[string][]byte{"x/c.parquet": parquetOf(t, other, `[{"i":"x"}]`, 10)})
	out := &emitted{rows: map[string][]map[string]any{}, batches: map[string][]arrow.RecordBatch{}, resumes: map[string][][]byte{}, done: map[string][]byte{}, ended: map[string]bool{}}
	err = s.readSplit(t.Context(), b, typed, source.ConfiguredStream{Name: "x", SyncMode: source.FullRefresh}, source.Split{}, out.Split("x", ""))
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Errorf("a file of a type the read didn't see: %v", err)
	}
	checkBatches(t, cat, out)
}

const avroSchema = `{"type": "record", "name": "Event", "fields": [
	{"name": "b", "type": "boolean"},
	{"name": "i", "type": "int"},
	{"name": "l", "type": "long"},
	{"name": "f", "type": "float"},
	{"name": "d", "type": "double"},
	{"name": "s", "type": "string"},
	{"name": "raw", "type": "bytes"},
	{"name": "color", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]}},
	{"name": "fx", "type": {"type": "fixed", "name": "Two", "size": 2}},
	{"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
	{"name": "day", "type": {"type": "int", "logicalType": "date"}},
	{"name": "tms", "type": {"type": "int", "logicalType": "time-millis"}},
	{"name": "tus", "type": {"type": "long", "logicalType": "time-micros"}},
	{"name": "atms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
	{"name": "atus", "type": {"type": "long", "logicalType": "timestamp-micros"}},
	{"name": "local", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
	{"name": "id", "type": {"type": "string", "logicalType": "uuid"}},
	{"name": "maybe", "type": ["null", "string"]},
	{"name": "either", "type": ["null", "int", "string"]},
	{"name": "rec", "type": {"type": "record", "name": "Inner", "fields": [{"name": "n", "type": "int"}]}},
	{"name": "arr", "type": {"type": "array", "items": "long"}},
	{"name": "m", "type": {"type": "map", "values": "string"}}
]}`

func avroEvent(i int) map[string]any {
	at := time.Date(2026, 1, 2, 3, 4, 5, 678_000_000, time.UTC)
	ev := map[string]any{
		"b": i%2 == 0, "i": int32(-i), "l": int64(i) << 40, "f": float32(1.5), "d": 0.1, "s": fmt.Sprint("s", i),
		"raw": []byte{1, 2}, "color": "GREEN", "fx": [2]byte{3, 4}, "price": big.NewRat(-1234567891, 100),
		"day": at, "tms": 3*time.Hour + 4*time.Millisecond, "tus": 5 * time.Microsecond, "atms": at, "atus": at.Add(time.Microsecond),
		"local": at, "id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "maybe": nil, "either": int32(i),
		"rec": map[string]any{"n": int32(i)}, "arr": []any{int64(1), int64(2)}, "m": map[string]any{"k": "v"},
	}
	if i%2 == 1 {
		ev["maybe"], ev["either"] = "yes", "two"
	}
	return ev
}

// avroOf is an object container file of n events, from first, in blocks of
// block records.
func avroOf(t *testing.T, first, n, block int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, avro.MustParse(avroSchema), ocf.WithBlockCount(block), ocf.WithCodec(ocf.SnappyCodec()))
	if err != nil {
		t.Fatal(err)
	}
	for i := first; i < first+n; i++ {
		if err := w.Encode(avroEvent(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func testAvro(t *testing.T, cfg map[string]any) {
	put(t, cfg, map[string][]byte{"v/a.avro": avroOf(t, 0, 2, 1), "v/b.avro.gz": gz(t, string(avroOf(t, 2, 1, 1)))})
	s := newSource(t, cfg, map[string]any{"name": "v", "glob": "v/*", "format": "avro"})
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]string{}
	for _, f := range cat.Streams[0].Schema.Fields() {
		types[f.Name] = f.Type.String()
		if ext := extName(f); ext != "" {
			types[f.Name] = ext
		}
	}
	for k, want := range map[string]string{
		"b": "bool", "i": "int64", "l": "int64", "f": "float64", "d": "float64", "s": "utf8", "raw": "binary",
		"color": "utf8", "fx": "binary", "price": "decimal(10, 2)", "day": "date32", "tms": "time64[us]", "tus": "time64[us]",
		"atms": "timestamp[us, tz=UTC]", "atus": "timestamp[us, tz=UTC]", "local": "timestamp[us]", "id": uuidExt,
		"maybe": "utf8", "either": jsonExt, "rec": jsonExt, "arr": jsonExt, "m": jsonExt,
	} {
		if types[k] != want {
			t.Errorf("%s: declared %s, want %s", k, types[k], want)
		}
	}
	r := mustRead(t, s, "v", source.FullRefresh, nil).rows["v/"]
	if len(r) != 3 {
		t.Fatalf("%d rows", len(r))
	}
	for k, want := range map[string]string{
		"b": "true", "i": "0", "l": "0", "f": "1.5", "d": "0.1", "s": "s0", "raw": "AQI=", "color": "GREEN", "fx": "AwQ=",
		"price": "-12345678.91", "day": "2026-01-02", "tms": "03:00:00.004000", "tus": "00:00:00.000005",
		"atms": "2026-01-02T03:04:05.678Z", "atus": "2026-01-02T03:04:05.678001Z", "local": "2026-01-02T03:04:05.678",
		"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "maybe": "", lineColumn: "1",
	} {
		if got := values(r[:1], k)[0]; got != want {
			t.Errorf("%s: %q, want %q", k, got, want)
		}
	}
	// JSON columns' values are JSON, not text holding it.
	for k, want := range map[string]string{"rec": `{"n":0}`, "arr": "[1,2]", "m": `{"k":"v"}`, "either": "0"} {
		if got, _ := json.Marshal(r[0][k]); string(got) != want {
			t.Errorf("%s: %s, want %s", k, got, want)
		}
	}
	if got, _ := json.Marshal(r[1]["either"]); string(got) != `"two"` {
		t.Errorf("either: %s", got)
	}
	if got := values(r, lineColumn); !slices.Equal(got, []string{"1", "2", "1"}) {
		t.Errorf("lines: %q", got)
	}
}

const (
	uuidExt = "arrow.uuid"
	jsonExt = "arrow.json"
)

// Resplit halves a running split's files left: every file is read once,
// a read again up to a resume point past the halving stops there with the
// first read's records, and the next read goes on from the splits it left.
func testResplit(t *testing.T, cfg map[string]any) {
	files := map[string][]byte{}
	for i := range 60 {
		files[fmt.Sprintf("q/%02d.jsonl", i)] = fmt.Appendf(nil, "{\"id\":%d}\n", i)
	}
	put(t, cfg, files)
	s := newSource(t, cfg, map[string]any{"name": "q", "glob": "q/*", "format": "jsonl"})
	for _, mode := range []source.SyncMode{source.FullRefresh, source.Incremental} {
		cs := source.ConfiguredStream{Name: "q", SyncMode: mode}
		var mu sync.Mutex
		asked := map[string]bool{}
		resplit := 0
		onResume := func(at string) {
			split := strings.TrimPrefix(at, "q/")
			mu.Lock()
			defer mu.Unlock()
			if asked[split] {
				return
			}
			asked[split] = true
			ok, err := s.Resplit(t.Context(), "q", split)
			if err != nil {
				t.Error(err)
			}
			if ok {
				resplit++
			}
		}
		e, err := readWith(t, s, onResume, cs)
		if err != nil {
			t.Fatal(err)
		}
		if got := lines(e); len(got) != 60 || len(slices.Compact(got)) != 60 {
			t.Fatalf("%v: read %d records, %d distinct", mode, len(got), len(slices.Compact(got)))
		}
		if resplit == 0 {
			t.Fatalf("%v: no split was halved", mode)
		}
		var ended []source.Split
		for at, done := range e.ended {
			if done {
				ended = append(ended, source.Split{ID: strings.TrimPrefix(at, "q/"), State: e.done[at]})
			}
		}
		if len(ended) != 1+resplit {
			t.Fatalf("%v: %d splits completed, after %d halved", mode, len(ended), resplit)
		}

		// A read of a halved split from its first resume point, up to the
		// halving's and to its last.
		redone := 0
		for at, states := range e.resumes {
			var first, last readState
			if len(states) < 3 || json.Unmarshal(states[0], &first) != nil || json.Unmarshal(states[len(states)-1], &last) != nil || len(first.Gave) > 0 || len(last.Gave) == 0 {
				continue
			}
			want := map[string]bool{}
			for _, r := range e.rows[at] {
				want[fmt.Sprint(r[fileColumn], ":", r[lineColumn])] = true
			}
			for _, until := range [][]byte{states[1], states[len(states)-1]} {
				redo := cs
				redo.Splits = []source.Split{{ID: strings.TrimPrefix(at, "q/"), State: states[0], Until: until}}
				got, err := readStreams(t, s, redo)
				if err != nil {
					t.Fatal(err)
				}
				if done := got.done[at]; !bytes.Equal(done, until) || !got.ended[at] {
					t.Errorf("%v: %s read up to %s completed at %s", mode, at, until, done)
				}
				n := 0
				for _, r := range got.rows[at] {
					if k := fmt.Sprint(r[fileColumn], ":", r[lineColumn]); !want[k] {
						t.Errorf("%v: %s read again %s, which it gave away", mode, at, k)
					}
					n++
				}
				if n == 0 {
					t.Errorf("%v: %s read again nothing", mode, at)
				}
			}
			redone++
		}
		if redone == 0 {
			t.Errorf("%v: no halved split to read again", mode)
		}
		if mode != source.Incremental {
			continue
		}

		splits, err := s.Splits(t.Context(), cs, ended)
		if err != nil {
			t.Fatal(err)
		}
		put(t, cfg, map[string][]byte{"q/new.jsonl": []byte("{\"id\":99}\n")})
		cs.Splits = splits
		next, err := readStreams(t, s, cs)
		if err != nil {
			t.Fatal(err)
		}
		if got := lines(next); !slices.Equal(got, []string{"new.jsonl:1"}) {
			t.Fatalf("the next read read %v", got)
		}
	}
}

func TestHalve(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e", "f"}
	for _, at := range []string{"", "c", "zz"} {
		keep, give, ok := halve(wholeRange, keys, at)
		if !ok || keep.Hi != give.Lo && give.Hi != keep.Lo {
			t.Fatalf("at %q: %v, %v", at, keep, give)
		}
		gave := 0
		for _, k := range keys {
			if give.has(k) == keep.has(k) {
				t.Fatalf("at %q: %s in both or neither", at, k)
			}
			if give.has(k) {
				gave++
			}
		}
		if at != "" && !keep.has(at) || gave == 0 || gave == len(keys) {
			t.Errorf("at %q: gave %d of %d", at, gave, len(keys))
		}
	}
	if _, _, ok := halve(wholeRange, nil, "a"); ok {
		t.Error("halved no files")
	}
}

func mustOpenBucket(t *testing.T, s *Source) *blob.Bucket {
	t.Helper()
	b, err := s.store.open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// Preflight reads a file of each stream as its format, warns of a stream
// with none, and fails one whose file isn't of its format.
func testSourcePreflight(t *testing.T, cfg map[string]any) {
	put(t, cfg, map[string][]byte{"pf/a/1.jsonl": []byte("{\"id\":1}\n"), "pf/c/1.parquet": []byte("{\"id\":1}\n")})
	s := newSource(t, cfg,
		map[string]any{"name": "has", "glob": "pf/a/*.jsonl", "format": "jsonl"},
		map[string]any{"name": "none", "glob": "pf/b/*.jsonl", "format": "jsonl"},
		map[string]any{"name": "wrong", "glob": "pf/c/*", "format": "parquet"})
	req := source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: "has"}, {Name: "none"}, {Name: "wrong"}}}
	checks := s.Preflight(req)
	if err := check.Validate(append(s.Checks(), checks...)); err != nil {
		t.Fatal(err)
	}
	r := check.Run(t.Context(), checks)
	got := map[string]string{}
	for _, it := range r.Items {
		got[it.Name] = it.Status.String()
		if it.Failure != nil {
			got[it.Name] += " " + it.Failure.Reason
		}
	}
	want := map[string]string{
		"stream.has.read": "passed", "stream.has.format": "passed",
		"stream.none.read": "warning no_files", "stream.none.format": "skipped",
		"stream.wrong.read": "passed", "stream.wrong.format": "failed file_unreadable",
	}
	if !maps.Equal(got, want) {
		t.Errorf("preflight: %v", got)
	}
	if it := r.Items[0].Rendered(); !strings.HasSuffix(it.Message, "pf/a/1.jsonl.") || it.Field != "streams.0.glob" {
		t.Errorf("a stream with a file: %+v", it)
	}
	if r := check.Run(t.Context(), s.Checks()); !r.OK() {
		t.Errorf("check: %v", r.Err())
	}
}

// Streams whose names give one item name get items of their own.
func TestPreflightNames(t *testing.T) {
	cfg := map[string]any{"url": "file://" + t.TempDir()}
	s := newSource(t, cfg,
		map[string]any{"name": "Orders", "glob": "a/*", "format": "jsonl"},
		map[string]any{"name": "orders", "glob": "b/*", "format": "jsonl"},
		map[string]any{"name": "orders-2", "glob": "c/*", "format": "jsonl"})
	req := source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "Orders"}, {Name: "orders"}, {Name: "orders-2"}}}
	if err := check.Validate(append(s.Checks(), s.Preflight(req)...)); err != nil {
		t.Error(err)
	}
	b, _ := json.Marshal(cfg)
	d, err := NewDestination(b)
	if err != nil {
		t.Fatal(err)
	}
	dreq := destination.PreflightRequest{Streams: []destination.Stream{{Name: "a.b", Mode: destination.Append}, {Name: "a_b", Mode: destination.Overwrite}}}
	if err := check.Validate(append(d.Checks(), d.Preflight(dreq)...)); err != nil {
		t.Error(err)
	}
}
