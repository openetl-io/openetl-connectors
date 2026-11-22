package objstore

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"

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

// avroFile is an object container file of schema's records.
func avroFile(t *testing.T, schema string, records ...any) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, avro.MustParse(schema))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if err := w.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// A decimal within a record, an array or a union keeps its digits.
func TestAvroNestedDecimal(t *testing.T) {
	cfg := map[string]any{"url": "file://" + t.TempDir()}
	tiny := new(big.Rat).SetFrac(big.NewInt(1), new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil))
	half := big.NewRat(1, 2)
	put(t, cfg, map[string][]byte{"d/a.avro": avroFile(t, `{"type": "record", "name": "R", "fields": [
		{"name": "rec", "type": {"type": "record", "name": "In", "fields": [
			{"name": "x", "type": {"type": "bytes", "logicalType": "decimal", "precision": 25, "scale": 20}}]}},
		{"name": "arr", "type": {"type": "array", "items": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 1}}},
		{"name": "either", "type": ["null", "boolean", {"type": "bytes", "logicalType": "decimal", "precision": 25, "scale": 20}]}
	]}`, map[string]any{"rec": map[string]any{"x": tiny}, "arr": []any{half}, "either": tiny})})
	s := newSource(t, cfg, map[string]any{"name": "d", "glob": "d/*", "format": "avro"})
	r := mustRead(t, s, "d", source.FullRefresh, nil).rows["d/"]
	for k, want := range map[string]string{"rec": `{"x":"0.00000000000000000001"}`, "arr": `["0.5"]`, "either": `"0.00000000000000000001"`} {
		if got, _ := json.Marshal(r[0][k]); string(got) != want {
			t.Errorf("%s: %s, want %s", k, got, want)
		}
	}
}

// A named type that fields name again maps as it does where it's defined.
func TestAvroNamedTypes(t *testing.T) {
	cfg := map[string]any{"url": "file://" + t.TempDir()}
	id := [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	put(t, cfg, map[string][]byte{"n/a.avro": avroFile(t, `{"type": "record", "name": "R", "fields": [
		{"name": "a", "type": {"type": "fixed", "name": "U", "size": 16, "logicalType": "uuid"}},
		{"name": "b", "type": "U"},
		{"name": "c", "type": ["null", "U"]},
		{"name": "list", "type": {"type": "record", "name": "L", "fields": [{"name": "next", "type": ["null", "L"]}]}}
	]}`, map[string]any{"a": id, "b": id, "c": id, "list": map[string]any{"next": map[string]any{"next": nil}}})})
	s := newSource(t, cfg, map[string]any{"name": "n", "glob": "n/*", "format": "avro"})
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r := mustRead(t, s, "n", source.FullRefresh, nil).rows["n/"]
	for i, f := range cat.Streams[0].Schema.Fields()[:3] {
		if extName(f) != uuidExt {
			t.Errorf("%s: declared %s", f.Name, f.Type)
		}
		if got := values(r, f.Name); len(got) != 1 || got[0] != "6ba7b810-9dad-11d1-80b4-00c04fd430c8" {
			t.Errorf("field %d, %s: %q", i, f.Name, got)
		}
	}
	if got, _ := json.Marshal(r[0]["list"]); string(got) != `{"next":{"next":null}}` {
		t.Errorf("a recursive record: %s", got)
	}
}

// A duration logical type is declared as JSON and read as a JSON object of
// months, days and milliseconds, at the top level, in unions and nested.
func TestAvroDuration(t *testing.T) {
	cfg := map[string]any{"url": "file://" + t.TempDir()}
	d1 := avro.Duration{Months: 1, Days: 2, Milliseconds: 3}
	d0 := avro.Duration{}
	raw12 := [12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	schema := `{"type": "record", "name": "R", "fields": [
		{"name": "d", "type": {"type": "fixed", "name": "Dur", "size": 12, "logicalType": "duration"}},
		{"name": "named", "type": "Dur"},
		{"name": "maybe", "type": ["null", "Dur"]},
		{"name": "either", "type": ["null", "string", "Dur"]},
		{"name": "rec", "type": {"type": "record", "name": "In", "fields": [{"name": "d", "type": "Dur"}]}},
		{"name": "arr", "type": {"type": "array", "items": "Dur"}},
		{"name": "m", "type": {"type": "map", "values": "Dur"}},
		{"name": "raw12", "type": {"type": "fixed", "name": "Raw12", "size": 12}}
	]}`
	put(t, cfg, map[string][]byte{"dur/a.avro": avroFile(t, schema,
		map[string]any{
			"d": d1, "named": d1, "maybe": d1, "either": d1,
			"rec": map[string]any{"d": d1}, "arr": []any{d1}, "m": map[string]any{"k": d1},
			"raw12": raw12,
		},
		map[string]any{
			"d": d0, "named": d0, "maybe": nil, "either": "none",
			"rec": map[string]any{"d": d0}, "arr": []any{d0}, "m": map[string]any{},
			"raw12": raw12,
		},
	)})
	s := newSource(t, cfg, map[string]any{"name": "dur", "glob": "dur/*", "format": "avro"})
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	gotTypes := map[string]string{}
	for _, f := range cat.Streams[0].Schema.Fields() {
		gotTypes[f.Name] = f.Type.String()
		if ext := extName(f); ext != "" {
			gotTypes[f.Name] = ext
		}
	}
	for _, col := range []string{"d", "named", "maybe", "either", "rec", "arr", "m"} {
		if gotTypes[col] != jsonExt {
			t.Errorf("%s: declared %s, want %s", col, gotTypes[col], jsonExt)
		}
	}
	if gotTypes["raw12"] != "binary" {
		t.Errorf("raw12: declared %s, want binary", gotTypes["raw12"])
	}

	e := mustRead(t, s, "dur", source.FullRefresh, nil)
	checkBatches(t, cat, e)
	r := e.rows["dur/"]
	if len(r) != 2 {
		t.Fatalf("%d rows", len(r))
	}
	// JSON columns' values are JSON objects/arrays, not text holding them.
	if _, ok := r[0]["d"].(map[string]any); !ok {
		t.Fatalf("d is %T, want JSON object", r[0]["d"])
	}
	for k, want := range map[string]string{
		"d":      `{"days":2,"milliseconds":3,"months":1}`,
		"named":  `{"days":2,"milliseconds":3,"months":1}`,
		"maybe":  `{"days":2,"milliseconds":3,"months":1}`,
		"either": `{"days":2,"milliseconds":3,"months":1}`,
		"rec":    `{"d":{"days":2,"milliseconds":3,"months":1}}`,
		"arr":    `[{"days":2,"milliseconds":3,"months":1}]`,
		"m":      `{"k":{"days":2,"milliseconds":3,"months":1}}`,
	} {
		if got, _ := json.Marshal(r[0][k]); string(got) != want {
			t.Errorf("row 0 %s: %s, want %s", k, got, want)
		}
	}
	if got, _ := json.Marshal(r[1]["d"]); string(got) != `{"days":0,"milliseconds":0,"months":0}` {
		t.Errorf("row 1 d: %s", got)
	}
	if v, ok := r[1]["maybe"]; !ok || v != nil {
		t.Errorf("row 1 maybe: %v (present=%v), want nil", v, ok)
	}
	if got, _ := json.Marshal(r[1]["either"]); string(got) != `"none"` {
		t.Errorf("row 1 either: %s", got)
	}
	if got := values(r[:1], "raw12")[0]; got != "AQIDBAUGBwgJCgsM" {
		t.Errorf("raw12: %q, want AQIDBAUGBwgJCgsM", got)
	}

	// Declared as string via columns: emitted as JSON text.
	ss := newSource(t, cfg, map[string]any{"name": "dur", "glob": "dur/*", "format": "avro", "columns": map[string]string{"d": "string"}})
	sr := mustRead(t, ss, "dur", source.FullRefresh, nil).rows["dur/"]
	if got, ok := sr[0]["d"].(string); !ok || got != `{"days":2,"milliseconds":3,"months":1}` {
		t.Errorf("string-declared d: %#v (%T)", sr[0]["d"], sr[0]["d"])
	}

	// Cross-file widening between duration and string yields utf8 JSON text.
	put(t, cfg, map[string][]byte{
		"wide/a.avro": avroFile(t, `{"type": "record", "name": "W", "fields": [{"name": "d", "type": {"type": "fixed", "name": "Dur", "size": 12, "logicalType": "duration"}}]}`, map[string]any{"d": d1}),
		"wide/b.avro": avroFile(t, `{"type": "record", "name": "W", "fields": [{"name": "d", "type": "string"}]}`, map[string]any{"d": "plain"}),
	})
	ws := newSource(t, cfg, map[string]any{"name": "wide", "glob": "wide/*", "format": "avro"})
	wcat, err := ws.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	wf := wcat.Streams[0].Schema.Fields()[0]
	if wf.Type.ID() != arrow.STRING || extName(wf) != "" {
		t.Errorf("widened d: %s (ext %q), want plain utf8", wf.Type, extName(wf))
	}
	we := mustRead(t, ws, "wide", source.FullRefresh, nil)
	checkBatches(t, wcat, we)
	if got := values(we.rows["wide/"], "d"); !slices.Equal(got, []string{`{"days":2,"milliseconds":3,"months":1}`, "plain"}) {
		t.Errorf("widened d rows: %q", got)
	}
}

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
			keys := func(rows []map[string]any) []string {
				var out []string
				for _, r := range rows {
					out = append(out, fmt.Sprint(r[fileColumn], ":", r[lineColumn]))
				}
				return out
			}
			// Each file's one record, then its resume point.
			sent := keys(e.rows[at])
			if len(sent) != len(states) {
				t.Fatalf("%v: %s sent %d records and %d resume points", mode, at, len(sent), len(states))
			}
			for _, i := range []int{1, len(states) - 1} {
				until := states[i]
				redo := cs
				redo.Splits = []source.Split{{ID: strings.TrimPrefix(at, "q/"), State: states[0], Until: until}}
				got, err := readStreams(t, s, redo)
				if err != nil {
					t.Fatal(err)
				}
				if done := got.done[at]; !bytes.Equal(done, until) || !got.ended[at] {
					t.Errorf("%v: %s read up to %s completed at %s", mode, at, until, done)
				}
				if again := keys(got.rows[at]); !slices.Equal(again, sent[1:i+1]) {
					t.Errorf("%v: %s read up to its resume point %d again %v, not %v", mode, at, i, again, sent[1:i+1])
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
	put(t, cfg, map[string][]byte{"pf/a/1.jsonl": []byte("{\"id\":1}\n"), "pf/c/1.parquet": []byte("{\"id\":1}\n"), "pf/e/1.jsonl": nil})
	s := newSource(t, cfg,
		map[string]any{"name": "has", "glob": "pf/a/*.jsonl", "format": "jsonl"},
		map[string]any{"name": "none", "glob": "pf/b/*.jsonl", "format": "jsonl"},
		map[string]any{"name": "wrong", "glob": "pf/c/*", "format": "parquet"},
		map[string]any{"name": "empty", "glob": "pf/e/*", "format": "jsonl"})
	req := source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: "has"}, {Name: "none"}, {Name: "wrong"}, {Name: "empty"}}}
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
		"stream.empty.read": "passed", "stream.empty.format": "passed",
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
	d, err := testDestination(b)
	if err != nil {
		t.Fatal(err)
	}
	dreq := destination.PreflightRequest{Streams: []destination.Stream{{Name: "a.b", Mode: destination.Append}, {Name: "a_b", Mode: destination.Overwrite}}}
	if err := check.Validate(append(d.Checks(), d.Preflight(dreq)...)); err != nil {
		t.Error(err)
	}
}

// A CSV column with a value of a leading zero, as a code like 007 has, is
// text, which keeps the zero; 0 itself and decimals like 0.5 aren't such.
func TestInferLeadingZeros(t *testing.T) {
	for _, c := range []struct {
		values []string
		want   string
	}{
		{[]string{"1", "007"}, "utf8"},
		{[]string{"-007", "1"}, "utf8"},
		{[]string{"00.5", "1.5"}, "utf8"},
		{[]string{"0", "10", "-3"}, "int64"},
		{[]string{"0.5", "-0.25", "0"}, "float64"},
	} {
		in := &inferred{}
		for _, v := range c.values {
			in.text(v)
		}
		if got := in.field("n").Type.String(); got != c.want {
			t.Errorf("%v: %s, want %s", c.values, got, c.want)
		}
	}
}

// A text or JSON column of numbers that a 64-bit float can't hold exactly,
// such as IDs past int64's range, is text, which keeps their digits, not
// number, which would round them.
func TestInferExactNumbers(t *testing.T) {
	for _, c := range []struct {
		values []string
		want   string
	}{
		{[]string{"1", "2.5", "1e3", "-0.125", "0.1"}, "float64"},
		{[]string{"1", "9223372036854775807"}, "int64"},
		{[]string{"1", "12345678901234567890"}, "utf8"},
		{[]string{"0.1", "9007199254740993"}, "utf8"},
		{[]string{"1.5", "1234567890.123456789"}, "utf8"},
	} {
		text, js := &inferred{}, &inferred{}
		for _, v := range c.values {
			text.text(v)
			js.json(json.RawMessage(v))
		}
		for name, in := range map[string]*inferred{"text": text, "json": js} {
			if got := in.field("n").Type.String(); got != c.want {
				t.Errorf("%s %v: %s, want %s", name, c.values, got, c.want)
			}
		}
	}
	nan := &inferred{}
	nan.text("1.5")
	nan.text("NaN")
	if got := nan.field("n").Type.String(); got != "float64" {
		t.Errorf("NaN: %s", got)
	}
}

// A file's own _file, _line or _modified column, as a file the destination
// wrote from a files source has, gives way to the source's: each format's
// read sends one column of each name, with the source's values.
func TestControlColumnsOfFiles(t *testing.T) {
	cfg := map[string]any{"url": "file://" + t.TempDir()}
	own := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "_file", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "_line", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	put(t, cfg, map[string][]byte{
		"p/a.parquet": parquetOf(t, own, `[{"id":1,"_file":"elsewhere","_line":7}]`, 1),
		"c/a.csv":     []byte("id,_file,_line\n1,elsewhere,7\n"),
		"j/a.jsonl":   []byte(`{"id":1,"_file":"elsewhere","_line":7}` + "\n"),
		"a/a.avro": avroFile(t, `{"type": "record", "name": "R", "fields": [{"name": "id", "type": "long"}, {"name": "_file", "type": "string"}, {"name": "_line", "type": "long"}]}`,
			map[string]any{"id": int64(1), "_file": "elsewhere", "_line": int64(7)}),
	})
	s := newSource(t, cfg,
		map[string]any{"name": "p", "glob": "p/*", "format": "parquet"},
		map[string]any{"name": "c", "glob": "c/*", "format": "csv"},
		map[string]any{"name": "j", "glob": "j/*", "format": "jsonl"},
		map[string]any{"name": "a", "glob": "a/*", "format": "avro"},
	)
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{"p", "c", "j", "a"} {
		e, err := read(t, s, stream, source.FullRefresh, nil)
		if err != nil {
			t.Errorf("%s: %v", stream, err)
			continue
		}
		checkBatches(t, cat, e)
		for _, b := range e.batches[stream+"/"] {
			names := map[string]bool{}
			for _, f := range b.Schema().Fields() {
				if names[f.Name] {
					t.Errorf("%s: two columns %q", stream, f.Name)
				}
				names[f.Name] = true
			}
		}
		r := e.rows[stream+"/"]
		if len(r) != 1 || !strings.HasSuffix(fmt.Sprint(r[0][fileColumn]), stream+"/a."+map[string]string{"p": "parquet", "c": "csv", "j": "jsonl", "a": "avro"}[stream]) ||
			fmt.Sprint(r[0][lineColumn]) != "1" {
			t.Errorf("%s: %v", stream, r)
		}
	}
}

// A file of a compression the source doesn't read fails the read with
// DATA_INVALID, which isn't retried, rather than giving its compressed bytes
// as text.
func TestUnreadCompression(t *testing.T) {
	cfg := map[string]any{"url": "file://" + t.TempDir()}
	put(t, cfg, map[string][]byte{"c/a.csv.xz": []byte("\xfd7zXZ\x00 not csv\n")})
	s := newSource(t, cfg, map[string]any{"name": "c", "glob": "c/*", "format": "csv"})
	e, err := read(t, s, "c", source.FullRefresh, nil)
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID || !strings.Contains(err.Error(), "c/a.csv.xz") {
		t.Errorf("read: %v, %v", err, e.rows)
	}
	r := check.Run(t.Context(), s.Preflight(source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "c"}}}))
	if it := r.Items[len(r.Items)-1]; it.Name != "stream.c.format" || it.Failure == nil || it.Failure.Reason != "file_unreadable" {
		t.Errorf("preflight: %+v", it)
	}
}

// A file named .bz2 is decompressed with bzip2.
func TestBzip2(t *testing.T) {
	cfg := map[string]any{"url": "file://" + t.TempDir()}
	// "id,name\n1,a\n2,b\n", from bzip2 -9.
	bz, _ := base64.StdEncoding.DecodeString("QlpoOTFBWSZTWaAwe34AAAbZAAAQAAQwADYjIAAxANNNBAMQIEFFl0b17fi7kinChIUBg9vw")
	put(t, cfg, map[string][]byte{"c/a.csv.bz2": bz})
	s := newSource(t, cfg, map[string]any{"name": "c", "glob": "c/*", "format": "csv"})
	r := mustRead(t, s, "c", source.FullRefresh, nil).rows["c/"]
	if got := values(r, "name"); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("names %v", got)
	}
}
