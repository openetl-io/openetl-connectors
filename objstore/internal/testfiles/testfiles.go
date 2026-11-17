// Package testfiles makes the files the files connectors' tests read.
package testfiles

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
	"gocloud.dev/blob"
)

// ParquetOf is a Parquet file of rows, as JSON, with rowGroup rows in each
// row group.
func ParquetOf(t testing.TB, sc *arrow.Schema, rows string, rowGroup int64) []byte {
	t.Helper()
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(rows))
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(sc, &buf, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(rowGroup)), pqarrow.DefaultWriterProps())
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

// Gz is s, gzipped.
func Gz(t testing.TB, s string) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write([]byte(s))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// Zst is s, in a zstd frame.
func Zst(t testing.TB, s string) []byte {
	w, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	return w.EncodeAll([]byte(s), nil)
}

// Sz is s, in snappy's framing format.
func Sz(t testing.TB, s string) []byte {
	var buf bytes.Buffer
	w := snappy.NewBufferedWriter(&buf)
	_, _ = w.Write([]byte(s))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// AvroSchema is the schema of AvroEvent: a field of each Avro type.
const AvroSchema = `{"type": "record", "name": "Event", "fields": [
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

// AvroEvent is the i-th event of AvroSchema.
func AvroEvent(i int) map[string]any {
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

// AvroOf is an object container file of n events, from first, in blocks of
// block records.
func AvroOf(t testing.TB, first, n, block int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, avro.MustParse(AvroSchema), ocf.WithBlockCount(block), ocf.WithCodec(ocf.SnappyCodec()))
	if err != nil {
		t.Fatal(err)
	}
	for i := first; i < first+n; i++ {
		if err := w.Encode(AvroEvent(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// Put writes files to the bucket open opens.
func Put(t testing.TB, open func(context.Context) (*blob.Bucket, error), files map[string][]byte) {
	t.Helper()
	b, err := open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	for _, k := range slices.Sorted(maps.Keys(files)) {
		if err := b.WriteAll(t.Context(), k, files[k], nil); err != nil {
			t.Fatal(err)
		}
	}
}
