package kafka

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

func meta(ext string) arrow.Metadata {
	return arrow.NewMetadata([]string{extensionNameKey}, []string{ext})
}

var tableMeta = arrow.NewMetadata([]string{tableKey}, []string{"items"})

// row is one row of every type the destination writes.
func row(t *testing.T) arrow.RecordBatch {
	t.Helper()
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "price", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "local", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "day", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "uid", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true, Metadata: meta(uuidExtension)},
		{Name: "doc", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: meta(jsonExtension)},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "ok", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "raw", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "missing", Type: arrow.BinaryTypes.String, Nullable: true},
	}, &tableMeta)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	at := time.Date(2026, 11, 2, 10, 30, 0, 123456000, time.UTC)
	b.Field(0).(*array.Int64Builder).Append(7)
	b.Field(1).(*array.StringBuilder).Append(`a "quoted" <name>`)
	b.Field(2).(*array.Decimal128Builder).Append(decimal128.FromI64(1999))
	b.Field(3).(*array.TimestampBuilder).Append(arrow.Timestamp(at.UnixMicro()))
	b.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(at.UnixMicro()))
	b.Field(5).(*array.Date32Builder).Append(arrow.Date32FromTime(at))
	b.Field(6).(*array.FixedSizeBinaryBuilder).Append([]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8})
	b.Field(7).(*array.StringBuilder).Append(`{"a":[1,2]}`)
	lb := b.Field(8).(*array.ListBuilder)
	lb.Append(true)
	lb.ValueBuilder().(*array.StringBuilder).AppendValues([]string{"x", "y"}, nil)
	b.Field(9).(*array.BooleanBuilder).Append(true)
	b.Field(10).(*array.Float64Builder).Append(math.NaN())
	b.Field(11).(*array.BinaryBuilder).Append([]byte{0, 1, 2})
	b.Field(12).(*array.StringBuilder).AppendNull()
	return b.NewRecordBatch()
}

func values(t *testing.T, batch arrow.RecordBatch) ([]col, []any) {
	t.Helper()
	cols, err := colsOf(batch.Schema())
	if err != nil {
		t.Fatal(err)
	}
	vals := make([]any, len(cols))
	for i := range cols {
		if vals[i], err = cols[i].at(batch.Column(i), 0); err != nil {
			t.Fatal(err)
		}
	}
	return cols, vals
}

// Each format's encoding decodes to the same JSON row, with the types the
// engine parses: decimals as numbers, timestamps as RFC 3339, bytes as
// base64.
func TestFormatsRoundTrip(t *testing.T) {
	fake := srfake.New()
	defer fake.Close()
	reg, err := sr.NewClient(sr.URLs(fake.URL()))
	if err != nil {
		t.Fatal(err)
	}
	batch := row(t)
	defer batch.Release()
	cols, vals := values(t, batch)
	want := map[string]any{
		"id": float64(7), "name": `a "quoted" <name>`, "price": 19.99,
		"at": "2026-11-02T10:30:00.123456Z", "local": "2026-11-02T10:30:00.123456", "day": "2026-11-02",
		"uid": "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "tags": []any{"x", "y"}, "ok": true,
		"score": "NaN", "raw": "AAEC", "missing": nil,
	}
	for _, format := range []string{formatJSON, formatAvro, formatProtobuf} {
		t.Run(format, func(t *testing.T) {
			enc, err := newRowEncoder(t.Context(), format, reg, "items_"+format+"-value", "items", cols)
			if err != nil {
				t.Fatal(err)
			}
			b, err := enc.encode(vals)
			if err != nil {
				t.Fatal(err)
			}
			out, err := newDecoder(format, reg).decode(t.Context(), b)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(out, &got); err != nil {
				t.Fatalf("%v: %s", err, out)
			}
			for k, v := range want {
				g := got[k]
				if format == formatProtobuf && k == "id" {
					// Protobuf's JSON spells 64-bit integers as strings.
					g = map[bool]any{true: float64(7)}[g == "7"]
				}
				if format == formatProtobuf && k == "price" {
					g = map[bool]any{true: 19.99}[g == "19.99"]
				}
				if format == formatProtobuf && k == "local" {
					// Protobuf's timestamps are all in UTC.
					v = "2026-11-02T10:30:00.123456Z"
				}
				if gj, wj := mustJSON(t, g), mustJSON(t, v); gj != wj {
					t.Errorf("%s = %s, want %s", k, gj, wj)
				}
			}
			// Protobuf carries JSON as text.
			wantDoc := `{"a":[1,2]}`
			if format == formatProtobuf {
				wantDoc = mustJSON(t, wantDoc)
			}
			if doc := mustJSON(t, got["doc"]); doc != wantDoc {
				t.Errorf("doc = %s, want %s", doc, wantDoc)
			}
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestProtoSchema(t *testing.T) {
	batch := row(t)
	defer batch.Release()
	cols, _ := values(t, batch)
	text, err := protoSchema("items", cols)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"optional int64 id = 1;", "google.protobuf.Timestamp at = 4;", "repeated string tags = 9;", "optional string missing = 13;"} {
		if !strings.Contains(text, want) {
			t.Errorf("schema lacks %q:\n%s", want, text)
		}
	}
}
