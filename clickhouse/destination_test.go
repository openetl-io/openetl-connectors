//go:build integration

package clickhouse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

var (
	tsUTC   = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	idField = arrow.Field{Name: idColumn, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: arrow.NewMetadata(
		[]string{extensionNameKey, "ARROW:extension:metadata"}, []string{uuidExtension, ""})}
	metaFields = []arrow.Field{
		idField,
		{Name: opColumn, Type: arrow.BinaryTypes.String},
		{Name: seqColumn, Type: tsUTC},
		{Name: syncedColumn, Type: tsUTC},
	}
)

// kit is a destination writing to a database of the test's own.
type kit struct {
	t  *testing.T
	db string
	d  *Destination
}

func newKit(t *testing.T) kit {
	db := newDatabase(t)
	d, err := NewDestination(testConfig(testAddr(), "openetl", db))
	if err != nil {
		t.Fatal(err)
	}
	return kit{t: t, db: db, d: d}
}

// open opens lane 0's writer as generation gen, and recovers e.
func (k kit) open(gen uint64, streams []destination.Stream, e destination.Epoch, staged []destination.Staged) (*writer, destination.Recovery) {
	k.t.Helper()
	w := k.writer(gen, streams)
	r, err := w.Recover(k.t.Context(), gen, e, staged)
	if err != nil {
		k.t.Fatalf("recover generation %d: %v", gen, err)
	}
	return w, r
}

// writer opens lane 0's writer as generation gen, without recovering.
func (k kit) writer(gen uint64, streams []destination.Stream) *writer {
	k.t.Helper()
	w, err := k.d.Open(k.t.Context(), destination.OpenConfig{Pipeline: "p", Generation: gen, Scope: destination.ScopeStream, Namespace: k.db, Streams: streams})
	if err != nil {
		k.t.Fatal(err)
	}
	k.t.Cleanup(func() { _ = w.Close(k.t.Context()) })
	return w.(*writer)
}

// exists reports whether the kit's database has table.
func (k kit) exists(table string) bool {
	k.t.Helper()
	return slices.Equal(k.query("SELECT toString(count()) FROM system.tables WHERE database = '"+k.db+"' AND name = '"+table+"'"), []string{"1"})
}

func (k kit) apply(w *writer, schemas ...*arrow.Schema) destination.SchemaApplied {
	k.t.Helper()
	var ss []destination.StreamSchema
	for _, s := range schemas {
		name, _ := s.Metadata().GetValue(tableKey)
		ss = append(ss, destination.StreamSchema{Name: name, Schema: s})
	}
	a, err := w.ApplySchema(k.t.Context(), ss)
	if err != nil {
		k.t.Fatalf("apply schema: %v", err)
	}
	return a
}

// stage writes batches in epoch e, and pre-commits it.
func (k kit) stage(w *writer, e destination.Epoch, batches ...arrow.RecordBatch) destination.Staged {
	k.t.Helper()
	for _, b := range batches {
		name, _ := b.Schema().Metadata().GetValue(tableKey)
		if err := w.Write(k.t.Context(), e, name, b); err != nil {
			k.t.Fatalf("write %s: %v", name, err)
		}
		b.Release()
	}
	st, err := w.PreCommit(k.t.Context(), e)
	if err != nil {
		k.t.Fatalf("pre-commit: %v", err)
	}
	return st
}

func (k kit) commit(w *writer, e destination.Epoch, batches ...arrow.RecordBatch) destination.Committed {
	k.t.Helper()
	st := k.stage(w, e, batches...)
	c, err := w.Commit(k.t.Context(), e, []destination.Staged{st})
	if err != nil {
		k.t.Fatalf("commit: %v", err)
	}
	return c
}

// rows reads a table as a reader should, each column as its text, ordered.
func (k kit) rows(table string, cols ...string) []string {
	k.t.Helper()
	conn := testConn(k.t)
	final := ""
	var engine string
	if err := conn.QueryRow(k.t.Context(), "SELECT engine FROM system.tables WHERE database = ? AND name = ?", k.db, table).Scan(&engine); err == nil && engine == "ReplacingMergeTree" {
		final = " FINAL"
	}
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = "ifNull(toString(" + ident(c) + "), 'NULL')"
	}
	rows, err := conn.Query(k.t.Context(), "SELECT concatWithSeparator('|', "+strings.Join(sel, ", ")+") AS r FROM "+qualified(k.db, table)+final+" ORDER BY r")
	if err != nil {
		k.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			k.t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func (k kit) query(q string) []string {
	k.t.Helper()
	rows, err := testConn(k.t).Query(k.t.Context(), strings.ReplaceAll(q, "$db", ident(k.db)))
	if err != nil {
		k.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			k.t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func schemaOf(table string, data ...arrow.Field) *arrow.Schema {
	md := arrow.NewMetadata([]string{tableKey}, []string{table})
	return arrow.NewSchema(append(slices.Clone(data), metaFields...), &md)
}

// row is a record's data values, its op and its seq, in microseconds.
type row struct {
	vals []any
	op   string
	seq  int64
}

// batchOf builds a batch of schema's rows. Values: nil, int64, int32,
// string, bool, []string for a list of strings.
func batchOf(schema *arrow.Schema, rows ...row) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	m := schema.NumFields() - len(metaFields)
	for _, r := range rows {
		for i, v := range r.vals {
			fb := b.Field(i)
			if v == nil {
				fb.AppendNull()
				continue
			}
			switch fb := fb.(type) {
			case *array.Int64Builder:
				fb.Append(v.(int64))
			case *array.Int32Builder:
				fb.Append(v.(int32))
			case *array.StringBuilder:
				fb.Append(v.(string))
			case *array.BooleanBuilder:
				fb.Append(v.(bool))
			case *array.ListBuilder:
				fb.Append(true)
				for _, s := range v.([]string) {
					fb.ValueBuilder().(*array.StringBuilder).Append(s)
				}
			default:
				panic(fmt.Sprintf("no value for %T", fb))
			}
		}
		h := sha256.Sum256(fmt.Appendf(nil, "%v %s %d", r.vals, r.op, r.seq))
		b.Field(m).(*array.FixedSizeBinaryBuilder).Append(h[:16])
		b.Field(m + 1).(*array.StringBuilder).Append(r.op)
		b.Field(m + 2).(*array.TimestampBuilder).Append(arrow.Timestamp(r.seq))
		b.Field(m + 3).(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UnixMicro()))
	}
	return b.NewRecordBatch()
}

func ins(seq int64, vals ...any) row { return row{vals: vals, op: "insert", seq: seq} }
func upd(seq int64, vals ...any) row { return row{vals: vals, op: "update", seq: seq} }
func del(seq int64, vals ...any) row { return row{vals: vals, op: "delete", seq: seq} }

var (
	idCol = arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}
	vCol  = arrow.Field{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true}
	wCol  = arrow.Field{Name: "w", Type: arrow.BinaryTypes.String, Nullable: true}
)

func epoch(gen, n uint64) destination.Epoch { return destination.Epoch{Generation: gen, N: n} }

func wantRows(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s:\n got %q\nwant %q", what, got, want)
	}
}

// Merge keeps each key's latest version by _openetl_seq, within an epoch
// and across them; a hard delete removes the row for readers using FINAL,
// and a soft delete keeps its other columns. A column an update didn't send
// keeps the target's value, or the epoch's latest that sent it.
func TestDestinationMerge(t *testing.T) { testDestinationMerge(t, newKit(t)) }

func testDestinationMerge(t *testing.T, k kit) {
	unchanged := arrow.Field{Name: unchangedColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
	deleted := arrow.Field{Name: deletedColumn, Type: arrow.FixedWidthTypes.Boolean}
	hard := schemaOf("hard", idCol, vCol, wCol, deleted)
	soft := schemaOf("soft", idCol, vCol, wCol, unchanged, deleted)
	streams := []destination.Stream{
		{Name: "hard", Mode: destination.Merge, PrimaryKey: []string{"id"}, Deletes: destination.HardDelete},
		{Name: "soft", Mode: destination.Merge, PrimaryKey: []string{"id"}, Deletes: destination.SoftDelete},
	}
	w, _ := k.open(1, streams, destination.Epoch{}, nil)
	k.apply(w, hard, soft)
	k.commit(w, epoch(1, 1),
		batchOf(hard, ins(10, int64(1), "a", "x", false), ins(10, int64(2), "b", "y", false), upd(20, int64(1), "a2", "x2", false), ins(10, int64(3), "c", "z", false)),
		batchOf(soft, ins(10, int64(1), "a", "x", nil, false), ins(10, int64(2), "b", "y", nil, false), upd(20, int64(2), "b2", nil, []string{"w"}, false)),
	)
	wantRows(t, "hard after epoch 1", k.rows("hard", "id", "v", "w"), "1|a2|x2", "2|b|y", "3|c|z")
	wantRows(t, "soft after epoch 1", k.rows("soft", "id", "v", "w", unchangedColumn), "1|a|x|[]", "2|b2|y|[]")
	k.commit(w, epoch(1, 2),
		// An older version than the target's loses.
		batchOf(hard, del(30, int64(2), nil, nil, true), upd(5, int64(3), "old", "old", false)),
		batchOf(soft, del(30, int64(1), nil, nil, nil, true), upd(30, int64(2), nil, "y3", []string{"v"}, false)),
	)
	wantRows(t, "hard after epoch 2", k.rows("hard", "id", "v", "w"), "1|a2|x2", "3|c|z")
	wantRows(t, "soft after epoch 2", k.rows("soft", "id", "v", "w", deletedColumn, opColumn), "1|a|x|true|delete", "2|b2|y3|false|update")
	// An insert after a hard delete brings the row back.
	k.commit(w, epoch(1, 3), batchOf(hard, ins(40, int64(2), "again", "w", false)))
	wantRows(t, "hard after epoch 3", k.rows("hard", "id", "v", "w"), "1|a2|x2", "2|again|w", "3|c|z")
}

// Dedupe skips the rows whose _openetl_id the epoch or the target has, and
// counts them collapsed.
func TestDestinationDedupe(t *testing.T) { testDestinationDedupe(t, newKit(t)) }

func testDestinationDedupe(t *testing.T, k kit) {
	s := schemaOf("a", idCol, vCol)
	streams := []destination.Stream{{Name: "a", Mode: destination.Append, Dedupe: destination.DedupeTarget}}
	w, _ := k.open(1, streams, destination.Epoch{}, nil)
	k.apply(w, s)
	c := k.commit(w, epoch(1, 1), batchOf(s, ins(1, int64(1), "a"), ins(1, int64(1), "a"), ins(1, int64(2), "b")))
	if c.Collapsed["a"] != 1 {
		t.Errorf("epoch 1 collapsed %v, not 1", c.Collapsed)
	}
	c = k.commit(w, epoch(1, 2), batchOf(s, ins(1, int64(2), "b"), ins(1, int64(3), "c")))
	if c.Collapsed["a"] != 1 {
		t.Errorf("epoch 2 collapsed %v, not 1", c.Collapsed)
	}
	wantRows(t, "a", k.rows("a", "id", "v"), "1|a", "2|b", "3|c")
}

// An overwrite's rows replace the target's only once its read ends, and an
// empty read empties it.
func TestDestinationOverwrite(t *testing.T) { testDestinationOverwrite(t, newKit(t)) }

func testDestinationOverwrite(t *testing.T, k kit) {
	s := schemaOf("o", idCol, vCol)
	streams := []destination.Stream{{Name: "o", Mode: destination.Overwrite}}
	w, _ := k.open(1, streams, destination.Epoch{}, nil)
	k.apply(w, s)
	start := time.Now()
	if err := w.StartReads(t.Context(), map[string]time.Time{"o": start}); err != nil {
		t.Fatal(err)
	}
	k.commit(w, epoch(1, 1), batchOf(s, ins(1, int64(1), "a")))
	if err := w.EndRead(t.Context(), epoch(1, 2), "o"); err != nil {
		t.Fatal(err)
	}
	k.commit(w, epoch(1, 2), batchOf(s, ins(1, int64(2), "b")))
	wantRows(t, "o after its first read", k.rows("o", "id", "v"), "1|a", "2|b")
	if err := w.StartReads(t.Context(), map[string]time.Time{"o": start.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	k.commit(w, epoch(1, 3), batchOf(s, ins(1, int64(3), "c")))
	wantRows(t, "o while its second read runs", k.rows("o", "id", "v"), "1|a", "2|b")
	if err := w.EndRead(t.Context(), epoch(1, 4), "o"); err != nil {
		t.Fatal(err)
	}
	k.commit(w, epoch(1, 4))
	wantRows(t, "o after its second read", k.rows("o", "id", "v"), "3|c")
	if err := w.StartReads(t.Context(), map[string]time.Time{"o": start.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := w.EndRead(t.Context(), epoch(1, 5), "o"); err != nil {
		t.Fatal(err)
	}
	k.commit(w, epoch(1, 5))
	wantRows(t, "o after an empty read", k.rows("o", "id", "v"))
	wantRows(t, "read tables left", k.query("SELECT name FROM system.tables WHERE database = '"+k.db+"' AND startsWith(name, '_openetl_read_')"))
}

// A commit cut off after any step of the apply is finished by the next
// generation's Recover, and lands its rows once.
func TestDestinationRecoverEachStep(t *testing.T) {
	for _, step := range []string{"staged", "built", "moved"} {
		t.Run(step, func(t *testing.T) {
			k := newKit(t)
			s := schemaOf("a", idCol, vCol)
			m := schemaOf("m", idCol, vCol)
			streams := []destination.Stream{
				{Name: "a", Mode: destination.Append},
				{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"id"}, Deletes: destination.HardDelete},
			}
			w, _ := k.open(1, streams, destination.Epoch{}, nil)
			k.apply(w, s, m)
			e := epoch(1, 1)
			st := k.stage(w, e, batchOf(s, ins(1, int64(1), "a"), ins(1, int64(2), "b")), batchOf(m, ins(1, int64(1), "a"), upd(2, int64(1), "b")))
			desc, err := w.mergeLanes([]destination.Staged{st})
			if err != nil {
				t.Fatal(err)
			}
			for _, ss := range desc.Streams {
				ready := readyName("p", ss.Name, e.Generation, e.N)
				if step != "staged" {
					if _, err := w.build(t.Context(), desc, ss, ready); err != nil {
						t.Fatal(err)
					}
				}
				if step == "moved" {
					if err := w.move(t.Context(), ready, ss.into()); err != nil {
						t.Fatal(err)
					}
				}
			}
			_, r := k.open(2, streams, e, []destination.Staged{st})
			if r != destination.Redone {
				t.Errorf("recovered %v, not redone", r)
			}
			wantRows(t, "a", k.rows("a", "id", "v"), "1|a", "2|b")
			wantRows(t, "m", k.rows("m", "id", "v"), "1|b")
			wantRows(t, "tables left", k.query("SELECT name FROM system.tables WHERE database = '"+k.db+"' AND startsWith(name, '_openetl_') AND name NOT IN ('_openetl_fence', '_openetl_loads') ORDER BY name"))
			// The stale generation's commit is fenced.
			if _, err := w.Commit(t.Context(), e, []destination.Staged{st}); !errorsIsFenced(err) {
				t.Errorf("generation 1's commit after generation 2 recovered: %v", err)
			}
		})
	}
}

func errorsIsFenced(err error) bool {
	return err != nil && strings.Contains(err.Error(), destination.ErrFenced.Error())
}

// Each Arrow type lands as the README's table says: a row of values, one
// of nulls, and one of extremes, which are clamped where the table says.
func TestDestinationTypes(t *testing.T) {
	k := newKit(t)
	dec, _ := decimal128.FromString("-12345678901234567890123456789.123456789", 38, 9)
	big76, _ := decimal256.FromString(strings.Repeat("9", 56)+"."+strings.Repeat("9", 20), 76, 20)
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "fb", Type: &arrow.FixedSizeBinaryType{ByteWidth: 3}, Nullable: true},
		{Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: 9}, Nullable: true},
		{Name: "d76", Type: &arrow.Decimal256Type{Precision: 76, Scale: 20}, Nullable: true},
		{Name: "day", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "ts", Type: tsUTC, Nullable: true},
		{Name: "ts_local", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: true},
		{Name: "ts_ns", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "tod", Type: arrow.FixedWidthTypes.Time64us, Nullable: true},
		{Name: "dur", Type: arrow.FixedWidthTypes.Duration_ms, Nullable: true},
		{Name: "iv", Type: arrow.FixedWidthTypes.MonthDayNanoInterval, Nullable: true},
		{Name: "list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		{Name: "map", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), Nullable: true},
		{Name: "st", Type: arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}, arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true}), Nullable: true},
		{Name: "dict", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}, Nullable: true},
		{Name: "u", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true, Metadata: arrow.NewMetadata([]string{extensionNameKey}, []string{uuidExtension})},
		{Name: "j", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.NewMetadata([]string{extensionNameKey}, []string{jsonExtension})},
		{Name: "ts_s", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
		{Name: "tod_s", Type: arrow.FixedWidthTypes.Time32s, Nullable: true},
	}
	schema := schemaOf("types", fields...)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	day := func(y int, m time.Month, d int) arrow.Date32 {
		return arrow.Date32FromTime(time.Date(y, m, d, 0, 0, 0, 0, time.UTC))
	}
	at := func(y int, unit arrow.TimeUnit) arrow.Timestamp {
		ts, _ := arrow.TimestampFromTime(time.Date(y, 1, 2, 3, 4, 5, 123456789, time.UTC), unit)
		return ts
	}
	for id := range 3 {
		b.Field(0).(*array.Int64Builder).Append(int64(id + 1))
		if id == 1 {
			for i := 1; i < len(fields); i++ {
				b.Field(i).AppendNull()
			}
		} else {
			first := id == 0
			b.Field(1).(*array.BooleanBuilder).Append(first)
			b.Field(2).(*array.Int8Builder).Append(map[bool]int8{true: 7, false: -128}[first])
			b.Field(3).(*array.Uint64Builder).Append(map[bool]uint64{true: 1, false: 18446744073709551615}[first])
			b.Field(4).(*array.Float32Builder).Append(map[bool]float32{true: 1.5, false: float32(math.Inf(-1))}[first])
			b.Field(5).(*array.Float64Builder).Append(map[bool]float64{true: 2.25, false: math.NaN()}[first])
			b.Field(6).(*array.StringBuilder).Append(map[bool]string{true: "naïve", false: ""}[first])
			b.Field(7).(*array.BinaryBuilder).Append(map[bool][]byte{true: {0, 255, 'a'}, false: {}}[first])
			b.Field(8).(*array.FixedSizeBinaryBuilder).Append([]byte("abc"))
			b.Field(9).(*array.Decimal128Builder).Append(map[bool]decimal128.Num{true: decimal128.FromI64(15), false: dec}[first])
			b.Field(10).(*array.Decimal256Builder).Append(map[bool]decimal256.Num{true: decimal256.FromI64(1), false: big76}[first])
			b.Field(11).(*array.Date32Builder).Append(map[bool]arrow.Date32{true: day(2026, 11, 3), false: day(9999, 12, 31)}[first])
			b.Field(12).(*array.TimestampBuilder).Append(map[bool]arrow.Timestamp{true: at(2026, arrow.Microsecond), false: at(1, arrow.Microsecond)}[first])
			b.Field(13).(*array.TimestampBuilder).Append(map[bool]arrow.Timestamp{true: at(2026, arrow.Millisecond), false: at(9999, arrow.Millisecond)}[first])
			b.Field(14).(*array.TimestampBuilder).Append(map[bool]arrow.Timestamp{true: at(2026, arrow.Nanosecond), false: arrow.Timestamp(math.MinInt64)}[first])
			b.Field(15).(*array.Time64Builder).Append(map[bool]arrow.Time64{true: arrow.Time64(time.Hour / time.Microsecond), false: arrow.Time64((24*time.Hour - time.Microsecond) / time.Microsecond)}[first])
			b.Field(16).(*array.DurationBuilder).Append(arrow.Duration(map[bool]int64{true: 1500, false: math.MinInt64}[first]))
			b.Field(17).(*array.MonthDayNanoIntervalBuilder).Append(arrow.MonthDayNanoInterval{Months: 1, Days: -2, Nanoseconds: 3})
			lb := b.Field(18).(*array.ListBuilder)
			lb.Append(true)
			if first {
				lb.ValueBuilder().(*array.Int64Builder).Append(1)
				lb.ValueBuilder().AppendNull()
			}
			mb := b.Field(19).(*array.MapBuilder)
			mb.Append(true)
			if first {
				mb.KeyBuilder().(*array.StringBuilder).Append("x")
				mb.ItemBuilder().(*array.Int32Builder).Append(1)
			}
			sb := b.Field(20).(*array.StructBuilder)
			sb.Append(true)
			sb.FieldBuilder(0).(*array.Int32Builder).Append(1)
			sb.FieldBuilder(1).AppendNull()
			if err := b.Field(21).(*array.BinaryDictionaryBuilder).AppendString(map[bool]string{true: "red", false: ""}[first]); err != nil {
				t.Fatal(err)
			}
			b.Field(22).(*array.FixedSizeBinaryBuilder).Append(map[bool][]byte{true: {0x61, 0xf0, 0xc4, 4, 0x5c, 0xb3, 0x11, 0xe7, 0x90, 0x7b, 0xa6, 0, 0x6a, 0xd3, 0xdb, 0xa0}, false: make([]byte, 16)}[first])
			b.Field(23).(*array.StringBuilder).Append(map[bool]string{true: `{"a":[1,null]}`, false: `"text"`}[first])
			b.Field(24).(*array.TimestampBuilder).Append(map[bool]arrow.Timestamp{true: at(2026, arrow.Second), false: at(9999, arrow.Second)}[first])
			b.Field(25).(*array.Time32Builder).Append(map[bool]arrow.Time32{true: 3661, false: 86399}[first])
		}
		m := len(fields)
		h := sha256.Sum256(fmt.Appendf(nil, "%d", id))
		b.Field(m).(*array.FixedSizeBinaryBuilder).Append(h[:16])
		b.Field(m + 1).(*array.StringBuilder).Append("insert")
		b.Field(m + 2).(*array.TimestampBuilder).Append(1)
		b.Field(m + 3).(*array.TimestampBuilder).Append(1)
	}
	w, _ := k.open(1, []destination.Stream{{Name: "types", Mode: destination.Append}}, destination.Epoch{}, nil)
	if a := k.apply(w, schema); len(a.Refused) > 0 {
		t.Fatalf("refused %v", a.Refused)
	}
	k.commit(w, epoch(1, 1), b.NewRecordBatch())
	// Applied again, the schema changes nothing.
	modified := "SELECT toString(metadata_modification_time) FROM system.tables WHERE database = '" + k.db + "' AND name = 'types'"
	before := k.query(modified)
	time.Sleep(1100 * time.Millisecond)
	if a := k.apply(w, schema); len(a.Refused) > 0 {
		t.Errorf("applied again, refused %v", a.Refused)
	}
	wantRows(t, "types' modification time after its schema again", k.query(modified), before...)

	want := map[string][]string{
		"b":        {"Nullable(Bool)", "true", `\N`, "false"},
		"i8":       {"Nullable(Int8)", "7", `\N`, "-128"},
		"u64":      {"Nullable(UInt64)", "1", `\N`, "18446744073709551615"},
		"f32":      {"Nullable(Float32)", "1.5", `\N`, "-inf"},
		"f64":      {"Nullable(Float64)", "2.25", `\N`, "nan"},
		"s":        {"Nullable(String)", "naïve", `\N`, ""},
		"bin":      {"Nullable(String)", "00FF61", `\N`, ""},
		"fb":       {"Nullable(FixedString(3))", "abc", `\N`, "abc"},
		"d":        {"Nullable(Decimal(38, 9))", "0.000000015", `\N`, "-12345678901234567890123456789.123456789"},
		"d76":      {"Nullable(Decimal(76, 20))", "0.00000000000000000001", `\N`, strings.Repeat("9", 56) + "." + strings.Repeat("9", 20)},
		"day":      {"Nullable(Date32)", "2026-11-03", `\N`, "2299-12-31"},
		"ts":       {"Nullable(DateTime64(6, 'UTC'))", "2026-01-02 03:04:05.123456", `\N`, "1900-01-01 00:00:00.000000"},
		"ts_local": {"Nullable(DateTime64(3, 'UTC'))", "2026-01-02 03:04:05.123", `\N`, "2262-04-11 23:47:16.854"},
		"ts_ns":    {"Nullable(DateTime64(9, 'UTC'))", "2026-01-02 03:04:05.123456789", `\N`, "1900-01-01 00:00:00.000000000"},
		"tod":      {"Nullable(Time64(6))", "01:00:00.000000", `\N`, "23:59:59.999999"},
		"dur":      {"Nullable(Int64)", "1500", `\N`, "-9223372036854775808"},
		"iv":       {intervalType, "(1,-2,3)", "(0,0,0)", "(1,-2,3)"},
		"list":     {"Array(Nullable(Int64))", "[1,NULL]", "[]", "[]"},
		"map":      {"Map(String, Nullable(Int32))", "{'x':1}", "{}", "{}"},
		"st":       {"Tuple(a Nullable(Int32), b Nullable(String))", "(1,NULL)", "(NULL,NULL)", "(1,NULL)"},
		"dict":     {"LowCardinality(Nullable(String))", "red", `\N`, ""},
		"u":        {"Nullable(UUID)", "61f0c404-5cb3-11e7-907b-a6006ad3dba0", `\N`, "00000000-0000-0000-0000-000000000000"},
		"j":        {"Nullable(String)", `{"a":[1,null]}`, `\N`, `"text"`},
		"ts_s":     {"Nullable(DateTime64(0, 'UTC'))", "2026-01-02 03:04:05", `\N`, "2262-04-11 23:47:16"},
		"tod_s":    {"Nullable(Time64(0))", "01:01:01", `\N`, "23:59:59"},
	}
	for _, f := range fields[1:] {
		sel := "toString(" + ident(f.Name) + ")"
		if f.Name == "bin" {
			sel = "hex(" + ident(f.Name) + ")"
		}
		got := k.query("SELECT toTypeName(" + ident(f.Name) + ") FROM $db.types LIMIT 1")
		got[0] = canonical(got[0])
		got = append(got, k.query("SELECT ifNull("+sel+", '\\\\N') FROM $db.types ORDER BY id")...)
		if !slices.Equal(got, want[f.Name]) {
			t.Errorf("%s: got %q, want %q", f.Name, got, want[f.Name])
		}
	}
}

// ApplySchema is idempotent; it adds new columns, refuses a type change
// the stream doesn't allow with the statement that makes it, refuses one
// it can't make, and leaves a column its user made NOT NULL alone.
func TestDestinationSchemaChanges(t *testing.T) {
	k := newKit(t)
	n32 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	n64 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	nStr := arrow.Field{Name: "n", Type: arrow.BinaryTypes.String, Nullable: true}
	key32 := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32}
	streams := []destination.Stream{
		{Name: "a", Mode: destination.Append},
		{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"id"}, AllowTypeChanges: true},
	}
	w, _ := k.open(1, streams, destination.Epoch{}, nil)
	k.apply(w, schemaOf("a", idCol, vCol, n32), schemaOf("m", key32, vCol))
	modified := func() []string {
		return k.query("SELECT concat(name, ' ', toString(metadata_modification_time)) FROM system.tables WHERE database = '" + k.db + "' ORDER BY name")
	}
	before := modified()
	time.Sleep(1100 * time.Millisecond)
	if a := k.apply(w, schemaOf("a", idCol, vCol, n32), schemaOf("m", key32, vCol)); len(a.Refused) > 0 {
		t.Errorf("the same schemas again: refused %v", a.Refused)
	}
	wantRows(t, "tables after the same schemas again", modified(), before...)

	a := k.apply(w, schemaOf("a", idCol, vCol, n64, wCol), schemaOf("m", idCol, vCol))
	if len(a.Refused) != 2 {
		t.Fatalf("refused %+v, not a's n and m's id", a.Refused)
	}
	for _, r := range a.Refused {
		switch r.Stream {
		case "a":
			if r.Column != "n" || r.FromType != "Nullable(Int32)" || r.ToType != "Nullable(Int64)" || r.Fix != "ALTER TABLE "+qualified(k.db, "a")+" MODIFY COLUMN `n` Nullable(Int64)" {
				t.Errorf("a's refusal: %+v", r)
			}
		case "m":
			if r.Column != "id" || r.Fix != "" || !strings.Contains(r.Reason, "key") {
				t.Errorf("m's refusal: %+v", r)
			}
		}
	}
	wantRows(t, "a's columns", k.query("SELECT concat(name, ' ', type) FROM system.columns WHERE database = '"+k.db+"' AND table = 'a' AND name IN ('n', 'w') ORDER BY name"),
		"n Nullable(Int32)", "w Nullable(String)")
	if a := k.apply(w, schemaOf("a", idCol, vCol, nStr, wCol)); len(a.Refused) != 1 || a.Refused[0].Fix == "" {
		t.Errorf("Int32 to String, not allowed: refused %+v", a.Refused)
	}

	// The user makes v NOT NULL.
	if err := testConn(t).Exec(t.Context(), "ALTER TABLE "+qualified(k.db, "a")+" MODIFY COLUMN v String DEFAULT ''"); err != nil {
		t.Fatal(err)
	}
	a = k.apply(w, schemaOf("a", idCol, vCol, n32, wCol))
	if len(a.Refused) > 0 || !slices.Contains(a.Limits["a"].NotNull, "v") {
		t.Errorf("after v was made NOT NULL: %+v", a)
	}
	wantRows(t, "v's type", k.query("SELECT type FROM system.columns WHERE database = '"+k.db+"' AND table = 'a' AND name = 'v'"), "String")
	k.commit(w, epoch(1, 1), batchOf(schemaOf("a", idCol, vCol, n32, wCol), ins(1, int64(1), "x", int32(2), "y")))
	wantRows(t, "a", k.rows("a", "id", "v", "n", "w"), "1|x|2|y")
	// The destination takes no holds: a null for v fails the apply, and
	// lands nothing.
	st := k.stage(w, epoch(1, 2), batchOf(schemaOf("a", idCol, vCol, n32, wCol), ins(1, int64(2), nil, int32(2), "y")))
	if _, err := w.Commit(t.Context(), epoch(1, 2), []destination.Staged{st}); err == nil {
		t.Error("a null for a column its user made NOT NULL was applied")
	}
	wantRows(t, "a after a null for v", k.rows("a", "id", "v", "n", "w"), "1|x|2|y")
}

// A merge stream whose seq is a Decimal, as Postgres's CDC's is, is
// versioned by it all the same: an older version loses, and a hard delete
// removes the row.
func TestDestinationMergeDecimalSeq(t *testing.T) {
	k := newKit(t)
	seq := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	md := arrow.NewMetadata([]string{tableKey}, []string{"m"})
	schema := arrow.NewSchema([]arrow.Field{idCol, vCol, idField, {Name: opColumn, Type: arrow.BinaryTypes.String}, {Name: seqColumn, Type: seq}}, &md)
	batch := func(rows ...row) arrow.RecordBatch {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		for i, r := range rows {
			b.Field(0).(*array.Int64Builder).Append(r.vals[0].(int64))
			if r.vals[1] == nil {
				b.Field(1).AppendNull()
			} else {
				b.Field(1).(*array.StringBuilder).Append(r.vals[1].(string))
			}
			h := sha256.Sum256(fmt.Appendf(nil, "%d %d", r.seq, i))
			b.Field(2).(*array.FixedSizeBinaryBuilder).Append(h[:16])
			b.Field(3).(*array.StringBuilder).Append(r.op)
			b.Field(4).(*array.Decimal128Builder).Append(decimal128.FromI64(r.seq))
		}
		return b.NewRecordBatch()
	}
	streams := []destination.Stream{{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"id"}, Deletes: destination.HardDelete}}
	w, _ := k.open(1, streams, destination.Epoch{}, nil)
	k.apply(w, schema)
	k.commit(w, epoch(1, 1), batch(ins(100, int64(1), "a"), ins(100, int64(2), "b"), ins(100, int64(3), "c")))
	k.commit(w, epoch(1, 2), batch(upd(50, int64(1), "older"), upd(200, int64(2), "newer"), del(200, int64(3), nil)))
	wantRows(t, "m", k.rows("m", "id", "v"), "1|a", "2|newer")
	wantRows(t, "m's engine", k.query("SELECT engine_full FROM system.tables WHERE database = '"+k.db+"' AND name = 'm'"),
		"ReplacingMergeTree(_openetl_version, _openetl_is_deleted) ORDER BY (id) SETTINGS index_granularity = 8192")
}

// rowID is the _openetl_id batchOf gives r.
func rowID(r row) []byte {
	h := sha256.Sum256(fmt.Appendf(nil, "%v %s %d", r.vals, r.op, r.seq))
	return h[:16]
}

// heldHex is how a target's _openetl_held names r's version.
func heldHex(r row) string { return fmt.Sprintf("%X", rowID(r)) }

var heldCol = arrow.Field{Name: heldColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}

// patchBatch is a batch of patches of table: each fills the cells held
// lists, in the row of the key id, from the version of r. fields are the
// columns they fill, and vals their values.
func patchBatch(t *testing.T, table string, fields []arrow.Field, patches ...patchRow) arrow.RecordBatch {
	t.Helper()
	md := arrow.NewMetadata([]string{tableKey}, []string{table})
	schema := arrow.NewSchema(append([]arrow.Field{idCol, idField, {Name: seqColumn, Type: tsUTC}, {Name: syncedColumn, Type: tsUTC}, heldCol}, fields...), &md)
	var rows []map[string]any
	for _, p := range patches {
		r := map[string]any{"id": p.version.vals[0], idColumn: rowID(p.version), seqColumn: 1, syncedColumn: 1, heldColumn: p.held}
		for i, f := range fields {
			r[f.Name] = p.vals[i]
		}
		rows = append(rows, r)
	}
	j, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(j))
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

type patchRow struct {
	version row
	held    []string
	vals    []any
}

// commitPatches commits epoch e of patches, and of rows, in w.
func (k kit) commitPatches(w *writer, e destination.Epoch, stream string, patches arrow.RecordBatch, rows ...arrow.RecordBatch) error {
	k.t.Helper()
	if err := w.Patch(k.t.Context(), e, stream, patches); err != nil {
		k.t.Fatal(err)
	}
	patches.Release()
	for _, b := range rows {
		if err := w.Write(k.t.Context(), e, stream, b); err != nil {
			k.t.Fatal(err)
		}
		b.Release()
	}
	st, err := w.PreCommit(k.t.Context(), e)
	if err != nil {
		k.t.Fatal(err)
	}
	_, err = w.Commit(k.t.Context(), e, []destination.Staged{st})
	return err
}

func (k kit) tablesLeft() []string {
	return k.query("SELECT name FROM system.tables WHERE database = '" + k.db + "' AND startsWith(name, '_openetl_') AND name NOT IN ('_openetl_fence', '_openetl_loads') ORDER BY name")
}

// Holds by ID: an append stream's held cells are filled by patches from
// the versions that hold them, across patch rows, and a patch column whose
// FillsKey names another fills its own cell for the other's hold. A patch
// from a version that holds nothing, or for an ID the table doesn't have,
// changes nothing; a patch whose commit is cut off after it filled is redone
// without filling again; and rows land after patches as before.
func TestDestinationPatchByID(t *testing.T) {
	k := newKit(t)
	w2Col := arrow.Field{Name: "w__v2", Type: arrow.BinaryTypes.String, Nullable: true}
	s := schemaOf("a", idCol, vCol, wCol, w2Col, heldCol)
	streams := []destination.Stream{{Name: "a", Mode: destination.Append}}
	w, _ := k.open(1, streams, destination.Epoch{}, nil)
	a := k.apply(w, s)
	if l := a.Limits["a"]; len(l.NotNull) > 0 || len(l.Unholdable) > 0 {
		t.Errorf("limits %+v", l)
	}
	r1, r2 := ins(1, int64(1), nil, nil, nil, []string{"v", "w"}), ins(1, int64(2), "b", "x", nil, nil)
	k.commit(w, epoch(1, 1), batchOf(s, r1, r2))
	cols := []string{"id", "v", "w", "w__v2", heldColumn}
	wantRows(t, "a with held cells", k.rows("a", cols...), "1|NULL|NULL|NULL|{'v':'"+heldHex(r1)+"','w':'"+heldHex(r1)+"'}", "2|b|x|NULL|{}")

	e := epoch(1, 2)
	fills := w2Col
	fills.Metadata = arrow.NewMetadata([]string{destination.FillsKey}, []string{"w"})
	pf := []arrow.Field{vCol, fills}
	if err := w.Patch(t.Context(), e, "a", patchBatch(t, "a", pf, patchRow{r1, []string{"v"}, []any{"pv", nil}}, patchRow{r1, []string{"w"}, []any{nil, "pw"}},
		patchRow{r2, []string{"v"}, []any{"nope", nil}}, patchRow{ins(1, int64(9), nil), []string{"v"}, []any{"p9", nil}})); err != nil {
		t.Fatal(err)
	}
	st, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	// Cut off after the patch filled, before the marker.
	desc, err := w.mergeLanes([]destination.Staged{st})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.patch(t.Context(), desc, desc.Streams[0]); err != nil {
		t.Fatal(err)
	}
	w2, r := k.open(2, streams, e, []destination.Staged{st})
	if r != destination.Redone {
		t.Errorf("recovered %v, not redone", r)
	}
	want := []string{"1|pv|NULL|pw|{}", "2|b|x|NULL|{}"}
	wantRows(t, "a after patches", k.rows("a", cols...), want...)
	wantRows(t, "synced at, from the patch where it filled", k.rows("a", "id", syncedColumn)[:1], "1|1970-01-01 00:00:00.000001")
	wantRows(t, "tables left", k.tablesLeft())

	r3 := ins(3, int64(3), nil, "c", nil, []string{"v"})
	if err := k.commitPatches(w2, epoch(2, 3), "a", patchBatch(t, "a", pf, patchRow{r1, []string{"v"}, []any{"again", nil}}), batchOf(s, r3)); err != nil {
		t.Fatal(err)
	}
	want = append(want, "3|NULL|c|NULL|{'v':'"+heldHex(r3)+"'}")
	wantRows(t, "a after a repeated patch and a row", k.rows("a", cols...), want...)
	if err := k.commitPatches(w2, epoch(2, 4), "a", patchBatch(t, "a", pf, patchRow{r3, []string{"v"}, []any{"p3", nil}})); err != nil {
		t.Fatal(err)
	}
	want[2] = "3|p3|c|NULL|{}"
	wantRows(t, "a after the row's patch", k.rows("a", cols...), want...)
	wantRows(t, "mutations not done", k.query("SELECT toString(count()) FROM system.mutations WHERE database = '"+k.db+"' AND NOT is_done"), "0")
}

// Holds by key: a merge stream's patch fills the version the table holds,
// and a newer version that lands after it replaces the patched row.
func TestDestinationPatchMerge(t *testing.T) { testDestinationPatchMerge(t, newKit(t)) }

func testDestinationPatchMerge(t *testing.T, k kit) {
	s := schemaOf("m", idCol, vCol, wCol, heldCol)
	streams := []destination.Stream{{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"id"}}}
	w, _ := k.open(1, streams, destination.Epoch{}, nil)
	if l := k.apply(w, s).Limits["m"]; !slices.Equal(l.Unholdable, []string{"id"}) {
		t.Errorf("limits %+v", l)
	}
	r1 := ins(1, int64(1), nil, "w1", []string{"v"})
	k.commit(w, epoch(1, 1), batchOf(s, r1, ins(1, int64(2), "b", "w2", nil)))
	if err := k.commitPatches(w, epoch(1, 2), "m", patchBatch(t, "m", []arrow.Field{vCol}, patchRow{r1, []string{"v"}, []any{"p1"}})); err != nil {
		t.Fatal(err)
	}
	wantRows(t, "m after the patch", k.rows("m", "id", "v", "w", heldColumn), "1|p1|w1|{}", "2|b|w2|{}")
	r1b := upd(2, int64(1), nil, "w1b", []string{"v"})
	k.commit(w, epoch(1, 3), batchOf(s, r1b, upd(2, int64(2), "b2", "w2b", nil)))
	wantRows(t, "m after newer versions", k.rows("m", "id", "v", "w", heldColumn), "1|NULL|w1b|{'v':'"+heldHex(r1b)+"'}", "2|b2|w2b|{}")
	if err := k.commitPatches(w, epoch(1, 4), "m", patchBatch(t, "m", []arrow.Field{vCol}, patchRow{r1, []string{"v"}, []any{"old"}}, patchRow{r1b, []string{"v"}, []any{"p1b"}})); err != nil {
		t.Fatal(err)
	}
	wantRows(t, "m after the newer version's patch", k.rows("m", "id", "v", "w", heldColumn), "1|p1b|w1b|{}", "2|b2|w2b|{}")
}

// A column held while the destination refuses its type change arrives at
// the table's type; once the user has run the refusal's fix, patches at
// the new type fill it, or fill col__v2 for col's hold. A patch whose type
// the column doesn't hold fails, and fills nothing.
func TestDestinationHeldTypeChange(t *testing.T) {
	k := newKit(t)
	n32 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	n64 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	n2 := arrow.Field{Name: "n__v2", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	streams := []destination.Stream{{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"id"}}}
	w, _ := k.open(1, streams, destination.Epoch{}, nil)
	k.apply(w, schemaOf("m", idCol, n32, heldCol))
	a := k.apply(w, schemaOf("m", idCol, n64, heldCol))
	if len(a.Refused) != 1 || a.Refused[0].Fix == "" {
		t.Fatalf("refused %+v", a.Refused)
	}
	held := schemaOf("m", idCol, n32, heldCol)
	r1, r2 := ins(1, int64(1), nil, []string{"n"}), ins(1, int64(2), nil, []string{"n"})
	k.commit(w, epoch(1, 1), batchOf(held, r1, r2))
	// A patch at the new type before the fix: the column doesn't hold it.
	err := k.commitPatches(w, epoch(1, 2), "m", patchBatch(t, "m", []arrow.Field{n64}, patchRow{r1, []string{"n"}, []any{int64(1) << 40}}))
	if err == nil || !strings.Contains(err.Error(), "doesn't hold") {
		t.Errorf("a patch wider than its column: %v", err)
	}
	wantRows(t, "m after the refused patch", k.rows("m", "id", "n"), "1|NULL", "2|NULL")

	if err := testConn(t).Exec(t.Context(), a.Refused[0].Fix); err != nil {
		t.Fatal(err)
	}
	w2, _ := k.open(2, streams, destination.Epoch{}, nil)
	if a := k.apply(w2, schemaOf("m", idCol, n64, n2, heldCol)); len(a.Refused) > 0 {
		t.Fatalf("after the fix, refused %+v", a.Refused)
	}
	n2fills := n2
	n2fills.Metadata = arrow.NewMetadata([]string{destination.FillsKey}, []string{"n"})
	if err := k.commitPatches(w2, epoch(2, 3), "m", patchBatch(t, "m", []arrow.Field{n64, n2fills},
		patchRow{r1, []string{"n"}, []any{int64(1) << 40, nil}}, patchRow{r2, []string{"n"}, []any{nil, int64(7)}})); err != nil {
		t.Fatal(err)
	}
	wantRows(t, "m after patches at the new type", k.rows("m", "id", "n", "n__v2", heldColumn), "1|1099511627776|NULL|{}", "2|NULL|7|{}")
}

// A failed patch kills its own mutation, which names its Join table, and
// leaves the table's others, such as a user's, which aren't the
// destination's to kill.
func TestKillPatch(t *testing.T) {
	k := newKit(t)
	conn := testConn(t)
	if err := conn.Exec(t.Context(), "CREATE TABLE "+qualified(k.db, "t")+" (id Int64, v String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(t.Context(), "INSERT INTO "+qualified(k.db, "t")+" VALUES (1, 'a')"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "KILL MUTATION WHERE database = ?", k.db) })
	// Mutations that fail on their first row, and stay until they're killed.
	async := ch.Context(t.Context(), ch.WithSettings(ch.Settings{"mutations_sync": 0}))
	for _, name := range []string{"_openetl_stage_mine", "_openetl_stage_theirs"} {
		if err := conn.Exec(async, "ALTER TABLE "+qualified(k.db, "t")+" UPDATE v = if(throwIf(id > 0, "+literal(name)+") = 0, v, v) WHERE 1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Exec(t.Context(), killPatch, k.db, "t", "_openetl_stage_mine"); err != nil {
		t.Fatal(err)
	}
	left := func() []string {
		return k.query("SELECT command FROM system.mutations WHERE database = '" + k.db + "' AND table = 't' AND NOT is_done ORDER BY create_time")
	}
	for range 50 {
		if got := left(); len(got) == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := left(); len(got) != 1 || !strings.Contains(got[0], "_openetl_stage_theirs") {
		t.Errorf("mutations left: %q, not only theirs", got)
	}
}

// A table its user partitioned finely takes an epoch that spans more of
// its partitions than one insert block may by default, 100: the epoch's
// rows are built in a table of the destination's own, whose partitions
// then move one at a time.
func TestDestinationManyPartitions(t *testing.T) {
	k := newKit(t)
	if err := testConn(t).Exec(t.Context(), "CREATE TABLE "+qualified(k.db, "p")+" (id Int64, v Nullable(String)) ENGINE = MergeTree PARTITION BY id ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	s := schemaOf("p", idCol, vCol)
	w, _ := k.open(1, []destination.Stream{{Name: "p", Mode: destination.Append}}, destination.Epoch{}, nil)
	k.apply(w, s)
	var rows []row
	for i := range 150 {
		rows = append(rows, ins(1, int64(i), "x"))
	}
	k.commit(w, epoch(1, 1), batchOf(s, rows...))
	wantRows(t, "p's rows and partitions", k.query("SELECT concat(toString(count()), ' ', toString(uniqExact(_partition_id))) FROM $db.p"), "150 150")
}

// An overwrite's replace that comes after another applier finished the
// epoch, as one paused past its checks would, leaves the table as that
// applier made it: the read's table is gone because the epoch is applied,
// not because the read was empty.
func TestDestinationReplaceAfterApplied(t *testing.T) {
	k := newKit(t)
	s := schemaOf("o", idCol, vCol)
	w, _ := k.open(1, []destination.Stream{{Name: "o", Mode: destination.Overwrite}}, destination.Epoch{}, nil)
	k.apply(w, s)
	if err := w.StartReads(t.Context(), map[string]time.Time{"o": time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := w.EndRead(t.Context(), epoch(1, 1), "o"); err != nil {
		t.Fatal(err)
	}
	st := k.stage(w, epoch(1, 1), batchOf(s, ins(1, int64(1), "a")))
	if _, err := w.Commit(t.Context(), epoch(1, 1), []destination.Staged{st}); err != nil {
		t.Fatal(err)
	}
	wantRows(t, "o", k.rows("o", "id", "v"), "1|a")
	desc, err := w.mergeLanes([]destination.Staged{st})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.replace(t.Context(), desc, desc.Streams[0]); err != nil {
		t.Fatal(err)
	}
	wantRows(t, "o after a late replace", k.rows("o", "id", "v"), "1|a")
}

// A replaced attempt that drops its stream's abandoned reads after a newer
// generation started a read of its own, as one paused past its checks
// would, leaves the newer read's table, which holds that read's epochs.
func TestDestinationStaleDropsNoNewerRead(t *testing.T) {
	k := newKit(t)
	s := schemaOf("o", idCol, vCol)
	streams := []destination.Stream{{Name: "o", Mode: destination.Overwrite}}
	w1, _ := k.open(1, streams, destination.Epoch{}, nil)
	k.apply(w1, s)
	start := time.Now()
	if err := w1.StartReads(t.Context(), map[string]time.Time{"o": start}); err != nil {
		t.Fatal(err)
	}
	if err := w1.EndRead(t.Context(), epoch(1, 1), "o"); err != nil {
		t.Fatal(err)
	}
	st := k.stage(w1, epoch(1, 1), batchOf(s, ins(1, int64(1), "a")))
	w2, r := k.open(2, streams, epoch(1, 1), []destination.Staged{st})
	if r != destination.Redone {
		t.Fatalf("recovered %v, not redone", r)
	}
	k.apply(w2, s)
	if err := w2.StartReads(t.Context(), map[string]time.Time{"o": start.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	k.commit(w2, epoch(2, 2), batchOf(s, ins(1, int64(2), "b")))
	desc, err := w1.mergeLanes([]destination.Staged{st})
	if err != nil {
		t.Fatal(err)
	}
	if err := w1.dropAbandonedReads(t.Context(), w1.abandoned(desc.Streams[0])); err != nil {
		t.Fatal(err)
	}
	if err := w2.EndRead(t.Context(), epoch(2, 3), "o"); err != nil {
		t.Fatal(err)
	}
	k.commit(w2, epoch(2, 3))
	wantRows(t, "o after generation 2's read", k.rows("o", "id", "v"), "2|b")
}

// A lane keeps at most maxPendingBytes of batches in all, whatever its
// number of streams: past it, it inserts its largest stream's.
func TestDestinationPendingBound(t *testing.T) {
	k := newKit(t)
	var streams []destination.Stream
	var schemas []*arrow.Schema
	for i := range 9 {
		streams = append(streams, destination.Stream{Name: fmt.Sprintf("s%d", i), Mode: destination.Append})
		schemas = append(schemas, schemaOf(fmt.Sprintf("s%d", i), idCol, vCol))
	}
	w, _ := k.open(1, streams, destination.Epoch{}, nil)
	k.apply(w, schemas...)
	big := strings.Repeat("x", 1<<20)
	var batches []arrow.RecordBatch
	for _, s := range schemas {
		var rows []row
		for i := range 30 {
			rows = append(rows, ins(int64(i), int64(i), big))
		}
		batches = append(batches, batchOf(s, rows...))
	}
	for i, b := range batches {
		if err := w.Write(t.Context(), epoch(1, 1), streams[i].Name, b); err != nil {
			t.Fatal(err)
		}
		b.Release()
	}
	var pending int64
	for _, ls := range w.staging[1] {
		pending += ls.pendingBytes
	}
	if pending > maxPendingBytes {
		t.Errorf("the lane keeps %d bytes of 9 streams' batches", pending)
	}
	st, err := w.PreCommit(t.Context(), epoch(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), epoch(1, 1), []destination.Staged{st}); err != nil {
		t.Fatal(err)
	}
	for _, s := range streams {
		wantRows(t, s.Name, k.query("SELECT toString(count()) FROM $db."+ident(s.Name)), "30")
	}
}

// Generation 0's fence is a fence like any other: once its Recover has
// raised it, its commits apply. A pipeline whose fence has no row is told
// to recover first.
func TestDestinationFenceOfGenerationZero(t *testing.T) {
	k := newKit(t)
	s := schemaOf("a", idCol, vCol)
	streams := []destination.Stream{{Name: "a", Mode: destination.Append}}
	w, _ := k.open(0, streams, destination.Epoch{}, nil)
	k.apply(w, s)
	k.commit(w, epoch(0, 1), batchOf(s, ins(1, int64(1), "a")))
	wantRows(t, "a", k.rows("a", "id", "v"), "1|a")
	other, err := k.d.Open(t.Context(), destination.OpenConfig{Pipeline: "q", Generation: 1, Scope: destination.ScopeStream, Namespace: k.db, Streams: streams})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close(t.Context()) })
	if err := other.(*writer).checkFence(t.Context()); err == nil || !strings.Contains(err.Error(), "Recover must run first") {
		t.Errorf("a pipeline whose fence has no row: %v", err)
	}
}

// A generation that no longer overwrites a stream drops the stream's read
// tables when it recovers, but none once a newer generation has raised the
// fence, which may overwrite the stream and be writing the read, and none a
// newer generation made. Each generation here drops them as its Recover
// would once paused past its own raise of the fence.
func TestDestinationFencedKeepsNewerReads(t *testing.T) {
	k := newKit(t)
	s := schemaOf("o", idCol, vCol)
	overwrite := []destination.Stream{{Name: "o", Mode: destination.Overwrite}}
	appends := []destination.Stream{{Name: "o", Mode: destination.Append}}
	start := time.Now()
	w1, _ := k.open(1, overwrite, destination.Epoch{}, nil)
	k.apply(w1, s)
	if err := w1.StartReads(t.Context(), map[string]time.Time{"o": start}); err != nil {
		t.Fatal(err)
	}
	k.commit(w1, epoch(1, 1), batchOf(s, ins(1, int64(1), "a")))
	// Generation 2 goes on with the read, whose table keeps generation 1's
	// label.
	w2, _ := k.open(2, overwrite, destination.Epoch{}, nil)
	k.apply(w2, s)
	if err := w2.StartReads(t.Context(), map[string]time.Time{"o": start}); err != nil {
		t.Fatal(err)
	}
	k.commit(w2, epoch(2, 2), batchOf(s, ins(1, int64(2), "b")))
	read := readName("p", "o", start)
	k.writer(1, appends).dropRemovedReads(t.Context(), 1)
	if !k.exists(read) {
		t.Fatal("a generation behind the fence dropped the read a newer one goes on with")
	}
	// Generation 3 makes a read of its own, as it would once it raised the
	// fence after generation 2's drop read it.
	newer := readName("p", "o", start.Add(time.Second))
	if err := k.writer(3, overwrite).ensureRead(t.Context(), stagedStream{Name: "o", Table: "o", Read: newer}); err != nil {
		t.Fatal(err)
	}
	k.writer(2, appends).dropRemovedReads(t.Context(), 2)
	if !k.exists(newer) {
		t.Error("the fence's generation dropped a newer generation's read")
	}
	if k.exists(read) {
		t.Error("the fence's generation, which appends, kept the stream's read")
	}
}

// raceConn runs race once, before the first query of system.tables, which
// Recover makes once it has read the epoch's markers.
type raceConn struct {
	driver.Conn
	race func()
}

func (c *raceConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if race := c.race; race != nil && strings.HasPrefix(query, "SELECT count() FROM system.tables") {
		c.race = nil
		race()
	}
	return c.Conn.QueryRow(ctx, query, args...)
}

// A Recover that finds an epoch's markers missing and its staging gone
// reports it lost, unless a newer generation has raised the fence since,
// and may have applied the epoch and dropped its staging: then it's fenced.
func TestDestinationRecoverLostOnlyBehindItsOwnFence(t *testing.T) {
	for _, newer := range []bool{false, true} {
		t.Run(fmt.Sprint("newer=", newer), func(t *testing.T) {
			k := newKit(t)
			s := schemaOf("a", idCol, vCol)
			streams := []destination.Stream{{Name: "a", Mode: destination.Append}}
			w1, _ := k.open(1, streams, destination.Epoch{}, nil)
			k.apply(w1, s)
			e := epoch(1, 1)
			st := k.stage(w1, e, batchOf(s, ins(1, int64(1), "a")))
			w2 := k.writer(2, streams)
			race := func() {
				w1.drop(t.Context(), k.tablesLeft()...)
			}
			if newer {
				race = func() {
					if _, r := k.open(3, streams, e, []destination.Staged{st}); r != destination.Redone {
						t.Errorf("generation 3 recovered %v, not redone", r)
					}
				}
			}
			w2.conn = &raceConn{Conn: w2.conn, race: race}
			r, err := w2.Recover(t.Context(), 2, e, []destination.Staged{st})
			switch {
			case !newer && (err != nil || r != destination.Lost):
				t.Errorf("with its staging gone, recovered %v, %v; want lost", r, err)
			case newer && !errorsIsFenced(err):
				t.Errorf("after generation 3 applied the epoch meanwhile, recovered %v, %v; want fenced", r, err)
			}
		})
	}
}
