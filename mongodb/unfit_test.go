package mongodb

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A field declared as fieldType declares it takes a value's type exactly
// where discovery, having seen that type too, still declares the field so:
// for every set of up to four types, but a set of nulls alone, which
// declares text, and text takes any type.
func TestTakes(t *testing.T) {
	var names []string
	for _, k := range typeNames {
		names = append(names, k.name)
	}
	check := func(seen []string) {
		declared := fieldType(seen)
		got := takes(declared)
		nulls := !slices.ContainsFunc(seen, func(n string) bool { return n != "null" && n != "undefined" })
		for j, n := range names {
			want := nulls || arrow.TypeEqual(fieldType(append(slices.Clip(seen), n)), declared)
			if got&(1<<j) != 0 != want {
				t.Errorf("declared %s from %v: takes %s is %v, want %v", declared, seen, n, !want, want)
			}
		}
	}
	var sets func(from int, seen []string)
	sets = func(from int, seen []string) {
		check(seen)
		if len(seen) == 4 {
			return
		}
		for i := from; i < len(names); i++ {
			sets(i+1, append(slices.Clip(seen), names[i]))
		}
	}
	sets(0, nil)

	// The types as a read request carries them.
	for _, n := range names {
		declared := fieldType([]string{n})
		b, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{{Name: "f", Type: declared, Nullable: true}}, nil))
		if err != nil {
			t.Fatal(err)
		}
		s, err := arrowipc.DecodeSchema(b)
		if err != nil {
			t.Fatal(err)
		}
		if got := takes(s.Field(0).Type); got != takes(declared) {
			t.Errorf("%s through IPC, %s, takes %b, want %b", declared, s.Field(0).Type, got, takes(declared))
		}
	}
	if !arrow.TypeEqual(fieldType([]string{"decimal"}), decimalType) || takes(&arrow.Decimal128Type{Precision: 20, Scale: 2}) != takes(decimalType) {
		t.Errorf("decimal(20, 2) takes %b, want decimal(38, 9)'s %b", takes(&arrow.Decimal128Type{Precision: 20, Scale: 2}), takes(decimalType))
	}
	if takes(arrow.PrimitiveTypes.Int32) != allKinds {
		t.Errorf("int32, which discovery doesn't declare, takes %b", takes(arrow.PrimitiveTypes.Int32))
	}
}

// Each value's kind is its BSON type, as $type names it, and a UUID's is
// uuid, as discovery names it.
func TestKindOf(t *testing.T) {
	oid, _ := bson.ObjectIDFromHex("65f0a1b2c3d4e5f601234567")
	dec, _ := bson.ParseDecimal128("1.5")
	for _, c := range []struct {
		v    any
		want string
	}{
		{1.5, "double"}, {"s", "string"}, {bson.D{{Key: "a", Value: 1}}, "object"}, {bson.A{1}, "array"},
		{bson.Binary{Data: []byte{1}}, "binData"}, {bson.Undefined{}, "undefined"}, {oid, "objectId"}, {true, "bool"},
		{bson.DateTime(0), "date"}, {nil, "null"}, {bson.Regex{Pattern: "a"}, "regex"}, {bson.DBPointer{DB: "d", Pointer: oid}, "dbPointer"},
		{bson.JavaScript("x"), "javascript"}, {bson.Symbol("x"), "symbol"}, {bson.CodeWithScope{Code: "x", Scope: bson.D{}}, "javascriptWithScope"},
		{int32(1), "int"}, {bson.Timestamp{T: 1}, "timestamp"}, {int64(1), "long"}, {dec, "decimal"}, {bson.MinKey{}, "minKey"}, {bson.MaxKey{}, "maxKey"},
		{bson.Binary{Subtype: bson.TypeBinaryUUID, Data: make([]byte, 16)}, "uuid"},
		{bson.Binary{Subtype: bson.TypeBinaryUUID, Data: make([]byte, 15)}, "binData"},
		{bson.Binary{Subtype: bson.TypeBinaryUUIDOld, Data: make([]byte, 16)}, "binData"},
	} {
		if got := typeNames[kindOf(raw(t, c.v))].name; got != c.want {
			t.Errorf("%v's kind is %s, want %s", c.v, got, c.want)
		}
	}
}

func marshal(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// unfitSchema declares n int64, x float64, d decimal(20, 2), as an override
// can, u a UUID, j JSON and s text.
var unfitSchema = arrow.NewSchema([]arrow.Field{
	{Name: "_id", Type: arrow.BinaryTypes.String}, {Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, {Name: "d", Type: &arrow.Decimal128Type{Precision: 20, Scale: 2}, Nullable: true},
	{Name: "u", Type: extensions.NewUUIDType(), Nullable: true}, {Name: "j", Type: jsonType, Nullable: true},
	{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func unfitBatcher(s *Source, changes bool) *batcher {
	cols := []string{"_id", "n", "x", "d", "u", "j", "s"}
	if changes {
		cols = append(cols, controlNames...)
	}
	return s.newBatcher(source.ConfiguredStream{Name: "c", Columns: cols, Schema: unfitSchema}, cols, changes)
}

func TestUnfit(t *testing.T) {
	dec, _ := bson.ParseDecimal128("1.25")
	uuid := bson.Binary{Subtype: bson.TypeBinaryUUID, Data: make([]byte, 16)}
	b := unfitBatcher(&Source{}, false)
	defer b.Release()
	// Values of the types discovery folds into each declared type, nulls,
	// and anything nested in JSON.
	for _, d := range []bson.D{
		{{Key: "_id", Value: 1}, {Key: "n", Value: int32(1)}, {Key: "x", Value: 2}, {Key: "d", Value: int64(3)}, {Key: "u", Value: uuid},
			{Key: "j", Value: bson.D{{Key: "a", Value: bson.A{1, "x", true}}}}, {Key: "s", Value: 1.5}},
		{{Key: "_id", Value: 2}, {Key: "n", Value: nil}, {Key: "x", Value: 2.5}, {Key: "d", Value: dec}, {Key: "u", Value: bson.Undefined{}},
			{Key: "j", Value: bson.A{1}}, {Key: "s", Value: bson.D{}}},
	} {
		if err := b.add(marshal(t, d), "", seq{}); err != nil {
			t.Errorf("%v: %v", d, err)
		}
	}
	for _, c := range []struct {
		field    string
		v        any
		declared string
		typ      string
	}{
		{"n", "secret", "int64", "string"},
		{"n", 2.5, "int64", "double"},
		{"x", dec, "float64", "decimal"},
		{"d", 2.5, "decimal(20, 2)", "double"},
		{"u", bson.Binary{Data: make([]byte, 16)}, "arrow.uuid", "binData"},
		{"j", 1, "arrow.json", "int"},
	} {
		err := b.add(marshal(t, bson.D{{Key: "_id", Value: 3}, {Key: c.field, Value: c.v}}), "", seq{})
		if connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT ||
			!strings.Contains(err.Error(), `c's field "`+c.field+`" is declared `+c.declared+`, and a document has a `+c.typ+` value there`) ||
			strings.Contains(err.Error(), "secret") {
			t.Errorf("%s of %v: %s: %v", c.field, c.v, connectortest.CodeOf(err), err)
		}
	}
	rec := &connectortest.Recorder{}
	if err := b.Flush(rec.Split("c", source.WholeStream)); err != nil {
		t.Fatal(err)
	}
	rows := rec.RowsOf("c")
	if len(rows) != 2 || rows[0]["x"] != "2" || rows[0]["d"] != "3" || rows[0]["s"] != "1.5" || rows[1]["n"] != "" || rows[1]["d"] != "1.25" {
		t.Errorf("rows = %v", rows)
	}

	// A sample can miss the value again, so it's sent, and without the
	// run's types there's nothing to check.
	for _, s := range []*Source{{cfg: SourceConfig{SampleSize: 100}}, nil} {
		var b *batcher
		if s != nil {
			b = unfitBatcher(s, false)
		} else {
			cols := []string{"_id", "n"}
			b = (&Source{}).newBatcher(source.ConfiguredStream{Name: "c", Columns: cols}, cols, false)
		}
		defer b.Release()
		if err := b.add(marshal(t, bson.D{{Key: "_id", Value: 3}, {Key: "n", Value: "x"}}), "", seq{}); err != nil {
			t.Errorf("sampled %v: %v", s != nil, err)
		}
		rec := &connectortest.Recorder{}
		if err := b.Flush(rec.Split("c", source.WholeStream)); err != nil {
			t.Fatal(err)
		}
		if rows := rec.RowsOf("c"); len(rows) != 1 || rows[0]["n"] != "x" {
			t.Errorf("sampled %v: rows = %v", s != nil, rows)
		}
	}

	// Packed, a document is one JSON value.
	p := (&Source{cfg: SourceConfig{Packed: true}}).newBatcher(source.ConfiguredStream{Name: "c", Columns: []string{"_id", "data"},
		Schema: arrow.NewSchema([]arrow.Field{{Name: "_id", Type: arrow.BinaryTypes.String}, {Name: "data", Type: jsonType, Nullable: true}}, nil)}, []string{dataColumn}, false)
	defer p.Release()
	if err := p.add(marshal(t, bson.D{{Key: "_id", Value: 1}, {Key: "data", Value: 1}}), "", seq{}); err != nil {
		t.Errorf("packed: %v", err)
	}
}

// In cdc, a value no document has any more is sent, since discovery
// wouldn't declare a type that takes it, and a delete's is left out, since
// it only says what the delete deleted.
func TestUnfitChanges(t *testing.T) {
	b := unfitBatcher(&Source{}, true)
	defer b.Release()
	var asked [][2]string
	gone := false
	b.gone = func(field, typ string) (bool, error) {
		asked = append(asked, [2]string{field, typ})
		return gone, nil
	}
	q := seq{t: bson.Timestamp{T: 1}, place: 1}
	err := b.add(marshal(t, bson.D{{Key: "_id", Value: 1}, {Key: "n", Value: "x"}}), source.OpInsert, q)
	if connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Errorf("a value a document has: %v", err)
	}
	gone = true
	if err := b.add(marshal(t, bson.D{{Key: "_id", Value: 1}, {Key: "u", Value: bson.Binary{Data: []byte{1}}}}), source.OpUpdate, q); err != nil {
		t.Errorf("a value no document has: %v", err)
	}
	if err := b.add(marshal(t, bson.D{{Key: "_id", Value: 2}, {Key: "n", Value: "x"}, {Key: "s", Value: "kept"}}), source.OpDelete, q); err != nil {
		t.Errorf("a delete's: %v", err)
	}
	if want := [][2]string{{"n", "string"}, {"u", "binData"}}; !slices.Equal(asked, want) {
		t.Errorf("asked %v, want %v", asked, want)
	}
	rec := &connectortest.Recorder{}
	if err := b.Flush(rec.Split("c", source.WholeStream)); err != nil {
		t.Fatal(err)
	}
	rows := rec.RowsOf("c")
	if len(rows) != 2 || rows[0]["u"] != "AQ==" || rows[1]["openetl.op"] != "delete" || rows[1]["s"] != "kept" {
		t.Errorf("rows = %v", rows)
	}
	if _, ok := rows[1]["n"]; ok {
		t.Errorf("a delete's unfit value is sent: %v", rows[1])
	}
}

type discard struct{}

func (discard) Rows([]json.RawMessage) error { return nil }
func (discard) Records(b arrow.RecordBatch) error {
	b.Release()
	return nil
}
func (discard) ResumePoint([]byte, ...source.Split) error { return nil }
func (discard) Complete([]byte, ...source.Split) error    { return nil }

// BenchmarkBatcher adds documents of ten fields, as a full refresh reads
// them, with the run's types, which it checks each value against, and
// without.
func BenchmarkBatcher(b *testing.B) {
	oid, _ := bson.ObjectIDFromHex("65f0a1b2c3d4e5f601234567")
	dec, _ := bson.ParseDecimal128("1234.5")
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "_id", Type: arrow.BinaryTypes.String}, {Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, {Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ok", Type: arrow.FixedWidthTypes.Boolean, Nullable: true}, {Name: "at", Type: timestampType, Nullable: true},
		{Name: "doc", Type: jsonType, Nullable: true}, {Name: "amount", Type: decimalType, Nullable: true},
		{Name: "ref", Type: arrow.BinaryTypes.String, Nullable: true}, {Name: "u", Type: extensions.NewUUIDType(), Nullable: true},
		{Name: "tags", Type: jsonType, Nullable: true},
	}, nil)
	cols := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		cols[i] = f.Name
	}
	docs := make([]bson.Raw, 1000)
	for i := range docs {
		d, err := bson.Marshal(bson.D{
			{Key: "_id", Value: oid}, {Key: "n", Value: int64(i)}, {Key: "x", Value: float64(i) / 3}, {Key: "s", Value: "some text"},
			{Key: "ok", Value: i%2 == 0}, {Key: "at", Value: bson.DateTime(1767225600000 + int64(i))},
			{Key: "doc", Value: bson.D{{Key: "a", Value: i}}}, {Key: "amount", Value: dec}, {Key: "ref", Value: oid},
			{Key: "u", Value: bson.Binary{Subtype: bson.TypeBinaryUUID, Data: make([]byte, 16)}}, {Key: "tags", Value: bson.A{"a", "b"}},
		})
		if err != nil {
			b.Fatal(err)
		}
		docs[i] = d
	}
	for _, c := range []struct {
		name   string
		schema *arrow.Schema
	}{{"types", schema}, {"no types", nil}} {
		b.Run(c.name, func(b *testing.B) {
			bt := (&Source{}).newBatcher(source.ConfiguredStream{Name: "s", Columns: cols, Schema: c.schema}, cols, false)
			defer bt.Release()
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				if err := bt.addFlushing(docs[i%len(docs)], "", seq{}, discard{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
