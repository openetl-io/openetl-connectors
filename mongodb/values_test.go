package mongodb

import (
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestFieldType(t *testing.T) {
	uuidType := extensions.NewUUIDType()
	for _, c := range []struct {
		types []string
		want  arrow.DataType
	}{
		{nil, arrow.BinaryTypes.String},
		{[]string{"null"}, arrow.BinaryTypes.String},
		{[]string{"int"}, arrow.PrimitiveTypes.Int64},
		{[]string{"int", "long", "null"}, arrow.PrimitiveTypes.Int64},
		{[]string{"int", "double"}, arrow.PrimitiveTypes.Float64},
		{[]string{"long", "decimal"}, decimalType},
		{[]string{"double", "decimal"}, arrow.BinaryTypes.String},
		{[]string{"bool", "undefined"}, arrow.FixedWidthTypes.Boolean},
		{[]string{"string", "objectId", "symbol"}, arrow.BinaryTypes.String},
		{[]string{"date"}, timestampType},
		{[]string{"binData"}, arrow.BinaryTypes.Binary},
		{[]string{"uuid"}, uuidType},
		{[]string{"uuid", "binData"}, arrow.BinaryTypes.String},
		{[]string{"object", "array", "regex", "timestamp", "minKey", "maxKey", "javascript", "dbPointer"}, jsonType},
		{[]string{"object", "int"}, arrow.BinaryTypes.String},
		{[]string{"date", "string"}, arrow.BinaryTypes.String},
	} {
		if got := fieldType(c.types); !arrow.TypeEqual(got, c.want) {
			t.Errorf("fieldType(%v) = %s, want %s", c.types, got, c.want)
		}
	}
}

func raw(t *testing.T, v any) bson.RawValue {
	t.Helper()
	b, err := bson.Marshal(bson.D{{Key: "v", Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return bson.Raw(b).Lookup("v")
}

func TestText(t *testing.T) {
	oid, _ := bson.ObjectIDFromHex("65f0a1b2c3d4e5f601234567")
	dec, _ := bson.ParseDecimal128("-12.50")
	u := []byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11}
	for _, c := range []struct {
		v    any
		want string
	}{
		{"héllo", "héllo"},
		{bson.Symbol("s"), "s"},
		{oid, "65f0a1b2c3d4e5f601234567"},
		{true, "true"},
		{int32(-7), "-7"},
		{int64(math.MaxInt64), "9223372036854775807"},
		{1.5, "1.5"},
		{1e300, "1e+300"},
		{math.NaN(), "NaN"},
		{math.Inf(1), "inf"},
		{math.Inf(-1), "-inf"},
		{dec, "-12.50"},
		{bson.NewDateTimeFromTime(time.Date(2026, 11, 3, 10, 0, 0, 123e6, time.UTC)), "2026-11-03T10:00:00.123Z"},
		{bson.Binary{Data: []byte{0, 255}}, "AP8="},
		{bson.Binary{Subtype: 4, Data: u}, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{bson.Binary{Subtype: 3, Data: u}, "oO68mZwLTvi7bWu5vTgKEQ=="},
		{bson.D{{Key: "a", Value: bson.A{1, "x"}}, {Key: "d", Value: 2.0}}, `{"a":[1,"x"],"d":2.0}`},
		{bson.A{}, `[]`},
		{bson.Regex{Pattern: "^a", Options: "i"}, `{"$regularExpression":{"pattern":"^a","options":"i"}}`},
		{bson.Timestamp{T: 1, I: 2}, `{"$timestamp":{"t":1,"i":2}}`},
		{bson.MinKey{}, `{"$minKey":1}`},
		{bson.MaxKey{}, `{"$maxKey":1}`},
		{bson.JavaScript("f()"), `{"$code":"f()"}`},
	} {
		got, ok := text(raw(t, c.v))
		if !ok || got != c.want {
			t.Errorf("text(%v) = %q, %v; want %q", c.v, got, ok, c.want)
		}
	}
	for _, v := range []any{nil, bson.Undefined{}} {
		if got, ok := text(raw(t, v)); ok {
			t.Errorf("text(%v) = %q, want none", v, got)
		}
	}
}

func TestKeyText(t *testing.T) {
	oid, _ := bson.ObjectIDFromHex("65f0a1b2c3d4e5f601234567")
	dec, _ := bson.ParseDecimal128("0.1")
	date := bson.NewDateTimeFromTime(time.Date(2026, 1, 2, 3, 4, 5, 6e6, time.UTC))
	u := bson.Binary{Subtype: 4, Data: []byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11}}
	// Values whose text() is the same as another's: 5 and "5", 0.1 and its
	// decimal, an ObjectId and its hex, a date and its text, a UUID and its.
	for _, c := range []struct {
		v    any
		want string
	}{
		{int32(5), `{"$numberInt":"5"}`},
		{"5", `"5"`},
		{int64(5), `{"$numberLong":"5"}`},
		{int64(1 << 40), `{"$numberLong":"1099511627776"}`},
		{"1099511627776", `"1099511627776"`},
		{0.1, `{"$numberDouble":"0.1"}`},
		{dec, `{"$numberDecimal":"0.1"}`},
		{oid, "65f0a1b2c3d4e5f601234567"},
		{"65f0a1b2c3d4e5f601234567", `"65f0a1b2c3d4e5f601234567"`},
		{date, `{"$date":{"$numberLong":"1767323045006"}}`},
		{"2026-01-02T03:04:05.006Z", `"2026-01-02T03:04:05.006Z"`},
		{u, `{"$binary":{"base64":"oO68mZwLTvi7bWu5vTgKEQ==","subType":"04"}}`},
		{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`},
		{nil, `null`},
		{"null", `"null"`},
		{true, `true`},
		{bson.MinKey{}, `{"$minKey":1}`},
		{bson.MaxKey{}, `{"$maxKey":1}`},
		// Documents of the same fields in another order are other _ids.
		{bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "x"}}, `{"a":{"$numberInt":"1"},"b":"x"}`},
		{bson.D{{Key: "b", Value: "x"}, {Key: "a", Value: int32(1)}}, `{"b":"x","a":{"$numberInt":"1"}}`},
		{bson.A{int32(1)}, `[{"$numberInt":"1"}]`},
		{bson.Binary{Data: []byte{0, 255}}, `{"$binary":{"base64":"AP8=","subType":"00"}}`},
		{bson.Binary{Subtype: 2, Data: []byte{1}}, `{"$binary":{"base64":"AQ==","subType":"02"}}`},
		{bson.Symbol("s"), `{"$symbol":"s"}`},
		{bson.Timestamp{T: 1, I: 2}, `{"$timestamp":{"t":1,"i":2}}`},
		{bson.JavaScript("f()"), `{"$code":"f()"}`},
		{bson.Regex{Pattern: "^a", Options: "i"}, `{"$regularExpression":{"pattern":"^a","options":"i"}}`},
		{math.NaN(), `{"$numberDouble":"NaN"}`},
		{math.Inf(1), `{"$numberDouble":"Infinity"}`},
		{math.Inf(-1), `{"$numberDouble":"-Infinity"}`},
		{math.Copysign(0, -1), `{"$numberDouble":"-0.0"}`},
		{bson.NewDecimal128(0x7c00000000000000, 0), `{"$numberDecimal":"NaN"}`},
		{bson.NewDecimal128(0x7800000000000000, 0), `{"$numberDecimal":"Infinity"}`},
	} {
		in := raw(t, c.v)
		got := keyText(in)
		if got != c.want {
			t.Errorf("keyText(%v) = %s, want %s", c.v, got, c.want)
			continue
		}
		back, err := bsonKey(got)
		if err != nil {
			t.Errorf("bsonKey(%s): %v", got, err)
			continue
		}
		if out := raw(t, back); out.Type != in.Type || !out.Equal(in) {
			t.Errorf("%v came back from %s as %v", c.v, got, out)
		}
	}
	for _, s := range []string{"5", "abc", `{"$numberInt": 5}`, "65F0A1B2C3D4E5F601234567", `{ "$oid": "65f0a1b2c3d4e5f601234567" }`} {
		if v, err := bsonKey(s); err == nil {
			t.Errorf("bsonKey(%s) = %v, want an error", s, v)
		}
	}
}

func TestValueJSON(t *testing.T) {
	oid, _ := bson.ObjectIDFromHex("65f0a1b2c3d4e5f601234567")
	for _, v := range []any{int32(5), int64(5), 5.0, "5", oid, bson.D{{Key: "k", Value: int32(1)}}, bson.MinKey{}, bson.NewDateTimeFromTime(time.UnixMilli(1))} {
		in := value{raw(t, v)}
		b, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		var out value
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("%s: %v", b, err)
		}
		if out.Type != in.Type || !in.Equal(out.RawValue) {
			t.Errorf("%v went through JSON as %s and came back %v", v, b, out)
		}
	}
}

func TestSeqOrder(t *testing.T) {
	seqs := []seq{
		{t: bson.Timestamp{T: 1, I: 0}},
		{t: bson.Timestamp{T: 1, I: 0}, place: 1},
		{t: bson.Timestamp{T: 1, I: 0}, place: math.MaxUint32},
		{t: bson.Timestamp{T: 1, I: 1}},
		{t: bson.Timestamp{T: 2, I: 0}},
		{t: bson.Timestamp{T: math.MaxUint32, I: math.MaxUint32}, place: math.MaxUint32},
	}
	for i := 1; i < len(seqs); i++ {
		a, b := seqs[i-1].decimal(), seqs[i].decimal()
		if a.Cmp(b) >= 0 {
			t.Errorf("seq %v isn't below %v", seqs[i-1], seqs[i])
		}
	}
	if !seqs[len(seqs)-1].decimal().FitsInPrecision(seqType.Precision) {
		t.Errorf("the largest seq doesn't fit decimal(38, 0)")
	}
}

func TestTokens(t *testing.T) {
	tok := func(data string) *value { return &value{raw(t, bson.D{{Key: "_data", Value: data}})} }
	a := tok("826A1B2C3D000000022B042C0100296E5A1004")
	b := tok("826A1B2C3D000000032B042C0100296E5A1004")
	if tokenCompare(a, b) >= 0 || tokenCompare(nil, a) >= 0 || tokenCompare(a, a) != 0 {
		t.Errorf("tokens don't order as their _data does")
	}
	at, ok := tokenTime(a)
	if !ok || at != (bson.Timestamp{T: 0x6A1B2C3D, I: 2}) {
		t.Errorf("tokenTime = %v, %v", at, ok)
	}
	if _, ok := tokenTime(tok("zz")); ok {
		t.Errorf("tokenTime of a token that isn't hex")
	}
}

// A position moved to a post-batch resume token at the cluster time of the
// last change read, as one within a transaction is, keeps that change's
// place, so that a change after it at that time, read from the state, takes
// the next; at a later time, a change takes the first.
func TestMoveToKeepsPlace(t *testing.T) {
	tok := func(data string) *value { return &value{raw(t, bson.D{{Key: "_data", Value: data}})} }
	last := bson.Timestamp{T: 0x6A1B2C3D, I: 2}
	r := &cdcReader{st: cdcState{Token: tok("826A1B2C3D000000022B042C0100296E5A1004"), Place: 2}}
	r.moveTo(tok("826A1B2C3D000000022B042C0100296E5A1009"), last, 2)
	if r.st.Place != 2 {
		t.Errorf("moved within the last change's cluster time: place %d, want 2", r.st.Place)
	}
	r.moveTo(tok("826A1B2C3D000000032B042C0100296E5A1004"), last, 2)
	if r.st.Place != 0 {
		t.Errorf("moved past the last change's cluster time: place %d, want 0", r.st.Place)
	}
}

func TestDecimalValue(t *testing.T) {
	small, _ := new(big.Int).SetString("-1250", 10)
	v, err := decimalValue(small, 2)
	if d, ok := v.(bson.Decimal128); err != nil || !ok || d.String() != "-12.50" {
		t.Errorf("decimalValue(-1250, 2) = %v, %v", v, err)
	}
	large, _ := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	v, err = decimalValue(large, 9)
	if s, ok := v.(string); err != nil || !ok || s != "99999999999999999999999999999.999999999" {
		t.Errorf("decimalValue of 38 digits = %v, %v", v, err)
	}
	// A negative scale multiplies by its power of ten.
	v, err = decimalValue(large, -2)
	if s, ok := v.(string); err != nil || !ok || s != "9999999999999999999999999999999999999900" {
		t.Errorf("decimalValue of 38 digits at scale -2 = %v, %v", v, err)
	}
}

func TestParseJSON(t *testing.T) {
	for _, c := range []struct {
		in   string
		want bson.Type
	}{
		{`{"a": 1}`, bson.TypeEmbeddedDocument},
		{`[1, 2]`, bson.TypeArray},
		{`null`, bson.TypeNull},
		{`123`, bson.TypeInt32},
		{`"text"`, bson.TypeString},
		{`{"$oid": "65f0a1b2c3d4e5f601234567"}`, bson.TypeObjectID},
		{`{"$date": "2026-01-01T00:00:00Z"}`, bson.TypeDateTime},
	} {
		v, err := parseJSON(c.in)
		if err != nil {
			t.Fatalf("parseJSON(%s): %v", c.in, err)
		}
		if rv, ok := v.(bson.RawValue); !ok || rv.Type != c.want {
			t.Errorf("parseJSON(%s) = %v, want a %s", c.in, v, c.want)
		}
	}
	// Valid JSON that isn't valid Extended JSON stays text.
	if v, err := parseJSON(`{"$oid": 1}`); err != nil || v != `{"$oid": 1}` {
		t.Errorf("parseJSON of a bad $oid = %v, %v", v, err)
	}
	if _, err := parseJSON(`{`); err == nil {
		t.Errorf("parseJSON of text that isn't JSON")
	}
}

func TestBatcher(t *testing.T) {
	doc := func(d bson.D) bson.Raw {
		b, err := bson.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	rec := &connectortest.Recorder{}
	out := rec.Split("s", source.WholeStream)
	cols := []string{"_id", "a", "openetl.op", "openetl.seq"}
	b := (&Source{}).newBatcher(source.ConfiguredStream{Name: "s", Columns: cols}, cols, true)
	defer b.Release()
	q := seq{t: bson.Timestamp{T: 1}, place: 1}
	if err := b.add(doc(bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: "x"}}), source.OpInsert, q); err != nil {
		t.Fatal(err)
	}
	// A delete's undeclared fields say only what it deleted.
	if err := b.add(doc(bson.D{{Key: "_id", Value: 2}, {Key: "z", Value: 1}}), source.OpDelete, q); err != nil {
		t.Fatal(err)
	}
	err := b.add(doc(bson.D{{Key: "_id", Value: 3}, {Key: "z", Value: 1}}), source.OpUpdate, q)
	if err == nil || !strings.Contains(err.Error(), `new field "z"`) || strings.Contains(err.Error(), "packed") {
		t.Errorf("an undeclared field: %v", err)
	}
	// Discovery that samples can miss the field again: the error says how not to.
	sampled := (&Source{cfg: SourceConfig{SampleSize: 100}}).newBatcher(source.ConfiguredStream{Name: "s", Columns: cols}, cols, true)
	defer sampled.Release()
	err = sampled.add(doc(bson.D{{Key: "_id", Value: 3}, {Key: "z", Value: 1}}), source.OpUpdate, q)
	if connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT || !strings.Contains(err.Error(), "sample of 100") || !strings.Contains(err.Error(), "packed") {
		t.Errorf("an undeclared field, sampled: %v", err)
	}
	b.gone = func(string) (bool, error) { return true, nil }
	if err := b.add(doc(bson.D{{Key: "_id", Value: 4}, {Key: "z", Value: 1}}), source.OpUpdate, q); err != nil {
		t.Errorf("a field no document has: %v", err)
	}
	if err := b.Flush(out); err != nil {
		t.Fatal(err)
	}
	rows := rec.RowsOf("s")
	want := []map[string]string{
		{"_id": intID(1), "a": "x", "openetl.op": "insert"},
		{"_id": intID(2), "openetl.op": "delete"},
		{"_id": intID(4), "openetl.op": "update"},
	}
	if len(rows) != len(want) {
		t.Fatalf("rows = %v", rows)
	}
	for i, w := range want {
		delete(rows[i], "openetl.seq")
		if len(rows[i]) != len(w) || rows[i]["_id"] != w["_id"] || rows[i]["a"] != w["a"] || rows[i]["openetl.op"] != w["openetl.op"] {
			t.Errorf("row %d = %v, want %v", i, rows[i], w)
		}
	}

	// Without the run's columns, a batch has the fields its documents have.
	b2 := (&Source{}).newBatcher(source.ConfiguredStream{Name: "s"}, nil, false)
	defer b2.Release()
	for _, d := range []bson.D{{{Key: "_id", Value: 1}}, {{Key: "_id", Value: 2}, {Key: "n", Value: 1}}} {
		if err := b2.add(doc(d), "", seq{}); err != nil {
			t.Fatal(err)
		}
	}
	rec2 := &connectortest.Recorder{}
	if err := b2.Flush(rec2.Split("s", "")); err != nil {
		t.Fatal(err)
	}
	if rows := rec2.RowsOf("s"); len(rows) != 2 || rows[0]["n"] != "" || rows[1]["n"] != "1" {
		t.Errorf("rows = %v", rows)
	}

	// Packed, a row is _id and the whole document, whatever its fields; a
	// delete known by its key alone has no document.
	p := &Source{cfg: SourceConfig{Packed: true}}
	cs := source.ConfiguredStream{Name: "s", Columns: []string{"_id", "data", "openetl.op", "openetl.seq"}}
	b3 := p.newBatcher(cs, []string{dataColumn}, true)
	defer b3.Release()
	if err := b3.add(doc(bson.D{{Key: "_id", Value: 1}, {Key: "new", Value: int64(2)}}), source.OpInsert, q); err != nil {
		t.Fatal(err)
	}
	if err := b3.addKey(raw(t, "k"), q); err != nil {
		t.Fatal(err)
	}
	rec3 := &connectortest.Recorder{}
	if err := b3.Flush(rec3.Split("s", "")); err != nil {
		t.Fatal(err)
	}
	rows = rec3.RowsOf("s")
	if len(rows) != 2 || rows[0]["_id"] != intID(1) || rows[0]["data"] != `{"_id":{"$numberInt":"1"},"new":{"$numberLong":"2"}}` ||
		rows[1]["_id"] != `"k"` || rows[1]["data"] != "" || rows[1]["openetl.op"] != "delete" {
		t.Errorf("packed rows = %v", rows)
	}
}

func TestJSONSchema(t *testing.T) {
	b, _ := bson.Marshal(bson.D{{Key: "$jsonSchema", Value: bson.D{
		{Key: "required", Value: bson.A{"a", "_openetl_id"}},
		{Key: "properties", Value: bson.D{
			{Key: "a", Value: bson.D{{Key: "bsonType", Value: "int"}}},
			{Key: "b", Value: bson.D{{Key: "type", Value: bson.A{"number", "boolean"}}}},
		}},
	}}})
	v := &jsonSchema{b}
	if got := v.required(); len(got) != 2 || got[0] != "a" {
		t.Errorf("required = %v", got)
	}
	if !accepts(v.kinds("a"), "int") || accepts(v.kinds("a"), "long") {
		t.Errorf("a's kinds = %v", v.kinds("a"))
	}
	if !accepts(v.kinds("b"), "double") || !accepts(v.kinds("b"), "bool") || accepts(v.kinds("b"), "string") {
		t.Errorf("b's kinds = %v", v.kinds("b"))
	}
	if v.kinds("c") != nil {
		t.Errorf("c, which the validator doesn't name, has kinds %v", v.kinds("c"))
	}
	w, err := v.widen("b", "string")
	if err != nil {
		t.Fatal(err)
	}
	wb, _ := bson.Marshal(w)
	got := (&jsonSchema{wb}).kinds("b")
	if !accepts(got, "string") || !accepts(got, "bool") || !accepts(got, "long") {
		t.Errorf("widened, b's kinds = %v", got)
	}
	if _, err := bson.Raw(wb).LookupErr("$jsonSchema", "properties", "b", "type"); err == nil {
		t.Errorf("widened, b keeps its type beside its bsonType: %s", bson.Raw(wb))
	}
}

func TestPositionLost(t *testing.T) {
	r := &cdcReader{cs: source.ConfiguredStream{Name: "c"}}
	for _, code := range []int32{codeChangeStreamLost, codeChangeStreamFatal, codeCappedPositionLost} {
		err := r.failed(mongo.CommandError{Code: code, Message: "Resume of change stream was not possible"})
		var c interface{ ErrorCode() protocolv1.ErrorCode }
		if !errors.As(err, &c) || c.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST || !strings.Contains(err.Error(), "no longer in the oplog") {
			t.Errorf("code %d: %v", code, err)
		}
	}
	err := r.failed(mongo.CommandError{Code: 13, Message: "not authorized"})
	var c interface{ ErrorCode() protocolv1.ErrorCode }
	if !errors.As(err, &c) || c.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Errorf("code 13: %v", err)
	}
}

// A read that stops at its until emits the rows it holds before it
// completes, whether or not the until's document is still there.
func TestEndAtFlushes(t *testing.T) {
	rec := &connectortest.Recorder{}
	s := &Source{}
	b := s.newBatcher(source.ConfiguredStream{Name: "s"}, nil, false)
	defer b.Release()
	d, err := bson.Marshal(bson.D{{Key: "_id", Value: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.add(d, "", seq{}); err != nil {
		t.Fatal(err)
	}
	if err := s.endAt(t.Context(), nil, nil, nil, []byte(`{}`), b, "", seq{}, rec.Split("s", source.WholeStream)); err != nil {
		t.Fatal(err)
	}
	got := rec.Snapshot()
	if len(got) != 3 || len(got[0].Rows) != 1 || !got[1].Resume || !got[2].Complete {
		t.Errorf("emitted %+v", got)
	}
}

// Of the run's columns, a batcher leaves out only the control columns it
// adds itself, not a field whose name starts as theirs do.
func TestBatcherControlNames(t *testing.T) {
	cols := []string{"_id", "openetl.note", "openetl.op", "openetl.seq"}
	b := (&Source{}).newBatcher(source.ConfiguredStream{Name: "s", Columns: cols}, cols, true)
	defer b.Release()
	d, err := bson.Marshal(bson.D{{Key: "_id", Value: 1}, {Key: "openetl.note", Value: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.add(d, source.OpInsert, seq{}); err != nil {
		t.Fatalf("a declared field named openetl.note: %v", err)
	}
	if _, ok := b.Index["openetl.op"]; ok {
		t.Errorf("the batch has openetl.op as a field too: %v", b.Names)
	}
	rec := &connectortest.Recorder{}
	if err := b.Flush(rec.Split("s", source.WholeStream)); err != nil {
		t.Fatal(err)
	}
	if rows := rec.RowsOf("s"); len(rows) != 1 || rows[0]["openetl.note"] != "x" {
		t.Errorf("rows = %v", rows)
	}
}

// An append deduped against the collection is keyed on its _openetl_id, so
// a later epoch's row of that ID finds it; deduped within each epoch only,
// it's keyed on an ObjectId, so that a later epoch's lands beside it.
func TestDedupeIDs(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String}, {Name: idColumn, Type: arrow.BinaryTypes.String}}, nil)
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	rb.Field(0).(*array.StringBuilder).Append("a")
	rb.Field(1).(*array.StringBuilder).Append("id-1")
	batch := rb.NewRecordBatch()
	defer batch.Release()
	for dedupe, want := range map[destination.Dedupe]bson.Type{destination.DedupeTarget: bson.TypeString, destination.DedupeEpoch: bson.TypeObjectID} {
		rows, err := (&writer{}).rows(destination.Stream{Name: "s", Mode: destination.Append, Dedupe: dedupe}, batch)
		if err != nil {
			t.Fatal(err)
		}
		doc := rows[0].(stagedRow).Doc
		if id := doc.Lookup("_id"); id.Type != want || doc.Lookup(idColumn).StringValue() != "id-1" {
			t.Errorf("dedupe %d: staged %s", dedupe, doc)
		}
	}
}

// The columns a validator refuses share one fix, which takes every one's
// types, since each collMod replaces the whole validator; a decimal of more
// digits than Decimal128 has lands as a string too.
func TestWidening(t *testing.T) {
	b, _ := bson.Marshal(bson.D{{Key: "$jsonSchema", Value: bson.D{{Key: "properties", Value: bson.D{
		{Key: "a", Value: bson.D{{Key: "bsonType", Value: "int"}}},
		{Key: "d", Value: bson.D{{Key: "bsonType", Value: "decimal"}}},
		{Key: "n", Value: bson.D{{Key: "bsonType", Value: "decimal"}}},
	}}}}})
	v := &jsonSchema{b}
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: 9}},
		{Name: "n", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}},
	}, nil)
	cmd, refused, err := widening(v, "orders", "s", sch)
	if err != nil {
		t.Fatal(err)
	}
	if len(refused) != 2 || refused[0].Column != "a" || refused[1].Column != "d" || refused[1].ToType != "decimal|string" || refused[0].Fix != refused[1].Fix {
		t.Fatalf("refused %+v", refused)
	}
	got := &jsonSchema{cmd[1].Value.(bson.Raw)}
	if !accepts(got.kinds("a"), "long") || !accepts(got.kinds("d"), "string") || !accepts(got.kinds("d"), "decimal") {
		t.Errorf("the fix's validator: %s", got.raw)
	}
	// One column's fix is the collMod of that column's widening.
	cmd, refused, err = widening(v, "orders", "s", arrow.NewSchema(sch.Fields()[:1], nil))
	if err != nil || len(refused) != 1 {
		t.Fatalf("refused %+v: %v", refused, err)
	}
	fix, _ := v.widen("a", "long")
	text, _ := bson.MarshalExtJSON(bson.D{{Key: "collMod", Value: "orders"}, {Key: "validator", Value: fix}}, false, false)
	if refused[0].Fix != "db.runCommand("+string(text)+")" {
		t.Errorf("fix %s, want db.runCommand(%s)", refused[0].Fix, text)
	}
	if cmd, refused, _ := widening(nil, "orders", "s", sch); cmd != nil || refused != nil {
		t.Errorf("without a validator: %v, %v", cmd, refused)
	}
}

// The destination reads its staging, markers and collections from the
// primary, and its writes are acknowledged, whatever the uri asks: a
// secondary that's behind could miss staged rows, which the apply would
// then leave out, and unacknowledged writes report no errors.
func TestDestinationReadsWhatItWrote(t *testing.T) {
	d, err := NewDestination([]byte(`{"uri": "mongodb://u:p@h1,h2/?replicaSet=rs0&readPreference=secondaryPreferred&readConcernLevel=majority&w=0", "database": "d"}`))
	if err != nil {
		t.Fatal(err)
	}
	if rp := d.opts.ReadPreference; rp == nil || rp.Mode() != readpref.PrimaryMode {
		t.Errorf("read preference %v, want primary", rp)
	}
	if rc := d.opts.ReadConcern; rc == nil || rc.Level != "local" {
		t.Errorf("read concern %v, want local", rc)
	}
	if wc := d.opts.WriteConcern; !wc.Acknowledged() {
		t.Errorf("write concern %v isn't acknowledged", wc)
	}
	// The uri's acknowledged write concern stays.
	if d, err = NewDestination([]byte(`{"uri": "mongodb://u:p@h1/?w=2", "database": "d"}`)); err != nil {
		t.Fatal(err)
	}
	if wc := d.opts.WriteConcern; wc == nil || wc.W != 2 {
		t.Errorf("write concern %v, want the uri's w=2", wc)
	}
}
