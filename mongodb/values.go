package mongodb

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// A source sends every field as text, which the engine parses into the type
// discovery declared: text is what values of each BSON type a column takes,
// as takes says, parse from. Each BSON value has one spelling: strings as
// they are, numbers and booleans as JSON spells them, dates in RFC 3339 with
// milliseconds, ObjectIds in hex, UUIDs canonically, other binaries in
// base64, and documents, arrays and the rest in relaxed Extended JSON. A
// value of a type its column doesn't take fails the read, where the run
// gives its types (unfit), and one that still doesn't parse into it, as a
// date past 9999, is quarantined. _id is the exception: keyText spells it.

// keyText is an _id's text, which differs for every two _ids a collection
// can hold, whatever their types, and doesn't depend on discovery: an
// ObjectId's 24 hex digits, and any other value in canonical Extended JSON,
// which tags each type and never spells bare hex: 5 is {"$numberInt":"5"}
// and "5" is "5" with its quotes. A null _id is null, a value.
func keyText(v bson.RawValue) string {
	if v.Type == bson.TypeObjectID {
		return v.ObjectID().Hex()
	}
	return extJSON(v, true)
}

// fieldType is the type a top-level field is declared as, given the BSON
// types its values have, as $type names them, with "uuid" for 16-byte
// binaries of subtype 4. Integers, int or long, are int64, and with doubles
// float64, or with decimals decimal(38, 9); documents and arrays are JSON; a
// field with values of types that declare differently is text.
func fieldType(types []string) arrow.DataType {
	var kinds []arrow.DataType
	ints, floats, decimals := false, false, false
	for _, t := range types {
		switch t {
		case "null", "undefined":
			continue
		case "int", "long":
			ints = true
			continue
		case "double":
			floats = true
			continue
		case "decimal":
			decimals = true
			continue
		}
		kinds = append(kinds, kindType(t))
	}
	switch {
	case floats && decimals:
		kinds = append(kinds, arrow.BinaryTypes.String)
	case floats:
		kinds = append(kinds, arrow.PrimitiveTypes.Float64)
	case decimals:
		kinds = append(kinds, decimalType)
	case ints:
		kinds = append(kinds, arrow.PrimitiveTypes.Int64)
	}
	if len(kinds) == 0 {
		return arrow.BinaryTypes.String
	}
	for _, k := range kinds[1:] {
		if !arrow.TypeEqual(k, kinds[0]) {
			return arrow.BinaryTypes.String
		}
	}
	return kinds[0]
}

var (
	decimalType   = &arrow.Decimal128Type{Precision: 38, Scale: 9}
	timestampType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	jsonType      = func() arrow.DataType {
		t, err := extensions.NewJSONType(arrow.BinaryTypes.String)
		if err != nil {
			panic(err)
		}
		return t
	}()
)

// typeNames are the BSON types as $type names them, and "uuid" for a 16-byte
// binary of subtype 4, as discovery names it apart. A value's kind is its
// type's index here.
var typeNames = [...]struct {
	t    bson.Type
	name string
}{
	{bson.TypeDouble, "double"}, {bson.TypeString, "string"}, {bson.TypeEmbeddedDocument, "object"},
	{bson.TypeArray, "array"}, {bson.TypeBinary, "binData"}, {bson.TypeUndefined, "undefined"},
	{bson.TypeObjectID, "objectId"}, {bson.TypeBoolean, "bool"}, {bson.TypeDateTime, "date"},
	{bson.TypeNull, "null"}, {bson.TypeRegex, "regex"}, {bson.TypeDBPointer, "dbPointer"},
	{bson.TypeJavaScript, "javascript"}, {bson.TypeSymbol, "symbol"}, {bson.TypeCodeWithScope, "javascriptWithScope"},
	{bson.TypeInt32, "int"}, {bson.TypeTimestamp, "timestamp"}, {bson.TypeInt64, "long"},
	{bson.TypeDecimal128, "decimal"}, {bson.TypeMinKey, "minKey"}, {bson.TypeMaxKey, "maxKey"},
	{bson.TypeBinary, "uuid"},
}

// kindSet is a set of kinds. Its bit otherKind is a type typeNames hasn't,
// which every set has, so that such a value is sent as it is.
type kindSet uint32

const (
	uuidKind  = len(typeNames) - 1
	otherKind = len(typeNames)
	allKinds  = ^kindSet(0)
)

// kindOfType is the kind of each BSON type but uuid's.
var kindOfType = func() (out [256]int) {
	for i := range out {
		out[i] = otherKind
	}
	for i, k := range typeNames[:uuidKind] {
		out[k.t] = i
	}
	return out
}()

func kindOf(v bson.RawValue) int {
	if v.Type == bson.TypeBinary {
		if sub, data, ok := v.BinaryOK(); ok && isUUID(sub, data) {
			return uuidKind
		}
	}
	return kindOfType[v.Type]
}

type declaredType struct {
	t     arrow.DataType
	takes kindSet
}

// declaredTypes are the types fieldType declares, each with the kinds of
// the values a field so declared takes: those that discovery, having seen
// them beside the kinds that alone it declares as that type, still declares
// as it. Whichever kinds discovery saw, that's the same set, but for nulls
// alone, declared as text, which takes every kind, as text does.
var declaredTypes = func() (out []declaredType) {
	var own [][]string
	for _, k := range typeNames {
		t := fieldType([]string{k.name})
		i := slices.IndexFunc(out, func(d declaredType) bool { return arrow.TypeEqual(d.t, t) })
		if i < 0 {
			i = len(out)
			out, own = append(out, declaredType{t: t, takes: 1 << otherKind}), append(own, nil)
		}
		own[i] = append(own[i], k.name)
	}
	for i := range out {
		for j, k := range typeNames {
			if arrow.TypeEqual(fieldType(append(slices.Clip(own[i]), k.name)), out[i].t) {
				out[i].takes |= 1 << j
			}
		}
	}
	return out
}()

// takes are the kinds of the values a field declared t takes. A decimal
// takes what discovery's does, whatever its precision and scale; a type
// discovery doesn't declare takes every kind.
func takes(t arrow.DataType) kindSet {
	for _, d := range declaredTypes {
		if arrow.TypeEqual(t, d.t) || arrow.IsDecimal(t.ID()) && arrow.IsDecimal(d.t.ID()) {
			return d.takes
		}
	}
	return allKinds
}

func kindType(t string) arrow.DataType {
	switch t {
	case "bool":
		return arrow.FixedWidthTypes.Boolean
	case "string", "symbol", "objectId":
		return arrow.BinaryTypes.String
	case "date":
		return timestampType
	case "binData":
		return arrow.BinaryTypes.Binary
	case "uuid":
		return extensions.NewUUIDType()
	}
	// object, array, regex, javascript, javascriptWithScope, dbPointer,
	// timestamp, minKey, maxKey.
	return jsonType
}

// text is v's spelling; false for null and undefined.
func text(v bson.RawValue) (string, bool) {
	switch v.Type {
	case bson.TypeNull, bson.TypeUndefined:
		return "", false
	case bson.TypeString:
		return v.StringValue(), true
	case bson.TypeSymbol:
		return v.Symbol(), true
	case bson.TypeObjectID:
		return v.ObjectID().Hex(), true
	case bson.TypeBoolean:
		return strconv.FormatBool(v.Boolean()), true
	case bson.TypeInt32:
		return strconv.FormatInt(int64(v.Int32()), 10), true
	case bson.TypeInt64:
		return strconv.FormatInt(v.Int64(), 10), true
	case bson.TypeDouble:
		return floatText(v.Double()), true
	case bson.TypeDecimal128:
		return v.Decimal128().String(), true
	case bson.TypeDateTime:
		return dateText(v.DateTime()), true
	case bson.TypeBinary:
		sub, data := v.Binary()
		if isUUID(sub, data) {
			return uuidText(data), true
		}
		return base64.StdEncoding.EncodeToString(data), true
	}
	return extJSON(v, false), true
}

func floatText(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// dateText spells milliseconds since 1970 in UTC. Years past 9999 have more
// digits, and those before 1 a sign, which the engine finds out of range.
func dateText(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z")
}

func isUUID(subtype byte, data []byte) bool {
	return subtype == bson.TypeBinaryUUID && len(data) == 16
}

func uuidText(b []byte) string {
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// extJSON is v in Extended JSON: canonical keeps every type, relaxed spells
// numbers and dates as JSON does where they fit.
func extJSON(v bson.RawValue, canonical bool) string {
	b, err := bson.MarshalExtJSON(bson.D{{Key: "v", Value: v}}, canonical, false)
	if err != nil {
		return fmt.Sprintf("%q", v.String())
	}
	b = bytes.TrimPrefix(b, []byte(`{"v":`))
	return string(bytes.TrimSuffix(b, []byte("}")))
}

// value is a BSON value that keeps its type through JSON, in canonical
// Extended JSON: a key or a cursor in a state.
type value struct{ bson.RawValue }

func (v value) MarshalJSON() ([]byte, error) { return []byte(extJSON(v.RawValue, true)), nil }

func (v *value) UnmarshalJSON(b []byte) error {
	var doc bson.Raw
	if err := bson.UnmarshalExtJSON(append(append([]byte(`{"v":`), b...), '}'), true, &doc); err != nil {
		return err
	}
	v.RawValue = doc.Lookup("v")
	return nil
}
