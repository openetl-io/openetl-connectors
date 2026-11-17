package mongodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	extensionNameKey = "ARROW:extension:name"
	jsonExtension    = "arrow.json"
	uuidExtension    = "arrow.uuid"
	// The engine names each stream's table in its schema's metadata.
	tableKey = "openetl.table"
)

// convert is a column's value of row i, which isn't null, as BSON takes it.
type convert func(a arrow.Array, i int) (any, error)

// converter maps an Arrow type to BSON: integers to int32 or int64,
// floats to doubles, decimals to Decimal128 where its 34 digits hold them
// and to text past that, timestamps and dates to BSON dates, which keep
// milliseconds, times of day to "15:04:05.000000" text, intervals to
// documents of months, days and nanoseconds, UUIDs to binary subtype 4,
// lists to arrays, and JSON to the value it spells, read as relaxed
// Extended JSON so that {"$oid": ...} and the like come back as their types.
func converter(f arrow.Field) (convert, string, error) {
	dt, ext := f.Type, ""
	if et, ok := dt.(arrow.ExtensionType); ok {
		ext, dt = et.ExtensionName(), et.StorageType()
	} else if v, ok := f.Metadata.GetValue(extensionNameKey); ok {
		ext = v
	}
	switch {
	case ext == jsonExtension && isString(dt):
		return func(a arrow.Array, i int) (any, error) { return parseJSON(str(a, i)) }, "", nil
	case ext == uuidExtension && dt.ID() == arrow.FIXED_SIZE_BINARY:
		return func(a arrow.Array, i int) (any, error) {
			return bson.Binary{Subtype: bson.TypeBinaryUUID, Data: storage(a).(*array.FixedSizeBinary).Value(i)}, nil
		}, "binData", nil
	}
	switch t := dt.(type) {
	case *arrow.NullType:
		return func(arrow.Array, int) (any, error) { return nil, nil }, "", nil
	case *arrow.BooleanType:
		return func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Boolean).Value(i), nil }, "bool", nil
	case *arrow.Int8Type:
		return func(a arrow.Array, i int) (any, error) { return int32(storage(a).(*array.Int8).Value(i)), nil }, "int", nil
	case *arrow.Int16Type:
		return func(a arrow.Array, i int) (any, error) { return int32(storage(a).(*array.Int16).Value(i)), nil }, "int", nil
	case *arrow.Int32Type:
		return func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Int32).Value(i), nil }, "int", nil
	case *arrow.Int64Type:
		return func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Int64).Value(i), nil }, "long", nil
	case *arrow.Uint8Type:
		return func(a arrow.Array, i int) (any, error) { return int32(storage(a).(*array.Uint8).Value(i)), nil }, "int", nil
	case *arrow.Uint16Type:
		return func(a arrow.Array, i int) (any, error) { return int32(storage(a).(*array.Uint16).Value(i)), nil }, "int", nil
	case *arrow.Uint32Type:
		return func(a arrow.Array, i int) (any, error) { return int64(storage(a).(*array.Uint32).Value(i)), nil }, "long", nil
	case *arrow.Float32Type:
		return func(a arrow.Array, i int) (any, error) { return float64(storage(a).(*array.Float32).Value(i)), nil }, "double", nil
	case *arrow.Float64Type:
		return func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Float64).Value(i), nil }, "double", nil
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return func(a arrow.Array, i int) (any, error) { return str(a, i), nil }, "string", nil
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType:
		return func(a arrow.Array, i int) (any, error) {
			return bson.Binary{Data: storage(a).(interface{ Value(int) []byte }).Value(i)}, nil
		}, "binData", nil
	case *arrow.Decimal128Type:
		scale := t.Scale
		return func(a arrow.Array, i int) (any, error) {
			return decimalValue(storage(a).(*array.Decimal128).Value(i).BigInt(), scale)
		}, "decimal", nil
	case *arrow.TimestampType:
		unit := t.Unit
		return func(a arrow.Array, i int) (any, error) {
			return bson.NewDateTimeFromTime(storage(a).(*array.Timestamp).Value(i).ToTime(unit)), nil
		}, "date", nil
	case *arrow.Date32Type:
		return func(a arrow.Array, i int) (any, error) {
			return bson.NewDateTimeFromTime(storage(a).(*array.Date32).Value(i).ToTime()), nil
		}, "date", nil
	case *arrow.Time64Type:
		unit := time.Duration(1000)
		if t.Unit == arrow.Nanosecond {
			unit = 1
		}
		return func(a arrow.Array, i int) (any, error) {
			d := time.Duration(storage(a).(*array.Time64).Value(i)) * unit
			return time.Time{}.Add(d).Format("15:04:05.000000"), nil
		}, "string", nil
	case *arrow.MonthDayNanoIntervalType:
		return func(a arrow.Array, i int) (any, error) {
			v := storage(a).(*array.MonthDayNanoInterval).Value(i)
			return bson.D{{Key: "months", Value: v.Months}, {Key: "days", Value: v.Days}, {Key: "nanoseconds", Value: v.Nanoseconds}}, nil
		}, "object", nil
	case *arrow.ListType:
		elem, _, err := converter(t.ElemField())
		if err != nil {
			return nil, "", err
		}
		return func(a arrow.Array, i int) (any, error) {
			l := storage(a).(*array.List)
			from, to := l.ValueOffsets(i)
			out := bson.A{}
			for j := from; j < to; j++ {
				if l.ListValues().IsNull(int(j)) {
					out = append(out, nil)
					continue
				}
				v, err := elem(l.ListValues(), int(j))
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
			return out, nil
		}, "array", nil
	}
	return nil, "", fmt.Errorf("mongodb: column %q has unsupported Arrow type %s", f.Name, f.Type)
}

// decimalValue is a decimal as Decimal128, or as text past its 34 digits.
func decimalValue(unscaled *big.Int, scale int32) (any, error) {
	if len(new(big.Int).Abs(unscaled).String()) <= 34 {
		if d, ok := bson.ParseDecimal128FromBigInt(unscaled, -int(scale)); ok {
			return d, nil
		}
	}
	r := new(big.Rat).SetFrac(unscaled, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
	return r.FloatString(int(scale)), nil
}

// parseJSON reads JSON text as relaxed Extended JSON; text that isn't
// Extended JSON is kept as a string.
func parseJSON(s string) (any, error) {
	var doc bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(`{"v":`+s+`}`), false, &doc); err != nil {
		if !json.Valid([]byte(s)) {
			return nil, fmt.Errorf("mongodb: a JSON value isn't JSON: %w", err)
		}
		return s, nil
	}
	return doc.Lookup("v"), nil
}

// bsonKey reads a key as the MongoDB source spells _id (keyText): 24 hex
// digits as an ObjectId, and anything else as canonical Extended JSON.
func bsonKey(s string) (any, error) {
	if len(s) == 24 {
		if id, err := bson.ObjectIDFromHex(s); err == nil && id.Hex() == s {
			return id, nil
		}
	}
	var doc bson.Raw
	err := bson.UnmarshalExtJSON([]byte(`{"v":`+s+`}`), true, &doc)
	// Only keyText's spelling: the parser takes 5 for {"$numberInt":"5"}.
	if err == nil && keyText(doc.Lookup("v")) != s {
		err = errors.New("not as the source spells it")
	}
	if err != nil {
		return nil, fmt.Errorf("mongodb: key %q isn't an ObjectId's hex or canonical Extended JSON: %w", s, err)
	}
	return doc.Lookup("v"), nil
}

func storage(a arrow.Array) arrow.Array {
	if e, ok := a.(array.ExtensionArray); ok {
		return e.Storage()
	}
	return a
}

func isString(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return true
	}
	return false
}

func str(a arrow.Array, i int) string {
	return storage(a).(interface{ Value(int) string }).Value(i)
}

// targetCollection is the collection the engine named, or for batches that
// didn't pass through the engine, the stream's name normalized.
func targetCollection(schema *arrow.Schema, stream string) string {
	if t, ok := schema.Metadata().GetValue(tableKey); ok {
		return t
	}
	var b strings.Builder
	for _, r := range strings.ToLower(stream) {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	s := b.String()
	if s == "" || (s[0] >= '0' && s[0] <= '9') {
		s = "_" + s
	}
	return s
}
