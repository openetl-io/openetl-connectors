package objstore

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl-connectors/records"
)

// The columns the source adds to every record.
const (
	fileColumn     = "_file"
	lineColumn     = "_line"
	modifiedColumn = "_modified"
)

var (
	utcMicros   = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	localMicros = &arrow.TimestampType{Unit: arrow.Microsecond}
	jsonMeta    = arrow.NewMetadata([]string{records.ExtensionNameKey, "ARROW:extension:metadata"}, []string{records.JSONExtension, ""})
)

func controlColumn(name string) bool {
	return name == fileColumn || name == lineColumn || name == modifiedColumn
}

func controlFields() []arrow.Field {
	return []arrow.Field{
		{Name: fileColumn, Type: arrow.BinaryTypes.String},
		{Name: lineColumn, Type: arrow.PrimitiveTypes.Int64},
		{Name: modifiedColumn, Type: utcMicros},
	}
}

var decimalType = regexp.MustCompile(`^decimal\((\d+),\s*(\d+)\)$`)

// declaredField is a column of the type a stream's columns setting names.
func declaredField(name, typ string) (arrow.Field, error) {
	f := arrow.Field{Name: name, Nullable: true}
	switch typ {
	case "string":
		f.Type = arrow.BinaryTypes.String
	case "integer":
		f.Type = arrow.PrimitiveTypes.Int64
	case "number":
		f.Type = arrow.PrimitiveTypes.Float64
	case "boolean":
		f.Type = arrow.FixedWidthTypes.Boolean
	case "date":
		f.Type = arrow.FixedWidthTypes.Date32
	case "time":
		f.Type = arrow.FixedWidthTypes.Time64us
	case "timestamp":
		f.Type = localMicros
	case "timestamp_tz":
		f.Type = utcMicros
	case "json":
		f.Type, f.Metadata = arrow.BinaryTypes.String, jsonMeta
	case "binary":
		f.Type = arrow.BinaryTypes.Binary
	default:
		m := decimalType.FindStringSubmatch(typ)
		if m == nil {
			return f, fmt.Errorf("column %q: type %q is none of string, integer, number, boolean, date, time, timestamp, timestamp_tz, json, binary and decimal(P,S)", name, typ)
		}
		p, _ := strconv.Atoi(m[1])
		s, _ := strconv.Atoi(m[2])
		if p < 1 || p > 38 || s > p {
			return f, fmt.Errorf("column %q: %s needs a precision of 1 to 38, and a scale no more than it", name, typ)
		}
		f.Type = &arrow.Decimal128Type{Precision: int32(p), Scale: int32(s)}
	}
	return f, nil
}

// conversion is how a Parquet column's values become the type it's
// declared with.
type conversion int

const (
	keep conversion = iota
	cast
	// As JSON text, a struct's or a list's.
	asJSON
	// As text, a type the platform has no logical type for.
	asText
	// A timestamp in microseconds, floored, and in UTC.
	micros
	// Binary JSON text as JSON, which must be UTF-8.
	jsonText
)

// logical is the logical type a Parquet file's column is declared with,
// and how its values become it.
func logical(f arrow.Field) (arrow.Field, conversion) {
	out := arrow.Field{Name: f.Name, Nullable: true, Type: f.Type}
	if v, _ := f.Metadata.GetValue(records.ExtensionNameKey); v == records.JSONExtension && f.Type.ID() == arrow.BINARY {
		out.Type, out.Metadata = arrow.BinaryTypes.String, jsonMeta
		return out, jsonText
	}
	if et, ok := f.Type.(arrow.ExtensionType); ok {
		switch et.ExtensionName() {
		case records.UUIDExtension, records.JSONExtension:
			return out, keep
		}
		f.Type = et.StorageType()
		out.Type = f.Type
	}
	to := func(t arrow.DataType) (arrow.Field, conversion) {
		out.Type = t
		return out, cast
	}
	switch t := f.Type.(type) {
	case *arrow.BooleanType, *arrow.Int64Type, *arrow.Float64Type, *arrow.StringType, *arrow.BinaryType, *arrow.Date32Type, *arrow.MonthDayNanoIntervalType:
		return out, keep
	// Integers are int64, and floats float64, so files with columns of
	// different widths agree.
	case *arrow.Int8Type, *arrow.Uint8Type, *arrow.Int16Type, *arrow.Uint16Type, *arrow.Int32Type, *arrow.Uint32Type:
		return to(arrow.PrimitiveTypes.Int64)
	case *arrow.Float16Type, *arrow.Float32Type:
		return to(arrow.PrimitiveTypes.Float64)
	case *arrow.Uint64Type:
		return to(&arrow.Decimal128Type{Precision: 20})
	case *arrow.LargeStringType, *arrow.StringViewType:
		return to(arrow.BinaryTypes.String)
	case *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		return to(arrow.BinaryTypes.Binary)
	case *arrow.TimestampType:
		out.Type = localMicros
		if t.TimeZone != "" {
			out.Type = utcMicros
		}
		if arrow.TypeEqual(out.Type, t) {
			return out, keep
		}
		return out, micros
	case *arrow.Date64Type:
		return to(arrow.FixedWidthTypes.Date32)
	case *arrow.Time32Type:
		return to(arrow.FixedWidthTypes.Time64us)
	case *arrow.Time64Type:
		if t.Unit == arrow.Microsecond {
			return out, keep
		}
		return to(arrow.FixedWidthTypes.Time64us)
	case *arrow.Decimal128Type:
		if logicalDecimal(t.Precision, t.Scale) {
			return out, keep
		}
	case *arrow.Decimal32Type:
		if logicalDecimal(t.Precision, t.Scale) {
			return to(&arrow.Decimal128Type{Precision: t.Precision, Scale: t.Scale})
		}
	case *arrow.Decimal64Type:
		if logicalDecimal(t.Precision, t.Scale) {
			return to(&arrow.Decimal128Type{Precision: t.Precision, Scale: t.Scale})
		}
	case *arrow.Decimal256Type:
		if logicalDecimal(t.Precision, t.Scale) {
			return to(&arrow.Decimal128Type{Precision: t.Precision, Scale: t.Scale})
		}
	case *arrow.DictionaryType:
		v, c := logical(arrow.Field{Name: f.Name, Type: t.ValueType})
		if c == keep || c == cast {
			return to(v.Type)
		}
	case *arrow.ListType:
		if e, c := logical(t.ElemField()); c == keep {
			out.Type = arrow.ListOfField(e)
			if arrow.TypeEqual(out.Type, f.Type) {
				return out, keep
			}
			return out, cast
		}
		out.Type, out.Metadata = arrow.BinaryTypes.String, jsonMeta
		return out, asJSON
	case *arrow.StructType, *arrow.LargeListType, *arrow.FixedSizeListType:
		out.Type, out.Metadata = arrow.BinaryTypes.String, jsonMeta
		return out, asJSON
	}
	out.Type = arrow.BinaryTypes.String
	return out, asText
}

// logicalDecimal says whether a decimal of precision p and scale s is a
// logical type: one of up to 38 digits, with a scale from 0 to p. Others are
// text.
func logicalDecimal(p, s int32) bool { return p >= 1 && p <= 38 && s >= 0 && s <= p }

// convert makes a column of a Parquet file's batch its declared type.
func convert(ctx context.Context, a arrow.Array, f arrow.Field, c conversion) (arrow.Array, error) {
	switch c {
	case keep:
		a.Retain()
		return a, nil
	case cast:
		if d, ok := a.DataType().(*arrow.DictionaryType); ok {
			plain, err := compute.CastArray(ctx, a, compute.UnsafeCastOptions(d.ValueType))
			if err != nil {
				return nil, err
			}
			defer plain.Release()
			if arrow.TypeEqual(plain.DataType(), f.Type) {
				plain.Retain()
				return plain, nil
			}
			a = plain
		}
		return compute.CastArray(ctx, a, compute.UnsafeCastOptions(f.Type))
	}
	if c == micros {
		return toMicros(a, f.Type.(*arrow.TimestampType))
	}
	if c == jsonText {
		return compute.CastArray(ctx, a, compute.SafeCastOptions(arrow.BinaryTypes.String))
	}
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	var enc records.Encoder
	var col records.Col
	if c == asJSON {
		cols, err := records.Cols(arrow.NewSchema([]arrow.Field{{Name: "v", Type: a.DataType()}}, nil))
		if err != nil {
			c = asText
		} else {
			col = cols[0]
			if enc, err = records.NewEncoder(ctx, records.JSON, nil, "", "", cols, false); err != nil {
				return nil, err
			}
		}
	}
	for i := range a.Len() {
		if a.IsNull(i) {
			b.AppendNull()
			continue
		}
		if c == asText {
			b.Append(a.ValueStr(i))
			continue
		}
		v, err := col.At(a, i)
		if err != nil {
			return nil, err
		}
		obj, err := enc.Encode([]any{v})
		if err != nil {
			return nil, err
		}
		// {"v":VALUE}
		b.Append(string(obj[5 : len(obj)-1]))
	}
	return b.NewArray(), nil
}

// inferred is what a text column's sampled values say its type is.
type inferred struct {
	seen                                         bool
	notBool, notInt, notFloat, notDate, notStamp bool
	zoned, local                                 bool
	// JSON Lines: values that are objects or arrays, and other values.
	nested, flat bool
}

var (
	dateText  = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	stampText = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|z|[+-]\d{2}(:?\d{2})?)?$`)
	zoneText  = regexp.MustCompile(`(Z|z|[+-]\d{2}(:?\d{2})?)$`)
)

// text adds a CSV value, or a JSON string's, to what the column has seen.
func (in *inferred) text(s string) {
	in.seen, in.flat = true, true
	if s != "true" && s != "false" {
		in.notBool = true
	}
	if _, err := strconv.ParseInt(s, 10, 64); err != nil {
		in.notInt = true
	}
	if !exactFloat(s) || strings.ContainsAny(s, "xXpP_") {
		in.notFloat = true
	}
	if _, err := time.Parse(time.DateOnly, s); err != nil || !dateText.MatchString(s) {
		in.notDate = true
	}
	if !stampText.MatchString(s) {
		in.notStamp = true
	} else if zoneText.MatchString(s[10:]) {
		in.zoned = true
	} else {
		in.local = true
	}
}

// json adds a JSON Lines value to what the column has seen. Strings are
// text, whatever they hold.
func (in *inferred) json(raw json.RawMessage) {
	switch {
	case len(raw) == 0 || string(raw) == "null":
		return
	case raw[0] == '{' || raw[0] == '[':
		in.seen, in.nested = true, true
		return
	}
	in.seen, in.flat = true, true
	in.notDate, in.notStamp = true, true
	switch {
	case raw[0] == '"':
		in.notBool, in.notInt, in.notFloat = true, true, true
	case string(raw) == "true" || string(raw) == "false":
		in.notInt, in.notFloat = true, true
	default:
		in.notBool = true
		if _, err := strconv.ParseInt(string(raw), 10, 64); err != nil {
			in.notInt = true
		}
		if !exactFloat(string(raw)) {
			in.notFloat = true
		}
	}
}

// exactFloat: s is a number that a 64-bit float holds as s says it, to the
// digits the float's shortest text has, or NaN or an infinity.
func exactFloat(s string) bool {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return false
	}
	want, ok := new(big.Rat).SetString(s)
	if !ok {
		return true
	}
	got, _ := new(big.Rat).SetString(strconv.FormatFloat(f, 'g', -1, 64))
	return want.Cmp(got) == 0
}

// field is the column's inferred type: text for one with no values.
func (in *inferred) field(name string) arrow.Field {
	f := arrow.Field{Name: name, Nullable: true, Type: arrow.BinaryTypes.String}
	switch {
	case !in.seen:
	case in.nested && !in.flat:
		f.Metadata = jsonMeta
	case in.nested:
	case !in.notBool:
		f.Type = arrow.FixedWidthTypes.Boolean
	case !in.notInt:
		f.Type = arrow.PrimitiveTypes.Int64
	case !in.notFloat:
		f.Type = arrow.PrimitiveTypes.Float64
	case !in.notDate:
		f.Type = arrow.FixedWidthTypes.Date32
	case !in.notStamp && in.zoned && !in.local:
		f.Type = utcMicros
	case !in.notStamp && in.local && !in.zoned:
		f.Type = localMicros
	}
	return f
}

// toMicros is a timestamp column in microseconds, floored where it's finer.
func toMicros(a arrow.Array, to *arrow.TimestampType) (arrow.Array, error) {
	ts := a.(*array.Timestamp)
	per := map[arrow.TimeUnit]int64{arrow.Second: 1, arrow.Millisecond: 1e3, arrow.Microsecond: 1e6, arrow.Nanosecond: 1e9}[ts.DataType().(*arrow.TimestampType).Unit]
	b := array.NewTimestampBuilder(memory.DefaultAllocator, to)
	defer b.Release()
	b.Reserve(ts.Len())
	for i := range ts.Len() {
		if ts.IsNull(i) {
			b.AppendNull()
			continue
		}
		v := int64(ts.Value(i))
		switch {
		case per > 1e6:
			q := v / (per / 1e6)
			if v%(per/1e6) < 0 {
				q--
			}
			v = q
		case per < 1e6:
			m := 1e6 / per
			if v > math.MaxInt64/m || v < math.MinInt64/m {
				return nil, fmt.Errorf("row %d: %d is out of range for microseconds", i, v)
			}
			v *= m
		}
		b.Append(arrow.Timestamp(v))
	}
	return b.NewArray(), nil
}
