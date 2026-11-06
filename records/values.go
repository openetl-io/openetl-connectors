package records

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/google/uuid"
)

const (
	ExtensionNameKey = "ARROW:extension:name"
	JSONExtension    = "arrow.json"
	UUIDExtension    = "arrow.uuid"
	// The engine names each stream's table in its schema's metadata.
	TableKey = "openetl.table"
)

type kind int

const (
	kNull kind = iota
	kBool
	kInt32
	kInt64
	kUint64
	kFloat32
	kFloat64
	kString
	kBytes
	kDecimal
	kDate
	kTime
	kTimestamp
	kUUID
	kJSON
	kList
	kStruct
	kInterval
)

// Col reads one column's values as Go values, which each format encodes:
// bool, int32, int64, uint64, float32, float64, string, []byte, decimal,
// time.Time (dates and timestamps), time.Duration (times of day), uuid as
// a string, json.RawMessage, []any, []any for a struct's fields in order,
// and interval.
type Col struct {
	name  string
	kind  kind
	scale int32
	prec  int32
	// Timestamps: in UTC rather than naive, and in nanoseconds.
	zoned, nanos bool
	elem         *Col
	fields       []Col
	value        func(a arrow.Array, i int) (any, error)
}

type decimal struct {
	num   decimal128.Num
	scale int32
}

func (d decimal) String() string { return d.num.ToString(d.scale) }

func (d decimal) Rat() *big.Rat {
	return new(big.Rat).SetFrac(d.num.BigInt(), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.scale)), nil))
}

type interval struct {
	Months, Days int32
	Nanoseconds  int64
}

func storage(a arrow.Array) arrow.Array {
	if e, ok := a.(array.ExtensionArray); ok {
		return e.Storage()
	}
	return a
}

func Cols(sc *arrow.Schema) ([]Col, error) {
	cols := make([]Col, sc.NumFields())
	for i, f := range sc.Fields() {
		c, err := colOf(f)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", f.Name, err)
		}
		cols[i] = c
	}
	return cols, nil
}

func colOf(f arrow.Field) (Col, error) {
	c := Col{name: f.Name}
	dt := f.Type
	ext := ""
	if et, ok := dt.(arrow.ExtensionType); ok {
		ext, dt = et.ExtensionName(), et.StorageType()
	} else if v, ok := f.Metadata.GetValue(ExtensionNameKey); ok {
		ext = v
	}
	switch {
	case ext == JSONExtension && isString(dt):
		c.kind = kJSON
		c.value = func(a arrow.Array, i int) (any, error) { return json.RawMessage(str(a, i)), nil }
		return c, nil
	case ext == UUIDExtension && dt.ID() == arrow.FIXED_SIZE_BINARY:
		c.kind = kUUID
		c.value = func(a arrow.Array, i int) (any, error) {
			u, err := uuid.FromBytes(storage(a).(*array.FixedSizeBinary).Value(i))
			return u.String(), err
		}
		return c, nil
	}
	switch t := dt.(type) {
	case *arrow.NullType:
		c.kind = kNull
		c.value = func(arrow.Array, int) (any, error) { return nil, nil }
	case *arrow.BooleanType:
		c.kind = kBool
		c.value = func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Boolean).Value(i), nil }
	case *arrow.Int8Type:
		c.kind = kInt32
		c.value = func(a arrow.Array, i int) (any, error) { return int32(storage(a).(*array.Int8).Value(i)), nil }
	case *arrow.Int16Type:
		c.kind = kInt32
		c.value = func(a arrow.Array, i int) (any, error) { return int32(storage(a).(*array.Int16).Value(i)), nil }
	case *arrow.Int32Type:
		c.kind = kInt32
		c.value = func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Int32).Value(i), nil }
	case *arrow.Int64Type:
		c.kind = kInt64
		c.value = func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Int64).Value(i), nil }
	case *arrow.Uint8Type:
		c.kind = kInt32
		c.value = func(a arrow.Array, i int) (any, error) { return int32(storage(a).(*array.Uint8).Value(i)), nil }
	case *arrow.Uint16Type:
		c.kind = kInt32
		c.value = func(a arrow.Array, i int) (any, error) { return int32(storage(a).(*array.Uint16).Value(i)), nil }
	case *arrow.Uint32Type:
		c.kind = kInt64
		c.value = func(a arrow.Array, i int) (any, error) { return int64(storage(a).(*array.Uint32).Value(i)), nil }
	case *arrow.Uint64Type:
		c.kind = kUint64
		c.value = func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Uint64).Value(i), nil }
	case *arrow.Float16Type:
		c.kind = kFloat32
		c.value = func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Float16).Value(i).Float32(), nil }
	case *arrow.Float32Type:
		c.kind = kFloat32
		c.value = func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Float32).Value(i), nil }
	case *arrow.Float64Type:
		c.kind = kFloat64
		c.value = func(a arrow.Array, i int) (any, error) { return storage(a).(*array.Float64).Value(i), nil }
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		c.kind = kString
		c.value = func(a arrow.Array, i int) (any, error) { return str(a, i), nil }
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		c.kind = kBytes
		c.value = func(a arrow.Array, i int) (any, error) {
			return append([]byte(nil), storage(a).(interface{ Value(int) []byte }).Value(i)...), nil
		}
	case *arrow.Decimal128Type:
		c.kind, c.prec, c.scale = kDecimal, t.Precision, t.Scale
		c.value = func(a arrow.Array, i int) (any, error) {
			return decimal{storage(a).(*array.Decimal128).Value(i), t.Scale}, nil
		}
	case *arrow.Date32Type:
		c.kind = kDate
		c.value = func(a arrow.Array, i int) (any, error) {
			return storage(a).(*array.Date32).Value(i).ToTime(), nil
		}
	case *arrow.Date64Type:
		c.kind = kDate
		c.value = func(a arrow.Array, i int) (any, error) {
			return storage(a).(*array.Date64).Value(i).ToTime(), nil
		}
	case *arrow.Time32Type:
		c.kind = kTime
		c.value = func(a arrow.Array, i int) (any, error) {
			return time.Duration(storage(a).(*array.Time32).Value(i)) * t.Unit.Multiplier(), nil
		}
	case *arrow.Time64Type:
		c.kind = kTime
		c.value = func(a arrow.Array, i int) (any, error) {
			return time.Duration(storage(a).(*array.Time64).Value(i)) * t.Unit.Multiplier(), nil
		}
	case *arrow.TimestampType:
		c.kind, c.zoned, c.nanos = kTimestamp, t.TimeZone != "", t.Unit == arrow.Nanosecond
		toTime, err := t.GetToTimeFunc()
		if err != nil {
			return c, err
		}
		c.value = func(a arrow.Array, i int) (any, error) {
			return toTime(storage(a).(*array.Timestamp).Value(i)).UTC(), nil
		}
	case *arrow.MonthDayNanoIntervalType:
		c.kind = kInterval
		c.value = func(a arrow.Array, i int) (any, error) {
			v := storage(a).(*array.MonthDayNanoInterval).Value(i)
			return interval{v.Months, v.Days, v.Nanoseconds}, nil
		}
	case *arrow.ListType, *arrow.LargeListType:
		ef := t.(arrow.ListLikeType).ElemField()
		e, err := colOf(ef)
		if err != nil {
			return c, err
		}
		c.kind, c.elem = kList, &e
		c.value = func(a arrow.Array, i int) (any, error) {
			l := storage(a).(array.ListLike)
			start, end := l.ValueOffsets(i)
			vals := l.ListValues()
			out := make([]any, 0, end-start)
			for j := int(start); j < int(end); j++ {
				v, err := e.At(vals, j)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
			return out, nil
		}
	case *arrow.StructType:
		c.kind = kStruct
		for _, sf := range t.Fields() {
			fc, err := colOf(sf)
			if err != nil {
				return c, err
			}
			c.fields = append(c.fields, fc)
		}
		c.value = func(a arrow.Array, i int) (any, error) {
			s := storage(a).(*array.Struct)
			out := make([]any, len(c.fields))
			for j := range c.fields {
				v, err := c.fields[j].At(s.Field(j), i)
				if err != nil {
					return nil, err
				}
				out[j] = v
			}
			return out, nil
		}
	default:
		return c, fmt.Errorf("type %s isn't supported", dt)
	}
	return c, nil
}

// At is row i's value, nil for null.
func (c *Col) At(a arrow.Array, i int) (any, error) {
	if a.IsNull(i) {
		return nil, nil
	}
	return c.value(a, i)
}

func isString(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return true
	}
	return false
}

func str(a arrow.Array, i int) string { return storage(a).(interface{ Value(int) string }).Value(i) }

// appendJSON appends v, a value of c, as JSON.
func (c *Col) appendJSON(b []byte, v any) ([]byte, error) {
	if v == nil {
		return append(b, "null"...), nil
	}
	switch c.kind {
	case kBool:
		return strconv.AppendBool(b, v.(bool)), nil
	case kInt32:
		return strconv.AppendInt(b, int64(v.(int32)), 10), nil
	case kInt64:
		return strconv.AppendInt(b, v.(int64), 10), nil
	case kUint64:
		return strconv.AppendUint(b, v.(uint64), 10), nil
	case kFloat32:
		return appendFloat(b, float64(v.(float32)), 32), nil
	case kFloat64:
		return appendFloat(b, v.(float64), 64), nil
	case kString, kUUID:
		return AppendJSONString(b, v.(string)), nil
	case kBytes:
		return AppendJSONString(b, base64.StdEncoding.EncodeToString(v.([]byte))), nil
	case kDecimal:
		return append(b, v.(decimal).String()...), nil
	case kDate:
		return AppendJSONString(b, v.(time.Time).Format(time.DateOnly)), nil
	case kTime:
		return AppendJSONString(b, timeOfDay(v.(time.Duration))), nil
	case kTimestamp:
		return AppendJSONString(b, timestampText(v.(time.Time), c.zoned)), nil
	case kJSON:
		raw := v.(json.RawMessage)
		if !json.Valid(raw) {
			return nil, fmt.Errorf("column %q: invalid JSON", c.name)
		}
		return append(b, raw...), nil
	case kInterval:
		iv := v.(interval)
		return fmt.Appendf(b, `{"months":%d,"days":%d,"nanoseconds":%d}`, iv.Months, iv.Days, iv.Nanoseconds), nil
	case kList:
		b = append(b, '[')
		for j, e := range v.([]any) {
			if j > 0 {
				b = append(b, ',')
			}
			var err error
			if b, err = c.elem.appendJSON(b, e); err != nil {
				return nil, err
			}
		}
		return append(b, ']'), nil
	case kStruct:
		return appendJSONObject(b, c.fields, v.([]any))
	}
	return append(b, "null"...), nil
}

func appendJSONObject(b []byte, cols []Col, vals []any) ([]byte, error) {
	b = append(b, '{')
	for j := range cols {
		if j > 0 {
			b = append(b, ',')
		}
		b = AppendJSONString(b, cols[j].name)
		b = append(b, ':')
		var err error
		if b, err = cols[j].appendJSON(b, vals[j]); err != nil {
			return nil, err
		}
	}
	return append(b, '}'), nil
}

// NaN and the infinities as Protobuf's JSON spells them, since JSON has no
// numbers for them.
func appendFloat(b []byte, f float64, bits int) []byte {
	switch {
	case math.IsNaN(f):
		return append(b, `"NaN"`...)
	case math.IsInf(f, 1):
		return append(b, `"Infinity"`...)
	case math.IsInf(f, -1):
		return append(b, `"-Infinity"`...)
	}
	return strconv.AppendFloat(b, f, 'g', -1, bits)
}

func AppendJSONString(b []byte, s string) []byte {
	const hex = "0123456789abcdef"
	b = append(b, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\':
				b = append(b, '\\', c)
			case c == '\n':
				b = append(b, '\\', 'n')
			case c == '\r':
				b = append(b, '\\', 'r')
			case c == '\t':
				b = append(b, '\\', 't')
			case c < 0x20:
				b = append(b, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			default:
				b = append(b, c)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = append(b, `�`...)
		} else {
			b = append(b, s[i:i+size]...)
		}
		i += size
	}
	return append(b, '"')
}

func timeOfDay(d time.Duration) string {
	return time.Unix(0, int64(d)).UTC().Format("15:04:05.999999999")
}

func timestampText(t time.Time, zoned bool) string {
	if zoned {
		return t.Format(time.RFC3339Nano)
	}
	return t.Format("2006-01-02T15:04:05.999999999")
}
