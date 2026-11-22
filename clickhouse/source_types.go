package clickhouse

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// chType is a ClickHouse type, parsed: Array(Nullable(Int64)) is Array with
// one argument, Nullable with one, Int64.
type chType struct {
	name string
	// Its arguments, as written.
	args []string
}

// parseType splits a type into its name and top-level arguments.
func parseType(s string) chType {
	s = strings.TrimSpace(s)
	i := strings.IndexByte(s, '(')
	if i < 0 || !strings.HasSuffix(s, ")") {
		return chType{name: s}
	}
	t := chType{name: s[:i]}
	depth, quoted, start := 0, false, i+1
	for j := i + 1; j < len(s)-1; j++ {
		switch c := s[j]; {
		case quoted && c == '\\':
			j++
		case c == '\'':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			t.args = append(t.args, strings.TrimSpace(s[start:j]))
			start = j + 1
		}
	}
	t.args = append(t.args, strings.TrimSpace(s[start:len(s)-1]))
	return t
}

func (t chType) arg(i int) chType { return parseType(t.args[i]) }

// unwrap is the type a value of t is: past Nullable, LowCardinality and
// SimpleAggregateFunction; and whether it can be null.
func (t chType) unwrap() (chType, bool) {
	null := false
	for {
		switch {
		case t.name == "Nullable" && len(t.args) == 1:
			t, null = t.arg(0), true
		case t.name == "LowCardinality" && len(t.args) == 1:
			t = t.arg(0)
		case t.name == "SimpleAggregateFunction" && len(t.args) == 2:
			t = t.arg(1)
		default:
			return t, null
		}
	}
}

// errOpaque is a type the source reads as JSON text: it has no Arrow type.
var errOpaque = errors.New("read as JSON")

var utcMicros = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}

// arrowType is the logical type a column of t is declared as, which the
// engine takes. errOpaque for a type read as its JSON text, which may be
// anywhere within t: maps, tuples, and in an array, a time of day, whose
// values can be past what the engine's time holds.
func arrowType(t chType) (arrow.DataType, error) { return logicalType(t, false) }

func logicalType(t chType, inArray bool) (arrow.DataType, error) {
	t, _ = t.unwrap()
	switch t.name {
	case "Bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "Int8", "UInt8", "Int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "UInt16", "Int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "UInt32", "Int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "UInt64":
		return uint64Type, nil
	case "Int128", "UInt128", "Int256", "UInt256":
		// Up to 78 digits, past what a decimal holds: their decimal text.
		return arrow.BinaryTypes.String, nil
	case "Float32", "BFloat16":
		return arrow.PrimitiveTypes.Float32, nil
	case "Float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256":
		p, s, err := decimalArgs(t)
		if err != nil {
			return nil, err
		}
		if p <= 38 {
			return &arrow.Decimal128Type{Precision: p, Scale: s}, nil
		}
		return arrow.BinaryTypes.String, nil
	case "String", "Enum8", "Enum16", "IPv4", "IPv6":
		return arrow.BinaryTypes.String, nil
	case "FixedString":
		return arrow.BinaryTypes.Binary, nil
	case "UUID":
		return extensions.NewUUIDType(), nil
	case "Date", "Date32":
		return arrow.FixedWidthTypes.Date32, nil
	case "DateTime":
		return utcMicros, nil
	case "DateTime64":
		p, err := strconv.Atoi(t.args[0])
		if err != nil {
			return nil, fmt.Errorf("type DateTime64(%s): %w", t.args[0], err)
		}
		if p > 6 && inArray {
			// JSON keeps a list's nanoseconds; a column's are text (sentAsText).
			return nil, errOpaque
		}
		return utcMicros, nil
	case "Time", "Time64":
		if inArray {
			return nil, errOpaque
		}
		// Time64's digits past the sixth are cut: the engine's times are in
		// microseconds.
		return arrow.FixedWidthTypes.Time64us, nil
	case "Array":
		elem, err := logicalType(t.arg(0), true)
		if err != nil {
			return nil, err
		}
		return arrow.ListOf(elem), nil
	}
	return nil, errOpaque
}

// uint64Type holds every UInt64.
var uint64Type = &arrow.Decimal128Type{Precision: 20, Scale: 0}

// sentAsText says whether a column of t is sent as text, which the engine
// parses into its declared type: a DateTime64 of more than 6 digits, whose
// nanoseconds a stream that keeps them keeps, and whose years past 2262
// nanoseconds since 1970 don't reach.
func sentAsText(t chType) bool {
	t, _ = t.unwrap()
	if t.name != "DateTime64" {
		return false
	}
	p, err := strconv.Atoi(t.args[0])
	return err == nil && p > 6
}

func decimalArgs(t chType) (int32, int32, error) {
	var p, s int
	var err error
	switch t.name {
	case "Decimal":
		if len(t.args) != 2 {
			return 0, 0, fmt.Errorf("type Decimal(%s)", strings.Join(t.args, ", "))
		}
		if p, err = strconv.Atoi(t.args[0]); err == nil {
			s, err = strconv.Atoi(t.args[1])
		}
	default:
		p = map[string]int{"Decimal32": 9, "Decimal64": 18, "Decimal128": 38, "Decimal256": 76}[t.name]
		s, err = strconv.Atoi(t.args[0])
	}
	if err != nil || p < 1 || p > 76 || s < 0 || s > p {
		return 0, 0, fmt.Errorf("type %s(%s)", t.name, strings.Join(t.args, ", "))
	}
	return int32(p), int32(s), nil
}

// errPast is a time of day past the engine's, which ends at 24:00.
var errPast = errors.New("past what a time of day holds")

// appendValue appends a value the driver scanned, a pointer to one, or nil,
// to b. It fails with errPast before appending anything if the value is past
// what b's type holds.
func appendValue(b array.Builder, v any) error {
	rv := deref(reflect.ValueOf(v))
	if !rv.IsValid() {
		b.AppendNull()
		return nil
	}
	if err := fits(b.Type(), rv); err != nil {
		return err
	}
	return add(b, rv)
}

// deref follows pointers and interfaces to a value, but for *big.Int's,
// which the driver scans 128- and 256-bit integers into. It returns the
// zero Value for nil.
func deref(v reflect.Value) reflect.Value {
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return reflect.Value{}
		}
		if v.Type() == bigIntType {
			break
		}
		v = v.Elem()
	}
	return v
}

var bigIntType = reflect.TypeFor[*big.Int]()

// fits checks a value against what dt holds: a ClickHouse Time can be
// negative or past a day.
func fits(dt arrow.DataType, v reflect.Value) error {
	if v = deref(v); !v.IsValid() {
		return nil
	}
	if _, ok := dt.(*arrow.Time64Type); ok {
		if d, ok := v.Interface().(time.Duration); ok && (d < 0 || d >= 24*time.Hour) {
			return errPast
		}
	}
	return nil
}

// scaled is a decimal or integer value as an integer of scale's places.
func scaled(v any, scale int32) (*big.Int, error) {
	switch n := v.(type) {
	case decimal.Decimal:
		return n.Shift(scale).BigInt(), nil
	case uint64:
		return scaled(new(big.Int).SetUint64(n), scale)
	case *big.Int:
		return new(big.Int).Mul(n, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)), nil
	case big.Int:
		return scaled(&n, scale)
	}
	return nil, fmt.Errorf("a %T as a decimal", v)
}

// integer is a signed or unsigned integer's value. arrowType gives each
// ClickHouse integer a column wide enough for it, and UInt64 a decimal.
func integer(v reflect.Value) (int64, error) {
	if !v.CanUint() {
		return v.Int(), nil
	}
	if u := v.Uint(); u <= math.MaxInt64 {
		return int64(u), nil
	}
	return 0, fmt.Errorf("%d past an int64", v.Uint())
}

func add(b array.Builder, v reflect.Value) error {
	if v = deref(v); !v.IsValid() {
		b.AppendNull()
		return nil
	}
	x := v.Interface()
	switch b := b.(type) {
	case *array.BooleanBuilder:
		b.Append(v.Bool())
	case *array.Int16Builder, *array.Int32Builder, *array.Int64Builder:
		n, err := integer(v)
		if err != nil {
			return err
		}
		switch b := b.(type) {
		case *array.Int16Builder:
			b.Append(int16(n))
		case *array.Int32Builder:
			b.Append(int32(n))
		case *array.Int64Builder:
			b.Append(n)
		}
	case *array.Float32Builder:
		b.Append(float32(v.Float()))
	case *array.Float64Builder:
		b.Append(v.Float())
	case *array.StringBuilder:
		b.Append(text(x))
	case *array.BinaryBuilder:
		b.Append([]byte(v.String()))
	case *array.Decimal128Builder:
		n, err := scaled(x, b.Type().(*arrow.Decimal128Type).Scale)
		if err != nil {
			return err
		}
		b.Append(decimal128.FromBigInt(n))
	case *array.Date32Builder:
		b.Append(arrow.Date32FromTime(x.(time.Time)))
	case *array.TimestampBuilder:
		if n, ok := x.(int64); ok {
			b.Append(arrow.Timestamp(n))
			break
		}
		ts, err := arrow.TimestampFromTime(x.(time.Time), b.Type().(*arrow.TimestampType).Unit)
		if err != nil {
			return err
		}
		b.Append(ts)
	case *array.Time64Builder:
		b.Append(arrow.Time64(x.(time.Duration) / time.Microsecond))
	case *extensions.UUIDBuilder:
		b.Append(x.(uuid.UUID))
	case *array.ListBuilder:
		b.Append(true)
		for i := range v.Len() {
			if err := add(b.ValueBuilder(), v.Index(i)); err != nil {
				return err
			}
		}
	case *array.ExtensionBuilder:
		return add(b.StorageBuilder(), v)
	default:
		return fmt.Errorf("no way to build %s from a %T", b.Type(), x)
	}
	return nil
}

// text is a value as an Arrow string: valid UTF-8, with U+FFFD for bytes
// that aren't.
func text(x any) string {
	var s string
	switch v := x.(type) {
	case string:
		s = v
	case net.IP:
		// An IPv6 column's, which String spells as IPv4 where it's mapped.
		if len(v) == net.IPv6len {
			s = netip.AddrFrom16([16]byte(v)).String()
		} else {
			s = v.String()
		}
	case fmt.Stringer:
		s = v.String()
	default:
		s = fmt.Sprint(v)
	}
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	return s
}
