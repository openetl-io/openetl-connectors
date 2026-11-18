package dynamodb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)

const (
	extensionNameKey = "ARROW:extension:name"
	jsonExtension    = "arrow.json"
	uuidExtension    = "arrow.uuid"
	// The engine names each stream's table in its schema's metadata.
	tableMetaKey = "openetl.table"
	// The largest item DynamoDB takes, in bytes as it counts them.
	maxItem = 400 * 1024
)

// convert is a column's value of row i, which isn't null.
type convert func(a arrow.Array, i int) (types.AttributeValue, error)

func storage(a arrow.Array) arrow.Array {
	if e, ok := a.(array.ExtensionArray); ok {
		return e.Storage()
	}
	return a
}

func extensionOf(f arrow.Field) (string, arrow.DataType) {
	if et, ok := f.Type.(arrow.ExtensionType); ok {
		return et.ExtensionName(), et.StorageType()
	}
	ext, _ := f.Metadata.GetValue(extensionNameKey)
	return ext, f.Type
}

func num(s string) types.AttributeValue { return &types.AttributeValueMemberN{Value: s} }
func str(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

// converter maps an Arrow type to an attribute type: booleans to BOOL;
// integers, and decimals and floats that a number holds, to N; floats past
// N's range of 1E-130 to 9.9E+125, NaN and infinities to S, spelled as Go
// spells them; text to S and binaries to B; UUIDs to S, as text; timestamps
// to S in RFC 3339 with up to nanoseconds, with Z where they have a time
// zone, dates to S as 2006-01-02 and times of day as 15:04:05.999999999;
// intervals to M of months, days and nanoseconds; lists to L; JSON to the
// value it spells, objects as M and arrays as L. Others go as their Arrow
// text.
func converter(f arrow.Field) convert {
	ext, dt := extensionOf(f)
	switch {
	case ext == jsonExtension:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return parseJSONValue(storage(a).ValueStr(i))
		}
	case ext == uuidExtension && dt.ID() == arrow.FIXED_SIZE_BINARY:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			u, err := uuid.FromBytes(storage(a).(*array.FixedSizeBinary).Value(i))
			return str(u.String()), err
		}
	}
	switch t := dt.(type) {
	case *arrow.NullType:
		return func(arrow.Array, int) (types.AttributeValue, error) {
			return &types.AttributeValueMemberNULL{Value: true}, nil
		}
	case *arrow.BooleanType:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return &types.AttributeValueMemberBOOL{Value: storage(a).(*array.Boolean).Value(i)}, nil
		}
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type:
		return func(a arrow.Array, i int) (types.AttributeValue, error) { return num(storage(a).ValueStr(i)), nil }
	case *arrow.Float32Type:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return float(float64(storage(a).(*array.Float32).Value(i)), 32), nil
		}
	case *arrow.Float64Type:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return float(storage(a).(*array.Float64).Value(i), 64), nil
		}
	case *arrow.Decimal128Type:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return decimal(storage(a).(*array.Decimal128).Value(i).ToString(t.Scale)), nil
		}
	case *arrow.Decimal256Type:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return decimal(storage(a).(*array.Decimal256).Value(i).ToString(t.Scale)), nil
		}
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return func(a arrow.Array, i int) (types.AttributeValue, error) { return str(storage(a).ValueStr(i)), nil }
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return &types.AttributeValueMemberB{Value: bytes.Clone(storage(a).(interface{ Value(int) []byte }).Value(i))}, nil
		}
	case *arrow.TimestampType:
		layout := "2006-01-02T15:04:05.999999999"
		if t.TimeZone != "" {
			layout = time.RFC3339Nano
		}
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return str(storage(a).(*array.Timestamp).Value(i).ToTime(t.Unit).UTC().Format(layout)), nil
		}
	case *arrow.Date32Type:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return str(storage(a).(*array.Date32).Value(i).ToTime().Format(time.DateOnly)), nil
		}
	case *arrow.Date64Type:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return str(storage(a).(*array.Date64).Value(i).ToTime().Format(time.DateOnly)), nil
		}
	case *arrow.Time32Type:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return str(storage(a).(*array.Time32).Value(i).ToTime(t.Unit).Format("15:04:05.999999999")), nil
		}
	case *arrow.Time64Type:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			return str(storage(a).(*array.Time64).Value(i).ToTime(t.Unit).Format("15:04:05.999999999")), nil
		}
	case *arrow.MonthDayNanoIntervalType:
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			v := storage(a).(*array.MonthDayNanoInterval).Value(i)
			return &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"months": num(strconv.Itoa(int(v.Months))), "days": num(strconv.Itoa(int(v.Days))), "nanoseconds": num(strconv.FormatInt(v.Nanoseconds, 10)),
			}}, nil
		}
	case arrow.ListLikeType:
		elem := converter(t.ElemField())
		return func(a arrow.Array, i int) (types.AttributeValue, error) {
			l := storage(a).(array.ListLike)
			from, to := l.ValueOffsets(i)
			vals := l.ListValues()
			out := make([]types.AttributeValue, 0, to-from)
			for j := from; j < to; j++ {
				if vals.IsNull(int(j)) {
					out = append(out, &types.AttributeValueMemberNULL{Value: true})
					continue
				}
				v, err := elem(vals, int(j))
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			}
			return &types.AttributeValueMemberL{Value: out}, nil
		}
	}
	return func(a arrow.Array, i int) (types.AttributeValue, error) { return str(a.ValueStr(i)), nil }
}

// numeric says whether s, in decimal, is a number DynamoDB holds: at most
// 38 significant digits, 0 or between 1E-130 and 9.9...E+125.
func numeric(s string) bool {
	n, err := parseNumber(s)
	if err != nil {
		return false
	}
	abs := strings.TrimPrefix(n.text, "-")
	sig := strings.Trim(strings.ReplaceAll(abs, ".", ""), "0")
	if sig == "" {
		return true
	}
	// The power of ten of the first significant digit.
	lead := n.intDigits - 1
	if n.intDigits == 0 {
		_, frac, _ := strings.Cut(abs, ".")
		lead = -(len(frac) - len(strings.TrimLeft(frac, "0")) + 1)
	}
	return len(sig) <= 38 && lead <= 125 && lead >= -130
}

func float(f float64, bits int) types.AttributeValue {
	s := strconv.FormatFloat(f, 'g', -1, bits)
	if math.IsNaN(f) || math.IsInf(f, 0) || !numeric(s) {
		return str(floatText(f))
	}
	return num(s)
}

func decimal(s string) types.AttributeValue {
	if numeric(s) {
		return num(s)
	}
	return str(s)
}

// parseJSONValue is the value JSON text spells, numbers exact.
func parseJSONValue(text string) (types.AttributeValue, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("JSON %q: %w", text, err)
	}
	return jsonAttribute(v), nil
}

func jsonAttribute(v any) types.AttributeValue {
	switch x := v.(type) {
	case nil:
		return &types.AttributeValueMemberNULL{Value: true}
	case bool:
		return &types.AttributeValueMemberBOOL{Value: x}
	case json.Number:
		return decimal(x.String())
	case string:
		return str(x)
	case []any:
		out := make([]types.AttributeValue, len(x))
		for i, e := range x {
			out[i] = jsonAttribute(e)
		}
		return &types.AttributeValueMemberL{Value: out}
	case map[string]any:
		out := make(map[string]types.AttributeValue, len(x))
		for k, e := range x {
			out[k] = jsonAttribute(e)
		}
		return &types.AttributeValueMemberM{Value: out}
	}
	return str(fmt.Sprint(v))
}

// scalarType is the attribute type a column's values take as a key: N for
// numbers, B for binaries, S for the rest.
func scalarType(f arrow.Field) types.ScalarAttributeType {
	ext, dt := extensionOf(f)
	if ext != "" {
		return types.ScalarAttributeTypeS
	}
	switch dt.(type) {
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type,
		*arrow.Float32Type, *arrow.Float64Type, *arrow.Decimal128Type, *arrow.Decimal256Type:
		return types.ScalarAttributeTypeN
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		return types.ScalarAttributeTypeB
	}
	return types.ScalarAttributeTypeS
}

// asKey is v as a key attribute of type t: as it is where it has that type,
// else its text as S.
func asKey(v types.AttributeValue, t types.ScalarAttributeType) (types.AttributeValue, error) {
	if kind(v) == string(t) {
		return v, nil
	}
	if s, ok := text(v); ok && t == types.ScalarAttributeTypeS {
		return str(s), nil
	}
	return nil, fmt.Errorf("a %s value isn't a key of type %s", kind(v), t)
}

// size is about how many bytes DynamoDB counts an item as, at least as many
// as it does: its attribute names and values, numbers as their digits.
func size(item map[string]types.AttributeValue) int {
	n := 0
	for k, v := range item {
		n += len(k) + valueSize(v)
	}
	return n
}

func valueSize(av types.AttributeValue) int {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return len(v.Value)
	case *types.AttributeValueMemberN:
		return len(v.Value)
	case *types.AttributeValueMemberB:
		return len(v.Value)
	case *types.AttributeValueMemberM:
		return 3 + len(v.Value) + size(v.Value)
	case *types.AttributeValueMemberL:
		n := 3
		for _, e := range v.Value {
			n += 1 + valueSize(e)
		}
		return n
	case *types.AttributeValueMemberSS:
		n := 0
		for _, e := range v.Value {
			n += len(e)
		}
		return n
	case *types.AttributeValueMemberNS:
		n := 0
		for _, e := range v.Value {
			n += len(e)
		}
		return n
	case *types.AttributeValueMemberBS:
		n := 0
		for _, e := range v.Value {
			n += len(e)
		}
		return n
	}
	return 1
}

// tableName is a stream's table, as its schema names it, in the characters
// DynamoDB takes in names.
func tableName(schema *arrow.Schema, stream string) string {
	t, ok := schema.Metadata().GetValue(tableMetaKey)
	if !ok {
		t = stream
	}
	return strings.Map(func(r rune) rune {
		if r == '_' || r == '-' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, t)
}
