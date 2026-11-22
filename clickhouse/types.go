package clickhouse

import (
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	chcol "github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

const (
	extensionNameKey = "ARROW:extension:name"
	jsonExtension    = "arrow.json"
	uuidExtension    = "arrow.uuid"
	// The engine names each stream's table in its schema's metadata.
	tableKey = "openetl.table"
)

// column is a destination column: its ClickHouse type, spelled as
// system.columns spells it, and how a batch's cell becomes the value the
// driver appends, nil for a null.
type column struct {
	name  string
	typ   string
	value func(a arrow.Array, i int) any
}

// mapped is a type that values convert to: its ClickHouse type without
// Nullable, whether Nullable can wrap it, and a cell's value, which is
// never null.
type mapped struct {
	typ      string
	nullable bool
	value    func(a arrow.Array, i int) any
	// The value a null takes in a column that can't hold one.
	zero any
	// A null's value where Nullable wraps the type, in a column, an array,
	// a map or a tuple, where the driver needs it typed; nil otherwise.
	null any
}

// The ranges dates and times are written in. Values past them are clamped
// to them. Date32 holds 1900 to 2299; DateTime64 too, but the driver
// encodes its values from nanoseconds since 1970, which end in 2262.
var (
	minDate     = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	maxDate     = time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC)
	maxDateTime = time.Unix(0, 1<<63-1).UTC()
)

func clamp(t, lo, hi time.Time) time.Time {
	switch {
	case t.Before(lo):
		return lo
	case t.After(hi):
		return hi
	}
	return t
}

func extensionOf(f arrow.Field) (string, arrow.DataType) {
	if et, ok := f.Type.(arrow.ExtensionType); ok {
		return et.ExtensionName(), et.StorageType()
	}
	ext, _ := f.Metadata.GetValue(extensionNameKey)
	return ext, f.Type
}

// storage is an extension array's storage, or a itself.
func storage(a arrow.Array) arrow.Array {
	if e, ok := a.(array.ExtensionArray); ok {
		return e.Storage()
	}
	return a
}

func columnsOf(schema *arrow.Schema, notNull func(name string) bool) ([]column, error) {
	cols := make([]column, 0, schema.NumFields())
	for _, f := range schema.Fields() {
		c, err := columnOf(f, notNull(f.Name))
		if err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, nil
}

// columnOf maps a field to a column; notNull makes it a column that holds
// no null, as a merge's key and the metadata columns are.
func columnOf(f arrow.Field, notNull bool) (column, error) {
	ext, dt := extensionOf(f)
	m, err := mapType(dt, ext)
	if err != nil {
		return column{}, fmt.Errorf("clickhouse: column %q: %w", f.Name, err)
	}
	c := column{name: f.Name, typ: m.typ}
	if m.nullable && !notNull {
		c.typ = "Nullable(" + m.typ + ")"
		c.value = func(a arrow.Array, i int) any {
			if a.IsNull(i) {
				return m.null
			}
			return m.value(a, i)
		}
		return c, nil
	}
	c.value = func(a arrow.Array, i int) any {
		if a.IsNull(i) {
			return m.zero
		}
		return m.value(a, i)
	}
	return c, nil
}

func mapType(dt arrow.DataType, ext string) (mapped, error) {
	switch {
	case ext == jsonExtension && isString(dt):
		return mapped{typ: "String", nullable: true, value: stringValue, zero: ""}, nil
	case ext == uuidExtension && dt.ID() == arrow.FIXED_SIZE_BINARY:
		return mapped{typ: "UUID", nullable: true, zero: uuid.Nil, value: func(a arrow.Array, i int) any {
			var u uuid.UUID
			copy(u[:], storage(a).(*array.FixedSizeBinary).Value(i))
			return u
		}}, nil
	}
	num := func(typ string, zero any, v func(a arrow.Array, i int) any) (mapped, error) {
		return mapped{typ: typ, nullable: true, zero: zero, value: v}, nil
	}
	switch t := dt.(type) {
	case *arrow.NullType:
		return mapped{typ: "String", nullable: true, zero: "", value: func(arrow.Array, int) any { return nil }}, nil
	case *arrow.BooleanType:
		return num("Bool", false, func(a arrow.Array, i int) any { return storage(a).(*array.Boolean).Value(i) })
	case *arrow.Int8Type:
		return num("Int8", int8(0), func(a arrow.Array, i int) any { return storage(a).(*array.Int8).Value(i) })
	case *arrow.Int16Type:
		return num("Int16", int16(0), func(a arrow.Array, i int) any { return storage(a).(*array.Int16).Value(i) })
	case *arrow.Int32Type:
		return num("Int32", int32(0), func(a arrow.Array, i int) any { return storage(a).(*array.Int32).Value(i) })
	case *arrow.Int64Type:
		return num("Int64", int64(0), func(a arrow.Array, i int) any { return storage(a).(*array.Int64).Value(i) })
	case *arrow.Uint8Type:
		return num("UInt8", uint8(0), func(a arrow.Array, i int) any { return storage(a).(*array.Uint8).Value(i) })
	case *arrow.Uint16Type:
		return num("UInt16", uint16(0), func(a arrow.Array, i int) any { return storage(a).(*array.Uint16).Value(i) })
	case *arrow.Uint32Type:
		return num("UInt32", uint32(0), func(a arrow.Array, i int) any { return storage(a).(*array.Uint32).Value(i) })
	case *arrow.Uint64Type:
		return num("UInt64", uint64(0), func(a arrow.Array, i int) any { return storage(a).(*array.Uint64).Value(i) })
	case *arrow.Float16Type:
		return num("Float32", float32(0), func(a arrow.Array, i int) any { return storage(a).(*array.Float16).Value(i).Float32() })
	case *arrow.Float32Type:
		return num("Float32", float32(0), func(a arrow.Array, i int) any { return storage(a).(*array.Float32).Value(i) })
	case *arrow.Float64Type:
		return num("Float64", float64(0), func(a arrow.Array, i int) any { return storage(a).(*array.Float64).Value(i) })
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return num("String", "", stringValue)
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType:
		return num("String", "", func(a arrow.Array, i int) any {
			return string(storage(a).(interface{ Value(int) []byte }).Value(i))
		})
	case *arrow.FixedSizeBinaryType:
		return num("FixedString("+strconv.Itoa(t.ByteWidth)+")", "", func(a arrow.Array, i int) any {
			return string(storage(a).(*array.FixedSizeBinary).Value(i))
		})
	case *arrow.Decimal32Type:
		return decimalType(t.Precision, t.Scale, func(a arrow.Array, i int) *big.Int {
			return big.NewInt(int64(storage(a).(*array.Decimal32).Value(i)))
		})
	case *arrow.Decimal64Type:
		return decimalType(t.Precision, t.Scale, func(a arrow.Array, i int) *big.Int {
			return big.NewInt(int64(storage(a).(*array.Decimal64).Value(i)))
		})
	case *arrow.Decimal128Type:
		return decimalType(t.Precision, t.Scale, func(a arrow.Array, i int) *big.Int { return storage(a).(*array.Decimal128).Value(i).BigInt() })
	case *arrow.Decimal256Type:
		return decimalType(t.Precision, t.Scale, func(a arrow.Array, i int) *big.Int { return storage(a).(*array.Decimal256).Value(i).BigInt() })
	case *arrow.Date32Type:
		return num("Date32", minDate, func(a arrow.Array, i int) any {
			return clamp(storage(a).(*array.Date32).Value(i).ToTime(), minDate, maxDate)
		})
	case *arrow.Date64Type:
		return num("Date32", minDate, func(a arrow.Array, i int) any {
			return clamp(storage(a).(*array.Date64).Value(i).ToTime().Truncate(24*time.Hour), minDate, maxDate)
		})
	case *arrow.TimestampType:
		p, unit := precision(t.Unit), t.Unit
		return num(fmt.Sprintf("DateTime64(%d, 'UTC')", p), minDate, func(a arrow.Array, i int) any {
			return clamp(storage(a).(*array.Timestamp).Value(i).ToTime(unit), minDate, maxDateTime)
		})
	case *arrow.Time32Type:
		unit := t.Unit
		return mapped{typ: fmt.Sprintf("Time64(%d)", precision(unit)), nullable: true, zero: time.Duration(0), null: (*time.Duration)(nil),
			value: func(a arrow.Array, i int) any {
				return time.Duration(storage(a).(*array.Time32).Value(i)) * unit.Multiplier()
			}}, nil
	case *arrow.Time64Type:
		unit := t.Unit
		return mapped{typ: fmt.Sprintf("Time64(%d)", precision(unit)), nullable: true, zero: time.Duration(0), null: (*time.Duration)(nil),
			value: func(a arrow.Array, i int) any {
				return time.Duration(storage(a).(*array.Time64).Value(i)) * unit.Multiplier()
			}}, nil
	case *arrow.DurationType:
		return num("Int64", int64(0), func(a arrow.Array, i int) any { return int64(storage(a).(*array.Duration).Value(i)) })
	case *arrow.MonthDayNanoIntervalType:
		return mapped{typ: intervalType, zero: []any{int32(0), int32(0), int64(0)}, value: func(a arrow.Array, i int) any {
			v := storage(a).(*array.MonthDayNanoInterval).Value(i)
			return []any{v.Months, v.Days, v.Nanoseconds}
		}}, nil
	case *arrow.MonthIntervalType:
		return num("Int32", int32(0), func(a arrow.Array, i int) any { return int32(storage(a).(*array.MonthInterval).Value(i)) })
	case *arrow.DayTimeIntervalType:
		return mapped{typ: intervalType, zero: []any{int32(0), int32(0), int64(0)}, value: func(a arrow.Array, i int) any {
			v := storage(a).(*array.DayTimeInterval).Value(i)
			return []any{int32(0), v.Days, int64(v.Milliseconds) * int64(time.Millisecond)}
		}}, nil
	case arrow.ListLikeType:
		return listType(t)
	case *arrow.StructType:
		return structType(t)
	case *arrow.DictionaryType:
		inner, err := mapType(t.ValueType, "")
		if err != nil {
			return mapped{}, err
		}
		typ := inner.typ
		if inner.nullable {
			typ = "Nullable(" + typ + ")"
		}
		// A null takes the zero value only where Nullable can't wrap the
		// dictionary's type.
		zero := inner.zero
		if inner.nullable {
			zero = inner.null
		}
		return mapped{typ: "LowCardinality(" + typ + ")", zero: zero, value: func(a arrow.Array, i int) any {
			d := storage(a).(*array.Dictionary)
			j := d.GetValueIndex(i)
			if d.Dictionary().IsNull(j) {
				return zero
			}
			return inner.value(d.Dictionary(), j)
		}}, nil
	}
	return mapped{}, fmt.Errorf("no ClickHouse type for Arrow's %s", dt)
}

// Arrow's month-day-nanosecond interval, which ClickHouse has no column
// type for.
const intervalType = "Tuple(months Int32, days Int32, nanoseconds Int64)"

func precision(u arrow.TimeUnit) int {
	return [...]int{0, 3, 6, 9}[u]
}

// decimalType is the Decimal that holds Arrow's decimal(p, s). ClickHouse's
// scale is 0 to its precision: a negative scale, of values in multiples of
// a power of ten, takes as many more digits at scale 0, so decimal(5, -3)
// is Decimal(8, 0), and a scale past p takes that many digits.
func decimalType(p, s int32, v func(a arrow.Array, i int) *big.Int) (mapped, error) {
	cp, cs := max(p-min(s, 0), s), max(s, 0)
	if cp > 76 {
		return mapped{}, fmt.Errorf("decimal(%d, %d) has more digits than ClickHouse's 76", p, s)
	}
	return mapped{typ: fmt.Sprintf("Decimal(%d, %d)", cp, cs), nullable: true, zero: decimal.Zero, value: func(a arrow.Array, i int) any {
		return decimal.NewFromBigInt(v(a, i), -s)
	}}, nil
}

// listType is an Array of the element's type, Nullable where it can be:
// ClickHouse's Array can't itself be Nullable, so a null list is empty.
func listType(t arrow.ListLikeType) (mapped, error) {
	if m, ok := t.(*arrow.MapType); ok {
		return mapOf(m)
	}
	ef := t.ElemField()
	ext, edt := extensionOf(ef)
	elem, err := mapType(edt, ext)
	if err != nil {
		return mapped{}, err
	}
	typ := elem.typ
	if elem.nullable {
		typ = "Nullable(" + typ + ")"
	}
	value := func(a arrow.Array, i int) any {
		l := storage(a).(array.ListLike)
		start, end := l.ValueOffsets(i)
		vals := l.ListValues()
		out := make([]any, 0, end-start)
		for j := int(start); j < int(end); j++ {
			switch {
			case !vals.IsNull(j):
				out = append(out, elem.value(vals, j))
			case elem.nullable:
				out = append(out, elem.null)
			default:
				out = append(out, elem.zero)
			}
		}
		return out
	}
	return mapped{typ: "Array(" + typ + ")", zero: []any{}, value: value}, nil
}

// mapOf is a Map of the key's and the item's types.
func mapOf(t *arrow.MapType) (mapped, error) {
	kext, kdt := extensionOf(t.KeyField())
	key, err := mapType(kdt, kext)
	if err != nil {
		return mapped{}, err
	}
	iext, idt := extensionOf(t.ItemField())
	item, err := mapType(idt, iext)
	if err != nil {
		return mapped{}, err
	}
	ityp := item.typ
	if item.nullable {
		ityp = "Nullable(" + ityp + ")"
	}
	return mapped{typ: "Map(" + key.typ + ", " + ityp + ")", zero: &orderedMap{}, value: func(a arrow.Array, i int) any {
		m := storage(a).(*array.Map)
		start, end := m.ValueOffsets(i)
		keys, items := m.Keys(), m.Items()
		out := &orderedMap{}
		for j := int(start); j < int(end); j++ {
			v := item.null
			switch {
			case !items.IsNull(j):
				v = item.value(items, j)
			case !item.nullable:
				v = item.zero
			}
			out.Put(key.value(keys, j), v)
		}
		return out
	}}, nil
}

// structType is a named Tuple of the fields' types, each Nullable where it
// can be: a null struct is a tuple of nulls.
func structType(t *arrow.StructType) (mapped, error) {
	fields := make([]mapped, t.NumFields())
	names := make([]string, t.NumFields())
	for i, f := range t.Fields() {
		ext, dt := extensionOf(f)
		m, err := mapType(dt, ext)
		if err != nil {
			return mapped{}, err
		}
		typ := m.typ
		if m.nullable {
			typ = "Nullable(" + typ + ")"
		}
		fields[i], names[i] = m, tupleName(f.Name)+" "+typ
	}
	zero := make([]any, len(fields))
	for i, m := range fields {
		zero[i] = m.null
		if !m.nullable {
			zero[i] = m.zero
		}
	}
	return mapped{typ: "Tuple(" + strings.Join(names, ", ") + ")", zero: zero, value: func(a arrow.Array, i int) any {
		s := storage(a).(*array.Struct)
		out := make([]any, len(fields))
		for j, m := range fields {
			f := s.Field(j)
			switch {
			case !f.IsNull(i):
				out[j] = m.value(f, i)
			case m.nullable:
				out[j] = m.null
			default:
				out[j] = m.zero
			}
		}
		return out
	}}, nil
}

var plainName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// tupleName is a tuple element's name as ClickHouse spells it in a type.
func tupleName(n string) string {
	if plainName.MatchString(n) {
		return n
	}
	return ident(n)
}

func isString(dt arrow.DataType) bool {
	switch dt.(type) {
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return true
	}
	return false
}

func stringValue(a arrow.Array, i int) any {
	return storage(a).(interface{ Value(int) string }).Value(i)
}

// orderedMap is a Map's value, in the batch's order.
type orderedMap struct {
	keys, values []any
}

var _ chcol.IterableOrderedMap = (*orderedMap)(nil)

func (m *orderedMap) Put(k, v any) { m.keys, m.values = append(m.keys, k), append(m.values, v) }

func (m *orderedMap) Iterator() chcol.MapIterator { return &mapIterator{m: m, i: -1} }

type mapIterator struct {
	m *orderedMap
	i int
}

func (it *mapIterator) Next() bool { it.i++; return it.i < len(it.m.keys) }
func (it *mapIterator) Key() any   { return it.m.keys[it.i] }
func (it *mapIterator) Value() any { return it.m.values[it.i] }

// canonical spells a type as system.columns does, for comparisons.
func canonical(typ string) string {
	s := strings.Join(strings.Fields(typ), " ")
	s = strings.NewReplacer("( ", "(", " )", ")", " ,", ",").Replace(s)
	return strings.ReplaceAll(strings.ReplaceAll(s, ", ", ","), ",", ", ")
}

// unwrap is a type without its Nullable, and whether it had one.
func unwrapNullable(typ string) (string, bool) {
	if inner, ok := strings.CutPrefix(typ, "Nullable("); ok {
		return strings.TrimSuffix(inner, ")"), true
	}
	return typ, false
}

var (
	intType     = regexp.MustCompile(`^(U?)Int(8|16|32|64|128|256)$`)
	decimalSpec = regexp.MustCompile(`^Decimal\((\d+), (\d+)\)$`)
	dt64Spec    = regexp.MustCompile(`^DateTime64\((\d)(, '([^']*)')?\)$`)
	time64Spec  = regexp.MustCompile(`^Time64\((\d)\)$`)
)

// widens reports whether a column of type from can take every value of type
// to's place without loss, by a MODIFY COLUMN: to is a wider type.
func widens(from, to string) bool {
	from, to = canonical(from), canonical(to)
	if from == to {
		return true
	}
	f, fn := unwrapNullable(from)
	t, tn := unwrapNullable(to)
	if fn && !tn {
		return false
	}
	if f == t {
		return true
	}
	if t == "String" {
		return !strings.HasPrefix(f, "Array(") && !strings.HasPrefix(f, "Map(") && !strings.HasPrefix(f, "Tuple(")
	}
	if fm, tm := intType.FindStringSubmatch(f), intType.FindStringSubmatch(t); fm != nil && tm != nil {
		fb, _ := strconv.Atoi(fm[2])
		tb, _ := strconv.Atoi(tm[2])
		switch {
		case fm[1] == tm[1]:
			return tb > fb
		case fm[1] == "U":
			return tb > fb
		}
		return false
	}
	// Floats hold integers of up to 24 and 53 bits exactly.
	if m := intType.FindStringSubmatch(f); m != nil {
		bits, _ := strconv.Atoi(m[2])
		return t == "Float32" && bits <= 16 || t == "Float64" && bits <= 32
	}
	if f == "Float32" && t == "Float64" {
		return true
	}
	if fm, tm := decimalSpec.FindStringSubmatch(f), decimalSpec.FindStringSubmatch(t); fm != nil && tm != nil {
		fp, _ := strconv.Atoi(fm[1])
		fs, _ := strconv.Atoi(fm[2])
		tp, _ := strconv.Atoi(tm[1])
		ts, _ := strconv.Atoi(tm[2])
		return ts >= fs && tp-ts >= fp-fs
	}
	// DateTime64(9) ends at 2262-04-11, Date32 and DateTime64's others at
	// 2299-12-31, past which ClickHouse would clamp a value or fail to read it.
	if fm, tm := dt64Spec.FindStringSubmatch(f), dt64Spec.FindStringSubmatch(t); fm != nil && tm != nil {
		return fm[3] == tm[3] && tm[1] >= fm[1] && (tm[1] != "9" || fm[1] == "9")
	}
	if fm, tm := time64Spec.FindStringSubmatch(f), time64Spec.FindStringSubmatch(t); fm != nil && tm != nil {
		return tm[1] >= fm[1]
	}
	if m := dt64Spec.FindStringSubmatch(t); f == "Date32" && m != nil {
		return m[1] != "9"
	}
	if inner, ok := strings.CutPrefix(f, "Array("); ok {
		if tinner, ok := strings.CutPrefix(t, "Array("); ok {
			return widens(strings.TrimSuffix(inner, ")"), strings.TrimSuffix(tinner, ")"))
		}
	}
	return false
}

// canHoldNull reports whether a column of type typ can hold a null.
func canHoldNull(typ string) bool {
	return strings.HasPrefix(typ, "Nullable(") || strings.HasPrefix(typ, "LowCardinality(Nullable(")
}

func names(cols []column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.name
	}
	return out
}

func quoteAll(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = ident(n)
	}
	return strings.Join(q, ", ")
}

func indexOfColumn(cols []column, name string) int {
	return slices.IndexFunc(cols, func(c column) bool { return c.name == name })
}
