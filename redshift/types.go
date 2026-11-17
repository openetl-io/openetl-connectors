package redshift

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/google/uuid"
)

// column is a target column, and how a staging file carries it.
type column struct {
	name string
	// As normType spells it.
	typ string
	// The file's Arrow type for it, one that COPY loads into loadType(load).
	load arrow.DataType
	// Appends row i of a batch's array, which isn't null, to a builder of
	// load; nil for a column of nulls.
	put func(b array.Builder, a arrow.Array, i int) error
	// From the load table's column expr to typ; nil when loadType(load) is
	// typ.
	cast func(expr string) string
}

const (
	extensionNameKey = "ARROW:extension:name"
	jsonExtension    = "arrow.json"
	uuidExtension    = "arrow.uuid"
	// The engine names each stream's table in its schema's metadata.
	tableKey = "openetl.table"

	// Bytes, Redshift's limits: VARCHAR's, and VARBYTE's.
	maxVarchar = 65535
	maxVarbyte = 16777216
	varcharMax = "varchar(65535)"
	varbyteMax = "varbyte(16777216)"
	// SUPER's values arrive as JSON text that JSON_PARSE reads from a
	// VARCHAR, so they're held to its limit.
	superType = "super"
)

var (
	loadString = arrow.BinaryTypes.String
	loadMicros = &arrow.TimestampType{Unit: arrow.Microsecond}
)

// loadType is the type of a load table's column that COPY fills from a
// file's column of Arrow type dt, one of those columnOf gives.
func loadType(dt arrow.DataType) string {
	switch t := dt.(type) {
	case *arrow.BooleanType:
		return "boolean"
	case *arrow.Int32Type:
		return "integer"
	case *arrow.Int64Type:
		return "bigint"
	case *arrow.Float32Type:
		return "real"
	case *arrow.Float64Type:
		return "double precision"
	case *arrow.BinaryType:
		return varbyteMax
	case *arrow.Decimal128Type:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision, t.Scale)
	case *arrow.Date32Type:
		return "date"
	case *arrow.TimestampType:
		return "timestamp"
	}
	return varcharMax
}

func castAs(typ string) func(string) string {
	return func(expr string) string { return "CAST(" + expr + " AS " + typ + ")" }
}

func jsonParse(expr string) string { return "JSON_PARSE(" + expr + ")" }

// columnsOf maps a batch's fields to columns.
func columnsOf(schema *arrow.Schema) ([]column, error) {
	cols := make([]column, 0, schema.NumFields())
	for _, f := range schema.Fields() {
		c, err := columnOf(f)
		if err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, nil
}

// columnOf is a field's column. Its value is staged as one of a few Arrow
// types whose Parquet COPY loads, and cast to the target's type from there:
// times, JSON and the like as text.
func columnOf(f arrow.Field) (column, error) {
	c := column{name: f.Name}
	dt := f.Type
	ext := ""
	if et, ok := dt.(arrow.ExtensionType); ok {
		ext, dt = et.ExtensionName(), et.StorageType()
	} else if v, ok := f.Metadata.GetValue(extensionNameKey); ok {
		ext = v
	}
	switch {
	case f.Name == heldColumn || f.Name == unchangedColumn:
		// Names, as JSON: the batch writer makes them.
		c.typ, c.load = varcharMax, loadString
		return c, nil
	case ext == jsonExtension && isString(dt):
		c.typ, c.load, c.put, c.cast = superType, loadString, putText(func(a arrow.Array, i int) (string, error) { return str(a, i), nil }), jsonParse
		return c, nil
	case ext == uuidExtension && dt.ID() == arrow.FIXED_SIZE_BINARY:
		c.typ, c.load, c.cast = "varchar(36)", loadString, castAs("varchar(36)")
		c.put = putText(func(a arrow.Array, i int) (string, error) {
			u, err := uuid.FromBytes(storage(a).(*array.FixedSizeBinary).Value(i))
			return u.String(), err
		})
		return c, nil
	}

	switch t := dt.(type) {
	case *arrow.NullType:
		c.typ, c.load = varcharMax, loadString
	case *arrow.BooleanType:
		c.typ, c.load = "boolean", arrow.FixedWidthTypes.Boolean
		c.put = func(b array.Builder, a arrow.Array, i int) error {
			b.(*array.BooleanBuilder).Append(storage(a).(*array.Boolean).Value(i))
			return nil
		}
	case *arrow.Int8Type:
		c.typ, c.load, c.cast = "smallint", arrow.PrimitiveTypes.Int32, castAs("smallint")
		c.put = putInt32(func(a arrow.Array, i int) int32 { return int32(storage(a).(*array.Int8).Value(i)) })
	case *arrow.Int16Type:
		c.typ, c.load, c.cast = "smallint", arrow.PrimitiveTypes.Int32, castAs("smallint")
		c.put = putInt32(func(a arrow.Array, i int) int32 { return int32(storage(a).(*array.Int16).Value(i)) })
	case *arrow.Uint8Type:
		c.typ, c.load, c.cast = "smallint", arrow.PrimitiveTypes.Int32, castAs("smallint")
		c.put = putInt32(func(a arrow.Array, i int) int32 { return int32(storage(a).(*array.Uint8).Value(i)) })
	case *arrow.Int32Type:
		c.typ, c.load = "integer", arrow.PrimitiveTypes.Int32
		c.put = putInt32(func(a arrow.Array, i int) int32 { return storage(a).(*array.Int32).Value(i) })
	case *arrow.Uint16Type:
		c.typ, c.load = "integer", arrow.PrimitiveTypes.Int32
		c.put = putInt32(func(a arrow.Array, i int) int32 { return int32(storage(a).(*array.Uint16).Value(i)) })
	case *arrow.Int64Type:
		c.typ, c.load = "bigint", arrow.PrimitiveTypes.Int64
		c.put = putInt64(func(a arrow.Array, i int) int64 { return storage(a).(*array.Int64).Value(i) })
	case *arrow.Uint32Type:
		c.typ, c.load = "bigint", arrow.PrimitiveTypes.Int64
		c.put = putInt64(func(a arrow.Array, i int) int64 { return int64(storage(a).(*array.Uint32).Value(i)) })
	case *arrow.Uint64Type:
		c.typ, c.load = "decimal(20,0)", &arrow.Decimal128Type{Precision: 20, Scale: 0}
		c.put = func(b array.Builder, a arrow.Array, i int) error {
			b.(*array.Decimal128Builder).Append(decimal128.FromU64(storage(a).(*array.Uint64).Value(i)))
			return nil
		}
	case *arrow.Float32Type:
		c.typ, c.load = "real", arrow.PrimitiveTypes.Float32
		c.put = func(b array.Builder, a arrow.Array, i int) error {
			b.(*array.Float32Builder).Append(storage(a).(*array.Float32).Value(i))
			return nil
		}
	case *arrow.Float64Type:
		c.typ, c.load = "double precision", arrow.PrimitiveTypes.Float64
		c.put = func(b array.Builder, a arrow.Array, i int) error {
			b.(*array.Float64Builder).Append(storage(a).(*array.Float64).Value(i))
			return nil
		}
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		c.typ, c.load = varcharMax, loadString
		c.put = putText(func(a arrow.Array, i int) (string, error) { return str(a, i), nil })
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		c.typ, c.load = varbyteMax, arrow.BinaryTypes.Binary
		c.put = func(b array.Builder, a arrow.Array, i int) error {
			v := storage(a).(interface{ Value(int) []byte }).Value(i)
			if len(v) > maxVarbyte {
				return fmt.Errorf("a value of %d bytes, and Redshift's VARBYTE holds at most %d", len(v), maxVarbyte)
			}
			b.(*array.BinaryBuilder).Append(v)
			return nil
		}
	case *arrow.Date32Type:
		c.typ, c.load = "date", arrow.FixedWidthTypes.Date32
		c.put = func(b array.Builder, a arrow.Array, i int) error {
			b.(*array.Date32Builder).Append(storage(a).(*array.Date32).Value(i))
			return nil
		}
	case *arrow.Date64Type:
		c.typ, c.load = "date", arrow.FixedWidthTypes.Date32
		c.put = func(b array.Builder, a arrow.Array, i int) error {
			b.(*array.Date32Builder).Append(arrow.Date32FromTime(storage(a).(*array.Date64).Value(i).ToTime()))
			return nil
		}
	case *arrow.Time32Type, *arrow.Time64Type:
		// TIME keeps microseconds.
		c.typ, c.load, c.cast = "time", loadString, castAs("time")
		c.put = putText(func(a arrow.Array, i int) (string, error) { return clock(timeOfDay(a, i)), nil })
	case *arrow.TimestampType:
		// TIMESTAMPTZ from the UTC time the file has, as the session's time
		// zone is UTC. Nanoseconds are cut to microseconds.
		c.typ, c.load = "timestamp", loadMicros
		if t.TimeZone != "" {
			c.typ, c.cast = "timestamptz", castAs("timestamptz")
		}
		unit := t.Unit
		c.put = func(b array.Builder, a arrow.Array, i int) error {
			b.(*array.TimestampBuilder).Append(arrow.Timestamp(storage(a).(*array.Timestamp).Value(i).ToTime(unit).UnixMicro()))
			return nil
		}
	case *arrow.Decimal128Type:
		p, ok := decimalPrecision(t.Precision, t.Scale)
		if !ok {
			c.typ, c.load = varcharMax, loadString
			c.put = putText(func(a arrow.Array, i int) (string, error) {
				return storage(a).(*array.Decimal128).Value(i).ToString(t.Scale), nil
			})
			break
		}
		c.typ, c.load = fmt.Sprintf("decimal(%d,%d)", p, t.Scale), &arrow.Decimal128Type{Precision: p, Scale: t.Scale}
		c.put = func(b array.Builder, a arrow.Array, i int) error {
			b.(*array.Decimal128Builder).Append(storage(a).(*array.Decimal128).Value(i))
			return nil
		}
	case *arrow.Decimal256Type:
		p, ok := decimalPrecision(t.Precision, t.Scale)
		if !ok {
			c.typ, c.load = varcharMax, loadString
			c.put = putText(func(a arrow.Array, i int) (string, error) {
				return storage(a).(*array.Decimal256).Value(i).ToString(t.Scale), nil
			})
			break
		}
		c.typ, c.load = fmt.Sprintf("decimal(%d,%d)", p, t.Scale), &arrow.Decimal128Type{Precision: p, Scale: t.Scale}
		c.put = func(b array.Builder, a arrow.Array, i int) error {
			b.(*array.Decimal128Builder).Append(decimal128.FromBigInt(storage(a).(*array.Decimal256).Value(i).BigInt()))
			return nil
		}
	case *arrow.MonthDayNanoIntervalType:
		// Redshift's intervals are either years and months or days and
		// seconds; text keeps both.
		c.typ, c.load, c.cast = "varchar(64)", loadString, castAs("varchar(64)")
		c.put = putText(func(a arrow.Array, i int) (string, error) {
			return isoInterval(storage(a).(*array.MonthDayNanoInterval).Value(i)), nil
		})
	case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType, *arrow.StructType, *arrow.MapType:
		c.typ, c.load, c.cast = superType, loadString, jsonParse
		c.put = putText(func(a arrow.Array, i int) (string, error) {
			b, err := json.Marshal(storage(a).GetOneForMarshal(i))
			return string(b), err
		})
	default:
		return c, fmt.Errorf("redshift: column %q has unsupported Arrow type %s", f.Name, f.Type)
	}
	return c, nil
}

// decimalPrecision is the DECIMAL precision that holds a decimal's values,
// and whether one does: 19 digits are stored in 64 bits, which hold only up
// to 2^63-1, so they take 20.
func decimalPrecision(p, s int32) (int32, bool) {
	if p > 38 || s < 0 || s > 37 || s > p {
		return 0, false
	}
	if p == 19 {
		p = 20
	}
	return p, true
}

func putInt32(v func(arrow.Array, int) int32) func(array.Builder, arrow.Array, int) error {
	return func(b array.Builder, a arrow.Array, i int) error {
		b.(*array.Int32Builder).Append(v(a, i))
		return nil
	}
}

func putInt64(v func(arrow.Array, int) int64) func(array.Builder, arrow.Array, int) error {
	return func(b array.Builder, a arrow.Array, i int) error {
		b.(*array.Int64Builder).Append(v(a, i))
		return nil
	}
}

// putText appends text that VARCHAR holds: at most maxVarchar bytes.
func putText(v func(arrow.Array, int) (string, error)) func(array.Builder, arrow.Array, int) error {
	return func(b array.Builder, a arrow.Array, i int) error {
		s, err := v(a, i)
		if err != nil {
			return err
		}
		if len(s) > maxVarchar {
			return fmt.Errorf("a value of %d bytes, and Redshift's VARCHAR holds at most %d", len(s), maxVarchar)
		}
		b.(*array.StringBuilder).Append(s)
		return nil
	}
}

func timeOfDay(a arrow.Array, i int) time.Duration {
	switch t := storage(a).(type) {
	case *array.Time32:
		return time.Duration(t.Value(i)) * t.DataType().(*arrow.Time32Type).Unit.Multiplier()
	case *array.Time64:
		return time.Duration(t.Value(i)) * t.DataType().(*arrow.Time64Type).Unit.Multiplier()
	}
	return 0
}

// clock is a time of day as TIME reads it, to the microsecond.
func clock(d time.Duration) string {
	us := d.Microseconds()
	return fmt.Sprintf("%02d:%02d:%02d.%06d", us/3_600_000_000, us/60_000_000%60, us/1_000_000%60, us%1_000_000)
}

// isoInterval is an interval as an ISO 8601 duration with every part, such
// as P1M2DT3.5S: months, days and seconds, each with its own sign.
func isoInterval(v arrow.MonthDayNanoInterval) string {
	sec, frac := v.Nanoseconds/1e9, v.Nanoseconds%1e9
	sign := ""
	if v.Nanoseconds < 0 {
		sign, sec, frac = "-", -sec, -frac
	}
	s := sign + strconv.FormatInt(sec, 10)
	if frac != 0 {
		s += strings.TrimRight(fmt.Sprintf(".%09d", frac), "0")
	}
	return fmt.Sprintf("P%dM%dDT%sS", v.Months, v.Days, s)
}

// jsonName is name as a JSON string with every character but ASCII letters,
// digits and _ escaped, so that no name's text holds a quote, comma, colon,
// bracket or brace: the metadata columns' JSON can then be searched for a
// name's text, and found only where it names it.
func jsonName(name string) string {
	var b strings.Builder
	b.WriteByte('"')
	for len(name) > 0 {
		r, n := utf8.DecodeRuneInString(name)
		name = name[n:]
		switch {
		case r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9':
			b.WriteRune(r)
		case r > 0xFFFF:
			r1, r2 := utf16.EncodeRune(r)
			fmt.Fprintf(&b, `\u%04x\u%04x`, r1, r2)
		default:
			fmt.Fprintf(&b, `\u%04x`, r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// nameList is names as a JSON array, as _openetl_unchanged holds them.
func nameList(names []string) string {
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = jsonName(n)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// heldObject maps each of names to id, the row version whose value is
// held, as _openetl_held holds them; empty for none.
func heldObject(names []string, id string) string {
	names = slices.Compact(slices.Sorted(slices.Values(names)))
	if len(names) == 0 {
		return ""
	}
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = jsonName(n) + `:"` + id + `"`
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// listValues is the strings of row i of a list of strings, or of a JSON
// array as text.
func listValues(a arrow.Array, i int) ([]string, error) {
	a = storage(a)
	if isString(a.DataType()) {
		var out []string
		err := json.Unmarshal([]byte(str(a, i)), &out)
		return out, err
	}
	l, ok := a.(array.ListLike)
	if !ok {
		return nil, fmt.Errorf("a %s, where a list of names belongs", a.DataType())
	}
	start, end := l.ValueOffsets(i)
	var out []string
	for j := start; j < end; j++ {
		if v := l.ListValues(); !v.IsNull(int(j)) {
			if !isString(v.DataType()) {
				return nil, fmt.Errorf("a list of %s, where a list of names belongs", v.DataType())
			}
			out = append(out, str(v, int(j)))
		}
	}
	return out, nil
}

// normType spells a type as format_type prints it as columnOf does.
func normType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	base, args, _ := strings.Cut(t, "(")
	if args != "" {
		args = "(" + strings.ReplaceAll(args, " ", "")
	}
	switch strings.TrimSpace(base) {
	case "character varying", "varchar", "nvarchar", "text":
		base = "varchar"
	case "character", "char", "bpchar", "nchar":
		base = "char"
	case "numeric", "decimal":
		base = "decimal"
	case "timestamp without time zone", "timestamp":
		base = "timestamp"
	case "timestamp with time zone", "timestamptz":
		base = "timestamptz"
	case "time without time zone", "time":
		base = "time"
	case "time with time zone", "timetz":
		base = "timetz"
	case "binary varying", "varbinary", "varbyte":
		base = "varbyte"
	case "int2":
		base = "smallint"
	case "int", "int4":
		base = "integer"
	case "int8":
		base = "bigint"
	case "float4":
		base = "real"
	case "float", "float8":
		base = "double precision"
	case "bool":
		base = "boolean"
	default:
		base = strings.TrimSpace(base)
	}
	return base + args
}

// sized is a type's base and its one size, such as varchar and 36; false
// for a type without one.
func sized(t string) (string, int, bool) {
	base, args, ok := strings.Cut(t, "(")
	n, err := strconv.Atoi(strings.TrimSuffix(args, ")"))
	return base, n, ok && err == nil
}

func decimalPS(t string) (p, s int, ok bool) {
	inner, found := strings.CutPrefix(t, "decimal(")
	if !found {
		return 0, 0, false
	}
	ps, ss, comma := strings.Cut(strings.TrimSuffix(inner, ")"), ",")
	p, perr := strconv.Atoi(ps)
	s, serr := strconv.Atoi(ss)
	return p, s, comma && perr == nil && serr == nil
}

var intDigits = map[string]int{"smallint": 5, "integer": 10, "bigint": 19}

// widens reports whether a column of type from can become type to without
// losing values; bigint to double precision counts, as it does for the
// engine.
func widens(from, to string) bool {
	if from == to {
		return true
	}
	switch from + " " + to {
	case "smallint integer", "smallint bigint", "smallint real", "smallint double precision",
		"integer bigint", "integer double precision", "bigint double precision", "real double precision":
		return true
	}
	if base, n, ok := sized(to); ok && (base == "varchar" || base == "varbyte") {
		if fb, m, ok := sized(from); ok && (fb == base || base == "varchar" && fb == "char") {
			return m <= n
		}
		// Anything but binary and SUPER has text that fits the widest.
		return base == "varchar" && n == maxVarchar && from != superType && !strings.HasPrefix(from, "varbyte")
	}
	p2, s2, ok := decimalPS(to)
	if !ok {
		return false
	}
	if n, ok := intDigits[from]; ok {
		return p2-s2 >= n
	}
	p1, s1, ok := decimalPS(from)
	return ok && s2 >= s1 && p2-s2 >= p1-s1
}

// castTo is expr, of type from, as type to, which it widens to.
func castTo(expr, from, to string) string {
	switch {
	case from == to:
		return expr
	case from == "boolean":
		// Redshift doesn't cast a boolean to text.
		return "CASE WHEN " + expr + " THEN 'true' WHEN NOT " + expr + " THEN 'false' END"
	}
	return "CAST(" + expr + " AS " + to + ")"
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

// targetTable is the table the engine named, or for batches that didn't
// pass through the engine, the stream's name normalized.
func targetTable(schema *arrow.Schema, stream string) string {
	if t, ok := schema.Metadata().GetValue(tableKey); ok {
		return t
	}
	return tableName(stream)
}

// tableName lowercases stream and replaces anything but [a-z0-9_] with _.
func tableName(stream string) string {
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
