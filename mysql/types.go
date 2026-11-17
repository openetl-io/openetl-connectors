package mysql

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/uuid"
)

// column is a target column. typ is spelled as normType spells a column's
// COLUMN_TYPE.
type column struct {
	name string
	typ  string
	// The value of row i, which is not null, as the driver takes it, or an
	// error.
	value func(arr arrow.Array, i int) any
}

const (
	extensionNameKey = "ARROW:extension:name"
	jsonExtension    = "arrow.json"
	uuidExtension    = "arrow.uuid"
	// The engine names each stream's table in its schema's metadata.
	tableKey = "openetl.table"
	// InnoDB's limit on an index's key, in bytes, less room for a key's
	// columns that aren't text or binary.
	keyBytes = 3000
)

// columnsOf maps a batch's fields to columns. Text and binary columns in key
// are sized to fit, together, in a unique index.
func columnsOf(schema *arrow.Schema, key []string) ([]column, error) {
	sized := 0
	for _, k := range key {
		if i := schema.FieldIndices(k); len(i) > 0 && variable(schema.Field(i[0])) {
			sized++
		}
	}
	cols := make([]column, 0, schema.NumFields())
	for _, f := range schema.Fields() {
		c, err := columnOf(f)
		if err != nil {
			return nil, err
		}
		if slices.Contains(key, f.Name) && variable(f) {
			switch c.typ {
			case "longtext":
				c.typ = fmt.Sprintf("varchar(%d)", keyBytes/4/sized)
			case "longblob":
				c.typ = fmt.Sprintf("varbinary(%d)", keyBytes/sized)
			}
		}
		cols = append(cols, c)
	}
	return cols, nil
}

// variable reports whether a field maps to text or binary of any length.
func variable(f arrow.Field) bool {
	c, err := columnOf(f)
	return err == nil && (c.typ == "longtext" || c.typ == "longblob")
}

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
	case ext == jsonExtension && isString(dt):
		c.typ, c.value = "json", stringValue
		return c, nil
	case ext == uuidExtension && dt.ID() == arrow.FIXED_SIZE_BINARY:
		c.typ = "char(36)"
		c.value = func(a arrow.Array, i int) any {
			u, err := uuid.FromBytes(storage(a).(*array.FixedSizeBinary).Value(i))
			if err != nil {
				return err
			}
			return u.String()
		}
		return c, nil
	}

	switch t := dt.(type) {
	case *arrow.NullType:
		c.typ, c.value = "longtext", nil
	case *arrow.BooleanType:
		c.typ = "tinyint(1)"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Boolean).Value(i) }
	case *arrow.Int8Type:
		c.typ = "tinyint"
		c.value = func(a arrow.Array, i int) any { return int64(storage(a).(*array.Int8).Value(i)) }
	case *arrow.Int16Type:
		c.typ = "smallint"
		c.value = func(a arrow.Array, i int) any { return int64(storage(a).(*array.Int16).Value(i)) }
	case *arrow.Int32Type:
		c.typ = "int"
		c.value = func(a arrow.Array, i int) any { return int64(storage(a).(*array.Int32).Value(i)) }
	case *arrow.Int64Type:
		c.typ = "bigint"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Int64).Value(i) }
	case *arrow.Uint8Type:
		c.typ = "tinyint unsigned"
		c.value = func(a arrow.Array, i int) any { return uint64(storage(a).(*array.Uint8).Value(i)) }
	case *arrow.Uint16Type:
		c.typ = "smallint unsigned"
		c.value = func(a arrow.Array, i int) any { return uint64(storage(a).(*array.Uint16).Value(i)) }
	case *arrow.Uint32Type:
		c.typ = "int unsigned"
		c.value = func(a arrow.Array, i int) any { return uint64(storage(a).(*array.Uint32).Value(i)) }
	case *arrow.Uint64Type:
		c.typ = "bigint unsigned"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Uint64).Value(i) }
	case *arrow.Float32Type:
		// MySQL keeps no NaN or infinity: they land as null.
		c.typ = "float"
		c.value = func(a arrow.Array, i int) any { return finite(float64(storage(a).(*array.Float32).Value(i))) }
	case *arrow.Float64Type:
		c.typ = "double"
		c.value = func(a arrow.Array, i int) any { return finite(storage(a).(*array.Float64).Value(i)) }
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		c.typ, c.value = "longtext", stringValue
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		c.typ = "longblob"
		c.value = func(a arrow.Array, i int) any { return storage(a).(interface{ Value(int) []byte }).Value(i) }
	case *arrow.Date32Type:
		c.typ = "date"
		c.value = func(a arrow.Array, i int) any {
			return inRange(storage(a).(*array.Date32).Value(i).ToTime(), "2006-01-02")
		}
	case *arrow.Time64Type:
		c.typ = "time(6)"
		unit := t.Unit
		c.value = func(a arrow.Array, i int) any {
			return clock(time.Duration(storage(a).(*array.Time64).Value(i)) * unit.Multiplier())
		}
	case *arrow.TimestampType:
		// Both kinds of timestamp are DATETIME, in UTC: TIMESTAMP ends in 2038.
		// Nanoseconds are cut to microseconds.
		c.typ = "datetime(6)"
		unit := t.Unit
		c.value = func(a arrow.Array, i int) any {
			return inRange(storage(a).(*array.Timestamp).Value(i).ToTime(unit), "2006-01-02 15:04:05.000000")
		}
	case *arrow.Decimal128Type:
		c.typ, c.value = decimalColumn(t.Precision, t.Scale, func(a arrow.Array, i int) string {
			return storage(a).(*array.Decimal128).Value(i).ToString(t.Scale)
		})
	case *arrow.Decimal256Type:
		c.typ, c.value = decimalColumn(t.Precision, t.Scale, func(a arrow.Array, i int) string {
			return storage(a).(*array.Decimal256).Value(i).ToString(t.Scale)
		})
	case *arrow.MonthDayNanoIntervalType:
		c.typ = "json"
		c.value = func(a arrow.Array, i int) any {
			v := storage(a).(*array.MonthDayNanoInterval).Value(i)
			return fmt.Sprintf(`{"months": %d, "days": %d, "nanoseconds": %d}`, v.Months, v.Days, v.Nanoseconds)
		}
	case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType, *arrow.StructType, *arrow.MapType:
		c.typ = "json"
		c.value = func(a arrow.Array, i int) any {
			b, err := json.Marshal(storage(a).GetOneForMarshal(i))
			if err != nil {
				return err
			}
			return string(b)
		}
	default:
		return c, fmt.Errorf("mysql: column %q has unsupported Arrow type %s", f.Name, f.Type)
	}
	return c, nil
}

// decimalColumn is DECIMAL where MySQL's 65 digits and scale of 30 hold the
// type, and text where they don't.
func decimalColumn(p, s int32, text func(arrow.Array, int) string) (string, func(arrow.Array, int) any) {
	value := func(a arrow.Array, i int) any { return text(a, i) }
	if p > 65 || s > 30 || s < 0 {
		return "longtext", value
	}
	return fmt.Sprintf("decimal(%d,%d)", p, s), value
}

func finite(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return f
}

// inRange formats t, or is an error if DATE and DATETIME can't hold it.
func inRange(t time.Time, layout string) any {
	if t.Year() < 1 || t.Year() > 9999 {
		return fmt.Errorf("%s is outside the years 1 to 9999 that MySQL holds", t.Format(time.RFC3339Nano))
	}
	return t.Format(layout)
}

// clock formats a time of day as TIME takes it.
func clock(d time.Duration) string {
	us := d.Microseconds()
	sign := ""
	if us < 0 {
		sign, us = "-", -us
	}
	return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, us/3_600_000_000, us/60_000_000%60, us/1_000_000%60, us%1_000_000)
}

var (
	displayWidth = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|bigint)\(\d+\)`)
	decimalType  = regexp.MustCompile(`^decimal\((\d+),(\d+)\)$`)
	sizedType    = regexp.MustCompile(`^(varchar|varbinary)\((\d+)\)$`)
)

// normType spells a COLUMN_TYPE as columnOf does: integers without the
// display widths MariaDB shows, but tinyint(1), which is a boolean.
func normType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	if !strings.HasPrefix(t, "tinyint(1)") {
		t = displayWidth.ReplaceAllString(t, "$1")
	}
	return strings.TrimSuffix(t, " zerofill")
}

var intRank = map[string]int{"tinyint": 1, "smallint": 2, "mediumint": 3, "int": 4, "bigint": 5}

// intDigits is how many digits each integer type needs.
var intDigits = map[string]int{
	"tinyint": 3, "smallint": 5, "mediumint": 7, "int": 10, "bigint": 19,
	"tinyint unsigned": 3, "smallint unsigned": 5, "mediumint unsigned": 8, "int unsigned": 10, "bigint unsigned": 20,
}

// widens reports whether a column of type from can become type to without
// losing values; bigint to double counts, as it does for the engine.
func widens(from, to string) bool {
	switch {
	case from == to:
		return true
	case to == "longtext":
		return !binary(from)
	case to == "longblob":
		return binary(from)
	case from == "float" && to == "double":
		return true
	}
	fi, fu := intType(from)
	ti, tu := intType(to)
	if fi > 0 && ti > 0 {
		// An unsigned integer fits a wider signed one.
		return (fu == tu && ti >= fi) || (fu && !tu && ti > fi)
	}
	if fi > 0 && (to == "double" || to == "float" && fi <= 2) {
		return true
	}
	if m := sizedType.FindStringSubmatch(to); m != nil {
		n, _ := strconv.Atoi(m[2])
		f := sizedType.FindStringSubmatch(from)
		return f != nil && f[1] == m[1] && atoi(f[2]) <= n
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

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// intType is an integer type's rank, 0 for other types, and whether it's
// unsigned.
func intType(t string) (int, bool) {
	base, unsigned := strings.CutSuffix(t, " unsigned")
	return intRank[base], unsigned
}

func binary(t string) bool {
	return t == "longblob" || t == "blob" || t == "mediumblob" || t == "tinyblob" || strings.HasPrefix(t, "varbinary(") || strings.HasPrefix(t, "binary(")
}

func decimalPS(t string) (p, s int, ok bool) {
	m := decimalType.FindStringSubmatch(t)
	if m == nil {
		return 0, 0, false
	}
	return atoi(m[1]), atoi(m[2]), true
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

func stringValue(a arrow.Array, i int) any { return str(a, i) }

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
