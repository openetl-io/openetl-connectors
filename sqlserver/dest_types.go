package sqlserver

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	mssql "github.com/microsoft/go-mssqldb"
)

// column is a target column. typ is spelled as a column definition spells
// it, as sqlType prints a catalog's.
type column struct {
	name string
	typ  string
	// The value of row i, which is not null, as bulk copy takes it; nil for
	// null, or an error.
	value func(arr arrow.Array, i int) any
	// A text column's collation, as the catalog has it; empty otherwise, and
	// for a column not read from the catalog.
	collation string
}

// A merge's key columns of text and binary are bounded, so that an index
// can hold them: SQL Server's index keys take 900 bytes, or 1,700 for a
// nonclustered index.
const (
	keyText, keyChars   = "nvarchar(450)", 450
	keyBinary, keyBytes = "varbinary(900)", 900
)

// keyed bounds the key columns of cols, whose values then fail a write
// where they don't fit.
func keyed(cols []column, key []string) {
	for i, c := range cols {
		if !slices.Contains(key, c.name) || c.value == nil {
			continue
		}
		value := c.value
		switch c.typ {
		case "nvarchar(max)":
			cols[i].typ = keyText
			cols[i].value = func(a arrow.Array, j int) any {
				v := value(a, j)
				n := 0
				for _, r := range v.(string) {
					n += utf16.RuneLen(r)
				}
				if n > keyChars {
					return fmt.Errorf("a key's value of %d characters is longer than the %d a key column holds", n, keyChars)
				}
				return v
			}
		case "varbinary(max)":
			cols[i].typ = keyBinary
			cols[i].value = func(a arrow.Array, j int) any {
				v := value(a, j)
				if n := len(v.([]byte)); n > keyBytes {
					return fmt.Errorf("a key's value of %d bytes is longer than the %d a key column holds", n, keyBytes)
				}
				return v
			}
		}
	}
}

// binaryCollation compares text by code point.
const binaryCollation = "Latin1_General_100_BIN2"

func isText(typ string) bool {
	base, _, _ := strings.Cut(typ, "(")
	return base == "char" || base == "varchar" || base == "nchar" || base == "nvarchar"
}

func isBinary(typ string) bool {
	base, _, _ := strings.Cut(typ, "(")
	return base == "binary" || base == "varbinary"
}

// keyEqual compares key column k of rows a and b as the source's values
// differ. SQL Server's = ignores trailing spaces and trailing zero bytes,
// and compares text as its collation does: so text is compared again, by
// code point with a character after it, and binary by length too. The
// first comparison takes k's collation, if it has one, so that it can seek
// k's index, whatever b's collation.
func keyEqual(a, b string, k column) string {
	x, y := a+"."+quote(k.name), b+"."+quote(k.name)
	eq := x + " = " + y
	switch {
	case isText(k.typ):
		if k.collation != "" {
			eq += " COLLATE " + k.collation
		}
		return eq + " AND " + exact(x) + " = " + exact(y)
	case isBinary(k.typ):
		return eq + " AND DATALENGTH(" + x + ") = DATALENGTH(" + y + ")"
	}
	return eq
}

// keyPart is what partitions rel's rows by key column k as keyEqual
// compares them.
func keyPart(rel string, k column) string {
	x := rel + "." + quote(k.name)
	switch {
	case isText(k.typ):
		return exact(x)
	case isBinary(k.typ):
		return x + ", DATALENGTH(" + x + ")"
	}
	return x
}

func exact(x string) string {
	return "(CONVERT(nvarchar(max), " + x + ") + N'.') COLLATE " + binaryCollation
}

const (
	extensionNameKey = "ARROW:extension:name"
	jsonExtension    = "arrow.json"
	uuidExtension    = "arrow.uuid"
	// The engine names each stream's table in its schema's metadata.
	tableKey = "openetl.table"
)

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
		c.typ, c.value = "nvarchar(max)", stringValue
		return c, nil
	case ext == uuidExtension && dt.ID() == arrow.FIXED_SIZE_BINARY:
		c.typ = "uniqueidentifier"
		c.value = func(a arrow.Array, i int) any {
			var u mssql.UniqueIdentifier
			copy(u[:], storage(a).(*array.FixedSizeBinary).Value(i))
			return u
		}
		return c, nil
	}

	switch t := dt.(type) {
	case *arrow.NullType:
		c.typ, c.value = "nvarchar(max)", nil
	case *arrow.BooleanType:
		c.typ = "bit"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Boolean).Value(i) }
	case *arrow.Int8Type:
		c.typ = "smallint"
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
		c.typ = "tinyint"
		c.value = func(a arrow.Array, i int) any { return int64(storage(a).(*array.Uint8).Value(i)) }
	case *arrow.Uint16Type:
		c.typ = "int"
		c.value = func(a arrow.Array, i int) any { return int64(storage(a).(*array.Uint16).Value(i)) }
	case *arrow.Uint32Type:
		c.typ = "bigint"
		c.value = func(a arrow.Array, i int) any { return int64(storage(a).(*array.Uint32).Value(i)) }
	case *arrow.Uint64Type:
		c.typ = "decimal(20,0)"
		c.value = func(a arrow.Array, i int) any { return strconv.FormatUint(storage(a).(*array.Uint64).Value(i), 10) }
	case *arrow.Float32Type:
		// SQL Server's floats hold no NaN or infinity: they land as null.
		c.typ = "real"
		c.value = func(a arrow.Array, i int) any { return finite(float64(storage(a).(*array.Float32).Value(i))) }
	case *arrow.Float64Type:
		c.typ = "float"
		c.value = func(a arrow.Array, i int) any { return finite(storage(a).(*array.Float64).Value(i)) }
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		c.typ, c.value = "nvarchar(max)", stringValue
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType:
		c.typ = "varbinary(max)"
		c.value = func(a arrow.Array, i int) any {
			return storage(a).(interface{ Value(int) []byte }).Value(i)
		}
	case *arrow.Date32Type:
		c.typ = "date"
		c.value = func(a arrow.Array, i int) any { return inRange(storage(a).(*array.Date32).Value(i).ToTime()) }
	case *arrow.Time64Type:
		c.typ = "time(6)"
		if t.Unit == arrow.Nanosecond {
			c.typ = "time(7)"
		}
		unit := t.Unit
		c.value = func(a arrow.Array, i int) any {
			d := time.Duration(storage(a).(*array.Time64).Value(i))
			if unit == arrow.Microsecond {
				d *= time.Microsecond
			}
			return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Add(d)
		}
	case *arrow.TimestampType:
		scale := "6"
		if t.Unit == arrow.Nanosecond {
			scale = "7"
		}
		c.typ = "datetime2(" + scale + ")"
		if t.TimeZone != "" {
			c.typ = "datetimeoffset(" + scale + ")"
		}
		unit := t.Unit
		c.value = func(a arrow.Array, i int) any { return inRange(storage(a).(*array.Timestamp).Value(i).ToTime(unit)) }
	case *arrow.Decimal128Type:
		if t.Precision > 38 || t.Scale < 0 || t.Scale > t.Precision {
			return c, fmt.Errorf("sqlserver: column %q is %s, past SQL Server's decimal(38)", f.Name, f.Type)
		}
		c.typ = fmt.Sprintf("decimal(%d,%d)", t.Precision, t.Scale)
		scale := t.Scale
		c.value = func(a arrow.Array, i int) any { return formatDecimal(storage(a).(*array.Decimal128).Value(i), scale) }
	case *arrow.MonthDayNanoIntervalType:
		// As ISO 8601: P1M2DT3.000000004S.
		c.typ = "nvarchar(max)"
		c.value = func(a arrow.Array, i int) any {
			v := storage(a).(*array.MonthDayNanoInterval).Value(i)
			return fmt.Sprintf("P%dM%dDT%sS", v.Months, v.Days, seconds(v.Nanoseconds))
		}
	case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType, *arrow.StructType, *arrow.MapType:
		c.typ = "nvarchar(max)"
		c.value = func(a arrow.Array, i int) any {
			b, err := json.Marshal(storage(a).GetOneForMarshal(i))
			if err != nil {
				return err
			}
			return string(b)
		}
	default:
		return c, fmt.Errorf("sqlserver: column %q has unsupported Arrow type %s", f.Name, f.Type)
	}
	return c, nil
}

func finite(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return f
}

// inRange is t, or an error past what date, datetime2 and datetimeoffset
// hold.
func inRange(t time.Time) any {
	if t.Year() < 1 || t.Year() > 9999 {
		return fmt.Errorf("%s is past what SQL Server's dates hold, years 1 to 9999", t.Format(time.RFC3339Nano))
	}
	return t
}

// seconds spells nanoseconds as seconds, with the fraction it has.
func seconds(ns int64) string {
	sign := ""
	u := uint64(ns)
	if ns < 0 {
		sign, u = "-", uint64(-(ns+1))+1
	}
	s := sign + strconv.FormatUint(u/1e9, 10)
	if frac := u % 1e9; frac != 0 {
		s += "." + strings.TrimRight(fmt.Sprintf("%09d", frac), "0")
	}
	return s
}

// widens reports whether a column of type from can become type to without
// losing values; bigint to float counts, as it does for the engine.
func widens(from, to string) bool {
	from, to = strings.Replace(from, "numeric(", "decimal(", 1), strings.Replace(to, "numeric(", "decimal(", 1)
	if from == to || to == "nvarchar(max)" {
		return true
	}
	// Text into nvarchar, and binary into varbinary, at least as long, as a
	// merge's bounded key columns take.
	if base, n, ok := sized(to); ok && (base == "nvarchar" && isText(from) || base == "varbinary" && isBinary(from)) {
		_, m, ok := sized(from)
		return ok && m <= n
	}
	switch from + " " + to {
	case "tinyint smallint", "tinyint int", "tinyint bigint", "tinyint real", "tinyint float",
		"smallint int", "smallint bigint", "smallint real", "smallint float",
		"int bigint", "int float", "bigint float", "real float",
		"time(6) time(7)", "datetime2(6) datetime2(7)", "datetimeoffset(6) datetimeoffset(7)":
		return true
	}
	p2, s2, ok := decimalPS(to)
	if !ok {
		return false
	}
	if n, ok := map[string]int{"tinyint": 3, "smallint": 5, "int": 10, "bigint": 19}[from]; ok {
		return p2-s2 >= n
	}
	p1, s1, ok := decimalPS(from)
	return ok && s2 >= s1 && p2-s2 >= p1-s1
}

// longer reports whether to is from at a greater length, short of max: the
// change SQL Server makes to a column in an index.
func longer(from, to string) bool {
	b1, _, ok1 := sized(from)
	b2, n, ok2 := sized(to)
	return ok1 && ok2 && b1 == b2 && n != math.MaxInt && (b1 == "varchar" || b1 == "nvarchar" || b1 == "varbinary")
}

// sized parses a type of a length, such as nvarchar(450); max is the
// longest.
func sized(typ string) (base string, n int, ok bool) {
	base, inner, found := strings.Cut(typ, "(")
	inner, closed := strings.CutSuffix(inner, ")")
	if !found || !closed {
		return "", 0, false
	}
	if inner == "max" {
		return base, math.MaxInt, true
	}
	n, err := strconv.Atoi(inner)
	return base, n, err == nil
}

// decimalPS parses decimal(p,s).
func decimalPS(typ string) (p, s int, ok bool) {
	inner, found := strings.CutPrefix(typ, "decimal(")
	if !found {
		return 0, 0, false
	}
	inner, found = strings.CutSuffix(inner, ")")
	ps, ss, comma := strings.Cut(inner, ",")
	if !found || !comma {
		return 0, 0, false
	}
	p, perr := strconv.Atoi(ps)
	s, serr := strconv.Atoi(ss)
	return p, s, perr == nil && serr == nil
}

// indexable reports whether a column of the type can be in an index's key.
func indexable(typ string) bool {
	return !strings.HasSuffix(typ, "(max)") && typ != "xml" && typ != "text" && typ != "ntext" && typ != "image" &&
		typ != "geography" && typ != "geometry" && typ != "sql_variant"
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

func stringValue(a arrow.Array, i int) any {
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
