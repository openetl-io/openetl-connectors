package postgres

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5/pgtype"
)

// column is a target column. typ is spelled as format_type() prints it.
type column struct {
	name string
	typ  string
	// The value of row i, which is not null, or an error.
	value func(arr arrow.Array, i int) any
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
		c.typ = "jsonb"
		c.value = func(a arrow.Array, i int) any { return json.RawMessage(str(a, i)) }
		return c, nil
	case ext == uuidExtension && dt.ID() == arrow.FIXED_SIZE_BINARY:
		c.typ = "uuid"
		c.value = func(a arrow.Array, i int) any {
			var u [16]byte
			copy(u[:], storage(a).(*array.FixedSizeBinary).Value(i))
			return u
		}
		return c, nil
	}

	switch t := dt.(type) {
	case *arrow.NullType:
		c.typ, c.value = "text", nil
	case *arrow.BooleanType:
		c.typ = "boolean"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Boolean).Value(i) }
	case *arrow.Int8Type:
		c.typ = "smallint"
		c.value = func(a arrow.Array, i int) any { return int16(storage(a).(*array.Int8).Value(i)) }
	case *arrow.Int16Type:
		c.typ = "smallint"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Int16).Value(i) }
	case *arrow.Int32Type:
		c.typ = "integer"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Int32).Value(i) }
	case *arrow.Int64Type:
		c.typ = "bigint"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Int64).Value(i) }
	case *arrow.Uint8Type:
		c.typ = "smallint"
		c.value = func(a arrow.Array, i int) any { return int16(storage(a).(*array.Uint8).Value(i)) }
	case *arrow.Uint16Type:
		c.typ = "integer"
		c.value = func(a arrow.Array, i int) any { return int32(storage(a).(*array.Uint16).Value(i)) }
	case *arrow.Uint32Type:
		c.typ = "bigint"
		c.value = func(a arrow.Array, i int) any { return int64(storage(a).(*array.Uint32).Value(i)) }
	case *arrow.Float32Type:
		c.typ = "real"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Float32).Value(i) }
	case *arrow.Float64Type:
		c.typ = "double precision"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Float64).Value(i) }
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		c.typ = "text"
		c.value = stringValue
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType:
		c.typ = "bytea"
		c.value = func(a arrow.Array, i int) any {
			return storage(a).(interface{ Value(int) []byte }).Value(i)
		}
	case *arrow.Date32Type:
		c.typ = "date"
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Date32).Value(i).ToTime() }
	case *arrow.Time64Type:
		c.typ = "time without time zone"
		div := int64(1)
		if t.Unit == arrow.Nanosecond {
			div = 1000
		}
		c.value = func(a arrow.Array, i int) any {
			return pgtype.Time{Microseconds: int64(storage(a).(*array.Time64).Value(i)) / div, Valid: true}
		}
	case *arrow.TimestampType:
		c.typ = "timestamp without time zone"
		if t.TimeZone != "" {
			c.typ = "timestamp with time zone"
		}
		unit := t.Unit
		c.value = func(a arrow.Array, i int) any { return storage(a).(*array.Timestamp).Value(i).ToTime(unit) }
	case *arrow.Decimal128Type:
		c.typ = fmt.Sprintf("numeric(%d,%d)", t.Precision, t.Scale)
		scale := t.Scale
		c.value = func(a arrow.Array, i int) any {
			return pgtype.Numeric{Int: storage(a).(*array.Decimal128).Value(i).BigInt(), Exp: -scale, Valid: true}
		}
	case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType, *arrow.StructType, *arrow.MapType:
		c.typ = "jsonb"
		c.value = func(a arrow.Array, i int) any {
			b, err := json.Marshal(storage(a).GetOneForMarshal(i))
			if err != nil {
				return err
			}
			return json.RawMessage(b)
		}
	default:
		return c, fmt.Errorf("postgres: column %q has unsupported Arrow type %s", f.Name, f.Type)
	}
	return c, nil
}

// widens reports whether a column of type from can become type to without
// losing values; bigint to double precision counts, as it does for the
// engine.
func widens(from, to string) bool {
	if from == to || to == "text" {
		return true
	}
	switch from + " " + to {
	case "smallint integer", "smallint bigint", "smallint real", "smallint double precision",
		"integer bigint", "integer double precision", "bigint double precision", "real double precision":
		return true
	}
	p2, s2, ok := numericPS(to)
	if !ok {
		return false
	}
	if n, ok := map[string]int{"smallint": 5, "integer": 10, "bigint": 19}[from]; ok {
		return p2-s2 >= n
	}
	p1, s1, ok := numericPS(from)
	return ok && s2 >= s1 && p2-s2 >= p1-s1
}

// numericPS parses numeric(p,s) as format_type prints it.
func numericPS(typ string) (p, s int, ok bool) {
	inner, found := strings.CutPrefix(typ, "numeric(")
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
