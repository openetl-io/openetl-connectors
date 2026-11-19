package sqlserver

import (
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// sourceColumn is a table's column as sys.columns has it.
type sourceColumn struct {
	name string
	// The system type's name, such as nvarchar; an alias type's base type;
	// a CLR type's own name, which can hold any character.
	typ string
	// In bytes; -1 for (max).
	maxLength     int
	precision     int
	scale         int
	notNull       bool
	computed      bool
	collation     string
	isUserDefined bool
}

// sqlType spells the column's type as a column definition would.
func (c sourceColumn) sqlType() string {
	switch c.typ {
	case "decimal", "numeric":
		return fmt.Sprintf("%s(%d,%d)", c.typ, c.precision, c.scale)
	case "char", "varchar", "binary", "varbinary":
		if c.maxLength < 0 {
			return c.typ + "(max)"
		}
		return fmt.Sprintf("%s(%d)", c.typ, c.maxLength)
	case "nchar", "nvarchar":
		if c.maxLength < 0 {
			return c.typ + "(max)"
		}
		return fmt.Sprintf("%s(%d)", c.typ, c.maxLength/2)
	case "datetime2", "datetimeoffset", "time":
		return fmt.Sprintf("%s(%d)", c.typ, c.scale)
	case "timestamp", "rowversion":
		return "binary(8)"
	}
	if !systemName(c.typ) {
		return quote(c.typ)
	}
	return c.typ
}

// systemName reports whether a type's name is spelled as SQL Server's own
// are, in lowercase letters, digits and underscores, which need no quotes.
func systemName(typ string) bool {
	return typ != "" && strings.Trim(typ, "abcdefghijklmnopqrstuvwxyz0123456789_") == ""
}

// selectExpr is how a query selects the column for its colBuilder: the CLR
// types and sql_variant as text.
func (c sourceColumn) selectExpr(alias string) string {
	q := quote(c.name)
	if alias != "" {
		q = alias + "." + q
	}
	switch c.typ {
	case "geography", "geometry", "hierarchyid":
		return "CONVERT(nvarchar(max), " + q + ".ToString())"
	case "sql_variant":
		// A variant's varchar holds up to 8000 characters, and its binary as
		// hex twice as many. A variant converted to nvarchar(max) directly
		// keeps only 4000 characters, so a varchar goes through its own type.
		return "CASE WHEN SQL_VARIANT_PROPERTY(" + q + ", 'BaseType') IN ('datetime', 'smalldatetime', 'datetime2', 'date', 'time', 'datetimeoffset') " +
			"THEN CONVERT(nvarchar(4000), " + q + ", 126) " +
			"WHEN SQL_VARIANT_PROPERTY(" + q + ", 'BaseType') IN ('binary', 'varbinary', 'timestamp') THEN CONVERT(nvarchar(max), CONVERT(varbinary(8000), " + q + "), 1) " +
			"WHEN SQL_VARIANT_PROPERTY(" + q + ", 'BaseType') IN ('varchar', 'char') THEN CONVERT(nvarchar(max), CONVERT(varchar(8000), " + q + ")) " +
			"WHEN SQL_VARIANT_PROPERTY(" + q + ", 'BaseType') IN ('float', 'real') THEN CONVERT(nvarchar(4000), CONVERT(float, " + q + "), 3) " +
			"ELSE CONVERT(nvarchar(max), " + q + ") END"
	}
	return q
}

// The styles of CONVERT that spell a type as text that converts back to the
// same value.
func (c sourceColumn) style() string {
	switch c.typ {
	case "date", "datetime", "smalldatetime", "datetime2", "time":
		return ", 126"
	case "datetimeoffset":
		return ", 127"
	case "binary", "varbinary", "timestamp", "rowversion":
		return ", 1"
	case "float", "real":
		return ", 3"
	case "money", "smallmoney":
		return ", 2"
	}
	return ""
}

// textOf spells expr, a value of the column's type, as nvarchar text that
// fromText reads back as the same value: how keys and cursors are kept.
func (c sourceColumn) textOf(expr string) string {
	if c.typ == "timestamp" || c.typ == "rowversion" {
		// A rowversion converts to text only as binary.
		expr = "CONVERT(binary(8), " + expr + ")"
	}
	// nvarchar(4000) spells every type's values but a char's or varchar's of
	// more than 4000 characters.
	to := "nvarchar(4000)"
	if (c.typ == "char" || c.typ == "varchar") && c.maxLength > 4000 {
		to = "nvarchar(max)"
	}
	return "CONVERT(" + to + ", " + expr + c.style() + ")"
}

// fromText is the value of the column's type that text, an nvarchar
// expression textOf made, spells. Character types convert under the
// column's collation, whose code page their values are in.
func (c sourceColumn) fromText(text string) string {
	switch c.typ {
	case "char", "varchar", "text":
		if c.collation != "" {
			text = "(" + text + " COLLATE " + c.collation + ")"
		}
	case "binary", "varbinary", "timestamp", "rowversion":
		// Style 1 reads 0x hex, from varchar.
		text = "CONVERT(varchar(8000), " + text + ")"
	}
	return "CONVERT(" + c.sqlType() + ", " + text + c.style() + ")"
}

// colBuilder decodes one column's non-null values, as the driver scans
// them, into an Arrow builder.
type colBuilder struct {
	b   array.Builder
	add func(v any) error
	// The type the catalog declares, where b's differs: the engine parses
	// the text b holds into it.
	decl arrow.DataType
}

func badValue(v any, want string) error { return fmt.Errorf("a %T value, want %s", v, want) }

func newColBuilder(mem memory.Allocator, c sourceColumn) (*colBuilder, error) {
	cb := &colBuilder{}
	switch c.typ {
	case "bit":
		b := array.NewBooleanBuilder(mem)
		cb.b, cb.add = b, func(v any) error {
			x, ok := v.(bool)
			if !ok {
				return badValue(v, "bool")
			}
			b.Append(x)
			return nil
		}
	case "tinyint":
		// OpenETL's integers are signed: 16 bits hold 0 to 255.
		b := array.NewInt16Builder(mem)
		cb.b, cb.add = b, intAdd(func(x int64) { b.Append(int16(x)) }, 0, math.MaxUint8)
	case "smallint":
		b := array.NewInt16Builder(mem)
		cb.b, cb.add = b, intAdd(func(x int64) { b.Append(int16(x)) }, math.MinInt16, math.MaxInt16)
	case "int":
		b := array.NewInt32Builder(mem)
		cb.b, cb.add = b, intAdd(func(x int64) { b.Append(int32(x)) }, math.MinInt32, math.MaxInt32)
	case "bigint":
		b := array.NewInt64Builder(mem)
		cb.b, cb.add = b, intAdd(b.Append, math.MinInt64, math.MaxInt64)
	case "real":
		b := array.NewFloat32Builder(mem)
		cb.b, cb.add = b, func(v any) error {
			x, ok := v.(float64)
			if !ok {
				return badValue(v, "float64")
			}
			b.Append(float32(x))
			return nil
		}
	case "float":
		b := array.NewFloat64Builder(mem)
		cb.b, cb.add = b, func(v any) error {
			x, ok := v.(float64)
			if !ok {
				return badValue(v, "float64")
			}
			b.Append(x)
			return nil
		}
	case "decimal", "numeric", "money", "smallmoney":
		t := &arrow.Decimal128Type{Precision: int32(c.precision), Scale: int32(c.scale)}
		switch c.typ {
		case "money":
			t.Precision, t.Scale = 19, 4
		case "smallmoney":
			t.Precision, t.Scale = 10, 4
		}
		b := array.NewDecimal128Builder(mem, t)
		cb.b, cb.add = b, func(v any) error {
			s, ok := text(v)
			if !ok {
				return badValue(v, "decimal text")
			}
			n, err := parseDecimal(s, t.Scale)
			if err != nil {
				return err
			}
			b.Append(n)
			return nil
		}
	case "date":
		b := array.NewDate32Builder(mem)
		cb.b, cb.add = b, timeAdd(func(t time.Time) { b.Append(arrow.Date32(daysSinceEpoch(t))) })
	case "time":
		// The engine keeps times in microseconds: time(7)'s seventh digit,
		// 100 ns, is dropped.
		b := array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Microsecond})
		cb.b, cb.add = b, timeAdd(func(t time.Time) {
			us := int64(t.Hour())*int64(time.Hour/time.Microsecond) + int64(t.Minute())*int64(time.Minute/time.Microsecond) +
				int64(t.Second())*int64(time.Second/time.Microsecond) + int64(t.Nanosecond()/1000)
			b.Append(arrow.Time64(us))
		})
	case "datetime", "smalldatetime", "datetime2", "datetimeoffset":
		t := &arrow.TimestampType{Unit: arrow.Microsecond}
		if c.typ == "datetimeoffset" {
			t.TimeZone = "UTC"
		}
		if c.scale > 6 {
			// A seventh digit, 100 ns, arrives as text, which the engine
			// parses: into microseconds, or nanoseconds for a stream that keeps
			// them, which end in 2262, where SQL Server's run to 9999.
			layout := "2006-01-02T15:04:05.0000000"
			if t.TimeZone != "" {
				layout += "Z07:00"
			}
			b := array.NewStringBuilder(mem)
			cb.b, cb.decl, cb.add = b, t, timeAdd(func(v time.Time) {
				if t.TimeZone != "" {
					v = v.UTC()
				}
				b.Append(v.Format(layout))
			})
			break
		}
		b := array.NewTimestampBuilder(mem, t)
		cb.b, cb.add = b, timeAdd(func(v time.Time) { b.Append(arrow.Timestamp(v.UnixMicro())) })
	case "uniqueidentifier":
		b := array.NewExtensionBuilder(mem, extensions.NewUUIDType())
		fb := b.Builder.(*array.FixedSizeBinaryBuilder)
		cb.b, cb.add = b, func(v any) error {
			x, ok := v.([]byte)
			if !ok || len(x) != 16 {
				return badValue(v, "16 bytes")
			}
			fb.Append(guidToUUID(x))
			return nil
		}
	case "binary", "varbinary", "image", "timestamp", "rowversion":
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		cb.b, cb.add = b, func(v any) error {
			x, ok := v.([]byte)
			if !ok {
				return badValue(v, "bytes")
			}
			b.Append(x)
			return nil
		}
	default:
		// Text, xml, sql_variant, the CLR types as text, and any other type the
		// driver gives as text.
		b := array.NewStringBuilder(mem)
		cb.b, cb.add = b, func(v any) error {
			s, ok := text(v)
			if !ok {
				return badValue(v, "text")
			}
			b.Append(s)
			return nil
		}
	}
	return cb, nil
}

func intAdd(appendInt func(int64), lo, hi int64) func(any) error {
	return func(v any) error {
		x, ok := v.(int64)
		if !ok {
			return badValue(v, "int64")
		}
		if x < lo || x > hi {
			return fmt.Errorf("%d is out of the column's range", x)
		}
		appendInt(x)
		return nil
	}
}

func timeAdd(appendTime func(time.Time)) func(any) error {
	return func(v any) error {
		x, ok := v.(time.Time)
		if !ok {
			return badValue(v, "time.Time")
		}
		appendTime(x)
		return nil
	}
}

func text(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	}
	return "", false
}

// daysSinceEpoch is the days from 1970-01-01 to t's date, as its wall clock
// has it.
func daysSinceEpoch(t time.Time) int32 {
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return int32(d.Unix() / 86400)
}

// guidToUUID turns a uniqueidentifier's bytes, as SQL Server sends them with
// its first three groups little-endian, into the RFC 4122 order.
func guidToUUID(g []byte) []byte {
	u := make([]byte, 16)
	copy(u, g)
	u[0], u[1], u[2], u[3] = g[3], g[2], g[1], g[0]
	u[4], u[5] = g[5], g[4]
	u[6], u[7] = g[7], g[6]
	return u
}

// parseDecimal reads a decimal's text, as SQL Server spells it, at scale.
func parseDecimal(s string, scale int32) (decimal128.Num, error) {
	s = strings.TrimSpace(s)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	whole, frac, _ := strings.Cut(s, ".")
	if int32(len(frac)) > scale {
		if strings.TrimRight(frac[scale:], "0") != "" {
			return decimal128.Num{}, fmt.Errorf("%q has more than %d decimal places", s, scale)
		}
		frac = frac[:scale]
	}
	frac += strings.Repeat("0", int(scale)-len(frac))
	n, ok := new(big.Int).SetString(whole+frac, 10)
	if !ok {
		return decimal128.Num{}, fmt.Errorf("%q isn't a decimal", s)
	}
	if neg {
		n.Neg(n)
	}
	if n.BitLen() > 127 {
		return decimal128.Num{}, fmt.Errorf("%q is past a decimal's range", s)
	}
	return decimal128.FromBigInt(n), nil
}

// formatDecimal spells a decimal at scale.
func formatDecimal(n decimal128.Num, scale int32) string {
	s := n.BigInt().String()
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	if scale > 0 {
		if len(s) <= int(scale) {
			s = strings.Repeat("0", int(scale)-len(s)+1) + s
		}
		s = s[:len(s)-int(scale)] + "." + s[len(s)-int(scale):]
	}
	if neg {
		s = "-" + s
	}
	return s
}
