package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// colBuilder decodes one column's non-null values into an Arrow builder.
// Types without a binary decoder are read as text into a string column;
// numerics too, declared as decimals for the engine to parse.
type colBuilder struct {
	format int16
	b      array.Builder
	add    func(v []byte) error
	// The column's logical type, if the builder's isn't.
	declared arrow.DataType
	// past, if set, renders a value the builder's type can't hold as text.
	past func(v []byte) (string, bool)
}

// Microseconds and days from 1970-01-01 to Postgres's epoch, 2000-01-01.
// Postgres's infinity and -infinity are the types' largest and smallest
// values, which the engine also reads as infinite, so they pass unshifted.
const (
	pgEpochMicros = 946_684_800_000_000
	pgEpochDays   = 10_957
)

// timestampText renders microseconds since 2000-01-01 as text the engine
// parses. Postgres's timestamps go on to 294276, past where Arrow's
// microseconds since 1970 end (and the last of those reads as infinity).
func timestampText(us int64, zoned bool) string {
	const day = 86_400_000_000
	s := time.Date(2000, 1, 1+int(us/day), 0, 0, 0, int(us%day)*1000, time.UTC).Format("2006-01-02T15:04:05.999999")
	if zoned {
		s += "Z"
	}
	return s
}

func fixed(n int, f func(v []byte)) func([]byte) error {
	return func(v []byte) error {
		if len(v) != n {
			return fmt.Errorf("a %d-byte value, want %d", len(v), n)
		}
		f(v)
		return nil
	}
}

// unpad drops the spaces Postgres pads a char(n) value with, as its cast to
// text does: they aren't part of the value.
func unpad(v []byte) []byte { return bytes.TrimRight(v, " ") }

// beInt16, beInt32 and beInt64 read big-endian two's complement integers,
// as Postgres sends them.
func beInt16(v []byte) int16 { return int16(v[0])<<8 | int16(v[1]) }

func beInt32(v []byte) int32 {
	return int32(beInt16(v))<<16 | int32(binary.BigEndian.Uint16(v[2:]))
}

func beInt64(v []byte) int64 {
	return int64(beInt32(v))<<32 | int64(binary.BigEndian.Uint32(v[4:]))
}

func newColBuilder(mem memory.Allocator, c sourceColumn) (*colBuilder, error) {
	be := binary.BigEndian
	cb := &colBuilder{format: pgx.BinaryFormatCode}
	switch c.oid {
	case pgtype.BoolOID:
		b := array.NewBooleanBuilder(mem)
		cb.b, cb.add = b, fixed(1, func(v []byte) { b.Append(v[0] != 0) })
	case pgtype.Int2OID:
		b := array.NewInt16Builder(mem)
		cb.b, cb.add = b, fixed(2, func(v []byte) { b.Append(beInt16(v)) })
	case pgtype.Int4OID:
		b := array.NewInt32Builder(mem)
		cb.b, cb.add = b, fixed(4, func(v []byte) { b.Append(beInt32(v)) })
	case pgtype.Int8OID:
		b := array.NewInt64Builder(mem)
		cb.b, cb.add = b, fixed(8, func(v []byte) { b.Append(beInt64(v)) })
	case pgtype.OIDOID:
		b := array.NewInt64Builder(mem)
		cb.b, cb.add = b, fixed(4, func(v []byte) { b.Append(int64(be.Uint32(v))) })
	case pgtype.Float4OID:
		b := array.NewFloat32Builder(mem)
		cb.b, cb.add = b, fixed(4, func(v []byte) { b.Append(math.Float32frombits(be.Uint32(v))) })
	case pgtype.Float8OID:
		b := array.NewFloat64Builder(mem)
		cb.b, cb.add = b, fixed(8, func(v []byte) { b.Append(math.Float64frombits(be.Uint64(v))) })
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.NameOID:
		b := array.NewStringBuilder(mem)
		cb.b, cb.add = b, func(v []byte) error { b.BinaryBuilder.Append(v); return nil }
	case pgtype.BPCharOID:
		b := array.NewStringBuilder(mem)
		cb.b, cb.add = b, func(v []byte) error { b.BinaryBuilder.Append(unpad(v)); return nil }
	case pgtype.ByteaOID:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		cb.b, cb.add = b, func(v []byte) error { b.Append(v); return nil }
	case pgtype.DateOID:
		b := array.NewDate32Builder(mem)
		cb.b, cb.add = b, func(v []byte) error {
			if len(v) != 4 {
				return fmt.Errorf("a %d-byte date", len(v))
			}
			d := beInt32(v)
			if d != math.MaxInt32 && d != math.MinInt32 {
				d += pgEpochDays
			}
			b.Append(arrow.Date32(d))
			return nil
		}
	case pgtype.TimeOID:
		b := array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Microsecond})
		cb.b, cb.add = b, fixed(8, func(v []byte) { b.Append(arrow.Time64(beInt64(v))) })
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		t := &arrow.TimestampType{Unit: arrow.Microsecond}
		if c.oid == pgtype.TimestamptzOID {
			t.TimeZone = "UTC"
		}
		b := array.NewTimestampBuilder(mem, t)
		cb.b, cb.add = b, func(v []byte) error {
			if len(v) != 8 {
				return fmt.Errorf("a %d-byte timestamp", len(v))
			}
			us := beInt64(v)
			if us != math.MaxInt64 && us != math.MinInt64 {
				us += pgEpochMicros
			}
			b.Append(arrow.Timestamp(us))
			return nil
		}
		cb.past = func(v []byte) (string, bool) {
			if len(v) != 8 {
				return "", false
			}
			us := beInt64(v)
			if us == math.MaxInt64 || us < math.MaxInt64-pgEpochMicros {
				return "", false
			}
			return timestampText(us, t.TimeZone != ""), true
		}
	case pgtype.UUIDOID:
		b := array.NewExtensionBuilder(mem, extensions.NewUUIDType())
		fb := b.Builder.(*array.FixedSizeBinaryBuilder)
		cb.b, cb.add = b, fixed(16, func(v []byte) { fb.Append(v) })
	case pgtype.JSONOID, pgtype.JSONBOID:
		jt, err := extensions.NewJSONType(arrow.BinaryTypes.String)
		if err != nil {
			return nil, err
		}
		b := array.NewExtensionBuilder(mem, jt)
		sb := b.Builder.(*array.StringBuilder)
		jsonb := c.oid == pgtype.JSONBOID
		cb.b, cb.add = b, func(v []byte) error {
			if jsonb {
				if len(v) == 0 || v[0] != 1 {
					return errors.New("an unknown jsonb version")
				}
				v = v[1:]
			}
			sb.BinaryBuilder.Append(v)
			return nil
		}
	case pgtype.NumericOID:
		cb.declared = numericType(c.typmod)
	}
	if tt := typedText(c); tt != nil {
		b := array.NewBuilder(mem, tt.dt)
		cb.format, cb.b, cb.add = pgx.TextFormatCode, b, func(v []byte) error { return tt.add(b, v) }
	}
	if cb.b == nil {
		b := array.NewStringBuilder(mem)
		cb.format, cb.b, cb.add = pgx.TextFormatCode, b, func(v []byte) error { b.BinaryBuilder.Append(v); return nil }
	}
	return cb, nil
}

// declaredType is the type a column of c's type is declared as; nil if it
// can't be read.
func declaredType(c sourceColumn) arrow.DataType {
	cb, err := newColBuilder(memory.DefaultAllocator, c)
	if err != nil {
		return nil
	}
	defer cb.b.Release()
	if cb.declared != nil {
		return cb.declared
	}
	return cb.b.Type()
}

// numericType is the decimal a numeric column is declared as: its own
// precision and scale where Decimal128 holds them, otherwise decimal(38, 9).
func numericType(typmod int32) *arrow.Decimal128Type {
	if p, s, ok := numericTypmod(typmod); ok {
		return &arrow.Decimal128Type{Precision: p, Scale: s}
	}
	return &arrow.Decimal128Type{Precision: 38, Scale: 9}
}

// numericTypmod returns a numeric column's precision and scale if Decimal128
// can hold it.
func numericTypmod(typmod int32) (p, s int32, ok bool) {
	if typmod < 4 {
		return 0, 0, false
	}
	m := typmod - 4
	p = (m >> 16) & 0xffff
	s = m & 0x7ff
	if s&0x400 != 0 {
		s -= 0x800 // negative scales, from Postgres 15
	}
	return p, s, p >= 1 && p <= 38 && s >= 0 && s <= p
}

// textTyped is a column the engine can't parse from text, built from
// Postgres's text form: an interval, or a one-dimensional array whose items
// have a type of their own in Arrow.
type textTyped struct {
	dt  arrow.DataType
	add func(b array.Builder, v []byte) error
}

// The array types read as lists, by their items' types.
var listItems = map[uint32]arrow.DataType{
	pgtype.BoolArrayOID:    arrow.FixedWidthTypes.Boolean,
	pgtype.Int2ArrayOID:    arrow.PrimitiveTypes.Int16,
	pgtype.Int4ArrayOID:    arrow.PrimitiveTypes.Int32,
	pgtype.Int8ArrayOID:    arrow.PrimitiveTypes.Int64,
	pgtype.Float4ArrayOID:  arrow.PrimitiveTypes.Float32,
	pgtype.Float8ArrayOID:  arrow.PrimitiveTypes.Float64,
	pgtype.TextArrayOID:    arrow.BinaryTypes.String,
	pgtype.VarcharArrayOID: arrow.BinaryTypes.String,
	pgtype.BPCharArrayOID:  arrow.BinaryTypes.String,
}

// typedText is how c is built from text, nil if it goes as text, which the
// engine parses as its declared type. An array column goes as a list only
// if it was declared with one dimension.
func typedText(c sourceColumn) *textTyped {
	if c.oid == pgtype.IntervalOID {
		return &textTyped{dt: arrow.FixedWidthTypes.MonthDayNanoInterval, add: func(b array.Builder, v []byte) error {
			iv, err := parseInterval(string(v))
			if err != nil {
				return err
			}
			b.(*array.MonthDayNanoIntervalBuilder).Append(iv)
			return nil
		}}
	}
	item, ok := listItems[c.oid]
	// A view's columns, and some tables', don't say: their arrays may have
	// any number of dimensions.
	if !ok || c.dims != 1 {
		return nil
	}
	padded := c.oid == pgtype.BPCharArrayOID
	return &textTyped{dt: arrow.ListOf(item), add: func(b array.Builder, v []byte) error {
		items, err := arrayItems(v)
		if err != nil {
			return err
		}
		lb := b.(*array.ListBuilder)
		lb.Append(true)
		vb := lb.ValueBuilder()
		for _, it := range items {
			if it == nil {
				vb.AppendNull()
				continue
			}
			if padded {
				it = unpad(it)
			}
			if err := appendItem(vb, string(it)); err != nil {
				return dataInvalid("an array item %q: %v", it, err)
			}
		}
		return nil
	}}
}

// changeTyped is how a CDC read, which reads text, builds c: as typedText
// says, and a date or timestamp as a full refresh reads it, since the
// engine can't parse a BC one's text.
func changeTyped(c sourceColumn) *textTyped {
	switch c.oid {
	case pgtype.DateOID:
		return &textTyped{dt: arrow.FixedWidthTypes.Date32, add: func(b array.Builder, v []byte) error {
			d, err := pgTime(string(v), false)
			if err != nil {
				return err
			}
			b.(*array.Date32Builder).Append(arrow.Date32(d))
			return nil
		}}
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		t := &arrow.TimestampType{Unit: arrow.Microsecond}
		if c.oid == pgtype.TimestamptzOID {
			t.TimeZone = "UTC"
		}
		return &textTyped{dt: t, add: func(b array.Builder, v []byte) error {
			us, err := pgTime(string(v), true)
			if err != nil {
				return err
			}
			b.(*array.TimestampBuilder).Append(arrow.Timestamp(us))
			return nil
		}}
	}
	return typedText(c)
}

// pgTime reads a date, or with clock a timestamp, as Postgres writes it with
// DateStyle ISO: days, or microseconds, since 1970-01-01. A year past 9999
// has more digits, one before 1 ends in BC, and a timestamptz's offset
// follows its time. Infinities are the type's largest and smallest values,
// as a full refresh reads them.
func pgTime(s string, clock bool) (int64, error) {
	top, bottom := int64(math.MaxInt32), int64(math.MinInt32)
	if clock {
		top, bottom = math.MaxInt64, math.MinInt64
	}
	switch s {
	case "infinity":
		return top, nil
	case "-infinity":
		return bottom, nil
	}
	bad := func() (int64, error) { return 0, dataInvalid("a date or time %q", s) }
	text, bc := strings.CutSuffix(s, " BC")
	date, rest, _ := strings.Cut(text, " ")
	ys, md, _ := strings.Cut(date, "-")
	ms, ds, _ := strings.Cut(md, "-")
	y, err1 := strconv.Atoi(ys)
	m, err2 := strconv.Atoi(ms)
	d, err3 := strconv.Atoi(ds)
	if errors.Join(err1, err2, err3) != nil {
		return bad()
	}
	if bc {
		y = 1 - y
	}
	days := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Unix() / 86_400
	if !clock {
		return days, nil
	}
	const day = 86_400_000_000
	if days >= math.MaxInt64/day || days <= math.MinInt64/day {
		return 0, dataInvalid("a timestamp %q, past what microseconds since 1970 hold", s)
	}
	hms, zone := rest, ""
	if i := strings.IndexAny(rest, "+-"); i >= 0 {
		hms, zone = rest[:i], rest[i:]
	}
	t, err := time.Parse("15:04:05", hms)
	if err != nil {
		return bad()
	}
	us := days*day + int64(t.Hour()*3600+t.Minute()*60+t.Second())*1_000_000 + int64(t.Nanosecond()/1000)
	if zone != "" {
		var off int64
		for i, part := range strings.Split(zone[1:], ":") {
			n, err := strconv.Atoi(part)
			if err != nil || i > 2 {
				return bad()
			}
			off += int64(n) * []int64{3600, 60, 1}[i] * 1_000_000
		}
		if zone[0] == '-' {
			off = -off
		}
		us -= off
	}
	return us, nil
}

// appendItem appends an array item's text form.
func appendItem(b array.Builder, s string) error {
	switch b := b.(type) {
	case *array.BooleanBuilder:
		switch s {
		case "t":
			b.Append(true)
		case "f":
			b.Append(false)
		default:
			return errors.New("not a boolean")
		}
	case *array.Int16Builder:
		n, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return err
		}
		b.Append(int16(n))
	case *array.Int32Builder:
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		b.Append(int32(n))
	case *array.Int64Builder:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		b.Append(n)
	case *array.Float32Builder:
		f, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return err
		}
		b.Append(float32(f))
	case *array.Float64Builder:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		b.Append(f)
	case *array.StringBuilder:
		b.Append(s)
	default:
		return fmt.Errorf("no item type %T", b)
	}
	return nil
}

func dataInvalid(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, fmt.Errorf(format, args...))
}

// arrayItems are a one-dimensional array's items in Postgres's text form,
// nil for NULL. Bounds it states, such as [0:1]={a,b}, aren't kept. An array
// of more dimensions has no list to go in: it's DATA_INVALID.
func arrayItems(v []byte) ([][]byte, error) {
	s := v
	if len(s) > 0 && s[0] == '[' {
		eq := bytes.IndexByte(s, '=')
		if eq < 0 {
			return nil, dataInvalid("an array %q", v)
		}
		if bytes.Count(s[:eq], []byte("[")) > 1 {
			return nil, dataInvalid("an array of more than one dimension, %.40q, has no list to go in; read the column cast to text, through a view", v)
		}
		s = s[eq+1:]
	}
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil, dataInvalid("an array %q", v)
	}
	s = s[1 : len(s)-1]
	if len(s) > 0 && s[0] == '{' {
		return nil, dataInvalid("an array of more than one dimension, %.40q, has no list to go in; read the column cast to text, through a view", v)
	}
	var out [][]byte
	if len(s) == 0 {
		return out, nil
	}
	for i := 0; ; {
		var item []byte
		quoted, escaped := i < len(s) && s[i] == '"', false
		if quoted {
			i++
		}
		for ; i < len(s); i++ {
			c := s[i]
			if c == '\\' && i+1 < len(s) {
				i++
				item, escaped = append(item, s[i]), true
				continue
			}
			if quoted && c == '"' || !quoted && c == ',' {
				break
			}
			item = append(item, c)
		}
		if quoted {
			if i >= len(s) {
				return nil, dataInvalid("an array %q", v)
			}
			i++
		}
		switch {
		case !quoted && !escaped && bytes.EqualFold(item, []byte("NULL")):
			out = append(out, nil)
		case item == nil:
			out = append(out, []byte{})
		default:
			out = append(out, item)
		}
		if i >= len(s) {
			return out, nil
		}
		if s[i] != ',' {
			return nil, dataInvalid("an array %q", v)
		}
		i++
	}
}

// parseInterval reads an interval as Postgres writes it with IntervalStyle
// iso_8601, such as P1Y2M3DT4H5M6.5S or P-1DT-2H. The engine's interval
// holds months, days and nanoseconds, so one whose hours, minutes and
// seconds are past about 292 years, or an infinite one, has nowhere to go:
// it's DATA_INVALID.
func parseInterval(s string) (arrow.MonthDayNanoInterval, error) {
	past := func() (arrow.MonthDayNanoInterval, error) {
		return arrow.MonthDayNanoInterval{}, dataInvalid("an interval of %s is past what the engine's interval holds; read the column cast to text, through a view", s)
	}
	rest, ok := strings.CutPrefix(s, "P")
	if !ok {
		if s == "infinity" || s == "-infinity" {
			return past()
		}
		return arrow.MonthDayNanoInterval{}, dataInvalid("an interval %q", s)
	}
	var months, days, micros int64
	timePart := false
	for rest != "" {
		if rest[0] == 'T' {
			timePart, rest = true, rest[1:]
			continue
		}
		end := strings.IndexAny(rest, "YMWDHS")
		if end <= 0 {
			return arrow.MonthDayNanoInterval{}, dataInvalid("an interval %q", s)
		}
		num, unit := rest[:end], rest[end]
		rest = rest[end+1:]
		if !timePart || unit != 'S' {
			n, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				return arrow.MonthDayNanoInterval{}, dataInvalid("an interval %q", s)
			}
			var per int64
			switch {
			case !timePart && unit == 'Y':
				months += n * 12
			case !timePart && unit == 'M':
				months += n
			case !timePart && unit == 'W':
				days += n * 7
			case !timePart && unit == 'D':
				days += n
			case timePart && unit == 'H':
				per = 3_600_000_000
			case timePart && unit == 'M':
				per = 60_000_000
			default:
				return arrow.MonthDayNanoInterval{}, dataInvalid("an interval %q", s)
			}
			if per != 0 {
				if n > math.MaxInt64/per || n < math.MinInt64/per {
					return past()
				}
				micros += n * per
			}
			continue
		}
		neg := strings.HasPrefix(num, "-")
		whole, frac, _ := strings.Cut(strings.TrimPrefix(num, "-"), ".")
		if len(frac) > 6 {
			return arrow.MonthDayNanoInterval{}, dataInvalid("an interval %q", s)
		}
		w, err := strconv.ParseInt(whole, 10, 64)
		f, ferr := strconv.ParseInt(frac+strings.Repeat("0", 6-len(frac)), 10, 64)
		if err != nil || ferr != nil {
			return arrow.MonthDayNanoInterval{}, dataInvalid("an interval %q", s)
		}
		if w > (math.MaxInt64-f)/1_000_000 {
			return past()
		}
		us := w*1_000_000 + f
		if neg {
			us = -us
		}
		micros += us
	}
	if months > math.MaxInt32 || months < math.MinInt32 || days > math.MaxInt32 || days < math.MinInt32 {
		return past()
	}
	if micros > math.MaxInt64/1000 || micros < math.MinInt64/1000 {
		return past()
	}
	return arrow.MonthDayNanoInterval{Months: int32(months), Days: int32(days), Nanoseconds: micros * 1000}, nil
}
