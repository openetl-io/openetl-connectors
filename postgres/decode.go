package postgres

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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

func newColBuilder(mem memory.Allocator, c sourceColumn) (*colBuilder, error) {
	be := binary.BigEndian
	cb := &colBuilder{format: pgx.BinaryFormatCode}
	switch c.oid {
	case pgtype.BoolOID:
		b := array.NewBooleanBuilder(mem)
		cb.b, cb.add = b, fixed(1, func(v []byte) { b.Append(v[0] != 0) })
	case pgtype.Int2OID:
		b := array.NewInt16Builder(mem)
		cb.b, cb.add = b, fixed(2, func(v []byte) { b.Append(int16(be.Uint16(v))) }) //nolint:gosec // two's complement
	case pgtype.Int4OID:
		b := array.NewInt32Builder(mem)
		cb.b, cb.add = b, fixed(4, func(v []byte) { b.Append(int32(be.Uint32(v))) }) //nolint:gosec // two's complement
	case pgtype.Int8OID:
		b := array.NewInt64Builder(mem)
		cb.b, cb.add = b, fixed(8, func(v []byte) { b.Append(int64(be.Uint64(v))) }) //nolint:gosec // two's complement
	case pgtype.OIDOID:
		b := array.NewInt64Builder(mem)
		cb.b, cb.add = b, fixed(4, func(v []byte) { b.Append(int64(be.Uint32(v))) })
	case pgtype.Float4OID:
		b := array.NewFloat32Builder(mem)
		cb.b, cb.add = b, fixed(4, func(v []byte) { b.Append(math.Float32frombits(be.Uint32(v))) })
	case pgtype.Float8OID:
		b := array.NewFloat64Builder(mem)
		cb.b, cb.add = b, fixed(8, func(v []byte) { b.Append(math.Float64frombits(be.Uint64(v))) })
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID:
		b := array.NewStringBuilder(mem)
		cb.b, cb.add = b, func(v []byte) error { b.BinaryBuilder.Append(v); return nil }
	case pgtype.ByteaOID:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		cb.b, cb.add = b, func(v []byte) error { b.Append(v); return nil }
	case pgtype.DateOID:
		b := array.NewDate32Builder(mem)
		cb.b, cb.add = b, func(v []byte) error {
			if len(v) != 4 {
				return fmt.Errorf("a %d-byte date", len(v))
			}
			d := int32(be.Uint32(v)) //nolint:gosec // two's complement
			if d != math.MaxInt32 && d != math.MinInt32 {
				d += pgEpochDays
			}
			b.Append(arrow.Date32(d))
			return nil
		}
	case pgtype.TimeOID:
		b := array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Microsecond})
		cb.b, cb.add = b, fixed(8, func(v []byte) { b.Append(arrow.Time64(be.Uint64(v))) }) //nolint:gosec // microseconds since midnight
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
			us := int64(be.Uint64(v)) //nolint:gosec // two's complement
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
			us := int64(be.Uint64(v)) //nolint:gosec // two's complement
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
	if cb.b == nil {
		b := array.NewStringBuilder(mem)
		cb.format, cb.b, cb.add = pgx.TextFormatCode, b, func(v []byte) error { b.BinaryBuilder.Append(v); return nil }
	}
	return cb, nil
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
