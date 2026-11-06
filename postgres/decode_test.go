package postgres

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestNumericTypmod(t *testing.T) {
	enc := func(p, s int32) int32 { return (p<<16 | s&0x7ff) + 4 }
	for _, c := range []struct {
		typmod int32
		p, s   int32
		ok     bool
	}{
		{-1, 0, 0, false},
		{enc(10, 2), 10, 2, true},
		{enc(38, 38), 38, 38, true},
		{enc(39, 2), 39, 2, false},
		{enc(5, -2), 5, -2, false},
	} {
		p, s, ok := numericTypmod(c.typmod)
		if p != c.p || s != c.s || ok != c.ok {
			t.Errorf("typmod %d = (%d, %d, %v), want (%d, %d, %v)", c.typmod, p, s, ok, c.p, c.s, c.ok)
		}
	}
}

func TestNumericType(t *testing.T) {
	enc := func(p, s int32) int32 { return (p<<16 | s&0x7ff) + 4 }
	for typmod, want := range map[int32]string{
		-1:          "decimal(38, 9)",
		enc(12, 2):  "decimal(12, 2)",
		enc(39, 2):  "decimal(38, 9)",
		enc(5, -2):  "decimal(38, 9)",
		enc(38, 38): "decimal(38, 38)",
	} {
		if got := numericType(typmod).String(); got != want {
			t.Errorf("typmod %d = %s, want %s", typmod, got, want)
		}
	}
}

func TestWidens(t *testing.T) {
	for _, c := range []struct {
		from, to string
		want     bool
	}{
		{"bigint", "bigint", true},
		{"integer", "bigint", true},
		{"bigint", "double precision", true},
		{"boolean", "text", true},
		{"jsonb", "text", true},
		{"integer", "numeric(12,2)", true},
		{"integer", "numeric(11,2)", false},
		{"numeric(10,2)", "numeric(12,4)", true},
		{"numeric(10,2)", "numeric(12,1)", false},
		{"bigint", "integer", false},
		{"text", "bigint", false},
		{"boolean", "bigint", false},
		{"double precision", "bigint", false},
	} {
		if got := widens(c.from, c.to); got != c.want {
			t.Errorf("widens(%s, %s) = %v", c.from, c.to, got)
		}
	}
}

// Timestamps past Arrow's microseconds since 1970 become text, and so does
// the last of those, which reads as infinity.
func TestTimestampsPastArrow(t *testing.T) {
	const last = math.MaxInt64 - pgEpochMicros
	for _, c := range []struct {
		us   int64
		oid  uint32
		want string // empty if it fits
	}{
		{0, pgtype.TimestamptzOID, ""},
		{last - 1, pgtype.TimestamptzOID, ""},
		{last, pgtype.TimestamptzOID, "294247-01-10T04:00:54.775807Z"},
		{last + 1, pgtype.TimestampOID, "294247-01-10T04:00:54.775808"},
		// Postgres's last timestamp.
		{9_223_371_331_200_000_000 - 1, pgtype.TimestampOID, "294276-12-31T23:59:59.999999"},
		{9_223_371_331_200_000_000 - 86_400_000_000, pgtype.TimestamptzOID, "294276-12-31T00:00:00Z"},
		{math.MaxInt64, pgtype.TimestamptzOID, ""},
		{math.MinInt64, pgtype.TimestamptzOID, ""},
	} {
		cb, err := newColBuilder(memory.DefaultAllocator, sourceColumn{oid: c.oid})
		if err != nil {
			t.Fatal(err)
		}
		got, ok := cb.past(binary.BigEndian.AppendUint64(nil, uint64(c.us)))
		if got != c.want || ok != (c.want != "") {
			t.Errorf("%d: %q, %v; want %q", c.us, got, ok, c.want)
		}
		cb.b.Release()
	}
}
