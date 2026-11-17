package clickhouse

import (
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestWidens(t *testing.T) {
	for _, c := range []struct {
		from, to string
		want     bool
	}{
		{"Int32", "Int64", true},
		{"Int64", "Int32", false},
		{"UInt32", "Int64", true},
		{"UInt64", "Int64", false},
		{"Int64", "UInt64", false},
		{"Int16", "Float32", true},
		{"Int32", "Float32", false},
		{"Int32", "Float64", true},
		{"Int64", "Float64", false},
		{"Float32", "Float64", true},
		{"Int64", "String", true},
		{"Array(Int64)", "String", false},
		{"Nullable(Int32)", "Nullable(Int64)", true},
		{"Int32", "Nullable(Int32)", true},
		{"Nullable(Int32)", "Int32", false},
		{"Decimal(10, 2)", "Decimal(12, 2)", true},
		{"Decimal(10, 2)", "Decimal(10, 3)", false},
		{"Decimal(10, 2)", "Decimal(11, 3)", true},
		{"DateTime64(3, 'UTC')", "DateTime64(6, 'UTC')", true},
		{"DateTime64(6, 'UTC')", "DateTime64(3, 'UTC')", false},
		{"DateTime64(3, 'UTC')", "DateTime64(6, 'Asia/Tokyo')", false},
		{"Date32", "DateTime64(6, 'UTC')", true},
		{"Time64(3)", "Time64(6)", true},
		{"Array(Nullable(Int32))", "Array(Nullable(Int64))", true},
		{"Tuple(\n    months Int32,\n    days Int32,\n    nanoseconds Int64)", intervalType, true},
		{"String", "Int64", false},
	} {
		if got := widens(c.from, c.to); got != c.want {
			t.Errorf("widens(%q, %q) = %v", c.from, c.to, got)
		}
	}
}

func TestParseType(t *testing.T) {
	// Each line is a type and how it reads, empty if it doesn't.
	types := `
Int64                                  -> int64
Int8                                   -> int16
UInt8                                  -> int16
UInt16                                 -> int32
UInt32                                 -> int64
UInt64                                 -> decimal(20, 0)
Nullable(Int128)                       -> utf8
UInt256                                -> utf8
LowCardinality(Nullable(String))       -> utf8
Decimal(76, 10)                        -> utf8
Decimal(18, 4)                         -> decimal(18, 4)
Decimal32(2)                           -> decimal(9, 2)
DateTime('Asia/Tokyo')                 -> timestamp[us, tz=UTC]
DateTime64(9)                          -> timestamp[us, tz=UTC]
DateTime64(4, 'Europe/Paris')          -> timestamp[us, tz=UTC]
Array(DateTime64(9))                   ->
Time                                   -> time64[us]
Time64(3)                              -> time64[us]
Time64(9)                              -> time64[us]
Array(Time)                            ->
Array(Array(Nullable(Int8)))           -> list<item: list<item: int16, nullable>, nullable>
Array(UInt64)                          -> list<item: decimal(20, 0), nullable>
Map(LowCardinality(String), UInt16)    ->
Array(Tuple(Int8, String))             ->
Enum8('a,b' = 1, 'c' = 2)              -> utf8
SimpleAggregateFunction(max, UInt64)   -> decimal(20, 0)
FixedString(16)                        -> binary
Array(JSON)                            ->
Variant(Int64, String)                 ->
AggregateFunction(uniq, String)        ->
Nested(a Int32)                        ->
` + "Tuple(a Int32, `b c` Nullable(String)) ->\n"
	for line := range strings.Lines(types) {
		typ, want, ok := strings.Cut(line, " ->")
		if !ok {
			continue
		}
		typ, want = strings.TrimSpace(typ), strings.TrimSpace(want)
		dt, err := arrowType(parseType(typ))
		got := ""
		if err == nil {
			got = dt.String()
		}
		if got != want {
			t.Errorf("%s reads as %q (%v), not %q", typ, got, err, want)
		}
	}
}

func TestSortingColumns(t *testing.T) {
	cols := []sourceColumn{
		{name: "a", dt: arrow.PrimitiveTypes.Int64},
		{name: "b c", dt: arrow.BinaryTypes.String},
		{name: "n", dt: arrow.PrimitiveTypes.Int64, nullable: true},
		{name: "l", dt: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	}
	for key, want := range map[string]struct {
		key []int
		all bool
	}{
		"a, `b c`":        {[]int{0, 1}, true},
		"a, toDate(b)":    {[]int{0}, false},
		"n, a":            {nil, false},
		"a, l":            {[]int{0}, false},
		"":                {nil, false},
		"toStartOfDay(a)": {nil, false},
	} {
		got, all := sortingColumns(cols, key)
		if !slices.Equal(got, want.key) || all != want.all {
			t.Errorf("sorting key %q: %v, %v", key, got, all)
		}
	}
}

func TestCutRanges(t *testing.T) {
	bounds := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}
	r := cutRanges(bounds, 900, 300)
	if len(r) != 3 || r[0].Lo != nil || *r[0].Hi != "3" || *r[1].Lo != "3" || *r[1].Hi != "6" || *r[2].Lo != "6" || r[2].Hi != nil {
		t.Errorf("cut %v", r)
	}
	if r := cutRanges(bounds, 200, 300); r != nil {
		t.Errorf("a table smaller than a chunk is cut in %d", len(r))
	}
}

func TestQuoting(t *testing.T) {
	if got := ident("a`b\\c"); got != "`a\\`b\\\\c`" {
		t.Errorf("ident: %s", got)
	}
	if got := literal("it's \\"); got != `'it\'s \\'` {
		t.Errorf("literal: %s", got)
	}
}

func TestMapType(t *testing.T) {
	for _, c := range []struct {
		dt   arrow.DataType
		want string
	}{
		{arrow.PrimitiveTypes.Uint64, "UInt64"},
		{&arrow.Decimal256Type{Precision: 76, Scale: 20}, "Decimal(76, 20)"},
		{&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Asia/Tokyo"}, "DateTime64(9, 'UTC')"},
		{arrow.FixedWidthTypes.Time32ms, "Time64(3)"},
		{arrow.ListOf(arrow.BinaryTypes.String), "Array(Nullable(String))"},
		{arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), "Map(String, Nullable(Int64))"},
		{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}, "LowCardinality(Nullable(String))"},
		{arrow.FixedWidthTypes.MonthDayNanoInterval, intervalType},
	} {
		m, err := mapType(c.dt, "")
		if err != nil || m.typ != c.want {
			t.Errorf("%s maps to %q (%v), not %q", c.dt, m.typ, err, c.want)
		}
	}
	if _, err := mapType(&arrow.Decimal256Type{Precision: 76, Scale: 0}, ""); err != nil {
		t.Error(err)
	}
}
