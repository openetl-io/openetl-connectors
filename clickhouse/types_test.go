package clickhouse

import (
	"net"
	"slices"
	"strings"
	"testing"

	chcol "github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/shopspring/decimal"

	"github.com/openetl-io/openetl-connectors/connectortest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
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
		{name: "f", dt: arrow.PrimitiveTypes.Float64},
	}
	for key, want := range map[string]struct {
		key []int
		all bool
	}{
		"a, `b c`":     {[]int{0, 1}, true},
		"a, toDate(b)": {[]int{0}, false},
		"n, a":         {nil, false},
		"a, l":         {[]int{0}, false},
		// A NaN compares false with every value, so a read can't resume
		// after one, or past one in order.
		"a, f":            {[]int{0}, false},
		"f, a":            {nil, false},
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

// A read whose request declared columns fails, retryably, before it sends
// a column that can come as text and isn't among them.
func TestUndeclaredText(t *testing.T) {
	tbl := table{name: "e", cols: []sourceColumn{
		{name: "id", dt: arrow.PrimitiveTypes.Int64},
		{name: "dt9", dt: utcMicros, text: true},
		{name: "t", dt: arrow.FixedWidthTypes.Time64us},
	}}
	for _, columns := range [][]string{nil, {"id", "dt9", "t"}, {"dt9", "t"}} {
		if err := undeclaredText(tbl, columns); err != nil {
			t.Errorf("columns %v: %v", columns, err)
		}
	}
	for _, columns := range [][]string{{"id", "t"}, {"id", "dt9"}} {
		err := undeclaredText(tbl, columns)
		if code := connectortest.CodeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
			t.Errorf("columns %v: %v, code %v", columns, err, code)
		}
	}
}

// An order column's text scans into a *string, or into a **string where
// it's Nullable, as a Nullable cursor's is.
func TestScannedString(t *testing.T) {
	s := "6162"
	p := &s
	var null *string
	for v, want := range map[any]string{&s: "6162", &p: "6162"} {
		if got, ok := scannedString(v); !ok || got != want {
			t.Errorf("%T: %q, %v", v, got, ok)
		}
	}
	if _, ok := scannedString(&null); ok {
		t.Error("a null scanned as a string")
	}
}

// The highest cursor a split's read goes up to is of the rows the read
// sees: those FINAL merges, in a table whose engine merges them.
func TestUntilQuery(t *testing.T) {
	s := &Source{cfg: SourceConfig{connConfig: connConfig{Database: "db"}}}
	at := sourceColumn{name: "at", typ: "Int64"}
	for _, c := range []struct {
		t    table
		want string
	}{
		{table{name: "s", final: true}, "SELECT if(count() > 0, hex(toString(max(`at`))), NULL) FROM `db`.`s` FINAL WHERE `at` IS NOT NULL"},
		{table{name: "m"}, "SELECT if(count() > 0, hex(toString(max(`at`))), NULL) FROM `db`.`m` WHERE `at` IS NOT NULL"},
	} {
		if got := untilQuery(s.from(c.t), at); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.t.name, got, c.want)
		}
	}
	// A NaN is never the highest: no row is at or below it, so a read up to
	// it, and every read after, would read nothing.
	f := sourceColumn{name: "f", typ: "Nullable(Float64)"}
	want := "SELECT if(count() > 0, hex(toString(max(`f`))), NULL) FROM `db`.`m` WHERE `f` IS NOT NULL AND NOT isNaN(`f`)"
	if got := untilQuery(s.from(table{name: "m"}), f); got != want {
		t.Errorf("float cursor:\n got %s\nwant %s", got, want)
	}
}

// A DateTime64 is read as its microseconds, in arrays too, as the driver's
// time.Time is wrong past 2262.
func TestMicros(t *testing.T) {
	for typ, want := range map[string]string{
		"Nullable(DateTime64(3, 'Asia/Tokyo'))": "toUnixTimestamp64Micro(`c`)",
		"Array(DateTime64(6, 'UTC'))":           "arrayMap(x0 -> toUnixTimestamp64Micro(x0), `c`)",
		"Array(Array(Nullable(DateTime64(3))))": "arrayMap(x0 -> arrayMap(x1 -> toUnixTimestamp64Micro(x1), x0), `c`)",
		"Array(Int64)":                          "",
		"DateTime('UTC')":                       "",
		"Map(String, DateTime64(3))":            "",
	} {
		if got := micros(parseType(typ), "`c`", 0); got != want {
			t.Errorf("%s: %q, want %q", typ, got, want)
		}
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
		// ClickHouse's scale is 0 to the precision.
		{&arrow.Decimal128Type{Precision: 5, Scale: -3}, "Decimal(8, 0)"},
		{&arrow.Decimal128Type{Precision: 3, Scale: 5}, "Decimal(5, 5)"},
	} {
		m, err := mapType(c.dt, "")
		if err != nil || m.typ != c.want {
			t.Errorf("%s maps to %q (%v), not %q", c.dt, m.typ, err, c.want)
		}
	}
	if _, err := mapType(&arrow.Decimal256Type{Precision: 76, Scale: 0}, ""); err != nil {
		t.Error(err)
	}
	if _, err := mapType(&arrow.Decimal256Type{Precision: 75, Scale: -2}, ""); err == nil {
		t.Error("decimal(75, -2), of 77 digits, maps")
	}
	// A negative scale's value keeps its exponent: 12 at scale -3 is 12000.
	dt := &arrow.Decimal128Type{Precision: 5, Scale: -3}
	b := array.NewDecimal128Builder(memory.DefaultAllocator, dt)
	defer b.Release()
	b.Append(decimal128.FromI64(12))
	arr := b.NewArray()
	defer arr.Release()
	m, err := mapType(dt, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := m.value(arr, 0).(decimal.Decimal); !got.Equal(decimal.NewFromInt(12000)) {
		t.Errorf("12 at scale -3 is %s", got)
	}
}

// A null in an array, a map or a tuple, and a null tuple, of a time of day,
// which the driver needs typed, stage as the driver's columns take them.
func TestContainerNulls(t *testing.T) {
	mem := memory.DefaultAllocator
	tm := arrow.FixedWidthTypes.Time64us
	lb := array.NewListBuilder(mem, tm)
	defer lb.Release()
	lb.Append(true)
	lb.ValueBuilder().AppendNull()
	lb.ValueBuilder().(*array.Time64Builder).Append(1)
	mb := array.NewMapBuilder(mem, arrow.BinaryTypes.String, tm, false)
	defer mb.Release()
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("k")
	mb.ItemBuilder().AppendNull()
	sb := array.NewStructBuilder(mem, arrow.StructOf(arrow.Field{Name: "t", Type: tm, Nullable: true}))
	defer sb.Release()
	sb.Append(true)
	sb.FieldBuilder(0).AppendNull()
	sb.AppendNull()
	for _, b := range []array.Builder{lb, mb, sb} {
		arr := b.NewArray()
		defer arr.Release()
		c, err := columnOf(arrow.Field{Name: "c", Type: arr.DataType(), Nullable: true}, false)
		if err != nil {
			t.Fatal(err)
		}
		col, err := chcol.Type(c.typ).Column("c", nil)
		if err != nil {
			t.Fatalf("%s: %v", c.typ, err)
		}
		for i := range arr.Len() {
			if err := col.AppendRow(c.value(arr, i)); err != nil {
				t.Errorf("%s, row %d: %v", c.typ, i, err)
			}
		}
	}
}

// A chunk's rows after the last one's order columns, compared column by
// column, as a key's index skips granules by that but never by a tuple's
// comparison.
func TestAfter(t *testing.T) {
	if got, want := after([]string{"`a`"}, []string{"1"}), "(`a` > 1)"; got != want {
		t.Errorf("one column: %s, not %s", got, want)
	}
	if got, want := after([]string{"`a`", "`b`", "`c`"}, []string{"1", "2", "3"}), "(`a` > 1 OR `a` = 1 AND (`b` > 2 OR `b` = 2 AND (`c` > 3)))"; got != want {
		t.Errorf("three columns: %s, not %s", got, want)
	}
}

// An IPv6 address keeps its text, an IPv4-mapped one too, which net.IP
// spells as IPv4; an IPv4 column's scans in 4 bytes.
func TestIPText(t *testing.T) {
	for ip, want := range map[string]string{"::ffff:1.2.3.4": "::ffff:1.2.3.4", "::1": "::1", "1.2.3.4": "1.2.3.4"} {
		v := net.ParseIP(ip)
		if !strings.Contains(ip, ":") {
			v = v.To4()
		}
		if got := text(v); got != want {
			t.Errorf("%s: %q", ip, got)
		}
	}
}
