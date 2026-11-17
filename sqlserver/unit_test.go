package sqlserver

import (
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestDecimals(t *testing.T) {
	for _, c := range []struct {
		text  string
		scale int32
		want  string
	}{
		{"0", 0, "0"},
		{"-1.5", 2, "-1.50"},
		{"99999999999999999999999999999.999999999", 9, "99999999999999999999999999999.999999999"},
		{"-0.001", 3, "-0.001"},
		{".5", 1, "0.5"},
	} {
		n, err := parseDecimal(c.text, c.scale)
		if err != nil {
			t.Errorf("%s: %v", c.text, err)
			continue
		}
		if got := formatDecimal(n, c.scale); got != c.want {
			t.Errorf("%s at scale %d = %s, want %s", c.text, c.scale, got, c.want)
		}
	}
	if _, err := parseDecimal("1.234", 2); err == nil {
		t.Error("parsed a value past its scale")
	}
	if got := formatDecimal(decimal128.FromI64(-5), 3); got != "-0.005" {
		t.Errorf("formatDecimal = %s", got)
	}
}

func TestGUIDByteOrder(t *testing.T) {
	// SQL Server sends 6F9619FF-8B86-D011-B42D-00C04FC964FF with its first
	// three groups little-endian.
	wire := []byte{0xff, 0x19, 0x96, 0x6f, 0x86, 0x8b, 0x11, 0xd0, 0xb4, 0x2d, 0x00, 0xc0, 0x4f, 0xc9, 0x64, 0xff}
	want := []byte{0x6f, 0x96, 0x19, 0xff, 0x8b, 0x86, 0xd0, 0x11, 0xb4, 0x2d, 0x00, 0xc0, 0x4f, 0xc9, 0x64, 0xff}
	if got := guidToUUID(wire); !slices.Equal(got, want) {
		t.Errorf("guidToUUID = %x", got)
	}
}

func TestWidens(t *testing.T) {
	for _, c := range []struct {
		from, to string
		want     bool
	}{
		{"int", "bigint", true},
		{"bigint", "int", false},
		{"bigint", "float", true},
		{"real", "float", true},
		{"int", "decimal(10,0)", true},
		{"int", "decimal(9,0)", false},
		{"decimal(10,2)", "decimal(12,3)", true},
		{"decimal(10,2)", "decimal(10,3)", false},
		{"numeric(5,0)", "decimal(5,0)", true},
		{"datetime2(6)", "datetime2(7)", true},
		{"bit", "nvarchar(max)", true},
		{"nvarchar(max)", "int", false},
		{"bit", "int", false},
		{"nvarchar(50)", "nvarchar(450)", true},
		{"varchar(20)", "nvarchar(450)", true},
		{"nvarchar(500)", "nvarchar(450)", false},
		{"nvarchar(450)", "nvarchar(500)", true},
		{"nvarchar(max)", "nvarchar(450)", false},
		{"varbinary(16)", "varbinary(900)", true},
		{"varbinary(900)", "varbinary(max)", true},
		{"varbinary(max)", "varbinary(900)", false},
		{"nvarchar(10)", "varbinary(900)", false},
		{"int", "nvarchar(450)", false},
	} {
		if got := widens(c.from, c.to); got != c.want {
			t.Errorf("widens(%s, %s) = %v", c.from, c.to, got)
		}
	}
}

func TestColumnTypes(t *testing.T) {
	for _, c := range []struct {
		typ  arrow.DataType
		want string
	}{
		{arrow.PrimitiveTypes.Int8, "smallint"},
		{arrow.PrimitiveTypes.Uint8, "tinyint"},
		{arrow.PrimitiveTypes.Uint64, "decimal(20,0)"},
		{arrow.PrimitiveTypes.Float32, "real"},
		{&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, "datetimeoffset(7)"},
		{&arrow.TimestampType{Unit: arrow.Microsecond}, "datetime2(6)"},
		{arrow.FixedWidthTypes.Time64us, "time(6)"},
		{&arrow.Decimal128Type{Precision: 38, Scale: 9}, "decimal(38,9)"},
		{arrow.ListOf(arrow.PrimitiveTypes.Int64), "nvarchar(max)"},
	} {
		col, err := columnOf(arrow.Field{Name: "c", Type: c.typ})
		if err != nil || col.typ != c.want {
			t.Errorf("%s = %s, %v; want %s", c.typ, col.typ, err, c.want)
		}
	}
	if _, err := columnOf(arrow.Field{Name: "c", Type: &arrow.Decimal256Type{Precision: 60, Scale: 2}}); err == nil {
		t.Error("decimal256 has a column")
	}
}

func TestSeconds(t *testing.T) {
	for ns, want := range map[int64]string{0: "0", 3_000_000_004: "3.000000004", -1_500_000_000: "-1.5", -9223372036854775808: "-9223372036.854775808"} {
		if got := seconds(ns); got != want {
			t.Errorf("seconds(%d) = %s, want %s", ns, got, want)
		}
	}
}

func TestLSN(t *testing.T) {
	l := []byte{0, 0, 0, 0x2e, 0, 0, 0x25, 0xe8, 0, 3}
	if got := lsnText(l); got != "0x0000002E000025E80003" {
		t.Errorf("lsnText = %s", got)
	}
	if got := lsnBytes(lsnInt(lsnText(l))); !slices.Equal(got, l) {
		t.Errorf("round trip = %x", got)
	}
	// The largest LSN's seq fits decimal(38).
	m := &capture{}
	n, err := m.seq("0xFFFFFFFFFFFFFFFFFFFF")
	if err != nil || !n.FitsInPrecision(38) {
		t.Errorf("seq of the largest LSN: %v, %v", n, err)
	}
}

func TestQuoting(t *testing.T) {
	if got := qualified("a]b", "c"); got != "[a]]b].[c]" {
		t.Errorf("qualified = %s", got)
	}
	if got := literal("it's"); got != "N'it''s'" {
		t.Errorf("literal = %s", got)
	}
	if got := alterColumn("[s].[t]", "k", "bigint", true, []string{"_openetl_key_x"}); got != "DROP INDEX [_openetl_key_x] ON [s].[t]; ALTER TABLE [s].[t] ALTER COLUMN [k] bigint NOT NULL;" {
		t.Errorf("alterColumn = %s", got)
	}
}

// The check lists are valid, also for names that collide once made names.
func TestCheckListsAreValid(t *testing.T) {
	const dsn = "sqlserver://u:p@db.example.com:1433?database=d"
	s, err := NewSource([]byte(`{"dsn": "` + dsn + `", "tables": ["My Orders", "my_orders"], "cdcMethod": "change_data_capture"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Validate(s.Checks()); err != nil {
		t.Error(err)
	}
	req := source.PreflightRequest{Interval: time.Hour, Streams: []source.ConfiguredStream{
		{Name: "a", SyncMode: source.CDC}, {Name: "A", SyncMode: source.Incremental, Cursor: "x"}, {Name: "b", SyncMode: source.FullRefresh},
	}}
	if err := check.Validate(s.Preflight(req)); err != nil {
		t.Error(err)
	}
	d, err := NewDestination([]byte(`{"dsn": "` + dsn + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Validate(d.Checks()); err != nil {
		t.Error(err)
	}
	sch := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	dreq := destination.PreflightRequest{Streams: []destination.Stream{{Name: "x"}, {Name: "X"}}, Schemas: map[string]*arrow.Schema{"x": sch, "X": sch}}
	if err := check.Validate(d.Preflight(dreq)); err != nil {
		t.Error(err)
	}
	// A named instance without a port is reached through SQL Server
	// Browser, so there's no address to dial.
	sv := serverOf("sqlserver://u:p@host/inst?database=d")
	if sv.address != "" || sv.principal != "u" {
		t.Errorf("named instance: %+v", sv)
	}
}
