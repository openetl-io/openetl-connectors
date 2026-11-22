package sqlserver

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	mssql "github.com/microsoft/go-mssqldb"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
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

// A key's or cursor's text holds its whole value: a varchar of up to 8000
// characters doesn't fit nvarchar(4000).
func TestTextOf(t *testing.T) {
	for _, c := range []struct {
		col  sourceColumn
		want string
	}{
		{sourceColumn{typ: "varchar", maxLength: 8000}, "CONVERT(nvarchar(max), x)"},
		{sourceColumn{typ: "char", maxLength: 4001}, "CONVERT(nvarchar(max), x)"},
		{sourceColumn{typ: "varchar", maxLength: 4000}, "CONVERT(nvarchar(4000), x)"},
		{sourceColumn{typ: "nvarchar", maxLength: 8000}, "CONVERT(nvarchar(4000), x)"},
		{sourceColumn{typ: "datetime2", scale: 7}, "CONVERT(nvarchar(4000), x, 126)"},
	} {
		if got := c.col.textOf("x"); got != c.want {
			t.Errorf("textOf of %s(%d) = %s, want %s", c.col.typ, c.col.maxLength, got, c.want)
		}
	}
}

// Rows of nulls and empty values count toward a batch's size, so a read of
// them flushes; a value's bytes count as they did.
func TestBatchBytes(t *testing.T) {
	b, err := newBatcher(memory.DefaultAllocator, []sourceColumn{{name: "s", typ: "nvarchar"}, {name: "n", typ: "int"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.release()
	for _, row := range [][]any{{nil, nil}, {"", nil}, {[]byte{}, int64(1)}} {
		if err := b.add(row); err != nil {
			t.Fatal(err)
		}
	}
	if b.bytes != 6*slotBytes {
		t.Errorf("nulls and empty values count %d bytes, want %d", b.bytes, 6*slotBytes)
	}
	b.bytes = 0
	if err := b.add([]any{strings.Repeat("x", 100), int64(1)}); err != nil || b.bytes != 100+slotBytes {
		t.Errorf("a row of 100 bytes of text and an int counts %d, %v", b.bytes, err)
	}
}

// A cdc stream given as its one split, the whole stream, is read from that
// split's state and to its until, as one given State and Until is; one
// given another read's splits starts again.
func TestWholeStream(t *testing.T) {
	st, until := []byte(`{"method":"change_tracking","position":"7"}`), []byte(`{"method":"change_tracking","position":"9"}`)
	cs := wholeStream(source.ConfiguredStream{Name: "t", Splits: []source.Split{{ID: source.WholeStream, State: st, Until: until}}})
	if string(cs.State) != string(st) || string(cs.Until) != string(until) || cs.Splits != nil {
		t.Errorf("whole split: %+v", cs)
	}
	if cs = wholeStream(source.ConfiguredStream{Name: "t", State: st}); string(cs.State) != string(st) {
		t.Errorf("no splits: %+v", cs)
	}
	if cs = wholeStream(source.ConfiguredStream{Name: "t", Splits: []source.Split{{ID: "0", State: st}, {ID: "1"}}}); cs.State != nil || cs.Splits != nil {
		t.Errorf("key ranges: %+v", cs)
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

// A poll reads the old capture instance up to where the new one starts,
// and the new one from there; a new one the capture job hasn't started yet,
// whose start_lsn is NULL, has no changes of its own, so the old one is read
// on.
func TestInstanceRanges(t *testing.T) {
	n := func(i int64) *big.Int { return big.NewInt(i) }
	old := captureInstance{name: "old", start: n(1)}
	for _, c := range []struct {
		name string
		ins  []captureInstance
		want string
	}{
		{"one", []captureInstance{old}, "old 10-20"},
		{"new starts within", []captureInstance{old, {name: "new", start: n(15)}}, "old 10-14 new 15-20"},
		{"new started before", []captureInstance{old, {name: "new", start: n(8)}}, "new 10-20"},
		{"new starts after", []captureInstance{old, {name: "new", start: n(25)}}, "old 10-20"},
		{"new not started", []captureInstance{old, {name: "new"}}, "old 10-20"},
	} {
		var got []string
		for _, r := range instanceRanges(c.ins, n(10), n(20)) {
			got = append(got, fmt.Sprintf("%s %s-%s", c.ins[r.i].name, r.lo, r.hi))
		}
		if strings.Join(got, " ") != c.want {
			t.Errorf("%s: %v, want %s", c.name, got, c.want)
		}
	}
}

// Errors SQL Server and Azure SQL raise for a moment, which a retry gets
// past, are TRANSIENT.
func TestClassifyTransient(t *testing.T) {
	for _, n := range []int32{1204, 1205, 1222, 601, 4221, 40143, 40197, 40501, 40540, 40613, 10928, 10929, 49918} {
		var coded interface{ ErrorCode() protocolv1.ErrorCode }
		if err := classify(mssql.Error{Number: n}); !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
			t.Errorf("error %d = %v", n, err)
		}
	}
}

// The certificate is verified only where the dsn says to: go-mssqldb trusts
// any without encrypt, or with trustServerCertificate=true.
func TestServerVerifiesCertificate(t *testing.T) {
	for suffix, want := range map[string]bool{
		"": false, "&trustServerCertificate=false": true, "&encrypt=true": true, "&encrypt=true&trustServerCertificate=true": false,
		"&encrypt=strict&trustServerCertificate=true": true, "&encrypt=disable": false,
	} {
		if got := serverOf("sqlserver://u:p@host:1433?database=d" + suffix).verified; got != want {
			t.Errorf("%q: verified = %v, want %v", suffix, got, want)
		}
	}
}

// Change tables have __$command_id from SQL Server 2016 RTM CU5 and SP1 CU2:
// SP1 and its first CU, and their GDRs, came before it.
func TestCommandIDs(t *testing.T) {
	for v, want := range map[string]bool{
		"13.0.1601.5": false, "13.0.1745.2": false, "13.0.2197.0": true, "13.0.2216.0": true, "13.0.4001.0": false,
		"13.0.4259.0": false, "13.0.4411.0": false, "13.0.4422.0": true, "13.0.5026.0": true, "14.0.1000.169": true,
		"16.0.4135.4": true, "Azure SQL": true,
	} {
		if got := commandIDs(v); got != want {
			t.Errorf("%s: %v, want %v", v, got, want)
		}
	}
}

// Of two patch columns that fill one held column, the first's UPDATE leaves
// it held for the second's to match, and the second's takes it out.
func TestPatchSharedHeldColumn(t *testing.T) {
	w := &writer{schema: "s"}
	s := stagedStream{Table: "t", Mode: modeMerge, Key: []string{"id"}, Fills: map[string]string{"price__v2": "price"}}
	stmts := w.PatchSQL(s, prepared{patch: []string{"id", "price__v2", "price", idColumn, heldColumn}, patches: lanesRows{rel: "[s].[p]"}, key: []column{{name: "id", typ: "bigint"}}})
	if len(stmts) != 2 {
		t.Fatalf("%d statements: %q", len(stmts), stmts)
	}
	unhold := quote(heldColumn) + " = NULLIF("
	for i, want := range []bool{false, true} {
		if got := strings.Contains(stmts[i], unhold); got != want {
			t.Errorf("statement %d takes the held column out: %v, want %v\n%s", i, got, want, stmts[i])
		}
	}
}
