package oracle

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

func mustDestination(t *testing.T, config string) *Destination {
	t.Helper()
	d, err := NewDestination([]byte(config))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestDestinationConfig(t *testing.T) {
	d := mustDestination(t, `{"dsn": "oracle://u:secret@db.example.com:1521/SVC"}`)
	if d.addr != "db.example.com:1521" || d.user != "u" || d.service != "SVC" {
		t.Errorf("addr %q, user %q, service %q", d.addr, d.user, d.service)
	}
	for config, want := range map[string]string{
		`{}`: "needs a dsn",
		`{"dsn": "oracle://u:p@h/S", "schema": "X"}`: "unknown field",
		`{"dsn": "postgres://u:p@h/S"}`:              "oracle",
	} {
		if _, err := NewDestination([]byte(config)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want an error with %q", config, err, want)
		}
	}
}

// The lists are valid, name every item the README does, and never carry
// the password.
func TestDestinationCheckLists(t *testing.T) {
	d := mustDestination(t, `{"dsn": "oracle://u:secret@db.example.com:1522/SVC"}`)
	checks := d.Checks()
	if err := check.Validate(checks); err != nil {
		t.Fatal(err)
	}
	want := []string{"net.dns", "net.connect", "db.connect", "server.version", "server.compatible", "privilege.create-table", "quota.tablespace"}
	if got := checkNamesOf(checks); !slices.Equal(got, want) {
		t.Errorf("checks %v, want %v", got, want)
	}
	md := arrow.NewMetadata([]string{metaTable}, []string{"Orders"})
	req := destination.PreflightRequest{Pipeline: "p", Namespace: "SALES", Streams: []destination.Stream{
		{Name: "orders", Mode: destination.Merge, PrimaryKey: []string{"id"}}, {Name: "ORDERS", Mode: destination.Append}, {Name: "events", Mode: destination.Overwrite},
	}, Schemas: map[string]*arrow.Schema{"orders": arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, &md)}}
	pre := d.Preflight(req)
	if err := check.Validate(pre); err != nil {
		t.Fatal(err)
	}
	want = []string{"db.connect", "privilege.create-table", "quota.tablespace", "schema.write",
		"stream.orders.names", "stream.orders.mode", "stream.orders.write", "stream.orders.types",
		"stream.orders.names-2", "stream.orders.mode-2", "stream.orders.write-2", "stream.orders.types-2",
		"stream.events.names", "stream.events.mode", "stream.events.write", "stream.events.types"}
	if got := checkNamesOf(pre); !slices.Equal(got, want) {
		t.Errorf("preflight %v, want %v", got, want)
	}
	for _, c := range slices.Concat(checks, pre) {
		for k, v := range c.Params {
			if strings.Contains(v, "secret") {
				t.Errorf("%s: param %s holds the password", c.Name, k)
			}
		}
		if c.Field != "" && c.Field != "dsn" {
			t.Errorf("%s: field %q isn't a config property", c.Name, c.Field)
		}
		if c.Name == "stream.orders.names" && c.Params["table"] != `"SALES"."Orders"` {
			t.Errorf("stream.orders.names's table is %s", c.Params["table"])
		}
	}
}

func TestDestinationTypeMapping(t *testing.T) {
	jsonExt := arrow.NewMetadata([]string{metaExtension}, []string{"arrow.json"})
	uuidExt := arrow.NewMetadata([]string{metaExtension}, []string{"arrow.uuid"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "dec", Type: &arrow.Decimal128Type{Precision: 12, Scale: 3}},
		{Name: "negscale", Type: &arrow.Decimal128Type{Precision: 10, Scale: -2}},
		{Name: "wide", Type: &arrow.Decimal256Type{Precision: 60, Scale: 0}},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "ls", Type: arrow.BinaryTypes.LargeString},
		{Name: "bin", Type: arrow.BinaryTypes.Binary},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32},
		{Name: "t", Type: arrow.FixedWidthTypes.Time64us},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Second}},
		{Name: "tsz", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Europe/Paris"}},
		{Name: "j", Type: arrow.BinaryTypes.String, Metadata: jsonExt},
		{Name: "u", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: uuidExt},
		{Name: "m", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
		{Name: "k1", Type: arrow.BinaryTypes.String},
		{Name: "k2", Type: arrow.BinaryTypes.Binary},
		{Name: colOp, Type: arrow.BinaryTypes.String},
	}, nil)
	want := map[bool][]string{
		true: {"NUMBER(3)", "NUMBER(20)", "BINARY_FLOAT", "NUMBER(12,3)", "VARCHAR2(15 BYTE)", "VARCHAR2(63 BYTE)", "CLOB", "CLOB", "BLOB", "BOOLEAN", "DATE",
			"INTERVAL DAY(0) TO SECOND(6)", "TIMESTAMP(6)", "TIMESTAMP(9) WITH TIME ZONE", "CLOB", "VARCHAR2(36 BYTE)", "CLOB", "VARCHAR2(2000 BYTE)", "RAW(2000)", "VARCHAR2(32 BYTE)"},
	}
	want[false] = slices.Clone(want[true])
	want[false][9] = "NUMBER(1)"
	for _, boolean := range []bool{true, false} {
		cols, err := destColumns(schema, []string{"k1", "k2"}, boolean)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, c := range cols {
			got = append(got, c.typ)
		}
		if !slices.Equal(got, want[boolean]) {
			t.Errorf("boolean %v:\n got  %q\n want %q", boolean, got, want[boolean])
		}
	}
	if _, err := destColumns(arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.FixedWidthTypes.Time32ms}}, nil), nil, true); err == nil {
		t.Error("a time32 column mapped")
	}
}

// fits and alterable say which types a column takes, and which Oracle
// changes in place; modify's fix for the others moves the values over.
func TestDestinationTypeChanges(t *testing.T) {
	for _, c := range []struct {
		from, to        string
		fits, alterable bool
		fixHas          string
	}{
		{"NUMBER(10)", "NUMBER(19)", true, true, `MODIFY ("c" NUMBER(19))`},
		{"NUMBER(10,2)", "NUMBER(20,4)", true, true, `MODIFY ("c" NUMBER(20,4))`},
		{"NUMBER(19)", "NUMBER(10)", false, false, ""},
		{"NUMBER(10,2)", "NUMBER(10,4)", false, false, ""},
		{"NUMBER(19)", "BINARY_DOUBLE", true, false, `= TO_BINARY_DOUBLE("c")`},
		{"NUMBER(19)", "CLOB", true, false, `= TO_CLOB("c")`},
		{"BINARY_FLOAT", "BINARY_DOUBLE", true, false, `= TO_BINARY_DOUBLE("c")`},
		{"VARCHAR2(10 BYTE)", "VARCHAR2(20 BYTE)", true, true, `MODIFY ("c" VARCHAR2(20 BYTE))`},
		{"VARCHAR2(20 BYTE)", "CLOB", true, false, `= TO_CLOB("c")`},
		{"RAW(16)", "BLOB", true, false, `= TO_BLOB("c")`},
		{"TIMESTAMP(6)", "TIMESTAMP(9)", true, true, `MODIFY ("c" TIMESTAMP(9))`},
		{"DATE", "TIMESTAMP(6)", true, true, `MODIFY ("c" TIMESTAMP(6))`},
		{"TIMESTAMP(6)", "TIMESTAMP(6) WITH TIME ZONE", false, false, ""},
		{"INTERVAL DAY(0) TO SECOND(6)", "INTERVAL DAY(0) TO SECOND(9)", true, true, `MODIFY ("c" INTERVAL DAY(0) TO SECOND(9))`},
		{"BOOLEAN", "NUMBER(1)", true, false, `CASE "c" WHEN TRUE THEN 1 WHEN FALSE THEN 0 END`},
		{"CLOB", "NUMBER(19)", false, false, ""},
		{"BLOB", "CLOB", false, false, ""},
	} {
		if got := fits(c.from, c.to); got != c.fits {
			t.Errorf("fits(%s, %s) = %v", c.from, c.to, got)
		}
		if got := alterable(c.from, c.to); got != c.alterable {
			t.Errorf("alterable(%s, %s) = %v", c.from, c.to, got)
		}
		if c.fixHas == "" {
			continue
		}
		fix := modify(`"S"."T"`, "c", c.from, c.to)
		if !strings.HasPrefix(fix, `ALTER TABLE "S"."T" `) || !strings.Contains(fix, c.fixHas) || c.alterable == strings.Contains(fix, "_openetl_new") {
			t.Errorf("%s to %s: fix %q", c.from, c.to, fix)
		}
	}
}

// plan adds new columns, takes what fits, changes in place only what it's
// allowed to, refuses the rest, and stops adding at 999 columns, as staging
// has one more.
func TestDestinationPlan(t *testing.T) {
	have := &tableInfo{cols: []tableColumn{
		{dcol: dcol{name: "a", typ: "NUMBER(10)"}}, {dcol: dcol{name: "b", typ: "NUMBER(19)"}}, {dcol: dcol{name: "c", typ: "CLOB"}},
	}}
	cols := []dcol{{name: "a", typ: "NUMBER(19)"}, {name: "b", typ: "NUMBER(5)"}, {name: "c", typ: "BLOB"}, {name: "d", typ: "DATE"}}
	adds, widen, refused := plan(`"S"."T"`, have, cols, false)
	if len(adds) != 1 || adds[0].name != "d" || len(widen) != 0 || len(refused) != 2 {
		t.Fatalf("adds %v, widen %v, refused %+v", adds, widen, refused)
	}
	if r := refused[0]; r.Column != "a" || r.FromType != "NUMBER(10)" || r.ToType != "NUMBER(19)" || r.Fix != `ALTER TABLE "S"."T" MODIFY ("a" NUMBER(19))` || !strings.Contains(r.Reason, "typeChanges") {
		t.Errorf("refused %+v", r)
	}
	if r := refused[1]; r.Column != "c" || r.Fix != "" || r.FromType != "CLOB" || r.ToType != "BLOB" {
		t.Errorf("refused %+v", r)
	}
	_, widen, refused = plan(`"S"."T"`, have, cols, true)
	if len(widen) != 1 || widen[0].typ != "NUMBER(19)" || len(refused) != 1 {
		t.Errorf("allowed: widen %v, refused %+v", widen, refused)
	}
	many := &tableInfo{}
	for i := range maxTargetCols - 1 {
		many.cols = append(many.cols, tableColumn{dcol: dcol{name: "x" + strings.Repeat("y", i), typ: "CLOB"}})
	}
	adds, _, refused = plan(`"S"."T"`, many, []dcol{{name: "n1", typ: "CLOB"}, {name: "n2", typ: "CLOB"}}, false)
	if len(adds) != 1 || len(refused) != 1 || refused[0].Column != "n2" || !strings.Contains(refused[0].Reason, "999 columns") {
		t.Errorf("at 998 columns: adds %v, refused %+v", adds, refused)
	}
	if l := many.limits(); l.MaxColumns != 999 {
		t.Errorf("limits %+v", l)
	}
}

func TestDestinationNameChecks(t *testing.T) {
	ok := arrow.NewSchema([]arrow.Field{{Name: "Mixed Case ✓", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if f := nameFailure("Table 'x'", ok); f != nil {
		t.Errorf("a name Oracle takes: %v", f)
	}
	for table, s := range map[string]*arrow.Schema{
		`a"b`:                   ok,
		"":                      ok,
		strings.Repeat("é", 65): ok,
		"T":                     arrow.NewSchema([]arrow.Field{{Name: "x\x00", Type: arrow.PrimitiveTypes.Int64}}, nil),
	} {
		var f *check.Failure
		if err := nameFailure(table, s); !errors.As(err, &f) || f.Reason != "name_invalid" {
			t.Errorf("%q: %v", table, err)
		}
	}
	if err := validName("column", `a"b`); err == nil {
		t.Error(`a column named a"b passed`)
	}
}

// Of two patch columns that fill one held column, the first's UPDATE leaves
// it held for the second's to match, and the second's takes it out.
func TestPatchSQLSharedHeldColumn(t *testing.T) {
	w := &writer{schema: "S", user: "U"}
	s := stagedStream{Table: "T", Fills: map[string]string{"v__v2": "v"}}
	types := map[string]string{"v__v2": "CLOB", "v": "NUMBER(10)", colID: "VARCHAR2(36 BYTE)", colHeld: "CLOB"}
	p := prepared{patch: []string{"v__v2", "v", colID, colHeld}, patches: `"U"."P"`, patchTypes: types, into: types}
	stmts := w.PatchSQL(s, p)
	if len(stmts) != 2 {
		t.Fatalf("%d statements: %q", len(stmts), stmts)
	}
	unhold := `t."` + colHeld + `" = `
	for i, want := range []bool{false, true} {
		if got := strings.Contains(stmts[i], unhold); got != want {
			t.Errorf("statement %d takes the held column out: %v, want %v\n%s", i, got, want, stmts[i])
		}
	}
}

// An overwrite read's table takes the type a person widened its target to,
// where Oracle changes it in place.
func TestWidenedTo(t *testing.T) {
	read := &tableInfo{cols: []tableColumn{
		{dcol: dcol{name: "a", typ: "NUMBER(10)"}}, {dcol: dcol{name: "b", typ: "NUMBER(10)"}},
		{dcol: dcol{name: "c", typ: "NUMBER(19)"}}, {dcol: dcol{name: "d", typ: "NUMBER(10)"}},
	}}
	target := map[string]string{"a": "NUMBER(19)", "b": "CLOB", "c": "NUMBER(19)"}
	got := widenedTo(read, target)
	if len(got) != 1 || got[0].name != "a" || got[0].typ != "NUMBER(19)" {
		t.Errorf("widened %+v, want a to NUMBER(19)", got)
	}
}
