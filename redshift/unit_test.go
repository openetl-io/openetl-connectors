package redshift

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func TestIsoInterval(t *testing.T) {
	for v, want := range map[arrow.MonthDayNanoInterval]string{
		{}: "P0M0DT0S",
		{Months: 1, Days: 2, Nanoseconds: 3_500_000_000}: "P1M2DT3.5S",
		{Months: -1, Nanoseconds: -500_000_000}:          "P-1M0DT-0.5S",
		{Days: -3, Nanoseconds: 1}:                       "P0M-3DT0.000000001S",
	} {
		if got := isoInterval(v); got != want {
			t.Errorf("isoInterval(%v) = %s, want %s", v, got, want)
		}
	}
}

var plainJSON = regexp.MustCompile(`^"(?:[A-Za-z0-9_]|\\u[0-9a-f]{4})*"$`)

// The metadata columns' JSON is searched as text, so a name's text must
// hold nothing that could match across another's.
func TestJSONNames(t *testing.T) {
	names := []string{"a", "a b", `q"uote`, "comma,colon:", "[]{}", "é", "😀", ""}
	for _, n := range names {
		j := jsonName(n)
		if !plainJSON.MatchString(j) {
			t.Errorf("jsonName(%q) = %s", n, j)
		}
		var back string
		if err := json.Unmarshal([]byte(j), &back); err != nil || back != n {
			t.Errorf("jsonName(%q) = %s, reads back as %q, %v", n, j, back, err)
		}
	}
	var list []string
	if err := json.Unmarshal([]byte(nameList(names)), &list); err != nil || len(list) != len(names) {
		t.Errorf("nameList: %v %v", list, err)
	}
	var held map[string]string
	if err := json.Unmarshal([]byte(heldObject([]string{"b", "a", "b"}, "id")), &held); err != nil || len(held) != 2 || held["a"] != "id" {
		t.Errorf("heldObject: %v %v", held, err)
	}
	if heldObject(nil, "id") != "" {
		t.Error("heldObject of no names isn't empty")
	}
}

func TestColumnOf(t *testing.T) {
	tz := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	for dt, want := range map[arrow.DataType]string{
		arrow.PrimitiveTypes.Int8:                      "smallint",
		arrow.PrimitiveTypes.Uint16:                    "integer",
		arrow.PrimitiveTypes.Uint32:                    "bigint",
		arrow.PrimitiveTypes.Uint64:                    "decimal(20,0)",
		arrow.BinaryTypes.LargeString:                  varcharMax,
		arrow.BinaryTypes.Binary:                       varbyteMax,
		tz:                                             "timestamptz",
		&arrow.TimestampType{Unit: arrow.Microsecond}:  "timestamp",
		&arrow.Decimal128Type{Precision: 10, Scale: 2}: "decimal(10,2)",
		&arrow.Decimal256Type{Precision: 50, Scale: 2}: varcharMax,
		arrow.FixedWidthTypes.MonthDayNanoInterval:     "varchar(64)",
		arrow.ListOf(arrow.PrimitiveTypes.Int64):       superType,
		arrow.FixedWidthTypes.Time64us:                 "time",
		arrow.FixedWidthTypes.Date64:                   "date",
	} {
		c, err := columnOf(arrow.Field{Name: "c", Type: dt})
		if err != nil || c.typ != want {
			t.Errorf("columnOf(%s) = %q, %v; want %q", dt, c.typ, err, want)
		}
	}
}

func TestTypes(t *testing.T) {
	for in, want := range map[string]string{
		"character varying(65535)":    "varchar(65535)",
		"numeric(20, 0)":              "decimal(20,0)",
		"timestamp with time zone":    "timestamptz",
		"timestamp without time zone": "timestamp",
		"binary varying(16777216)":    "varbyte(16777216)",
		"double precision":            "double precision",
	} {
		if got := normType(in); got != want {
			t.Errorf("normType(%q) = %q, want %q", in, got, want)
		}
	}
	for _, c := range []struct {
		from, to string
		ok       bool
	}{
		{"integer", "bigint", true},
		{"bigint", "double precision", true},
		{"bigint", varcharMax, true},
		{"bigint", "integer", false},
		{"varchar(36)", varcharMax, true},
		{varcharMax, "varchar(36)", false},
		{"bigint", "decimal(20,0)", true},
		{"bigint", "decimal(19,1)", false},
		{"decimal(10,2)", "decimal(12,3)", true},
		{varbyteMax, varcharMax, false},
		{superType, varcharMax, false},
	} {
		if got := widens(c.from, c.to); got != c.ok {
			t.Errorf("widens(%s, %s) = %v", c.from, c.to, got)
		}
	}
	if got := castTo(`"b"`, "boolean", varcharMax); !strings.Contains(got, "'true'") {
		t.Errorf("castTo of a boolean: %s", got)
	}
}

func TestLiterals(t *testing.T) {
	if got := (dialect{}).lit(`it's \`); got != `'it''s \'` {
		t.Errorf("lit: %s", got)
	}
	if got := (dialect{escapes: true}).lit(`it's \`); got != `'it''s \\'` {
		t.Errorf("lit with escapes: %s", got)
	}
	if got := jsonOf("[", "]", nil); got != "CAST(NULL AS varchar(65535))" {
		t.Errorf("jsonOf of nothing: %s", got)
	}
}

func TestConfig(t *testing.T) {
	const ok = `"dsn": "postgres://u:p@h:5439/db", "bucket": "my-bucket", "iamRole": "arn:aws:iam::123456789012:role/Copy"`
	if _, err := NewDestination([]byte(`{` + ok + `}`)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		`{"dsn": "postgres://h/db", "iamRole": "default"}`,
		`{"dsn": "postgres://h/db", "bucket": "b-b", "iamRole": "default", "prefix": "a b"}`,
		`{"dsn": "postgres://h/db", "bucket": "b-b", "iamRole": "arn:aws:iam::1:role/x' x"}`,
		`{"dsn": "postgres://h/db", "bucket": "b-b"}`,
		`{"dsn": "postgres://h/db", "bucket": "b-b", "accessKeyId": "AKIA"}`,
		`{` + ok + `, "unknown": 1}`,
	} {
		if _, err := NewDestination([]byte(bad)); err == nil {
			t.Errorf("NewDestination(%s) took it", bad)
		}
	}
	d, err := NewDestination([]byte(`{"dsn": "postgres://u:p@h:5439/db", "bucket": "b-b", "iamRole": "default,arn:aws:iam::123456789012:role/A"}`))
	if err == nil {
		t.Fatalf("a chain with default: %v", d)
	}
}

func TestCheckLists(t *testing.T) {
	d, err := NewDestination([]byte(`{"dsn": "postgres://u:p@h:5439/db?sslmode=verify-full", "bucket": "b-b", "iamRole": "default"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Validate(d.Checks()); err != nil {
		t.Fatal(err)
	}
	sch := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	pre := d.Preflight(destination.PreflightRequest{
		Streams: []destination.Stream{{Name: "a.b", Mode: destination.Merge, PrimaryKey: []string{"id"}}, {Name: "a-b"}},
		Schemas: map[string]*arrow.Schema{"a.b": sch},
	})
	if err := check.Validate(pre); err != nil {
		t.Fatal(err)
	}
}

func TestColumnChanges(t *testing.T) {
	have := []tableColumn{{column: column{name: "a", typ: "integer"}}, {column: column{name: "k", typ: "integer"}, key: true}}
	want := []column{{name: "a", typ: "bigint"}, {name: "k", typ: "bigint"}, {name: "new", typ: "bigint"}}
	gains, refused := columnChanges(have, want, true)
	if len(gains) != 1 || len(refused) != 1 || !strings.HasPrefix(refused[0], "k ") {
		t.Errorf("allowed: gains %v, refused %v", gains, refused)
	}
	if _, refused := columnChanges(have, want, false); len(refused) != 2 {
		t.Errorf("not allowed: refused %v", refused)
	}
}

// A batch of every type stages as a file of the load types, with the
// values COPY casts from, and a null in each column.
func TestStage(t *testing.T) {
	meta := func(ext string) arrow.Metadata { return arrow.NewMetadata([]string{extensionNameKey}, []string{ext}) }
	fields := []arrow.Field{
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "d128", Type: &arrow.Decimal128Type{Precision: 19, Scale: 2}},
		{Name: "d256", Type: &arrow.Decimal256Type{Precision: 50, Scale: 2}},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "bin", Type: arrow.BinaryTypes.Binary},
		{Name: "d32", Type: arrow.FixedWidthTypes.Date32},
		{Name: "d64", Type: arrow.FixedWidthTypes.Date64},
		{Name: "t32", Type: arrow.FixedWidthTypes.Time32ms},
		{Name: "t64", Type: arrow.FixedWidthTypes.Time64us},
		{Name: "tsz", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond}},
		{Name: "iv", Type: arrow.FixedWidthTypes.MonthDayNanoInterval},
		{Name: idColumn, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: meta(uuidExtension)},
		{Name: "j", Type: arrow.BinaryTypes.String, Metadata: meta(jsonExtension)},
		{Name: "l", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{Name: "st", Type: arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true})},
		{Name: "m", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
		{Name: "nul", Type: arrow.Null},
		{Name: heldColumn, Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: unchangedColumn, Type: arrow.ListOf(arrow.BinaryTypes.String)},
	}
	for i := range fields {
		fields[i].Nullable = true
	}
	schema := arrow.NewSchema(fields, nil)
	id := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	rows := fmt.Sprintf(`[{"b": true, "i8": -8, "u64": 18446744073709551615, "f32": 1.5, "d128": "12345678901234567.89",
		"d256": "123456789012345678901234567890123456789012345678.90", "s": "hi", "bin": "AAE=", "d32": 19000, "d64": 1641600000000,
		"t32": 3723004, "t64": 3723000005, "tsz": 1767323045123456789, "ts": 1767323045500,
		"iv": {"months": 1, "days": 2, "nanoseconds": 3500000000}, %q: %q, "j": "{\"a\": [1]}", "l": [1, null, 3],
		"st": {"a": 7}, "m": [{"key": "k", "value": 1}], "nul": null, %q: ["v", "b"], %q: ["n"]}, {}]`,
		idColumn, base64.StdEncoding.EncodeToString(id[:]), heldColumn, unchangedColumn)
	batch, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(rows))
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	w := &writer{dir: t.TempDir(), Epochs: sqlstage.NewEpochs[*stagingTable]()}
	e := destination.Epoch{N: 1}
	st, err := w.stage(w.Staging, "stg", e, "s", "t", nil, batch)
	if err != nil {
		t.Fatal(err)
	}
	p := st.parts[0]
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(p.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	tbl, err := pqarrow.ReadTable(t.Context(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Release()
	if int(tbl.NumCols()) != len(p.cols)+1 || tbl.NumRows() != 2 {
		t.Fatalf("%d columns, %d rows", tbl.NumCols(), tbl.NumRows())
	}
	got := map[string]string{}
	for i, c := range p.cols {
		ch := tbl.Column(i).Data().Chunk(0)
		if loaded := ch.DataType(); loaded.ID() != c.load.ID() || loadType(loaded) != loadType(c.load) {
			t.Errorf("%s: the file has %s, and its load type is %s", c.name, loaded, c.load)
		}
		if c.name != "nul" && (ch.IsNull(0) || !ch.IsNull(1)) {
			t.Errorf("%s: nulls %v %v", c.name, ch.IsNull(0), ch.IsNull(1))
		}
		got[c.name] = ch.ValueStr(0)
	}
	for name, want := range map[string]string{
		"u64": "18446744073709551615", "d128": "12345678901234567.89", "d256": "123456789012345678901234567890123456789012345678.90",
		"d64": "2022-01-08", "t32": "01:02:03.004000", "t64": "01:02:03.000005", "tsz": "2026-01-02T03:04:05.123456",
		"iv": "P1M2DT3.5S", idColumn: id.String(), "l": "[1,null,3]", "st": `{"a":7}`,
		heldColumn: heldObject([]string{"b", "v"}, id.String()), unchangedColumn: `["n"]`,
	} {
		if !strings.HasPrefix(got[name], want) {
			t.Errorf("%s: %q, want %q", name, got[name], want)
		}
	}
	if ord := tbl.Column(len(p.cols)).Data().Chunk(0).ValueStr(1); ord != "2" {
		t.Errorf("_openetl_ord of row 2: %s", ord)
	}

	// Another batch's columns start the next staging table.
	narrow, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema(fields[:2], nil), strings.NewReader(`[{"b": false}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer narrow.Release()
	if st, err = w.stage(w.Staging, "stg", e, "s", "t", nil, narrow); err != nil || st.Name != "stg_2" || fmt.Sprint(st.Earlier) != "[stg]" || st.Rows != 3 {
		t.Fatalf("%+v %v", st, err)
	}
	long, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema(fields[6:7], nil),
		strings.NewReader(`[{"s": "`+strings.Repeat("x", maxVarchar+1)+`"}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer long.Release()
	_, err = w.stage(w.Staging, "stg", e, "s", "t", nil, long)
	if f := check.Classify(err); f == nil || f.Code != protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID {
		t.Fatalf("a string past VARCHAR's limit: %v", err)
	}
	for _, p := range w.Staging[1]["s"].parts {
		p.discard()
	}
}

// Batches whose Arrow types differ but stage as the same columns share a
// file, each converted from its own types.
func TestStageMixedSources(t *testing.T) {
	first, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "s", Type: arrow.Null, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: true},
	}, nil), strings.NewReader(`[{"i": -8, "s": null, "ts": 1767323045500}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
	}, nil), strings.NewReader(`[{"i": 300, "s": "x", "ts": 1767323045123456789}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()

	w := &writer{dir: t.TempDir(), Epochs: sqlstage.NewEpochs[*stagingTable]()}
	e := destination.Epoch{N: 1}
	var st *stagingTable
	for _, b := range []arrow.RecordBatch{first, second} {
		if st, err = w.stage(w.Staging, "stg", e, "s", "t", nil, b); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, p := range st.parts {
			p.discard()
		}
	}()
	if len(st.parts) != 1 {
		t.Fatalf("%d files", len(st.parts))
	}
	p := st.parts[0]
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(p.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	tbl, err := pqarrow.ReadTable(t.Context(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Release()
	for i, want := range [][]string{{"-8", "300"}, {"(null)", "x"}, {"2026-01-02T03:04:05.5", "2026-01-02T03:04:05.123456"}} {
		var got []string
		for _, ch := range tbl.Column(i).Data().Chunks() {
			for r := range ch.Len() {
				got = append(got, ch.ValueStr(r))
			}
		}
		if len(got) != 2 || !strings.HasPrefix(got[0], want[0]) || !strings.HasPrefix(got[1], want[1]) {
			t.Errorf("%s: %q, want %q", p.cols[i].name, got, want)
		}
	}
}

// A merge's unchanged column comes only from a version that isn't older
// than the target's row, which mergePicks joins; without a seq, from any.
func TestMergePicks(t *testing.T) {
	w := &writer{schema: "s"}
	cols := []column{{name: "id", typ: "bigint"}, {name: "name", typ: varcharMax}, {name: seqColumn, typ: "decimal(38,0)"}, {name: unchangedColumn, typ: varcharMax}}
	s := stagedStream{Name: "users", Table: "users", Key: []string{"id"}, Mode: modeMerge}
	picks := func(cols []column) string {
		for _, q := range w.merge(s, prepared{cols: cols, rows: `"s"."stg"`, unchanged: []string{"name"}, everUnchanged: []string{"name"}}) {
			if strings.Contains(q, `TEMP TABLE "`+mergePicks+`"`) {
				return q
			}
		}
		return ""
	}
	q := picks(cols)
	if !strings.Contains(q, `LEFT JOIN "s"."users" cur ON cur."id" = x."id"`) || !strings.Contains(q, `NOT coalesce(cur."_openetl_seq" > x."_openetl_seq", false) THEN`) {
		t.Errorf("with a seq: %s", q)
	}
	if q := picks(slices.Delete(slices.Clone(cols), 2, 3)); q == "" || strings.Contains(q, "cur") {
		t.Errorf("without a seq: %s", q)
	}
}

// Of two patch columns that fill one held column, the first's UPDATE leaves
// it held for the second's to match, and the second's takes it out.
func TestPatchSharedHeldColumn(t *testing.T) {
	w := &writer{schema: "s"}
	s := stagedStream{Table: "t", Mode: modeMerge, Key: []string{"id"}, Fills: map[string]string{"price__v2": "price"}}
	patch := []column{{name: "id", typ: "bigint"}, {name: "price__v2", typ: "bigint"}, {name: "price", typ: "integer"}, {name: idColumn, typ: "varchar(36)"}, {name: heldColumn, typ: varcharMax}}
	stmts := w.patch(s, prepared{patch: patch, patches: `"s"."p"`})
	if len(stmts) != 2 {
		t.Fatalf("%d statements: %q", len(stmts), stmts)
	}
	unhold := `"` + heldColumn + `" = NULLIF(`
	for i, want := range []bool{false, true} {
		if got := strings.Contains(stmts[i], unhold); got != want {
			t.Errorf("statement %d takes the held column out: %v, want %v\n%s", i, got, want, stmts[i])
		}
	}
}

// Redshift's own errors are XX000, and say what they are in their message
// or, as a serializable isolation violation does, in its detail.
func TestClassify(t *testing.T) {
	for _, c := range []struct {
		err  *pgconn.PgError
		want protocolv1.ErrorCode
	}{
		{&pgconn.PgError{Code: "XX000", Message: "1023", Detail: "Serializable isolation violation on table - 119371, transactions forming the cycle are: 1745830, 1745762 (pid:15021)"}, protocolv1.ErrorCode_ERROR_CODE_TRANSIENT},
		{&pgconn.PgError{Code: "XX000", Message: "Disk Full", Detail: "error:  Disk Full\ncode:      1016"}, protocolv1.ErrorCode_ERROR_CODE_DESTINATION_QUOTA},
		{&pgconn.PgError{Code: "40001", Message: "could not serialize access"}, protocolv1.ErrorCode_ERROR_CODE_TRANSIENT},
		{&pgconn.PgError{Code: "42501", Message: "permission denied for schema s"}, protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED},
	} {
		if f := check.Classify(classify(c.err)); f == nil || f.Code != c.want {
			t.Errorf("%s %s: %v, want %s", c.err.Message, c.err.Detail, f, c.want)
		}
	}
}

// Each load's object is its own, though another deployment of a pipeline of
// the same name stages a table of the same name in the same bucket, and no
// key is a prefix of another, whose object COPY would read too.
func TestStoreKeys(t *testing.T) {
	s := &store{bucket: "b", prefix: "p"}
	var keys []string
	for _, name := range []string{"_openetl_stage_a", "_openetl_stage_a", "_openetl_stage_a_2", "_openetl_stage_a_20"} {
		keys = append(keys, s.key(name))
	}
	for i, a := range keys {
		if !strings.HasPrefix(a, "p/") || !strings.HasSuffix(a, ".parquet") {
			t.Errorf("key %q", a)
		}
		for j, b := range keys {
			if i != j && strings.HasPrefix(b, a) {
				t.Errorf("COPY of %q reads %q too", a, b)
			}
		}
	}
}
