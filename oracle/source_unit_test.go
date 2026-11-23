package oracle

import (
	"encoding/json"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	go_ora "github.com/sijms/go-ora/v2"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/check"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func checkNamesOf(checks []check.Check) []string {
	out := make([]string, len(checks))
	for i, c := range checks {
		out[i] = c.Name
	}
	return out
}

func mustSource(t *testing.T, config string) *Source {
	t.Helper()
	s, err := NewSource([]byte(config))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The lists are valid for any config and request, streams whose names make
// the same check name included, and hold what the config calls for.
func TestSourceCheckLists(t *testing.T) {
	s := mustSource(t, `{"dsn": "oracle://u:p@db.example.com:/SVC", "tables": ["Orders", "ORDERS"]}`)
	if s.addr != "db.example.com:1521" || s.user != "u" || s.service != "SVC" {
		t.Errorf("addr %q, user %q, service %q", s.addr, s.user, s.service)
	}
	checks := s.Checks()
	if err := check.Validate(checks); err != nil {
		t.Error(err)
	}
	cdcItems := []string{"cdc.archivelog", "cdc.supplemental-logging", "cdc.privilege.logmining", "cdc.privilege.dbms-logmnr", "cdc.privilege.views", "cdc.logminer"}
	want := append([]string{"net.dns", "net.connect", "db.connect", "schema.exists", "table.orders.read", "table.orders.read-2"}, cdcItems...)
	if got := checkNamesOf(checks); !slices.Equal(got, want) {
		t.Errorf("checks %v, want %v", got, want)
	}
	for _, c := range checks {
		if strings.HasPrefix(c.Name, "cdc.") != c.Advisory {
			t.Errorf("%s: advisory %v", c.Name, c.Advisory)
		}
		if strings.HasPrefix(c.Name, "cdc.") && !strings.Contains(c.Description, "cdc") {
			t.Errorf("%s: description %q", c.Name, c.Description)
		}
		if c.Field != "" && !slices.Contains([]string{"dsn", "schema", "tables"}, c.Field) {
			t.Errorf("%s: field %q isn't a config property", c.Name, c.Field)
		}
	}

	req := source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{
		{Name: "Orders", SyncMode: source.CDC}, {Name: "ORDERS", SyncMode: source.Incremental, Cursor: "AT"}, {Name: "X"},
	}}
	pf := s.Preflight(req)
	if err := check.Validate(pf); err != nil {
		t.Error(err)
	}
	want = append(append([]string{"db.connect"}, cdcItems...), "cdc.retention",
		"stream.orders.read", "stream.orders.mode", "stream.orders.supplemental-logging", "stream.orders.flashback",
		"stream.orders.read-2", "stream.orders.mode-2", "stream.x.read", "stream.x.mode")
	if got := checkNamesOf(pf); !slices.Equal(got, want) {
		t.Errorf("preflight %v, want %v", got, want)
	}
	for _, c := range pf {
		if c.Advisory != (c.Name == "cdc.retention") {
			t.Errorf("preflight %s: advisory %v", c.Name, c.Advisory)
		}
	}
	// Without cdc, nothing of it.
	if got := checkNamesOf(s.Preflight(source.PreflightRequest{Streams: req.Streams[2:]})); !slices.Equal(got, []string{"db.connect", "stream.x.read", "stream.x.mode"}) {
		t.Errorf("preflight without cdc %v", got)
	}

	// No tables named: one item for the schema's.
	s = mustSource(t, `{"dsn": "oracle://u:p@db.example.com:1522/SVC", "schema": "APP"}`)
	if s.addr != "db.example.com:1522" {
		t.Errorf("addr %q", s.addr)
	}
	if got := checkNamesOf(s.Checks()); !slices.Equal(got, append([]string{"net.dns", "net.connect", "db.connect", "schema.exists", "tables.read"}, cdcItems...)) {
		t.Errorf("checks without tables %v", got)
	}
}

func TestSourceConfig(t *testing.T) {
	s := mustSource(t, `{"dsn": "oracle://u:p@h:1521/S"}`)
	if c := s.cfg; c.ChunkRows != 100_000 || c.BatchBytes != 4<<20 || c.SnapshotChunkRows != 10_000 || c.MaxConnections != 4 || c.Schema != "" {
		t.Errorf("defaults %+v", c)
	}
	for _, bad := range []string{
		`{}`,
		`{"dsn": "oracle://u:p@h:1521/S", "tls": {}}`,
		`{"dsn": "oracle://u:p@h:1521/S", "chunkRows": 0}`,
		`{"dsn": "postgres://u:p@h:1521/S"}`,
		`{"dsn": "oracle://u:hunter2@h:port/S"}`,
		`{"dsn": "oracle://u:hunter2@h:1521/S?CONNECTION TIMEOUT=x"}`,
		`{"dsn": "oracle://u:hunter2@h:1521/S", "chunkRows": "x"}`,
		// A URL that doesn't parse, which net/url quotes, escaped, and whose
		// password a / cuts short.
		`{"dsn": "oracle://u:hunter2\"x@h:1521/S"}`,
		`{"dsn": "oracle://u:hunter2/x@h:1521/S"}`,
		`{"dsn": "oracle://u:hunter2%zz@h:1521/S"}`,
	} {
		_, err := NewSource([]byte(bad))
		switch {
		case err == nil:
			t.Errorf("%s: no error", bad)
		case strings.Contains(err.Error(), "hunter2"):
			t.Errorf("%s: the error shows the password: %v", bad, err)
		}
	}

	// The schema's properties each have a description, and its defaults are
	// the source's.
	var schema struct {
		Properties map[string]struct {
			Description string `json:"description"`
			Default     any    `json:"default"`
			Advanced    bool   `json:"x-advanced"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(SourceConfigSchema, &schema); err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"chunkRows": 100_000, "batchBytes": 4 << 20, "snapshotChunkRows": 10_000, "maxConnections": 4}
	for name, p := range schema.Properties {
		if p.Description == "" {
			t.Errorf("%s has no description", name)
		}
		if d, ok := want[name]; ok && (p.Default != d || !p.Advanced) {
			t.Errorf("%s: default %v, advanced %v", name, p.Default, p.Advanced)
		}
	}
	if len(schema.Properties) != 7 {
		t.Errorf("%d properties", len(schema.Properties))
	}
}

func TestKindOf(t *testing.T) {
	for _, c := range []struct {
		typ        string
		prec, scal int
		want       kind
	}{
		{"NUMBER", 4, 0, kInt16}, {"NUMBER", 9, 0, kInt32}, {"NUMBER", 18, 0, kInt64}, {"NUMBER", 19, 0, kDecimal},
		{"NUMBER", -1, -1, kDecimal}, {"NUMBER", 10, 2, kDecimal}, {"NUMBER", 5, -2, kDecimal}, {"NUMBER", -1, 0, kDecimal},
		{"FLOAT", 126, -1, kFloat64}, {"BINARY_DOUBLE", -1, -1, kFloat64}, {"BINARY_FLOAT", -1, -1, kFloat32},
		{"BOOLEAN", -1, -1, kBool}, {"VARCHAR2", -1, -1, kString}, {"NCHAR", -1, -1, kString}, {"CLOB", -1, -1, kString},
		{"LONG", -1, -1, kString}, {"ROWID", -1, -1, kString}, {"UROWID", -1, -1, kString}, {"XMLTYPE", -1, -1, kString},
		{"JSON", -1, -1, kJSON}, {"RAW", -1, -1, kBinary}, {"BLOB", -1, -1, kBinary}, {"LONG RAW", -1, -1, kBinary},
		{"DATE", -1, -1, kTimestamp}, {"TIMESTAMP(0)", -1, 0, kTimestamp}, {"TIMESTAMP(6)", -1, 6, kTimestamp},
		{"TIMESTAMP(9)", -1, 9, kTimestampNS}, {"TIMESTAMP(6) WITH TIME ZONE", -1, 6, kZoned},
		{"TIMESTAMP(9) WITH TIME ZONE", -1, 9, kZonedNS}, {"TIMESTAMP(3) WITH LOCAL TIME ZONE", -1, 3, kZoned},
		{"INTERVAL YEAR(9) TO MONTH", 9, 0, kYearMonth}, {"INTERVAL DAY(9) TO SECOND(9)", 9, 9, kDaySecond},
		{"BFILE", -1, -1, 0}, {"VECTOR", -1, -1, 0}, {"SDO_GEOMETRY", -1, -1, 0},
	} {
		if got := kindOf(column{dataType: c.typ, precision: c.prec, scale: c.scal}); got != c.want {
			t.Errorf("%s(%d, %d): kind %d, want %d", c.typ, c.prec, c.scal, got, c.want)
		}
	}
}

func TestDeclaredDecimal(t *testing.T) {
	for _, c := range []struct{ p, s, wp, ws int32 }{
		{10, 2, 10, 2}, {38, 0, 38, 0}, {38, 38, 38, 38}, {-1, -1, 38, 9}, {-1, 0, 38, 0},
		{5, -2, 7, 0}, {37, -5, 38, 0}, {2, 5, 5, 5}, {3, 127, 38, 38}, {19, 0, 19, 0},
	} {
		got := declaredDecimal(int(c.p), int(c.s))
		if got.Precision != c.wp || got.Scale != c.ws {
			t.Errorf("NUMBER(%d, %d): decimal(%d, %d), want (%d, %d)", c.p, c.s, got.Precision, got.Scale, c.wp, c.ws)
		}
	}
}

func TestParseInterval(t *testing.T) {
	for _, c := range []struct {
		s    string
		ym   bool
		want arrow.MonthDayNanoInterval
	}{
		{"+02-03", true, arrow.MonthDayNanoInterval{Months: 27}},
		{"-99-11", true, arrow.MonthDayNanoInterval{Months: -1199}},
		{"+178956970-07", true, arrow.MonthDayNanoInterval{Months: 2147483647}},
		{"-178956970-08", true, arrow.MonthDayNanoInterval{Months: -2147483648}},
		{"+01 02:03:04.5", false, arrow.MonthDayNanoInterval{Days: 1, Nanoseconds: (3600*2+60*3+4)*1e9 + 5e8}},
		{"-999999999 23:59:59.999999999", false, arrow.MonthDayNanoInterval{Days: -999999999, Nanoseconds: -86399999999999}},
		{"+000000000 00:00:00.000000001", false, arrow.MonthDayNanoInterval{Nanoseconds: 1}},
		{"+00 00:00:00", false, arrow.MonthDayNanoInterval{}},
	} {
		got, err := parseInterval(c.s, c.ym)
		if err != nil || got != c.want {
			t.Errorf("%q: %+v, %v, want %+v", c.s, got, err, c.want)
		}
	}
	for _, bad := range []string{"+999999999-11", "x", "1 2:3", "+01 00:00:00.0000000001"} {
		if _, err := parseInterval(bad, !strings.Contains(bad, ":")); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}

func cols(specs ...string) []column {
	var out []column
	for _, s := range specs {
		name, typ, _ := strings.Cut(s, " ")
		c := column{name: name, dataType: typ, precision: -1, scale: -1}
		if typ == "NUMBER" {
			c.precision, c.scale = 10, 0
		}
		c.kind = kindOf(c)
		out = append(out, c)
	}
	return out
}

func TestWhereBuilder(t *testing.T) {
	cs := cols("A NUMBER", "B VARCHAR2", "C RAW")
	var w where
	if err := w.after(cs, []string{"1", "x", "00ff"}); err != nil {
		t.Fatal(err)
	}
	if err := w.compare(cs[:1], "<=", []string{"9"}); err != nil {
		t.Fatal(err)
	}
	want := ` WHERE t."A" >= TO_NUMBER(:1) AND (t."A" > TO_NUMBER(:2) OR (t."A" = TO_NUMBER(:3) AND (t."B" > :4 OR (t."B" = :5 AND t."C" > :6))))` +
		` AND t."A" <= TO_NUMBER(:7)`
	if got := w.sql(); got != want {
		t.Errorf("sql\n%s\nwant\n%s", got, want)
	}
	if len(w.args) != 7 || w.args[3] != "x" || string(w.args[5].([]byte)) != "\x00\xff" || w.args[6] != "9" {
		t.Errorf("args %#v", w.args)
	}

	// The upper bound of a two-column key, after a bind already made.
	w = where{args: []any{uint64(7)}}
	if err := w.compare(cs[:2], "<=", []string{"5", "y"}); err != nil {
		t.Fatal(err)
	}
	if got, want := w.sql(), ` WHERE t."A" <= TO_NUMBER(:2) AND (t."A" < TO_NUMBER(:3) OR (t."A" = TO_NUMBER(:4) AND t."B" <= :5))`; got != want {
		t.Errorf("sql\n%s\nwant\n%s", got, want)
	}

	// Each orderable type's conversion.
	for typ, want := range map[string]string{
		"DATE":                              `TO_DATE(:1, 'SYYYY-MM-DD HH24:MI:SS')`,
		"TIMESTAMP(9)":                      `TO_TIMESTAMP(:1, 'SYYYY-MM-DD HH24:MI:SS.FF9')`,
		"TIMESTAMP(6) WITH TIME ZONE":       `TO_TIMESTAMP_TZ(:1, 'SYYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')`,
		"TIMESTAMP(6) WITH LOCAL TIME ZONE": `CAST(TO_TIMESTAMP_TZ(:1, 'SYYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM') AS TIMESTAMP WITH LOCAL TIME ZONE)`,
		"NVARCHAR2":                         `:1`,
		"CHAR":                              `:1`,
	} {
		var w where
		got, err := w.param(cols("X " + typ)[0], "v")
		if err != nil || got != want {
			t.Errorf("%s: %s, %v", typ, got, err)
		}
		if _, n := w.args[0].(go_ora.NVarChar); n != (typ == "NVARCHAR2") {
			t.Errorf("%s: bound as %T", typ, w.args[0])
		}
	}
	for _, typ := range []string{"BINARY_DOUBLE", "FLOAT", "CLOB", "BLOB", "INTERVAL DAY(2) TO SECOND(6)", "ROWID", "JSON", "BOOLEAN", "LONG"} {
		var w where
		if _, err := w.param(cols("X " + typ)[0], "v"); err == nil {
			t.Errorf("%s orders a read", typ)
		}
	}
	if err := (&where{}).compare(cs, ">", []string{"1"}); err == nil {
		t.Error("a short position compared")
	}
}

func TestKeyText(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		typ  string
		v    any
		want string
	}{
		{"NUMBER", "-0.0000000001", "-0.0000000001"},
		{"VARCHAR2", "ab  ", "ab  "},
		{"RAW", []byte{0, 0xab}, "00ab"},
		{"DATE", time.Date(-4712, 1, 1, 0, 0, 0, 0, time.UTC), "-4712-01-01 00:00:00"},
		{"DATE", time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC), "9999-12-31 23:59:59"},
		{"TIMESTAMP(9)", time.Date(1, 2, 3, 4, 5, 6, 1, time.UTC), "0001-02-03 04:05:06.000000001"},
		{"TIMESTAMP(3)", time.Date(2026, 1, 1, 0, 0, 0, 500_000_000, time.UTC), "2026-01-01 00:00:00.500000000"},
		{"TIMESTAMP(6) WITH TIME ZONE", time.Date(2026, 7, 1, 12, 0, 0, 0, ny), "2026-07-01 16:00:00.000000000 +00:00"},
		{"TIMESTAMP(6) WITH LOCAL TIME ZONE", time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC), "2026-07-01 12:00:00.000000000 +00:00"},
	} {
		if got := keyText(cols("X " + c.typ)[0], c.v); got != c.want {
			t.Errorf("%s %v: %q, want %q", c.typ, c.v, got, c.want)
		}
	}
}

// A state with a position or a key range holds only in the order it was
// made in, a range not started yet included; one without either holds in
// any.
func TestSameOrder(t *testing.T) {
	lo := "50"
	cs := cols("A NUMBER", "B NUMBER")
	byA, byB, loose := table{name: "T", cols: cs, key: []int{0}}, table{name: "T", cols: cs, key: []int{1}}, table{name: "T", cols: cs}
	for _, c := range []struct {
		t    table
		st   sourceState
		lost bool
	}{
		{byA, sourceState{}, false},
		{byA, sourceState{Done: true}, false},
		{loose, sourceState{Done: true}, false},
		{byA, sourceState{keyRange: keyRange{Lo: &lo}, Key: []string{"A"}}, false},
		{byA, sourceState{After: []string{"1"}, Key: []string{"A"}}, false},
		{byB, sourceState{keyRange: keyRange{Hi: &lo}, Key: []string{"A"}}, true},
		{byB, sourceState{After: []string{"1"}, Key: []string{"A"}}, true},
		{loose, sourceState{After: []string{"1"}, Key: []string{"A"}}, true},
		{byA, sourceState{After: []string{"1"}}, true},
	} {
		if err := sameOrder(c.t, c.st); (connectortest.CodeOf(err) == protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST) != c.lost {
			t.Errorf("key %v, state %+v: %v", orderKey(c.t), c.st, err)
		}
	}
}

func TestOrderableTypes(t *testing.T) {
	keyed := func(specs ...string) table {
		t := table{cols: cols(specs...)}
		for i := range t.cols {
			t.key = append(t.key, i)
		}
		return t
	}
	for _, c := range []struct {
		t                   table
		chunked, splittable bool
	}{
		{keyed("A NUMBER", "B NVARCHAR2"), true, true},
		{keyed("A DATE", "B NUMBER"), true, false},
		{keyed("A NUMBER", "B BINARY_DOUBLE"), false, false},
		{keyed("A INTERVAL DAY(2) TO SECOND(6)"), false, false},
		{table{cols: cols("A NUMBER")}, false, false},
	} {
		if chunked(c.t) != c.chunked || splittable(c.t) != c.splittable {
			t.Errorf("%v: chunked %v, splittable %v", c.t.cols, chunked(c.t), splittable(c.t))
		}
	}
}

func TestSplitArithmetic(t *testing.T) {
	text := func(rs []keyRange) string {
		var out []string
		for _, r := range rs {
			lo, hi := "-", "-"
			if r.Lo != nil {
				lo = *r.Lo
			}
			if r.Hi != nil {
				hi = *r.Hi
			}
			out = append(out, "("+lo+","+hi+"]")
		}
		return strings.Join(out, " ")
	}
	for _, c := range []struct {
		low, high int64
		n         int
		want      string
	}{
		{1, 100, 4, "(-,25] (25,50] (50,75] (75,-]"},
		{1, 3, 10, "(-,2] (2,-]"},
		{-10, 10, 2, "(-,0] (0,-]"},
		{5, 5, 4, ""},
	} {
		if got := text(cutRanges(big.NewInt(c.low), big.NewInt(c.high), c.n)); got != c.want {
			t.Errorf("%d..%d in %d: %s, want %s", c.low, c.high, c.n, got, c.want)
		}
	}
	huge := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	if got := cutRanges(big.NewInt(0), huge, 2); len(got) != 2 || *got[0].Hi != "5"+strings.Repeat("0", 37) {
		t.Errorf("0..1e38 in 2: %s", text(got))
	}

	for _, c := range []struct {
		s         string
		up        bool
		want      string
		wantExact bool
	}{
		{"7", false, "7", true}, {"5.5", false, "5", false}, {"5.5", true, "6", false},
		{"-5.5", false, "-6", false}, {"-5.5", true, "-5", false}, {"-0." + strings.Repeat("0", 128) + "1", false, "-1", false},
		{"1" + strings.Repeat("0", 125), true, "1" + strings.Repeat("0", 125), true},
	} {
		got, exact, err := parseNum(c.s, c.up)
		if err != nil || got.String() != c.want || exact != c.wantExact {
			t.Errorf("parseNum(%q, %v): %v %v %v", c.s, c.up, got, exact, err)
		}
	}

	str := func(s string) *string { return &s }
	for _, c := range []struct {
		r         keyRange
		pos       *string
		single    bool
		low, high string
	}{
		{keyRange{}, str("10"), true, "11", "<nil>"},
		{keyRange{}, str("10"), false, "10", "<nil>"},
		{keyRange{}, str("10.5"), true, "11", "<nil>"},
		{keyRange{Lo: str("20"), Hi: str("50")}, nil, true, "21", "50"},
		{keyRange{Hi: str("50")}, nil, true, "<nil>", "50"},
	} {
		low, high, err := remainder(c.r, c.pos, c.single)
		if err != nil || low.String() != c.low || high.String() != c.high {
			t.Errorf("remainder(%+v, %v, %v): %v %v %v", c.r, c.pos, c.single, low, high, err)
		}
	}
}

// The archived redo's reach is compared with twice the interval as it is,
// and shown to the minute.
func TestRetentionReach(t *testing.T) {
	for _, c := range []struct {
		secs  float64
		short bool
		shown string
	}{
		{91, true, "2m0s"}, {119.9, true, "2m0s"}, {120, false, "2m0s"}, {3600, false, "1h0m0s"},
	} {
		reach, short := retentionReach(c.secs, time.Minute)
		if short != c.short || reach.String() != c.shown {
			t.Errorf("%v seconds: reach %v, short %v", c.secs, reach, short)
		}
	}
}

// Rows of nulls count towards a batch's bytes, so a table of them is sent in
// batches of batchBytes too.
func TestBatcherCountsNulls(t *testing.T) {
	b := newBatcher(memory.NewGoAllocator(), cols("A NUMBER", "B VARCHAR2"), false)
	defer b.release()
	for range 3 {
		if err := b.add([]any{nil, nil}); err != nil {
			t.Fatal(err)
		}
	}
	if b.bytes == 0 {
		t.Error("three rows of nulls have no bytes")
	}
}
