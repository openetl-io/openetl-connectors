package mysql

import (
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func TestNormTypeAndWidens(t *testing.T) {
	for in, want := range map[string]string{
		"INT(11)": "int", "int(10) unsigned zerofill": "int unsigned", "tinyint(1)": "tinyint(1)", "bigint(20) unsigned": "bigint unsigned",
		"decimal(10,2)": "decimal(10,2)", "varchar(64)": "varchar(64)",
	} {
		if got := normType(in); got != want {
			t.Errorf("normType(%q) = %q, want %q", in, got, want)
		}
	}
	for _, c := range []struct {
		from, to string
		ok       bool
	}{
		{"int", "bigint", true}, {"bigint", "int", false}, {"int unsigned", "bigint", true}, {"int unsigned", "int", false},
		{"float", "double", true}, {"double", "float", false}, {"varchar(10)", "varchar(20)", true}, {"varchar(20)", "varchar(10)", false},
		{"varchar(10)", "longtext", true}, {"varbinary(10)", "longblob", true}, {"longblob", "longtext", false},
		{"int", "decimal(12,0)", true}, {"bigint", "decimal(12,0)", false}, {"decimal(10,2)", "decimal(12,2)", true},
		{"decimal(10,2)", "decimal(10,3)", false},
	} {
		if got := widens(c.from, c.to); got != c.ok {
			t.Errorf("widens(%q, %q) = %v", c.from, c.to, got)
		}
	}
}

func TestBinlogPositionsAndSeqs(t *testing.T) {
	a := binlogPos{File: "binlog.000009", Pos: 900}
	b := binlogPos{File: "binlog.000010", Pos: 4}
	if a.number() != 9 || !a.less(b) || b.less(a) || a.less(a) {
		t.Fatalf("%d %v %v", a.number(), a.less(b), b.less(a))
	}
	// Seqs order by epoch, then file, then position, then row.
	seqs := []string{
		seqOf(0, a, 0).BigInt().String(), seqOf(0, a, 1).BigInt().String(), seqOf(0, binlogPos{File: "binlog.000009", Pos: 901}, 0).BigInt().String(),
		seqOf(0, b, 0).BigInt().String(), seqOf(1, binlogPos{File: "binlog.000001", Pos: 4}, 0).BigInt().String(),
	}
	for i := 1; i < len(seqs); i++ {
		x, _ := new(big.Int).SetString(seqs[i-1], 10)
		y, _ := new(big.Int).SetString(seqs[i], 10)
		if x.Cmp(y) >= 0 || y.BitLen() > 126 {
			t.Fatalf("seq %s then %s", seqs[i-1], seqs[i])
		}
	}
}

func TestCutRanges(t *testing.T) {
	r := cutRanges(big.NewInt(0), big.NewInt(100), 4)
	var bounds []string
	for _, k := range r {
		if k.Hi != nil {
			bounds = append(bounds, *k.Hi)
		}
	}
	if len(r) != 4 || r[0].Lo != nil || r[3].Hi != nil || !slices.Equal(bounds, []string{"25", "50", "75"}) {
		t.Fatalf("%v", bounds)
	}
	if r := cutRanges(big.NewInt(5), big.NewInt(6), 8); r != nil {
		t.Fatalf("a range of one value cut into %d", len(r))
	}
	lo, hi, err := intRemainder(keyRange{Lo: new("10"), Hi: new("20")}, nil, true)
	if err != nil || lo.Int64() != 11 || hi.Int64() != 20 {
		t.Fatalf("%v %v %v", lo, hi, err)
	}
	lo, _, _ = intRemainder(keyRange{}, new("15"), false)
	if lo.Int64() != 15 {
		t.Fatalf("a composite key's position %v", lo)
	}
}

func TestKinds(t *testing.T) {
	for _, c := range []struct {
		dataType, columnType string
		precision            int
		want                 kind
	}{
		{"tinyint", "tinyint(1)", 0, kBool}, {"tinyint", "tinyint(1) unsigned", 0, kInt16}, {"smallint", "smallint unsigned", 0, kInt32},
		{"int", "int unsigned", 0, kInt64}, {"bigint", "bigint unsigned", 0, kUint64}, {"bit", "bit(1)", 1, kBool}, {"bit", "bit(9)", 9, kBit},
		{"point", "point", 0, kGeometry}, {"json", "json", 0, kJSON}, {"year", "year", 0, kYear}, {"vector", "vector(3)", 0, 0},
	} {
		if got := kindOf(sourceColumn{dataType: c.dataType, columnType: c.columnType, precision: c.precision}); got != c.want {
			t.Errorf("%s: kind %d, want %d", c.columnType, got, c.want)
		}
	}
	if d := declaredDecimal(65, 30); d.Precision != 38 || d.Scale != 9 {
		t.Fatalf("decimal(65, 30) declared %v", d)
	}
}

func TestBinlogValuesReadAsSelectDoes(t *testing.T) {
	enum := sourceColumn{kind: kEnum}
	set := sourceColumn{kind: kSet}
	bit := sourceColumn{kind: kBit, precision: 10}
	bin := sourceColumn{kind: kBinary, dataType: "binary", octets: 4}
	dec, err := charsetDecoder("latin1")
	if err != nil {
		t.Fatal(err)
	}
	latin := sourceColumn{kind: kString, decoder: dec}
	bc := binlogColumn{enum: []string{"sad", "ok"}, set: []string{"a", "b", "c"}}
	for _, c := range []struct {
		col  sourceColumn
		in   any
		want string
	}{
		{enum, int64(2), "ok"}, {enum, int64(0), ""}, {set, int64(5), "a,c"}, {set, int64(0), ""},
		{bit, int64(0x2aa), "\x02\xaa"}, {bin, []byte{1, 2}, "\x01\x02\x00\x00"}, {latin, "caf\xe9", "café"},
		{sourceColumn{kind: kUint64}, uint64(18446744073709551615), "18446744073709551615"},
	} {
		got, err := binlogValue(c.col, bc, c.in)
		if err != nil {
			t.Fatal(err)
		}
		if b, _ := text(got); string(b) != c.want {
			t.Errorf("%v: %q, want %q", c.in, b, c.want)
		}
	}
	if v, _ := binlogValue(sourceColumn{kind: kInt32}, bc, uint16(65535)); v != int64(65535) {
		t.Fatalf("%v", v)
	}
	if _, err := charsetDecoder("dec8"); err == nil {
		t.Fatal("dec8 has a decoder")
	}
}

func TestTemporalValues(t *testing.T) {
	for in, ok := range map[string]bool{"0000-00-00": false, "2026-00-15": false, "2026-11-00 10:00:00": false, "2026-11-03": true} {
		layout := "2006-01-02"
		if len(in) > 10 {
			layout = "2006-01-02 15:04:05.999999"
		}
		_, got, err := parseTemporal([]byte(in), layout)
		if err != nil || got != ok {
			t.Errorf("%s: %v %v", in, got, err)
		}
	}
	tm, _, _ := parseTemporal([]byte("2026-11-03 10:00:00.5"), "2006-01-02 15:04:05.999999")
	if !tm.Equal(time.Date(2026, 11, 3, 10, 0, 0, 5e8, time.UTC)) {
		t.Fatal(tm)
	}
}

func TestAddersNormalize(t *testing.T) {
	cols := []sourceColumn{{name: "t", kind: kTime}, {name: "j", kind: kJSON}, {name: "b", kind: kBool}, {name: "i", kind: kInt16}}
	b, err := newBatcher(memory.NewGoAllocator(), cols, false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.release()
	if err := b.add([]any{[]byte("10:00:00.000000"), []byte(`{"a": [1, 2]}`), int64(5), int64(7)}); err != nil {
		t.Fatal(err)
	}
	if err := b.add([]any{[]byte("-01:00:00.500000"), []byte(""), []byte{0}, nil}); err != nil {
		t.Fatal(err)
	}
	var rec arrow.RecordBatch
	_ = b.flush(func(r arrow.RecordBatch) error { rec = r; return nil })
	defer rec.Release()
	got := []string{rec.Column(0).ValueStr(0), rec.Column(0).ValueStr(1), rec.Column(1).ValueStr(0), rec.Column(1).ValueStr(1),
		rec.Column(2).ValueStr(0), rec.Column(2).ValueStr(1)}
	want := []string{"10:00:00", "-01:00:00.500000", `{"a":[1,2]}`, "null", "true", "false"}
	if !slices.Equal(got, want) {
		t.Fatalf("%q", got)
	}
	small, err := newBatcher(memory.NewGoAllocator(), cols[3:], false)
	if err != nil {
		t.Fatal(err)
	}
	defer small.release()
	if err := small.add([]any{int64(40000)}); err == nil {
		t.Fatal("40000 fits an int16")
	}
}

func TestParseDSN(t *testing.T) {
	c, err := parseDSN("mysql://u:p%40ss@db.example:3307/app?tls=skip-verify")
	if err != nil || c.User != "u" || c.Passwd != "p@ss" || c.Addr != "db.example:3307" || c.DBName != "app" || c.TLSConfig != "skip-verify" {
		t.Fatalf("%+v %v", c, err)
	}
	c, err = parseDSN("u:p@tcp(h)/d")
	if err != nil || c.Addr != "h:3306" || c.DBName != "d" {
		t.Fatalf("%+v %v", c, err)
	}
	if got := quoteLiteral("a'b\\c\x00"); got != `'a\'b\\c\0'` {
		t.Fatal(got)
	}
}

// quoteIdent's, quoteLiteral's and the driver's escapes hold only in a
// session in utf8mb4 that keeps backslash escapes, whatever the DSN asks.
func TestSessionsAreUTF8WithBackslashEscapes(t *testing.T) {
	c, err := parseDSN("u:p@tcp(h)/d?charset=gbk&SQL_MODE='NO_BACKSLASH_ESCAPES'&@@session.character_set_client=sjis&Time_Zone='+05:30'&wait_timeout=60")
	if err != nil {
		t.Fatal(err)
	}
	got := connConfig(c)
	if dsn := got.FormatDSN(); !strings.Contains(dsn, "charset=utf8mb4&collation=utf8mb4_general_ci") {
		t.Errorf("%s: not in utf8mb4", dsn)
	}
	want := map[string]string{"wait_timeout": "60", "time_zone": "'+00:00'", "sql_mode": "'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'"}
	if !maps.Equal(got.Params, want) {
		t.Errorf("params %v, want %v", got.Params, want)
	}
	c, _ = parseDSN("u:p@tcp(h)/d?collation=utf8mb4_0900_ai_ci")
	if dsn := connConfig(c).FormatDSN(); !strings.Contains(dsn, "charset=utf8mb4&collation=utf8mb4_0900_ai_ci") {
		t.Errorf("%s: not in the DSN's collation", dsn)
	}
}

func TestTLSConfigErrors(t *testing.T) {
	c, _ := parseDSN("u:p@tcp(h)/d")
	for _, tc := range []*TLSConfig{{Mode: "on"}, {CA: "not a certificate"}, {Mode: "required", Cert: "x", Key: sdk.NewSecret("y")}} {
		if err := applyTLS(c, tc); err == nil {
			t.Errorf("%+v: no error", tc)
		}
	}
}

func TestTooManyConnectionsIsTransient(t *testing.T) {
	for _, err := range []error{&mysql.MySQLError{Number: 1040, Message: "Too many connections"}, &gomysql.MyError{Code: 1040}, &mysql.MySQLError{Number: 1226}} {
		if code := classify(fmt.Errorf("mysql: connect: %w", err)).(interface{ ErrorCode() protocolv1.ErrorCode }).ErrorCode(); code != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
			t.Errorf("%v: %v", err, code)
		}
	}
}

func TestStateTimesAreUTC(t *testing.T) {
	r := &cdcReader{server: "s", pos: binlogPos{File: "binlog.000001", Pos: 4}, lastCommit: time.Date(2026, 11, 16, 19, 0, 0, 0, time.FixedZone("IST", 19800))}
	b, err := r.state(&cdcStream{})
	if err != nil || !strings.Contains(string(b), `"at":"2026-11-16T13:30:00Z"`) {
		t.Fatalf("%s %v", b, err)
	}
}
