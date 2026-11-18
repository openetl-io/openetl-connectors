package oracle

import (
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// The statements are as LogMiner wrote them on Oracle 23ai Free.
func TestParseRedo(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		op   string
		vals map[string]expr
		// An UPDATE's WHERE.
		old map[string]expr
	}{
		{
			sql: `insert into "OPENETL"."T"("ID","C","N") values ('1','it''s',NULL);`,
			op:  source.OpInsert,
			vals: map[string]expr{
				"ID": {lit: true, text: "1"}, "C": {lit: true, text: "it's"}, "N": {null: true},
			},
		},
		{
			sql: `update "OPENETL"."T" set "NAME" = 'dos' where "ID" = '2' and "NAME" = 'two' and "D" IS NULL;`,
			op:  source.OpUpdate,
			vals: map[string]expr{
				"ID": {lit: true, text: "2"}, "NAME": {lit: true, text: "dos"}, "D": {null: true},
			},
			old: map[string]expr{
				"ID": {lit: true, text: "2"}, "NAME": {lit: true, text: "two"}, "D": {null: true},
			},
		},
		{
			sql: `delete from "OPENETL"."T" where "ID" = '3' and "D" = TO_DATE('-4712-01-01 13:14:15', 'SYYYY-MM-DD HH24:MI:SS') and ROWID = 'AAAR3HAATAAAAPrAAA';`,
			op:  source.OpDelete,
			vals: map[string]expr{
				"ID": {lit: true, text: "3"},
				"D":  {fn: "TO_DATE", args: []expr{{lit: true, text: "-4712-01-01 13:14:15"}, {lit: true, text: "SYYYY-MM-DD HH24:MI:SS"}}},
			},
		},
		{
			sql: `insert into "OPENETL"."CDC_MM"("COL 1","COL 2") values (HEXTORAW('c102'),HEXTORAW('61'));`,
			op:  source.OpInsert,
			vals: map[string]expr{
				"COL 1": {fn: "HEXTORAW", args: []expr{{lit: true, text: "c102"}}},
				"COL 2": {fn: "HEXTORAW", args: []expr{{lit: true, text: "61"}}},
			},
		},
		{
			sql: `update "OPENETL"."T" a set a."CL" = NULL where a."ID" = '1' and a."N" IS NULL and ROWID = 'AAAR3HAATAAAAPrAAA';`,
			op:  source.OpUpdate,
			vals: map[string]expr{
				"ID": {lit: true, text: "1"}, "N": {null: true}, "CL": {null: true},
			},
			old: map[string]expr{
				"ID": {lit: true, text: "1"}, "N": {null: true},
			},
		},
		{
			sql: "DECLARE \n loc_c CLOB; \n buf_c VARCHAR2(6156);\n loc_b BLOB;\nBEGIN\n select \"CL\" into loc_c from \"OPENETL\".\"T\" where \"ID\" = '3' and \"N\" = 'x' for update;\n",
			op:  source.OpUpdate,
			vals: map[string]expr{
				"ID": {lit: true, text: "3"}, "N": {lit: true, text: "x"},
			},
		},
		{
			sql: `XML DOC BEGIN:  select "X" from "OPENETL"."T" where "ID" = '1' and "N" = 'a'`,
			op:  source.OpUpdate,
			vals: map[string]expr{
				"ID": {lit: true, text: "1"}, "N": {lit: true, text: "a"},
			},
		},
		{
			sql: `insert into "S"."T"("A","B") values ('x' || UNISTR('\00FC'),EMPTY_CLOB());`,
			op:  source.OpInsert,
			vals: map[string]expr{
				"A": {cat: []expr{{lit: true, text: "x"}, {fn: "UNISTR", args: []expr{{lit: true, text: `\00FC`}}}}},
				"B": {fn: "EMPTY_CLOB"},
			},
		},
	} {
		r, err := parseRedo(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if r.op != tc.op || !reflect.DeepEqual(r.vals, tc.vals) {
			t.Errorf("%s:\n got %s %#v\nwant %s %#v", tc.sql, r.op, r.vals, tc.op, tc.vals)
		}
		if !reflect.DeepEqual(r.old, tc.old) {
			t.Errorf("%s: old values %#v, want %#v", tc.sql, r.old, tc.old)
		}
	}
	for _, bad := range []string{
		`insert into "S"."T"("A") values (Unsupported Type);`,
		`select 1 from dual;`,
		`update "S"."T" set "A" = 'x' where "A" = 'y' and;`,
	} {
		if _, err := parseRedo(bad); err == nil {
			t.Errorf("%s: parsed", bad)
		}
	}
}

func TestRedoValue(t *testing.T) {
	ts := func(s string) expr { return expr{fn: "TO_TIMESTAMP", args: []expr{{lit: true, text: s}}} }
	for _, tc := range []struct {
		c    column
		e    expr
		want any
	}{
		{column{dataType: "NUMBER", kind: kDecimal}, expr{lit: true, text: "-1.5000000000000000000000000000000000000E-130"}, "-0." + zeros(129) + "15"},
		{column{dataType: "NUMBER", kind: kDecimal}, expr{lit: true, text: "9.99000000000000000000000000000000000000E+125"}, "999" + zeros(123)},
		{column{dataType: "NUMBER", kind: kInt64}, expr{lit: true, text: "42"}, "42"},
		{column{dataType: "BINARY_FLOAT", kind: kFloat32}, expr{lit: true, text: "1.25E+000"}, float32(1.25)},
		{column{dataType: "BINARY_DOUBLE", kind: kFloat64}, expr{lit: true, text: "-Inf"}, math.Inf(-1)},
		{column{dataType: "NVARCHAR2", kind: kString}, expr{fn: "UNISTR", args: []expr{{lit: true, text: `\65E5\672C\00FC\D83D\DE00`}}}, "日本ü😀"},
		{column{dataType: "RAW", kind: kBinary}, expr{fn: "HEXTORAW", args: []expr{{lit: true, text: "deadbeef"}}}, []byte{0xde, 0xad, 0xbe, 0xef}},
		{column{dataType: "DATE", kind: kTimestamp}, expr{fn: "TO_DATE", args: []expr{{lit: true, text: "-4712-01-01 13:14:15"}}}, time.Date(-4712, 1, 1, 13, 14, 15, 0, time.UTC)},
		{column{dataType: "TIMESTAMP(9)", kind: kTimestampNS}, ts(" 2026-11-03 10:00:00.123456789"), time.Date(2026, 11, 3, 10, 0, 0, 123456789, time.UTC)},
		{column{dataType: "INTERVAL DAY(3) TO SECOND(6)", kind: kDaySecond}, expr{fn: "TO_DSINTERVAL", args: []expr{{lit: true, text: "+003 04:05:06.789000"}}}, "+003 04:05:06.789000"},
		{column{dataType: "VARCHAR2", kind: kString}, expr{null: true}, nil},
	} {
		got, err := redoValue(tc.c, tc.e)
		if err != nil {
			t.Fatalf("%s %#v: %v", tc.c.dataType, tc.e, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s %#v: got %#v, want %#v", tc.c.dataType, tc.e, got, tc.want)
		}
	}
	tz, err := redoValue(column{kind: kZoned}, expr{fn: "TO_TIMESTAMP_TZ", args: []expr{{lit: true, text: " 2026-11-03 10:00:00.500000000 -05:30"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 11, 3, 15, 30, 0, 5e8, time.UTC); !tz.(time.Time).Equal(want) {
		t.Errorf("TO_TIMESTAMP_TZ: got %v, want %v", tz, want)
	}
	if f, err := redoValue(column{dataType: "BINARY_DOUBLE", kind: kFloat64}, expr{lit: true, text: "Nan"}); err != nil || !math.IsNaN(f.(float64)) {
		t.Errorf("Nan: %v, %v", f, err)
	}
}

func zeros(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '0'
	}
	return string(b)
}

// The internal formats are as LogMiner gave them for rows it couldn't
// decode with the dictionary.
func TestRawValue(t *testing.T) {
	raw := func(s string) []byte {
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for _, tc := range []struct {
		c    column
		hex  string
		want any
	}{
		{column{dataType: "NUMBER", kind: kDecimal}, "c10b", "10"},
		{column{dataType: "NUMBER", kind: kInt32}, "c102", "1"},
		{column{dataType: "VARCHAR2", kind: kString}, "61", "a"},
		{column{dataType: "NVARCHAR2", kind: kString}, "65e5672c", "日本"},
		{column{dataType: "DATE", kind: kTimestamp}, "787e0b10100922", time.Date(2026, 11, 16, 15, 8, 33, 0, time.UTC)},
		{column{dataType: "RAW", kind: kBinary}, "0102", []byte{1, 2}},
	} {
		got, err := rawValue(tc.c, raw(tc.hex))
		if err != nil {
			t.Fatalf("%s %s: %v", tc.c.dataType, tc.hex, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s %s: got %#v, want %#v", tc.c.dataType, tc.hex, got, tc.want)
		}
	}
}

func TestPlainNumber(t *testing.T) {
	for in, want := range map[string]string{
		"1.25":    "1.25",
		"1.5E+3":  "1500",
		"1.5E-3":  "0.0015",
		"-2E+0":   "-2",
		"0E+0":    "0",
		"1.0E+40": "1" + zeros(40),
		"12.5E-1": "1.25",
	} {
		got, err := plainNumber(in)
		if err != nil || got != want {
			t.Errorf("%s: got %q, %v; want %q", in, got, err, want)
		}
	}
}

func TestCovered(t *testing.T) {
	logs := []redoLog{{first: 100, next: 200, thread: 1, sequence: 7}, {first: 200, next: 300, thread: 1, sequence: 8}, {first: 300, next: math.MaxUint64, thread: 1, sequence: 9, online: true}}
	if err := covered(logs, 150); err != nil {
		t.Error(err)
	}
	if err := covered(logs, 50); err == nil {
		t.Error("an SCN before the oldest log is covered")
	}
	gone := append([]redoLog{}, logs...)
	gone[1].unavailable = true
	if err := covered(gone, 150); err == nil {
		t.Error("a deleted log is covered")
	}
	if err := covered([]redoLog{logs[0], logs[2]}, 150); err == nil {
		t.Error("a missing sequence is covered")
	}
}

// A window takes each thread's logs up to its end, though the logs are
// listed by thread and an earlier thread's run past it.
func TestUpTo(t *testing.T) {
	logs := []redoLog{
		{first: 100, thread: 1, sequence: 7}, {first: 200, thread: 1, sequence: 8}, {first: 300, thread: 1, sequence: 9},
		{first: 100, thread: 2, sequence: 4}, {first: 260, thread: 2, sequence: 5},
	}
	var got []string
	for _, l := range upTo(logs, 250) {
		got = append(got, fmt.Sprintf("%d/%d", l.thread, l.sequence))
	}
	if want := []string{"1/7", "1/8", "2/4"}; !slices.Equal(got, want) {
		t.Errorf("up to 250: %v, want %v", got, want)
	}
	if n := len(upTo(logs, 0)); n != len(logs) {
		t.Errorf("up to the current SCN: %d logs", n)
	}
	if len(logs) != 5 || logs[2].sequence != 9 {
		t.Errorf("upTo changed its argument: %v", logs)
	}
}

func TestEra(t *testing.T) {
	// Oracle's 1 BC, as go-ora reads it, is Go's year 0.
	if got := era(time.Date(-1, 3, 1, 0, 0, 0, 0, time.UTC)); got.Year() != 0 {
		t.Errorf("1 BC: got year %d", got.Year())
	}
	if got := wall(time.Date(-4712, 1, 1, 13, 0, 0, 0, time.UTC)); got.Year() != -4711 {
		t.Errorf("4712 BC: got year %d", got.Year())
	}
}

// Retention reports on the database's earliest position, whatever another
// database's positions are.
func TestEarliest(t *testing.T) {
	positions := []cdcState{{From: 100, DB: "1:5"}, {From: 50, DB: "2:5"}, {From: 80, DB: "1:5"}}
	if got := earliest(positions, "1:5"); got == nil || got.From != 80 {
		t.Errorf("of 1:5: %+v", got)
	}
	if got := earliest(positions, "3:5"); got != nil {
		t.Errorf("of 3:5, which has none: %+v", got)
	}
}

// An update moves its row when a key column's value changes.
func TestKeyMoved(t *testing.T) {
	tb := table{cols: cols("ID NUMBER", "V VARCHAR2"), key: []int{0}}
	for _, c := range []struct {
		old, new []any
		moved    bool
	}{
		{[]any{"1", "a"}, []any{"2", "a"}, true},
		{[]any{"1", "a"}, []any{"1", "b"}, false},
		{[]any{nil, "a"}, []any{"2", "a"}, false},
	} {
		if got := keyMoved(tb, c.old, c.new); got != c.moved {
			t.Errorf("%v to %v: moved %v", c.old, c.new, got)
		}
	}
}
