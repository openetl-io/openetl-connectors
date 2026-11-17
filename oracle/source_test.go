//go:build integration

package oracle

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl-connectors/keysplit"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// srcUser creates a user, dropping one of its name first, with the grants,
// and gives its dsn; the test drops it after.
func srcUser(t *testing.T, name string, create string, grants ...string) string {
	t.Helper()
	admin := adminDB(t)
	drop := `BEGIN EXECUTE IMMEDIATE 'DROP USER ` + name + ` CASCADE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1918 THEN RAISE; END IF; END;`
	exec(t, admin, drop, "CREATE USER "+name+" IDENTIFIED BY Openetl_S_pw1 "+create)
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), drop); err != nil {
			t.Errorf("drop %s: %v", name, err)
		}
	})
	for _, g := range grants {
		exec(t, admin, strings.ReplaceAll(g, "{user}", name))
	}
	u, err := url.Parse(testDSN())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(name, "Openetl_S_pw1")
	return u.String()
}

func srcSource(t *testing.T, dsn, extra string) *Source {
	t.Helper()
	s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q%s}`, dsn, extra))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// srcTable creates a table, dropping one of its name first, and drops it
// after the test.
func srcTable(t *testing.T, db *sql.DB, name, def string, stmts ...string) {
	t.Helper()
	dropTable(t, db, name)
	exec(t, db, "CREATE TABLE "+name+" "+def)
	t.Cleanup(func() { dropTable(t, db, name) })
	exec(t, db, stmts...)
}

// srcEvent is a read's batch of rows, resume point or completion.
type srcEvent struct {
	resume, complete bool
	state            string
	rows             []map[string]string
	added            []source.Split
}

// srcLog keeps a read's events in order. resplit, if set, runs before each
// batch is kept.
type srcLog struct {
	mu      sync.Mutex
	events  []srcEvent
	resplit func()
}

func (l *srcLog) add(e srcEvent) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
	return nil
}

func (l *srcLog) Rows(string, []json.RawMessage) error { return errors.New("unexpected raw rows") }
func (l *srcLog) Records(_ string, b arrow.RecordBatch) error {
	defer b.Release()
	if l.resplit != nil {
		l.resplit()
	}
	return l.add(srcEvent{rows: cells(b)})
}
func (l *srcLog) ResumePoint(_ string, s []byte) error {
	return l.add(srcEvent{resume: true, state: string(s)})
}
func (l *srcLog) Complete(_ string, s []byte) error {
	return l.add(srcEvent{complete: true, state: string(s)})
}
func (l *srcLog) Split(string, string) source.SplitEmitter { return srcSplit{l} }

type srcSplit struct{ l *srcLog }

func (o srcSplit) Rows([]json.RawMessage) error      { return errors.New("unexpected raw rows") }
func (o srcSplit) Records(b arrow.RecordBatch) error { return o.l.Records("", b) }
func (o srcSplit) ResumePoint(s []byte, added ...source.Split) error {
	return o.l.add(srcEvent{resume: true, state: string(s), added: added})
}
func (o srcSplit) Complete(s []byte, added ...source.Split) error {
	return o.l.add(srcEvent{complete: true, state: string(s), added: added})
}

// rows are the rows of events[from+1:to].
func srcRows(events []srcEvent, from, to int) []map[string]string {
	var out []map[string]string
	for _, e := range events[from+1 : to] {
		out = append(out, e.rows...)
	}
	return out
}

func (l *srcLog) rows() []map[string]string { return srcRows(l.events, -1, len(l.events)) }

func (l *srcLog) resumes() []int {
	var out []int
	for i, e := range l.events {
		if e.resume {
			out = append(out, i)
		}
	}
	return out
}

// completed is the state the read completed with.
func (l *srcLog) completed(t *testing.T) string {
	t.Helper()
	if n := len(l.events); n == 0 || !l.events[n-1].complete {
		t.Fatalf("the read didn't complete: %+v", l.events)
	}
	return l.events[len(l.events)-1].state
}

func srcRead(t *testing.T, s *Source, lanes int, streams ...source.ConfiguredStream) *srcLog {
	t.Helper()
	l := &srcLog{}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: streams, Lanes: lanes}, l); err != nil {
		t.Fatal(err)
	}
	return l
}

// cells are a batch's rows, each value spelled so that a test can say it
// exactly: timestamps as the UTC time they are, with Go's year (0 is 1 BC)
// and nine fraction digits; binary as hex; floats as Go spells them;
// intervals as months, days and nanoseconds; NULL as NULL.
func cells(b arrow.RecordBatch) []map[string]string {
	var out []map[string]string
	for i := range int(b.NumRows()) {
		row := map[string]string{}
		for j, f := range b.Schema().Fields() {
			row[f.Name] = cell(b.Column(j), i)
		}
		out = append(out, row)
	}
	return out
}

func cell(a arrow.Array, i int) string {
	if a.IsNull(i) {
		return "NULL"
	}
	switch x := a.(type) {
	case array.ExtensionArray:
		return cell(x.Storage(), i)
	case *array.Timestamp:
		v := x.Value(i).ToTime(x.DataType().(*arrow.TimestampType).Unit).UTC()
		return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%09d", v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second(), v.Nanosecond())
	case *array.Binary:
		return hex.EncodeToString(x.Value(i))
	case *array.Float32:
		return strconv.FormatFloat(float64(x.Value(i)), 'g', -1, 32)
	case *array.Float64:
		return strconv.FormatFloat(x.Value(i), 'g', -1, 64)
	case *array.MonthDayNanoInterval:
		v := x.Value(i)
		return fmt.Sprintf("%dm %dd %dns", v.Months, v.Days, v.Nanoseconds)
	}
	return a.ValueStr(i)
}

func rowKey(r map[string]string, cols ...string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = r[c]
	}
	return strings.Join(parts, "|")
}

func rowKeys(rows []map[string]string, cols ...string) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = rowKey(r, cols...)
	}
	return out
}

func fullRefresh(name string) source.ConfiguredStream {
	return source.ConfiguredStream{Name: name, SyncMode: source.FullRefresh}
}

const typesTable = `(id NUMBER(10) PRIMARY KEY,
	n4 NUMBER(4), n9 NUMBER(9), n18 NUMBER(18), n38 NUMBER(38), nn NUMBER, n102 NUMBER(10, 2),
	fl FLOAT, bf BINARY_FLOAT, bd BINARY_DOUBLE, bo BOOLEAN,
	v VARCHAR2(40), nv NVARCHAR2(20), c CHAR(5), nc NCHAR(4), cl CLOB, ncl NCLOB, lg LONG, rid ROWID, urid UROWID,
	x XMLTYPE, j JSON, r RAW(16), bl BLOB,
	d DATE, ts TIMESTAMP(6), ts9 TIMESTAMP(9), tz TIMESTAMP(6) WITH TIME ZONE, tz9 TIMESTAMP(9) WITH TIME ZONE,
	ltz TIMESTAMP(6) WITH LOCAL TIME ZONE, ym INTERVAL YEAR(9) TO MONTH, ds INTERVAL DAY(9) TO SECOND(9))`

// Every type the source reads, at its extremes, as NULL and as the values
// Oracle treats specially, comes as its value.
func TestSourceReadsEveryType(t *testing.T) {
	db := testDB(t)
	srcTable(t, db, "ST_TYPES", typesTable,
		`INSERT INTO ST_TYPES VALUES (1, 9999, 999999999, 999999999999999999, 99999999999999999999999999999999999999, 1e125, 99999999.99,
		  1e125, BINARY_FLOAT_NAN, BINARY_DOUBLE_INFINITY, TRUE,
		  'héllo ✓', N'日本語 ✓𝄞', 'ab', N'日本', NULL, N'ñ ✓', 'long text', CHARTOROWID('AAAR3sAAEAAAACXAAA'), CHARTOROWID('AAAR3sAAEAAAACXAAA'),
		  XMLTYPE('<a>1</a>'), JSON('{"a":[1,2.5,"x"],"b":null}'), HEXTORAW('00FF10'), NULL,
		  TO_DATE('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), TIMESTAMP '9999-12-31 23:59:59.999999', TIMESTAMP '9999-12-31 23:59:59.999999999',
		  TIMESTAMP '2026-07-01 12:00:00.123456 America/New_York', TIMESTAMP '9999-12-31 23:59:59.999999999 +00:00',
		  TIMESTAMP '2026-01-01 00:00:00.5 +02:00', INTERVAL '178956970-07' YEAR(9) TO MONTH, INTERVAL '999999999 23:59:59.999999999' DAY(9) TO SECOND(9))`,
		`INSERT INTO ST_TYPES VALUES (2, -9999, -999999999, -999999999999999999, -99999999999999999999999999999999999999, -1e-130, -0.01,
		  -1.5, -BINARY_FLOAT_INFINITY, BINARY_DOUBLE_NAN, FALSE,
		  '', N'', 'x', N'a', EMPTY_CLOB(), NULL, NULL, NULL, NULL,
		  NULL, NULL, NULL, EMPTY_BLOB(),
		  TO_DATE('-4712-01-01', 'SYYYY-MM-DD'), TO_TIMESTAMP('-4712-01-01 00:00:00.000001', 'SYYYY-MM-DD HH24:MI:SS.FF'),
		  TO_TIMESTAMP('0001-01-01 00:00:00.000000001', 'YYYY-MM-DD HH24:MI:SS.FF'),
		  TO_TIMESTAMP_TZ('-4712-01-01 00:00:00 +00:00', 'SYYYY-MM-DD HH24:MI:SS TZH:TZM'), TIMESTAMP '2026-01-01 05:30:00.000000001 Asia/Kolkata',
		  TIMESTAMP '1970-01-01 00:00:00 +00:00', INTERVAL '-178956970-07' YEAR(9) TO MONTH, INTERVAL '-999999999 23:59:59.999999999' DAY(9) TO SECOND(9))`,
		`INSERT INTO ST_TYPES (id) VALUES (3)`,
		`INSERT INTO ST_TYPES (id, nn, n102, fl, bf, bd, d, ts, tz, ym, ds) VALUES (4, 0, 0, 0.1, 3.40282347E+38F, 1.7976931348623157E+308D,
		  TO_DATE('-0001-12-31 23:00:00', 'SYYYY-MM-DD HH24:MI:SS'), TIMESTAMP '1582-10-15 00:00:00', TO_TIMESTAMP_TZ('-0001-12-31 23:00:00 -05:00', 'SYYYY-MM-DD HH24:MI:SS TZH:TZM'),
		  INTERVAL '0-0' YEAR TO MONTH, INTERVAL '0 00:00:00.000000001' DAY TO SECOND(9))`,
		// LOBs past 1 MB.
		`DECLARE lc CLOB; lb BLOB; BEGIN
		  DBMS_LOB.CREATETEMPORARY(lc, TRUE); DBMS_LOB.CREATETEMPORARY(lb, TRUE);
		  FOR i IN 1..1500 LOOP DBMS_LOB.WRITEAPPEND(lc, 1000, RPAD('é', 1000, 'x')); END LOOP;
		  FOR i IN 1..600 LOOP DBMS_LOB.WRITEAPPEND(lb, 2000, UTL_RAW.COPIES(HEXTORAW('DEADBEEF'), 500)); END LOOP;
		  UPDATE ST_TYPES SET cl = lc, bl = lb WHERE id = 1; COMMIT; END;`,
	)
	s := srcSource(t, testDSN(), `, "tables": ["ST_TYPES"], "batchBytes": 1`)
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Streams) != 1 {
		t.Fatalf("catalog %+v", cat)
	}
	cs := cat.Streams[0]
	// BOOLEAN, JSON, LONG and UROWID keep its changes from LogMiner.
	if !slices.Equal(cs.SyncModes, []source.SyncMode{source.FullRefresh, source.Incremental}) || !cs.SplitsByKey || !slices.Equal(cs.PrimaryKey, []string{"ID"}) || cs.Replay != source.Exact {
		t.Errorf("stream %+v", cs)
	}
	declared := map[string]string{}
	for _, f := range cs.Schema.Fields() {
		declared[f.Name] = f.Type.String()
	}
	for col, want := range map[string]string{
		"ID": "int64", "N4": "int16", "N18": "int64", "N38": "decimal(38, 0)", "NN": "decimal(38, 9)", "N102": "decimal(10, 2)",
		"FL": "float64", "BF": "float32", "BO": "bool", "BL": "binary", "D": "timestamp[us]", "TS9": "timestamp[us]",
		"TZ": "timestamp[us, tz=UTC]", "TZ9": "timestamp[us, tz=UTC]", "LTZ": "timestamp[us, tz=UTC]", "YM": "month_day_nano_interval",
	} {
		if declared[col] != want {
			t.Errorf("%s is declared %s, want %s", col, declared[col], want)
		}
	}

	rows := srcRead(t, s, 1, fullRefresh("ST_TYPES")).rows()
	if len(rows) != 4 {
		t.Fatalf("%d rows", len(rows))
	}
	slices.SortFunc(rows, func(a, b map[string]string) int { return strings.Compare(a["ID"], b["ID"]) })
	bigClob := strings.Repeat("é"+strings.Repeat("x", 999), 1500)
	bigBlob := strings.Repeat("deadbeef", 300_000)
	want := []map[string]string{{
		"ID": "1", "N4": "9999", "N9": "999999999", "N18": "999999999999999999", "N38": strings.Repeat("9", 38),
		"NN": "1" + strings.Repeat("0", 125), "N102": "99999999.99", "FL": "1e+125", "BF": "NaN", "BD": "+Inf", "BO": "true",
		"V": "héllo ✓", "NV": "日本語 ✓𝄞", "C": "ab   ", "NC": "日本  ", "CL": bigClob, "NCL": "ñ ✓", "LG": "long text",
		"RID": "AAAR3sAAEAAAACXAAA", "URID": "AAAR3sAAEAAAACXAAA", "X": "<a>1</a>", "J": `{"a":[1,2.5,"x"],"b":null}`,
		"R": "00ff10", "BL": bigBlob,
		"D": "9999-12-31 23:59:59.000000000", "TS": "9999-12-31 23:59:59.999999000", "TS9": "9999-12-31 23:59:59.999999999",
		"TZ": "2026-07-01 16:00:00.123456000", "TZ9": "9999-12-31 23:59:59.999999999Z", "LTZ": "2025-12-31 22:00:00.500000000",
		"YM": "2147483647m 0d 0ns", "DS": "0m 999999999d 86399999999999ns",
	}, {
		"ID": "2", "N4": "-9999", "N9": "-999999999", "N18": "-999999999999999999", "N38": "-" + strings.Repeat("9", 38),
		"NN": "-0." + strings.Repeat("0", 129) + "1", "N102": "-0.01", "FL": "-1.5", "BF": "-Inf", "BD": "NaN", "BO": "false",
		// Oracle stores an empty string as NULL. go-ora reads an empty LOB,
		// which isn't NULL to Oracle, as NULL.
		"V": "NULL", "NV": "NULL", "C": "x    ", "NC": "a   ", "CL": "NULL", "NCL": "NULL", "LG": "NULL",
		"RID": "NULL", "URID": "NULL", "X": "NULL", "J": "NULL", "R": "NULL", "BL": "NULL",
		// Oracle's -4712 is 4712 BC, which Go numbers -4711.
		"D": "-4711-01-01 00:00:00.000000000", "TS": "-4711-01-01 00:00:00.000001000", "TS9": "0001-01-01 00:00:00.000000001",
		"TZ": "-4711-01-01 00:00:00.000000000", "TZ9": "2026-01-01 00:00:00.000000001Z", "LTZ": "1970-01-01 00:00:00.000000000",
		"YM": "-2147483647m 0d 0ns", "DS": "0m -999999999d -86399999999999ns",
	}, {
		"ID": "3", "NN": "NULL", "TS9": "NULL", "TZ9": "NULL", "CL": "NULL", "BL": "NULL", "BO": "NULL", "YM": "NULL", "DS": "NULL",
	}, {
		"ID": "4", "NN": "0", "N102": "0", "FL": "0.1", "BF": "3.4028235e+38", "BD": "1.7976931348623157e+308",
		// 1 BC, which Go numbers 0. Oracle holds 23:00 at -05:00 on its last
		// day as 1 BC's first day, 04:00 UTC, as SYS_EXTRACT_UTC says.
		"D": "0-12-31 23:00:00.000000000", "TS": "1582-10-15 00:00:00.000000000", "TZ": "0-01-01 04:00:00.000000000",
		"YM": "0m 0d 0ns", "DS": "0m 0d 1ns",
	}}
	for i, w := range want {
		for col, v := range w {
			got, ok := rows[i][col]
			if col == "X" {
				// XMLSERIALIZE ends a document with a line break.
				got = strings.TrimSuffix(got, "\n")
			}
			if !ok || got != v {
				if len(v) > 100 {
					t.Errorf("row %d %s: %d bytes, %q..., want %d", i+1, col, len(got), got[:min(len(got), 20)], len(v))
					continue
				}
				t.Errorf("row %d %s: %q, want %q", i+1, col, got, v)
			}
		}
	}
	for col := range rows[0] {
		if col != "ID" && rows[2][col] != "NULL" {
			t.Errorf("row 3 %s: %q, want NULL", col, rows[2][col])
		}
	}
}

// An INTERVAL YEAR TO MONTH beyond what an Arrow interval holds fails the
// read, saying so.
func TestSourceRefusesTooLongAnInterval(t *testing.T) {
	db := testDB(t)
	srcTable(t, db, "ST_YM", "(id NUMBER(10) PRIMARY KEY, ym INTERVAL YEAR(9) TO MONTH)",
		`INSERT INTO ST_YM VALUES (1, INTERVAL '999999999-11' YEAR(9) TO MONTH)`)
	s := srcSource(t, testDSN(), `, "tables": ["ST_YM"]`)
	err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{fullRefresh("ST_YM")}}, &srcLog{})
	if err == nil || !strings.Contains(err.Error(), "longer than an Arrow interval holds") {
		t.Fatalf("read: %v", err)
	}
}

// A read resumed at any of its resume points yields exactly the rows after
// it, by keys of every orderable kind.
func TestSourceChunksAndResumesExactly(t *testing.T) {
	db := testDB(t)
	var vals []string
	for i := range 30 {
		vals = append(vals, fmt.Sprintf("('k%d', HEXTORAW('%02X'), TO_TIMESTAMP('2026-01-01 00:00:%02d.%09d', 'YYYY-MM-DD HH24:MI:SS.FF9'), %d.25, %d)", i%3, i%5, i, i, i, i))
	}
	srcTable(t, db, "ST_CHUNK", "(a VARCHAR2(10), b RAW(4), c TIMESTAMP(9), d NUMBER(10, 2), v NUMBER, PRIMARY KEY (a, b, c, d))",
		"INSERT INTO ST_CHUNK VALUES "+strings.Join(vals, ", "))
	vals = vals[:0]
	for i := range 20 {
		vals = append(vals, fmt.Sprintf("(N'é%d', TO_DATE('%d-%02d-28 23:59:59', 'SYYYY-MM-DD HH24:MI:SS'), TIMESTAMP '2026-01-01 00:00:00.123 +00:00' + NUMTODSINTERVAL(%d, 'SECOND'), %d)",
			i%2, []int{-4712, -1, 1, 1582, 9999}[i%5], 1+i%12, i, i))
	}
	srcTable(t, db, "ST_CHUNK2", "(n NCHAR(3), dt DATE, lt TIMESTAMP(3) WITH LOCAL TIME ZONE, v NUMBER, PRIMARY KEY (n, dt, lt))",
		"INSERT INTO ST_CHUNK2 VALUES "+strings.Join(vals, ", "))
	s := srcSource(t, testDSN(), `, "tables": ["ST_CHUNK", "ST_CHUNK2"], "chunkRows": 4`)
	for name, n := range map[string]int{"ST_CHUNK": 30, "ST_CHUNK2": 20} {
		first := srcRead(t, s, 1, fullRefresh(name))
		all := first.rows()
		if len(all) != n {
			t.Fatalf("%s: %d rows", name, len(all))
		}
		vs := rowKeys(all, "V")
		points := first.resumes()
		if len(points) != n/4+1 {
			t.Fatalf("%s: %d resume points", name, len(points))
		}
		for i, p := range points {
			cs := fullRefresh(name)
			cs.State = []byte(first.events[p].state)
			rest := rowKeys(srcRead(t, s, 1, cs).rows(), "V")
			if want := vs[min((i+1)*4, n):]; !slices.Equal(rest, want) {
				t.Fatalf("%s from resume point %d (%s): %v, want %v", name, i, cs.State, rest, want)
			}
		}
	}
}

// An incremental read reads from its floor up to the cursor's highest value
// when it starts, resumes exactly, and the next read reads the rows at that
// value again and the ones after.
func TestSourceReadsIncrementally(t *testing.T) {
	db := testDB(t)
	srcTable(t, db, "ST_INC", "(id NUMBER(10) PRIMARY KEY, at TIMESTAMP(6) WITH TIME ZONE, amount NUMBER(38), name NVARCHAR2(10), d DATE)",
		`INSERT INTO ST_INC VALUES (1, TIMESTAMP '2026-01-01 00:00:00 +00:00', 1, N'a', DATE '2026-01-01'),
		  (2, TIMESTAMP '2026-01-01 12:00:00 America/New_York', 99999999999999999999999999999999999999, N'é', DATE '2026-01-01'),
		  (3, NULL, 3, NULL, DATE '2026-01-02'), (4, TIMESTAMP '2026-01-01 10:00:00 +00:00', 3, N'b', DATE '2026-01-02')`)
	s := srcSource(t, testDSN(), `, "tables": ["ST_INC"], "chunkRows": 1`)
	for _, c := range []struct {
		cursor          string
		first, floor    []string
		newRow, nextRun string
	}{
		{"AT", []string{"1", "4", "2"}, []string{"2"}, "(5, TIMESTAMP '2026-01-02 00:00:00 +00:00', 0, N'0', DATE '2000-01-01')", "2 5"},
		{"AMOUNT", []string{"5", "1", "3", "4", "2"}, []string{"2"}, "(6, NULL, 100000000000000000000000000000000000000 - 1, NULL, NULL)", "2 6"},
		{"NAME", []string{"5", "1", "4", "2"}, []string{"2"}, "(7, NULL, NULL, N'ü', NULL)", "2 7"},
		{"D", []string{"5", "1", "2", "3", "4"}, []string{"3", "4"}, "(8, NULL, NULL, NULL, DATE '2026-01-03')", "3 4 8"},
	} {
		cs := source.ConfiguredStream{Name: "ST_INC", SyncMode: source.Incremental, Cursor: c.cursor}
		first := srcRead(t, s, 1, cs)
		if got := rowKeys(first.rows(), "ID"); !slices.Equal(got, c.first) {
			t.Fatalf("%s: read %v, want %v", c.cursor, got, c.first)
		}
		// Resumed after its first row, the rest.
		resumed := cs
		resumed.State = []byte(first.events[first.resumes()[0]].state)
		if got := rowKeys(srcRead(t, s, 1, resumed).rows(), "ID"); !slices.Equal(got, c.first[1:]) {
			t.Fatalf("%s: resumed at %s, read %v", c.cursor, resumed.State, got)
		}
		next := cs
		next.State = []byte(first.completed(t))
		if got := rowKeys(srcRead(t, s, 1, next).rows(), "ID"); !slices.Equal(got, c.floor) {
			t.Fatalf("%s: read again from %s: %v, want %v", c.cursor, next.State, got, c.floor)
		}
		exec(t, db, "INSERT INTO ST_INC VALUES "+c.newRow)
		if got := strings.Join(rowKeys(srcRead(t, s, 1, next).rows(), "ID"), " "); got != c.nextRun {
			t.Fatalf("%s: after an insert, read %s, want %s", c.cursor, got, c.nextRun)
		}
	}
	// A cursor of a type that can't order a read is refused.
	err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "ST_INC", SyncMode: source.Incremental, Cursor: "NOPE"}}}, &srcLog{})
	if code := cdcCode(err); code.String() != "ERROR_CODE_CONFIG_INVALID" {
		t.Errorf("a missing cursor: %v", err)
	}
}

// A table split by key ranges, of an integer key by statistics or a
// decimal one by a count, is read whole, each row once.
func TestSplitsReadEveryRowOnce(t *testing.T) {
	db := testDB(t)
	srcTable(t, db, "ST_SPLIT", "(id NUMBER(38) PRIMARY KEY, v NUMBER)",
		"INSERT INTO ST_SPLIT SELECT level * 7, level FROM dual CONNECT BY level <= 2000",
		"INSERT INTO ST_SPLIT VALUES (99999999999999999999999999999999999999, 0), (-99999999999999999999999999999999999999, 0)",
		"BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, 'ST_SPLIT'); END;")
	srcTable(t, db, "ST_SPLITD", "(id NUMBER(10, 2) PRIMARY KEY, v NUMBER)",
		"INSERT INTO ST_SPLITD SELECT level / 2 - 500.25, level FROM dual CONNECT BY level <= 2000")
	s := srcSource(t, testDSN(), `, "tables": ["ST_SPLIT", "ST_SPLITD"], "chunkRows": 100`)
	for name, n := range map[string]int{"ST_SPLIT": 2002, "ST_SPLITD": 2000} {
		cs := fullRefresh(name)
		splits, err := s.Splits(t.Context(), cs, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(splits) != min((n+99)/100, keysplit.MaxRanges) {
			t.Fatalf("%s: %d splits", name, len(splits))
		}
		cs.Splits = splits
		seen := map[string]int{}
		for _, r := range srcRead(t, s, 3, cs).rows() {
			seen[r["ID"]]++
		}
		if len(seen) != n {
			t.Fatalf("%s: %d keys", name, len(seen))
		}
		for k, c := range seen {
			if c != 1 {
				t.Fatalf("%s: key %s read %d times", name, k, c)
			}
		}
	}
	// Incremental splits share the cursor's highest value, and a cdc
	// stream is read whole.
	splits, err := s.Splits(t.Context(), source.ConfiguredStream{Name: "ST_SPLIT", SyncMode: source.Incremental, Cursor: "V"}, nil)
	if err != nil || len(splits) != 21 {
		t.Fatalf("incremental splits: %d, %v", len(splits), err)
	}
	var st sourceState
	if err := json.Unmarshal(splits[3].State, &st); err != nil || st.Until == nil || *st.Until != "2000" {
		t.Fatalf("an incremental split's state %s", splits[3].State)
	}
	if splits, err := s.Splits(t.Context(), source.ConfiguredStream{Name: "ST_SPLIT", SyncMode: source.CDC}, nil); err != nil || len(splits) != 1 || splits[0].ID != source.WholeStream {
		t.Fatalf("cdc splits %v, %v", splits, err)
	}
}

// Asked to split while it reads, a split divides what's left at its
// midpoint: it keeps the lower part, and the new split takes the upper.
func TestResplitDividesTheRemainder(t *testing.T) {
	db := testDB(t)
	srcTable(t, db, "ST_RESPLIT", "(id NUMBER(10) PRIMARY KEY)", "INSERT INTO ST_RESPLIT SELECT level FROM dual CONNECT BY level <= 100")
	s := srcSource(t, testDSN(), `, "tables": ["ST_RESPLIT"], "chunkRows": 10`)
	tables, schema, err := s.tables(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	out := &srcLog{}
	asked := false
	out.resplit = func() {
		if !asked {
			asked = true
			if ok, _ := s.Resplit(t.Context(), "ST_RESPLIT", "x"); !ok {
				t.Error("resplit refused")
			}
		}
	}
	if err := s.readSplit(t.Context(), &connPool{DB: db, Held: make(chan struct{}, 1)}, schema, tables, fullRefresh("ST_RESPLIT"), source.Split{ID: "x"}, nil, srcSplit{out}); err != nil {
		t.Fatal(err)
	}
	var added []source.Split
	for _, e := range out.events {
		added = append(added, e.added...)
	}
	if len(added) != 1 {
		t.Fatalf("added %v", added)
	}
	var st sourceState
	if err := json.Unmarshal(added[0].State, &st); err != nil {
		t.Fatal(err)
	}
	// The first chunk read 1..10, so the remainder 11..100 divides at 55.
	if st.Lo == nil || *st.Lo != "55" || st.Hi != nil || st.After != nil {
		t.Fatalf("upper split %s", added[0].State)
	}
	if rows := rowKeys(out.rows(), "ID"); len(rows) != 55 || rows[54] != "55" {
		t.Fatalf("the lower part read %d rows", len(rows))
	}
	if ok, _ := s.Resplit(t.Context(), "ST_RESPLIT", "x"); ok {
		t.Error("a split that has ended split")
	}
	// The upper split reads the rest.
	cs := fullRefresh("ST_RESPLIT")
	cs.Splits = added
	if rows := rowKeys(srcRead(t, s, 1, cs).rows(), "ID"); len(rows) != 45 || rows[0] != "56" {
		t.Fatalf("the upper part read %v", rows)
	}
}

// A split read again up to one of its resume points sends the rows it sent
// before that point, then a resume point and its completion there.
func TestSplitReadStopsAtUntil(t *testing.T) {
	db := testDB(t)
	srcTable(t, db, "ST_UNTIL", "(id NUMBER(10) PRIMARY KEY, v NUMBER, at DATE NOT NULL)",
		"INSERT INTO ST_UNTIL SELECT level, level, DATE '2026-01-01' + MOD(level, 17) / 24 FROM dual CONNECT BY level <= 3000",
		"BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, 'ST_UNTIL'); END;")
	srcTable(t, db, "ST_UNTIL_NOKEY", "(v NUMBER)", "INSERT INTO ST_UNTIL_NOKEY SELECT level FROM dual CONNECT BY level <= 30")
	s := srcSource(t, testDSN(), `, "tables": ["ST_UNTIL", "ST_UNTIL_NOKEY"], "chunkRows": 10`)
	for _, cs := range []source.ConfiguredStream{
		{Name: "ST_UNTIL", SyncMode: source.FullRefresh},
		{Name: "ST_UNTIL", SyncMode: source.Incremental, Cursor: "AT"},
	} {
		splits, err := s.Splits(t.Context(), cs, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(splits) < 2 {
			t.Fatalf("%d splits", len(splits))
		}
		sp := splits[1]
		cs.Splits = []source.Split{sp}
		first := srcRead(t, s, 1, cs)
		points := first.resumes()
		if len(points) < 6 {
			t.Fatalf("%v: %d resume points", cs.SyncMode, len(points))
		}
		for _, k := range []int{0, 2, len(points) - 1} {
			from := -1
			sp.State = splits[1].State
			if k > 0 {
				from = points[k-1]
				sp.State = []byte(first.events[from].state)
			}
			sp.Until = []byte(first.events[points[k]].state)
			cs.Splits = []source.Split{sp}
			again := srcRead(t, s, 1, cs)
			endsAt(t, again.events, string(sp.Until))
			if got, want := rowKeys(again.rows(), "ID"), rowKeys(srcRows(first.events, from, points[k]), "ID"); !slices.Equal(got, want) {
				t.Fatalf("%v, resume point %d: read again %v, not %v", cs.SyncMode, k+1, got, want)
			}
		}
	}
	// A table read in one query, up to its one resume point; or, from it, no
	// rows.
	cs := fullRefresh("ST_UNTIL_NOKEY")
	first := srcRead(t, s, 1, cs)
	until := first.events[first.resumes()[0]].state
	cs.Until = []byte(until)
	again := srcRead(t, s, 1, cs)
	endsAt(t, again.events, until)
	if len(again.rows()) != 30 {
		t.Fatalf("read again %d rows", len(again.rows()))
	}
	cs.State = []byte(until)
	again = srcRead(t, s, 1, cs)
	endsAt(t, again.events, until)
	if len(again.rows()) != 0 {
		t.Fatalf("read from its end, %d rows", len(again.rows()))
	}
}

// endsAt fails unless a read's last events are a resume point at until and
// its completion there, with no records after them.
func endsAt(t *testing.T, events []srcEvent, until string) {
	t.Helper()
	n := len(events)
	if n < 2 || !events[n-2].resume || events[n-2].state != until || !events[n-1].complete || events[n-1].state != until {
		t.Fatalf("a read until %s ended with %+v", until, events[max(n-2, 0):])
	}
}

// expectItems checks the items named in want, and logs every item, as it
// renders, when one isn't as wanted or with -v.
func expectItems(t *testing.T, what string, r check.Report, want map[string]string) {
	t.Helper()
	got := connectortest.Statuses(r)
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s: %s is %q, want %q", what, name, got[name], w)
		}
	}
	if len(got) != len(r.Items) {
		t.Errorf("%s: repeated names", what)
	}
	t.Logf("%s:", what)
	for _, it := range r.Items {
		it = it.Rendered()
		line := fmt.Sprintf("  %s %s: %s: %s", it.Status, it.Name, it.Title, it.Message)
		if it.Failure != nil && it.Failure.Fix != "" {
			line += " Fix: " + it.Failure.Fix
		}
		t.Log(line)
	}
}

// fixOf is an item's rendered fix.
func fixOf(r check.Report, name string) string {
	for _, it := range r.Items {
		if it.Name == name && it.Failure != nil {
			return it.Rendered().Failure.Fix
		}
	}
	return ""
}

var cdcPassed = map[string]string{
	"cdc.archivelog": "passed", "cdc.supplemental-logging": "passed", "cdc.privilege.logmining": "passed",
	"cdc.privilege.dbms-logmnr": "passed", "cdc.privilege.views": "passed", "cdc.logminer": "passed",
}

func with(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// The source's checks, item by item, for the connector's user, a user with
// only SELECT, users that can't sign in, and an address nothing listens at.
func TestSourceChecksItemByItem(t *testing.T) {
	db := testDB(t)
	srcTable(t, db, "ST_CHK", "(id NUMBER(10) PRIMARY KEY, v VARCHAR2(10))", "INSERT INTO ST_CHK VALUES (1, 'a')")

	s := srcSource(t, testDSN(), `, "tables": ["ST_CHK", "ST_NOPE"]`)
	expectItems(t, "the connector's user", check.Run(t.Context(), s.Checks()), with(cdcPassed, map[string]string{
		"net.dns": "passed", "net.connect": "passed", "db.connect": "passed", "schema.exists": "passed",
		"table.st-chk.read": "passed", "table.st-nope.read": "failed table_unreadable",
	}))
	if err := s.Check(t.Context()); err == nil {
		t.Error("Check passed with a table that isn't there")
	}

	expectItems(t, "a schema that isn't there", check.Run(t.Context(), srcSource(t, testDSN(), `, "schema": "ST_NO_SUCH_SCHEMA"`).Checks()), map[string]string{
		"schema.exists": "failed not_found", "tables.read": "skipped",
	})

	expectItems(t, "a bad password", check.Run(t.Context(), srcSource(t, badPasswordDSN(), "").Checks()), map[string]string{
		"net.connect": "passed", "db.connect": "failed credentials_rejected", "schema.exists": "skipped", "tables.read": "skipped",
		"cdc.archivelog": "skipped", "cdc.logminer": "skipped",
	})

	u, err := url.Parse(testDSN())
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/ST_NO_SUCH_SERVICE"
	r := check.Run(t.Context(), srcSource(t, u.String(), "").Checks())
	expectItems(t, "a service the listener doesn't know", r, map[string]string{"net.connect": "passed", "db.connect": "failed not_found"})
	if fix := fixOf(r, "db.connect"); !strings.Contains(fix, "lsnrctl services") {
		t.Errorf("an unknown service's fix: %s", fix)
	}

	start := time.Now()
	closed := check.Run(t.Context(), srcSource(t, closedDSN, "").Checks())
	expectItems(t, "a closed port", closed, map[string]string{
		"net.dns": "passed", "net.connect": "failed connection_refused", "db.connect": "skipped", "cdc.logminer": "skipped",
	})
	if time.Since(start) > 5*time.Second {
		t.Errorf("the checks of a closed port took %v", time.Since(start))
	}

	// SELECT on one table, and nothing of what cdc needs: cdc's items warn.
	lp := srcUser(t, "OPENETL_S_LP", "", "GRANT CREATE SESSION TO {user}", "GRANT SELECT ON OPENETL.ST_CHK TO {user}")
	r = check.Run(t.Context(), srcSource(t, lp, `, "schema": "OPENETL"`).Checks())
	expectItems(t, "a user with only SELECT", r, map[string]string{
		"db.connect": "passed", "schema.exists": "passed", "tables.read": "passed",
		"cdc.archivelog": "warning privilege_missing", "cdc.supplemental-logging": "warning privilege_missing",
		"cdc.privilege.logmining": "warning privilege_missing", "cdc.privilege.dbms-logmnr": "warning privilege_missing",
		"cdc.privilege.views": "warning privilege_missing", "cdc.logminer": "skipped",
	})
	if !r.OK() {
		t.Errorf("a user that can read its tables fails the check: %v", r.Err())
	}
	for name, fix := range map[string]string{
		"cdc.archivelog":            "GRANT SELECT ON SYS.V_$DATABASE TO OPENETL_S_LP;",
		"cdc.privilege.logmining":   "GRANT LOGMINING TO OPENETL_S_LP;",
		"cdc.privilege.dbms-logmnr": "GRANT EXECUTE ON SYS.DBMS_LOGMNR TO OPENETL_S_LP;",
		"cdc.privilege.views":       "GRANT SELECT ON SYS.V_$DATABASE TO OPENETL_S_LP; GRANT SELECT ON SYS.V_$LOG TO OPENETL_S_LP;",
	} {
		if got := fixOf(r, name); !strings.Contains(got, fix) {
			t.Errorf("%s's fix %q doesn't say %q", name, got, fix)
		}
	}
	// It reads the table.
	if rows := srcRead(t, srcSource(t, lp, `, "schema": "OPENETL", "tables": ["ST_CHK"]`), 1, fullRefresh("ST_CHK")).rows(); len(rows) != 1 {
		t.Errorf("the user read %d rows", len(rows))
	}
	// Its own schema has no tables.
	expectItems(t, "a schema without tables", check.Run(t.Context(), srcSource(t, lp, "").Checks()), map[string]string{
		"schema.exists": "passed", "tables.read": "warning no_tables",
	})

	locked := srcUser(t, "OPENETL_S_LOCK", "ACCOUNT LOCK", "GRANT CREATE SESSION TO {user}")
	r = check.Run(t.Context(), srcSource(t, locked, "").Checks())
	expectItems(t, "a locked account", r, map[string]string{"db.connect": "failed account_locked"})
	if fix := fixOf(r, "db.connect"); !strings.Contains(fix, "ALTER USER OPENETL_S_LOCK ACCOUNT UNLOCK") {
		t.Errorf("a locked account's fix: %s", fix)
	}
	expired := srcUser(t, "OPENETL_S_EXP", "PASSWORD EXPIRE", "GRANT CREATE SESSION TO {user}")
	expectItems(t, "an expired password", check.Run(t.Context(), srcSource(t, expired, "").Checks()), map[string]string{"db.connect": "failed credentials_expired"})
	nosession := srcUser(t, "OPENETL_S_NOSESS", "")
	expectItems(t, "a user without CREATE SESSION", check.Run(t.Context(), srcSource(t, nosession, "").Checks()), map[string]string{"db.connect": "failed privilege_missing"})
}

// Preflight says, stream by stream, whether the pipeline can read it as it
// says, and for cdc what the database, the user and each table need.
func TestSourcePreflight(t *testing.T) {
	db := testDB(t)
	srcTable(t, db, "ST_PF_ALL", "(id NUMBER(10) PRIMARY KEY, v VARCHAR2(10), f BINARY_DOUBLE)",
		"ALTER TABLE ST_PF_ALL ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS")
	srcTable(t, db, "ST_PF_NOLOG", "(id NUMBER(10) PRIMARY KEY)")
	srcTable(t, db, "ST_PF_NOKEY", "(v NUMBER)")
	s := srcSource(t, testDSN(), "")
	streams := []source.ConfiguredStream{
		{Name: "ST_PF_ALL", SyncMode: source.FullRefresh},
		{Name: "ST_PF_ALL", SyncMode: source.Incremental, Cursor: "V"},
		{Name: "ST_PF_ALL", SyncMode: source.Incremental, Cursor: "F"},
		{Name: "ST_PF_ALL", SyncMode: source.Incremental, Cursor: "NOPE"},
		{Name: "ST_PF_ALL", SyncMode: source.CDC},
		{Name: "ST_PF_NOLOG", SyncMode: source.CDC},
		{Name: "ST_PF_NOKEY", SyncMode: source.CDC},
		{Name: "ST_GONE", SyncMode: source.FullRefresh},
	}
	r := check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "p", Streams: streams}))
	expectItems(t, "the connector's user", r, with(cdcPassed, map[string]string{
		"db.connect": "passed", "cdc.retention": "passed",
		"stream.st-pf-all.read": "passed", "stream.st-pf-all.mode": "passed",
		"stream.st-pf-all.read-2": "passed", "stream.st-pf-all.mode-2": "passed",
		"stream.st-pf-all.read-3": "passed", "stream.st-pf-all.mode-3": "failed cursor_unsupported",
		"stream.st-pf-all.read-4": "passed", "stream.st-pf-all.mode-4": "failed cursor_missing",
		"stream.st-pf-all.read-5": "passed", "stream.st-pf-all.mode-5": "passed",
		"stream.st-pf-all.supplemental-logging": "passed", "stream.st-pf-all.flashback": "passed",
		"stream.st-pf-nolog.read": "passed", "stream.st-pf-nolog.mode": "passed",
		"stream.st-pf-nolog.supplemental-logging": "failed supplemental_logging_missing", "stream.st-pf-nolog.flashback": "passed",
		"stream.st-pf-nokey.read": "passed", "stream.st-pf-nokey.mode": "failed cdc_unsupported",
		"stream.st-pf-nokey.supplemental-logging": "skipped", "stream.st-pf-nokey.flashback": "passed",
		"stream.st-gone.read": "failed not_found", "stream.st-gone.mode": "skipped",
	}))
	if fix := fixOf(r, "stream.st-pf-nolog.supplemental-logging"); !strings.Contains(fix, `ALTER TABLE "OPENETL"."ST_PF_NOLOG" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;`) {
		t.Errorf("the supplemental logging's fix: %s", fix)
	}
	for _, it := range r.Items {
		if it.Name == "stream.st-pf-nokey.mode" && !strings.Contains(it.Rendered().Message, "no primary key") {
			t.Errorf("a table without a key: %s", it.Rendered().Message)
		}
	}

	// Without cdc, nothing of it.
	r = check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "p", Streams: streams[:2]}))
	if got := slices.Collect(func(yield func(string) bool) {
		for _, it := range r.Items {
			if !yield(it.Name) {
				return
			}
		}
	}); !slices.Equal(got, []string{"db.connect", "stream.st-pf-all.read", "stream.st-pf-all.mode", "stream.st-pf-all.read-2", "stream.st-pf-all.mode-2"}) || !r.OK() {
		t.Errorf("a preflight without cdc: %v, %v", got, r.Err())
	}

	// The retention item compares how far back the archived log on disk
	// reaches with twice the schedule's interval.
	var reach sql.NullFloat64
	if err := db.QueryRowContext(t.Context(), `SELECT (SYSDATE - min(first_time)) * 86400 FROM v$archived_log
	  WHERE status = 'A' AND deleted = 'NO' AND standby_dest = 'NO'`).Scan(&reach); err != nil {
		t.Fatal(err)
	}
	streams = streams[4:5]
	for _, interval := range []time.Duration{time.Second, 1000 * time.Hour} {
		want := "passed"
		switch {
		case !reach.Valid:
			want = "warning archived_log_missing"
		case reach.Float64 < 2*interval.Seconds():
			want = "warning retention_short"
		}
		expectItems(t, "a schedule of "+interval.String(), check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "p", Streams: streams, Interval: interval})),
			map[string]string{"cdc.retention": want})
	}

	// The least a user needs for cdc, but FLASHBACK on another schema's table.
	lp := srcUser(t, "OPENETL_S_LP", "", "GRANT CREATE SESSION TO {user}", "GRANT SELECT ON OPENETL.ST_PF_ALL TO {user}",
		"GRANT LOGMINING TO {user}", "GRANT EXECUTE ON SYS.DBMS_LOGMNR TO {user}",
		"GRANT SELECT ON SYS.V_$DATABASE TO {user}", "GRANT SELECT ON SYS.V_$LOG TO {user}", "GRANT SELECT ON SYS.V_$LOGFILE TO {user}",
		"GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO {user}",
		"GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO {user}", "GRANT SELECT ON SYS.V_$TRANSACTION TO {user}")
	ls := srcSource(t, lp, `, "schema": "OPENETL"`)
	r = check.Run(t.Context(), ls.Preflight(source.PreflightRequest{Pipeline: "p", Streams: streams}))
	expectItems(t, "a user without FLASHBACK", r, with(cdcPassed, map[string]string{
		"db.connect": "passed", "stream.st-pf-all.read": "passed", "stream.st-pf-all.mode": "passed",
		"stream.st-pf-all.supplemental-logging": "passed", "stream.st-pf-all.flashback": "failed privilege_missing",
	}))
	if fix := fixOf(r, "stream.st-pf-all.flashback"); !strings.Contains(fix, `GRANT FLASHBACK ON "OPENETL"."ST_PF_ALL" TO OPENETL_S_LP;`) {
		t.Errorf("the flashback's fix: %s", fix)
	}
	exec(t, adminDB(t), "GRANT FLASHBACK ON OPENETL.ST_PF_ALL TO OPENETL_S_LP")
	expectItems(t, "a user with FLASHBACK", check.Run(t.Context(), ls.Preflight(source.PreflightRequest{Pipeline: "p", Streams: streams})),
		map[string]string{"stream.st-pf-all.flashback": "passed"})
}

// readChunk reads a table's rows in key order, a chunk at a time, after a
// key and as of an SCN.
func TestReadChunk(t *testing.T) {
	db := testDB(t)
	srcTable(t, db, "ST_RC", "(a NUMBER(10), b VARCHAR2(5), PRIMARY KEY (a, b))",
		"INSERT INTO ST_RC VALUES (1, 'x'), (1, 'y'), (2, 'x'), (3, 'z')")
	tables, err := describe(t.Context(), db, "OPENETL", []string{"ST_RC"})
	if err != nil {
		t.Fatal(err)
	}
	// A new table's definition is newer than SCNs a few seconds old.
	time.Sleep(6 * time.Second)
	scn, err := currentSCN(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, db, "INSERT INTO ST_RC VALUES (1, 'z')")
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	chunk := func(asOf uint64, after []string, n int) (got []string, last []string) {
		t.Helper()
		last, err := readChunk(t.Context(), conn, "OPENETL", tables[0], asOf, after, n, func(vals []any) error {
			got = append(got, fmt.Sprintf("%v %v", vals[0], vals[1]))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return got, last
	}
	if got, last := chunk(0, nil, 2); !slices.Equal(got, []string{"1 x", "1 y"}) || !slices.Equal(last, []string{"1", "y"}) {
		t.Errorf("first chunk %v, last %v", got, last)
	}
	if got, last := chunk(0, []string{"1", "y"}, 2); !slices.Equal(got, []string{"1 z", "2 x"}) || !slices.Equal(last, []string{"2", "x"}) {
		t.Errorf("second chunk %v, last %v", got, last)
	}
	if got, _ := chunk(scn, []string{"1", "y"}, 10); !slices.Equal(got, []string{"2 x", "3 z"}) {
		t.Errorf("as of SCN %d: %v", scn, got)
	}
	if got, last := chunk(0, []string{"3", "z"}, 10); got != nil || last != nil {
		t.Errorf("past the end %v, last %v", got, last)
	}
}
