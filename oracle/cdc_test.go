//go:build integration

package oracle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// cdcOut keeps what a read emits: each record as text by column, and the
// states.
type cdcOut struct {
	mu       sync.Mutex
	rows     map[string][]map[string]string
	resumed  map[string][]byte
	complete map[string][]byte
	// "rows N", "resume STATE" and "complete STATE", in order, by stream.
	log map[string][]string
}

func newCDCOut() *cdcOut {
	return &cdcOut{rows: map[string][]map[string]string{}, resumed: map[string][]byte{}, complete: map[string][]byte{}, log: map[string][]string{}}
}

func (o *cdcOut) Rows(string, []json.RawMessage) error { return errors.New("unexpected raw rows") }

func (o *cdcOut) Records(stream string, b arrow.RecordBatch) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer b.Release()
	for i := range int(b.NumRows()) {
		row := map[string]string{}
		for j, f := range b.Schema().Fields() {
			c := b.Column(j)
			if c.IsNull(i) {
				row[f.Name] = "NULL"
			} else {
				row[f.Name] = c.ValueStr(i)
			}
		}
		o.rows[stream] = append(o.rows[stream], row)
	}
	o.log[stream] = append(o.log[stream], fmt.Sprintf("rows %d", b.NumRows()))
	return nil
}

func (o *cdcOut) ResumePoint(stream string, state []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.resumed[stream] = state
	o.log[stream] = append(o.log[stream], "resume "+string(state))
	return nil
}

func (o *cdcOut) Complete(stream string, next []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.complete[stream] = next
	o.log[stream] = append(o.log[stream], "complete "+string(next))
	return nil
}

func (o *cdcOut) Split(stream, _ string) source.SplitEmitter { return cdcSplit{o, stream} }

type cdcSplit struct {
	o      *cdcOut
	stream string
}

func (s cdcSplit) Rows([]json.RawMessage) error      { return errors.New("unexpected raw rows") }
func (s cdcSplit) Records(b arrow.RecordBatch) error { return s.o.Records(s.stream, b) }
func (s cdcSplit) ResumePoint(b []byte, _ ...source.Split) error {
	return s.o.ResumePoint(s.stream, b)
}
func (s cdcSplit) Complete(b []byte, _ ...source.Split) error { return s.o.Complete(s.stream, b) }

// state is the last state the read gave the stream.
func (o *cdcOut) state(stream string) []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	if b, ok := o.complete[stream]; ok {
		return b
	}
	return o.resumed[stream]
}

func (o *cdcOut) records(stream string) []map[string]string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.rows[stream])
}

func cdcSource(t *testing.T, extra string) *Source {
	t.Helper()
	s, err := NewSource([]byte(`{"dsn": "` + testDSN() + `", "snapshotChunkRows": 2` + extra + `}`))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// readCDCOnce reads the streams with cdc once, as a scheduled sync does.
func readCDCOnce(t *testing.T, s *Source, states map[string][]byte, names ...string) (*cdcOut, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	req := source.ReadRequest{Lanes: 1}
	for _, n := range names {
		req.Streams = append(req.Streams, source.ConfiguredStream{Name: n, SyncMode: source.CDC, State: states[n]})
	}
	out := newCDCOut()
	return out, s.Read(ctx, req, out)
}

func mustRead(t *testing.T, s *Source, states map[string][]byte, names ...string) *cdcOut {
	t.Helper()
	out, err := readCDCOnce(t, s, states, names...)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if out.complete[n] == nil {
			t.Fatalf("%s didn't complete: %v", n, out.log[n])
		}
	}
	return out
}

// cdcChanges spells each record by the given columns, with its op.
func cdcChanges(rows []map[string]string, cols ...string) []string {
	var out []string
	for _, r := range rows {
		parts := []string{r["openetl.op"]}
		for _, c := range cols {
			parts = append(parts, r[c])
		}
		out = append(out, strings.Join(parts, " "))
	}
	return out
}

func cdcCode(err error) protocolv1.ErrorCode {
	var c interface{ ErrorCode() protocolv1.ErrorCode }
	if errors.As(err, &c) {
		return c.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

func cdcTable(t *testing.T, db *sql.DB, name, def string) {
	t.Helper()
	dropTable(t, db, name)
	exec(t, db, `CREATE TABLE `+quoteIdent(name)+` `+def, `ALTER TABLE `+quoteIdent(name)+` ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`)
	t.Cleanup(func() { dropTable(t, db, name) })
}

func TestCDCSnapshotThenChanges(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_BASIC", `(id NUMBER(10) PRIMARY KEY, name VARCHAR2(20), amount NUMBER(10, 2))`)
	exec(t, db, `INSERT INTO cdc_basic VALUES (1, 'one', 1.5)`, `INSERT INTO cdc_basic VALUES (2, 'two', NULL)`, `INSERT INTO cdc_basic VALUES (3, 'three', 3)`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_BASIC")
	if got, want := cdcChanges(first.records("CDC_BASIC"), "ID", "NAME", "AMOUNT"), []string{"insert 1 one 1.5", "insert 2 two NULL", "insert 3 three 3"}; !slices.Equal(got, want) {
		t.Fatalf("snapshot: got %q, want %q", got, want)
	}
	exec(t, db,
		`INSERT INTO cdc_basic VALUES (4, 'four', 4)`,
		`UPDATE cdc_basic SET name = 'uno' WHERE id = 1`,
		`DELETE FROM cdc_basic WHERE id = 2`)
	// A savepoint rolled back, and a transaction rolled back.
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`INSERT INTO cdc_basic VALUES (5, 'five', 5)`, `SAVEPOINT sp`, `UPDATE cdc_basic SET name = 'cinco' WHERE id = 5`, `INSERT INTO cdc_basic VALUES (6, 'six', 6)`, `ROLLBACK TO SAVEPOINT sp`} {
		if _, err := tx.Exec(q); err != nil {
			t.Fatal(q, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO cdc_basic VALUES (7, 'seven', 7)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	second := mustRead(t, s, map[string][]byte{"CDC_BASIC": first.state("CDC_BASIC")}, "CDC_BASIC")
	rows := second.records("CDC_BASIC")
	if got, want := cdcChanges(rows, "ID", "NAME", "AMOUNT"), []string{"insert 4 four 4", "update 1 uno 1.5", "delete 2 two NULL", "insert 5 five 5"}; !slices.Equal(got, want) {
		t.Fatalf("changes: got %q, want %q", got, want)
	}
	for i := 1; i < len(rows); i++ {
		a, _ := new(big.Int).SetString(rows[i-1]["openetl.seq"], 10)
		b, _ := new(big.Int).SetString(rows[i]["openetl.seq"], 10)
		if a == nil || b == nil || b.Cmp(a) <= 0 {
			t.Errorf("seq %s after %s", rows[i]["openetl.seq"], rows[i-1]["openetl.seq"])
		}
	}
	third := mustRead(t, s, map[string][]byte{"CDC_BASIC": second.state("CDC_BASIC")}, "CDC_BASIC")
	if rows := third.records("CDC_BASIC"); len(rows) != 0 {
		t.Errorf("a read from the last state read again: %v", rows)
	}
}

// A transaction open when the snapshot is read, which commits after it,
// lands once: its changes before the snapshot aren't in it, and the redo
// from before the snapshot's SCN is read.
func TestCDCOpenTransactionAcrossSnapshot(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_LONG", `(id NUMBER(10) PRIMARY KEY, v VARCHAR2(20))`)
	exec(t, db, `INSERT INTO cdc_long VALUES (1, 'committed')`)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`INSERT INTO cdc_long VALUES (2, 'open')`); err != nil {
		t.Fatal(err)
	}
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_LONG")
	if got, want := cdcChanges(first.records("CDC_LONG"), "ID", "V"), []string{"insert 1 committed"}; !slices.Equal(got, want) {
		t.Fatalf("snapshot: got %q, want %q", got, want)
	}
	// Past a few windows.
	time.Sleep(3 * time.Second)
	if _, err := tx.Exec(`INSERT INTO cdc_long VALUES (3, 'later')`); err != nil {
		t.Fatal(err)
	}
	mid := mustRead(t, s, map[string][]byte{"CDC_LONG": first.state("CDC_LONG")}, "CDC_LONG")
	if rows := mid.records("CDC_LONG"); len(rows) != 0 {
		t.Fatalf("an open transaction's changes were read: %v", rows)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	second := mustRead(t, s, map[string][]byte{"CDC_LONG": mid.state("CDC_LONG")}, "CDC_LONG")
	if got, want := cdcChanges(second.records("CDC_LONG"), "ID", "V"), []string{"insert 2 open", "insert 3 later"}; !slices.Equal(got, want) {
		t.Fatalf("changes: got %q, want %q", got, want)
	}
}

// Each type's value read from the redo log is the value a full refresh
// reads.
func TestCDCTypesMatchFullRefresh(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_TYPES", `(id NUMBER(10) PRIMARY KEY,
	  n NUMBER, n5 NUMBER(5), n18 NUMBER(18), n38 NUMBER(38), nd NUMBER(12, 4), nneg NUMBER(5, -2), f FLOAT, bf BINARY_FLOAT, bd BINARY_DOUBLE,
	  vc VARCHAR2(100), nvc NVARCHAR2(50), ch CHAR(5), nch NCHAR(3), cl CLOB, ncl NCLOB, bl BLOB, rw RAW(16),
	  d DATE, ts TIMESTAMP, ts9 TIMESTAMP(9), tstz TIMESTAMP WITH TIME ZONE, tsltz TIMESTAMP WITH LOCAL TIME ZONE,
	  iym INTERVAL YEAR TO MONTH, ids INTERVAL DAY TO SECOND, rid ROWID, x XMLTYPE)`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_TYPES")
	exec(t, db,
		`INSERT INTO cdc_types (id) VALUES (1)`,
		`INSERT INTO cdc_types VALUES (2,
		  -1.5e-130, -12345, 999999999999999999, 99999999999999999999999999999999999999, -12345678.1234, 1234500, 3.14159, -1.25, 2.5e300d,
		  'it''s'||chr(10)||'multi', '日本ü', 'ab', 'ñ', rpad('c', 5000, 'x'), 'ncl日', HEXTORAW('00ff'), HEXTORAW('deadbeef'),
		  TO_DATE('-4712-01-01 13:14:15', 'SYYYY-MM-DD HH24:MI:SS'), TIMESTAMP '2026-11-03 10:00:00.123456', TIMESTAMP '2026-11-03 10:00:00.123456789',
		  TIMESTAMP '2026-11-03 10:00:00.5 Europe/Paris', TIMESTAMP '2026-11-03 10:00:00.5 +05:30',
		  INTERVAL '-3-4' YEAR TO MONTH, INTERVAL '3 04:05:06.789' DAY TO SECOND, 'AAAR3HAATAAAAPrAAA', XMLType('<a>1</a>'))`,
		`INSERT INTO cdc_types (id, bf, bd, cl, bl) VALUES (3, BINARY_FLOAT_NAN, -BINARY_DOUBLE_INFINITY, EMPTY_CLOB(), EMPTY_BLOB())`,
		`UPDATE cdc_types SET n5 = 7, vc = '' WHERE id = 2`,
		`UPDATE cdc_types SET x = XMLType('<b>2</b>') WHERE id = 2`,
		`DECLARE c CLOB; BEGIN SELECT cl INTO c FROM cdc_types WHERE id = 3 FOR UPDATE; FOR i IN 1 .. 30 LOOP DBMS_LOB.APPEND(c, rpad('y', 4000, 'z')); END LOOP; END;`)
	cdc := mustRead(t, s, map[string][]byte{"CDC_TYPES": first.state("CDC_TYPES")}, "CDC_TYPES")
	full := newCDCOut()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := s.Read(ctx, source.ReadRequest{Lanes: 1, Streams: []source.ConfiguredStream{{Name: "CDC_TYPES", SyncMode: source.FullRefresh}}}, full); err != nil {
		t.Fatal(err)
	}
	last := map[string]map[string]string{}
	for _, r := range cdc.records("CDC_TYPES") {
		last[r["ID"]] = r
	}
	want := map[string]map[string]string{}
	for _, r := range full.records("CDC_TYPES") {
		want[r["ID"]] = r
	}
	if len(last) != 3 || len(want) != 3 {
		t.Fatalf("cdc read %d rows, full refresh %d", len(last), len(want))
	}
	for id, w := range want {
		g := last[id]
		for _, c := range slices.Sorted(maps.Keys(w)) {
			if g[c] != w[c] {
				t.Errorf("row %s column %s: cdc %.100q, full refresh %.100q", id, c, g[c], w[c])
			}
		}
	}
}

// A change made before a column is added is read with the columns as they
// are now; one made after, with the new column. A read that sees a table's
// columns change stops there, and the next reads on.
func TestCDCColumnAdded(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_DDL", `(id NUMBER(10) PRIMARY KEY, v VARCHAR2(20))`)
	exec(t, db, `INSERT INTO cdc_ddl VALUES (1, 'a')`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_DDL")
	exec(t, db, `INSERT INTO cdc_ddl VALUES (2, 'b')`, `ALTER TABLE cdc_ddl ADD (w NUMBER(5))`, `INSERT INTO cdc_ddl VALUES (3, 'c', 30)`)
	second := mustRead(t, s, map[string][]byte{"CDC_DDL": first.state("CDC_DDL")}, "CDC_DDL")
	if got, want := cdcChanges(second.records("CDC_DDL"), "ID", "V", "W"), []string{"insert 2 b NULL", "insert 3 c 30"}; !slices.Equal(got, want) {
		t.Fatalf("changes: got %q, want %q", got, want)
	}

	// While reading continuously.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out := newCDCOut()
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Continuous: true, Lanes: 2, Streams: []source.ConfiguredStream{{Name: "CDC_DDL", SyncMode: source.CDC, State: second.state("CDC_DDL")}}}, out)
	}()
	time.Sleep(3 * time.Second)
	exec(t, db, `INSERT INTO cdc_ddl VALUES (4, 'd', 40)`, `ALTER TABLE cdc_ddl ADD (x VARCHAR2(5))`, `INSERT INTO cdc_ddl VALUES (5, 'e', 50, 'x')`)
	var err error
	select {
	case err = <-done:
	case <-time.After(45 * time.Second):
		t.Fatal("the read didn't stop at the column change")
	}
	if cdcCode(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Fatalf("got %v, want a TRANSIENT error", err)
	}
	if got, want := cdcChanges(out.records("CDC_DDL"), "ID", "V", "W"), []string{"insert 4 d 40"}; !slices.Equal(got, want) {
		t.Fatalf("before the change: got %q, want %q", got, want)
	}
	third := mustRead(t, s, map[string][]byte{"CDC_DDL": out.state("CDC_DDL")}, "CDC_DDL")
	if got, want := cdcChanges(third.records("CDC_DDL"), "ID", "V", "W", "X"), []string{"insert 5 e 50 x"}; !slices.Equal(got, want) {
		t.Fatalf("after the change: got %q, want %q", got, want)
	}
}

// An update that changes a row's key deletes the row under its old key,
// then writes it under the new, both with the update's sequence number.
func TestCDCKeyChanged(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_REKEY", `(id NUMBER(10) PRIMARY KEY, v VARCHAR2(20))`)
	exec(t, db, `INSERT INTO cdc_rekey VALUES (1, 'a')`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_REKEY")
	exec(t, db, `UPDATE cdc_rekey SET id = 10 WHERE id = 1`, `UPDATE cdc_rekey SET v = 'b' WHERE id = 10`)
	rows := mustRead(t, s, map[string][]byte{"CDC_REKEY": first.state("CDC_REKEY")}, "CDC_REKEY").records("CDC_REKEY")
	if got, want := cdcChanges(rows, "ID", "V"), []string{"delete 1 a", "update 10 a", "update 10 b"}; !slices.Equal(got, want) {
		t.Fatalf("changes: got %q, want %q", got, want)
	}
	if rows[0]["openetl.seq"] != rows[1]["openetl.seq"] {
		t.Errorf("the old key's delete has seq %s, the update %s", rows[0]["openetl.seq"], rows[1]["openetl.seq"])
	}
}

// A column the run didn't declare, such as one added since Discover, fails
// the read retryably, so the next attempt declares it, rather than leave
// its values out.
func TestCDCUndeclaredColumn(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_UCOL", `(id NUMBER(10) PRIMARY KEY, v VARCHAR2(20))`)
	exec(t, db, `INSERT INTO cdc_ucol VALUES (1, 'a')`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_UCOL")
	exec(t, db, `ALTER TABLE cdc_ucol ADD (cl CLOB)`, `UPDATE cdc_ucol SET v = 'b', cl = 'lob' WHERE id = 1`)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cs := source.ConfiguredStream{Name: "CDC_UCOL", SyncMode: source.CDC, State: first.state("CDC_UCOL"), Columns: []string{"ID", "V"}}
	err := s.Read(ctx, source.ReadRequest{Lanes: 1, Streams: []source.ConfiguredStream{cs}}, newCDCOut())
	if cdcCode(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT || !strings.Contains(err.Error(), `"CL"`) {
		t.Errorf("a column added since Discover: got %v, want TRANSIENT naming it", err)
	}
}

// A change made before a column is dropped can't be read: the read fails
// rather than reading it with the wrong columns.
func TestCDCColumnDropped(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_DROP", `(id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), b NUMBER(5))`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_DROP")
	exec(t, db, `INSERT INTO cdc_drop VALUES (1, 'a', 1)`, `ALTER TABLE cdc_drop DROP COLUMN a`)
	_, err := readCDCOnce(t, s, map[string][]byte{"CDC_DROP": first.state("CDC_DROP")}, "CDC_DROP")
	if cdcCode(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Fatalf("got %v, want POSITION_LOST", err)
	}
}

func TestCDCPositionLost(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_LOST", `(id NUMBER(10) PRIMARY KEY)`)
	// A truncate of a table that has never had rows, with no segment, isn't
	// in the redo log, and deletes nothing.
	exec(t, db, `INSERT INTO cdc_lost VALUES (1)`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_LOST")
	var st cdcState
	if err := json.Unmarshal(first.state("CDC_LOST"), &st); err != nil {
		t.Fatal(err)
	}
	gone := st
	gone.SCN, gone.From = 1, 1
	b, _ := json.Marshal(gone)
	if _, err := readCDCOnce(t, s, map[string][]byte{"CDC_LOST": b}, "CDC_LOST"); cdcCode(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST || !strings.Contains(err.Error(), "SCN 1") {
		t.Errorf("a position before the oldest redo: got %v, want POSITION_LOST", err)
	}
	other := st
	other.DB = "1:1"
	b, _ = json.Marshal(other)
	if _, err := readCDCOnce(t, s, map[string][]byte{"CDC_LOST": b}, "CDC_LOST"); cdcCode(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("another database's position: got %v, want POSITION_LOST", err)
	}
	exec(t, db, `TRUNCATE TABLE cdc_lost`)
	if _, err := readCDCOnce(t, s, map[string][]byte{"CDC_LOST": first.state("CDC_LOST")}, "CDC_LOST"); cdcCode(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a truncated table: got %v, want POSITION_LOST", err)
	}
}

// Truncating a partition deletes rows the redo log doesn't name: the read
// fails as lost rather than keep them.
func TestCDCPartitionTruncated(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_PART", `(id NUMBER(10) PRIMARY KEY, v VARCHAR2(10)) PARTITION BY RANGE (id) (PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	exec(t, db, `INSERT INTO cdc_part VALUES (1, 'a')`, `INSERT INTO cdc_part VALUES (20, 'b')`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_PART")
	exec(t, db, `INSERT INTO cdc_part VALUES (2, 'c')`, `ALTER TABLE cdc_part TRUNCATE PARTITION p1`)
	if _, err := readCDCOnce(t, s, map[string][]byte{"CDC_PART": first.state("CDC_PART")}, "CDC_PART"); cdcCode(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a truncated partition: got %v, want POSITION_LOST", err)
	}
}

// Exchanging another table's partition with a table read swaps the table's
// rows, which the redo log doesn't name either.
func TestCDCPartitionExchangedIn(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_XIN", `(id NUMBER(10), v VARCHAR2(10), CONSTRAINT cdc_xin_pk PRIMARY KEY (id))`)
	dropTable(t, db, "CDC_XOTHER")
	exec(t, db, `CREATE TABLE cdc_xother (id NUMBER(10), v VARCHAR2(10), CONSTRAINT cdc_xother_pk PRIMARY KEY (id) USING INDEX LOCAL)
	  PARTITION BY RANGE (id) (PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	t.Cleanup(func() { dropTable(t, db, "CDC_XOTHER") })
	exec(t, db, `INSERT INTO cdc_xin VALUES (1, 'a')`, `INSERT INTO cdc_xother VALUES (50, 'other')`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_XIN")
	exec(t, db, `INSERT INTO cdc_xin VALUES (2, 'b')`, `ALTER TABLE cdc_xother EXCHANGE PARTITION p1 WITH TABLE cdc_xin INCLUDING INDEXES`)
	if _, err := readCDCOnce(t, s, map[string][]byte{"CDC_XIN": first.state("CDC_XIN")}, "CDC_XIN"); cdcCode(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a partition exchanged in: got %v, want POSITION_LOST", err)
	}
}

func TestCDCNeedsSupplementalLogging(t *testing.T) {
	db := testDB(t)
	dropTable(t, db, "CDC_NOSUP")
	exec(t, db, `CREATE TABLE cdc_nosup (id NUMBER(10) PRIMARY KEY)`)
	t.Cleanup(func() { dropTable(t, db, "CDC_NOSUP") })
	_, err := readCDCOnce(t, cdcSource(t, ""), nil, "CDC_NOSUP")
	if cdcCode(err) != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID || !strings.Contains(err.Error(), "ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS") {
		t.Errorf("got %v, want CONFIG_INVALID with the fix", err)
	}
}

// A read again from a resume point until a later one sends the changes
// between them as the first read did, and nothing past.
func TestCDCUntil(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_UNTIL", `(id NUMBER(10) PRIMARY KEY, v VARCHAR2(20))`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_UNTIL")
	from := first.state("CDC_UNTIL")
	exec(t, db, `INSERT INTO cdc_until VALUES (1, 'a')`, `UPDATE cdc_until SET v = 'b' WHERE id = 1`)
	second := mustRead(t, s, map[string][]byte{"CDC_UNTIL": from}, "CDC_UNTIL")
	until := second.state("CDC_UNTIL")
	exec(t, db, `INSERT INTO cdc_until VALUES (2, 'past')`)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	again := newCDCOut()
	if err := s.Read(ctx, source.ReadRequest{Lanes: 1, Streams: []source.ConfiguredStream{{Name: "CDC_UNTIL", SyncMode: source.CDC, State: from, Until: until}}}, again); err != nil {
		t.Fatal(err)
	}
	if got, want := again.records("CDC_UNTIL"), second.records("CDC_UNTIL"); !slices.EqualFunc(got, want, maps.Equal) {
		t.Errorf("read again: got %v, want %v", got, want)
	}
	log := again.log["CDC_UNTIL"]
	if n := len(log); n < 2 || log[n-2] != "resume "+string(until) || log[n-1] != "complete "+string(until) {
		t.Errorf("read again didn't end at until: %q", log)
	}
}

// A read again from no state, as of a new stream's first part, up to a
// resume point its first read sent, sends what that read sent before it:
// the snapshot as of the same SCN, then the changes after it, with the same
// sequence numbers, though the table has changed since.
func TestCDCUntilFromNoState(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_AGAIN", `(id NUMBER(10) PRIMARY KEY, v VARCHAR2(20))`)
	exec(t, db, `INSERT INTO cdc_again VALUES (10, 'a')`, `INSERT INTO cdc_again VALUES (20, 'b')`, `INSERT INTO cdc_again VALUES (30, 'c')`)
	s := cdcSource(t, "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	first := newCDCOut()
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Continuous: true, Lanes: 1, Streams: []source.ConfiguredStream{{Name: "CDC_AGAIN", SyncMode: source.CDC}}}, first)
	}()
	// Until the first read has sent n rows, and a resume point after them.
	sent := func(n int) {
		t.Helper()
		for deadline := time.Now().Add(time.Minute); ; time.Sleep(200 * time.Millisecond) {
			first.mu.Lock()
			log, rows := slices.Clone(first.log["CDC_AGAIN"]), len(first.rows["CDC_AGAIN"])
			first.mu.Unlock()
			if rows >= n && len(log) > 0 && strings.HasPrefix(log[len(log)-1], "resume ") {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("the first read sent %q", log)
			}
		}
	}
	sent(3)
	exec(t, db, `INSERT INTO cdc_again VALUES (15, 'd')`, `UPDATE cdc_again SET v = 'e' WHERE id = 10`)
	sent(5)
	cancel()
	<-done
	exec(t, db, `INSERT INTO cdc_again VALUES (5, 'past')`, `INSERT INTO cdc_again VALUES (25, 'past')`, `UPDATE cdc_again SET v = 'past' WHERE id = 30`, `DELETE FROM cdc_again WHERE id = 20`)
	rows, n := first.records("CDC_AGAIN"), 0
	for _, e := range first.log["CDC_AGAIN"] {
		if k, ok := strings.CutPrefix(e, "rows "); ok {
			c, _ := strconv.Atoi(k)
			n += c
			continue
		}
		until, ok := strings.CutPrefix(e, "resume ")
		if !ok {
			continue
		}
		again := newCDCOut()
		if err := s.Read(t.Context(), source.ReadRequest{Lanes: 1, Streams: []source.ConfiguredStream{{Name: "CDC_AGAIN", SyncMode: source.CDC, Until: []byte(until)}}}, again); err != nil {
			t.Fatalf("until %s: %v", until, err)
		}
		if got, want := again.records("CDC_AGAIN"), rows[:n]; !slices.EqualFunc(got, want, maps.Equal) {
			t.Errorf("until %s: read again %v, want %v", until, got, want)
		}
		if log := again.log["CDC_AGAIN"]; len(log) < 2 || log[len(log)-2] != e || log[len(log)-1] != "complete "+until {
			t.Errorf("until %s: read again didn't end there: %q", until, log)
		}
	}
	if n != 5 {
		t.Fatalf("the first read sent %d rows before its last resume point: %q", n, first.log["CDC_AGAIN"])
	}
}

func TestCDCRetention(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_RET", `(id NUMBER(10) PRIMARY KEY)`)
	s := cdcSource(t, "")
	state := mustRead(t, s, nil, "CDC_RET").state("CDC_RET")
	ctx := context.Background()
	ps, err := s.Retention(ctx, "p", map[string][]byte{"CDC_RET": state}, nil)
	if err != nil || len(ps) != 1 || ps[0].LagBytes == nil || ps[0].RetentionLeft != nil {
		t.Fatalf("a position on disk: got %+v, %v", ps, err)
	}
	var st cdcState
	if err := json.Unmarshal(state, &st); err != nil {
		t.Fatal(err)
	}
	st.From = 1
	gone, _ := json.Marshal(st)
	ps, err = s.Retention(ctx, "p", map[string][]byte{"CDC_RET": gone}, nil)
	if err != nil || len(ps) != 1 || ps[0].RetentionLeft == nil || *ps[0].RetentionLeft != 0 {
		t.Fatalf("a position gone: got %+v, %v", ps, err)
	}
	if ps, err := s.Retention(ctx, "p", map[string][]byte{"CDC_RET": []byte(`{"scn":5,"from":5,"db":"1:1"}`)}, nil); err != nil || ps != nil {
		t.Errorf("another database's position: got %+v, %v", ps, err)
	}
	// Another database's earlier position doesn't hide this one's.
	ps, err = s.Retention(ctx, "p", map[string][]byte{"CDC_RET": state, "OTHER": []byte(`{"scn":5,"from":5,"db":"1:1"}`)}, nil)
	if err != nil || len(ps) != 1 || ps[0].LagBytes == nil || ps[0].RetentionLeft != nil {
		t.Errorf("a position beside another database's: got %+v, %v", ps, err)
	}
}

// Commits whose redo LGWR writes when it likes, made while a read runs,
// are each sent once.
func TestCDCNoWaitCommits(t *testing.T) {
	db := testDB(t)
	cdcTable(t, db, "CDC_NOWAIT", `(id NUMBER(10) PRIMARY KEY)`)
	s := cdcSource(t, "")
	first := mustRead(t, s, nil, "CDC_NOWAIT")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out := newCDCOut()
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Continuous: true, Lanes: 1, Streams: []source.ConfiguredStream{{Name: "CDC_NOWAIT", SyncMode: source.CDC, State: first.state("CDC_NOWAIT")}}}, out)
	}()
	exec(t, db, `BEGIN
	  FOR i IN 1..200 LOOP
	    INSERT INTO cdc_nowait VALUES (i);
	    COMMIT WRITE BATCH NOWAIT;
	    IF MOD(i, 20) = 0 THEN DBMS_SESSION.SLEEP(0.3); END IF;
	  END LOOP;
	END;`)
	for deadline := time.Now().Add(30 * time.Second); len(out.records("CDC_NOWAIT")) < 200 && time.Now().Before(deadline); {
		time.Sleep(200 * time.Millisecond)
	}
	cancel()
	<-done
	var got []string
	for _, row := range out.records("CDC_NOWAIT") {
		got = append(got, row["ID"])
	}
	want := make([]string, 200)
	for i := range want {
		want[i] = fmt.Sprint(i + 1)
	}
	if !slices.Equal(got, want) {
		t.Errorf("sent %d rows, want 1 to 200 once each, in order: %v", len(got), got)
	}
}
