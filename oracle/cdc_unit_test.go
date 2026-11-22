package oracle

import (
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl-connectors/connectortest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// cdcReaderOf is a reader of table T, (ID NUMBER(10) PRIMARY KEY, V
// VARCHAR2(20)), whose state is at SCN scn.
func cdcReaderOf(t *testing.T, scn uint64) (*cdcReader, *cdcStream, *connectortest.Recorder) {
	t.Helper()
	cols := []column{
		{name: "ID", dataType: "NUMBER", precision: 10, scale: 0, notNull: true, seg: 1},
		{name: "V", dataType: "VARCHAR2", precision: -1, scale: -1, seg: 2},
	}
	for i := range cols {
		cols[i].kind = kindOf(cols[i])
	}
	st := &cdcStream{t: table{name: "T", cols: cols, key: []int{0}}, all: cols, state: cdcState{SCN: scn, From: scn}}
	st.b = newBatcher(memory.DefaultAllocator, cols, true)
	t.Cleanup(st.b.release)
	out := &connectortest.Recorder{}
	r := &cdcReader{s: &Source{cfg: SourceConfig{BatchBytes: 1 << 20}}, out: out, schema: "S",
		streams: map[string]*cdcStream{"T": st}, order: []*cdcStream{st}, txns: map[string]*txn{}, sent: map[string]uint64{}, done: scn}
	return r, st, out
}

func insertRow(scn, usn uint64, id string) logRow {
	return logRow{scn: scn, usn: usn, slt: 1, sqn: 1, op: 1, rsID: " 0x1 ", ssn: scn, rowID: "AAA" + id, table: "T",
		redo: `insert into "S"."T"("ID","V") values ('` + id + `','v');`}
}

func commitRow(scn, usn uint64) logRow { return logRow{scn: scn, usn: usn, slt: 1, sqn: 1, op: 7} }

// A commit whose redo reached the log file only after the window that
// passed its SCN was mined is read by the next window, which mines the last
// flushLag again; a commit read by both windows is sent once.
func TestCDCReadsACommitWrittenLate(t *testing.T) {
	r, st, out := cdcReaderOf(t, 50)
	feed := func(rows ...logRow) {
		t.Helper()
		for _, x := range rows {
			if err := r.row(x); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The first window mines to SCN 200: transaction 1 inserted 1 and
	// committed at 110, and transaction 3 inserted 3 and committed at 160;
	// transaction 2 inserted 2, and its commit, at SCN 150, isn't in the log
	// file yet.
	feed(insertRow(100, 1, "1"), insertRow(105, 2, "2"), commitRow(110, 1), insertRow(155, 3, "3"), commitRow(160, 3))
	if err := r.reached(200); err != nil {
		t.Fatal(err)
	}
	// The next window mines from SCN 100 again, and finds the commit.
	feed(insertRow(100, 1, "1"), insertRow(105, 2, "2"), commitRow(110, 1), commitRow(150, 2), insertRow(155, 3, "3"), commitRow(160, 3))
	if err := r.reached(300); err != nil {
		t.Fatal(err)
	}
	if err := st.flush(r.out); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, row := range out.RowsOf("T") {
		got = append(got, row["ID"])
	}
	if want := []string{"1", "3", "2"}; !slices.Equal(got, want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
	// Past every commit sent, so a read from it sends none again.
	if p := r.position(st); p.SCN < 160 {
		t.Errorf("the position is at SCN %d, before the last commit sent, 160", p.SCN)
	}
}

// Truncating, dropping or exchanging a partition removes rows the redo log
// doesn't name, as TRUNCATE does: the read fails as lost.
func TestCDCPartitionRemovedRows(t *testing.T) {
	for _, stmt := range []string{
		"truncate table s.t",
		`ALTER TABLE "S"."T" TRUNCATE PARTITION p1 UPDATE INDEXES`,
		"alter table t truncate subpartition sp1",
		"ALTER TABLE t DROP PARTITION p1",
		"alter table t drop partitions p1, p2",
		"ALTER TABLE t\n  EXCHANGE PARTITION p1 WITH TABLE x",
	} {
		r, _, _ := cdcReaderOf(t, 50)
		err := r.ddl(logRow{scn: 100, op: 5, table: "T", redo: stmt})
		if code := connectortest.CodeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
			t.Errorf("%q: got %v, want POSITION_LOST", stmt, err)
		}
	}
}

// Changes of partitions that keep their rows don't match.
func TestPartitionRowsKeepsRows(t *testing.T) {
	for _, stmt := range []string{
		"alter table t drop column partition_id",
		"alter table t add (partitions number)",
		"alter table t split partition p1 at (10) into (partition p1a, partition p1b)",
		"alter table t move partition p1",
		"alter table t merge partitions p1, p2 into partition p3",
	} {
		if partitionRows.MatchString(stmt) {
			t.Errorf("%q matches", stmt)
		}
	}
}
