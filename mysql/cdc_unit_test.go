package mysql

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/openetl-io/openetl-connectors/connectortest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// binlogReader is a cdcReader of app.items (id int primary key, v
// varchar(20)) from binlog.000001 at 4, without a server: tests feed it
// events.
func binlogReader(t *testing.T, mariadb bool) (*cdcReader, *connectortest.Recorder) {
	t.Helper()
	items := table{name: "items", key: []int{0}, cols: []sourceColumn{
		{name: "id", dataType: "int", columnType: "int", kind: kInt32, notNull: true},
		{name: "v", dataType: "varchar", columnType: "varchar(20)", kind: kString},
	}}
	b, err := newBatcher(memory.DefaultAllocator, items.cols, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.release)
	rec := &connectortest.Recorder{}
	return &cdcReader{
		ctx: t.Context(), s: &Source{cfg: SourceConfig{Database: "app", BatchBytes: 1 << 20}}, out: rec, srv: server{mariadb: mariadb},
		streams: []*cdcStream{{t: items, b: b}}, rels: map[uint64]*relation{}, pos: binlogPos{File: "binlog.000001", Pos: 4},
	}, rec
}

// feed hands r the events, each ending at its position.
func feed(t *testing.T, r *cdcReader, events ...*replication.BinlogEvent) error {
	t.Helper()
	for _, ev := range events {
		if err := r.event(t.Context(), ev); err != nil {
			return err
		}
	}
	return nil
}

func at(pos uint32, typ replication.EventType, e replication.Event) *replication.BinlogEvent {
	return &replication.BinlogEvent{Header: &replication.EventHeader{EventType: typ, LogPos: pos, Timestamp: 1}, Event: e}
}

func itemsMap() *replication.TableMapEvent {
	return &replication.TableMapEvent{TableID: 1, Schema: []byte("app"), Table: []byte("items"), ColumnCount: 2,
		ColumnName: [][]byte{[]byte("id"), []byte("v")}}
}

func itemRows(rows ...[]any) *replication.RowsEvent {
	return &replication.RowsEvent{TableID: 1, ColumnCount: 2, Rows: rows}
}

func query(q string) *replication.QueryEvent { return &replication.QueryEvent{Query: []byte(q)} }

// emitted flushes the reader's batches and returns its rows of items, each
// as op, id and v.
func emitted(t *testing.T, r *cdcReader, rec *connectortest.Recorder) []map[string]string {
	t.Helper()
	if err := r.resumeStream(r.streams[0]); err != nil {
		t.Fatal(err)
	}
	return rec.RowsOf("items")
}

// TestMariaDBTransactionsHaveNoBegin reads a MariaDB transaction, which
// its GTID event begins without a BEGIN, with a SAVEPOINT inside it: the
// rows after the savepoint are the transaction's too.
func TestMariaDBTransactionsHaveNoBegin(t *testing.T) {
	r, rec := binlogReader(t, true)
	gtid := &replication.MariadbGTIDEvent{GTID: gomysql.MariadbGTID{ServerID: 1, SequenceNumber: 5}, Flags: replication.BINLOG_MARIADB_FL_TRANSACTIONAL}
	err := feed(t, r,
		at(100, replication.MARIADB_GTID_EVENT, gtid),
		at(150, replication.TABLE_MAP_EVENT, itemsMap()),
		at(200, replication.WRITE_ROWS_EVENTv1, itemRows([]any{int32(1), "a"})),
		at(250, replication.QUERY_EVENT, query("SAVEPOINT `s1`")),
		at(300, replication.TABLE_MAP_EVENT, itemsMap()),
		at(350, replication.WRITE_ROWS_EVENTv1, itemRows([]any{int32(2), "b"})),
		at(400, replication.XID_EVENT, &replication.XIDEvent{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if rows := emitted(t, r, rec); len(rows) != 2 || r.txn != nil || r.pos.Pos != 400 {
		t.Fatalf("rows %v, position %v, in a transaction %v", rows, r.pos, r.txn != nil)
	}
	// A DDL's group is standalone: its statement ends it.
	ddl := &replication.MariadbGTIDEvent{GTID: gomysql.MariadbGTID{ServerID: 1, SequenceNumber: 6}, Flags: replication.BINLOG_MARIADB_FL_STANDALONE | replication.BINLOG_MARIADB_FL_DDL}
	if err := feed(t, r, at(450, replication.MARIADB_GTID_EVENT, ddl), at(500, replication.QUERY_EVENT, query("ALTER TABLE items ADD COLUMN w int"))); err != nil {
		t.Fatal(err)
	}
	if r.txn != nil || r.pos.Pos != 500 {
		t.Fatalf("after a DDL: position %v, in a transaction %v", r.pos, r.txn != nil)
	}
}

// TestXATransactionsAreRead reads an XA transaction as MySQL logs it, in two
// parts: its changes between XA START and the XA_prepare event that ends
// them, and later its XA COMMIT on its own.
func TestXATransactionsAreRead(t *testing.T) {
	r, rec := binlogReader(t, false)
	err := feed(t, r,
		at(100, replication.ANONYMOUS_GTID_EVENT, &replication.GTIDEvent{}),
		at(200, replication.QUERY_EVENT, query("XA START X'78',X'',1")),
		at(250, replication.TABLE_MAP_EVENT, itemsMap()),
		at(300, replication.WRITE_ROWS_EVENTv2, itemRows([]any{int32(1), "a"})),
		at(350, replication.QUERY_EVENT, query("XA END X'78',X'',1")),
		at(400, replication.XA_PREPARE_LOG_EVENT, &replication.GenericEvent{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if r.txn != nil || r.pos.Pos != 400 {
		t.Fatalf("after XA PREPARE: position %v, in a transaction %v", r.pos, r.txn != nil)
	}
	if err := feed(t, r, at(450, replication.ANONYMOUS_GTID_EVENT, &replication.GTIDEvent{}), at(550, replication.QUERY_EVENT, query("XA COMMIT X'78',X'',1"))); err != nil {
		t.Fatal(err)
	}
	if rows := emitted(t, r, rec); len(rows) != 1 || rows[0]["id"] != "1" || r.txn != nil || r.pos.Pos != 550 {
		t.Fatalf("rows %v, position %v, in a transaction %v", rows, r.pos, r.txn != nil)
	}
}

// TestRowsOutsideATransactionFail: rows of a stream that come where the
// read sees no transaction fail it, rather than go unread.
func TestRowsOutsideATransactionFail(t *testing.T) {
	r, _ := binlogReader(t, false)
	err := feed(t, r, at(150, replication.TABLE_MAP_EVENT, itemsMap()), at(200, replication.WRITE_ROWS_EVENTv2, itemRows([]any{int32(1), "a"})))
	if err == nil || !strings.Contains(err.Error(), "outside a transaction") {
		t.Fatalf("rows outside a transaction: %v", err)
	}
}

// TestPartialRowImagesFail reads changes that a session logged with
// binlog_row_image MINIMAL or NOBLOB, or with a JSON column's partial
// update: an insert or an update without a column's value fails the read,
// rather than set the column null, and a delete needs only the key.
func TestPartialRowImagesFail(t *testing.T) {
	for name, c := range map[string]struct {
		typ     replication.EventType
		rows    [][]any
		skipped [][]int
		ok      bool
	}{
		"a delete of the key alone":            {replication.DELETE_ROWS_EVENTv2, [][]any{{int32(1), nil}}, [][]int{{1}}, true},
		"an update's old row of the key alone": {replication.UPDATE_ROWS_EVENTv2, [][]any{{int32(1), nil}, {int32(1), "b"}}, [][]int{{1}, {}}, true},
		"an update without v":                  {replication.UPDATE_ROWS_EVENTv2, [][]any{{int32(1), nil}, {int32(1), nil}}, [][]int{{1}, {1}}, false},
		"an insert without v":                  {replication.WRITE_ROWS_EVENTv2, [][]any{{int32(1), nil}}, [][]int{{1}}, false},
		"a delete without the key":             {replication.DELETE_ROWS_EVENTv2, [][]any{{nil, "a"}}, [][]int{{0}}, false},
		"a partial JSON update":                {replication.PARTIAL_UPDATE_ROWS_EVENT, [][]any{{int32(1), "a"}, {int32(1), &replication.JsonDiff{}}}, [][]int{{}, {}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			r, rec := binlogReader(t, false)
			ev := itemRows(c.rows...)
			ev.SkippedColumns = c.skipped
			err := feed(t, r, at(100, replication.QUERY_EVENT, query("BEGIN")), at(150, replication.TABLE_MAP_EVENT, itemsMap()), at(200, c.typ, ev))
			switch {
			case c.ok && err != nil:
				t.Fatal(err)
			case c.ok:
				if rows := emitted(t, r, rec); len(rows) != 1 || rows[0]["id"] != "1" {
					t.Fatalf("rows %v", rows)
				}
			case connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID || !strings.Contains(err.Error(), "binlog_row_"):
				t.Fatalf("%v (%v)", err, connectortest.CodeOf(err))
			}
		})
	}
}
