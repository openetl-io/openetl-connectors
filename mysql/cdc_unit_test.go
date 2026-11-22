package mysql

import (
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"

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

// TestStatementChangesFail reads changes a session logged as statements,
// with binlog_format STATEMENT or MIXED, which have no rows: one that writes
// a table the read has fails it, and others, which may name the table
// without writing it, are no concern of it.
func TestStatementChangesFail(t *testing.T) {
	for _, c := range []struct {
		schema, q string
		fails     bool
	}{
		{"app", "INSERT INTO items VALUES (3, 's')", true},
		{"app", "update `items` set v = 'x' where id = 1", true},
		{"other", "DELETE FROM app.items WHERE id = 1", true},
		{"app", "REPLACE INTO items (id, v) VALUES (1, 'x')", true},
		{"app", "INSERT IGNORE INTO items(id) VALUES (1)", true},
		{"app", "/* app */ INSERT INTO items VALUES (3, 's')", true},
		{"other", "DELETE LOW_PRIORITY QUICK FROM `app`.`items` WHERE id = 1", true},
		{"app", "INSERT INTO items_log VALUES (1)", false},
		{"other", "INSERT INTO items VALUES (1)", false},
		{"app", "INSERT INTO other.items VALUES (1)", false},
		{"app", "SAVEPOINT items", false},
		{"app", "ALTER TABLE items ADD COLUMN w int", false},
		{"app", "REPLACE INTO other.checksums SELECT COUNT(*) FROM app.items", false},
		{"app", "INSERT INTO audit VALUES ('items')", false},
		{"other", "UPDATE log JOIN app.items USING (id) SET n = 1", false},
		{"app", "UPDATE log JOIN app.items USING (id) SET app.items.v = 'x'", true},
		{"app", "UPDATE log l JOIN items AS i ON l.id = i.id SET l.n = 1, i.v = 'it''s'", true},
		{"app", "UPDATE log JOIN `items` USING (id) SET v = 'x'", true},
		{"app", "UPDATE log, items SET log.n = 1, items.v = 'x' WHERE log.id = items.id", true},
		{"app", "UPDATE items i JOIN log l USING (id) SET l.n = 1", false},
		{"app", "UPDATE log SET n = (SELECT count(*) FROM items) WHERE v = 'items'", false},
		{"app", "DELETE log, items FROM log JOIN items USING (id)", true},
		{"app", "DELETE log, i FROM log JOIN items AS i USING (id)", true},
		{"app", "DELETE FROM log.*, app.items.* USING log JOIN app.items USING (id)", true},
		{"app", "DELETE FROM items AS i WHERE i.id = 1", true},
		{"app", "DELETE log FROM log JOIN items USING (id)", false},
		{"app", "DELETE i FROM items AS l JOIN log AS i USING (id)", false},
	} {
		r, _ := binlogReader(t, false)
		err := feed(t, r, at(100, replication.QUERY_EVENT, query("BEGIN")), at(200, replication.QUERY_EVENT, &replication.QueryEvent{Schema: []byte(c.schema), Query: []byte(c.q)}))
		if c.fails != (connectortest.CodeOf(err) == protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID) || c.fails && !strings.Contains(err.Error(), "binlog_format") {
			t.Errorf("%s in %s: %v", c.q, c.schema, err)
		}
	}
}

// TestColumnsTheBinlogLacksAreUnchanged reads a change logged before the
// stream's column v was added, or renamed to v: the change has no v, which
// it sends as unchanged rather than null.
func TestColumnsTheBinlogLacksAreUnchanged(t *testing.T) {
	r, rec := binlogReader(t, false)
	before := &replication.TableMapEvent{TableID: 1, Schema: []byte("app"), Table: []byte("items"), ColumnCount: 1, ColumnName: [][]byte{[]byte("id")}}
	err := feed(t, r, at(100, replication.QUERY_EVENT, query("BEGIN")), at(150, replication.TABLE_MAP_EVENT, before),
		at(200, replication.UPDATE_ROWS_EVENTv2, &replication.RowsEvent{TableID: 1, ColumnCount: 1, Rows: [][]any{{int32(1)}, {int32(1)}}}),
		at(250, replication.TABLE_MAP_EVENT, itemsMap()), at(300, replication.WRITE_ROWS_EVENTv2, itemRows([]any{int32(2), "b"})),
		at(350, replication.XID_EVENT, &replication.XIDEvent{}))
	if err != nil {
		t.Fatal(err)
	}
	rows := emitted(t, r, rec)
	if len(rows) != 2 || rows[0]["openetl.unchanged"] != `["v"]` || rows[1]["openetl.unchanged"] != "" || rows[1]["v"] != "b" {
		t.Fatalf("rows %v", rows)
	}
}

// TestTaggedGTIDsAreKept reads a transaction whose GTID has a tag, as MySQL
// logs from 8.3: the read's GTIDs have it, so that a read on another server
// after a failover doesn't read it again.
func TestTaggedGTIDsAreKept(t *testing.T) {
	r, _ := binlogReader(t, false)
	const sid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	var err error
	if r.gset, err = gomysql.ParseMysqlGTIDSet(sid + ":1-6"); err != nil {
		t.Fatal(err)
	}
	b, _ := hex.DecodeString(strings.ReplaceAll(sid, "-", ""))
	tagged := &replication.GtidTaggedLogEvent{GTIDEvent: replication.GTIDEvent{SID: b, GNO: 7, Tag: gomysql.NewTag("t1")}}
	err = feed(t, r, at(100, replication.GTID_TAGGED_LOG_EVENT, tagged), at(150, replication.QUERY_EVENT, query("BEGIN")), at(200, replication.XID_EVENT, &replication.XIDEvent{}))
	if err != nil {
		t.Fatal(err)
	}
	want, err := gomysql.ParseMysqlGTIDSet(sid + ":t1:7")
	if err != nil {
		t.Fatal(err)
	}
	if !r.gset.Contain(want) || r.txn != nil || r.pos.Pos != 200 {
		t.Fatalf("GTIDs %s, position %v, in a transaction %v", r.gset, r.pos, r.txn != nil)
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

// TestUnexpectedBinlogValuesFail reads a value of a Go type the binlog's
// decoder doesn't give, as a newer one might: the read fails, as the binlog
// no longer reads as it did.
func TestUnexpectedBinlogValuesFail(t *testing.T) {
	r, _ := binlogReader(t, false)
	err := feed(t, r, at(100, replication.QUERY_EVENT, query("BEGIN")), at(150, replication.TABLE_MAP_EVENT, itemsMap()),
		at(200, replication.WRITE_ROWS_EVENTv2, itemRows([]any{int32(1), struct{}{}})))
	if connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED || !strings.Contains(err.Error(), "items.v") {
		t.Fatalf("%v (%v)", err, connectortest.CodeOf(err))
	}
}

// TestCDCDialsUnixSockets reads the binlog of a server the DSN reaches by a
// Unix socket: the binlog connection dials the socket.
func TestCDCDialsUnixSockets(t *testing.T) {
	dir, err := os.MkdirTemp("", "s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "m.sock")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	r, _ := binlogReader(t, false)
	if r.s.conn, err = mysql.ParseDSN("u:p@unix(" + sock + ")/app"); err != nil {
		t.Fatal(err)
	}
	syncer := replication.NewBinlogSyncer(r.syncerConfig("p"))
	defer syncer.Close()
	go func() { _, _ = syncer.StartSync(gomysql.Position{Name: "binlog.000001", Pos: 4}) }()
	_ = l.SetDeadline(time.Now().Add(5 * time.Second))
	conn, err := l.Accept()
	if err != nil {
		t.Fatalf("nothing dialed %s: %v", sock, err)
	}
	_ = conn.Close()
}
