//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl-connectors/connectortest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// sourceEnv is a fresh database on one server.
type sourceEnv struct {
	t        *testing.T
	srv      testServer
	db       *sql.DB
	database string
}

func newSourceEnv(t *testing.T, srv testServer) *sourceEnv {
	db, _ := testDB(t, srv.dsn)
	return &sourceEnv{t: t, srv: srv, db: db, database: newDatabase(t, db)}
}

func (e *sourceEnv) exec(stmt string, args ...any) {
	e.t.Helper()
	mustExec(e.t, e.db, strings.ReplaceAll(stmt, "$db", quoteIdent(e.database)), args...)
}

func (e *sourceEnv) source(extra string) *Source {
	e.t.Helper()
	s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q, "database": %q%s}`, e.srv.dsn, e.database, extra))
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

// capture records what a source emits, in order.
type capture struct {
	mu       sync.Mutex
	log      []string
	batches  map[string][]arrow.RecordBatch
	states   map[string][]byte
	progress []source.Progress
}

func (c *capture) Rows(stream string, _ []json.RawMessage) error {
	return fmt.Errorf("unexpected raw rows on %s", stream)
}

func (c *capture) Records(stream string, b arrow.RecordBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.batches == nil {
		c.batches = map[string][]arrow.RecordBatch{}
	}
	c.batches[stream] = append(c.batches[stream], b)
	c.log = append(c.log, fmt.Sprintf("%s:%d rows", stream, b.NumRows()))
	return nil
}

func (c *capture) ResumePoint(stream string, state []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = append(c.log, fmt.Sprintf("%s@%s", stream, state))
	if c.states == nil {
		c.states = map[string][]byte{}
	}
	c.states[stream] = state
	return nil
}

func (c *capture) Split(stream, id string) source.SplitEmitter { return splitOut{c, stream} }

// splitOut records a split's batches with its stream's.
type splitOut struct {
	c      *capture
	stream string
}

func (o splitOut) Rows([]json.RawMessage) error                  { return errors.New("unexpected raw rows") }
func (o splitOut) Records(b arrow.RecordBatch) error             { return o.c.Records(o.stream, b) }
func (o splitOut) ResumePoint(b []byte, _ ...source.Split) error { return o.c.ResumePoint(o.stream, b) }
func (o splitOut) Complete(b []byte, _ ...source.Split) error    { return o.c.Complete(o.stream, b) }

func (c *capture) Complete(stream string, next []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = append(c.log, fmt.Sprintf("%s complete%s", stream, next))
	if c.states == nil {
		c.states = map[string][]byte{}
	}
	c.states[stream] = next
	return nil
}

func (c *capture) Progress(p source.Progress) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.progress = append(c.progress, p)
	return nil
}

func (c *capture) release() {
	for _, bs := range c.batches {
		for _, b := range bs {
			b.Release()
		}
	}
}

func (c *capture) state(stream string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return string(c.states[stream])
}

// rows are a stream's rows, each column as Arrow prints it.
func (c *capture) rows(stream string) []map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []map[string]string
	for _, b := range c.batches[stream] {
		out = append(out, batchRows(b)...)
	}
	return out
}

func read(t *testing.T, s *Source, req source.ReadRequest) *capture {
	t.Helper()
	c := &capture{}
	t.Cleanup(c.release)
	if err := s.Read(t.Context(), req, c); err != nil {
		t.Fatal(err)
	}
	return c
}

func full(streams ...string) source.ReadRequest {
	var req source.ReadRequest
	for _, s := range streams {
		req.Streams = append(req.Streams, source.ConfiguredStream{Name: s, SyncMode: source.FullRefresh})
	}
	return req
}

func cdc(pipeline string, states map[string]string, streams ...string) source.ReadRequest {
	req := source.ReadRequest{Pipeline: pipeline, Lanes: 4}
	for _, s := range streams {
		cs := source.ConfiguredStream{Name: s, SyncMode: source.CDC}
		if st := states[s]; st != "" {
			cs.State = []byte(st)
		}
		req.Streams = append(req.Streams, cs)
	}
	return req
}

// merged is a changelog's latest version of each key, by key column's
// printed value, in seq order; deleted keys are left out.
func merged(t *testing.T, rows []map[string]string, key string) map[string]map[string]string {
	t.Helper()
	slices.SortStableFunc(rows, func(a, b map[string]string) int { return cmpSeq(a["openetl.seq"], b["openetl.seq"]) })
	out := map[string]map[string]string{}
	for _, r := range rows {
		if r["openetl.op"] == source.OpDelete {
			delete(out, r[key])
			continue
		}
		out[r[key]] = r
	}
	return out
}

func cmpSeq(a, b string) int {
	if len(a) != len(b) {
		return len(a) - len(b)
	}
	return strings.Compare(a, b)
}

// numbers is a derived table of the integers 1 to n, as column i.
func numbers(n int) string {
	return fmt.Sprintf(`(SELECT a.i * 100 + b.i + 1 AS i FROM
		(WITH RECURSIVE r(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM r WHERE i < 99) SELECT i FROM r) a,
		(WITH RECURSIVE r(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM r WHERE i < 99) SELECT i FROM r) b
		WHERE a.i * 100 + b.i < %d) n`, n)
}

const allTypes = `CREATE TABLE $db.%s (id int PRIMARY KEY, b tinyint(1), ti tinyint, tu tinyint unsigned, s smallint,
	su smallint unsigned, m mediumint, mu mediumint unsigned, i int, iu int unsigned, bi bigint, bu bigint unsigned, f float, d double,
	n decimal(12, 3), w decimal(65, 30), c char(3), v varchar(20), l1 varchar(20) CHARACTER SET latin1, t text,
	lt text CHARACTER SET latin1, e enum('sad', 'ok'), st set('a', 'b', 'c'), bn binary(4), vb varbinary(8), bl blob, bt bit(10),
	b1 bit(1), y year, dt date, dtm datetime(6), dz datetime, ts timestamp(3) NULL, tm time(6), j json, g geometry)`

const allValues = `INSERT INTO $db.%s VALUES
	(1, 1, -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615,
	 3.40282e38, 1.7976931348623157e308, 123456789.123, 12345678901234567890.123456789012345678901234567890, 'ab', 'héllo ✓', 'café',
	 'long text', 'œuvre', 'ok', 'a,c', X'01020000', X'00ff', X'deadbeef', b'1010101010', b'1', 2155, '9999-12-31',
	 '9999-12-31 23:59:59.999999', '0000-00-00 00:00:00', '2038-01-19 03:14:07.999', '838:59:59', '{"a": [1, 2.5, "x"], "b": null}',
	 ST_GeomFromText('POINT(1 2)')),
	(2, 0, 127, 0, 32767, 0, 8388607, 0, 2147483647, 0, 9223372036854775807, 0, -1.5, -2.2250738585072014e-308, -0.001, -1.5,
	 '', '', '', '', '', 'sad', '', X'', X'', X'', b'0', b'0', 1901, '1000-01-01', '1000-01-01 00:00:00', '2026-00-15 10:00:00',
	 '1970-01-01 00:00:01', '-12:30:00.5', 'null', ST_GeomFromText('LINESTRING(0 0, 1 1)')),
	(3, 5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 9223372036854775808, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0000-00-00', NULL, NULL, NULL, '00:00:00', NULL, NULL)`

func TestSourceReadsTypes(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec(fmt.Sprintf(allTypes, "kinds"))
		e.exec(fmt.Sprintf(allValues, "kinds"))
		s := e.source("")
		cat, err := s.Discover(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		types := map[string]string{}
		for _, f := range cat.Streams[0].Schema.Fields() {
			types[f.Name] = f.Type.String()
		}
		want := map[string]string{
			"b": "bool", "ti": "int16", "tu": "int16", "su": "int32", "m": "int32", "mu": "int32", "iu": "int64", "bu": "decimal(20, 0)",
			"f": "float32", "d": "float64", "n": "decimal(12, 3)", "w": "decimal(38, 9)", "l1": "utf8", "e": "utf8", "bn": "binary",
			"bt": "binary", "b1": "bool", "y": "int16", "dt": "date32", "dtm": "timestamp[us]", "ts": "timestamp[us, tz=UTC]",
			"tm": "time64[us]", "j": "extension<arrow.json[storage_type=utf8]>", "g": "binary",
		}
		for k, v := range want {
			if types[k] != v {
				t.Errorf("%s declared %s, want %s", k, types[k], v)
			}
		}
		c := read(t, s, full("kinds"))
		rows := c.rows("kinds")
		if len(rows) != 3 {
			t.Fatalf("%d rows", len(rows))
		}
		check := func(row int, col, want string) {
			t.Helper()
			if got := rows[row][col]; got != want {
				t.Errorf("row %d %s = %q, want %q", row+1, col, got, want)
			}
		}
		check(0, "b", "true")
		check(2, "b", "true") // MySQL's: 5 is true
		check(0, "bu", "18446744073709551615")
		check(2, "bu", "9223372036854775808")
		check(0, "w", "12345678901234567890.123456789012345678901234567890")
		check(0, "f", "3.40282e+38")
		check(0, "v", "héllo ✓")
		check(0, "l1", "café")
		check(0, "lt", "œuvre")
		check(0, "st", "a,c")
		check(0, "bn", "AQIAAA==")
		check(1, "bn", "AAAAAA==")
		check(0, "bt", "Aqo=")
		check(0, "b1", "true")
		check(1, "b1", "false")
		check(0, "dtm", "9999-12-31T23:59:59.999999")
		check(0, "dz", array.NullValueStr)
		check(1, "dz", array.NullValueStr)
		check(2, "dt", array.NullValueStr)
		check(0, "ts", "2038-01-19T03:14:07.999Z")
		check(0, "tm", "838:59:59")
		check(1, "tm", "-12:30:00.500000")
		check(0, "g", "AAAAAAEBAAAAAAAAAAAA8D8AAAAAAAAAQA==")
	})
}

// TestCDCReadsTypesAsTheSnapshotDoes inserts every type once before a CDC
// read's snapshot and once after, and expects the binlog's rows to read as
// the snapshot's do.
func TestCDCReadsTypesAsTheSnapshotDoes(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec(fmt.Sprintf(allTypes, "kinds"))
		e.exec(fmt.Sprintf(allValues, "kinds"))
		s := e.source("")
		first := read(t, s, cdc("types", nil, "kinds"))
		snap := first.rows("kinds")
		e.exec("DELETE FROM $db.kinds")
		e.exec(fmt.Sprintf(allValues, "kinds"))
		e.exec("UPDATE $db.kinds SET id = id + 10")
		next := read(t, s, cdc("types", map[string]string{"kinds": first.state("kinds")}, "kinds"))
		var ins, upd []map[string]string
		for _, r := range next.rows("kinds") {
			switch r["openetl.op"] {
			case source.OpInsert:
				ins = append(ins, r)
			case source.OpUpdate:
				upd = append(upd, r)
			}
		}
		if len(snap) != 3 || len(ins) != 3 || len(upd) != 3 {
			t.Log(first.log, next.log)
			t.Fatalf("%d snapshot rows, %d inserts, %d updates", len(snap), len(ins), len(upd))
		}
		for i := range snap {
			for col, v := range snap[i] {
				if strings.HasPrefix(col, "openetl.") {
					continue
				}
				if ins[i][col] != v {
					t.Errorf("row %d %s: the snapshot read %q, the binlog %q", i+1, col, v, ins[i][col])
				}
				if col != "id" && upd[i][col] != v {
					t.Errorf("row %d %s: the snapshot read %q, the binlog's update %q", i+1, col, v, upd[i][col])
				}
			}
		}
	})
}

func TestCDCSnapshotsThenReadsChanges(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY, name varchar(20), n int)")
		e.exec(`INSERT INTO $db.items WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r WHERE i < 95) SELECT i, CONCAT('x', i), i FROM r`)
		s := e.source(`, "snapshotChunkRows": 10`)
		c := read(t, s, cdc("changes", nil, "items"))
		got := merged(t, c.rows("items"), "id")
		if len(got) != 95 {
			t.Fatalf("snapshot: %d rows", len(got))
		}
		state := c.state("items")
		if strings.Contains(state, "snapshot") {
			t.Fatalf("snapshot not done: %s", state)
		}
		e.exec("INSERT INTO $db.items VALUES (100, 'new', 1)")
		e.exec("UPDATE $db.items SET name = 'changed' WHERE id = 5")
		e.exec("DELETE FROM $db.items WHERE id = 6")
		e.exec("UPDATE $db.items SET id = 200 WHERE id = 7")
		c = read(t, s, cdc("changes", map[string]string{"items": state}, "items"))
		var log []string
		for _, r := range c.rows("items") {
			log = append(log, r["openetl.op"]+" "+r["id"]+" "+r["name"])
		}
		want := []string{"insert 100 new", "update 5 changed", "delete 6 x6", "delete 7 x7", "update 200 x7"}
		if !slices.Equal(log, want) {
			t.Fatalf("changes %q, want %q", log, want)
		}
		// From the same state again, the same changes: the read is exact.
		again := read(t, s, cdc("changes", map[string]string{"items": state}, "items"))
		if len(again.rows("items")) != len(want) {
			t.Fatalf("a repeated read had %d changes", len(again.rows("items")))
		}
		// From the last state, none.
		last := read(t, s, cdc("changes", map[string]string{"items": c.state("items")}, "items"))
		if n := len(last.rows("items")); n != 0 {
			t.Fatalf("%d changes after the last state", n)
		}
	})
}

func TestCDCNewColumnRestartsTheRead(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY, name varchar(20))")
		s := e.source("")
		c := read(t, s, cdc("cols", nil, "items"))
		state := c.state("items")
		e.exec("ALTER TABLE $db.items ADD COLUMN extra int")
		e.exec("INSERT INTO $db.items VALUES (1, 'a', 7)")
		req := cdc("cols", map[string]string{"items": state}, "items")
		req.Streams[0].Columns = []string{"id", "name"}
		err := s.Read(t.Context(), req, &capture{})
		if code := codeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT || !strings.Contains(err.Error(), `new column "extra"`) {
			t.Fatalf("a read without the new column: %v (%v)", err, code)
		}
		req.Streams[0].Columns = []string{"id", "name", "extra"}
		c = read(t, s, req)
		rows := c.rows("items")
		if len(rows) != 1 || rows[0]["extra"] != "7" {
			t.Fatalf("rows %v", rows)
		}
		// A column dropped since is left out of what the binlog has of it.
		e.exec("ALTER TABLE $db.items ADD COLUMN gone int")
		e.exec("INSERT INTO $db.items VALUES (2, 'b', 8, 9)")
		e.exec("ALTER TABLE $db.items DROP COLUMN gone")
		c = read(t, s, cdc("cols", map[string]string{"items": c.state("items")}, "items"))
		if rows := c.rows("items"); len(rows) != 1 || rows[0]["id"] != "2" || rows[0]["extra"] != "8" {
			t.Fatalf("rows %v", rows)
		}
	})
}

// TestCDCFollowsConcurrentWrites reads a table's snapshot in key ranges on
// several connections, with small chunks, while writers insert, update,
// delete and move keys, some between a chunk's rows and its binlog
// position; the changelog merged by seq must be the table.
func TestCDCFollowsConcurrentWrites(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY, v int NOT NULL)")
		e.exec("INSERT INTO $db.items SELECT i, 0 FROM " + numbers(3000))
		e.exec("ANALYZE TABLE $db.items")
		ctx, cancel := context.WithCancel(t.Context())
		var wg sync.WaitGroup
		write := func(rng *rand.Rand) {
			id := rng.IntN(4000) + 1
			var err error
			switch rng.IntN(4) {
			case 0:
				_, err = e.db.ExecContext(ctx, "INSERT INTO "+quoteIdent(e.database)+".items VALUES (?, 1) ON DUPLICATE KEY UPDATE v = v + 1", id)
			case 1:
				_, err = e.db.ExecContext(ctx, "UPDATE "+quoteIdent(e.database)+".items SET v = v + 1 WHERE id = ?", id)
			case 2:
				_, err = e.db.ExecContext(ctx, "DELETE FROM "+quoteIdent(e.database)+".items WHERE id = ?", id)
			default:
				_, err = e.db.ExecContext(ctx, "UPDATE IGNORE "+quoteIdent(e.database)+".items SET id = ? WHERE id = ?", rng.IntN(4000)+4001, id)
			}
			if err != nil && ctx.Err() == nil {
				if n, _ := serverError(err); n != 1213 && n != 1205 && n != 1062 {
					t.Error(err)
				}
			}
		}
		for w := range 3 {
			wg.Go(func() {
				rng := rand.New(rand.NewPCG(uint64(w), 1))
				for ctx.Err() == nil {
					write(rng)
				}
			})
		}
		hook := rand.New(rand.NewPCG(9, 9))
		var hookMu sync.Mutex
		// Writes between a chunk's rows and its position: those to rows
		// the chunk locked wait for it.
		afterChunkSnapshot = func() {
			hookMu.Lock()
			defer hookMu.Unlock()
			for range 3 {
				done := make(chan struct{})
				wg.Go(func() {
					defer close(done)
					hookMu.Lock()
					rng := rand.New(rand.NewPCG(hook.Uint64(), 2))
					hookMu.Unlock()
					write(rng)
				})
				hookMu.Unlock()
				select {
				case <-done:
				case <-time.After(20 * time.Millisecond):
				}
				hookMu.Lock()
			}
		}
		t.Cleanup(func() { afterChunkSnapshot = nil })
		s := e.source(`, "snapshotChunkRows": 100, "maxConnections": 4`)
		var rows []map[string]string
		state := map[string]string{}
		for range 3 {
			c := read(t, s, cdc("concurrent", state, "items"))
			rows = append(rows, c.rows("items")...)
			state["items"] = c.state("items")
		}
		cancel()
		wg.Wait()
		afterChunkSnapshot = nil
		c := read(t, s, cdc("concurrent", state, "items"))
		rows = append(rows, c.rows("items")...)
		got := merged(t, rows, "id")
		want := map[string]string{}
		res, err := e.db.QueryContext(t.Context(), "SELECT id, v FROM "+quoteIdent(e.database)+".items")
		if err != nil {
			t.Fatal(err)
		}
		for res.Next() {
			var id, v int
			if err := res.Scan(&id, &v); err != nil {
				t.Fatal(err)
			}
			want[strconv.Itoa(id)] = strconv.Itoa(v)
		}
		_ = res.Close()
		if len(got) != len(want) {
			t.Errorf("%d keys, the table has %d", len(got), len(want))
		}
		for _, k := range slices.Sorted(maps.Keys(want)) {
			if got[k] == nil || got[k]["v"] != want[k] {
				t.Errorf("id %s: v %v, the table has %s", k, got[k], want[k])
				break
			}
		}
	})
}

func TestCDCRetentionAndPurgedPosition(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY)")
		s := e.source("")
		c := read(t, s, cdc("retention", nil, "items"))
		states := map[string][]byte{"items": []byte(c.state("items"))}
		e.exec("INSERT INTO $db.items VALUES (1)")
		p, err := s.Retention(t.Context(), "retention", states, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(p) != 1 || p[0].LagBytes == nil || *p[0].LagBytes == 0 || p[0].RetentionLeft == nil || *p[0].RetentionLeft <= 0 {
			t.Fatalf("retention %+v", p)
		}
		// The binlog the state is in purged: retention is up, and a read
		// can't continue.
		e.exec("FLUSH BINARY LOGS")
		var file string
		var pos any
		rows, err := e.db.QueryContext(t.Context(), map[bool]string{false: "SHOW BINARY LOG STATUS", true: "SHOW MASTER STATUS"}[srv.name == "mariadb"])
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		vals := make([]any, len(cols))
		vals[0], vals[1] = &file, &pos
		for i := 2; i < len(cols); i++ {
			vals[i] = new(any)
		}
		rows.Next()
		if err := rows.Scan(vals...); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		// MariaDB keeps a file until its engines' checkpoint passes it.
		for i := 0; ; i++ {
			e.exec("PURGE BINARY LOGS TO '" + file + "'")
			if !binlogHas(t, e.db, posFile(c.state("items"))) || i == 50 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		p, err = s.Retention(t.Context(), "retention", states, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(p) != 1 || p[0].RetentionLeft == nil || *p[0].RetentionLeft != 0 {
			t.Fatalf("retention of a purged position %+v", p)
		}
		err = s.Read(t.Context(), cdc("retention", map[string]string{"items": c.state("items")}, "items"), &capture{})
		if code := codeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
			t.Fatalf("a read from a purged position: %v (%v)", err, code)
		}
	})
}

func TestCDCRefusesBinlogWithoutRowImages(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY)")
		e.exec("SET GLOBAL binlog_row_image = 'MINIMAL'")
		t.Cleanup(func() { _, _ = e.db.ExecContext(context.Background(), "SET GLOBAL binlog_row_image = 'FULL'") })
		// The global setting is each new session's.
		err := e.source("").Read(t.Context(), cdc("image", nil, "items"), &capture{})
		if code := codeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID || !strings.Contains(err.Error(), "binlog_row_image is MINIMAL, and cdc needs FULL") {
			t.Fatalf("%v (%v)", err, code)
		}
	})
}

func TestCDCReportsProgress(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY)")
		s := e.source("")
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		req := cdc("progress", nil, "items")
		req.Continuous = true
		c := &capture{}
		if err := s.Read(ctx, req, c); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.progress) == 0 || c.progress[0].LagBytes == nil || c.progress[0].Lag == nil || c.progress[0].RetentionLeft == nil {
			t.Fatalf("progress %+v", c.progress)
		}
	})
}

func TestSourceChunksAndResumesExactly(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.k (a varchar(10), b varbinary(4), c datetime(3), d decimal(10, 2), PRIMARY KEY (a, b, c, d))")
		var vals []string
		for i := range 30 {
			vals = append(vals, fmt.Sprintf("('k%d', X'%02x', '2026-01-01 00:00:%02d.5', %d.25)", i%3, i%5, i, i))
		}
		e.exec("INSERT INTO $db.k VALUES " + strings.Join(vals, ", "))
		s := e.source(`, "chunkRows": 4`)
		all := read(t, s, full("k")).rows("k")
		if len(all) != 30 {
			t.Fatalf("%d rows", len(all))
		}
		// Resumed from each resume point, the read yields exactly the rows
		// after it.
		c := read(t, s, full("k"))
		var states []string
		for _, l := range c.log {
			if st, ok := strings.CutPrefix(l, "k@"); ok {
				states = append(states, st)
			}
		}
		for i, st := range states[:len(states)-1] {
			req := full("k")
			req.Streams[0].State = []byte(st)
			rest := read(t, s, req).rows("k")
			if want := all[min((i+1)*4, 30):]; len(rest) != len(want) || len(rest) > 0 && rest[0]["a"]+rest[0]["c"] != want[0]["a"]+want[0]["c"] {
				t.Fatalf("from resume point %d: %d rows, want %d", i, len(rest), len(want))
			}
		}
	})
}

func TestSourceReadsIncrementally(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.ev (id int PRIMARY KEY, at timestamp(6) NULL, amount bigint unsigned)")
		e.exec("INSERT INTO $db.ev VALUES (1, '2026-01-01 00:00:00', 1), (2, '2026-01-02 00:00:00', 18446744073709551615), (3, NULL, 3)")
		s := e.source(`, "chunkRows": 1`)
		for _, cursor := range []string{"at", "amount"} {
			req := source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "ev", SyncMode: source.Incremental, Cursor: cursor}}}
			c := read(t, s, req)
			n := len(c.rows("ev"))
			req.Streams[0].State = []byte(c.state("ev"))
			again := read(t, s, req)
			// The floor's rows again, and nothing older.
			if want := map[string]int{"at": 2, "amount": 3}[cursor]; n != want || len(again.rows("ev")) != 1 {
				t.Fatalf("cursor %s: %d rows, then %d", cursor, n, len(again.rows("ev")))
			}
		}
	})
}

func TestSplitsReadEveryRowOnce(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.big (id bigint unsigned PRIMARY KEY, v int)")
		e.exec("INSERT INTO $db.big SELECT i * 7, i FROM " + numbers(2000))
		e.exec("INSERT INTO $db.big VALUES (18446744073709551615, 0)")
		e.exec("ANALYZE TABLE $db.big")
		s := e.source(`, "chunkRows": 100`)
		cs := source.ConfiguredStream{Name: "big", SyncMode: source.FullRefresh}
		splits, err := s.Splits(t.Context(), cs, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(splits) < 2 {
			t.Fatalf("%d splits", len(splits))
		}
		cs.Splits = splits
		c := read(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 3})
		seen := map[string]int{}
		for _, r := range c.rows("big") {
			seen[r["id"]]++
		}
		if len(seen) != 2001 {
			t.Fatalf("%d keys", len(seen))
		}
		for k, n := range seen {
			if n != 1 {
				t.Fatalf("key %s read %d times", k, n)
			}
		}
	})
}

func TestResplitDividesTheRemainder(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.big (id int PRIMARY KEY)")
		e.exec(`INSERT INTO $db.big WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r WHERE i < 100) SELECT i FROM r`)
		s := e.source(`, "chunkRows": 10`)
		var added []source.Split
		out := &resplitter{s: s, added: &added}
		err := s.readSplit(t.Context(), &connPool{DB: e.db, Held: make(chan struct{}, 1)}, mustTables(t, s, e), source.ConfiguredStream{Name: "big", SyncMode: source.FullRefresh}, source.Split{ID: "x"}, nil, out)
		if err != nil {
			t.Fatal(err)
		}
		if len(added) != 1 || out.rows == 0 {
			t.Fatalf("added %v after %d rows", added, out.rows)
		}
		var st sourceState
		if err := json.Unmarshal(added[0].State, &st); err != nil {
			t.Fatal(err)
		}
		// The first chunk read 1..10, so the remainder 11..100 divides at 55.
		if st.Lo == nil || *st.Lo != "55" || st.After != nil {
			t.Fatalf("upper split %s", added[0].State)
		}
		if out.rows != 55 {
			t.Fatalf("the lower part read %d rows", out.rows)
		}
	})
}

func posFile(state string) string {
	var st cdcState
	_ = json.Unmarshal([]byte(state), &st)
	return st.File
}

func binlogHas(t *testing.T, db *sql.DB, file string) bool {
	rows, err := db.QueryContext(t.Context(), "SHOW BINARY LOGS")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]any, len(cols))
		var name string
		vals[0] = &name
		for i := 1; i < len(vals); i++ {
			vals[i] = new(any)
		}
		if err := rows.Scan(vals...); err != nil {
			t.Fatal(err)
		}
		if name == file {
			return true
		}
	}
	return false
}

func codeOf(err error) protocolv1.ErrorCode {
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if errors.As(err, &coded) {
		return coded.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

func mustTables(t *testing.T, s *Source, e *sourceEnv) []table {
	_, srv := testDB(t, e.srv.dsn)
	tables, err := s.tables(t.Context(), e.db, srv)
	if err != nil {
		t.Fatal(err)
	}
	return tables
}

// resplitter asks for a resplit as soon as the split has read a row.
type resplitter struct {
	s     *Source
	added *[]source.Split
	rows  int
}

func (r *resplitter) Rows([]json.RawMessage) error { return errors.New("raw rows") }
func (r *resplitter) Records(b arrow.RecordBatch) error {
	if r.rows == 0 {
		if ok, _ := r.s.Resplit(context.Background(), "big", "x"); !ok {
			return errors.New("resplit refused")
		}
	}
	r.rows += int(b.NumRows())
	b.Release()
	return nil
}
func (r *resplitter) ResumePoint(_ []byte, added ...source.Split) error {
	*r.added = append(*r.added, added...)
	return nil
}
func (r *resplitter) Complete(_ []byte, added ...source.Split) error {
	*r.added = append(*r.added, added...)
	return nil
}

// TestSourceWithLeastPrivileges reads with the grants the README lists.
func TestSourceWithLeastPrivileges(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY, v int)")
		e.exec("INSERT INTO $db.items VALUES (1, 1)")
		user := "r_" + e.database[2:12]
		e.exec("CREATE USER " + quoteLiteral(user) + " IDENTIFIED BY 'pw'")
		t.Cleanup(func() { _, _ = e.db.ExecContext(context.Background(), "DROP USER "+quoteLiteral(user)) })
		e.exec("GRANT SELECT ON $db.* TO " + quoteLiteral(user))
		e.exec("GRANT REPLICATION SLAVE, " + monitorPrivilege(srv) + " ON *.* TO " + quoteLiteral(user))
		c, err := parseDSN(srv.dsn)
		if err != nil {
			t.Fatal(err)
		}
		c.User, c.Passwd, c.DBName = user, "pw", e.database
		s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q, "database": %q}`, c.FormatDSN(), e.database))
		if err != nil {
			t.Fatal(err)
		}
		expectItems(t, connectortest.Items(t, s.Checks()), nil)
		expectItems(t, connectortest.Items(t, s.Preflight(source.PreflightRequest{Pipeline: "grants", Streams: cdc("grants", nil, "items").Streams})), nil)
		if n := len(read(t, s, full("items")).rows("items")); n != 1 {
			t.Fatalf("%d rows", n)
		}
		first := read(t, s, cdc("grants", nil, "items"))
		e.exec("INSERT INTO $db.items VALUES (2, 2)")
		next := read(t, s, cdc("grants", map[string]string{"items": first.state("items")}, "items"))
		if n := len(next.rows("items")); n != 1 {
			t.Fatalf("%d changes", n)
		}
		if _, err := s.Retention(t.Context(), "grants", map[string][]byte{"items": []byte(next.state("items"))}, nil); err != nil {
			t.Fatal(err)
		}
	})
}

// TestCDCContinuesOnAnotherServerByGTID reads from a state another server
// wrote, as after a failover: by its GTIDs, from where it got to, with seqs
// above the old server's; a state without GTIDs is lost.
func TestCDCContinuesOnAnotherServerByGTID(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY)")
		e.exec("INSERT INTO $db.items VALUES (1)")
		s := e.source("")
		first := read(t, s, cdc("failover", nil, "items"))
		e.exec("INSERT INTO $db.items VALUES (2)")
		var st map[string]any
		if err := json.Unmarshal([]byte(first.state("items")), &st); err != nil {
			t.Fatal(err)
		}
		if st["gtid"] == nil {
			t.Fatalf("no GTIDs in %s", first.state("items"))
		}
		st["server"] = "another"
		moved, _ := json.Marshal(st)
		next := read(t, s, cdc("failover", map[string]string{"items": string(moved)}, "items"))
		rows := next.rows("items")
		if len(rows) != 1 || rows[0]["id"] != "2" || rows[0]["openetl.op"] != source.OpInsert {
			t.Fatalf("after the move: %v", rows)
		}
		if cmpSeq(rows[0]["openetl.seq"], first.rows("items")[0]["openetl.seq"]) <= 0 {
			t.Fatalf("seq %s isn't above the old server's", rows[0]["openetl.seq"])
		}
		if !strings.Contains(next.state("items"), `"epoch":1`) {
			t.Fatalf("state %s", next.state("items"))
		}
		delete(st, "gtid")
		lost, _ := json.Marshal(st)
		err := s.Read(t.Context(), cdc("failover", map[string]string{"items": string(lost)}, "items"), &capture{})
		if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
			t.Fatalf("without GTIDs: %v", err)
		}
	})
}

// TestCDCReadsCompressedTransactions reads changes MySQL wrote to the binlog
// compressed, in one Transaction_payload event.
func TestCDCReadsCompressedTransactions(t *testing.T) {
	e := newSourceEnv(t, servers()[0])
	e.exec("CREATE TABLE $db.items (id int PRIMARY KEY, v varchar(20))")
	e.exec("INSERT INTO $db.items VALUES (1, 'a'), (2, 'b')")
	s := e.source("")
	first := read(t, s, cdc("compressed", nil, "items"))
	conn, err := e.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	db := quoteIdent(e.database)
	for _, q := range []string{
		"SET SESSION binlog_transaction_compression = ON", "BEGIN",
		"INSERT INTO " + db + ".items VALUES (3, 'c')", "UPDATE " + db + ".items SET v = 'z' WHERE id = 1", "DELETE FROM " + db + ".items WHERE id = 2",
		"COMMIT", "SET SESSION binlog_transaction_compression = OFF",
	} {
		if _, err := conn.ExecContext(t.Context(), q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	next := read(t, s, cdc("compressed", map[string]string{"items": first.state("items")}, "items"))
	var log []string
	for _, r := range next.rows("items") {
		log = append(log, r["openetl.op"]+" "+r["id"]+" "+r["v"])
	}
	if want := []string{"insert 3 c", "update 1 z", "delete 2 b"}; !slices.Equal(log, want) {
		t.Fatalf("changes %q, want %q", log, want)
	}
	var st struct{ File string }
	if err := json.Unmarshal([]byte(next.state("items")), &st); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.QueryContext(t.Context(), "SHOW BINLOG EVENTS IN "+quoteLiteral(st.File))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	compressed := false
	for rows.Next() {
		var file, typ, info string
		var pos, server, end int64
		if err := rows.Scan(&file, &pos, &typ, &server, &end, &info); err != nil {
			t.Fatal(err)
		}
		compressed = compressed || typ == "Transaction_payload"
	}
	if !compressed {
		t.Fatal("the binlog has no compressed transaction")
	}
}

// TestValuesPastTheirDeclaredTypesComeAsText reads decimals a decimal(38, 9)
// can't hold and times outside a day as MySQL writes them, by a query and
// from the binlog, for the platform to refuse: 30 integer digits, 10 places,
// a time below 0 and one past 24:00:00.
func TestValuesPastTheirDeclaredTypesComeAsText(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.q (id int PRIMARY KEY, w decimal(65, 30), tm time(6))")
		const values = `INSERT INTO $db.q VALUES (1, 12345678901234567890123456789.123456789, '23:59:59.999999'),
			(2, 123456789012345678901234567890.5, '24:00:01'), (3, 0.0000000001, '-00:00:01'), (4, -1, '838:59:59')`
		e.exec(values)
		want := []string{
			"12345678901234567890123456789.123456789000000000000000000000 23:59:59.999999",
			"123456789012345678901234567890.500000000000000000000000000000 24:00:01",
			"0.000000000100000000000000000000 -00:00:01",
			"-1.000000000000000000000000000000 838:59:59",
		}
		s := e.source("")
		cat, err := s.Discover(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		sch := cat.Streams[0].Schema
		if w, tm := sch.Field(sch.FieldIndices("w")[0]).Type.String(), sch.Field(sch.FieldIndices("tm")[0]).Type.String(); w != "decimal(38, 9)" || tm != "time64[us]" {
			t.Fatalf("declared %s and %s", w, tm)
		}
		texts := func(rows []map[string]string) []string {
			var out []string
			for _, r := range rows {
				out = append(out, r["w"]+" "+r["tm"])
			}
			return out
		}
		first := read(t, s, cdc("q", nil, "q"))
		if got := texts(first.rows("q")); !slices.Equal(got, want) {
			t.Fatalf("by a query: %q", got)
		}
		e.exec("DELETE FROM $db.q")
		e.exec(values)
		var ins []map[string]string
		for _, r := range read(t, s, cdc("q", map[string]string{"q": first.state("q")}, "q")).rows("q") {
			if r["openetl.op"] == source.OpInsert {
				ins = append(ins, r)
			}
		}
		if got := texts(ins); !slices.Equal(got, want) {
			t.Fatalf("from the binlog: %q", got)
		}
	})
}

// TestConnectionLimitsAreTransient checks as a user whose one connection is
// taken: the check fails, and may be tried again.
func TestConnectionLimitsAreTransient(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		user := "l_" + e.database[2:12]
		e.exec("CREATE USER " + quoteLiteral(user) + " IDENTIFIED BY 'pw' WITH MAX_USER_CONNECTIONS 1")
		t.Cleanup(func() { _, _ = e.db.ExecContext(context.Background(), "DROP USER "+quoteLiteral(user)) })
		e.exec("GRANT SELECT ON $db.* TO " + quoteLiteral(user))
		c, err := parseDSN(srv.dsn)
		if err != nil {
			t.Fatal(err)
		}
		c.User, c.Passwd, c.DBName = user, "pw", e.database
		held, _, err := open(t.Context(), c, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = held.Close() }()
		s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q}`, c.FormatDSN()))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Check(t.Context()); codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
			t.Fatalf("%v (%v)", err, codeOf(err))
		}
	})
}
