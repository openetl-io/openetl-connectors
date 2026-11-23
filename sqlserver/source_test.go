//go:build integration

package sqlserver

import (
	"cmp"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// dsn is the connectors' login, with the privileges the README lists.
func dsn() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_SQLSERVER_DSN"), "sqlserver://openetl:OpenETL-test-1@localhost:1433?database=openetl")
}

// adminDSN sets up what the tests read: tables, change tracking and change
// data capture, which need db_owner.
func adminDSN() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_SQLSERVER_ADMIN_DSN"), "sqlserver://sa:OpenETL-test-1@localhost:1433?database=openetl")
}

func admin(t *testing.T) *sql.DB {
	t.Helper()
	db, err := openDB(adminDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// sourceEnv is a fresh schema in the test database, which the connectors'
// login may read.
type sourceEnv struct {
	t      *testing.T
	db     *sql.DB
	schema string
}

func newSourceEnv(t *testing.T) *sourceEnv {
	t.Helper()
	e := &sourceEnv{t: t, db: admin(t), schema: "s_" + strings.ToLower(rand.Text())[:10]}
	t.Cleanup(func() { dropSchema(e.db, e.schema) })
	e.exec("CREATE SCHEMA $schema")
	e.exec("GRANT SELECT, VIEW CHANGE TRACKING ON SCHEMA::$schema TO openetl")
	return e
}

// dropSchema drops a schema and its views and tables, after turning off
// their change data capture and system versioning.
func dropSchema(db *sql.DB, schema string) {
	ctx := context.Background()
	_, _ = db.ExecContext(ctx, `DECLARE @t sysname, @i sysname;
		DECLARE c CURSOR LOCAL FOR SELECT OBJECT_NAME(source_object_id), capture_instance FROM cdc.change_tables WHERE OBJECT_SCHEMA_NAME(source_object_id) = @p1;
		OPEN c; FETCH NEXT FROM c INTO @t, @i;
		WHILE @@FETCH_STATUS = 0 BEGIN
			EXEC sys.sp_cdc_disable_table @source_schema = @p1, @source_name = @t, @capture_instance = @i;
			FETCH NEXT FROM c INTO @t, @i;
		END;
		CLOSE c;
		DECLARE @sql nvarchar(max) = N'';
		SELECT @sql += N'ALTER TABLE ' + QUOTENAME(@p1) + N'.' + QUOTENAME(name) + N' SET (SYSTEM_VERSIONING = OFF); ' FROM sys.tables WHERE schema_id = SCHEMA_ID(@p1) AND temporal_type = 2;
		SELECT @sql += N'DROP VIEW ' + QUOTENAME(@p1) + N'.' + QUOTENAME(name) + N'; ' FROM sys.views WHERE schema_id = SCHEMA_ID(@p1);
		SELECT @sql += N'DROP TABLE ' + QUOTENAME(@p1) + N'.' + QUOTENAME(name) + N'; ' FROM sys.tables WHERE schema_id = SCHEMA_ID(@p1);
		SELECT @sql += N'DROP TYPE ' + QUOTENAME(@p1) + N'.' + QUOTENAME(name) + N'; ' FROM sys.types WHERE schema_id = SCHEMA_ID(@p1);
		EXEC (@sql);
		SET @sql = N'DROP SCHEMA ' + QUOTENAME(@p1);
		IF SCHEMA_ID(@p1) IS NOT NULL EXEC (@sql);`, schema)
}

func (e *sourceEnv) exec(q string, args ...any) {
	e.t.Helper()
	if _, err := e.db.ExecContext(context.Background(), strings.ReplaceAll(q, "$schema", quote(e.schema)), args...); err != nil {
		e.t.Fatalf("%s: %v", q, err)
	}
}

// capture turns on change data capture for a table, and waits until the
// capture job knows it.
func (e *sourceEnv) capture(table string) {
	e.t.Helper()
	e.exec("EXEC sys.sp_cdc_enable_table @source_schema = @p1, @source_name = @p2, @role_name = NULL", e.schema, table)
}

// harvested waits until the capture job has harvested every change committed
// so far.
func (e *sourceEnv) harvested() {
	e.t.Helper()
	if err := waitHarvested(context.Background(), e.db); err != nil {
		e.t.Fatal(err)
	}
}

// waitHarvested waits until the capture job has harvested the changes
// committed so far, by writing one to a table of its own and waiting for it.
func waitHarvested(ctx context.Context, db *sql.DB) error {
	return harvestedWithin(ctx, db, time.Minute)
}

func harvestedWithin(ctx context.Context, db *sql.DB, wait time.Duration) error {
	var n int64
	if err := db.QueryRowContext(ctx, `IF OBJECT_ID(N'dbo.openetl_tick') IS NULL BEGIN
			CREATE TABLE dbo.openetl_tick (id int PRIMARY KEY, n bigint);
			INSERT INTO dbo.openetl_tick VALUES (1, 0);
			EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'openetl_tick', @role_name = NULL;
		END;
		UPDATE dbo.openetl_tick SET n = n + 1;
		SELECT n FROM dbo.openetl_tick`).Scan(&n); err != nil {
		return err
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		var got *int64
		if err := db.QueryRowContext(ctx, "SELECT max(n) FROM cdc.dbo_openetl_tick_CT").Scan(&got); err != nil {
			return err
		}
		if got != nil && *got >= n {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("the capture job didn't harvest a change in %s", wait)
}

// captureRunning reports whether the database's capture job is running, as
// SQL Server Agent's current session has it.
func captureRunning(ctx context.Context, db *sql.DB) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM msdb.dbo.sysjobactivity a JOIN msdb.dbo.sysjobs j ON j.job_id = a.job_id
		WHERE j.name = N'cdc.' + DB_NAME() + N'_capture' AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions)
		AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL`).Scan(&n)
	return n > 0, err
}

// stopCapture stops the capture job, and waits until it has: SQL Server
// Agent stops a job a while after it's asked.
func (e *sourceEnv) stopCapture() {
	e.t.Helper()
	e.exec("EXEC sys.sp_cdc_stop_job @job_type = N'capture'")
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(200 * time.Millisecond) {
		running, err := captureRunning(context.Background(), e.db)
		if err != nil {
			e.t.Fatal(err)
		}
		if !running {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatal("the capture job didn't stop in a minute")
		}
	}
}

// startCapture starts the capture job, and waits until it harvests. SQL
// Server Agent refuses to start a job that runs, or is still stopping, so
// while it refuses, a harvest tells which, and it's asked again.
func startCapture(ctx context.Context, db *sql.DB) error {
	for deadline := time.Now().Add(2 * time.Minute); ; {
		_, err := db.ExecContext(ctx, "EXEC sys.sp_cdc_start_job @job_type = N'capture'")
		if err == nil {
			return waitHarvested(ctx, db)
		}
		// The capture job scans every 5 seconds.
		if harvestedWithin(ctx, db, 12*time.Second) == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the capture job didn't start in 2 minutes: %w", err)
		}
	}
}

func (e *sourceEnv) source(extra string) *Source {
	e.t.Helper()
	return e.sourceAs(dsn(), extra)
}

func (e *sourceEnv) sourceAs(dsn, extra string) *Source {
	e.t.Helper()
	cfg := fmt.Sprintf(`{"dsn": %q, "schema": %q%s}`, dsn, e.schema, extra)
	s, err := NewSource([]byte(cfg))
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = s.db.Close() })
	return s
}

// capturer records what a source emits, in order.
type capturer struct {
	mu      sync.Mutex
	log     []string
	batches map[string][]arrow.RecordBatch
	states  map[string][]string
	// Where each stream's next read starts, once it's complete.
	next     map[string]string
	progress []source.Progress
	// Called after each resume point, if set.
	resumed func()
}

func (c *capturer) Rows(stream string, _ []json.RawMessage) error {
	return fmt.Errorf("unexpected raw rows on %s", stream)
}

func (c *capturer) Records(stream string, b arrow.RecordBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.batches == nil {
		c.batches = map[string][]arrow.RecordBatch{}
	}
	b.Retain()
	c.batches[stream] = append(c.batches[stream], b)
	c.log = append(c.log, fmt.Sprintf("%s:%d rows", stream, b.NumRows()))
	return nil
}

func (c *capturer) ResumePoint(stream string, state []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.states == nil {
		c.states = map[string][]string{}
	}
	c.states[stream] = append(c.states[stream], string(state))
	c.log = append(c.log, fmt.Sprintf("%s@%s", stream, state))
	if c.resumed != nil {
		c.resumed()
	}
	return nil
}

func (c *capturer) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(c, stream, id)
}

func (c *capturer) Complete(stream string, next []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = append(c.log, fmt.Sprintf("%s complete%s", stream, next))
	if c.next == nil {
		c.next = map[string]string{}
	}
	c.next[stream] = string(next)
	return nil
}

func (c *capturer) Progress(p source.Progress) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.progress = append(c.progress, p)
	return nil
}

func (c *capturer) release() {
	for _, bs := range c.batches {
		for _, b := range bs {
			b.Release()
		}
	}
}

// last is a stream's last resume point.
func (c *capturer) last(stream string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.states[stream]
	if len(s) == 0 {
		return ""
	}
	return s[len(s)-1]
}

// column returns a stream's values in one column, as Arrow prints them.
func (c *capturer) column(stream, name string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, b := range c.batches[stream] {
		idx := b.Schema().FieldIndices(name)
		if len(idx) != 1 {
			return nil
		}
		col := b.Column(idx[0])
		for i := range col.Len() {
			if col.IsNull(i) {
				out = append(out, array.NullValueStr)
				continue
			}
			out = append(out, col.ValueStr(i))
		}
	}
	return out
}

// rows returns a stream's rows, each its columns' values joined by commas.
func (c *capturer) rows(stream string, names ...string) []string {
	cols := make([][]string, len(names))
	for i, n := range names {
		cols[i] = c.column(stream, n)
	}
	var out []string
	for r := range cols[0] {
		vals := make([]string, len(cols))
		for i := range cols {
			vals[i] = cols[i][r]
		}
		out = append(out, strings.Join(vals, ","))
	}
	return out
}

func readReq(t *testing.T, s *Source, req source.ReadRequest) *capturer {
	t.Helper()
	c := &capturer{}
	t.Cleanup(c.release)
	if err := s.Read(t.Context(), req, c); err != nil {
		t.Fatal(err)
	}
	return c
}

// read reads streams fully, each from a state if it has one, in mode.
func read(t *testing.T, s *Source, mode source.SyncMode, streams map[string]string) *capturer {
	t.Helper()
	var req source.ReadRequest
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for n := range streams {
			if !yield(n) {
				return
			}
		}
	}) {
		cs := source.ConfiguredStream{Name: name, SyncMode: mode}
		if st := streams[name]; st != "" {
			cs.State = []byte(st)
		}
		req.Streams = append(req.Streams, cs)
	}
	return readReq(t, s, req)
}

func TestSourceDiscovers(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.keyed (b nvarchar(10), a int, v nvarchar(max), PRIMARY KEY (a, b))")
	e.exec("CREATE TABLE $schema.loose (v nvarchar(10), w nvarchar(10) NOT NULL)")
	e.exec("CREATE TABLE $schema.tracked (id int PRIMARY KEY, at datetime2, rv rowversion)")
	e.exec("ALTER TABLE $schema.tracked ENABLE CHANGE_TRACKING")
	e.exec("CREATE TABLE $schema.captured (id int PRIMARY KEY)")
	e.capture("captured")
	e.exec("CREATE VIEW $schema.view1 AS SELECT id, at FROM $schema.tracked")

	cat, err := e.source("").Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]source.CatalogStream{}
	for _, s := range cat.Streams {
		got[s.Name] = s
	}
	if len(got) != 5 {
		t.Errorf("streams %v, want keyed, loose, tracked, captured and view1", cat.Streams)
	}
	if s := got["keyed"]; !slices.Equal(s.PrimaryKey, []string{"a", "b"}) || s.Replay != source.Exact || !s.SplitsByKey || slices.Contains(s.SyncModes, source.CDC) {
		t.Errorf("keyed: %+v", s)
	}
	if s := got["loose"]; len(s.PrimaryKey) != 0 || s.SplitsByKey || slices.Contains(s.SyncModes, source.CDC) {
		t.Errorf("loose: %+v", s)
	}
	// Change tracking keeps a key's latest change alone, so its streams
	// replay a superset.
	if s := got["tracked"]; !slices.Contains(s.SyncModes, source.CDC) || !slices.Contains(s.SyncModes, source.Incremental) || s.Seq == nil || s.CursorField != "rv" || s.Replay != source.Superset {
		t.Errorf("tracked: %+v", s)
	}
	// A view is read in full or incrementally, never with cdc.
	if s := got["view1"]; len(s.PrimaryKey) != 0 || s.SplitsByKey || s.Replay != source.Exact || !slices.Equal(s.SyncModes, []source.SyncMode{source.FullRefresh, source.Incremental}) {
		t.Errorf("view1: %+v", s)
	}
	// Its columns are nullable, a NOT NULL table's under it too.
	if f, ok := got["view1"].Schema.FieldsByName("id"); !ok || !f[0].Nullable {
		t.Errorf("view1.id: %v", f)
	}
	if s := got["captured"]; slices.Contains(s.SyncModes, source.CDC) {
		t.Errorf("captured offers cdc with change tracking: %+v", s)
	}
	cat, err = e.source(`, "cdcMethod": "change_data_capture"`).Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range cat.Streams {
		if want := s.Name == "captured"; slices.Contains(s.SyncModes, source.CDC) != want || (s.Replay == source.Superset) != want {
			t.Errorf("with change data capture, %s offers cdc: %v, and replays %v", s.Name, s.SyncModes, s.Replay)
		}
	}
	// A column is nullable unless it's NOT NULL; a cdc delete's row has
	// nulls in the columns outside the key.
	if f, ok := got["loose"].Schema.FieldsByName("w"); !ok || f[0].Nullable {
		t.Errorf("loose.w: %v", f)
	}
}

func TestSourceConfiguredTables(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.a (id int PRIMARY KEY)")
	e.exec("CREATE TABLE $schema.b (id int PRIMARY KEY)")
	cat, err := e.source(`, "tables": ["b"]`).Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Streams) != 1 || cat.Streams[0].Name != "b" {
		t.Errorf("streams %v, want b", cat.Streams)
	}
	if _, err := e.source(`, "tables": ["missing"]`).Discover(t.Context()); err == nil {
		t.Error("a missing table discovered")
	}
}

func TestSourceFullRefreshResumes(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (a int, b nvarchar(10), v int, PRIMARY KEY (a, b))")
	e.exec("INSERT INTO $schema.users SELECT n / 3, CHAR(97 + n % 3), n FROM (SELECT TOP (25) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) - 1 AS n FROM sys.objects) x")
	e.exec("CREATE TABLE $schema.loose (v int)")
	e.exec("INSERT INTO $schema.loose VALUES (1), (2), (NULL)")
	s := e.source(`, "chunkRows": 4`)
	c := read(t, s, source.FullRefresh, map[string]string{"users": "", "loose": ""})
	if got := c.column("users", "v"); len(got) != 25 || got[0] != "0" || got[24] != "24" {
		t.Fatalf("users: %v", got)
	}
	if got := c.column("loose", "v"); len(got) != 3 {
		t.Errorf("loose: %v", got)
	}
	// From the third resume point, the rest.
	third := c.states["users"][2]
	c2 := read(t, s, source.FullRefresh, map[string]string{"users": third})
	if got := c2.column("users", "v"); len(got) != 13 || got[0] != "12" {
		t.Errorf("resumed: %v", got)
	}
}

// A read resumed after its table gained or lost its primary key goes on in
// the order its state was read in, or fails: a table read to its end
// without one, then given one, isn't read again; one read part way in its
// key's order, whose key is then dropped or made of another column, fails
// with POSITION_LOST rather than reading it again from the start or from a
// key of other columns, and so does a read again to such a key from no state.
func TestReadOrderChanges(t *testing.T) {
	for name, mode := range map[string]source.SyncMode{"full": source.FullRefresh, "incremental": source.Incremental} {
		t.Run(name, func(t *testing.T) {
			e := newSourceEnv(t)
			s := e.source(`, "chunkRows": 2`)
			read := func(table, state, until string) (*splitCapture, error) {
				cs := source.ConfiguredStream{Name: table, SyncMode: mode}
				if mode == source.Incremental {
					cs.Cursor = "at"
				}
				if state != "" {
					cs.State = []byte(state)
				}
				if until != "" {
					cs.Until = []byte(until)
				}
				c := &splitCapture{Col: "id"}
				return c, s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c)
			}
			resumes := func(c *splitCapture) []string {
				var out []string
				for _, ev := range c.Of(source.WholeStream) {
					if ev.Resume {
						out = append(out, ev.State)
					}
				}
				return out
			}
			e.exec("CREATE TABLE $schema.loose (id int NOT NULL, k int NOT NULL, at datetime2)")
			e.exec("CREATE TABLE $schema.keyed (id int NOT NULL CONSTRAINT pk_keyed PRIMARY KEY, k int NOT NULL, at datetime2)")
			for _, table := range []string{"loose", "keyed"} {
				e.exec("INSERT INTO $schema." + table + " SELECT n, 10 - n, DATEADD(day, n, '2026-01-01') FROM (SELECT TOP (5) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x")
			}

			first, err := read("loose", "", "")
			if err != nil {
				t.Fatal(err)
			}
			done := resumes(first)
			if len(done) != 1 || !strings.Contains(done[0], `"done":true`) {
				t.Fatalf("read without a key: resume points %v", done)
			}
			e.exec("ALTER TABLE $schema.loose ADD CONSTRAINT pk_loose PRIMARY KEY (id)")
			again, err := read("loose", done[0], "")
			if ev := again.Of(source.WholeStream); err != nil || len(again.Values()) != 0 || len(ev) == 0 || !ev[len(ev)-1].Complete {
				t.Errorf("from %s, once the table has a key: %v, read %v, events %+v", done[0], err, again.Values(), ev)
			}

			first, err = read("keyed", "", "")
			if err != nil {
				t.Fatal(err)
			}
			mid := resumes(first)[0]
			e.exec("ALTER TABLE $schema.keyed DROP CONSTRAINT pk_keyed")
			c, err := read("keyed", mid, "")
			if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
				t.Errorf("from %s, once the table has no key: %v, read %v", mid, err, c.Values())
			}
			e.exec("ALTER TABLE $schema.keyed ADD CONSTRAINT pk_keyed_k PRIMARY KEY (k)")
			c, err = read("keyed", mid, "")
			if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
				t.Errorf("from %s, once the table's key is another column: %v, read %v", mid, err, c.Values())
			}
			c, err = read("keyed", "", mid)
			if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
				t.Errorf("from no state to %s, once the table's key is another column: %v, read %v", mid, err, c.Values())
			}
		})
	}
}

// Every type a column can have, read with its nulls and extremes.
func TestSourceTypes(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TYPE $schema.code FROM nvarchar(5) NOT NULL")
	e.exec(`CREATE TABLE $schema.kinds (id int PRIMARY KEY,
		bt bit, ti tinyint, si smallint, i int, bi bigint,
		d decimal(38, 9), n numeric(5, 0), m money, sm smallmoney, r real, f float,
		dt date, t0 time(0), t7 time(7), d1 datetime, sd smalldatetime, d2 datetime2(7), d3 datetime2(3), o datetimeoffset(7),
		c char(3), vc varchar(10) COLLATE Latin1_General_CI_AS, vm varchar(max), nc nchar(2), nv nvarchar(20), nm nvarchar(max),
		tx text, ntx ntext,
		b binary(4), vb varbinary(10), vbm varbinary(max), img image,
		g uniqueidentifier, x xml, sv sql_variant, h hierarchyid, geo geography, gm geometry,
		rv rowversion, cc AS (si + 1), cp AS (CONVERT(nvarchar(11), i)) PERSISTED, code $schema.code)`)
	e.exec(`INSERT INTO $schema.kinds (id, bt, ti, si, i, bi, d, n, m, sm, r, f, dt, t0, t7, d1, sd, d2, d3, o, c, vc, vm, nc, nv, nm, tx, ntx,
		b, vb, vbm, img, g, x, sv, h, geo, gm, code) VALUES
		(1, 1, 255, 32767, 2147483647, 9223372036854775807,
		 99999999999999999999999999999.999999999, 99999, 922337203685477.5807, 214748.3647, 3.4e38, 1.7976931348623157e308,
		 '9999-12-31', '23:59:59', '23:59:59.9999999', '9999-12-31 23:59:59.997', '2079-06-06 23:59', '9999-12-31 23:59:59.9999999', '2026-11-16 10:00:00.123',
		 '9999-12-31 23:59:59.9999999 +14:00',
		 'abc', 'Ünïcode', REPLICATE(CONVERT(varchar(max), 'x'), 9000), N'語', N'日本語 ✓', REPLICATE(CONVERT(nvarchar(max), N'é'), 5000),
		 'text', N'ntext ✓',
		 0x00ff10ab, 0x, 0x0102, 0xcafe,
		 '6F9619FF-8B86-D011-B42D-00C04FC964FF', '<a b="1">x</a>', CONVERT(sql_variant, CONVERT(decimal(5, 2), 12.5)),
		 hierarchyid::Parse('/1/2/'), geography::Point(47.65, -122.34, 4326), geometry::STGeomFromText('LINESTRING (0 0, 1 1)', 0), N'ab'),
		(2, 0, 0, -32768, -2147483648, -9223372036854775808,
		 -99999999999999999999999999999.999999999, -99999, -922337203685477.5808, -214748.3648, -1.18e-38, -2.23e-308,
		 '0001-01-01', '00:00:00', '00:00:00', '1753-01-01 00:00:00', '1900-01-01 00:00', '0001-01-01 00:00:00', '1970-01-01', '0001-01-01 00:00:00 -14:00',
		 '', '', '', N'', N'', N'', '', N'',
		 0x00, 0x00, 0x, 0x,
		 '00000000-0000-0000-0000-000000000000', '', CONVERT(sql_variant, N'text'),
		 hierarchyid::GetRoot(), NULL, NULL, N''),
		(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, N'x')`)
	e.exec(`INSERT INTO $schema.kinds (id, sv, code) VALUES (4, CONVERT(sql_variant, CONVERT(datetime2, '2026-01-02 03:04:05.5')), N'y'),
		(5, CONVERT(sql_variant, 0x0a0b), N'z'), (6, CONVERT(sql_variant, 1.5e0), N'w'), (7, CONVERT(sql_variant, 42), N'v')`)
	e.exec(`INSERT INTO $schema.kinds (id, d2, o, t7, code) VALUES (8, '2026-01-02 03:04:05.1234567', '2026-01-02 03:04:05.1234567 -05:30', '01:02:03.1234567', N'u')`)
	s := e.source("")
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]string{}
	for _, f := range cat.Streams[0].Schema.Fields() {
		types[f.Name] = f.Type.String()
	}
	wantTypes := map[string]string{
		"bt": "bool", "ti": "int16", "si": "int16", "i": "int32", "bi": "int64",
		"d": "decimal(38, 9)", "n": "decimal(5, 0)", "m": "decimal(19, 4)", "sm": "decimal(10, 4)", "r": "float32", "f": "float64",
		"dt": "date32", "t0": "time64[us]", "t7": "time64[us]", "d1": "timestamp[us]", "sd": "timestamp[us]", "d2": "timestamp[us]",
		"d3": "timestamp[us]", "o": "timestamp[us, tz=UTC]",
		"c": "utf8", "vc": "utf8", "vm": "utf8", "nc": "utf8", "nv": "utf8", "nm": "utf8", "tx": "utf8", "ntx": "utf8",
		"b": "binary", "vb": "binary", "vbm": "binary", "img": "binary", "g": "extension<arrow.uuid>", "x": "utf8", "sv": "utf8",
		"h": "utf8", "geo": "utf8", "gm": "utf8", "rv": "binary", "cc": "int32", "cp": "utf8", "code": "utf8",
	}
	for c, want := range wantTypes {
		if types[c] != want {
			t.Errorf("%s is %s, want %s", c, types[c], want)
		}
	}
	got := read(t, s, source.FullRefresh, map[string]string{"kinds": ""})
	want := map[string][]string{
		"bt": {"true", "false", "(null)", "(null)", "(null)", "(null)", "(null)"}, "ti": {"255", "0", "(null)"},
		"si": {"32767", "-32768", "(null)"}, "i": {"2147483647", "-2147483648", "(null)"},
		"bi": {"9223372036854775807", "-9223372036854775808", "(null)"},
		"d":  {"99999999999999999999999999999.999999999", "-99999999999999999999999999999.999999999", "(null)"},
		"n":  {"99999", "-99999", "(null)"}, "m": {"922337203685477.5807", "-922337203685477.5808", "(null)"},
		"sm": {"214748.3647", "-214748.3648", "(null)"}, "r": {"3.4e+38", "-1.18e-38", "(null)"},
		"f": {"1.7976931348623157e+308", "-2.23e-308", "(null)"}, "dt": {"9999-12-31", "0001-01-01", "(null)"},
		"t0": {"23:59:59.000000", "00:00:00.000000", "(null)"},
		"t7": {"23:59:59.999999", "00:00:00.000000", "(null)"},
		"d1": {"9999-12-31T23:59:59.997", "1753-01-01T00:00:00", "(null)"},
		"sd": {"2079-06-06T23:59:00", "1900-01-01T00:00:00", "(null)"},
		"d3": {"2026-11-16T10:00:00.123", "1970-01-01T00:00:00", "(null)"}, "c": {"abc", "   ", "(null)"},
		"vc": {"Ünïcode", "", "(null)"}, "nc": {"語 ", "  ", "(null)"}, "nv": {"日本語 ✓", "", "(null)"},
		"tx": {"text", "", "(null)"}, "ntx": {"ntext ✓", "", "(null)"}, "b": {"AP8Qqw==", "AAAAAA==", "(null)"},
		"vb": {"", "AA==", "(null)"}, "vbm": {"AQI=", "", "(null)"}, "img": {"yv4=", "", "(null)"},
		"g":  {"6f9619ff-8b86-d011-b42d-00c04fc964ff", "00000000-0000-0000-0000-000000000000", "(null)"},
		"x":  {`<a b="1">x</a>`, "", "(null)"},
		"sv": {"12.50", "text", "(null)", "2026-01-02T03:04:05.5000000", "0x0A0B", "1.5000000000000000e+000", "42"},
		"h":  {"/1/2/", "/", "(null)"}, "geo": {"POINT (-122.34 47.65)", "(null)", "(null)"},
		"gm": {"LINESTRING (0 0, 1 1)", "(null)", "(null)"}, "cc": {"32768", "-32767", "(null)"},
		"cp": {"2147483647", "-2147483648", "(null)"}, "code": {"ab", "", "x"},
	}
	for c, w := range want {
		g := got.column("kinds", c)
		if len(g) < len(w) || !slices.Equal(g[:len(w)], w) {
			t.Errorf("%s = %q, want %q", c, g, w)
		}
	}
	if g := got.column("kinds", "vm"); len(g) < 2 || len(g[0]) != 9000 || g[1] != "" {
		t.Errorf("vm has %d values, the first %d long", len(g), len(g[0]))
	}
	if g := got.column("kinds", "nm"); len(g) < 1 || g[0] != strings.Repeat("é", 5000) {
		t.Errorf("nm is wrong")
	}
	if g := got.column("kinds", "rv"); len(g) < 1 || g[0] == "(null)" {
		t.Errorf("rv = %v", g)
	}
	// datetime2(7) and datetimeoffset(7), declared in microseconds, arrive
	// as text with their 100 ns digit, over their whole range, for the engine
	// to parse; time(7) arrives in microseconds, as the engine keeps times.
	for c, w := range map[string][]string{
		"d2": {"9999-12-31T23:59:59.9999999", "0001-01-01T00:00:00.0000000", "(null)", "(null)", "(null)", "(null)", "(null)", "2026-01-02T03:04:05.1234567"},
		"o":  {"9999-12-31T09:59:59.9999999Z", "0001-01-01T14:00:00.0000000Z", "(null)", "(null)", "(null)", "(null)", "(null)", "2026-01-02T08:34:05.1234567Z"},
		"t7": {"23:59:59.999999", "00:00:00.000000", "(null)", "(null)", "(null)", "(null)", "(null)", "01:02:03.123456"},
	} {
		if g := got.column("kinds", c); !slices.Equal(g, w) {
			t.Errorf("%s = %q, want %q", c, g, w)
		}
	}
}

// A column added since the run's Discover fails a full or incremental read,
// retryably, as it does a cdc read; the columns it declared read.
func TestSourceUndeclaredColumn(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, at datetime2(7))")
	e.exec("INSERT INTO $schema.t VALUES (1, '2026-01-02')")
	s := e.source("")
	for _, cs := range []source.ConfiguredStream{
		{Name: "t", SyncMode: source.FullRefresh, Columns: []string{"id"}},
		{Name: "t", SyncMode: source.Incremental, Cursor: "id", Columns: []string{"id"}},
	} {
		c := &capturer{}
		t.Cleanup(c.release)
		if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c); codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
			t.Errorf("%v read with at undeclared: %v", cs.SyncMode, err)
		}
	}
	got := readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.FullRefresh, Columns: []string{"id", "at"}}}})
	if v := got.column("t", "id"); !slices.Equal(v, []string{"1"}) {
		t.Errorf("declared: %v", v)
	}
}

// A primary key with a sql_variant column isn't the stream's: SQL Server
// compares a variant's type family before its value, so 1 and N'1' are two
// keys, which arrive as the same text. The table is read as one without a
// key, whole, which a read in key order by that text couldn't: past a
// number it read its first chunk again and again, and past a binary it
// ended.
func TestVariantKey(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.vk (k sql_variant PRIMARY KEY, v int)")
	e.exec("INSERT INTO $schema.vk VALUES (CONVERT(sql_variant, 1), 1), (CONVERT(sql_variant, N'1'), 2), (CONVERT(sql_variant, 0x01), 3), (CONVERT(sql_variant, 0x02), 4), (CONVERT(sql_variant, 2), 5)")
	e.exec("ALTER TABLE $schema.vk ENABLE CHANGE_TRACKING")
	s := e.source(`, "chunkRows": 2`)
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if st := cat.Streams[0]; len(st.PrimaryKey) != 0 || st.SplitsByKey || slices.Contains(st.SyncModes, source.CDC) {
		t.Errorf("vk: %+v", st)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	c := &capturer{}
	t.Cleanup(c.release)
	err = s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "vk", SyncMode: source.FullRefresh}}}, c)
	got := c.column("vk", "v")
	slices.Sort(got)
	if err != nil || !slices.Equal(got, []string{"1", "2", "3", "4", "5"}) {
		t.Errorf("read %d rows, %v", len(got), err)
	}
	pre := statuses(t, s.Preflight(source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "vk", SyncMode: source.CDC}}}))
	wantStatus(t, pre, "stream.vk.change-tracking", "failed: ["+e.schema+"].[vk]'s primary key has a sql_variant column")
}

// A sql_variant's text and binary arrive whole, past nvarchar(4000).
func TestSourceLongVariants(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.v (id int PRIMARY KEY, sv sql_variant)")
	e.exec(`INSERT INTO $schema.v VALUES (1, CONVERT(sql_variant, CONVERT(varchar(8000), REPLICATE('a', 5000)))),
		(2, CONVERT(sql_variant, CONVERT(varbinary(8000), REPLICATE('ab', 1500))))`)
	got := read(t, e.source(""), source.FullRefresh, map[string]string{"v": ""}).column("v", "sv")
	if want := []string{strings.Repeat("a", 5000), "0x" + strings.Repeat("6162", 1500)}; !slices.Equal(got, want) {
		t.Errorf("sv has %d values, of %v characters", len(got), func() (n []int) {
			for _, g := range got {
				n = append(n, len(g))
			}
			return n
		}())
	}
}

// A view is read in full or incrementally, a computed column has its
// value, and a system-versioned table its period columns, as does its
// history table, a stream of its own.
func TestViewsComputedAndTemporal(t *testing.T) {
	e := newSourceEnv(t)
	e.exec(`CREATE TABLE $schema.base (id int PRIMARY KEY, v int, at datetime2(3), w AS (v * 2),
		since datetime2 GENERATED ALWAYS AS ROW START NOT NULL, upto datetime2 GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME (since, upto))`)
	e.exec("ALTER TABLE $schema.base SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = $schema.base_history))")
	e.exec("CREATE VIEW $schema.vw AS SELECT id, v, w, at FROM $schema.base WHERE v > 0")
	e.exec("INSERT INTO $schema.base (id, v, at) VALUES (1, 1, '2026-01-01'), (2, 2, '2026-01-02'), (3, 0, '2026-01-03')")
	e.exec("UPDATE $schema.base SET v = 3 WHERE id = 2")
	s := e.source(`, "chunkRows": 1`)
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, cs := range cat.Streams {
		names = append(names, cs.Name)
	}
	if !slices.Equal(names, []string{"base", "base_history", "vw"}) {
		t.Fatalf("streams %v", names)
	}

	c := read(t, s, source.FullRefresh, map[string]string{"base": "", "base_history": "", "vw": ""})
	if got := c.rows("vw", "id", "v", "w"); !slices.Equal(got, []string{"1,1,2", "2,3,6"}) {
		t.Errorf("vw %v", got)
	}
	if got := c.rows("base", "id", "w", "upto"); !slices.Equal(got, []string{"1,2,9999-12-31T23:59:59.9999999", "2,6,9999-12-31T23:59:59.9999999", "3,0,9999-12-31T23:59:59.9999999"}) {
		t.Errorf("base %v", got)
	}
	since := c.column("base", "since")
	if got := c.rows("base_history", "id", "v", "w", "upto"); !slices.Equal(got, []string{"2,2,4," + since[1]}) {
		t.Errorf("base_history %v, the row's version ends where the current one starts, %s", got, since[1])
	}

	inc := source.ConfiguredStream{Name: "vw", SyncMode: source.Incremental, Cursor: "at"}
	first := readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{inc}})
	if got := first.column("vw", "id"); !slices.Equal(got, []string{"1", "2"}) {
		t.Fatalf("vw incremental %v", got)
	}
	e.exec("INSERT INTO $schema.base (id, v, at) VALUES (4, 4, '2026-01-04')")
	inc.State = []byte(first.next["vw"])
	next := readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{inc}})
	// From the cursor's last value, inclusive.
	if got := next.rows("vw", "id", "w"); !slices.Equal(got, []string{"2,6", "4,8"}) {
		t.Errorf("vw from %s: %v", inc.State, got)
	}

	// With change tracking, a change carries the computed column's value and
	// the period's start.
	e.exec("ALTER TABLE $schema.base ENABLE CHANGE_TRACKING")
	state := read(t, s, source.CDC, map[string]string{"base": ""}).last("base")
	e.exec("UPDATE $schema.base SET v = 5 WHERE id = 1")
	ch := read(t, s, source.CDC, map[string]string{"base": state})
	if got := changes(ch, "base", "id", "v", "w", "upto"); !slices.Equal(got, []string{"update,1,5,10,9999-12-31T23:59:59.9999999"}) {
		t.Errorf("changes %v", got)
	}
	if got := ch.column("base", "since"); len(got) != 1 || got[0] <= since[0] {
		t.Errorf("the changed row's period starts %v, before %s", got, since[0])
	}
}
