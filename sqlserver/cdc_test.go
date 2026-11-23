//go:build integration

package sqlserver

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/source"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// changes is a cdc read's records: op, then the columns.
func changes(c *capturer, stream string, cols ...string) []string {
	return c.rows(stream, append([]string{"openetl.op"}, cols...)...)
}

// fold merges cdc records of a table (id int, v int), as "op,id,v,seq", as
// the destination's merge does: each key's version with the highest seq, the
// later of equal ones, unless it's a delete. It gives the rows left, as
// "id,v", sorted.
func fold(records ...[]string) []string {
	type version struct {
		op, v string
		seq   *big.Int
	}
	latest := map[string]version{}
	for _, rs := range records {
		for _, r := range rs {
			f := strings.Split(r, ",")
			seq, _ := new(big.Int).SetString(f[3], 10)
			if l, ok := latest[f[1]]; ok && l.seq.Cmp(seq) > 0 {
				continue
			}
			latest[f[1]] = version{op: f[0], v: f[2], seq: seq}
		}
	}
	var out []string
	for id, v := range latest {
		if v.op != source.OpDelete {
			out = append(out, id+","+v.v)
		}
	}
	slices.Sort(out)
	return out
}

// table is t's rows, as "id,v", sorted.
func (e *sourceEnv) table() []string {
	e.t.Helper()
	rows, err := e.db.QueryContext(e.t.Context(), "SELECT CONCAT(id, ',', v) FROM "+quote(e.schema)+".t")
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, r)
	}
	slices.Sort(out)
	return out
}

func codeOf(err error) protocolv1.ErrorCode {
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if errors.As(err, &coded) {
		return coded.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

func readErr(t *testing.T, s *Source, stream, state string) error {
	t.Helper()
	c := &capturer{}
	t.Cleanup(c.release)
	return s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.CDC, State: []byte(state)}}}, c)
}

func TestCDC(t *testing.T) {
	for _, method := range []string{methodTracking, methodCapture} {
		t.Run(method, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v nvarchar(10), w AS (CONCAT(v, '!')))")
			e.exec("INSERT INTO $schema.t (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
			harvested := func() {}
			if method == methodTracking {
				e.exec("ALTER TABLE $schema.t ENABLE CHANGE_TRACKING")
			} else {
				e.capture("t")
				harvested = e.harvested
			}
			harvested()
			s := e.source(`, "snapshotChunkRows": 2, "chunkRows": 2, "cdcMethod": "` + method + `"`)
			c := read(t, s, source.CDC, map[string]string{"t": ""})
			if got := changes(c, "t", "id", "v", "w"); !slices.Equal(got, []string{"insert,1,a,a!", "insert,2,b,b!", "insert,3,c,c!"}) {
				t.Fatalf("snapshot %v", got)
			}
			// Two chunks, then the changes: a resume point after each.
			if n := len(c.states["t"]); n < 2 {
				t.Errorf("%d resume points", n)
			}
			state := c.last("t")

			e.exec("UPDATE $schema.t SET v = 'b2' WHERE id = 2")
			e.exec("DELETE FROM $schema.t WHERE id = 3")
			e.exec("INSERT INTO $schema.t (id, v) VALUES (4, 'd'), (5, 'e')")
			harvested()
			c = read(t, s, source.CDC, map[string]string{"t": state})
			got := changes(c, "t", "id", "v", "w")
			want := []string{"update,2,b2,b2!", "delete,3,(null),(null)", "insert,4,d,d!", "insert,5,e,e!"}
			if method == methodCapture {
				// Every change has its whole row; a computed column's value
				// isn't in the change tables, so it's unchanged.
				want = []string{"update,2,b2,(null)", "delete,3,c,(null)", "insert,4,d,(null)", "insert,5,e,(null)"}
				if u := c.column("t", "openetl.unchanged"); !slices.Equal(u, []string{`["w"]`, `["w"]`, `["w"]`, `["w"]`}) {
					t.Errorf("unchanged %v", u)
				}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("changes %v, want %v", got, want)
			}
			// With change tracking, the version of each key's change, which
			// one transaction's changes share; with change data capture, a
			// commit LSN and place, each its own.
			seqs := c.column("t", "openetl.seq")
			if !slices.IsSortedFunc(seqs, func(a, b string) int {
				x, _ := new(big.Int).SetString(a, 10)
				y, _ := new(big.Int).SetString(b, 10)
				return x.Cmp(y)
			}) || method == methodCapture && len(slices.Compact(slices.Clone(seqs))) != len(seqs) {
				t.Errorf("seqs %v", seqs)
			}
			// From the first resume point of the changes, the rest of them,
			// exactly.
			first := c.states["t"][0]
			c2 := read(t, s, source.CDC, map[string]string{"t": first})
			rest := changes(c2, "t", "id", "v", "w")
			if len(rest) == 0 || len(rest) >= len(want) || !slices.Equal(rest, want[len(want)-len(rest):]) {
				t.Errorf("from %s: %v", first, rest)
			}
			// A read from the end has nothing more.
			c3 := read(t, s, source.CDC, map[string]string{"t": c.last("t")})
			if got := changes(c3, "t", "id"); len(got) != 0 {
				t.Errorf("again: %v", got)
			}
		})
	}
}

// Rows changed while the snapshot is read, before and after the chunk that
// has them, arrive once each: the snapshot leaves out the rows changed after
// its position, which the changes then carry, and merged by seq they land as
// the table has them.
func TestCDCSnapshotHandoff(t *testing.T) {
	for _, method := range []string{methodTracking, methodCapture} {
		t.Run(method, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v int)")
			e.exec("INSERT INTO $schema.t SELECT n, n FROM (SELECT TOP (6) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x")
			if method == methodTracking {
				e.exec("ALTER TABLE $schema.t ENABLE CHANGE_TRACKING")
			} else {
				e.capture("t")
				e.harvested()
			}
			s := e.source(`, "snapshotChunkRows": 2, "cdcMethod": "` + method + `"`)
			c := read(t, s, source.CDC, map[string]string{"t": ""})
			// The state after the first chunk, ids 1 and 2.
			mid := c.states["t"][0]
			if !strings.Contains(mid, `"snapshot":{"after":["2"]}`) {
				t.Fatalf("first resume point %s", mid)
			}
			e.exec("UPDATE $schema.t SET v = 10 WHERE id = 1")
			e.exec("UPDATE $schema.t SET v = 50 WHERE id = 5")
			e.exec("DELETE FROM $schema.t WHERE id = 6")
			if method == methodCapture {
				e.harvested()
			}
			first := changes(c, "t", "id", "v", "openetl.seq")[:2]
			c = read(t, s, source.CDC, map[string]string{"t": mid})
			got := changes(c, "t", "id", "v")
			want := []string{"insert,3,3", "insert,4,4", "update,1,10", "update,5,50", "delete,6,(null)"}
			if method == methodCapture {
				want[4] = "delete,6,6"
			}
			if !slices.Equal(got, want) {
				t.Errorf("after the first chunk: %v, want %v", got, want)
			}
			if got, want := fold(first, changes(c, "t", "id", "v", "openetl.seq")), e.table(); !slices.Equal(got, want) {
				t.Errorf("merged by seq: %v, the table %v", got, want)
			}
		})
	}
}

// Updates of the key land, merged by seq, as the table has them: keys
// shifted, swapped and moved, and a key freed and taken again in one
// transaction. Change data capture records each as a delete and an insert,
// all of a statement's deletes first, and an insert shares its __$seqval
// with the delete of the row it was, which in a heap isn't the delete of
// its key.
func TestCDCKeyChanges(t *testing.T) {
	for _, method := range []string{methodTracking, methodCapture} {
		for name, ddl := range map[string]string{
			"clustered": "CREATE TABLE $schema.t (id int PRIMARY KEY, v int)",
			"heap":      "CREATE TABLE $schema.t (id int NOT NULL, v int, CONSTRAINT pk_t PRIMARY KEY NONCLUSTERED (id))",
		} {
			t.Run(method+"/"+name, func(t *testing.T) {
				e := newSourceEnv(t)
				e.exec(ddl)
				e.exec("INSERT INTO $schema.t VALUES (1, 1), (2, 2), (3, 3)")
				harvested := func() {}
				if method == methodTracking {
					e.exec("ALTER TABLE $schema.t ENABLE CHANGE_TRACKING")
				} else {
					e.capture("t")
					harvested = e.harvested
				}
				harvested()
				s := e.source(`, "cdcMethod": "` + method + `"`)
				snap := read(t, s, source.CDC, map[string]string{"t": ""})
				e.exec("UPDATE $schema.t SET id = id + 1")
				e.exec("UPDATE $schema.t SET id = CASE id WHEN 2 THEN 3 WHEN 3 THEN 2 ELSE id END")
				e.exec("UPDATE $schema.t SET id = 10 WHERE id = 4")
				e.exec("BEGIN TRAN; DELETE FROM $schema.t WHERE id = 10; INSERT INTO $schema.t VALUES (10, 99); UPDATE $schema.t SET id = 11 WHERE id = 10; INSERT INTO $schema.t VALUES (10, 100); COMMIT")
				// __$command_id counts through a transaction, not a statement.
				e.exec("BEGIN TRAN; INSERT INTO $schema.t VALUES (20, 20), (21, 21), (22, 22); DELETE FROM $schema.t WHERE id = 22; UPDATE $schema.t SET v = 210 WHERE id = 21; INSERT INTO $schema.t VALUES (22, 220); COMMIT")
				harvested()
				c := read(t, s, source.CDC, map[string]string{"t": snap.last("t")})
				got := fold(changes(snap, "t", "id", "v", "openetl.seq"), changes(c, "t", "id", "v", "openetl.seq"))
				if want := e.table(); !slices.Equal(got, want) {
					t.Errorf("merged by seq: %v, the table %v; changes %v", got, want, changes(c, "t", "id", "v", "openetl.seq"))
				}
			})
		}
	}
}

// A capture instance made for a new column is read from its start, and the
// old one up to it.
func TestCaptureSchemaChange(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v int)")
	e.capture("t")
	e.harvested()
	s := e.source(`, "cdcMethod": "change_data_capture"`)
	state := read(t, s, source.CDC, map[string]string{"t": ""}).last("t")
	e.exec("INSERT INTO $schema.t VALUES (1, 1)")
	e.exec("ALTER TABLE $schema.t ADD x int")
	e.exec("INSERT INTO $schema.t VALUES (2, 2, 20)")
	e.exec("EXEC sys.sp_cdc_enable_table @source_schema = @p1, @source_name = N't', @role_name = NULL, @capture_instance = @p2", e.schema, e.schema+"_t_v2")
	e.exec("INSERT INTO $schema.t VALUES (3, 3, 30)")
	e.harvested()
	s = e.source(`, "cdcMethod": "change_data_capture"`)
	c := read(t, s, source.CDC, map[string]string{"t": state})
	// The old instance has no x; the new one from its start.
	got := changes(c, "t", "id", "v", "x")
	if !slices.Equal(got, []string{"insert,1,1,(null)", "insert,2,2,(null)", "insert,3,3,30"}) {
		t.Errorf("changes %v", got)
	}
}

// A second capture instance whose start the capture job hasn't established
// yet, its start_lsn NULL, holds none of the changes: they're read from the
// old one.
func TestCaptureUnstartedInstance(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v int)")
	e.capture("t")
	e.harvested()
	s := e.source(`, "cdcMethod": "change_data_capture"`)
	state := read(t, s, source.CDC, map[string]string{"t": ""}).last("t")
	e.exec("INSERT INTO $schema.t VALUES (1, 1)")
	e.exec("EXEC sys.sp_cdc_enable_table @source_schema = @p1, @source_name = N't', @role_name = NULL, @capture_instance = @p2", e.schema, e.schema+"_t_v2")
	e.exec("INSERT INTO $schema.t VALUES (2, 2)")
	e.harvested()
	// As cdc.change_tables has it before the capture job has seen the
	// instance.
	e.exec("UPDATE cdc.change_tables SET start_lsn = NULL WHERE capture_instance = @p1", e.schema+"_t_v2")
	c := read(t, s, source.CDC, map[string]string{"t": state})
	if got := changes(c, "t", "id", "v"); !slices.Equal(got, []string{"insert,1,1", "insert,2,2"}) {
		t.Errorf("changes %v", got)
	}
}

func TestCDCPositionLost(t *testing.T) {
	t.Run(methodTracking, func(t *testing.T) {
		e := newSourceEnv(t)
		e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY)")
		e.exec("ALTER TABLE $schema.t ENABLE CHANGE_TRACKING")
		s := e.source("")
		state := read(t, s, source.CDC, map[string]string{"t": ""}).last("t")
		e.exec("INSERT INTO $schema.t VALUES (1)")
		// Truncating raises the table's oldest valid version past the state.
		e.exec("TRUNCATE TABLE $schema.t")
		if err := readErr(t, s, "t", state); codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
			t.Errorf("read = %v", err)
		}
		// The method changed under the state.
		if err := readErr(t, e.source(`, "cdcMethod": "change_data_capture"`), "t", state); codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
			t.Errorf("read with the other method = %v", err)
		}
	})
	t.Run(methodCapture, func(t *testing.T) {
		e := newSourceEnv(t)
		e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY)")
		e.capture("t")
		e.harvested()
		s := e.source(`, "cdcMethod": "change_data_capture"`)
		state := read(t, s, source.CDC, map[string]string{"t": ""}).last("t")
		e.exec("INSERT INTO $schema.t VALUES (1)")
		e.exec("INSERT INTO $schema.t VALUES (2)")
		e.harvested()
		// Clean up the change table past the state's position.
		e.exec("DECLARE @l binary(10) = sys.fn_cdc_get_max_lsn(); EXEC sys.sp_cdc_cleanup_change_table @capture_instance = @p1, @low_water_mark = @l, @threshold = 1000", e.schema+"_t")
		err := readErr(t, s, "t", state)
		if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
			t.Errorf("read = %v", err)
		}
		r, err := s.Retention(t.Context(), "", map[string][]byte{"t": []byte(state)}, nil)
		if err != nil || len(r) != 0 {
			t.Errorf("retention of a lost position: %v, %v", r, err)
		}
	})
}

// Cleanup during a read, past changes it may not have reached, fails the
// read before its next resume point, rather than moving past them.
func TestCaptureCleanupMidRead(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY)")
	e.capture("t")
	e.harvested()
	s := e.source(`, "cdcMethod": "change_data_capture", "chunkRows": 1`)
	state := read(t, s, source.CDC, map[string]string{"t": ""}).last("t")
	for i := range 4 {
		e.exec(fmt.Sprintf("INSERT INTO $schema.t VALUES (%d)", i))
	}
	e.harvested()
	var once sync.Once
	c := &capturer{resumed: func() {
		once.Do(func() {
			e.exec("DECLARE @l binary(10) = sys.fn_cdc_get_max_lsn(); EXEC sys.sp_cdc_cleanup_change_table @capture_instance = @p1, @low_water_mark = @l, @threshold = 1000", e.schema+"_t")
		})
	}}
	t.Cleanup(c.release)
	err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC, State: []byte(state)}}}, c)
	if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("read = %v, resume points %v", err, c.states["t"])
	}
}

func TestRetention(t *testing.T) {
	t.Run(methodTracking, func(t *testing.T) {
		e := newSourceEnv(t)
		e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY)")
		e.exec("ALTER TABLE $schema.t ENABLE CHANGE_TRACKING")
		s := e.source("")
		state := read(t, s, source.CDC, map[string]string{"t": ""}).last("t")
		e.exec("INSERT INTO $schema.t VALUES (1)")
		r, err := s.Retention(t.Context(), "", map[string][]byte{"t": []byte(state)}, nil)
		if err != nil || len(r) != 1 || r[0].Lag == nil || r[0].RetentionLeft == nil || *r[0].RetentionLeft <= 0 {
			t.Fatalf("retention %+v, %v", r, err)
		}
	})
	t.Run(methodCapture, func(t *testing.T) {
		e := newSourceEnv(t)
		e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY)")
		e.capture("t")
		e.harvested()
		cfg := `, "cdcMethod": "change_data_capture"`
		s := e.source(cfg)
		state := read(t, s, source.CDC, map[string]string{"t": ""}).last("t")
		e.exec("INSERT INTO $schema.t VALUES (1)")
		e.harvested()
		states := map[string][]byte{"t": []byte(state)}
		req := source.PreflightRequest{Interval: time.Hour, Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC}}}
		// The cleanup job's retention is in msdb, which the connectors'
		// login may not read: the lag alone, and the preflight skips it.
		r, err := s.Retention(t.Context(), "", states, nil)
		if err != nil || len(r) != 1 || r[0].Lag == nil || *r[0].Lag <= 0 || *r[0].Lag > time.Hour || r[0].RetentionLeft != nil {
			t.Fatalf("retention %+v, %v", r, err)
		}
		wantStatus(t, statuses(t, s.Preflight(req)), "cdc.retention", "skipped: reading the cleanup job's retention takes SELECT on msdb.dbo.cdc_jobs")
		// A login that may read it: what's left of the job's retention too.
		var keep int64
		if err := e.db.QueryRowContext(t.Context(), "SELECT retention FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'cleanup'").Scan(&keep); err != nil {
			t.Fatal(err)
		}
		retention := time.Duration(keep) * time.Minute
		s = e.sourceAs(e.msdbReader(), cfg)
		r, err = s.Retention(t.Context(), "", states, nil)
		if err != nil || len(r) != 1 || r[0].Lag == nil || *r[0].Lag <= 0 || r[0].RetentionLeft == nil ||
			*r[0].RetentionLeft >= retention || *r[0].RetentionLeft < retention-time.Hour {
			t.Fatalf("retention %+v, %v, of the job's %s", r, err, retention)
		}
		wantStatus(t, statuses(t, s.Preflight(req)), "cdc.retention", "passed")
		req.Interval = retention + time.Hour
		wantStatus(t, statuses(t, s.Preflight(req)), "cdc.retention", "warning: The database keeps changes for "+retention.String())
	})
}

// msdbReader makes a login that may read the schema, the change tables, and
// msdb's record of the capture jobs, which the connectors' login may not,
// for the test, and returns its dsn.
func (e *sourceEnv) msdbReader() string {
	e.t.Helper()
	name := "r_" + strings.ToLower(rand.Text())[:10]
	e.exec("CREATE LOGIN " + name + " WITH PASSWORD = 'OpenETL-test-1', CHECK_POLICY = OFF")
	e.t.Cleanup(func() {
		ctx := context.Background()
		_, _ = e.db.ExecContext(ctx, "EXEC msdb.sys.sp_executesql N'DROP USER IF EXISTS "+name+"'")
		_, _ = e.db.ExecContext(ctx, "DROP USER IF EXISTS "+name)
		_, _ = e.db.ExecContext(ctx, "DROP LOGIN "+name)
	})
	e.exec("CREATE USER " + name + " FOR LOGIN " + name)
	e.exec("GRANT SELECT ON SCHEMA::$schema TO " + name + "; GRANT SELECT ON SCHEMA::cdc TO " + name)
	e.exec("EXEC msdb.sys.sp_executesql N'CREATE USER " + name + " FOR LOGIN " + name + "; GRANT SELECT ON dbo.cdc_jobs TO " + name + "'")
	u, err := url.Parse(dsn())
	if err != nil {
		e.t.Fatal(err)
	}
	u.User = url.UserPassword(name, "OpenETL-test-1")
	return u.String()
}

// A snapshot's chunk read again, up to the resume point after it, has the
// rows the first read had, as the table has them then, as a full refresh's
// would: a read given an end leaves out no row changed since the snapshot's
// position, whose change past the end it doesn't read. So it has a row that
// a read without an end leaves to the changes too.
func TestSnapshotRedo(t *testing.T) {
	for _, method := range []string{methodTracking, methodCapture} {
		t.Run(method, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v int)")
			e.exec("INSERT INTO $schema.t VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)")
			if method == methodTracking {
				e.exec("ALTER TABLE $schema.t ENABLE CHANGE_TRACKING")
			} else {
				e.capture("t")
				e.harvested()
			}
			s := e.source(`, "snapshotChunkRows": 2, "cdcMethod": "` + method + `"`)
			states := read(t, s, source.CDC, map[string]string{"t": ""}).states["t"]
			from, until := states[0], states[1]
			e.exec("UPDATE $schema.t SET v = 30 WHERE id = 3")
			if method == methodCapture {
				e.harvested()
			}
			redo := readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC, State: []byte(from), Until: []byte(until)}}})
			if got := changes(redo, "t", "id", "v"); !slices.Equal(got, []string{"insert,3,30", "insert,4,4"}) {
				t.Errorf("redo from %s to %s: %v", from, until, got)
			}
			if got := redo.last("t"); got != until || redo.next["t"] != until {
				t.Errorf("redo ended at %s, completed at %s, want %s", got, redo.next["t"], until)
			}
			resumed := read(t, s, source.CDC, map[string]string{"t": from})
			if got := changes(resumed, "t", "id", "v"); !slices.Equal(got[:3], []string{"insert,4,4", "insert,5,5", "insert,6,6"}) || got[len(got)-1] != "update,3,30" {
				t.Errorf("read on from %s: %v", from, got)
			}
		})
	}
}

// A read again from no state, as of a new stream's first part, to a resume
// point past its snapshot, reads the snapshot with the seq of the position
// the first read's started at, then the changes from there up to the resume
// point: a row deleted since comes back as its delete. An end that doesn't
// say where its snapshot started fails with POSITION_LOST, as nothing else
// can tell; one in the snapshot still says it by its position.
func TestCDCRedoFromNoState(t *testing.T) {
	for _, method := range []string{methodTracking, methodCapture} {
		t.Run(method, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v int)")
			e.exec("INSERT INTO $schema.t VALUES (1, 1), (2, 2), (3, 3)")
			harvested := func() {}
			if method == methodTracking {
				e.exec("ALTER TABLE $schema.t ENABLE CHANGE_TRACKING")
			} else {
				e.capture("t")
				harvested = e.harvested
			}
			harvested()
			s := e.source(`, "snapshotChunkRows": 2, "cdcMethod": "` + method + `"`)
			first := read(t, s, source.CDC, map[string]string{"t": ""})
			snapSeq := first.column("t", "openetl.seq")[0]
			e.exec("UPDATE $schema.t SET v = 10 WHERE id = 1")
			e.exec("DELETE FROM $schema.t WHERE id = 2")
			e.exec("INSERT INTO $schema.t VALUES (4, 4)")
			harvested()
			second := read(t, s, source.CDC, map[string]string{"t": first.last("t")})
			until := second.last("t")
			redo := readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC, Until: []byte(until)}}})
			want := append([]string{"insert,1,10," + snapSeq, "insert,3,3," + snapSeq, "insert,4,4," + snapSeq}, changes(second, "t", "id", "v", "openetl.seq")...)
			if got := changes(redo, "t", "id", "v", "openetl.seq"); !slices.Equal(got, want) {
				t.Errorf("redo from no state to %s: %v, want %v", until, got, want)
			}
			if got := redo.last("t"); got != until || redo.next["t"] != until {
				t.Errorf("redo ended at %s, completed at %s, want %s", got, redo.next["t"], until)
			}

			var spelled map[string]any
			if err := json.Unmarshal([]byte(until), &spelled); err != nil {
				t.Fatal(err)
			}
			delete(spelled, "origin")
			old, err := json.Marshal(spelled)
			if err != nil {
				t.Fatal(err)
			}
			c := &capturer{}
			t.Cleanup(c.release)
			err = s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC, Until: old}}}, c)
			if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
				t.Errorf("redo from no state to %s, which doesn't say where its snapshot started: %v, read %v", old, err, changes(c, "t", "id", "v"))
			}

			inSnapshot := first.states["t"][0]
			redo = readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC, Until: []byte(inSnapshot)}}})
			if got := changes(redo, "t", "id", "v", "openetl.seq"); !slices.Equal(got, []string{"insert,1,10," + snapSeq}) || redo.next["t"] != inSnapshot {
				t.Errorf("redo from no state to %s: %v, completed at %s", inSnapshot, got, redo.next["t"])
			}
		})
	}
}

// A read asked to complete where a read completed, as a redo of a part of an
// epoch that ended there is, reads the changes up to that completion and
// none past it, and completes with its bytes: from the state the first read
// started at, and, a stream's first part, from no state, with the snapshot
// from where the first read's started.
func TestCDCCompleteAt(t *testing.T) {
	for _, method := range []string{methodTracking, methodCapture} {
		t.Run(method, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v int)")
			e.exec("INSERT INTO $schema.t VALUES (1, 1), (2, 2)")
			harvested := func() {}
			if method == methodTracking {
				e.exec("ALTER TABLE $schema.t ENABLE CHANGE_TRACKING")
			} else {
				e.capture("t")
				harvested = e.harvested
			}
			harvested()
			s := e.source(`, "cdcMethod": "` + method + `"`)
			first := read(t, s, source.CDC, map[string]string{"t": ""})
			snapSeq := first.column("t", "openetl.seq")[0]
			end1 := first.next["t"]
			e.exec("UPDATE $schema.t SET v = 10 WHERE id = 1")
			e.exec("INSERT INTO $schema.t VALUES (3, 3)")
			harvested()
			second := read(t, s, source.CDC, map[string]string{"t": end1})
			end2 := second.next["t"]
			e.exec("UPDATE $schema.t SET v = 20 WHERE id = 2")
			e.exec("INSERT INTO $schema.t VALUES (4, 4)")
			harvested()

			redo := readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC, State: []byte(end1), CompleteAt: []byte(end2)}}})
			if got, want := changes(redo, "t", "id", "v", "openetl.seq"), changes(second, "t", "id", "v", "openetl.seq"); !slices.Equal(got, want) {
				t.Errorf("from %s to its completion %s: %v, want %v", end1, end2, got, want)
			}
			if redo.next["t"] != end2 {
				t.Errorf("asked to complete at %s, completed at %s", end2, redo.next["t"])
			}

			redo = readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC, CompleteAt: []byte(end1)}}})
			for _, seq := range redo.column("t", "openetl.seq") {
				if seq != snapSeq {
					t.Errorf("from no state to its completion %s: a record with seq %s, past the snapshot's %s: %v", end1, seq, snapSeq, changes(redo, "t", "id", "v", "openetl.seq"))
					break
				}
			}
			if redo.next["t"] != end1 {
				t.Errorf("from no state, asked to complete at %s, completed at %s", end1, redo.next["t"])
			}
		})
	}
}

// Change data capture keeps every change: a read again up to a resume point
// has the same changes, with the same seqs, after the rows changed again.
func TestCaptureRedo(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v int)")
	e.exec("INSERT INTO $schema.t VALUES (1, 1), (2, 2)")
	e.capture("t")
	e.harvested()
	s := e.source(`, "cdcMethod": "change_data_capture"`)
	s1 := read(t, s, source.CDC, map[string]string{"t": ""}).last("t")
	e.exec("UPDATE $schema.t SET v = 10 WHERE id = 1")
	e.exec("UPDATE $schema.t SET v = 20 WHERE id = 2")
	e.harvested()
	first := read(t, s, source.CDC, map[string]string{"t": s1})
	want := changes(first, "t", "id", "v", "openetl.seq")
	if got := changes(first, "t", "id", "v"); !slices.Equal(got, []string{"update,1,10", "update,2,20"}) {
		t.Fatalf("first read %v", got)
	}
	s2 := first.last("t")
	e.exec("UPDATE $schema.t SET v = 100 WHERE id = 1")
	e.harvested()
	redo := readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC, State: []byte(s1), Until: []byte(s2)}}})
	if got := changes(redo, "t", "id", "v", "openetl.seq"); !slices.Equal(got, want) {
		t.Errorf("redo to %s: %v, want %v", s2, got, want)
	}
}

// Rows changed after the snapshot's position, before the capture job has
// harvested them, are in the snapshot as they are then, and arrive again as
// their changes once harvested, with higher seqs: none is lost, and merged
// by seq they land as the table has them.
func TestCaptureHandoffWindow(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v int)")
	e.exec("INSERT INTO $schema.t VALUES (1, 1), (2, 2), (3, 3), (4, 4)")
	e.capture("t")
	e.harvested()
	// The capture job is the database's: it runs again whatever happens here,
	// or the tests after this one wait for it in vain.
	t.Cleanup(func() {
		if err := startCapture(context.Background(), e.db); err != nil {
			t.Error(err)
		}
	})
	e.stopCapture()
	e.exec("UPDATE $schema.t SET v = 10 WHERE id = 1")
	e.exec("DELETE FROM $schema.t WHERE id = 2")
	e.exec("INSERT INTO $schema.t VALUES (5, 5)")
	e.exec("UPDATE $schema.t SET v = 40 WHERE id = 4")
	e.exec("UPDATE $schema.t SET v = 41 WHERE id = 4")
	s := e.source(`, "snapshotChunkRows": 2, "cdcMethod": "change_data_capture"`)
	snap := read(t, s, source.CDC, map[string]string{"t": ""})
	if got := changes(snap, "t", "id", "v"); !slices.Equal(got, []string{"insert,1,10", "insert,3,3", "insert,4,41", "insert,5,5"}) {
		t.Fatalf("snapshot %v", got)
	}
	if err := startCapture(t.Context(), e.db); err != nil {
		t.Fatal(err)
	}
	after := read(t, s, source.CDC, map[string]string{"t": snap.last("t")})
	if got := changes(after, "t", "id", "v"); !slices.Equal(got, []string{"update,1,10", "delete,2,2", "insert,5,5", "update,4,40", "update,4,41"}) {
		t.Fatalf("changes %v", got)
	}
	snapSeq, _ := new(big.Int).SetString(snap.column("t", "openetl.seq")[0], 10)
	for _, q := range after.column("t", "openetl.seq") {
		if seq, _ := new(big.Int).SetString(q, 10); seq.Cmp(snapSeq) <= 0 {
			t.Errorf("a change's seq %s, not above the snapshot's %s", q, snapSeq)
		}
	}
	if got, want := fold(changes(snap, "t", "id", "v", "openetl.seq"), changes(after, "t", "id", "v", "openetl.seq")), e.table(); !slices.Equal(got, want) {
		t.Errorf("merged by seq: %v, the table %v", got, want)
	}
}

// Change tracking keeps only each key's latest change: a read from a resume
// point brings each key changed since with its row as it is then, and a redo
// bounded at an end leaves out a key changed again past it, since its change
// is past the end. Its streams are declared superset; the runner fails such
// a redo, of fewer rows than the first read had, with POSITION_LOST.
func TestTrackingRedo(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v int)")
	e.exec("INSERT INTO $schema.t VALUES (1, 1), (2, 2)")
	e.exec("ALTER TABLE $schema.t ENABLE CHANGE_TRACKING")
	s := e.source("")
	s1 := read(t, s, source.CDC, map[string]string{"t": ""}).last("t")
	e.exec("UPDATE $schema.t SET v = 10 WHERE id = 1")
	e.exec("UPDATE $schema.t SET v = 20 WHERE id = 2")
	first := read(t, s, source.CDC, map[string]string{"t": s1})
	if got := changes(first, "t", "id", "v"); !slices.Equal(got, []string{"update,1,10", "update,2,20"}) {
		t.Fatalf("first read %v", got)
	}
	s2 := first.last("t")
	e.exec("UPDATE $schema.t SET v = 100 WHERE id = 1")
	redo := readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC, State: []byte(s1), Until: []byte(s2)}}})
	if got := changes(redo, "t", "id", "v"); !slices.Equal(got, []string{"update,2,20"}) {
		t.Errorf("redo to %s: %v", s2, got)
	}
	again := read(t, s, source.CDC, map[string]string{"t": s1})
	if got := changes(again, "t", "id", "v"); !slices.Equal(got, []string{"update,1,100", "update,2,20"}) {
		t.Errorf("read again from %s: %v", s1, got)
	}
	// A key sent by two polls before an end comes back once.
	e.exec("UPDATE $schema.t SET v = 30 WHERE id = 2")
	third := read(t, s, source.CDC, map[string]string{"t": s2})
	s3 := third.last("t")
	if got := changes(third, "t", "id", "v"); !slices.Equal(got, []string{"update,1,100", "update,2,30"}) {
		t.Fatalf("third read %v", got)
	}
	redo = readReq(t, s, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", SyncMode: source.CDC, State: []byte(s1), Until: []byte(s3)}}})
	if got := changes(redo, "t", "id", "v"); !slices.Equal(got, []string{"update,1,100", "update,2,30"}) {
		t.Errorf("redo to %s, of 4 changes read: %v", s3, got)
	}
}

// A column added to a tracked table arrives from the next read.
func TestTrackingSchemaChange(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v int)")
	e.exec("ALTER TABLE $schema.t ENABLE CHANGE_TRACKING")
	state := read(t, e.source(""), source.CDC, map[string]string{"t": ""}).last("t")
	e.exec("INSERT INTO $schema.t VALUES (1, 1)")
	e.exec("ALTER TABLE $schema.t ADD x int")
	e.exec("INSERT INTO $schema.t VALUES (2, 2, 20)")
	c := read(t, e.source(""), source.CDC, map[string]string{"t": state})
	if got := changes(c, "t", "id", "v", "x"); !slices.Equal(got, []string{"insert,1,1,(null)", "insert,2,2,20"}) {
		t.Errorf("changes %v", got)
	}
}
