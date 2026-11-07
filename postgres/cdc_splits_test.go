//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/openetl-io/openetl/sdk/go/source"
)

func (e *sourceEnv) readCDCLanes(s *Source, pipeline string, lanes int, states map[string]string) *capture {
	e.t.Helper()
	c := &capture{}
	e.t.Cleanup(c.release)
	req := cdcRequest(pipeline, false, states)
	req.Lanes = lanes
	if err := s.Read(e.t.Context(), req, c); err != nil {
		e.t.Fatal(err)
	}
	return c
}

func stateOf(t *testing.T, b string) cdcState {
	t.Helper()
	var st cdcState
	if err := json.Unmarshal([]byte(b), &st); err != nil {
		t.Fatalf("state %q: %v", b, err)
	}
	return st
}

// meetAt returns a hook for afterChunkSnapshot that holds each chunk until n
// have their snapshots at once, then runs then, once.
func meetAt(t *testing.T, n int, then func()) func() {
	t.Helper()
	var arrived atomic.Int64
	all := make(chan struct{})
	var once sync.Once
	return func() {
		if arrived.Add(1) == int64(n) {
			once.Do(func() {
				if then != nil {
					then()
				}
				close(all)
			})
		}
		select {
		case <-all:
		case <-time.After(30 * time.Second):
			t.Errorf("waited for %d chunks at a time", n)
		}
	}
}

// A snapshot not started is read in key ranges, a chunk of each at a time on
// a connection each, tracking every range's position; each row is read once.
func TestCDCSnapshotsInKeyRanges(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 4000) i")
	e.exec("ANALYZE $schema.users")
	app := e.schema
	sep := "?"
	if strings.Contains(dsn(), "?") {
		sep = "&"
	}
	cfg := fmt.Sprintf(`{"dsn": %q, "schema": %q, "snapshotChunkRows": 500, "publication": %q, "maxConnections": 3}`, dsn()+sep+"application_name="+app, e.schema, e.schema)
	s, err := NewSource([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	pipeline := "p" + e.schema
	t.Cleanup(func() {
		ctx := context.Background()
		if err := s.Teardown(ctx, pipeline); err != nil {
			t.Errorf("teardown: %v", err)
		}
		_, _ = e.conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{e.schema}.Sanitize())
	})
	var conns atomic.Int64
	afterChunkSnapshot = meetAt(t, 3, func() {
		var open int64
		if err := e.conn.QueryRow(t.Context(), "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND backend_type = 'client backend'", app).Scan(&open); err != nil {
			t.Error(err)
		}
		conns.Store(open)
	})
	t.Cleanup(func() { afterChunkSnapshot = nil })
	c := e.readCDCLanes(s, pipeline, 8, map[string]string{"users": ""})
	if got := conns.Load(); got != 3 {
		t.Errorf("%d connections reading, not counting the walsender's, want maxConnections", got)
	}
	got := c.changes(t, "users")
	if len(got) != 4000 {
		t.Errorf("%d rows", len(got))
	}
	seen := map[string]bool{}
	for _, ch := range got {
		if seen[ch.id] || ch.op != source.OpInsert {
			t.Errorf("row %+v, seen before: %v", ch, seen[ch.id])
		}
		seen[ch.id] = true
	}
	sameRows(t, merged(got), e.rows("users"))
	var inRanges int
	for _, l := range c.log {
		if !strings.HasPrefix(l, "users@") {
			continue
		}
		st := stateOf(t, strings.TrimPrefix(l, "users@"))
		if st.Snapshot == nil {
			continue
		}
		if len(st.Snapshot.Ranges) == 0 {
			t.Errorf("state without ranges: %s", l)
		}
		inRanges++
		for i, r := range st.Snapshot.Ranges {
			if i > 0 && r.Lo == nil {
				t.Errorf("a range without a lower bound: %s", l)
			}
		}
	}
	if inRanges == 0 {
		t.Errorf("no state in ranges: %q", c.log)
	}
	if st := stateOf(t, c.last("users")); st.Snapshot != nil {
		t.Errorf("completed at %s", c.last("users"))
	}
}

// A continuous read keeps one connection once the snapshot is read.
func TestCDCLetsConnectionsGoAfterTheSnapshot(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 1000) i")
	e.exec("ANALYZE $schema.users")
	app := e.schema
	sep := "?"
	if strings.Contains(dsn(), "?") {
		sep = "&"
	}
	s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q, "schema": %q, "snapshotChunkRows": 100, "publication": %q}`, dsn()+sep+"application_name="+app, e.schema, e.schema))
	if err != nil {
		t.Fatal(err)
	}
	pipeline := "p" + e.schema
	t.Cleanup(func() {
		ctx := context.Background()
		if err := s.Teardown(ctx, pipeline); err != nil {
			t.Errorf("teardown: %v", err)
		}
		_, _ = e.conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{e.schema}.Sanitize())
	})
	open := func() int64 {
		var n int64
		if err := e.conn.QueryRow(t.Context(), "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND backend_type = 'client backend'", app).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	var peak atomic.Int64
	afterChunkSnapshot = meetAt(t, 4, func() { peak.Store(open()) })
	t.Cleanup(func() { afterChunkSnapshot = nil })
	c := &capture{}
	t.Cleanup(c.release)
	ctx, cancel := context.WithCancel(t.Context())
	f := &following{capture: c, cancel: cancel, done: make(chan error, 1)}
	req := cdcRequest(pipeline, true, map[string]string{"users": ""})
	req.Lanes = 4
	go func() { f.done <- s.Read(ctx, req, c) }()
	f.await(t, "the snapshot", func() bool {
		st := f.last("users")
		return st != "" && stateOf(t, st).Snapshot == nil
	})
	f.await(t, "the connections to close", func() bool { return open() == 1 })
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 4 {
		t.Errorf("%d connections reading the snapshot", peak.Load())
	}
}

// Connections left over read other tables' chunks with the first's.
func TestCDCSnapshotsTablesAtOnce(t *testing.T) {
	e := newSourceEnv(t)
	for _, name := range []string{"a", "b"} {
		e.exec("CREATE TABLE $schema." + name + " (id int PRIMARY KEY, name text, doc text)")
		e.exec("INSERT INTO $schema." + name + " SELECT i, 'n' || i, NULL FROM generate_series(1, 30) i")
	}
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)
	afterChunkSnapshot = meetAt(t, 2, nil)
	t.Cleanup(func() { afterChunkSnapshot = nil })
	c := e.readCDCLanes(s, pipeline, 2, map[string]string{"a": "", "b": ""})
	for _, name := range []string{"a", "b"} {
		if got := c.changes(t, name); len(got) != 30 {
			t.Errorf("%s: %d rows", name, len(got))
		}
		sameRows(t, merged(c.changes(t, name)), e.rows(name))
	}
}

// Transactions that commit after every chunk's snapshot, before their WAL
// positions, aren't in the chunks: each one's change outlives its chunk's
// row, in each range, whichever the key moves to.
func TestCDCChunksMissATransactionBeforeTheirPositions(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'old', NULL FROM generate_series(1, 4000) i")
	e.exec("ANALYZE $schema.users")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 1000`)
	afterChunkSnapshot = meetAt(t, 4, func() {
		// An update in each range; a key moved between ranges and out of them;
		// a delete, and an insert below them all.
		e.exec("UPDATE $schema.users SET name = 'new' WHERE id IN (10, 1010, 2010, 3010)")
		e.exec("UPDATE $schema.users SET id = 3500 + 100000, name = 'moved' WHERE id = 3500")
		e.exec("UPDATE $schema.users SET id = -7, name = 'moved' WHERE id = 20")
		e.exec("DELETE FROM $schema.users WHERE id = 1020")
		e.exec("INSERT INTO $schema.users VALUES (-1, 'new', NULL)")
	})
	t.Cleanup(func() { afterChunkSnapshot = nil })
	c := e.readCDCLanes(s, pipeline, 4, map[string]string{"users": ""})
	got := c.changes(t, "users")
	ops := func(id string) []string {
		var out []string
		for _, ch := range got {
			if ch.id == id {
				out = append(out, ch.op+" "+ch.name)
			}
		}
		return out
	}
	for _, id := range []string{"10", "1010", "2010", "3010"} {
		if got := ops(id); !slices.Equal(got, []string{"insert old", "update new"}) {
			t.Errorf("id %s: %q", id, got)
		}
	}
	for _, id := range []string{"1020", "3500"} {
		if got := ops(id); !slices.Equal(got, []string{"insert old", "delete (null)"}) {
			t.Errorf("id %s: %q", id, got)
		}
	}
	sameRows(t, merged(got), e.rows("users"))
}

// Writers change the table throughout a continuous read in key ranges.
// Merged by key and seq, what's emitted is the table.
func TestCDCKeyRangesFollowConcurrentWrites(t *testing.T) {
	for run := range 3 {
		t.Run(fmt.Sprint(run), func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
			e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 3000) i")
			e.exec("ANALYZE $schema.users")
			s, pipeline := e.cdcSource(`, "snapshotChunkRows": 13`)
			c := &capture{}
			t.Cleanup(c.release)
			ctx, cancel := context.WithCancel(t.Context())
			f := &following{capture: c, cancel: cancel, done: make(chan error, 1)}
			req := cdcRequest(pipeline, true, map[string]string{"users": ""})
			req.Lanes = 4
			go func() { f.done <- s.Read(ctx, req, c) }()
			keepWriting(t, e, "users", 3200, 200)
			var end string
			if err := e.conn.QueryRow(t.Context(), "SELECT pg_current_wal_lsn()::text").Scan(&end); err != nil {
				t.Fatal(err)
			}
			endLSN, _ := pglogrepl.ParseLSN(end)
			f.await(t, "the read to catch up with "+end, func() bool {
				var st cdcState
				s := f.last("users")
				return s != "" && json.Unmarshal([]byte(s), &st) == nil && st.Snapshot == nil && pglogrepl.LSN(st.LSN) >= endLSN
			})
			if err := f.stop(); err != nil {
				t.Fatal(err)
			}
			sameRows(t, merged(c.changes(t, "users")), e.rows("users"))
		})
	}
}

// keepWriting makes changes across a table's ids, from several connections.
func keepWriting(t *testing.T, e *sourceEnv, name string, ids, changes int) {
	t.Helper()
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			conn, err := pgx.Connect(context.Background(), dsn())
			if err != nil {
				t.Error(err)
				return
			}
			defer func() { _ = conn.Close(context.Background()) }()
			table := pgx.Identifier{e.schema, name}.Sanitize()
			rng := mrand.New(mrand.NewPCG(uint64(w), 11))
			for i := range changes {
				id := rng.IntN(ids) + 1
				var q string
				switch rng.IntN(5) {
				case 0:
					q = fmt.Sprintf("DELETE FROM %s WHERE id = %d", table, id)
				case 1:
					q = fmt.Sprintf("INSERT INTO %s VALUES (%d, 'w%d-%d', NULL) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name", table, id, w, i)
				case 2:
					// A key changes, within or across ranges.
					q = fmt.Sprintf("UPDATE %s SET id = %d, name = 'm%d-%d' WHERE id = %d", table, rng.IntN(ids)+ids, w, i, id)
				default:
					q = fmt.Sprintf("UPDATE %s SET name = 'w%d-%d' WHERE id IN (%d, %d)", table, w, i, id, id+1)
				}
				if _, err := conn.Exec(context.Background(), q); err != nil {
					if pe, ok := errors.AsType[*pgconn.PgError](err); ok && (pe.Code == "40P01" || pe.Code == "23505") {
						continue
					}
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// cutCapture cancels a read at a chosen resume point, and keeps what's
// covered by the stream's last resume point, as a checkpoint would.
type cutCapture struct {
	*capture
	stream string
	cancel context.CancelFunc
	// Cancels at the first state it's true of.
	at func(state string) bool

	cut   int
	state string
}

func (c *cutCapture) ResumePoint(stream string, state []byte) error {
	if err := c.capture.ResumePoint(stream, state); err != nil {
		return err
	}
	if stream != c.stream {
		return nil
	}
	c.mu.Lock()
	c.cut, c.state = len(c.batches[stream]), string(state)
	c.mu.Unlock()
	if c.at(string(state)) {
		c.cancel()
	}
	return nil
}

// checkpointed drops what the last resume point doesn't cover.
func (c *cutCapture) checkpointed() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range c.batches[c.stream][c.cut:] {
		b.Release()
	}
	c.batches[c.stream] = c.batches[c.stream][:c.cut]
	return c.state
}

// A read restarted mid-snapshot goes on from every range's position, on fewer
// lanes or more: with nothing changing, every row is read exactly once.
func TestCDCRestartsMidSnapshotInKeyRanges(t *testing.T) {
	for name, lanes := range map[string][]int{"fewer": {4, 2}, "one": {4, 1}, "more": {2, 4}} {
		t.Run(name, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
			e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 4000) i")
			e.exec("ANALYZE $schema.users")
			s, pipeline := e.cdcSource(`, "snapshotChunkRows": 100`)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// Stop with two ranges part-way.
			first := &cutCapture{capture: &capture{}, stream: "users", cancel: cancel, at: func(state string) bool {
				st := stateOf(t, state)
				if st.Snapshot == nil {
					return false
				}
				n := 0
				for _, r := range st.Snapshot.Ranges {
					if r.After != nil {
						n++
					}
				}
				return n >= 2
			}}
			t.Cleanup(first.release)
			req := cdcRequest(pipeline, false, map[string]string{"users": ""})
			req.Lanes = lanes[0]
			if err := s.Read(ctx, req, first); !errors.Is(err, context.Canceled) {
				t.Fatalf("the first read: %v", err)
			}
			state := first.checkpointed()
			if st := stateOf(t, state); st.Snapshot == nil || len(st.Snapshot.Ranges) < 2 {
				t.Fatalf("stopped at %s", state)
			}
			rest := e.readCDCLanes(s, pipeline, lanes[1], map[string]string{"users": state})
			got := append(first.changes(t, "users"), rest.changes(t, "users")...)
			seen := map[string]int{}
			for _, ch := range got {
				seen[ch.id]++
			}
			if len(seen) != 4000 || len(got) != 4000 {
				t.Errorf("%d rows of %d ids", len(got), len(seen))
			}
			sameRows(t, merged(got), e.rows("users"))
			if st := stateOf(t, rest.last("users")); st.Snapshot != nil {
				t.Errorf("completed at %s", rest.last("users"))
			}
		})
	}
}

// With writers throughout and a restart mid-snapshot, the merge of what's
// checkpointed and the rest is the table.
func TestCDCRestartsMidSnapshotUnderWrites(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 3000) i")
	e.exec("ANALYZE $schema.users")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 20`)
	var writers sync.WaitGroup
	defer writers.Wait()
	writers.Go(func() { keepWriting(t, e, "users", 3200, 150) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var resumes atomic.Int64
	first := &cutCapture{capture: &capture{}, stream: "users", cancel: cancel, at: func(state string) bool {
		st := stateOf(t, state)
		return st.Snapshot != nil && resumes.Add(1) == 10
	}}
	t.Cleanup(first.release)
	req := cdcRequest(pipeline, true, map[string]string{"users": ""})
	req.Lanes = 4
	if err := s.Read(ctx, req, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("the first read: %v", err)
	}
	state := first.checkpointed()
	writers.Wait()
	rest := e.readCDCLanes(s, pipeline, 3, map[string]string{"users": state})
	sameRows(t, merged(append(first.changes(t, "users"), rest.changes(t, "users")...)), e.rows("users"))
}

// A state from before ranges goes on as one range, in the format it came in,
// however many lanes the read has.
func TestCDCReadsASnapshotStateFromBeforeRanges(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 100) i")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 30`)
	c := e.readCDCLanes(s, pipeline, 1, map[string]string{"users": ""})
	var old string
	for _, l := range c.log {
		if v, ok := strings.CutPrefix(l, "users@"); ok && strings.Contains(v, `"snapshot":{"after":["30"]}`) {
			old = v
			break
		}
	}
	if old == "" {
		t.Fatalf("no state at the first chunk: %q", c.log)
	}
	e.exec("ANALYZE $schema.users")
	rest := e.readCDCLanes(s, pipeline, 4, map[string]string{"users": old})
	got := rest.changes(t, "users")
	if len(got) != 70 || got[0].id != "31" {
		t.Errorf("%d rows, the first %+v", len(got), got[0])
	}
	if !slices.ContainsFunc(rest.log, func(l string) bool { return strings.Contains(l, `"snapshot":{"after":["60"]}`) }) {
		t.Errorf("states in another format: %q", rest.log)
	}
	if slices.ContainsFunc(rest.log, func(l string) bool { return strings.Contains(l, `"ranges"`) }) {
		t.Errorf("states in ranges: %q", rest.log)
	}

	// A state from a read not started is planned as a new one.
	st := stateOf(t, old)
	st.Snapshot = &snapshotCursor{}
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) == old {
		t.Fatal("same state")
	}
	e.exec("CREATE TABLE $schema.other (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.other SELECT i, 'n' || i, NULL FROM generate_series(1, 300) i")
	e.exec("ANALYZE $schema.other")
	c = e.readCDCLanes(s, pipeline, 4, map[string]string{"other": string(b)})
	if !slices.ContainsFunc(c.log, func(l string) bool { return strings.Contains(l, `"ranges"`) }) {
		t.Errorf("no states in ranges: %q", c.log)
	}
	if got := c.changes(t, "other"); len(got) != 300 {
		t.Errorf("%d rows", len(got))
	}
}

// One lane reads one chunk at a time, as it always has: states say where one
// position is.
func TestCDCReadsOneChunkAtATimeOnOneLane(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 100) i")
	e.exec("ANALYZE $schema.users")
	for _, extra := range []string{`, "snapshotChunkRows": 30`, `, "snapshotChunkRows": 30, "maxConnections": 1`} {
		s, pipeline := e.cdcSource(extra)
		lanes := 1
		if strings.Contains(extra, "maxConnections") {
			lanes = 4
		}
		c := e.readCDCLanes(s, pipeline, lanes, map[string]string{"users": ""})
		if slices.ContainsFunc(c.log, func(l string) bool { return strings.Contains(l, `"ranges"`) }) {
			t.Errorf("states in ranges: %q", c.log)
		}
		if got := c.changes(t, "users"); len(got) != 100 {
			t.Errorf("%d rows", len(got))
		}
	}
}

// Tables read in full or incrementally go on beside the log. On one lane they
// come first, since the log may never end.
func TestCDCReadsBesideTables(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 50) i")
	e.exec("CREATE TABLE $schema.plain (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.plain SELECT i, 'n' || i, NULL FROM generate_series(1, 50) i")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10, "chunkRows": 10`)
	streams := []source.ConfiguredStream{{Name: "users", SyncMode: source.CDC}, {Name: "plain", SyncMode: source.FullRefresh}}

	c := &capture{}
	t.Cleanup(c.release)
	if err := s.Read(t.Context(), source.ReadRequest{Streams: streams, Pipeline: pipeline, Lanes: 1}, c); err != nil {
		t.Fatal(err)
	}
	plainDone := slices.IndexFunc(c.log, func(l string) bool { return strings.HasPrefix(l, "plain complete") })
	first := slices.IndexFunc(c.log, func(l string) bool { return strings.HasPrefix(l, "users") })
	if plainDone < 0 || first < plainDone {
		t.Errorf("on one lane: %q", c.log)
	}
	if len(c.column("plain", "id")) != 50 || len(c.column("users", "id")) != 50 {
		t.Errorf("read %d and %d rows", len(c.column("plain", "id")), len(c.column("users", "id")))
	}

	// On two, a table completes while a continuous read follows the log, and
	// a failure of either stops the other.
	f := &capture{}
	t.Cleanup(f.release)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	req := source.ReadRequest{Streams: []source.ConfiguredStream{streams[0], {Name: "plain", SyncMode: source.FullRefresh}}, Pipeline: pipeline, Continuous: true, Lanes: 2}
	req.Streams[0].State = []byte(c.last("users"))
	go func() { done <- s.Read(ctx, req, f) }()
	await := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !ok() {
			select {
			case err := <-done:
				t.Fatalf("the read ended waiting for %s: %v", what, err)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("waited for %s", what)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	has := func(prefix string) bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return slices.ContainsFunc(f.log, func(l string) bool { return strings.HasPrefix(l, prefix) })
	}
	await("the table", func() bool { return has("plain complete") })
	e.exec("INSERT INTO $schema.users VALUES (51, 'n', NULL)")
	await("the insert", func() bool { return slices.Contains(f.column("users", "id"), "51") })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}

	bad := source.ReadRequest{Streams: []source.ConfiguredStream{streams[0], {Name: "plain", SyncMode: source.Incremental, Cursor: "missing"}}, Pipeline: pipeline, Continuous: true, Lanes: 2}
	bad.Streams[0].State = []byte(c.last("users"))
	err := s.Read(t.Context(), bad, &capture{})
	if err == nil || !strings.Contains(err.Error(), `no column "missing"`) {
		t.Errorf("a read with a bad table: %v", err)
	}
}
