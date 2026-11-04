//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	mrand "math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// cdcSource is a source reading the env's schema through a publication of
// its own, and a pipeline whose slot is dropped afterwards.
func (e *sourceEnv) cdcSource(extra string) (*Source, string) {
	e.t.Helper()
	s := e.source(fmt.Sprintf(`, "publication": %q%s`, e.schema, extra))
	pipeline := "p" + strings.ToLower(rand.Text())[:12]
	e.t.Cleanup(func() {
		ctx := context.Background()
		if err := s.Teardown(ctx, pipeline); err != nil {
			e.t.Errorf("teardown: %v", err)
		}
		_, _ = e.conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{e.schema}.Sanitize())
	})
	return s, pipeline
}

func cdcRequest(pipeline string, continuous bool, states map[string]string) source.ReadRequest {
	req := source.ReadRequest{Pipeline: pipeline, Continuous: continuous}
	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		cs := source.ConfiguredStream{Name: name, SyncMode: source.CDC}
		if st := states[name]; st != "" {
			cs.State = []byte(st)
		}
		req.Streams = append(req.Streams, cs)
	}
	return req
}

func readCDC(t *testing.T, s *Source, pipeline string, states map[string]string) *capture {
	t.Helper()
	c := &capture{}
	t.Cleanup(c.release)
	if err := s.Read(t.Context(), cdcRequest(pipeline, false, states), c); err != nil {
		t.Fatal(err)
	}
	return c
}

// following is a continuous read.
type following struct {
	*capture
	cancel context.CancelFunc
	done   chan error
	err    error
}

// follow reads continuously until stop is called.
func follow(t *testing.T, s *Source, pipeline string, states map[string]string) *following {
	t.Helper()
	c := &capture{}
	t.Cleanup(c.release)
	ctx, cancel := context.WithCancel(t.Context())
	f := &following{capture: c, cancel: cancel, done: make(chan error, 1)}
	go func() { f.done <- s.Read(ctx, cdcRequest(pipeline, true, states), c) }()
	return f
}

// await waits until ok, failing the test if the read ends first.
func (f *following) await(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !ok() {
		select {
		case err := <-f.done:
			t.Fatalf("the read ended waiting for %s: %v", what, err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited for %s; last state %s", what, f.last(f.streams()))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *capture) streams() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, l := range c.log {
		if i := strings.IndexAny(l, "@: "); i > 0 {
			return l[:i]
		}
	}
	return ""
}

func (f *following) stop() error {
	f.cancel()
	if err := <-f.done; !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (c *capture) lastLog() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.log) == 0 {
		return ""
	}
	return c.log[len(c.log)-1]
}

// last is a stream's last state, at a resume point or its completion.
func (c *capture) last(stream string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.log) - 1; i >= 0; i-- {
		if s, ok := strings.CutPrefix(c.log[i], stream+"@"); ok {
			return s
		}
		if s, ok := strings.CutPrefix(c.log[i], stream+" complete"); ok {
			return s
		}
	}
	return ""
}

// change is one emitted row.
type change struct {
	op, id, name, doc string
	seq               *big.Int
	unchanged         string
}

func (c *capture) changes(t *testing.T, stream string) []change {
	t.Helper()
	ops, ids, names, docs, seqs, unchanged := c.column(stream, "openetl.op"), c.column(stream, "id"), c.column(stream, "name"), c.column(stream, "doc"), c.column(stream, "openetl.seq"), c.column(stream, "openetl.unchanged")
	out := make([]change, len(ops))
	for i := range ops {
		seq, ok := new(big.Int).SetString(seqs[i], 10)
		if !ok {
			t.Fatalf("seq %q", seqs[i])
		}
		out[i] = change{op: ops[i], id: ids[i], name: names[i], doc: docs[i], seq: seq, unchanged: unchanged[i]}
	}
	return out
}

// merged applies changes as a merge write does: each key's latest version
// by seq, deletes removing it, unchanged columns keeping what's stored.
func merged(changes []change) map[string]change {
	type version struct {
		change
		deleted bool
	}
	rows := map[string]version{}
	sorted := slices.Clone(changes)
	slices.SortStableFunc(sorted, func(a, b change) int { return a.seq.Cmp(b.seq) })
	for _, c := range sorted {
		old, had := rows[c.id]
		if had && old.seq.Cmp(c.seq) > 0 {
			continue
		}
		v := version{change: c, deleted: c.op == source.OpDelete}
		if strings.Contains(c.unchanged, "doc") && had {
			v.doc = old.doc
		}
		rows[c.id] = v
	}
	out := map[string]change{}
	for id, v := range rows {
		if !v.deleted {
			out[id] = v.change
		}
	}
	return out
}

func (e *sourceEnv) rows(table string) map[string]change {
	e.t.Helper()
	rows, err := e.conn.Query(e.t.Context(), "SELECT id::text, coalesce(name, '(null)'), coalesce(doc, '(null)') FROM "+pgx.Identifier{e.schema, table}.Sanitize())
	if err != nil {
		e.t.Fatal(err)
	}
	out := map[string]change{}
	for rows.Next() {
		var c change
		if err := rows.Scan(&c.id, &c.name, &c.doc); err != nil {
			e.t.Fatal(err)
		}
		out[c.id] = c
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return out
}

func sameRows(t *testing.T, got, want map[string]change) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%d rows, want %d", len(got), len(want))
	}
	for id, w := range want {
		g, ok := got[id]
		if !ok || g.name != w.name || g.doc != w.doc {
			t.Errorf("row %s is %+v, want name %q doc %.20q", id, g, w.name, w.doc)
		}
	}
}

func TestCDCDiscovers(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.keyed (id int PRIMARY KEY)")
	e.exec("CREATE TABLE $schema.loose (v text)")
	e.exec("CREATE TABLE $schema.nothing (id int PRIMARY KEY)")
	e.exec("ALTER TABLE $schema.nothing REPLICA IDENTITY NOTHING")
	cat, err := e.source("").Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	modes := map[string][]source.SyncMode{}
	for _, s := range cat.Streams {
		modes[s.Name] = s.SyncModes
	}
	if m := modes["keyed"]; !slices.Equal(m, []source.SyncMode{source.FullRefresh, source.Incremental, source.CDC}) {
		t.Errorf("keyed reads %v", m)
	}
	for _, name := range []string{"loose", "nothing"} {
		if m := modes[name]; !slices.Equal(m, []source.SyncMode{source.FullRefresh, source.Incremental}) {
			t.Errorf("%s reads %v", name, m)
		}
	}
}

// A bounded read snapshots each table in chunks, then the next reads its
// changes: updates, deletes, a key's change, and an unchanged TOAST value.
func TestCDCSnapshotsThenReadsChanges(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text, flag boolean, blob bytea)")
	e.exec("ALTER TABLE $schema.users ALTER COLUMN doc SET STORAGE EXTERNAL")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL, i % 2 = 0, '\\x0102' FROM generate_series(1, 25) i")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)

	c := readCDC(t, s, pipeline, map[string]string{"users": ""})
	if got := c.changes(t, "users"); len(got) != 25 {
		t.Fatalf("snapshot: %d rows", len(got))
	}
	sameRows(t, merged(c.changes(t, "users")), e.rows("users"))
	if f := c.column("users", "flag"); f[0] != "false" || f[1] != "true" {
		t.Errorf("flags %v", f[:2])
	}
	if b := c.column("users", "blob"); b[0] != "AQI=" {
		t.Errorf("bytea as %q, want base64", b[0])
	}
	var resumes int
	for _, l := range c.log {
		if strings.HasPrefix(l, "users@") && strings.Contains(l, `"snapshot":{"after":`) {
			resumes++
		}
	}
	if resumes != 2 {
		t.Errorf("resume points mid-snapshot: %q", c.log)
	}
	state := c.last("users")
	var st cdcState
	if err := json.Unmarshal([]byte(state), &st); err != nil || st.Snapshot != nil || st.LSN == 0 || st.Slot != s.slotName(pipeline) {
		t.Fatalf("completed at %s (%v)", state, err)
	}

	big := strings.Repeat("x", 9000)
	e.exec("UPDATE $schema.users SET doc = '" + big + "' WHERE id = 3")
	e.exec("UPDATE $schema.users SET name = 'renamed' WHERE id = 1")
	e.exec("DELETE FROM $schema.users WHERE id = 2")
	e.exec("INSERT INTO $schema.users (id, name) VALUES (26, 'new')")
	e.exec("UPDATE $schema.users SET name = 'toasted' WHERE id = 3")
	e.exec("UPDATE $schema.users SET id = 40 WHERE id = 4")
	c2 := readCDC(t, s, pipeline, map[string]string{"users": state})
	got := c2.changes(t, "users")
	var ops []string
	for _, ch := range got {
		ops = append(ops, ch.op+" "+ch.id)
	}
	if want := []string{"update 3", "update 1", "delete 2", "insert 26", "update 3", "delete 4", "update 40"}; !slices.Equal(ops, want) {
		t.Fatalf("changes %q, want %q", ops, want)
	}
	if got[4].unchanged != `["doc"]` || got[4].doc != "(null)" {
		t.Errorf("the update that left doc alone: %+v", got[4])
	}
	if got[0].doc != big {
		t.Errorf("the update that set doc sent %d bytes", len(got[0].doc))
	}
	for i := 1; i < len(got); i++ {
		if got[i].seq.Cmp(got[i-1].seq) <= 0 {
			t.Errorf("seq %v after %v", got[i].seq, got[i-1].seq)
		}
	}
	sameRows(t, merged(append(c.changes(t, "users"), got...)), e.rows("users"))

	// Nothing new: the next read completes at once, where this one did.
	c3 := readCDC(t, s, pipeline, map[string]string{"users": c2.last("users")})
	if n := len(c3.changes(t, "users")); n != 0 {
		t.Errorf("read %d changes again", n)
	}
}

// Writers change the table throughout a continuous read with small chunks.
// Merged by key and seq, what's emitted is the table.
func TestCDCFollowsConcurrentWrites(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 400) i")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 7`)
	c := follow(t, s, pipeline, map[string]string{"users": ""})

	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			conn, err := pgx.Connect(context.Background(), dsn())
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close(context.Background())
			table := pgx.Identifier{e.schema, "users"}.Sanitize()
			rng := mrand.New(mrand.NewPCG(uint64(w), 7))
			for i := range 300 {
				id := rng.IntN(450) + 1
				var q string
				switch rng.IntN(4) {
				case 0:
					q = fmt.Sprintf("DELETE FROM %s WHERE id = %d", table, id)
				case 1:
					q = fmt.Sprintf("INSERT INTO %s VALUES (%d, 'w%d-%d', NULL) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name", table, id, w, i)
				default:
					q = fmt.Sprintf("UPDATE %s SET name = 'w%d-%d' WHERE id IN (%d, %d)", table, w, i, id, id+1)
				}
				if _, err := conn.Exec(context.Background(), q); err != nil {
					var pe *pgconn.PgError
					if errors.As(err, &pe) && (pe.Code == "40P01" || pe.Code == "23505") {
						continue
					}
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	var end string
	if err := e.conn.QueryRow(t.Context(), "SELECT pg_current_wal_lsn()::text").Scan(&end); err != nil {
		t.Fatal(err)
	}
	endLSN, _ := pglogrepl.ParseLSN(end)
	c.await(t, "the read to catch up with "+end, func() bool {
		var st cdcState
		s := c.last("users")
		return s != "" && json.Unmarshal([]byte(s), &st) == nil && st.Snapshot == nil && pglogrepl.LSN(st.LSN) >= endLSN
	})
	if err := c.stop(); err != nil {
		t.Fatal(err)
	}
	sameRows(t, merged(c.changes(t, "users")), e.rows("users"))
}

// A transaction that commits after a chunk's snapshot, but before its WAL
// position, isn't in the chunk; its change outlives the chunk's row.
func TestCDCChunkMissesATransactionBeforeItsPosition(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'old', NULL FROM generate_series(1, 5) i")
	s, pipeline := e.cdcSource("")
	once := sync.Once{}
	afterChunkSnapshot = func() {
		once.Do(func() { e.exec("UPDATE $schema.users SET name = 'new' WHERE id = 3") })
	}
	t.Cleanup(func() { afterChunkSnapshot = nil })
	c := readCDC(t, s, pipeline, map[string]string{"users": ""})
	got := c.changes(t, "users")
	var three []string
	for _, ch := range got {
		if ch.id == "3" {
			three = append(three, ch.op+" "+ch.name)
		}
	}
	if !slices.Equal(three, []string{"insert old", "update new"}) {
		t.Errorf("id 3: %q", three)
	}
	sameRows(t, merged(got), e.rows("users"))
}

func TestCDCAcknowledges(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	s, pipeline := e.cdcSource("")
	slot := s.slotName(pipeline)
	confirmed := func() pglogrepl.LSN {
		var at string
		if err := e.conn.QueryRow(t.Context(), "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&at); err != nil {
			t.Fatal(err)
		}
		l, _ := pglogrepl.ParseLSN(at)
		return l
	}
	c := readCDC(t, s, pipeline, map[string]string{"users": ""})
	before := confirmed()
	e.exec("INSERT INTO $schema.users VALUES (1, 'a', NULL)")
	c2 := readCDC(t, s, pipeline, map[string]string{"users": c.last("users")})
	state := c2.last("users")
	var st cdcState
	if err := json.Unmarshal([]byte(state), &st); err != nil {
		t.Fatal(err)
	}
	if confirmed() != before {
		t.Fatal("the slot moved before a checkpoint committed")
	}
	// Without a read running, Committed advances the slot itself.
	if err := s.Committed(t.Context(), map[string][]byte{"users": []byte(state), "other": []byte(`{"after":["1"]}`)}); err != nil {
		t.Fatal(err)
	}
	if got := confirmed(); got != pglogrepl.LSN(st.LSN) {
		t.Errorf("confirmed %s, want %s", got, pglogrepl.LSN(st.LSN))
	}

	// During one, the read tells the slot.
	c3 := follow(t, s, pipeline, map[string]string{"users": state})
	e.exec("INSERT INTO $schema.users VALUES (2, 'b', NULL)")
	c3.await(t, "the insert", func() bool {
		return len(c3.column("users", "id")) > 0 && json.Unmarshal([]byte(c3.last("users")), &st) == nil && st.LSN > 0 && strings.Contains(c3.lastLog(), "users@")
	})
	if err := json.Unmarshal([]byte(c3.last("users")), &st); err != nil {
		t.Fatal(err)
	}
	if err := s.Committed(t.Context(), map[string][]byte{"users": []byte(c3.last("users"))}); err != nil {
		t.Fatal(err)
	}
	c3.await(t, "the slot to confirm "+pglogrepl.LSN(st.LSN).String(), func() bool { return confirmed() >= pglogrepl.LSN(st.LSN) })
	if err := c3.stop(); err != nil {
		t.Fatal(err)
	}
}

// The read reports how far its slot is behind, how much more WAL the slot
// may hold, and how old the change being read is.
func TestCDCReportsProgress(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text)")
	e.exec("ALTER SYSTEM SET max_slot_wal_keep_size = '1GB'")
	e.exec("SELECT pg_reload_conf()")
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		_, _ = e.conn.Exec(ctx, "ALTER SYSTEM RESET max_slot_wal_keep_size")
		_, _ = e.conn.Exec(ctx, "SELECT pg_reload_conf()")
	})
	s, pipeline := e.cdcSource("")
	f := follow(t, s, pipeline, map[string]string{"users": ""})
	e.exec("INSERT INTO $schema.users VALUES (1, 'a')")
	f.await(t, "a progress report", func() bool { return len(f.reports()) > 0 })
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	p := f.reports()[0]
	if p.Stream != "" || p.LagBytes == nil || p.Lag == nil || *p.Lag < 0 || p.LagRecords != nil || p.RetentionLeft != nil {
		t.Errorf("progress %+v", p)
	}
	// The limit counts from the slot's segment, and one more is kept.
	if p.RetentionLeftBytes == nil || *p.RetentionLeftBytes == 0 || *p.RetentionLeftBytes > 1<<30+16<<20 {
		t.Errorf("retention left %+v, want up to 1 GB and a 16 MB segment", p)
	}
}

// A read takes the slot from a connection that still holds it; a slot
// that's gone while a stream has a position in it is lost.
func TestCDCSlotTakeoverAndLoss(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	s, pipeline := e.cdcSource("")
	c := readCDC(t, s, pipeline, map[string]string{"users": ""})
	state := c.last("users")

	cfg, err := pgconn.ParseConfig(dsn())
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["replication"] = "database"
	holder, err := pgconn.ConnectConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close(context.Background())
	if err := pglogrepl.StartReplication(t.Context(), holder, s.slotName(pipeline), 0, pglogrepl.StartReplicationOptions{PluginArgs: []string{"proto_version '1'", "publication_names '" + e.schema + "'"}}); err != nil {
		t.Fatal(err)
	}
	readCDC(t, s, pipeline, map[string]string{"users": state})

	if err := s.Teardown(t.Context(), pipeline); err != nil {
		t.Fatal(err)
	}
	err = s.Read(t.Context(), cdcRequest(pipeline, false, map[string]string{"users": state}), &capture{})
	if code := codeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("read without the slot: %v (%v)", err, code)
	}
	// Teardown again finds nothing to drop.
	if err := s.Teardown(t.Context(), pipeline); err != nil {
		t.Error(err)
	}
}

// A new column fails the read, so the next declares it; a truncate is
// only logged.
func TestCDCNewColumnsAndTruncates(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	s, pipeline := e.cdcSource("")
	state := readCDC(t, s, pipeline, map[string]string{"users": ""}).last("users")
	e.exec("TRUNCATE $schema.users")
	e.exec("INSERT INTO $schema.users VALUES (1, 'a', NULL)")
	c := readCDC(t, s, pipeline, map[string]string{"users": state})
	if n := len(c.changes(t, "users")); n != 1 {
		t.Errorf("%d changes after the truncate", n)
	}
	f := follow(t, s, pipeline, map[string]string{"users": c.last("users")})
	f.await(t, "a resume point", func() bool { return f.last("users") != "" })
	e.exec("ALTER TABLE $schema.users ADD COLUMN extra int")
	e.exec("INSERT INTO $schema.users VALUES (2, 'b', NULL, 7)")
	select {
	case err := <-f.done:
		if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT || !strings.Contains(err.Error(), `new column "extra"`) {
			t.Fatalf("a read that saw a new column: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the read didn't end at the new column")
	}
	// The next lists it.
	c = readCDC(t, s, pipeline, map[string]string{"users": f.last("users")})
	if got := c.column("users", "extra"); !slices.Equal(got, []string{"7"}) {
		t.Errorf("extra is %q", got)
	}
}

func codeOf(err error) protocolv1.ErrorCode {
	var c interface{ ErrorCode() protocolv1.ErrorCode }
	if errors.As(err, &c) {
		return c.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}
