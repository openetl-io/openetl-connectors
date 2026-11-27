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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// history is users' history as a test writes it: its rows from before the
// read's slot, as inserts, then each change, which an append of the stream
// holds once each. Its copies from on share it.
type history struct {
	e    *sourceEnv
	conn *pgx.Conn
	*written
}

type written struct {
	mu   sync.Mutex
	want []string
	n    int
}

// newHistory creates users with the even ids 2 to 2*rows.
func (e *sourceEnv) newHistory(rows int) *history {
	e.t.Helper()
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec(fmt.Sprintf("INSERT INTO $schema.users SELECT 2 * i, 'n' || 2 * i, NULL FROM generate_series(1, %d) i", rows))
	h := &history{e: e, conn: e.conn, written: &written{}}
	for i := 1; i <= rows; i++ {
		h.want = append(h.want, fmt.Sprintf("insert %d n%d", 2*i, 2*i))
	}
	return h
}

// on is h writing on conn.
func (h *history) on(conn *pgx.Conn) *history {
	return &history{e: h.e, conn: conn, written: h.written}
}

// write runs q, whose changes are those given; it may run on a read's
// goroutine.
func (h *history) write(q string, changes ...string) {
	if _, err := h.conn.Exec(context.Background(), strings.ReplaceAll(q, "$schema", pgx.Identifier{h.e.schema}.Sanitize())); err != nil {
		h.e.t.Errorf("%s: %v", q, err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.want = append(h.want, changes...)
}

func (h *history) name() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.n++
	return fmt.Sprintf("w%d", h.n)
}

func (h *history) insert(id int) {
	name := h.name()
	h.write(fmt.Sprintf("INSERT INTO $schema.users VALUES (%d, '%s', NULL)", id, name), fmt.Sprintf("insert %d %s", id, name))
}

func (h *history) update(id int) {
	name := h.name()
	h.write(fmt.Sprintf("UPDATE $schema.users SET name = '%s' WHERE id = %d", name, id), fmt.Sprintf("update %d %s", id, name))
}

func (h *history) delete(id int) {
	h.write(fmt.Sprintf("DELETE FROM $schema.users WHERE id = %d", id), fmt.Sprintf("delete %d", id))
}

// move changes a row's key, which the log carries as a delete and the row
// under its new key.
func (h *history) move(from, to int) {
	name := h.name()
	h.write(fmt.Sprintf("UPDATE $schema.users SET id = %d, name = '%s' WHERE id = %d", to, name, from), fmt.Sprintf("delete %d", from), fmt.Sprintf("update %d %s", to, name))
}

func (ch change) entry() string {
	if ch.op == source.OpDelete {
		return "delete " + ch.id
	}
	return ch.op + " " + ch.id + " " + ch.name
}

// appendsOnce checks that got, appended, holds each change of the history
// once, and nothing else.
func (h *history) appendsOnce(t *testing.T, got []change) {
	t.Helper()
	h.mu.Lock()
	count := map[string]int{}
	for _, w := range h.want {
		count[w]++
	}
	h.mu.Unlock()
	for _, ch := range got {
		count[ch.entry()]--
	}
	var extra, missing []string
	for k, n := range count {
		for ; n < 0; n++ {
			extra = append(extra, k)
		}
		for ; n > 0; n-- {
			missing = append(missing, k)
		}
	}
	slices.Sort(extra)
	slices.Sort(missing)
	if len(extra) > 0 || len(missing) > 0 {
		t.Errorf("appended, the stream has %d changes the table didn't make, %q, and lacks %d it made, %q", len(extra), extra, len(missing), missing)
	}
}

// upTo is what c emitted of stream before its resume point with state, as
// a checkpoint there holds it.
func (c *capture) upTo(t *testing.T, stream, state string) []change {
	t.Helper()
	n := 0
	for _, l := range c.log {
		if l == stream+"@"+state {
			part := &capture{batches: map[string][]arrow.RecordBatch{stream: c.batches[stream][:n]}}
			return part.changes(t, stream)
		}
		if strings.HasPrefix(l, stream+":") {
			n++
		}
	}
	t.Fatalf("no resume point %s: %q", state, c.log)
	return nil
}

// stateWith is the state of stream's first resume point with at least n
// rows before it.
func (c *capture) stateWith(t *testing.T, stream string, n int) string {
	t.Helper()
	rows := 0
	for _, l := range c.log {
		var k int
		if _, err := fmt.Sscanf(strings.TrimPrefix(l, stream+":"), "%d rows", &k); err == nil && strings.HasPrefix(l, stream+":") {
			rows += k
		}
		if s, ok := strings.CutPrefix(l, stream+"@"); ok && rows >= n {
			return s
		}
	}
	t.Fatalf("no resume point after %d rows: %q", n, c.log)
	return ""
}

// stateAfter is the state of stream's first resume point whose snapshot
// goes on after key.
func (c *capture) stateAfter(t *testing.T, stream, key string) string {
	t.Helper()
	for _, l := range c.log {
		if s, ok := strings.CutPrefix(l, stream+"@"); ok && strings.Contains(s, `"snapshot":{"after":["`+key+`"]}`) {
			return s
		}
	}
	t.Fatalf("no resume point after %s: %q", key, c.log)
	return ""
}

// A table written to while its snapshot is read, or between a read that
// stopped part-way through it and the read that resumes there, reaches an
// append once: each row that was there before the slot, as an insert, then
// each change, once. A row inserted, or moved to a new key, comes from the
// log alone, whichever chunk would read it.
func TestCDCSnapshotUnderWrites(t *testing.T) {
	for _, c := range []struct {
		name string
		// users has the even ids 2 to 2*rows, read 10 at a time.
		rows int
		// Writes in the first read: as it starts, before its first chunk's
		// snapshot, or after the snapshot of its chunk-th chunk.
		start  func(h *history)
		chunk  int
		during func(h *history)
		// Where a read stopped, whose resume point a checkpoint holds; nil:
		// where the first read completed.
		at func(t *testing.T, first *capture) string
		// Writes after the first read, before the read from at.
		between func(h *history)
	}{
		{
			name: "resumed after its last row, rows inserted since",
			rows: 20,
			at:   func(t *testing.T, c *capture) string { return c.stateWith(t, "users", 20) },
			between: func(h *history) {
				for id := 41; id <= 49; id += 2 {
					h.insert(id)
				}
				h.update(41)
				h.update(43)
				h.update(43)
				h.insert(51)
				h.delete(51)
				h.move(8, 53)
				h.update(4)
				h.delete(6)
				h.insert(7)
			},
		},
		{
			name:  "resumed after its last row, rows inserted after that chunk's snapshot",
			rows:  20,
			chunk: 2,
			during: func(h *history) {
				h.insert(41)
				h.update(41)
			},
			at: func(t *testing.T, c *capture) string { return c.stateWith(t, "users", 20) },
		},
		{
			name: "resumed mid-snapshot, changes behind it since",
			rows: 40,
			at:   func(t *testing.T, c *capture) string { return c.stateAfter(t, "users", "40") },
			between: func(h *history) {
				h.update(4)
				h.delete(6)
				h.insert(7)
				h.update(7)
				h.move(10, 11)
			},
		},
		{
			name: "resumed mid-snapshot, rows inserted ahead of it since",
			rows: 40,
			at:   func(t *testing.T, c *capture) string { return c.stateAfter(t, "users", "40") },
			between: func(h *history) {
				h.insert(51)
				h.update(51)
				h.insert(53)
				h.delete(53)
				h.insert(85)
				h.move(12, 57)
				h.insert(59)
				h.move(59, 61)
				h.insert(63)
				h.delete(63)
				h.insert(63)
			},
		},
		{
			name:  "rows inserted beyond its last chunk while it's read",
			rows:  20,
			chunk: 1,
			during: func(h *history) {
				h.insert(41)
				h.insert(43)
				h.update(43)
				h.insert(45)
				h.delete(45)
			},
		},
		{
			name:  "rows inserted ahead of it while it's read",
			rows:  40,
			chunk: 1,
			during: func(h *history) {
				h.insert(31)
				h.insert(33)
				h.update(33)
				h.move(14, 35)
				h.insert(85)
			},
		},
		{
			name: "rows inserted before its first chunk's snapshot, read while the chunk's held",
			rows: 20,
			start: func(h *history) {
				h.insert(7)
				h.update(7)
				h.insert(9)
			},
		},
		{
			name:  "changes after a chunk's snapshot, read while the chunk's held",
			rows:  20,
			chunk: 1,
			during: func(h *history) {
				h.update(4)
				h.delete(6)
				h.insert(7)
				h.update(7)
			},
		},
		{
			name:  "changes behind it while it's read",
			rows:  40,
			chunk: 3,
			during: func(h *history) {
				h.update(4)
				h.delete(6)
				h.insert(7)
				h.move(8, 9)
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newSourceEnv(t)
			h := e.newHistory(c.rows)
			s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)
			if c.start != nil {
				var once sync.Once
				beforeStartReplication = func() { once.Do(func() { c.start(h) }) }
				t.Cleanup(func() { beforeStartReplication = nil })
			}
			if c.during != nil {
				var chunks atomic.Int64
				afterChunkSnapshot = func() {
					if chunks.Add(1) == int64(c.chunk) {
						c.during(h)
					}
				}
				t.Cleanup(func() { afterChunkSnapshot = nil })
			}
			first := readCDC(t, s, pipeline, map[string]string{"users": ""})
			beforeStartReplication, afterChunkSnapshot = nil, nil
			state := first.last("users")
			got := first.changes(t, "users")
			if c.at != nil {
				state = c.at(t, first)
				got = first.upTo(t, "users", state)
			}
			if c.between != nil {
				c.between(h)
			}
			rest := readCDC(t, s, pipeline, map[string]string{"users": state})
			got = append(got, rest.changes(t, "users")...)
			h.appendsOnce(t, got)
			sameRows(t, merged(got), e.rows("users"))
		})
	}
}

// A snapshot whose last chunk is full is read with that chunk: its resume
// point has no snapshot left, and no chunk reads after it.
func TestCDCSnapshotEndsWithAFullChunk(t *testing.T) {
	e := newSourceEnv(t)
	e.newHistory(20)
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)
	var chunks atomic.Int64
	afterChunkSnapshot = func() { chunks.Add(1) }
	t.Cleanup(func() { afterChunkSnapshot = nil })
	c := readCDC(t, s, pipeline, map[string]string{"users": ""})
	if st := stateOf(t, c.stateWith(t, "users", 20)); st.Snapshot != nil {
		t.Errorf("after its last row, the snapshot goes on after %v: %q", st.Snapshot.After, c.log)
	}
	if n := chunks.Load(); n != 2 {
		t.Errorf("%d chunks", n)
	}
}

// keepInserting has writers, each on a connection of its own, insert rows of
// odd ids up to ids, and update, move and delete only rows they inserted.
func keepInserting(t *testing.T, h *history, seed uint64, ids, changes int) {
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
			hw := h.on(conn)
			rng := mrand.New(mrand.NewPCG(seed, uint64(w)))
			// The writer's ids are those 2w+1 past a multiple of 8.
			live := map[int]bool{}
			var ours []int
			fresh := func() int {
				for {
					if id := 8*rng.IntN(ids/8) + 2*w + 1; !live[id] {
						live[id] = true
						return id
					}
				}
			}
			for range changes {
				switch n := rng.IntN(6); {
				case len(ours) == 0 || n < 2:
					id := fresh()
					hw.insert(id)
					ours = append(ours, id)
				case n == 2:
					i := rng.IntN(len(ours))
					hw.delete(ours[i])
					delete(live, ours[i])
					ours = slices.Delete(ours, i, i+1)
				case n == 3:
					i := rng.IntN(len(ours))
					to := fresh()
					hw.move(ours[i], to)
					delete(live, ours[i])
					ours[i] = to
				default:
					hw.update(ours[rng.IntN(len(ours))])
				}
			}
		})
	}
	wg.Wait()
}

// Writers insert rows throughout a continuous read in key ranges, ahead of
// and behind each range's chunks, and change only rows they inserted.
// Appended, the stream holds each change once; merged, it's the table.
func TestCDCKeyRangesAppendRowsInsertedWhileReadOnce(t *testing.T) {
	for run := range 3 {
		t.Run(fmt.Sprint(run), func(t *testing.T) {
			e := newSourceEnv(t)
			h := e.newHistory(3000)
			e.exec("ANALYZE $schema.users")
			s, pipeline := e.cdcSource(`, "snapshotChunkRows": 13`)
			c := &capture{}
			t.Cleanup(c.release)
			ctx, cancel := context.WithCancel(t.Context())
			f := &following{capture: c, cancel: cancel, done: make(chan error, 1)}
			req := cdcRequest(pipeline, true, map[string]string{"users": ""})
			req.Lanes = 4
			go func() { f.done <- s.Read(ctx, req, c) }()
			// The slot's there by the first resume point.
			f.await(t, "a resume point", func() bool { return f.last("users") != "" })
			keepInserting(t, h, uint64(run), 6400, 150)
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
			got := c.changes(t, "users")
			h.appendsOnce(t, got)
			sameRows(t, merged(got), e.rows("users"))
		})
	}
}

// A read stopped mid-snapshot in key ranges, then resumed after writers
// inserted rows and changed only those, appends each change once.
func TestCDCRestartsMidSnapshotAfterInserts(t *testing.T) {
	e := newSourceEnv(t)
	h := e.newHistory(1500)
	e.exec("ANALYZE $schema.users")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 20`)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var resumes atomic.Int64
	first := &cutCapture{capture: &capture{}, stream: "users", cancel: cancel, at: func(state string) bool {
		return stateOf(t, state).Snapshot != nil && resumes.Add(1) == 10
	}}
	t.Cleanup(first.release)
	req := cdcRequest(pipeline, false, map[string]string{"users": ""})
	req.Lanes = 4
	if err := s.Read(ctx, req, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("the first read: %v", err)
	}
	state := first.checkpointed()
	if st := stateOf(t, state); st.Snapshot == nil {
		t.Fatalf("stopped at %s", state)
	}
	keepInserting(t, h, 7, 3200, 150)
	rest := e.readCDCLanes(s, pipeline, 3, map[string]string{"users": state})
	got := append(first.changes(t, "users"), rest.changes(t, "users")...)
	h.appendsOnce(t, got)
	sameRows(t, merged(got), e.rows("users"))
}
