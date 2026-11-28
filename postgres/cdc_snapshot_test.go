//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	mrand "math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// history is a table's history as a test writes it: each key's changes, in
// order, its rows from before the read's slot as inserts. A test changes a
// key on one connection at a time, so the order it writes a key's changes
// in is the order they commit in. Its copies from on share it.
type history struct {
	e    *sourceEnv
	conn *pgx.Conn
	*written
}

type written struct {
	mu    sync.Mutex
	table string
	want  []string
	n     int
	// How ids are spelled as keys.
	key func(id int) string
}

// newHistory creates users with the even ids 2 to 2*rows as its key.
func (e *sourceEnv) newHistory(rows int) *history {
	e.t.Helper()
	return e.historyOf(rows, "int", strconv.Itoa)
}

// historyOf creates users with the even ids 2 to 2*rows spelled by key, in
// a key column of typ.
func (e *sourceEnv) historyOf(rows int, typ string, key func(int) string) *history {
	e.t.Helper()
	return e.tableHistory("users", rows, typ, key)
}

// tableHistory creates table as historyOf creates users.
func (e *sourceEnv) tableHistory(table string, rows int, typ string, key func(int) string) *history {
	e.t.Helper()
	h := &history{e: e, conn: e.conn, written: &written{table: table, key: key}}
	e.exec("CREATE TABLE " + h.ident() + " (id " + typ + " PRIMARY KEY, name text, doc text)")
	var rowsIn []string
	for i := 1; i <= rows; i++ {
		rowsIn = append(rowsIn, fmt.Sprintf("(%s, 'n%d', NULL)", quoteLiteral(key(2*i)), 2*i))
		h.want = append(h.want, fmt.Sprintf("insert %s n%d", key(2*i), 2*i))
	}
	e.exec("INSERT INTO " + h.ident() + " VALUES " + strings.Join(rowsIn, ", "))
	return h
}

// on is h writing on conn.
func (h *history) on(conn *pgx.Conn) *history {
	return &history{e: h.e, conn: conn, written: h.written}
}

func (h *history) ident() string { return pgx.Identifier{h.e.schema, h.table}.Sanitize() }

// op is a statement of a history's, and the changes it makes.
type op struct {
	h       *history
	q       string
	changes []string
}

// run runs ops on h's connection, in one transaction, and records their
// changes; it may run on a read's goroutine.
func (h *history) run(ops ...op) {
	var qs []string
	for _, o := range ops {
		qs = append(qs, o.q)
	}
	q := strings.Join(qs, "; ")
	if len(ops) > 1 {
		q = "BEGIN; " + q + "; COMMIT"
	}
	if _, err := h.conn.Exec(context.Background(), q); err != nil {
		h.e.t.Errorf("%s: %v", q, err)
		return
	}
	for _, o := range ops {
		o.h.mu.Lock()
		o.h.want = append(o.h.want, o.changes...)
		o.h.mu.Unlock()
	}
}

func (h *history) name() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.n++
	return fmt.Sprintf("w%d", h.n)
}

func (h *history) lit(id int) string { return quoteLiteral(h.key(id)) }

func (h *history) inserting(id int) op {
	name := h.name()
	return op{h, fmt.Sprintf("INSERT INTO %s VALUES (%s, '%s', NULL)", h.ident(), h.lit(id), name), []string{fmt.Sprintf("insert %s %s", h.key(id), name)}}
}

func (h *history) updating(id int) op {
	name := h.name()
	return op{h, fmt.Sprintf("UPDATE %s SET name = '%s' WHERE id = %s", h.ident(), name, h.lit(id)), []string{fmt.Sprintf("update %s %s", h.key(id), name)}}
}

func (h *history) deleting(id int) op {
	return op{h, fmt.Sprintf("DELETE FROM %s WHERE id = %s", h.ident(), h.lit(id)), []string{"delete " + h.key(id)}}
}

// moving changes a row's key, which the log carries as a delete and the row
// under its new key.
func (h *history) moving(from, to int) op {
	name := h.name()
	return op{h, fmt.Sprintf("UPDATE %s SET id = %s, name = '%s' WHERE id = %s", h.ident(), h.lit(to), name, h.lit(from)), []string{"delete " + h.key(from), fmt.Sprintf("update %s %s", h.key(to), name)}}
}

func (h *history) insert(id int)     { h.run(h.inserting(id)) }
func (h *history) update(id int)     { h.run(h.updating(id)) }
func (h *history) delete(id int)     { h.run(h.deleting(id)) }
func (h *history) move(from, to int) { h.run(h.moving(from, to)) }

func (ch change) entry() string {
	if ch.op == source.OpDelete {
		return "delete " + ch.id
	}
	return ch.op + " " + ch.id + " " + ch.name
}

// read says ch is a row a chunk read, whose seq is its chunk's position
// alone.
func (ch change) read() bool {
	return new(big.Int).Mod(ch.seq, big.NewInt(1_000_000_000_000_000_000)).Sign() == 0
}

// appended spells what an append of got holds of id: a row a chunk read as
// "chunk NAME", and a change as its entry.
func appended(got []change, id string) []string {
	var out []string
	for _, ch := range got {
		switch {
		case ch.id != id:
		case ch.read():
			out = append(out, "chunk "+ch.name)
		default:
			out = append(out, ch.entry())
		}
	}
	return out
}

// fromChunks checks that got, appended, holds each key's history from where
// a chunk read it: the row the chunk read, if it had one, then each change
// after the chunk's snapshot, once, in order, with seqs in that order too,
// and nothing else.
func (h *history) fromChunks(t *testing.T, got []change) {
	t.Helper()
	h.mu.Lock()
	wrote := map[string][]string{}
	for _, w := range h.want {
		id := strings.Fields(w)[1]
		wrote[id] = append(wrote[id], w)
	}
	h.mu.Unlock()
	ids := map[string]bool{}
	for id := range wrote {
		ids[id] = true
	}
	for _, ch := range got {
		ids[ch.id] = true
	}
	// Each key's versions as a merge orders them, by seq, are in the order
	// they were emitted in.
	last := map[string]change{}
	for _, ch := range got {
		if prev, ok := last[ch.id]; ok && ch.seq.Cmp(prev.seq) <= 0 {
			t.Errorf("%s: %s, seq %v, after %s, seq %v", ch.id, ch.entry(), ch.seq, prev.entry(), prev.seq)
		}
		last[ch.id] = ch
	}
	bad := 0
	for id := range ids {
		if got := appended(got, id); !fromAChunk(wrote[id], got) {
			if bad++; bad <= 20 {
				t.Errorf("appended, %s is %q, which isn't its history %q from where a chunk read it", id, got, wrote[id])
			}
		}
	}
	if bad > 20 {
		t.Errorf("and %d keys more", bad-20)
	}
}

// fromAChunk says got is wrote after one of its changes, or from its start,
// with the row as that change left it, if it left one, as a chunk's.
func fromAChunk(wrote, got []string) bool {
	for i := 0; i <= len(wrote); i++ {
		rest := got
		if i > 0 {
			if f := strings.Fields(wrote[i-1]); f[0] != "delete" {
				if len(rest) == 0 || rest[0] != "chunk "+f[2] {
					continue
				}
				rest = rest[1:]
			}
		}
		if slices.Equal(rest, wrote[i:]) {
			return true
		}
	}
	return false
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
// append with each key's history from where a chunk read it: the row as the
// chunk read it, then each change after the chunk's snapshot, once. A change
// to a key no chunk has read yet isn't sent, nor is one the chunk's snapshot
// saw: the chunk's row has it.
func TestCDCSnapshotUnderWrites(t *testing.T) {
	for _, c := range []struct {
		name string
		// users has the even ids 2 to 2*rows, read 10 at a time.
		rows int
		// Writes in the first read: as it starts, before its first chunk's
		// snapshot, before the snapshot of its before-th chunk, or after the
		// snapshot of its chunk-th chunk.
		start  func(h *history)
		before int
		ahead  func(h *history)
		chunk  int
		during func(h *history)
		// Where a read stopped, whose resume point a checkpoint holds; nil:
		// where the first read completed.
		at func(t *testing.T, first *capture) string
		// Writes after the first read, before the read from at.
		between func(h *history)
		// What an append holds of some keys, as appended spells it.
		exact map[int][]string
	}{
		{
			name:  "changes before its chunk is read",
			rows:  40,
			chunk: 1,
			during: func(h *history) {
				h.update(30)
				h.update(30)
				h.delete(32)
				h.move(34, 35)
				h.delete(36)
				h.insert(36)
				h.insert(37)
			},
			exact: map[int][]string{30: {"chunk w2"}, 32: nil, 34: nil, 35: {"chunk w3"}, 36: {"chunk w4"}, 37: {"chunk w5"}},
		},
		{
			name:   "changes its chunk's snapshot saw, read while the chunk's held",
			rows:   20,
			before: 1,
			ahead: func(h *history) {
				h.update(4)
				h.delete(6)
				h.move(8, 9)
				h.insert(11)
			},
			exact: map[int][]string{4: {"chunk w1"}, 6: nil, 8: nil, 9: {"chunk w2"}, 11: {"chunk w3"}},
		},
		{
			name:   "changes the next chunk's snapshot saw, read while the chunk before is held",
			rows:   40,
			before: 2,
			ahead: func(h *history) {
				h.update(24)
				h.delete(26)
				h.insert(27)
				h.move(28, 61)
			},
			exact: map[int][]string{24: {"chunk w1"}, 26: nil, 27: {"chunk w2"}, 28: nil, 61: {"chunk w3"}},
		},
		{
			name:  "keys moved behind it, ahead of it, and within its chunk",
			rows:  40,
			chunk: 2,
			during: func(h *history) {
				h.move(4, 45)
				h.move(60, 5)
				h.move(30, 31)
			},
			exact: map[int][]string{4: {"chunk n4", "delete 4"}, 45: {"chunk w1"}, 60: nil, 5: {"update 5 w2"}, 30: {"chunk n30", "delete 30"}, 31: {"update 31 w3"}},
		},
		{
			name:  "resumed mid-snapshot, rows inserted ahead of it before its resume point",
			rows:  40,
			chunk: 1,
			during: func(h *history) {
				h.insert(51)
				h.update(51)
				h.insert(53)
				h.insert(55)
				h.delete(55)
				h.update(30)
			},
			at:    func(t *testing.T, c *capture) string { return c.stateAfter(t, "users", "20") },
			exact: map[int][]string{51: {"chunk w2"}, 53: {"chunk w3"}, 55: nil, 30: {"chunk w5"}},
		},
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
			exact: map[int][]string{4: {"chunk n4", "update 4 w1"}, 6: {"chunk n6", "delete 6"}, 7: {"insert 7 w2", "update 7 w3"}},
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
			if c.ahead != nil {
				var chunks atomic.Int64
				beforeChunkSnapshot = func() {
					if chunks.Add(1) == int64(c.before) {
						c.ahead(h)
					}
				}
				t.Cleanup(func() { beforeChunkSnapshot = nil })
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
			beforeStartReplication, beforeChunkSnapshot, afterChunkSnapshot = nil, nil, nil
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
			h.fromChunks(t, got)
			for id, want := range c.exact {
				if got := appended(got, strconv.Itoa(id)); !slices.Equal(got, want) {
					t.Errorf("appended, %d is %q, want %q", id, got, want)
				}
			}
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

// A row whose chunk reads it arrives whole, with what the log leaves out of
// its changes: one inserted into a table with a generated column, which the
// log doesn't send, and one a key change moved without its unchanged TOAST
// value, into a key whose row the log had brought in whole and taken out,
// while the chunk was held or before it was read.
func TestCDCSnapshotKeepsRowsTheLogSendsInPart(t *testing.T) {
	t.Run("generated column", func(t *testing.T) {
		e := newSourceEnv(t)
		e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text, twice int GENERATED ALWAYS AS (id * 2) STORED)")
		e.exec("INSERT INTO $schema.users (id, name) SELECT i, 'n' || i FROM generate_series(1, 20) i")
		s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)
		var once sync.Once
		beforeStartReplication = func() {
			once.Do(func() {
				if _, err := e.conn.Exec(context.Background(), "INSERT INTO "+pgx.Identifier{e.schema, "users"}.Sanitize()+" (id, name) VALUES (25, 'new')"); err != nil {
					t.Error(err)
				}
			})
		}
		t.Cleanup(func() { beforeStartReplication = nil })
		c := readCDC(t, s, pipeline, map[string]string{"users": ""})
		got, twice := c.changes(t, "users"), c.column("users", "twice")
		last := -1
		for i, ch := range got {
			if ch.id == "25" && (last < 0 || ch.seq.Cmp(got[last].seq) > 0) {
				last = i
			}
		}
		if last < 0 || twice[last] != "50" {
			t.Errorf("row 25's latest version has twice %q: %+v", twice, got)
		}
	})
	for _, c := range []struct {
		name string
		// Writes before the read's first chunk, or in the first chunk after
		// its snapshot and in the second before its snapshot.
		start, first, second func(h *history)
	}{
		{
			name: "while its chunk is held",
			start: func(h *history) {
				h.insert(7)
				h.delete(7)
				h.move(4, 7)
			},
		},
		{
			name:  "before its chunk is read",
			first: func(h *history) { h.insert(21) },
			second: func(h *history) {
				h.delete(21)
				h.move(4, 21)
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newSourceEnv(t)
			h := e.newHistory(20)
			e.exec("ALTER TABLE $schema.users ALTER COLUMN doc SET STORAGE EXTERNAL")
			e.exec("UPDATE $schema.users SET doc = '" + strings.Repeat("x", 9000) + "' WHERE id = 4")
			s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)
			var once sync.Once
			var before, after atomic.Int64
			beforeStartReplication = func() {
				if c.start != nil {
					once.Do(func() { c.start(h) })
				}
			}
			afterChunkSnapshot = func() {
				if after.Add(1) == 1 && c.first != nil {
					c.first(h)
				}
			}
			beforeChunkSnapshot = func() {
				if before.Add(1) == 2 && c.second != nil {
					c.second(h)
				}
			}
			t.Cleanup(func() { beforeStartReplication, afterChunkSnapshot, beforeChunkSnapshot = nil, nil, nil })
			got := readCDC(t, s, pipeline, map[string]string{"users": ""}).changes(t, "users")
			h.fromChunks(t, got)
			sameRows(t, merged(got), e.rows("users"))
		})
	}
}

// keepInserting has writers, each on a connection of its own, insert rows of
// odd ids up to ids, and update, move and delete only rows they inserted.
// wrote, if set, runs once the first writer has made 10 changes.
func keepInserting(t *testing.T, h *history, seed uint64, ids, changes int, wrote func()) {
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
			for i := range changes {
				if i == 10 && w == 0 && wrote != nil {
					wrote()
				}
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
// Appended, the stream holds each key's history from its chunk; merged,
// it's the table.
func TestCDCKeyRangesAppendRowsInsertedWhileReadFromTheirChunks(t *testing.T) {
	for run := range 3 {
		t.Run(fmt.Sprint(run), func(t *testing.T) {
			e := newSourceEnv(t)
			h := e.newHistory(3000)
			e.exec("ANALYZE $schema.users")
			s, pipeline := e.cdcSource(`, "snapshotChunkRows": 13`)
			// The writers start in the snapshot's second round of chunks,
			// which waits for their first changes, so they write while
			// it's read.
			var chunks atomic.Int64
			reading, wrote := make(chan struct{}), make(chan struct{})
			afterChunkSnapshot = func() {
				if chunks.Add(1) != 8 {
					return
				}
				close(reading)
				select {
				case <-wrote:
				case <-time.After(30 * time.Second):
					t.Error("no writes while the snapshot was read")
				}
			}
			t.Cleanup(func() { afterChunkSnapshot = nil })
			c := &capture{}
			t.Cleanup(c.release)
			ctx, cancel := context.WithCancel(t.Context())
			f := &following{capture: c, cancel: cancel, done: make(chan error, 1)}
			req := cdcRequest(pipeline, true, map[string]string{"users": ""})
			req.Lanes = 4
			ended := make(chan struct{})
			go func() {
				f.done <- s.Read(ctx, req, c)
				close(ended)
			}()
			t.Cleanup(func() {
				cancel()
				<-ended
			})
			select {
			case <-reading:
			case err := <-f.done:
				t.Fatalf("the read ended before its snapshot's second round: %v", err)
			case <-time.After(30 * time.Second):
				t.Fatal("waited for the snapshot's second round")
			}
			keepInserting(t, h, uint64(run), 6400, 150, func() { close(wrote) })
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
			h.fromChunks(t, got)
			sameRows(t, merged(got), e.rows("users"))
		})
	}
}

// A read stopped mid-snapshot in key ranges, then resumed after writers
// inserted rows and changed only those, appends each key's history from its
// chunk.
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
	keepInserting(t, h, 7, 3200, 150, nil)
	rest := e.readCDCLanes(s, pipeline, 3, map[string]string{"users": state})
	got := append(first.changes(t, "users"), rest.changes(t, "users")...)
	h.fromChunks(t, got)
	sameRows(t, merged(got), e.rows("users"))
}
