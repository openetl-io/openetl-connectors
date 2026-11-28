//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	mrand "math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var historyRuns = flag.Int("cdc.history-runs", 1, "how many times each case of TestCDCSnapshotHistoriesUnderWritesAndRestarts runs")

// exported starts a transaction that exports its snapshot, which sees what
// committed before it and nothing after, for as long as t runs.
func exported(t *testing.T) string {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	tx, err := conn.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	var id string
	if err := tx.QueryRow(t.Context(), "SELECT pg_export_snapshot()").Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// stale gives the read's next left snapshots one exported before the test
// wrote, as a snapshot is while a transaction the log has read is still in
// the proc array.
type stale struct {
	id   string
	left atomic.Int64
}

func (s *stale) from() string {
	if s.left.Add(-1) >= 0 {
		return s.id
	}
	return ""
}

// A transaction whose change waits for a chunk can be read from the log
// before new snapshots see it. A resume point past it waits until one does,
// so the chunk of a read resumed there, which doesn't read it from the log
// again, sees it.
func TestCDCResumePointWaitsForTheTransactionsItsChunksMustSee(t *testing.T) {
	e := newSourceEnv(t)
	h := e.newHistory(40)
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)
	old := &stale{id: exported(t)}
	var chunks atomic.Int64
	afterChunkSnapshot = func() {
		if chunks.Add(1) == 1 {
			old.left.Store(3)
			h.insert(41)
		}
	}
	snapshotFrom = old.from
	t.Cleanup(func() { afterChunkSnapshot, snapshotFrom = nil, nil })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	left := int64(-1)
	first := &cutCapture{capture: &capture{}, stream: "users", cancel: cancel, at: func(state string) bool {
		if !strings.Contains(state, `"after":["20"]`) {
			return false
		}
		left = old.left.Load()
		return true
	}}
	t.Cleanup(first.release)
	if err := s.Read(ctx, cdcRequest(pipeline, false, map[string]string{"users": ""}), first); !errors.Is(err, context.Canceled) {
		t.Fatalf("the first read: %v", err)
	}
	if left > 0 {
		t.Errorf("the resume point after the first chunk came with %d snapshots that don't see the insert ahead of it left", left)
	}
	state := first.checkpointed()
	rest := readCDC(t, s, pipeline, map[string]string{"users": state})
	got := append(first.changes(t, "users"), rest.changes(t, "users")...)
	h.fromChunks(t, got)
	if got := appended(got, "41"); !slices.Equal(got, []string{"chunk w1"}) {
		t.Errorf("appended, 41 is %q", got)
	}
	sameRows(t, merged(got), e.rows("users"))
}

// A chunk read after the log read a transaction that changed one of its keys
// waits until new snapshots see that transaction: here, of a table whose
// chunk the round before had no connection for.
func TestCDCChunkWaitsForTheTransactionsTheLogRead(t *testing.T) {
	e := newSourceEnv(t)
	for _, name := range []string{"a", "b"} {
		e.exec("CREATE TABLE $schema." + name + " (id int PRIMARY KEY, name text, doc text)")
		e.exec("INSERT INTO $schema." + name + " SELECT i, 'n' || i, NULL FROM generate_series(1, 5) i")
	}
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)
	old := &stale{id: exported(t)}
	var chunks atomic.Int64
	afterChunkSnapshot = func() {
		if chunks.Add(1) == 1 {
			old.left.Store(3)
			e.exec("INSERT INTO $schema.b VALUES (100, 'new', NULL)")
		}
	}
	snapshotFrom = old.from
	t.Cleanup(func() { afterChunkSnapshot, snapshotFrom = nil, nil })
	c := readCDC(t, s, pipeline, map[string]string{"a": "", "b": ""})
	if n := chunks.Load(); n != 2 {
		t.Fatalf("%d chunks, want a's and then b's", n)
	}
	if got := appended(c.changes(t, "b"), "100"); !slices.Equal(got, []string{"chunk new"}) {
		t.Errorf("appended, b's 100 is %q", got)
	}
	if got := len(merged(c.changes(t, "b"))); got != 6 {
		t.Errorf("b has %d rows", got)
	}
}

// A transaction that changes a key of one table's chunk and inserts into
// another table, whose keys the server places, which no chunk has read yet,
// is waited for before that table's chunk, though the first table's chunk
// passed, and its snapshot read, before the second's change was placed.
func TestCDCChunkWaitsForATransactionAnotherTableDeferredFirst(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.a (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.a SELECT i, 'n' || i, NULL FROM generate_series(1, 5) i")
	e.exec(`CREATE TABLE $schema.b (id text COLLATE "C" PRIMARY KEY, name text, doc text)`)
	e.exec("INSERT INTO $schema.b SELECT 'b' || i, 'n' || i, NULL FROM generate_series(1, 5) i")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)
	old := &stale{id: exported(t)}
	var chunks atomic.Int64
	afterChunkSnapshot = func() {
		if chunks.Add(1) == 1 {
			e.exec("BEGIN; UPDATE $schema.a SET name = 'x' WHERE id = 3; INSERT INTO $schema.b VALUES ('zzz', 'x', NULL); COMMIT")
			old.left.Store(3)
		}
	}
	snapshotFrom = old.from
	t.Cleanup(func() { afterChunkSnapshot, snapshotFrom = nil, nil })
	c := readCDC(t, s, pipeline, map[string]string{"a": "", "b": ""})
	if n := chunks.Load(); n != 2 {
		t.Fatalf("%d chunks, want a's and then b's", n)
	}
	if got := appended(c.changes(t, "a"), "3"); !slices.Equal(got, []string{"chunk n3", "update 3 x"}) {
		t.Errorf("appended, a's 3 is %q", got)
	}
	if got := appended(c.changes(t, "b"), "zzz"); !slices.Equal(got, []string{"chunk x"}) {
		t.Errorf("appended, b's zzz is %q", got)
	}
}

// A transaction the log read that new snapshots don't see in time fails the
// read, retryably, before a resume point passes it.
func TestCDCGivesUpOnATransactionNewSnapshotsDontSee(t *testing.T) {
	e := newSourceEnv(t)
	h := e.newHistory(40)
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)
	old := &stale{id: exported(t)}
	var chunks atomic.Int64
	afterChunkSnapshot = func() {
		if chunks.Add(1) == 1 {
			old.left.Store(1 << 40)
			h.insert(41)
		}
	}
	snapshotFrom = old.from
	was := visibleWithin
	visibleWithin = 300 * time.Millisecond
	t.Cleanup(func() { afterChunkSnapshot, snapshotFrom, visibleWithin = nil, nil, was })
	c := &capture{}
	t.Cleanup(c.release)
	err := s.Read(t.Context(), cdcRequest(pipeline, false, map[string]string{"users": ""}), c)
	if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT || !strings.Contains(err.Error(), "isn't visible") {
		t.Fatalf("a read whose transaction stays unseen: %v", err)
	}
	if slices.ContainsFunc(c.log, func(l string) bool { return strings.HasPrefix(l, "users@") && strings.Contains(l, `"after"`) }) {
		t.Errorf("a resume point past the first chunk: %q", c.log)
	}
}

// The server places keys of any type as readChunk's queries read them: a
// key is at or before a range's position, in a held chunk's keys or after
// them, as the queries' own conditions on the range's bounds and the
// positions say.
func TestPlaceKeysAsTheServerOrdersThem(t *testing.T) {
	letters := func(rng *mrand.Rand) string {
		const set = "aAbBcC~0é É-z"
		var b strings.Builder
		for range 1 + rng.IntN(3) {
			r := []rune(set)
			b.WriteRune(r[rng.IntN(len(r))])
		}
		return b.String()
	}
	for _, c := range []struct {
		name string
		// The key's columns' types, and a value of each.
		types []string
		gen   []func(rng *mrand.Rand) string
	}{
		{"integers", []string{"int"}, []func(*mrand.Rand) string{func(rng *mrand.Rand) string { return strconv.Itoa(rng.IntN(200) - 100) }}},
		{"text in C", []string{`text COLLATE "C"`}, []func(*mrand.Rand) string{letters}},
		{"text in ICU's English", []string{`text COLLATE "en-x-icu"`}, []func(*mrand.Rand) string{letters}},
		{"text in libc's English", []string{`text COLLATE "en_US.utf8"`}, []func(*mrand.Rand) string{letters}},
		{"numeric", []string{"numeric"}, []func(*mrand.Rand) string{func(rng *mrand.Rand) string { return fmt.Sprintf("%.2f", rng.Float64()*20-10) }}},
		{"an enum", []string{"$schema.mood"}, []func(*mrand.Rand) string{func(rng *mrand.Rand) string { return []string{"sad", "ok", "happy", "glad"}[rng.IntN(4)] }}},
		{"text and an integer", []string{`text COLLATE "en-x-icu"`, "int"}, []func(*mrand.Rand) string{letters, func(rng *mrand.Rand) string { return strconv.Itoa(rng.IntN(5)) }}},
		{"an integer and text", []string{"bigint", `varchar(5) COLLATE "C"`}, []func(*mrand.Rand) string{func(rng *mrand.Rand) string { return strconv.Itoa(rng.IntN(6)) }, letters}},
		{"timestamptz", []string{"timestamptz"}, []func(*mrand.Rand) string{func(rng *mrand.Rand) string {
			return fmt.Sprintf("2024-03-%02d %02d:%02d:%02d.%d%+03d", 1+rng.IntN(28), rng.IntN(24), rng.IntN(60), rng.IntN(60), rng.IntN(1000), rng.IntN(25)-12)
		}}},
		{"uuid", []string{"uuid"}, []func(*mrand.Rand) string{func(rng *mrand.Rand) string {
			return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", rng.Uint32(), rng.Uint32()&0xffff, rng.Uint32()&0xffff, rng.Uint32()&0xffff, rng.Uint64()&0xffffffffffff)
		}}},
		{"bytea", []string{"bytea"}, []func(*mrand.Rand) string{func(rng *mrand.Rand) string {
			b := make([]byte, rng.IntN(4))
			for i := range b {
				b[i] = byte(rng.IntN(256))
			}
			return fmt.Sprintf(`\x%x`, b)
		}}},
		{"a domain", []string{"$schema.code"}, []func(*mrand.Rand) string{letters}},
		{"char(3)", []string{`char(3) COLLATE "en-x-icu"`}, []func(*mrand.Rand) string{letters}},
		{"an array", []string{"int[]"}, []func(*mrand.Rand) string{func(rng *mrand.Rand) string {
			var v []string
			for range rng.IntN(4) {
				v = append(v, strconv.Itoa(rng.IntN(5)-2))
			}
			return "{" + strings.Join(v, ",") + "}"
		}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TYPE $schema.mood AS ENUM ('sad', 'ok', 'happy', 'glad')")
			e.exec(`CREATE DOMAIN $schema.code AS text COLLATE "en-x-icu" CHECK (VALUE <> 'never')`)
			var defs, names []string
			for i, typ := range c.types {
				names = append(names, "c"+strconv.Itoa(i))
				defs = append(defs, names[i]+" "+typ)
			}
			e.exec("CREATE TABLE $schema.k (" + strings.Join(defs, ", ") + ", PRIMARY KEY (" + strings.Join(names, ", ") + "))")
			rng := mrand.New(mrand.NewPCG(1, uint64(len(c.name))))
			for range 120 {
				var vals []string
				for _, g := range c.gen {
					vals = append(vals, quoteLiteral(g(rng)))
				}
				e.exec("INSERT INTO $schema.k VALUES (" + strings.Join(vals, ", ") + ") ON CONFLICT DO NOTHING")
			}
			ident := pgx.Identifier{e.schema, "k"}.Sanitize()
			texts := func(q string, args ...string) [][]string {
				t.Helper()
				var params [][]byte
				for _, a := range args {
					params = append(params, []byte(a))
				}
				rr := e.conn.PgConn().ExecParams(t.Context(), q, params, nil, nil, nil)
				var out [][]string
				for rr.NextRow() {
					var row []string
					for _, v := range rr.Values() {
						row = append(row, string(v))
					}
					out = append(out, row)
				}
				if _, err := rr.Close(); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				return out
			}
			list := strings.Join(names, ", ")
			keys := texts("SELECT " + list + " FROM " + ident + " ORDER BY " + list)
			firsts := texts("SELECT DISTINCT c0 FROM " + ident + " ORDER BY c0")
			cols := make([]sourceColumn, len(names))
			for i := range names {
				cols[i] = sourceColumn{name: names[i]}
			}
			st := &cdcStream{t: table{name: "k", cols: cols, key: []int{0, 1}[:len(names)]}}
			r := &cdcReader{s: &Source{cfg: SourceConfig{Schema: e.schema}}, conn: e.conn, streams: []*cdcStream{st}}
			for trial := range 30 {
				// Ranges cut at some of the first column's values, some read,
				// each with a position and a held chunk or not.
				var cuts []string
				for _, f := range firsts {
					if rng.IntN(8) == 0 {
						cuts = append(cuts, f[0])
					}
				}
				st.ranges, r.chunks, st.plan = nil, nil, nil
				for i := 0; i <= len(cuts); i++ {
					var rng0 snapshotRange
					if i > 0 {
						rng0.Lo = &cuts[i-1]
					}
					if i < len(cuts) {
						rng0.Hi = &cuts[i]
					}
					if rng.IntN(4) == 0 {
						continue
					}
					if rng.IntN(3) > 0 {
						rng0.After = keys[rng.IntN(len(keys))]
					}
					st.ranges = append(st.ranges, &rng0)
					if rng.IntN(3) > 0 {
						ch := &chunk{stream: st, rng: &rng0}
						if rng.IntN(2) == 0 {
							ch.full, ch.last = true, keys[rng.IntN(len(keys))]
						}
						r.chunks = append(r.chunks, ch)
					}
				}
				spelled := make([]string, len(keys))
				for i, k := range keys {
					spelled[i] = strings.Join(k, "\x00")
				}
				got, err := r.place(t.Context(), st, spelled)
				if err != nil {
					t.Fatal(err)
				}
				// What readChunk's conditions say of each key.
				want := map[string]placed{}
				for _, k := range spelled {
					want[k] = placed{behind: true}
				}
				quoted := make([]string, len(names))
				for i, n := range names {
					quoted[i] = pgx.Identifier{n}.Sanitize()
				}
				for _, rng0 := range st.ranges {
					var held *chunk
					for _, ch := range r.chunks {
						if ch.rng == rng0 {
							held = ch
						}
					}
					var args []string
					arg := func(v string) string {
						args = append(args, v)
						return "$" + strconv.Itoa(len(args))
					}
					in := rng0.where(quoted[0], arg)
					q := "SELECT " + list + " FROM " + ident
					if len(in) > 0 {
						q += " WHERE " + strings.Join(in, " AND ")
					}
					for _, k := range texts(q, args...) {
						want[strings.Join(k, "\x00")] = placed{}
					}
					if held == nil {
						continue
					}
					args = nil
					conds := rng0.where(quoted[0], arg)
					if rng0.After != nil {
						conds = append(conds, afterKey(quoted, rng0.After, arg))
					}
					if held.full {
						conds = append(conds, "NOT "+afterKey(quoted, held.last, arg))
					}
					q = "SELECT " + list + " FROM " + ident
					if len(conds) > 0 {
						q += " WHERE " + strings.Join(conds, " AND ")
					}
					for _, k := range texts(q, args...) {
						want[strings.Join(k, "\x00")] = placed{chunk: held}
					}
				}
				for _, rng0 := range st.ranges {
					if rng0.After == nil {
						continue
					}
					var args []string
					arg := func(v string) string {
						args = append(args, v)
						return "$" + strconv.Itoa(len(args))
					}
					conds := append(rng0.where(quoted[0], arg), "NOT "+afterKey(quoted, rng0.After, arg))
					for _, k := range texts("SELECT "+list+" FROM "+ident+" WHERE "+strings.Join(conds, " AND "), args...) {
						want[strings.Join(k, "\x00")] = placed{behind: true}
					}
				}
				for i, k := range spelled {
					if got[i] != want[k] {
						t.Errorf("trial %d: %q placed %+v, want %+v; ranges %s", trial, keys[i], got[i], want[k], describe(st, r.chunks))
					}
				}
			}
		})
	}
}

func describe(st *cdcStream, chunks []*chunk) string {
	var parts []string
	for _, rng := range st.ranges {
		b, _ := json.Marshal(rng)
		p := string(b)
		for _, c := range chunks {
			if c.rng == rng {
				p += fmt.Sprintf(" held %p", c)
				if c.full {
					p += fmt.Sprintf(" to %q", c.last)
				}
			}
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, "; ")
}

// Text keys in a collation whose order isn't their bytes', and keys of
// several columns, which the server orders, start each row's history at the
// chunk that reads it, as integer keys do.
func TestCDCSnapshotReadsKeysInTheirOrder(t *testing.T) {
	spelled := map[int]string{2: "a", 4: "c", 6: "e", 8: "g", 10: "i", 12: "k", 3: "B", 5: "F", 7: "~x", 9: "Z", 11: "b", 13: "D", 14: "Ab", 15: "0", 17: "É"}
	key := func(id int) string { return spelled[id] }
	for _, collation := range []string{"C", "en-x-icu", "en_US.utf8"} {
		t.Run(collation, func(t *testing.T) {
			e := newSourceEnv(t)
			h := e.historyOf(6, `text COLLATE "`+collation+`"`, key)
			s, pipeline := e.cdcSource(`, "snapshotChunkRows": 3`)
			var chunks atomic.Int64
			afterChunkSnapshot = func() {
				switch chunks.Add(1) {
				case 1:
					h.insert(3)
					h.insert(5)
					h.insert(13)
					h.update(4)
				case 2:
					h.insert(7)
					h.insert(9)
					h.insert(11)
					h.insert(15)
					h.insert(17)
					h.update(2)
					h.move(12, 14)
				}
			}
			t.Cleanup(func() { afterChunkSnapshot = nil })
			got := readCDC(t, s, pipeline, map[string]string{"users": ""}).changes(t, "users")
			h.fromChunks(t, got)
			sameRows(t, merged(got), e.rows("users"))
		})
	}
	t.Run("several columns", func(t *testing.T) {
		e := newSourceEnv(t)
		e.exec(`CREATE TABLE $schema.users (id text COLLATE "en-x-icu", n int, name text, doc text, PRIMARY KEY (id, n))`)
		e.exec("INSERT INTO $schema.users SELECT id, n, 'old', NULL FROM unnest(ARRAY['a', 'b']) id, generate_series(1, 4) n")
		s, pipeline := e.cdcSource(`, "snapshotChunkRows": 3`)
		var chunks atomic.Int64
		afterChunkSnapshot = func() {
			switch chunks.Add(1) {
			case 1:
				// After the first chunk's snapshot, which reads (a, 1) to
				// (a, 3): in its keys, after them, and in another case.
				e.exec("INSERT INTO $schema.users VALUES ('a', 0, 'new', NULL)")
				e.exec("UPDATE $schema.users SET name = 'new' WHERE (id, n) IN (('a', 2), ('b', 1))")
				e.exec("INSERT INTO $schema.users VALUES ('a', 5, 'new', NULL), ('A', 9, 'new', NULL)")
				e.exec("DELETE FROM $schema.users WHERE (id, n) = ('b', 2)")
			case 2:
				// The second reads (a, 4), (a, 5) and (A, 9): behind it,
				// among its keys, and after them.
				e.exec("UPDATE $schema.users SET name = 'newer' WHERE (id, n) IN (('a', 1), ('a', 4), ('b', 3))")
				e.exec("INSERT INTO $schema.users VALUES ('a', 10, 'new', NULL)")
			}
		}
		t.Cleanup(func() { afterChunkSnapshot = nil })
		c := readCDC(t, s, pipeline, map[string]string{"users": ""})
		ids, ns, names, ops, seqs := c.column("users", "id"), c.column("users", "n"), c.column("users", "name"), c.column("users", "openetl.op"), c.changes(t, "users")
		history := map[string][]string{}
		for i := range ids {
			k := ids[i] + "/" + ns[i]
			switch {
			case seqs[i].read():
				history[k] = append(history[k], "chunk "+names[i])
			case ops[i] == source.OpDelete:
				history[k] = append(history[k], "delete")
			default:
				history[k] = append(history[k], ops[i]+" "+names[i])
			}
		}
		for k, want := range map[string][]string{
			"a/0": {"insert new"}, "a/1": {"chunk old", "update newer"}, "a/2": {"chunk old", "update new"}, "a/3": {"chunk old"},
			"a/4": {"chunk old", "update newer"}, "a/5": {"chunk new"}, "A/9": {"chunk new"}, "a/10": {"insert new"},
			"b/1": {"chunk new"}, "b/2": nil, "b/3": {"chunk newer"}, "b/4": {"chunk old"},
		} {
			if !slices.Equal(history[k], want) {
				t.Errorf("appended, %s is %q, want %q", k, history[k], want)
			}
		}
		if len(history) != 11 {
			t.Errorf("keys %v", history)
		}
	})
}

// frozen gives the read each snapshot it asks for, a chunk's or one asking
// what's visible, as one exported at that moment, under a lock, so the
// snapshots it gives are in the order they were asked for. Now and then it
// gives one again for the next few, as snapshots are while the transactions
// committed since are still in the proc array.
type frozen struct {
	t     *testing.T
	mu    sync.Mutex
	rng   *mrand.Rand
	conns []*pgx.Conn
	next  int
	id    string
	left  int
	// How many snapshots it gave again.
	again int
}

func newFrozen(t *testing.T, seed uint64) *frozen {
	f := &frozen{t: t, rng: mrand.New(mrand.NewPCG(seed, 2))}
	for range 16 {
		conn, err := pgx.Connect(t.Context(), dsn())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		f.conns = append(f.conns, conn)
	}
	return f
}

func (f *frozen) from() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.left > 0 {
		f.left--
		f.again++
		return f.id
	}
	// A connection's export is in use until its importer has taken it, long
	// before the ring comes round to it again.
	conn := f.conns[f.next%len(f.conns)]
	f.next++
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "ROLLBACK"); err != nil {
		f.t.Error(err)
	}
	if _, err := conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ"); err != nil {
		f.t.Error(err)
	}
	if err := conn.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&f.id); err != nil {
		f.t.Error(err)
	}
	if f.rng.IntN(3) == 0 {
		f.left = 1 + f.rng.IntN(8)
	}
	return f.id
}

// cuts is a capture that cancels its read at a resume point at chosen, and
// keeps what each stream's last resume point covers, as a checkpoint does.
type cuts struct {
	*capture
	cancel context.CancelFunc
	at     func(stream, state string) bool
	cut    map[string]int
	state  map[string]string
}

func (c *cuts) ResumePoint(stream string, state []byte) error {
	if err := c.capture.ResumePoint(stream, state); err != nil {
		return err
	}
	c.mu.Lock()
	c.cut[stream], c.state[stream] = len(c.batches[stream]), string(state)
	c.mu.Unlock()
	if c.at(stream, string(state)) {
		c.cancel()
	}
	return nil
}

// checkpointed drops what each stream's last resume point doesn't cover, and
// gives the streams' states there.
func (c *cuts) checkpointed(streams map[string]string) map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]string{}
	for stream, was := range streams {
		for _, b := range c.batches[stream][c.cut[stream]:] {
			b.Release()
		}
		if c.batches != nil {
			c.batches[stream] = c.batches[stream][:c.cut[stream]]
		}
		out[stream] = cmpOr(c.state[stream], was)
	}
	return out
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// Writers change two tables throughout a read of their snapshots in small
// chunks, which stops at a resume point it picks at random and resumes there,
// a few times, on as many lanes as it picks. They insert rows, update and
// delete them, those that were there before the read too, and change their
// keys, in transactions that change one table or both. One table's keys are
// integers and the other's text, which the server places. With frozen, the
// read's snapshots now and then don't see the transactions committed since
// the one before. Appended, what's checkpointed holds each key's history from
// where a chunk read it; merged, it's each table.
func TestCDCSnapshotHistoriesUnderWritesAndRestarts(t *testing.T) {
	const letters = "qQrR"
	mixed := func(id int) string { return fmt.Sprintf("%c%04d", letters[id%4], id) }
	type kind struct {
		typ string
		key func(int) string
	}
	ints, texts := kind{"int", strconv.Itoa}, kind{`text COLLATE "en-x-icu"`, mixed}
	for _, c := range []struct {
		name         string
		users, items kind
		lanes        []int
		freeze       bool
	}{
		{"on one lane", ints, texts, []int{1, 1, 1, 1, 1}, false},
		{"in ranges, snapshots frozen", ints, texts, []int{4, 2, 3, 4, 1}, true},
		{"on one lane, snapshots frozen", texts, ints, []int{1, 1, 1, 1, 1}, true},
		{"in ranges", texts, ints, []int{4, 3, 1, 4, 2}, false},
	} {
		for run := range *historyRuns {
			t.Run(fmt.Sprintf("%s/%d", c.name, run), func(t *testing.T) {
				seed := uint64(time.Now().UnixNano())
				t.Logf("seed %d", seed)
				rng := mrand.New(mrand.NewPCG(seed, 0))
				e := newSourceEnv(t)
				hs := []*history{e.tableHistory("users", 1500, c.users.typ, c.users.key), e.tableHistory("items", 100, c.items.typ, c.items.key)}
				e.exec("ANALYZE $schema.users")
				e.exec("ANALYZE $schema.items")
				s, pipeline := e.cdcSource(`, "snapshotChunkRows": 5`)
				// Chunks wait a little before and after their snapshots, so the
				// writers' changes land in the gaps a chunk has.
				var mu sync.Mutex
				pause := mrand.New(mrand.NewPCG(seed, 1))
				wait := func() {
					mu.Lock()
					d := time.Duration(pause.IntN(1500)) * time.Microsecond
					mu.Unlock()
					time.Sleep(d)
				}
				beforeChunkSnapshot, afterChunkSnapshot = wait, wait
				var f *frozen
				if c.freeze {
					f = newFrozen(t, seed)
					snapshotFrom = f.from
				}
				t.Cleanup(func() { beforeChunkSnapshot, afterChunkSnapshot, snapshotFrom = nil, nil, nil })

				ctx, stop := context.WithCancel(t.Context())
				var writers sync.WaitGroup
				defer writers.Wait()
				defer stop()
				for w := range 4 {
					wseed := rng.Uint64()
					writers.Go(func() { changeOwnKeys(ctx, t, hs, w, 4, 6000, mrand.New(mrand.NewPCG(wseed, uint64(w)))) })
				}
				var got = map[string][]change{}
				states := map[string]string{"users": "", "items": ""}
				for _, lanes := range c.lanes {
					at := 1 + rng.IntN(80)
					rctx, cancel := context.WithCancel(t.Context())
					n := 0
					cut := &cuts{capture: &capture{}, cancel: cancel, cut: map[string]int{}, state: map[string]string{}, at: func(string, string) bool {
						n++
						return n >= at
					}}
					req := cdcRequest(pipeline, true, states)
					req.Lanes = lanes
					readTo(t, s, rctx, cancel, req, cut, fmt.Sprintf("its resume point %d", at))
					states = cut.checkpointed(states)
					for stream := range states {
						got[stream] = append(got[stream], cut.changes(t, stream)...)
					}
					cut.release()
				}
				stop()
				writers.Wait()
				var end string
				if err := e.conn.QueryRow(t.Context(), "SELECT pg_current_wal_lsn()::text").Scan(&end); err != nil {
					t.Fatal(err)
				}
				endLSN, _ := pglogrepl.ParseLSN(end)
				rctx, cancel := context.WithCancel(t.Context())
				caught := map[string]bool{}
				last := &cuts{capture: &capture{}, cancel: cancel, cut: map[string]int{}, state: map[string]string{}, at: func(stream, state string) bool {
					var st cdcState
					caught[stream] = json.Unmarshal([]byte(state), &st) == nil && st.Snapshot == nil && pglogrepl.LSN(st.LSN) >= endLSN
					return caught["users"] && caught["items"]
				}}
				t.Cleanup(last.release)
				req := cdcRequest(pipeline, true, states)
				req.Lanes = 2
				readTo(t, s, rctx, cancel, req, last, "the log's end, "+end)
				last.checkpointed(states)
				for i, stream := range []string{"users", "items"} {
					got[stream] = append(got[stream], last.changes(t, stream)...)
					h := hs[i]
					h.mu.Lock()
					t.Logf("%s: %d changes written; %d appended, %d of them read by chunks", stream, len(h.want), len(got[stream]), len(slices.DeleteFunc(slices.Clone(got[stream]), func(ch change) bool { return !ch.read() })))
					h.mu.Unlock()
					h.fromChunks(t, got[stream])
					sameRows(t, merged(got[stream]), e.rows(stream))
				}
				if f != nil {
					f.mu.Lock()
					t.Logf("%d snapshots of %d given again", f.again, f.next+f.again)
					f.mu.Unlock()
				}
			})
		}
	}
}

// readTo reads until the read's cut cancels it at what, and fails t if that
// takes over three minutes or the read fails.
func readTo(t *testing.T, s *Source, ctx context.Context, cancel context.CancelFunc, req source.ReadRequest, cut *cuts, what string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.Read(ctx, req, cut) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("a read to %s: %v", what, err)
		}
	case <-time.After(3 * time.Minute):
		cancel()
		<-done
		t.Fatalf("a read didn't reach %s in three minutes; last states %q", what, cut.state)
	}
	cancel()
}

// changeOwnKeys has writer w of n change, on a connection of its own until
// ctx ends, only ids below ids that it owns, those w past a multiple of n, in
// each of hs: inserts, updates, deletes and key changes, of rows it inserted
// and of those there before, keeping at most half of its ids live, in
// transactions of one to three changes to any of hs.
func changeOwnKeys(ctx context.Context, t *testing.T, hs []*history, w, n, ids int, rng *mrand.Rand) {
	conn, err := pgx.Connect(context.Background(), dsn())
	if err != nil {
		t.Error(err)
		return
	}
	defer func() { _ = conn.Close(context.Background()) }()
	type keys struct {
		h    *history
		live []int
		has  map[int]bool
	}
	var tables []*keys
	for _, h := range hs {
		k := &keys{h: h.on(conn), has: map[int]bool{}}
		ours := map[string]int{}
		for id := w; id < ids; id += n {
			ours[h.key(id)] = id
		}
		h.mu.Lock()
		for _, want := range h.want {
			if id, ok := ours[strings.Fields(want)[1]]; ok && !k.has[id] {
				k.live, k.has[id] = append(k.live, id), true
			}
		}
		h.mu.Unlock()
		tables = append(tables, k)
	}
	for ctx.Err() == nil {
		var ops []op
		for range 1 + rng.IntN(3) {
			k := tables[rng.IntN(len(tables))]
			fresh := func() int {
				for {
					if id := n*rng.IntN(ids/n) + w; id > 0 && !k.has[id] {
						return id
					}
				}
			}
			o := rng.IntN(20)
			// At most half its ids are live, so a fresh one is quick to find.
			if len(k.has) >= ids/n/2 && o < 6 {
				o = 6
			}
			switch {
			case len(k.live) == 0 || o < 6:
				id := fresh()
				ops = append(ops, k.h.inserting(id))
				k.live, k.has[id] = append(k.live, id), true
			case o < 9:
				i := rng.IntN(len(k.live))
				ops = append(ops, k.h.deleting(k.live[i]))
				delete(k.has, k.live[i])
				k.live = slices.Delete(k.live, i, i+1)
			case o < 13:
				i := rng.IntN(len(k.live))
				to := fresh()
				ops = append(ops, k.h.moving(k.live[i], to))
				delete(k.has, k.live[i])
				k.live[i], k.has[to] = to, true
			default:
				ops = append(ops, k.h.updating(k.live[rng.IntN(len(k.live))]))
			}
		}
		tables[0].h.run(ops...)
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(rng.IntN(3)) * time.Millisecond):
		}
	}
}

// A transaction that changes keys of two ranges' held chunks, which their
// snapshots didn't see, can be read from the log before new snapshots see
// it. The resume point when one chunk passes, while the other still holds
// the change, waits until new snapshots see the transaction, since a read
// resumed there reads the held chunk's keys again, and the log no more.
func TestCDCResumePointWaitsForATransactionAHeldChunkHolds(t *testing.T) {
	e := newSourceEnv(t)
	h := e.newHistory(400)
	e.exec("ANALYZE $schema.users")
	e.exec("CREATE TABLE $schema.other (id int)")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 200`)
	old := &stale{id: exported(t)}
	other, err := pgx.Connect(t.Context(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close(context.Background()) })
	var arrived, roles atomic.Int64
	both, wrote := make(chan struct{}), make(chan struct{})
	afterChunkSnapshot = func() {
		// Both chunks have their snapshots before the transaction; the first
		// takes its position after it, the second after another write.
		if arrived.Add(1) == 2 {
			close(both)
		}
		<-both
		if roles.Add(1) == 1 {
			h.run(h.updating(4), h.updating(796))
			old.left.Store(3)
			close(wrote)
			return
		}
		<-wrote
		time.Sleep(100 * time.Millisecond)
		if _, err := other.Exec(context.Background(), "INSERT INTO "+pgx.Identifier{e.schema, "other"}.Sanitize()+" VALUES (1)"); err != nil {
			t.Error(err)
		}
	}
	snapshotFrom = old.from
	t.Cleanup(func() { afterChunkSnapshot, snapshotFrom = nil, nil })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	left := int64(-1)
	cut := &cuts{capture: &capture{}, cancel: cancel, cut: map[string]int{}, state: map[string]string{}, at: func(_, state string) bool {
		st := stateOf(t, state)
		if st.Snapshot == nil || len(st.Snapshot.Ranges) != 1 {
			return false
		}
		left = old.left.Load()
		return true
	}}
	t.Cleanup(cut.release)
	req := cdcRequest(pipeline, false, map[string]string{"users": ""})
	req.Lanes = 2
	if err := s.Read(ctx, req, cut); !errors.Is(err, context.Canceled) {
		if err == nil {
			t.Skipf("both chunks passed together: %q", cut.log)
		}
		t.Fatalf("the first read: %v", err)
	}
	if left > 0 {
		t.Errorf("the resume point with a chunk still held came with %d snapshots that don't see the transaction it holds left", left)
	}
	states := cut.checkpointed(map[string]string{"users": ""})
	rest := e.readCDCLanes(s, pipeline, 2, states)
	got := append(cut.changes(t, "users"), rest.changes(t, "users")...)
	h.fromChunks(t, got)
	sameRows(t, merged(got), e.rows("users"))
}

// A transaction whose changes only a chunk held, which passed since, isn't
// waited for: a read resumed after the chunk reads its keys from the log,
// and the transaction is before where it resumes.
func TestCDCResumePointDoesntWaitForWhatAPassedChunkHeld(t *testing.T) {
	e := newSourceEnv(t)
	h := e.newHistory(40)
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10`)
	old := &stale{id: exported(t)}
	var chunks atomic.Int64
	afterChunkSnapshot = func() {
		if chunks.Add(1) == 1 {
			h.update(4)
			old.left.Store(1 << 40)
		}
	}
	snapshotFrom = old.from
	was := visibleWithin
	visibleWithin = 300 * time.Millisecond
	t.Cleanup(func() { afterChunkSnapshot, snapshotFrom, visibleWithin = nil, nil, was })
	got := readCDC(t, s, pipeline, map[string]string{"users": ""}).changes(t, "users")
	h.fromChunks(t, got)
	if got := appended(got, "4"); !slices.Equal(got, []string{"chunk n4", "update 4 w1"}) {
		t.Errorf("appended, 4 is %q", got)
	}
}

// A held chunk whose changes since its snapshot outgrow what it may hold, its
// rows' bytes or batchBytes, is dropped and read again: here, the last
// chunk, which holds the rest of the table, as rows are inserted at its end
// in transactions that commit before its position.
func TestCDCDropsAChunkWhoseChangesOutgrowIt(t *testing.T) {
	e := newSourceEnv(t)
	h := e.newHistory(20)
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 10, "batchBytes": 64`)
	var chunks atomic.Int64
	afterChunkSnapshot = func() {
		if chunks.Add(1) == 2 {
			for id := 41; id < 100; id += 2 {
				h.insert(id)
			}
			h.update(24)
		}
	}
	t.Cleanup(func() { afterChunkSnapshot = nil })
	got := readCDC(t, s, pipeline, map[string]string{"users": ""}).changes(t, "users")
	// Held to its end, the second chunk would have read the table; dropped,
	// the rest of it, 40 rows now, is read 10 at a time.
	if n := chunks.Load(); n != 6 {
		t.Errorf("%d chunks, want 6", n)
	}
	h.fromChunks(t, got)
	if got := appended(got, "41"); !slices.Equal(got, []string{"chunk w1"}) {
		t.Errorf("appended, 41 is %q", got)
	}
	sameRows(t, merged(got), e.rows("users"))
}
