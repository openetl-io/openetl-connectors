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
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TYPE $schema.mood AS ENUM ('sad', 'ok', 'happy', 'glad')")
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

// Writers change a table throughout a read of its snapshot in small chunks,
// which stops at a resume point it picks at random and resumes there, a few
// times, on as many lanes as it picks: they insert rows, update and delete
// them, those that were there before the read too, and change their keys.
// Appended, what's checkpointed holds each key's history from where a chunk
// read it; merged, it's the table.
func TestCDCSnapshotHistoriesUnderWritesAndRestarts(t *testing.T) {
	const letters = "qQrR"
	mixed := func(id int) string { return fmt.Sprintf("%c%04d", letters[id%4], id) }
	for _, c := range []struct {
		name  string
		typ   string
		key   func(int) string
		lanes []int
	}{
		{"integer key on one lane", "int", strconv.Itoa, []int{1, 1, 1, 1, 1}},
		{"integer key in ranges", "int", strconv.Itoa, []int{4, 2, 3, 4, 1}},
		{"text key on one lane", `text COLLATE "en-x-icu"`, mixed, []int{1, 1, 1, 1, 1}},
		{"text key in ranges", `text COLLATE "en-x-icu"`, mixed, []int{4, 3, 1, 4, 2}},
	} {
		for run := range *historyRuns {
			t.Run(fmt.Sprintf("%s/%d", c.name, run), func(t *testing.T) {
				seed := uint64(time.Now().UnixNano())
				t.Logf("seed %d", seed)
				rng := mrand.New(mrand.NewPCG(seed, 0))
				e := newSourceEnv(t)
				h := e.historyOf(1500, c.typ, c.key)
				e.exec("ANALYZE $schema.users")
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
				t.Cleanup(func() { beforeChunkSnapshot, afterChunkSnapshot = nil, nil })

				ctx, stop := context.WithCancel(t.Context())
				var writers sync.WaitGroup
				defer writers.Wait()
				defer stop()
				for w := range 4 {
					wseed := rng.Uint64()
					writers.Go(func() { changeOwnKeys(ctx, t, h, w, 4, 6000, mrand.New(mrand.NewPCG(wseed, uint64(w)))) })
				}
				var got []change
				state := ""
				for _, lanes := range c.lanes {
					at := 1 + rng.IntN(40)
					rctx, cancel := context.WithCancel(t.Context())
					n := 0
					cut := &cutCapture{capture: &capture{}, stream: "users", cancel: cancel, at: func(string) bool {
						n++
						return n >= at
					}}
					req := cdcRequest(pipeline, true, map[string]string{"users": state})
					req.Lanes = lanes
					readTo(t, s, rctx, cancel, req, cut, fmt.Sprintf("its resume point %d", at))
					state = cut.checkpointed()
					got = append(got, cut.changes(t, "users")...)
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
				last := &cutCapture{capture: &capture{}, stream: "users", cancel: cancel, at: func(state string) bool {
					var st cdcState
					return json.Unmarshal([]byte(state), &st) == nil && st.Snapshot == nil && pglogrepl.LSN(st.LSN) >= endLSN
				}}
				t.Cleanup(last.release)
				req := cdcRequest(pipeline, true, map[string]string{"users": state})
				req.Lanes = 2
				readTo(t, s, rctx, cancel, req, last, "the log's end, "+end)
				last.checkpointed()
				got = append(got, last.changes(t, "users")...)
				h.mu.Lock()
				t.Logf("%d changes written; %d appended, %d of them read by chunks", len(h.want), len(got), len(slices.DeleteFunc(slices.Clone(got), func(ch change) bool { return !ch.read() })))
				h.mu.Unlock()
				h.fromChunks(t, got)
				sameRows(t, merged(got), e.rows("users"))
			})
		}
	}
}

// readTo reads until the read's cut cancels it at what, and fails t if that
// takes over three minutes or the read fails.
func readTo(t *testing.T, s *Source, ctx context.Context, cancel context.CancelFunc, req source.ReadRequest, cut *cutCapture, what string) {
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
		t.Fatalf("a read didn't reach %s in three minutes; last state %s", what, cut.last("users"))
	}
	cancel()
}

// changeOwnKeys has writer w of n change, on a connection of its own until
// ctx ends, only ids below ids that it owns, those w past a multiple of n:
// inserts, updates, deletes and key changes, of rows it inserted and of
// those there before, keeping at most half of its ids live.
func changeOwnKeys(ctx context.Context, t *testing.T, h *history, w, n, ids int, rng *mrand.Rand) {
	conn, err := pgx.Connect(context.Background(), dsn())
	if err != nil {
		t.Error(err)
		return
	}
	defer func() { _ = conn.Close(context.Background()) }()
	hw := h.on(conn)
	ours := map[string]int{}
	for id := w; id < ids; id += n {
		ours[h.key(id)] = id
	}
	var live []int
	has := map[int]bool{}
	h.mu.Lock()
	for _, want := range h.want {
		if id, ok := ours[strings.Fields(want)[1]]; ok && !has[id] {
			live, has[id] = append(live, id), true
		}
	}
	h.mu.Unlock()
	fresh := func() int {
		for {
			if id := n*rng.IntN(ids/n) + w; id > 0 && !has[id] {
				return id
			}
		}
	}
	for ctx.Err() == nil {
		op := rng.IntN(20)
		// At most half its ids are live, so a fresh one is quick to find.
		if len(has) >= ids/n/2 && op < 6 {
			op = 6
		}
		switch {
		case len(live) == 0 || op < 6:
			id := fresh()
			hw.insert(id)
			live, has[id] = append(live, id), true
		case op < 9:
			i := rng.IntN(len(live))
			hw.delete(live[i])
			delete(has, live[i])
			live = slices.Delete(live, i, i+1)
		case op < 13:
			i := rng.IntN(len(live))
			to := fresh()
			hw.move(live[i], to)
			delete(has, live[i])
			live[i], has[to] = to, true
		default:
			hw.update(live[rng.IntN(len(live))])
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(rng.IntN(3)) * time.Millisecond):
		}
	}
}
