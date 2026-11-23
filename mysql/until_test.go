//go:build integration

package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// eventLog is a read's records, resume points and completions, in order.
type eventLog struct {
	mu     sync.Mutex
	events []logEvent
}

type logEvent struct {
	resume, complete bool
	state            string
	rows             []map[string]string
}

func (l *eventLog) add(e logEvent) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
	return nil
}

func (l *eventLog) Rows(string, []json.RawMessage) error { return errors.New("unexpected raw rows") }
func (l *eventLog) Records(_ string, b arrow.RecordBatch) error {
	defer b.Release()
	return l.add(logEvent{rows: batchRows(b)})
}
func (l *eventLog) ResumePoint(_ string, s []byte) error {
	return l.add(logEvent{resume: true, state: string(s)})
}
func (l *eventLog) Complete(_ string, s []byte) error {
	return l.add(logEvent{complete: true, state: string(s)})
}
func (l *eventLog) Split(string, string) source.SplitEmitter { return logSplit{l} }

type logSplit struct{ l *eventLog }

func (o logSplit) Rows([]json.RawMessage) error                  { return errors.New("unexpected raw rows") }
func (o logSplit) Records(b arrow.RecordBatch) error             { return o.l.Records("", b) }
func (o logSplit) ResumePoint(s []byte, _ ...source.Split) error { return o.l.ResumePoint("", s) }
func (o logSplit) Complete(s []byte, _ ...source.Split) error    { return o.l.Complete("", s) }

func readLog(t *testing.T, s *Source, cs source.ConfiguredStream) []logEvent {
	t.Helper()
	l := &eventLog{}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Pipeline: "until", Lanes: 1}, l); err != nil {
		t.Fatal(err)
	}
	return l.events
}

// rowsOf are the rows of events[from+1:to].
func rowsOf(events []logEvent, from, to int) []map[string]string {
	var out []map[string]string
	for _, e := range events[from+1 : to] {
		out = append(out, e.rows...)
	}
	return out
}

// endsAt fails unless a read's last events are a resume point at until and
// its completion there, with no records after them.
func endsAt(t *testing.T, events []logEvent, until string) {
	t.Helper()
	n := len(events)
	if n < 2 || !events[n-2].resume || events[n-2].state != until || !events[n-1].complete || events[n-1].state != until {
		t.Fatalf("a read until %s ended with %+v", until, events[max(n-2, 0):])
	}
}

func resumePoints(events []logEvent) []int {
	var out []int
	for i, e := range events {
		if e.resume {
			out = append(out, i)
		}
	}
	return out
}

// changes are the rows the binlog gave, not a snapshot: their seq's place in
// the transaction isn't 0.
func changes(rows []map[string]string, withSeq bool) []string {
	mask := new(big.Int).Lsh(big.NewInt(1), 32)
	var out []string
	for _, r := range rows {
		seq, _ := new(big.Int).SetString(r["openetl.seq"], 10)
		if new(big.Int).Mod(seq, mask).Sign() == 0 {
			continue
		}
		s := r["openetl.op"] + " " + r["id"] + " " + r["v"]
		if withSeq {
			s += " " + r["openetl.seq"]
		}
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}

func values(rows map[string]map[string]string) map[string]string {
	out := map[string]string{}
	for k, r := range rows {
		out[k] = r["v"]
	}
	return out
}

// TestCDCReadStopsAtUntil reads a table by CDC while it changes, then reads
// each part between two of the read's resume points again: the binlog's
// changes come again exactly, nothing past the second point comes, and the
// snapshot's rows of that part come as the table has them now, so that the
// read again, between the first read's other parts, lands as the first read
// did.
func TestCDCReadStopsAtUntil(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY, v varchar(20))")
		e.exec("INSERT INTO $db.items SELECT i, CONCAT('v', i) FROM " + numbers(60))
		s := e.source(`, "snapshotChunkRows": 10`)
		var wg sync.WaitGroup
		chunks := 0
		afterChunkSnapshot = func() {
			chunks++
			if chunks > 4 {
				return
			}
			n := chunks
			done := make(chan struct{})
			db := quoteIdent(e.database)
			wg.Go(func() {
				defer close(done)
				for _, q := range []string{
					fmt.Sprintf("UPDATE %s.items SET v = 'w%d' WHERE id IN (%d, %d)", db, n, n*3, 60-n*7),
					fmt.Sprintf("DELETE FROM %s.items WHERE id = %d", db, 20+n*5),
					fmt.Sprintf("INSERT INTO %s.items VALUES (%d, 'new')", db, 1000+n),
				} {
					if _, err := e.db.ExecContext(t.Context(), q); err != nil {
						t.Errorf("%s: %v", q, err)
					}
				}
			})
			select {
			case <-done:
			case <-time.After(20 * time.Millisecond):
			}
		}
		t.Cleanup(func() { afterChunkSnapshot = nil })
		cs := source.ConfiguredStream{Name: "items", SyncMode: source.CDC}
		first := readLog(t, s, cs)
		wg.Wait()
		afterChunkSnapshot = nil
		cs.State = []byte(first[len(first)-1].state)
		tailEvents := readLog(t, s, cs)
		tail := rowsOf(tailEvents, -1, len(tailEvents))
		cs.State = nil

		want := map[string]string{}
		rows, err := e.db.QueryContext(t.Context(), "SELECT id, v FROM "+quoteIdent(e.database)+".items")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id, v string
			if err := rows.Scan(&id, &v); err != nil {
				t.Fatal(err)
			}
			want[id] = v
		}
		_ = rows.Close()
		all := append(rowsOf(first, -1, len(first)), tail...)
		if got := values(merged(t, all, "id")); !maps.Equal(got, want) {
			t.Fatalf("the first read lands as %v, the table is %v", got, want)
		}

		points := resumePoints(first)
		if len(points) < 4 {
			t.Fatalf("%d resume points", len(points))
		}
		for k, j := range points {
			from := -1
			cs.State = nil
			if k > 0 {
				from = points[k-1]
				cs.State = []byte(first[from].state)
			}
			cs.Until = []byte(first[j].state)
			again := readLog(t, s, cs)
			endsAt(t, again, first[j].state)
			between := rowsOf(first, from, j)
			if got, want := changes(rowsOf(again, -1, len(again)), true), changes(between, true); !slices.Equal(got, want) {
				t.Fatalf("resume point %d: the binlog's changes read again are %q, not %q", k+1, got, want)
			}
			parts := append(append(append(rowsOf(first, -1, max(from, 0)), rowsOf(again, -1, len(again))...), rowsOf(first, j, len(first))...), tail...)
			if got := values(merged(t, parts, "id")); !maps.Equal(got, want) {
				t.Fatalf("resume point %d: read again, the parts land as %v, not %v", k+1, got, want)
			}
		}

		// On another server, by GTIDs: the same changes, from another epoch.
		other := func(state string) []byte {
			var m map[string]any
			if err := json.Unmarshal([]byte(state), &m); err != nil {
				t.Fatal(err)
			}
			m["server"] = "another"
			b, _ := json.Marshal(m)
			return b
		}
		k := len(points) / 2
		cs.State, cs.Until = other(first[points[k-1]].state), other(first[points[k]].state)
		again := readLog(t, s, cs)
		endsAt(t, again, string(cs.Until))
		redo := rowsOf(again, -1, len(again))
		if got, want := changes(redo, false), changes(rowsOf(first, points[k-1], points[k]), false); !slices.Equal(got, want) {
			t.Fatalf("by GTIDs, the binlog's changes read again are %q, not %q", got, want)
		}
		// The snapshot's rows come after the changes the redo read.
		var last, lastChange string
		for _, r := range redo {
			if len(changes([]map[string]string{r}, false)) > 0 {
				lastChange = max(lastChange, fmt.Sprintf("%040s", r["openetl.seq"]))
			} else {
				last = fmt.Sprintf("%040s", r["openetl.seq"])
			}
		}
		if last != "" && last <= lastChange {
			t.Fatalf("by GTIDs, the snapshot's rows have seq %s, below the changes' %s", last, lastChange)
		}
	})
}

// TestSplitReadStopsAtUntil reads a split, and an incremental read, again
// between two of their resume points.
func TestSplitReadStopsAtUntil(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.big (id int PRIMARY KEY, v int, at datetime NOT NULL)")
		e.exec("INSERT INTO $db.big SELECT i, i, DATE_ADD('2026-01-01', INTERVAL i % 17 HOUR) FROM " + numbers(3000))
		e.exec("ANALYZE TABLE $db.big")
		s := e.source(`, "chunkRows": 10`)
		for _, cs := range []source.ConfiguredStream{
			{Name: "big", SyncMode: source.FullRefresh},
			{Name: "big", SyncMode: source.Incremental, Cursor: "at"},
		} {
			splits, err := s.Splits(t.Context(), cs, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(splits) < 2 {
				t.Fatalf("%d splits", len(splits))
			}
			sp := splits[1]
			cs.Splits = []source.Split{sp}
			first := readLog(t, s, cs)
			points := resumePoints(first)
			if len(points) < 6 {
				t.Fatalf("%v: %d resume points", cs.SyncMode, len(points))
			}
			for _, k := range []int{0, 2, len(points) - 1} {
				from := -1
				sp.State = splits[1].State
				if k > 0 {
					from = points[k-1]
					sp.State = []byte(first[from].state)
				}
				sp.Until = []byte(first[points[k]].state)
				cs.Splits = []source.Split{sp}
				again := readLog(t, s, cs)
				endsAt(t, again, string(sp.Until))
				ids := func(rows []map[string]string) []string {
					var out []string
					for _, r := range rows {
						out = append(out, r["id"])
					}
					return out
				}
				if got, want := ids(rowsOf(again, -1, len(again))), ids(rowsOf(first, from, points[k])); !slices.Equal(got, want) {
					t.Fatalf("%v, resume point %d: read again %s, not %s", cs.SyncMode, k+1, strings.Join(got, " "), strings.Join(want, " "))
				}
			}
		}
	})
}

// TestReadCompletesAtCompleteAt reads a table by CDC, and incrementally with
// a key and without, from a completion to the next, then reads that part
// again, asked to complete where it completed, once the binlog and the
// cursor have moved on: it returns the same rows, and completes with the
// same state, not where the table is now.
func TestReadCompletesAtCompleteAt(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY, v varchar(20))")
		e.exec("CREATE TABLE $db.ev (id int PRIMARY KEY, v varchar(20), at int)")
		e.exec("CREATE TABLE $db.loose (v varchar(20), at int)")
		e.exec("INSERT INTO $db.items SELECT i, CONCAT('v', i) FROM " + numbers(10))
		e.exec("INSERT INTO $db.ev SELECT i, CONCAT('v', i), i FROM " + numbers(10))
		e.exec("INSERT INTO $db.loose SELECT CONCAT('v', i), i FROM " + numbers(10))
		n := 0
		change := func() {
			n++
			e.exec("INSERT INTO $db.items VALUES (?, ?)", 100+n, fmt.Sprint("r", n))
			e.exec("UPDATE $db.items SET v = ? WHERE id = ?", fmt.Sprint("r", n), n)
			e.exec("INSERT INTO $db.ev VALUES (?, ?, ?)", 100+n, fmt.Sprint("r", n), 10+n)
			e.exec("INSERT INTO $db.loose VALUES (?, ?)", fmt.Sprint("r", n), 10+n)
		}
		s := e.source(`, "chunkRows": 3`)
		for _, cs := range []source.ConfiguredStream{
			{Name: "items", SyncMode: source.CDC},
			{Name: "ev", SyncMode: source.Incremental, Cursor: "at"},
			{Name: "loose", SyncMode: source.Incremental, Cursor: "at"},
		} {
			first := readLog(t, s, cs)
			change()
			cs.State = []byte(first[len(first)-1].state)
			second := readLog(t, s, cs)
			end := second[len(second)-1]
			change()
			cs.CompleteAt = []byte(end.state)
			again := readLog(t, s, cs)
			if last := again[len(again)-1]; !end.complete || !last.complete || last.state != end.state {
				t.Fatalf("%s: asked to complete at %s, the read ended with %+v", cs.Name, end.state, last)
			}
			if got, want := rowTexts(rowsOf(again, -1, len(again))), rowTexts(rowsOf(second, -1, len(second))); !slices.Equal(got, want) {
				t.Fatalf("%s: read again to its completion %q, not %q", cs.Name, got, want)
			}
		}
		// From no state, as a new stream's first epoch: the snapshot's rows
		// come as the table is now, and none of the binlog's changes after
		// the completion.
		cs := source.ConfiguredStream{Name: "items", SyncMode: source.CDC}
		first := readLog(t, s, cs)
		change()
		cs.CompleteAt = []byte(first[len(first)-1].state)
		again := readLog(t, s, cs)
		if last := again[len(again)-1]; !last.complete || last.state != string(cs.CompleteAt) {
			t.Fatalf("from no state, asked to complete at %s, the read ended with %+v", cs.CompleteAt, last)
		}
		if c := changes(rowsOf(again, -1, len(again)), false); len(c) > 0 {
			t.Fatalf("from no state, the read sent changes past its completion: %q", c)
		}
	})
}

// rowTexts are rows as sorted text, each with its columns in name order.
func rowTexts(rows []map[string]string) []string {
	var out []string
	for _, r := range rows {
		var b strings.Builder
		for _, k := range slices.Sorted(maps.Keys(r)) {
			fmt.Fprintf(&b, "%s=%s ", k, r[k])
		}
		out = append(out, b.String())
	}
	slices.Sort(out)
	return out
}

// batchRows are a batch's rows, each column as Arrow prints it.
func batchRows(b arrow.RecordBatch) []map[string]string {
	var out []map[string]string
	for i := range int(b.NumRows()) {
		row := map[string]string{}
		for j, f := range b.Schema().Fields() {
			if col := b.Column(j); col.IsNull(i) {
				row[f.Name] = array.NullValueStr
			} else {
				row[f.Name] = col.ValueStr(i)
			}
		}
		out = append(out, row)
	}
	return out
}
