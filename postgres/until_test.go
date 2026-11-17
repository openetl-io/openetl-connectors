//go:build integration

package postgres

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pglogrepl"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// readUntil reads one stream from a state to an until, as a redo of an
// epoch's part does: whole if split is nil, or else as that split.
func readUntil(t *testing.T, s *Source, cs source.ConfiguredStream, from, until string, split *source.Split) *capture {
	t.Helper()
	if split != nil {
		split.Until = []byte(until)
		if from != "" {
			split.State = []byte(from)
		}
		cs.Splits = []source.Split{*split}
	} else {
		cs.Until = []byte(until)
		if from != "" {
			cs.State = []byte(from)
		}
	}
	c := &capture{}
	t.Cleanup(c.release)
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c); err != nil {
		t.Fatal(err)
	}
	return c
}

// resumes are a stream's resume points' states, in order.
func (c *capture) resumes(stream string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, l := range c.log {
		if s, ok := strings.CutPrefix(l, stream+"@"); ok {
			out = append(out, s)
		}
	}
	return out
}

// endsAt checks a read ended at until: a resume point there after its last
// records, then its completion there.
func endsAt(t *testing.T, c *capture, stream, until string) {
	t.Helper()
	n := len(c.log)
	if n < 2 || c.log[n-2] != stream+"@"+until || c.log[n-1] != stream+" complete"+until {
		t.Errorf("ended with %q, want a resume point and a completion at %s", c.log[max(n-3, 0):], until)
	}
}

func TestReadsStopAtUntil(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, at int NOT NULL, v text)")
	e.exec("INSERT INTO $schema.t SELECT 2 * i, i / 3, 'v' || i FROM generate_series(1, 100) i")
	e.exec("CREATE TABLE $schema.loose (v int)")
	e.exec("INSERT INTO $schema.loose SELECT generate_series(1, 5)")
	s := e.source(`, "chunkRows": 10`)

	full := read(t, s, map[string]string{"t": ""})
	points := full.resumes("t")
	if len(points) != 11 {
		t.Fatalf("resume points %q", points)
	}
	c := readUntil(t, s, source.ConfiguredStream{Name: "t"}, points[1], points[4], nil)
	if got := c.column("t", "id"); len(got) != 30 || got[0] != "42" || got[29] != "100" {
		t.Errorf("read %v, want 42 to 100", got)
	}
	endsAt(t, c, "t", points[4])
	// From the start, to the first resume point.
	c = readUntil(t, s, source.ConfiguredStream{Name: "t"}, "", points[0], nil)
	if got := c.column("t", "id"); len(got) != 10 || got[9] != "20" {
		t.Errorf("read %v, want 2 to 20", got)
	}
	endsAt(t, c, "t", points[0])
	// Rows past until, added since, aren't read either.
	e.exec("INSERT INTO $schema.t VALUES (51, 1, 'new'), (111, 1, 'new')")
	c = readUntil(t, s, source.ConfiguredStream{Name: "t"}, points[1], points[3], nil)
	if got := c.column("t", "id"); len(got) != 21 || !slices.Contains(got, "51") || slices.Contains(got, "111") {
		t.Errorf("read %v, want 42 to 80 with 51", got)
	}
	endsAt(t, c, "t", points[3])
	e.exec("DELETE FROM $schema.t WHERE id IN (51, 111)")

	t.Run("split", func(t *testing.T) {
		from := `{"after":["60"],"lo":"40","hi":"120"}`
		for until, want := range map[string][]string{
			`{"after":["90"],"lo":"40","hi":"120"}`: {"62", "90", "15"},
			// A range narrowed by a resplit since the split's state.
			`{"after":["80"],"lo":"40","hi":"100"}`: {"62", "80", "10"},
		} {
			c := readSplitUntil(t, s, from, until, nil)
			got := c.values()
			if strconv.Itoa(len(got)) != want[2] || got[0] != want[0] || got[len(got)-1] != want[1] {
				t.Errorf("read %v, want %s to %s", got, want[0], want[1])
			}
			splitEndsAt(t, c, until)
		}
	})

	t.Run("incremental", func(t *testing.T) {
		cs := source.ConfiguredStream{Name: "t", SyncMode: source.Incremental, Cursor: "at"}
		first := readStream(t, s, cs)
		points := first.resumes("t")
		if len(points) != 11 {
			t.Fatalf("resume points %q", points)
		}
		// Rows above the highest cursor value the read had are past until.
		e.exec("UPDATE $schema.t SET at = 1000 WHERE id = 100")
		defer e.exec("UPDATE $schema.t SET at = 50 / 3 WHERE id = 100")
		c := readUntil(t, s, cs, points[2], points[5], nil)
		got := c.column("t", "id")
		if len(got) != 29 || slices.Contains(got, "100") {
			t.Errorf("read %v, want the 30 rows after resume point 3 but 100", got)
		}
		endsAt(t, c, "t", points[5])
		c = readUntil(t, s, cs, "", points[0], nil)
		if got := c.column("t", "id"); len(got) != 10 {
			t.Errorf("read %v from the start", got)
		}
		endsAt(t, c, "t", points[0])
	})

	t.Run("without an order", func(t *testing.T) {
		c := readUntil(t, s, source.ConfiguredStream{Name: "loose"}, "", `{"done":true}`, nil)
		if got := c.column("loose", "v"); len(got) != 5 {
			t.Errorf("read %v", got)
		}
		endsAt(t, c, "loose", `{"done":true}`)
	})

	// A split read to an until isn't divided.
	t.Run("no resplit", func(t *testing.T) {
		var asked bool
		c := readSplitUntil(t, s, "", points[6], func(string, string) {
			ok, err := s.Resplit(t.Context(), "t", "1")
			asked = asked || ok || err != nil
		})
		if asked {
			t.Error("Resplit divided a split read to an until")
		}
		splitEndsAt(t, c, points[6])
	})
}

// readSplitUntil reads stream t's split 1 from a state to an until.
func readSplitUntil(t *testing.T, s *Source, from, until string, onBatch func(stream, split string)) *splitCapture {
	t.Helper()
	sp := source.Split{ID: "1", Until: []byte(until)}
	if from != "" {
		sp.State = []byte(from)
	}
	c := &splitCapture{col: "id", onBatch: onBatch}
	cs := source.ConfiguredStream{Name: "t", Splits: []source.Split{sp}}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c); err != nil {
		t.Fatal(err)
	}
	return c
}

func splitEndsAt(t *testing.T, c *splitCapture, until string) {
	t.Helper()
	n := len(c.events)
	if n < 2 || !c.events[n-2].resume || c.events[n-2].state != until || !c.events[n-1].complete || c.events[n-1].state != until {
		t.Errorf("ended with %+v, want a resume point and a completion at %s", c.events[max(n-2, 0):], until)
	}
}

func readStream(t *testing.T, s *Source, cs source.ConfiguredStream) *capture {
	t.Helper()
	c := &capture{}
	t.Cleanup(c.release)
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, c); err != nil {
		t.Fatal(err)
	}
	return c
}

// A CDC read to an until reads the transactions that committed before its
// position, and none after.
func TestCDCStopsAtUntil(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 10) i")
	s, pipeline := e.cdcSource("")
	start := readCDC(t, s, pipeline, map[string]string{"users": ""}).last("users")

	e.exec("UPDATE $schema.users SET name = 'a' WHERE id = 1")
	e.exec("INSERT INTO $schema.users VALUES (11, 'b', NULL)")
	var at string
	if err := e.conn.QueryRow(t.Context(), "SELECT pg_current_wal_lsn()::text").Scan(&at); err != nil {
		t.Fatal(err)
	}
	e.exec("DELETE FROM $schema.users WHERE id = 2")
	pos, err := pglogrepl.ParseLSN(at)
	if err != nil {
		t.Fatal(err)
	}
	until, err := json.Marshal(cdcState{Slot: stateOf(t, start).Slot, LSN: lsn(pos)})
	if err != nil {
		t.Fatal(err)
	}
	cs := source.ConfiguredStream{Name: "users", SyncMode: source.CDC}
	req := source.ReadRequest{Pipeline: pipeline, Streams: []source.ConfiguredStream{cs}}
	req.Streams[0].State, req.Streams[0].Until = []byte(start), until
	c := &capture{}
	t.Cleanup(c.release)
	if err := s.Read(t.Context(), req, c); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ch := range c.changes(t, "users") {
		got = append(got, ch.op+" "+ch.id+" "+ch.name)
	}
	if want := []string{"update 1 a", "insert 11 b"}; !slices.Equal(got, want) {
		t.Errorf("read %q, want %q", got, want)
	}
	endsAt(t, c, "users", string(until))

	// The read after it reads the rest.
	rest := readCDC(t, s, pipeline, map[string]string{"users": string(until)})
	got = nil
	for _, ch := range rest.changes(t, "users") {
		got = append(got, ch.op+" "+ch.id)
	}
	if want := []string{"delete 2"}; !slices.Equal(got, want) {
		t.Errorf("then read %q, want %q", got, want)
	}
}

// A redo of a CDC read's last part, from its last resume point with no
// until, completes where the read did, though the log has moved since.
func TestCDCRedoOfTheLastPartEndsWhereTheReadDid(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 10) i")
	s, pipeline := e.cdcSource("")
	first := readCDC(t, s, pipeline, map[string]string{"users": ""})
	points, end := first.resumes("users"), first.last("users")
	e.exec("CREATE TABLE $schema.other (id int)")
	again := readCDC(t, s, pipeline, map[string]string{"users": points[len(points)-1]})
	if got := again.last("users"); got != end {
		t.Errorf("the redo completed at %s, want %s", got, end)
	}
	// The read after it reads on.
	if next := stateOf(t, readCDC(t, s, pipeline, map[string]string{"users": end}).last("users")); next.LSN <= stateOf(t, end).LSN {
		t.Errorf("the next read completed at %s, not past %s", pglogrepl.LSN(next.LSN), end)
	}
}

// A redo from a position the slot was acknowledged past fails with
// POSITION_LOST, rather than reading from the slot's position and missing
// the changes between.
func TestCDCRedoBeforeTheSlotFails(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	s, pipeline := e.cdcSource("")
	slot := s.slotName(pipeline)
	start := readCDC(t, s, pipeline, map[string]string{"users": ""}).last("users")

	e.exec("INSERT INTO $schema.users VALUES (1, 'a', NULL)")
	var at string
	if err := e.conn.QueryRow(t.Context(), "SELECT pg_current_wal_lsn()::text").Scan(&at); err != nil {
		t.Fatal(err)
	}
	e.exec("INSERT INTO $schema.users VALUES (2, 'b', NULL)")
	pos, err := pglogrepl.ParseLSN(at)
	if err != nil {
		t.Fatal(err)
	}
	until, err := json.Marshal(cdcState{Slot: slot, LSN: lsn(pos)})
	if err != nil {
		t.Fatal(err)
	}
	later := readCDC(t, s, pipeline, map[string]string{"users": start}).last("users")

	confirmed := func() pglogrepl.LSN {
		var at string
		if err := e.conn.QueryRow(t.Context(), "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&at); err != nil {
			t.Fatal(err)
		}
		l, _ := pglogrepl.ParseLSN(at)
		return l
	}
	// Committed leaves a slot alone while a read's connection still holds it.
	acknowledge := func(state string) {
		t.Helper()
		want := pglogrepl.LSN(stateOf(t, state).LSN)
		for deadline := time.Now().Add(10 * time.Second); ; {
			if err := s.Committed(t.Context(), map[string][]byte{"users": []byte(state)}, nil); err != nil {
				t.Fatal(err)
			}
			if confirmed() >= want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("the slot confirmed %s, not %s", confirmed(), want)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	redo := func() (*capture, error) {
		cs := source.ConfiguredStream{Name: "users", SyncMode: source.CDC, State: []byte(start), Until: until}
		c := &capture{}
		t.Cleanup(c.release)
		return c, s.Read(t.Context(), source.ReadRequest{Pipeline: pipeline, Streams: []source.ConfiguredStream{cs}}, c)
	}

	// Acknowledged up to the redo's start, it reads the part.
	acknowledge(start)
	c, err := redo()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ch := range c.changes(t, "users") {
		got = append(got, ch.op+" "+ch.id)
	}
	if want := []string{"insert 1"}; !slices.Equal(got, want) {
		t.Errorf("read %q, want %q", got, want)
	}
	endsAt(t, c, "users", string(until))

	// Acknowledged past it, it can't.
	acknowledge(later)
	if from := pglogrepl.LSN(stateOf(t, start).LSN); confirmed() <= from {
		t.Fatalf("the slot confirmed %s, not past the redo's start %s", confirmed(), from)
	}
	c, err = redo()
	if code := codeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a redo from before the slot's position: %v (%v)", err, code)
	}
	if len(c.log) > 0 {
		t.Errorf("a redo from before the slot's position sent %q", c.log)
	}
	// So does a read without an end.
	err = s.Read(t.Context(), cdcRequest(pipeline, false, map[string]string{"users": start}), &capture{})
	if code := codeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a read from before the slot's position: %v (%v)", err, code)
	}
}

// segmented records each resume point's state and how many of the stream's
// batches came before it.
type segmented struct {
	*capture
	stream string
	states []string
	cuts   []int
}

func (c *segmented) ResumePoint(stream string, state []byte) error {
	if err := c.capture.ResumePoint(stream, state); err != nil {
		return err
	}
	if stream == c.stream {
		c.mu.Lock()
		c.states = append(c.states, string(state))
		c.cuts = append(c.cuts, len(c.batches[stream]))
		c.mu.Unlock()
	}
	return nil
}

// upTo is a capture of the stream's batches before resume point i.
func (c *segmented) upTo(i int) *capture {
	return &capture{batches: map[string][]arrow.RecordBatch{c.stream: c.batches[c.stream][:c.cuts[i]]}}
}

// Under writes throughout, a redo of a part that ends mid-snapshot stops at
// its until, and what was read before it, the redo, and a read on from until
// merge into the table: the redo's chunks, read later than the first read's,
// don't outrank changes the read after it reads.
func TestCDCRedoMidSnapshotUnderWrites(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 3000) i")
	e.exec("ANALYZE $schema.users")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 50`)

	var writers sync.WaitGroup
	defer writers.Wait()
	writers.Go(func() { keepWriting(t, e, "users", 3200, 300) })
	first := &segmented{capture: &capture{}, stream: "users"}
	t.Cleanup(first.release)
	req := cdcRequest(pipeline, false, map[string]string{"users": ""})
	req.Lanes = 4
	if err := s.Read(t.Context(), req, first); err != nil {
		t.Fatal(err)
	}
	var mid []int
	for i, st := range first.states {
		if stateOf(t, st).Snapshot != nil {
			mid = append(mid, i)
		}
	}
	if len(mid) < 6 {
		t.Fatalf("%d resume points mid-snapshot", len(mid))
	}
	from, to := mid[1], mid[len(mid)-2]
	redo := &capture{}
	t.Cleanup(redo.release)
	rreq := cdcRequest(pipeline, false, map[string]string{"users": first.states[from]})
	rreq.Lanes = 3
	rreq.Streams[0].Until = []byte(first.states[to])
	if err := s.Read(t.Context(), rreq, redo); err != nil {
		t.Fatal(err)
	}
	endsAt(t, redo, "users", first.states[to])
	writers.Wait()
	rest := e.readCDCLanes(s, pipeline, 2, map[string]string{"users": first.states[to]})
	all := append(first.upTo(from).changes(t, "users"), redo.changes(t, "users")...)
	sameRows(t, merged(append(all, rest.changes(t, "users")...)), e.rows("users"))
}

// A redo from before the snapshot started reads until's ranges up to their
// positions, and the table's rest whole.
func TestCDCRedoFromTheStartToMidSnapshot(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	e.exec("INSERT INTO $schema.users SELECT i, 'n' || i, NULL FROM generate_series(1, 2000) i")
	e.exec("ANALYZE $schema.users")
	s, pipeline := e.cdcSource(`, "snapshotChunkRows": 100`)
	first := &segmented{capture: &capture{}, stream: "users"}
	t.Cleanup(first.release)
	req := cdcRequest(pipeline, false, map[string]string{"users": ""})
	req.Lanes = 4
	if err := s.Read(t.Context(), req, first); err != nil {
		t.Fatal(err)
	}
	for i, st := range first.states {
		cur := stateOf(t, st).Snapshot
		if cur == nil || len(cur.Ranges) < 2 || i < 4 {
			continue
		}
		redo := &capture{}
		t.Cleanup(redo.release)
		rreq := cdcRequest(pipeline, false, map[string]string{"users": ""})
		rreq.Lanes = 2
		rreq.Streams[0].Until = []byte(st)
		if err := s.Read(t.Context(), rreq, redo); err != nil {
			t.Fatal(err)
		}
		endsAt(t, redo, "users", st)
		want := map[string]bool{}
		for _, ch := range first.upTo(i).changes(t, "users") {
			want[ch.id] = true
		}
		got := map[string]int{}
		for _, ch := range redo.changes(t, "users") {
			got[ch.id]++
		}
		if len(got) != len(want) {
			t.Errorf("redo read %d keys, the first read %d before its resume point %d", len(got), len(want), i)
		}
		for id, n := range got {
			if !want[id] || n != 1 {
				t.Errorf("redo read %s %d times; the first read had it: %v", id, n, want[id])
			}
		}
		return
	}
	t.Fatalf("no resume point mid-snapshot with ranges left: %q", first.states)
}
