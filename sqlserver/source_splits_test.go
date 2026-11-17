//go:build integration

package sqlserver

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// splitEvent is what a split emitted: rows' values in one column, or a
// resume point or completion.
type splitEvent struct {
	split    string
	rows     []string
	resume   bool
	complete bool
	state    string
	added    []source.Split
}

// splitCapture records what a source emits on each split, in order, keeping
// one column of its rows.
type splitCapture struct {
	col     string
	onBatch func(stream, split string)
	mu      sync.Mutex
	events  []splitEvent
}

func (c *splitCapture) Rows(stream string, _ []json.RawMessage) error {
	return fmt.Errorf("unexpected raw rows on %s", stream)
}
func (c *splitCapture) Records(stream string, b arrow.RecordBatch) error {
	return c.Split(stream, source.WholeStream).Records(b)
}
func (c *splitCapture) ResumePoint(stream string, state []byte) error {
	return c.Split(stream, source.WholeStream).ResumePoint(state)
}
func (c *splitCapture) Complete(stream string, next []byte) error {
	return c.Split(stream, source.WholeStream).Complete(next)
}
func (c *splitCapture) Split(stream, id string) source.SplitEmitter { return splitSink{c, stream, id} }

func (c *splitCapture) add(e splitEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

type splitSink struct {
	c          *splitCapture
	stream, id string
}

func (s splitSink) Rows([]json.RawMessage) error {
	return fmt.Errorf("unexpected raw rows on %s", s.stream)
}
func (s splitSink) Records(b arrow.RecordBatch) error {
	if s.c.onBatch != nil {
		s.c.onBatch(s.stream, s.id)
	}
	col := b.Column(b.Schema().FieldIndices(s.c.col)[0])
	rows := make([]string, col.Len())
	for i := range rows {
		rows[i] = col.ValueStr(i)
	}
	b.Release()
	s.c.add(splitEvent{split: s.id, rows: rows})
	return nil
}
func (s splitSink) ResumePoint(state []byte, added ...source.Split) error {
	s.c.add(splitEvent{split: s.id, resume: true, state: string(state), added: added})
	return nil
}
func (s splitSink) Complete(next []byte, added ...source.Split) error {
	s.c.add(splitEvent{split: s.id, complete: true, state: string(next), added: added})
	return nil
}

func (c *splitCapture) values() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, e := range c.events {
		out = append(out, e.rows...)
	}
	return out
}

func (c *splitCapture) of(split string) []splitEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []splitEvent
	for _, e := range c.events {
		if e.split == split {
			out = append(out, e)
		}
	}
	return out
}

func sameValues(t *testing.T, got []string, n int) {
	t.Helper()
	ints := make([]int, len(got))
	for i, g := range got {
		ints[i], _ = strconv.Atoi(g)
	}
	slices.Sort(ints)
	for i, v := range ints {
		if v != i+1 {
			t.Fatalf("read %d values, %v..., want 1 to %d once each", len(got), ints[:min(len(ints), 10)], n)
		}
	}
	if len(ints) != n {
		t.Fatalf("read %d values, want %d", len(ints), n)
	}
}

func TestSplitsReadEveryRowOnce(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, n int)")
	e.exec("INSERT INTO $schema.t SELECT n, n FROM (SELECT TOP (5000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b) x")
	e.exec("UPDATE STATISTICS $schema.t WITH FULLSCAN")
	s := e.source(`, "chunkRows": 300`)
	cs := source.ConfiguredStream{Name: "t", SyncMode: source.FullRefresh}
	splits, err := s.Splits(t.Context(), cs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) < 2 {
		t.Fatalf("%d splits", len(splits))
	}
	cs.Splits = splits
	c := &splitCapture{col: "n"}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 4}, c); err != nil {
		t.Fatal(err)
	}
	sameValues(t, c.values(), 5000)
	for _, sp := range splits {
		if ev := c.of(sp.ID); len(ev) == 0 || !ev[len(ev)-1].complete {
			t.Errorf("split %s didn't complete", sp.ID)
		}
	}
}

// Asked to split during a read, a range ends where it is past halfway, and
// the rest is a new split; each part resumes on its own.
func TestResplit(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, n int)")
	e.exec("INSERT INTO $schema.t SELECT n, n FROM (SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x")
	s := e.source(`, "chunkRows": 20`)
	cs := source.ConfiguredStream{Name: "t", SyncMode: source.FullRefresh, Splits: []source.Split{{ID: "a", State: []byte(`{}`)}}}
	c := &splitCapture{col: "n"}
	var asked atomic.Bool
	c.onBatch = func(stream, split string) {
		if split == "a" && !asked.Swap(true) {
			if ok, err := s.Resplit(t.Context(), stream, split); err != nil || !ok {
				t.Errorf("asked to split: %v, %v", ok, err)
			}
		}
	}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 2}, c); err != nil {
		t.Fatal(err)
	}
	sameValues(t, c.values(), 100)
	var cut *splitEvent
	for _, ev := range c.of("a") {
		if len(ev.added) > 0 {
			cut = &ev
			break
		}
	}
	if cut == nil {
		t.Fatalf("split a never added a split: %+v", c.of("a"))
	}
	if len(c.of(cut.added[0].ID)) == 0 {
		t.Errorf("the new split %s read nothing", cut.added[0].ID)
	}
	rest := &splitCapture{col: "n"}
	cs.Splits = []source.Split{{ID: "a", State: []byte(cut.state)}, cut.added[0]}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 2}, rest); err != nil {
		t.Fatal(err)
	}
	got := rest.values()
	if len(got) == 0 || slices.Contains(got, "1") || !slices.Contains(got, "100") {
		t.Errorf("resumed %v", got)
	}
}

// An incremental read reads rows from where the last one ended, by a
// datetime2 or a rowversion, and a resumed one the rest exactly.
func TestIncremental(t *testing.T) {
	for _, cursor := range []string{"at", "rv"} {
		t.Run(cursor, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, at datetime2, rv rowversion)")
			e.exec("INSERT INTO $schema.t (id, at) SELECT n, DATEADD(day, n, '2026-01-01') FROM (SELECT TOP (5) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x")
			s := e.source(`, "chunkRows": 2`)
			first := &splitCapture{col: "id"}
			cs := source.ConfiguredStream{Name: "t", SyncMode: source.Incremental, Cursor: cursor}
			if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, first); err != nil {
				t.Fatal(err)
			}
			ev := first.of(source.WholeStream)
			if got := first.values(); !slices.Equal(got, []string{"1", "2", "3", "4", "5"}) || !ev[len(ev)-1].complete {
				t.Fatalf("first read %v", got)
			}
			// From the second resume point, the rest.
			var resumes []string
			for _, x := range ev {
				if x.resume {
					resumes = append(resumes, x.state)
				}
			}
			mid := &splitCapture{col: "id"}
			cs.State = []byte(resumes[1])
			if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, mid); err != nil {
				t.Fatal(err)
			}
			if got := mid.values(); !slices.Equal(got, []string{"5"}) {
				t.Errorf("from %s: %v", resumes[1], got)
			}
			e.exec("UPDATE $schema.t SET at = '2027-01-01' WHERE id = 2")
			e.exec("INSERT INTO $schema.t (id, at) VALUES (6, '2027-01-02')")
			next := &splitCapture{col: "id"}
			cs.State = []byte(ev[len(ev)-1].state)
			if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, next); err != nil {
				t.Fatal(err)
			}
			// The floor is inclusive, so a datetime2 cursor reads its last
			// value's rows again; a rowversion moves on every write.
			want := []string{"5", "2", "6"}
			if cursor == "rv" {
				want = []string{"2", "6"}
			}
			if got := next.values(); !slices.Equal(got, want) {
				t.Errorf("next read %v, want %v", got, want)
			}
		})
	}
}

// A rowversion cursor's read stops below the oldest open transaction's
// rowversion, whose rows can commit after the read with values below the
// highest it saw.
func TestIncrementalRowversionWaitsForOpenTransactions(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, rv rowversion)")
	e.exec("CREATE TABLE $schema.other (id int PRIMARY KEY, rv rowversion)")
	e.exec("INSERT INTO $schema.t (id) VALUES (1)")
	tx, err := e.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), "INSERT INTO "+quote(e.schema)+".other (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	e.exec("INSERT INTO $schema.t (id) VALUES (2)")
	s := e.source("")
	cs := source.ConfiguredStream{Name: "t", SyncMode: source.Incremental, Cursor: "rv"}
	first := &splitCapture{col: "id"}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, first); err != nil {
		t.Fatal(err)
	}
	if got := first.values(); !slices.Equal(got, []string{"1"}) {
		t.Errorf("read with a transaction open %v", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ev := first.of(source.WholeStream)
	cs.State = []byte(ev[len(ev)-1].state)
	next := &splitCapture{col: "id"}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, next); err != nil {
		t.Fatal(err)
	}
	if got := next.values(); !slices.Equal(got, []string{"2"}) {
		t.Errorf("read after the commit %v", got)
	}
}
