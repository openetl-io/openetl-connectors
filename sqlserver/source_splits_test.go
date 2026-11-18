//go:build integration

package sqlserver

import (
	"slices"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type (
	splitEvent   = connectortest.SplitEvent
	splitCapture = connectortest.SplitCapture
)

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
	c := &splitCapture{Col: "n"}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 4}, c); err != nil {
		t.Fatal(err)
	}
	sameValues(t, c.Values(), 5000)
	for _, sp := range splits {
		if ev := c.Of(sp.ID); len(ev) == 0 || !ev[len(ev)-1].Complete {
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
	c := &splitCapture{Col: "n"}
	var asked atomic.Bool
	c.OnBatch = func(stream, split string) {
		if split == "a" && !asked.Swap(true) {
			if ok, err := s.Resplit(t.Context(), stream, split); err != nil || !ok {
				t.Errorf("asked to split: %v, %v", ok, err)
			}
		}
	}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 2}, c); err != nil {
		t.Fatal(err)
	}
	sameValues(t, c.Values(), 100)
	var cut *splitEvent
	for _, ev := range c.Of("a") {
		if len(ev.Added) > 0 {
			cut = &ev
			break
		}
	}
	if cut == nil {
		t.Fatalf("split a never added a split: %+v", c.Of("a"))
	}
	if len(c.Of(cut.Added[0].ID)) == 0 {
		t.Errorf("the new split %s read nothing", cut.Added[0].ID)
	}
	rest := &splitCapture{Col: "n"}
	cs.Splits = []source.Split{{ID: "a", State: []byte(cut.State)}, cut.Added[0]}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 2}, rest); err != nil {
		t.Fatal(err)
	}
	got := rest.Values()
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
			first := &splitCapture{Col: "id"}
			cs := source.ConfiguredStream{Name: "t", SyncMode: source.Incremental, Cursor: cursor}
			if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, first); err != nil {
				t.Fatal(err)
			}
			ev := first.Of(source.WholeStream)
			if got := first.Values(); !slices.Equal(got, []string{"1", "2", "3", "4", "5"}) || !ev[len(ev)-1].Complete {
				t.Fatalf("first read %v", got)
			}
			// From the second resume point, the rest.
			var resumes []string
			for _, x := range ev {
				if x.Resume {
					resumes = append(resumes, x.State)
				}
			}
			mid := &splitCapture{Col: "id"}
			cs.State = []byte(resumes[1])
			if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, mid); err != nil {
				t.Fatal(err)
			}
			if got := mid.Values(); !slices.Equal(got, []string{"5"}) {
				t.Errorf("from %s: %v", resumes[1], got)
			}
			e.exec("UPDATE $schema.t SET at = '2027-01-01' WHERE id = 2")
			e.exec("INSERT INTO $schema.t (id, at) VALUES (6, '2027-01-02')")
			next := &splitCapture{Col: "id"}
			cs.State = []byte(ev[len(ev)-1].State)
			if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, next); err != nil {
				t.Fatal(err)
			}
			// The floor is inclusive, so a datetime2 cursor reads its last
			// value's rows again; a rowversion moves on every write.
			want := []string{"5", "2", "6"}
			if cursor == "rv" {
				want = []string{"2", "6"}
			}
			if got := next.Values(); !slices.Equal(got, want) {
				t.Errorf("next read %v, want %v", got, want)
			}
		})
	}
}

// A varchar cursor's highest value is kept whole, past 4000 characters, so
// the row that holds it is read.
func TestIncrementalLongTextCursor(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, c varchar(8000))")
	e.exec("INSERT INTO $schema.t VALUES (1, 'a'), (2, REPLICATE('a', 4001))")
	cs := source.ConfiguredStream{Name: "t", SyncMode: source.Incremental, Cursor: "c"}
	got := &splitCapture{Col: "id"}
	if err := e.source("").Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, got); err != nil {
		t.Fatal(err)
	}
	if v := got.Values(); !slices.Equal(v, []string{"1", "2"}) {
		t.Errorf("read %v", v)
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
	first := &splitCapture{Col: "id"}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, first); err != nil {
		t.Fatal(err)
	}
	if got := first.Values(); !slices.Equal(got, []string{"1"}) {
		t.Errorf("read with a transaction open %v", got)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ev := first.Of(source.WholeStream)
	cs.State = []byte(ev[len(ev)-1].State)
	next := &splitCapture{Col: "id"}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, next); err != nil {
		t.Fatal(err)
	}
	if got := next.Values(); !slices.Equal(got, []string{"2"}) {
		t.Errorf("read after the commit %v", got)
	}
}
