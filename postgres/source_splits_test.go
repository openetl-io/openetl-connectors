//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/conformance"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// splitEvent is what a split emitted: rows' values in one column, or a resume
// point or completion.
type splitEvent struct {
	stream, split string
	rows          []string
	resume        bool
	complete      bool
	state         string
	added         []source.Split
}

// splitCapture records what a source emits on each split, in order, keeping
// one column of its rows.
type splitCapture struct {
	col string
	// Runs on the split's reader for each batch, before it's recorded.
	onBatch func(stream, split string)

	mu     sync.Mutex
	events []splitEvent
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

func (c *splitCapture) Split(stream, id string) source.SplitEmitter {
	return splitSink{c, stream, id}
}

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
	defer b.Release()
	if s.c.onBatch != nil {
		s.c.onBatch(s.stream, s.id)
	}
	col := b.Column(b.Schema().FieldIndices(s.c.col)[0])
	rows := make([]string, col.Len())
	for i := range rows {
		rows[i] = col.ValueStr(i)
	}
	s.c.add(splitEvent{stream: s.stream, split: s.id, rows: rows})
	return nil
}

func (s splitSink) ResumePoint(state []byte, added ...source.Split) error {
	s.c.add(splitEvent{stream: s.stream, split: s.id, resume: true, state: string(state), added: added})
	return nil
}

func (s splitSink) Complete(next []byte, added ...source.Split) error {
	s.c.add(splitEvent{stream: s.stream, split: s.id, complete: true, state: string(next), added: added})
	return nil
}

// values are the column's values read, in the order they were emitted.
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

func (c *splitCapture) added() []source.Split {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []source.Split
	for _, e := range c.events {
		out = append(out, e.added...)
	}
	return out
}

func valuesOf(events []splitEvent) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.rows...)
	}
	return out
}

func statesOf(t *testing.T, events []splitEvent) []sourceState {
	t.Helper()
	var out []sourceState
	for _, e := range events {
		if e.resume {
			out = append(out, parseState(t, e.state))
		}
	}
	return out
}

func parseState(t *testing.T, b string) sourceState {
	t.Helper()
	var st sourceState
	if err := json.Unmarshal([]byte(b), &st); err != nil {
		t.Fatalf("state %q: %v", b, err)
	}
	return st
}

// oneToN are the values 1 to n, as text.
func oneToN(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = strconv.Itoa(i + 1)
	}
	return out
}

// sameValues checks got holds each of want, once each.
func sameValues(t *testing.T, got, want []string) {
	t.Helper()
	counts := map[string]int{}
	for _, v := range got {
		counts[v]++
	}
	var missing, twice []string
	for _, v := range want {
		if counts[v] == 0 {
			missing = append(missing, v)
		}
		delete(counts, v)
	}
	for v, n := range counts {
		if n > 0 {
			twice = append(twice, v+" (unexpected)")
		}
	}
	seen := map[string]bool{}
	for _, v := range got {
		if seen[v] {
			twice = append(twice, v)
		}
		seen[v] = true
	}
	slices.Sort(missing)
	slices.Sort(twice)
	if len(missing) > 0 || len(twice) > 0 {
		t.Errorf("%d rows: missing %.20q, more than once or unexpected %.20q", len(got), missing, twice)
	}
}

// inSplits reads a stream in the splits given.
func inSplits(cs source.ConfiguredStream, splits []source.Split) source.ConfiguredStream {
	if len(splits) == 1 && splits[0].ID == source.WholeStream {
		cs.State = splits[0].State
	} else {
		cs.Splits = splits
	}
	return cs
}

func (e *sourceEnv) mustSplits(s *Source, cs source.ConfiguredStream, ended []source.Split) []source.Split {
	e.t.Helper()
	splits, err := s.Splits(e.t.Context(), cs, ended)
	if err != nil {
		e.t.Fatal(err)
	}
	return splits
}

func (e *sourceEnv) mustRead(s *Source, lanes int, c *splitCapture, streams ...source.ConfiguredStream) {
	e.t.Helper()
	if err := s.Read(e.t.Context(), source.ReadRequest{Streams: streams, Lanes: lanes}, c); err != nil {
		e.t.Fatal(err)
	}
}

// A table is cut at its histogram into contiguous ranges, about chunkRows
// rows each and at most 32, the first with no lower bound and the last with
// no upper.
func TestSplitsFromStatistics(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, v text) WITH (autovacuum_enabled = false)")
	e.exec("INSERT INTO $schema.t SELECT i, 'v' FROM generate_series(1, 10000) i")
	cs := source.ConfiguredStream{Name: "t", SyncMode: source.FullRefresh}

	// Without statistics.
	if got := e.mustSplits(e.source(`, "chunkRows": 1000`), cs, nil); len(got) != 1 || got[0].ID != source.WholeStream || got[0].State != nil {
		t.Errorf("without statistics: %q", got)
	}
	e.exec("ANALYZE $schema.t")

	check := func(extra string, want int) {
		t.Helper()
		splits := e.mustSplits(e.source(extra), cs, nil)
		if len(splits) != want {
			t.Fatalf("%s: %d splits, want %d", extra, len(splits), want)
		}
		var lo *string
		prev := -1
		for i, sp := range splits {
			if sp.ID != strconv.Itoa(i) {
				t.Errorf("split %d is %q", i, sp.ID)
			}
			st := parseState(t, string(sp.State))
			if (lo == nil) != (st.Lo == nil) || lo != nil && *lo != *st.Lo {
				t.Errorf("%s: split %d starts at %v after %v", extra, i, st.Lo, lo)
			}
			if i == len(splits)-1 {
				if st.Hi != nil {
					t.Errorf("%s: the last split ends at %q", extra, *st.Hi)
				}
				break
			}
			if st.Hi == nil {
				t.Fatalf("%s: split %d has no end", extra, i)
			}
			hi, err := strconv.Atoi(*st.Hi)
			if err != nil || hi <= prev {
				t.Errorf("%s: split %d ends at %q after %d", extra, i, *st.Hi, prev)
			}
			lo, prev = st.Hi, hi
		}
	}
	check(`, "chunkRows": 1000`, 10)
	check(`, "chunkRows": 3000`, 4)
	check(`, "chunkRows": 10`, 32)

	// Not a chunk's worth.
	if got := e.mustSplits(e.source(`, "chunkRows": 10000`), cs, nil); len(got) != 1 || got[0].ID != source.WholeStream {
		t.Errorf("a small table: %q", got)
	}
	// Tables without a key, and cdc, are read whole.
	e.exec("CREATE TABLE $schema.loose AS SELECT i FROM generate_series(1, 10000) i")
	e.exec("ANALYZE $schema.loose")
	s := e.source(`, "chunkRows": 10`)
	if got := e.mustSplits(s, source.ConfiguredStream{Name: "loose"}, nil); len(got) != 1 || got[0].ID != source.WholeStream {
		t.Errorf("a table without a key: %q", got)
	}
	cdc := source.ConfiguredStream{Name: "t", SyncMode: source.CDC}
	if got := e.mustSplits(s, cdc, []source.Split{{ID: source.WholeStream, State: []byte(`{"slot":"x"}`)}}); len(got) != 1 || got[0].ID != source.WholeStream || string(got[0].State) != `{"slot":"x"}` {
		t.Errorf("cdc: %q", got)
	}
	if got := e.mustSplits(s, cdc, []source.Split{{ID: "0"}, {ID: "1"}}); len(got) != 1 || got[0].ID != source.WholeStream || got[0].State != nil {
		t.Errorf("cdc after a read in splits: %q", got)
	}
	// All the bounds there are.
	e.exec("CREATE TABLE $schema.few (id int PRIMARY KEY) WITH (autovacuum_enabled = false)")
	e.exec("ALTER TABLE $schema.few ALTER COLUMN id SET STATISTICS 3")
	e.exec("INSERT INTO $schema.few SELECT generate_series(1, 10000)")
	e.exec("ANALYZE $schema.few")
	if got := e.mustSplits(s, source.ConfiguredStream{Name: "few"}, nil); len(got) != 3 {
		t.Errorf("a histogram of 4 bounds: %d splits, want its 3 buckets", len(got))
	}

	if _, err := s.Splits(t.Context(), source.ConfiguredStream{Name: "missing"}, nil); err == nil {
		t.Error("split a missing table")
	}
	_, err := s.Splits(t.Context(), source.ConfiguredStream{Name: "t", SyncMode: source.Incremental, Cursor: "missing"}, nil)
	if code := codeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID {
		t.Errorf("a missing cursor: %v (%v), want CONFIG_INVALID", err, code)
	}
}

func TestSourceDeclaresSplitsByKey(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.keyed (id int PRIMARY KEY)")
	e.exec("CREATE TABLE $schema.loose (v text)")
	cat, err := e.source("").Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range cat.Streams {
		if s.SplitsByKey != (s.Name == "keyed") {
			t.Errorf("%s splits by key: %v", s.Name, s.SplitsByKey)
		}
	}
}

func TestSourceConfigRefusesNoConnections(t *testing.T) {
	for _, extra := range []string{`"maxConnections": 0`, `"maxConnections": -1`} {
		if _, err := NewSource(fmt.Appendf(nil, `{"dsn": %q, %s}`, dsn(), extra)); err == nil {
			t.Errorf("accepted %s", extra)
		}
	}
	s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q}`, dsn()))
	if err != nil || s.cfg.MaxConnections != 4 {
		t.Errorf("default: %+v, %v", s, err)
	}
}

// Each key type's table is read in splits, concurrently: every row once,
// each split in key order, with those that came after the statistics.
func TestSplitsReadEveryRowOnce(t *testing.T) {
	for name, c := range map[string]struct{ cols, key, row string }{
		"int":       {"id int", "id", "i"},
		"bigint":    {"id bigint", "id", "i * 100000000000"},
		"smallint":  {"id smallint", "id", "i - 1500"},
		"text":      {"id text", "id", "md5(i::text)"},
		"char":      {"id char(40)", "id", "md5(i::text)"},
		"varchar":   {"id varchar(40)", "id", "md5(i::text)"},
		"uuid":      {"id uuid", "id", "md5(i::text)::uuid"},
		"numeric":   {"id numeric(12, 3)", "id", "i / 7.0"},
		"double":    {"id double precision", "id", "i / 7.0"},
		"timestamp": {"id timestamptz", "id", "timestamptz '2026-01-01 00:00Z' + i * interval '1 hour'"},
		"date":      {"id date", "id", "date '2020-01-01' + i"},
		"bytea":     {"id bytea", "id", "decode(md5(i::text), 'hex')"},
		"int text":  {"a int, b text", "a, b", "i % 500, md5(i::text)"},
		"text int":  {"a text, b int", "a, b", "(i % 500)::text, i"},
		// Values all common: the histogram is empty, and the table read whole.
		"few first": {"a int, b int", "a, b", "i % 3, i"},
	} {
		t.Run(name, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.t (" + c.cols + ", n int NOT NULL, PRIMARY KEY (" + c.key + "))")
			e.exec("INSERT INTO $schema.t SELECT " + c.row + ", i FROM generate_series(1, 2000) i")
			e.exec("ANALYZE $schema.t")
			e.exec("INSERT INTO $schema.t SELECT " + c.row + ", i FROM generate_series(2001, 2200) i")
			s := e.source(`, "chunkRows": 100`)
			cs := source.ConfiguredStream{Name: "t", SyncMode: source.FullRefresh}
			splits := e.mustSplits(s, cs, nil)
			if (len(splits) < 2) != (name == "few first") {
				t.Fatalf("%d splits", len(splits))
			}
			rc := &splitCapture{col: "n"}
			e.mustRead(s, 4, rc, inSplits(cs, splits))
			sameValues(t, rc.values(), oneToN(2200))
			for _, sp := range splits {
				events := rc.of(sp.ID)
				if !events[len(events)-1].complete {
					t.Errorf("split %q ended with %+v", sp.ID, events[len(events)-1])
				}
				if sp.State == nil {
					continue
				}
				for _, st := range statesOf(t, events) {
					want := parseState(t, string(sp.State))
					if !sameBound(st.Lo, want.Lo) || !sameBound(st.Hi, want.Hi) {
						t.Errorf("split %q's state has range %+v, want %+v", sp.ID, st.keyRange, want.keyRange)
					}
				}
			}
		})
	}
}

func sameBound(a, b *string) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}

// Keys beyond every bound the statistics saw belong to the first and last
// ranges.
func TestSplitsCoverKeysOutsideTheHistogram(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, n int)")
	e.exec("INSERT INTO $schema.t SELECT i, i FROM generate_series(1, 1000) i")
	e.exec("ANALYZE $schema.t")
	e.exec("INSERT INTO $schema.t VALUES (-2000000000, 1001), (2000000000, 1002)")
	s := e.source(`, "chunkRows": 100`)
	cs := source.ConfiguredStream{Name: "t"}
	splits := e.mustSplits(s, cs, nil)
	if len(splits) != 10 {
		t.Fatalf("%d splits", len(splits))
	}
	rc := &splitCapture{col: "n"}
	e.mustRead(s, 3, rc, inSplits(cs, splits))
	sameValues(t, rc.values(), oneToN(1002))
	if !slices.Contains(valuesOf(rc.of("0")), "1001") || !slices.Contains(valuesOf(rc.of("9")), "1002") {
		t.Errorf("first split read %d rows, last %d", len(rc.of("0")), len(rc.of("9")))
	}
}

// Splits read at a time follow the lanes, up to maxConnections, each holding
// a connection; no more connections are open than that.
func TestSplitsReadOnAtMostMaxConnections(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, n int)")
	e.exec("INSERT INTO $schema.t SELECT i, i FROM generate_series(1, 1000) i")
	e.exec("ANALYZE $schema.t")
	for _, c := range []struct{ lanes, maxConns, want int }{{0, 4, 1}, {1, 4, 1}, {2, 4, 2}, {4, 4, 4}, {8, 4, 4}, {8, 2, 2}, {8, 1, 1}} {
		t.Run(fmt.Sprintf("%d lanes, %d connections", c.lanes, c.maxConns), func(t *testing.T) {
			app := fmt.Sprintf("%s_%d_%d", e.schema, c.lanes, c.maxConns)
			sep := "?"
			if strings.Contains(dsn(), "?") {
				sep = "&"
			}
			cfg := fmt.Sprintf(`{"dsn": %q, "schema": %q, "chunkRows": 100, "maxConnections": %d}`, dsn()+sep+"application_name="+app, e.schema, c.maxConns)
			s, err := NewSource([]byte(cfg))
			if err != nil {
				t.Fatal(err)
			}
			cs := source.ConfiguredStream{Name: "t"}
			splits := e.mustSplits(e.source(`, "chunkRows": 100`), cs, nil)
			if len(splits) != 10 {
				t.Fatalf("%d splits", len(splits))
			}
			// A split waits, in its first batch, for as many as should run,
			// and no longer than it takes to see that more don't.
			var inFlight, peak, conns atomic.Int64
			reached := make(chan struct{})
			var once sync.Once
			rc := &splitCapture{col: "n"}
			rc.onBatch = func(string, string) {
				n := inFlight.Add(1)
				defer inFlight.Add(-1)
				for {
					cur := peak.Load()
					if n <= cur || peak.CompareAndSwap(cur, n) {
						break
					}
				}
				if n >= int64(c.want) {
					once.Do(func() {
						var open int64
						if err := e.conn.QueryRow(t.Context(), "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", app).Scan(&open); err != nil {
							t.Error(err)
						}
						conns.Store(open)
						// Time for one too many to show.
						time.Sleep(100 * time.Millisecond)
						close(reached)
					})
				}
				select {
				case <-reached:
				case <-time.After(10 * time.Second):
					t.Errorf("waited for %d splits at a time", c.want)
				}
			}
			e.mustRead(s, c.lanes, rc, inSplits(cs, splits))
			sameValues(t, rc.values(), oneToN(1000))
			if peak.Load() != int64(c.want) {
				t.Errorf("%d splits at a time, want %d", peak.Load(), c.want)
			}
			if conns.Load() > int64(c.want) {
				t.Errorf("%d connections open, want at most %d", conns.Load(), c.want)
			}
		})
	}
}

// A role that can't have as many connections reads with those it can have.
func TestSplitsReadWithTheConnectionsTheServerGives(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, n int)")
	e.exec("INSERT INTO $schema.t SELECT i, i FROM generate_series(1, 1000) i")
	e.exec("ANALYZE $schema.t")
	role := "r_" + e.schema
	e.exec("CREATE ROLE " + role + " LOGIN PASSWORD 'pw' CONNECTION LIMIT 2")
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = e.conn.Exec(ctx, "DROP OWNED BY "+role)
		_, _ = e.conn.Exec(ctx, "DROP ROLE "+role)
	})
	e.exec("GRANT USAGE ON SCHEMA $schema TO " + role)
	e.exec("GRANT SELECT ON ALL TABLES IN SCHEMA $schema TO " + role)
	u, err := url.Parse(dsn())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, "pw")
	s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q, "schema": %q, "chunkRows": 100}`, u.String(), e.schema))
	if err != nil {
		t.Fatal(err)
	}
	cs := source.ConfiguredStream{Name: "t"}
	splits := e.mustSplits(s, cs, nil)
	rc := &splitCapture{col: "n"}
	e.mustRead(s, 4, rc, inSplits(cs, splits))
	sameValues(t, rc.values(), oneToN(1000))

	// With none to give, the read fails as it always has.
	e.exec("ALTER ROLE " + role + " CONNECTION LIMIT 0")
	err = s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 4}, &splitCapture{col: "n"})
	if code := codeOf(err); code != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Errorf("without connections: %v (%v), want TRANSIENT", err, code)
	}
}

// A partitioned table's statistics, once it's analyzed, cover its partitions.
func TestSplitsOfAPartitionedTable(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, n int) PARTITION BY RANGE (id)")
	e.exec("CREATE TABLE $schema.t_1 PARTITION OF $schema.t FOR VALUES FROM (0) TO (500)")
	e.exec("CREATE TABLE $schema.t_2 PARTITION OF $schema.t FOR VALUES FROM (500) TO (1001)")
	e.exec("INSERT INTO $schema.t SELECT i, i FROM generate_series(1, 1000) i")
	s := e.source(`, "chunkRows": 100`)
	cs := source.ConfiguredStream{Name: "t"}
	if got := e.mustSplits(s, cs, nil); len(got) != 1 || got[0].ID != source.WholeStream {
		t.Errorf("before it's analyzed: %q", got)
	}
	e.exec("ANALYZE $schema.t")
	splits := e.mustSplits(s, cs, nil)
	if len(splits) != 10 {
		t.Fatalf("%d splits", len(splits))
	}
	rc := &splitCapture{col: "n"}
	e.mustRead(s, 4, rc, inSplits(cs, splits))
	sameValues(t, rc.values(), oneToN(1000))
}

// A read goes on from any mix of splits mid-way, not started and complete,
// exactly after each one's resume point.
func TestSplitsResumeFromAMix(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (a int, b text, n int NOT NULL, PRIMARY KEY (a, b))")
	e.exec("INSERT INTO $schema.t SELECT i % 500, md5(i::text), i FROM generate_series(1, 3000) i")
	e.exec("ANALYZE $schema.t")
	s := e.source(`, "chunkRows": 100`)
	cs := source.ConfiguredStream{Name: "t"}
	splits := e.mustSplits(s, cs, nil)
	first := &splitCapture{col: "n"}
	e.mustRead(s, 4, first, inSplits(cs, splits))

	var again []source.Split
	var before []string
	for i, sp := range splits {
		events := first.of(sp.ID)
		switch i % 3 {
		case 0: // Complete: not read again.
			before = append(before, valuesOf(events)...)
		case 1: // Mid-way, after its first resume point.
			at := slices.IndexFunc(events, func(e splitEvent) bool { return e.resume })
			before = append(before, valuesOf(events[:at])...)
			again = append(again, source.Split{ID: sp.ID, State: []byte(events[at].state)})
		default: // Not started.
			again = append(again, sp)
		}
	}
	rest := &splitCapture{col: "n"}
	e.mustRead(s, 3, rest, inSplits(cs, again))
	sameValues(t, append(before, rest.values()...), oneToN(3000))
	for _, sp := range again {
		events := rest.of(sp.ID)
		if !events[len(events)-1].complete {
			t.Errorf("split %q ended with %+v", sp.ID, events[len(events)-1])
		}
	}
}

// Every split of an incremental read reads up to the cursor's highest value
// when the read started, in (cursor, key) order, and the next read's floor is
// where they ended.
func TestSplitsReadIncrementally(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, seq bigint, n int)")
	e.exec("INSERT INTO $schema.t SELECT i, (i * 7) % 300, i FROM generate_series(1, 2000) i")
	e.exec("INSERT INTO $schema.t VALUES (2001, NULL, 2001), (2002, NULL, 2002)")
	e.exec("ANALYZE $schema.t")
	s := e.source(`, "chunkRows": 100`)
	cs := source.ConfiguredStream{Name: "t", SyncMode: source.Incremental, Cursor: "seq"}
	splits := e.mustSplits(s, cs, nil)
	if len(splits) < 2 {
		t.Fatalf("%d splits", len(splits))
	}
	for _, sp := range splits {
		if st := parseState(t, string(sp.State)); st.Until == nil || *st.Until != "299" || st.Since != nil {
			t.Errorf("split %q's state %s", sp.ID, sp.State)
		}
	}
	// What comes after the read starts waits for the next, whichever range it
	// lands in.
	e.exec("INSERT INTO $schema.t VALUES (3000, 500, 3000), (0, 400, 3001)")
	rc := &splitCapture{col: "n"}
	e.mustRead(s, 4, rc, inSplits(cs, splits))
	sameValues(t, rc.values(), oneToN(2000))
	var ended []source.Split
	for _, sp := range splits {
		events := rc.of(sp.ID)
		last := events[len(events)-1]
		if !last.complete || last.state != `{"since":"299"}` {
			t.Errorf("split %q ended with %+v", sp.ID, last)
		}
		ended = append(ended, source.Split{ID: sp.ID, State: []byte(last.state)})
		// Exactly after a resume point.
		states := statesOf(t, events)
		if len(states) > 1 && len(states[0].After) != 2 {
			t.Errorf("split %q's position %q", sp.ID, states[0].After)
		}
	}

	// A resume mid-way goes on from its (cursor, key).
	sp := splits[3]
	events := rc.of(sp.ID)
	at := slices.IndexFunc(events, func(e splitEvent) bool { return e.resume })
	if at < 0 || parseState(t, events[at].state).After == nil {
		t.Fatalf("split %q's events %+v", sp.ID, events)
	}
	mid := &splitCapture{col: "n"}
	e.mustRead(s, 1, mid, inSplits(cs, []source.Split{{ID: sp.ID, State: []byte(events[at].state)}}))
	sameValues(t, append(valuesOf(events[:at]), mid.values()...), valuesOf(events))

	// The next read starts at the floor, reading its rows again.
	next := e.mustSplits(s, cs, ended)
	for _, sp := range next {
		if st := parseState(t, string(sp.State)); st.Since == nil || *st.Since != "299" || st.Until == nil || *st.Until != "500" {
			t.Errorf("next read's split %q: %s", sp.ID, sp.State)
		}
	}
	rc = &splitCapture{col: "n"}
	e.mustRead(s, 4, rc, inSplits(cs, next))
	var want []string
	rows, err := e.conn.Query(t.Context(), "SELECT n::text FROM "+e.schema+".t WHERE seq >= 299")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		want = append(want, v)
	}
	sameValues(t, rc.values(), want)

	// A table read in splits that's read whole next takes their floor.
	whole := e.mustSplits(e.source(`, "chunkRows": 1000000`), cs, ended)
	if len(whole) != 1 || whole[0].ID != source.WholeStream || string(whole[0].State) != `{"since":"299"}` {
		t.Errorf("read whole after splits: %q", whole)
	}
	// One read whole, with a cursor, is split with its floor.
	again := e.mustSplits(s, cs, []source.Split{{ID: source.WholeStream, State: []byte(`{"since":"100"}`)}})
	if st := parseState(t, string(again[0].State)); len(again) < 2 || st.Since == nil || *st.Since != "100" {
		t.Errorf("split after a read whole: %q", again)
	}
	// A cursor with nothing in it leaves the table whole, for the read to see.
	e.exec("CREATE TABLE $schema.blank (id int PRIMARY KEY, seq bigint)")
	e.exec("INSERT INTO $schema.blank SELECT i, NULL FROM generate_series(1, 1000) i")
	e.exec("ANALYZE $schema.blank")
	got := e.mustSplits(s, source.ConfiguredStream{Name: "blank", SyncMode: source.Incremental, Cursor: "seq"}, nil)
	if len(got) != 1 || got[0].ID != source.WholeStream {
		t.Errorf("a cursor with no values: %q", got)
	}
}

// A cursor can be the key's first column, or any later one.
func TestSplitsReadIncrementallyByAKeyColumn(t *testing.T) {
	for name, key := range map[string]string{"first": "seq, id", "last": "id, seq"} {
		t.Run(name, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.t (id int, seq int, n int, PRIMARY KEY (" + key + "))")
			e.exec("INSERT INTO $schema.t SELECT i, (i * 7) % 300, i FROM generate_series(1, 1000) i")
			e.exec("ANALYZE $schema.t")
			s := e.source(`, "chunkRows": 50`)
			cs := source.ConfiguredStream{Name: "t", SyncMode: source.Incremental, Cursor: "seq"}
			splits := e.mustSplits(s, cs, nil)
			if len(splits) < 2 {
				t.Fatalf("%d splits", len(splits))
			}
			rc := &splitCapture{col: "n"}
			e.mustRead(s, 3, rc, inSplits(cs, splits))
			sameValues(t, rc.values(), oneToN(1000))
		})
	}
}

// A chunk under way when the split's asked can run past the midpoint of what
// was left when it started; the midpoint comes from where it stopped, so
// nothing read is in the new split.
func TestResplitAfterAChunkPastTheMidpoint(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, n int)")
	e.exec("INSERT INTO $schema.t SELECT i, i FROM generate_series(1, 100) i")
	s := e.source(`, "chunkRows": 80`)
	hi := "100"
	state, _ := json.Marshal(sourceState{keyRange: keyRange{Hi: &hi}})
	cs := source.ConfiguredStream{Name: "t", Splits: []source.Split{{ID: "a", State: state}}}
	rc := &splitCapture{col: "n"}
	var asked atomic.Bool
	rc.onBatch = func(stream, split string) {
		if split == "a" && !asked.Swap(true) {
			if ok, err := s.Resplit(t.Context(), stream, split); err != nil || !ok {
				t.Errorf("asked to split: %v, %v", ok, err)
			}
		}
	}
	e.mustRead(s, 1, rc, cs)
	sameValues(t, rc.values(), oneToN(100))
	events := rc.of("a")
	if len(events) < 2 || !events[1].resume || len(events[1].added) != 1 {
		t.Fatalf("split a's events: %+v", events)
	}
	if got := valuesOf(events[:1]); len(got) != 80 {
		t.Fatalf("first chunk read %d rows", len(got))
	}
	shrunk := parseState(t, events[1].state)
	if shrunk.Hi == nil || *shrunk.Hi != "90" || !slices.Equal(shrunk.After, []string{"80"}) {
		t.Errorf("split a at %s", events[1].state)
	}
	if want := valuesOf(events); slices.Contains(want, "91") || !slices.Contains(want, "90") {
		t.Errorf("split a read %q", want)
	}
	if got := string(events[1].added[0].State); got != `{"lo":"90","hi":"100"}` {
		t.Errorf("the new split: %s", got)
	}
	child := rc.of(events[1].added[0].ID)
	if got := valuesOf(child); !slices.Equal(got, []string{"91", "92", "93", "94", "95", "96", "97", "98", "99", "100"}) {
		t.Errorf("the new split read %q", got)
	}
	// Either part resumes on its own, from the state or the addition.
	rest := &splitCapture{col: "n"}
	e.mustRead(s, 2, rest, source.ConfiguredStream{Name: "t", Splits: []source.Split{{ID: "a", State: []byte(events[1].state)}, events[1].added[0]}})
	sameValues(t, rest.values(), []string{"81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "100"})
}

// The last range has no upper bound: the column's max says how far its
// remainder goes, and the new split keeps going past it. The same for a table
// read whole.
func TestResplitTheLastRange(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, n int)")
	e.exec("INSERT INTO $schema.t SELECT i, i FROM generate_series(1, 100) i")
	s := e.source(`, "chunkRows": 20`)
	rc := &splitCapture{col: "n"}
	var asked atomic.Bool
	rc.onBatch = func(stream, split string) {
		if split == source.WholeStream && !asked.Swap(true) {
			if ok, err := s.Resplit(t.Context(), stream, split); err != nil || !ok {
				t.Errorf("asked to split: %v, %v", ok, err)
			}
		}
	}
	e.mustRead(s, 2, rc, source.ConfiguredStream{Name: "t"})
	sameValues(t, rc.values(), oneToN(100))
	added := rc.added()
	if len(added) != 1 || string(added[0].State) != `{"lo":"60"}` {
		t.Fatalf("added %q", added)
	}
	if st := parseState(t, rc.of(source.WholeStream)[1].state); st.Hi == nil || *st.Hi != "60" {
		t.Errorf("the whole stream at %+v", st)
	}
}

// With no position yet, the first range's remainder starts at the column's
// min.
func TestResplitBeforeAnyRowUsesTheMin(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, seq int, n int)")
	e.exec("INSERT INTO $schema.t SELECT i, i % 10, i FROM generate_series(-50, 49) i")
	s := e.source(`, "chunkRows": 200`)
	mid, err := s.midpoint(t.Context(), e.conn, tableOf(t, e, s, "t"), keyRange{}, nil)
	if err != nil || mid == nil || *mid != "-1" {
		t.Errorf("midpoint of the table: %v, %v", mid, err)
	}
	mid, err = s.midpoint(t.Context(), e.conn, tableOf(t, e, s, "t"), keyRange{Lo: new("-40")}, nil)
	if err != nil || mid == nil || *mid != "5" {
		t.Errorf("midpoint above -40: %v, %v", mid, err)
	}
	e.exec("TRUNCATE $schema.t")
	mid, err = s.midpoint(t.Context(), e.conn, tableOf(t, e, s, "t"), keyRange{}, nil)
	if err != nil || mid != nil {
		t.Errorf("midpoint of an empty table: %v, %v", mid, err)
	}
}

func tableOf(t *testing.T, e *sourceEnv, s *Source, name string) table {
	t.Helper()
	tables, err := s.tables(t.Context(), e.conn)
	if err != nil {
		t.Fatal(err)
	}
	return tables[slices.IndexFunc(tables, func(tb table) bool { return tb.name == name })]
}

// Keys that aren't integers divide at a histogram bound strictly inside the
// remainder, in the column's own order.
func TestResplitAtHistogramBounds(t *testing.T) {
	for name, c := range map[string]struct{ cols, key, row string }{
		"text":      {"id text", "id", "md5(i::text)"},
		"collated":  {`id text COLLATE "C"`, "id", "md5(i::text)"},
		"char":      {"id char(32)", "id", "md5(i::text)"},
		"uuid":      {"id uuid", "id", "md5(i::text)::uuid"},
		"timestamp": {"id timestamptz", "id", "timestamptz '2026-01-01 00:00Z' + i * interval '1 hour'"},
		"text int":  {"a text, b int", "a, b", "(i % 200)::text, i"},
		"numeric":   {"id numeric(12, 3)", "id", "i / 7.0"},
	} {
		t.Run(name, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.t (" + c.cols + ", n int NOT NULL, PRIMARY KEY (" + c.key + "))")
			e.exec("INSERT INTO $schema.t SELECT " + c.row + ", i FROM generate_series(1, 1000) i")
			e.exec("ANALYZE $schema.t")
			s := e.source(`, "chunkRows": 50`)
			rc := &splitCapture{col: "n"}
			var asked atomic.Int64
			var wrong atomic.Value
			rc.onBatch = func(stream, split string) {
				// Each read split asks to split itself, a few times.
				if asked.Add(1) > 12 {
					return
				}
				if _, err := s.Resplit(t.Context(), stream, split); err != nil {
					wrong.Store(err)
				}
			}
			e.mustRead(s, 4, rc, source.ConfiguredStream{Name: "t"})
			if err := wrong.Load(); err != nil {
				t.Error(err)
			}
			sameValues(t, rc.values(), oneToN(1000))
			if n := len(rc.added()); n == 0 {
				t.Errorf("no split was added")
			}
			seen := map[string]bool{source.WholeStream: true}
			for _, a := range rc.added() {
				if seen[a.ID] {
					t.Errorf("split %q added twice", a.ID)
				}
				seen[a.ID] = true
			}
		})
	}
}

// Without bounds inside it, a range's remainder isn't divided, and the split
// says so to whoever asks next.
func TestResplitWithoutBoundsRefuses(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id text PRIMARY KEY, n int) WITH (autovacuum_enabled = false)")
	e.exec("INSERT INTO $schema.t SELECT lpad(i::text, 4, '0'), i FROM generate_series(1, 100) i")
	s := e.source(`, "chunkRows": 20`)
	rc := &splitCapture{col: "n"}
	var calls atomic.Int64
	var answers [3]atomic.Bool
	rc.onBatch = func(stream, split string) {
		i := calls.Add(1) - 1
		if i >= int64(len(answers)) {
			return
		}
		ok, err := s.Resplit(t.Context(), stream, split)
		if err != nil {
			t.Error(err)
		}
		answers[i].Store(ok)
	}
	e.mustRead(s, 2, rc, source.ConfiguredStream{Name: "t"})
	sameValues(t, rc.values(), oneToN(100))
	if n := len(rc.added()); n != 0 {
		t.Errorf("added %d splits without statistics", n)
	}
	if !answers[0].Load() || answers[1].Load() || answers[2].Load() {
		t.Errorf("answered %v, %v, %v: first yes, then no once it knows", answers[0].Load(), answers[1].Load(), answers[2].Load())
	}
	// Bounds only outside the remainder are none.
	e.exec("ANALYZE $schema.t")
	got, err := s.boundsInside(t.Context(), e.conn, tableOf(t, e, s, "t"), keyRange{Lo: new("0100"), Hi: new("9999")})
	if err != nil || len(got) != 0 {
		t.Errorf("bounds above the last: %q, %v", got, err)
	}
	got, err = s.boundsInside(t.Context(), e.conn, tableOf(t, e, s, "t"), keyRange{Lo: new("0010"), Hi: new("0012")})
	if err != nil || !slices.Equal(got, []string{"0011"}) {
		t.Errorf("bounds between 0010 and 0012: %q, %v", got, err)
	}
}

// What can't be split is refused at once.
func TestResplitRefuses(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.keyed (id int PRIMARY KEY, n int)")
	e.exec("INSERT INTO $schema.keyed SELECT i, i FROM generate_series(1, 100) i")
	e.exec("CREATE TABLE $schema.loose (n int)")
	e.exec("INSERT INTO $schema.loose SELECT generate_series(1, 100)")

	s := e.source(`, "chunkRows": 40`)
	ask := func(stream, split string) bool {
		t.Helper()
		ok, err := s.Resplit(t.Context(), stream, split)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	// No read running.
	if ask("keyed", source.WholeStream) {
		t.Error("split a split with no read running")
	}

	// Not keyed, unknown, and already asked.
	rc := &splitCapture{col: "n"}
	var once sync.Once
	rc.onBatch = func(stream, split string) {
		once.Do(func() {
			if ask("keyed", "other") {
				t.Error("split an unknown split")
			}
			if ask("nope", source.WholeStream) {
				t.Error("split an unknown stream")
			}
			if !ask("keyed", source.WholeStream) {
				t.Error("refused the first ask")
			}
			if ask("keyed", source.WholeStream) {
				t.Error("took a second ask before the first was answered")
			}
		})
	}
	e.mustRead(s, 2, rc, source.ConfiguredStream{Name: "keyed"})
	if len(rc.added()) != 1 {
		t.Errorf("added %q", rc.added())
	}

	// A table without a key.
	rc = &splitCapture{col: "n"}
	rc.onBatch = func(stream, split string) {
		if ask(stream, split) {
			t.Error("split a table without a key")
		}
	}
	e.mustRead(s, 1, rc, source.ConfiguredStream{Name: "loose"})

	// A split on its last chunk: its next ask is too late.
	rc = &splitCapture{col: "n"}
	lo := "80"
	state, _ := json.Marshal(sourceState{keyRange: keyRange{Lo: &lo}})
	rc.onBatch = func(stream, split string) {
		if !ask(stream, split) {
			t.Error("refused a split whose chunk isn't known to be its last")
		}
	}
	e.mustRead(s, 1, rc, source.ConfiguredStream{Name: "keyed", Splits: []source.Split{{ID: "tail", State: state}}})
	if len(rc.added()) != 0 {
		t.Errorf("added %q past the last row", rc.added())
	}

	// A range with no keys left to divide, once its first chunk is read.
	hi := "41"
	state, _ = json.Marshal(sourceState{keyRange: keyRange{Hi: &hi}})
	rc = &splitCapture{col: "n"}
	calls := 0
	rc.onBatch = func(stream, split string) {
		calls++
		if calls == 2 && ask(stream, split) {
			t.Error("took an ask for a range with no keys to divide")
		}
	}
	e.mustRead(s, 1, rc, source.ConfiguredStream{Name: "keyed", Splits: []source.Split{{ID: "short", State: state}}})
	if calls != 2 || len(rc.added()) != 0 {
		t.Errorf("%d batches, added %q", calls, rc.added())
	}
}

// A cdc stream isn't split.
func TestResplitRefusesCDC(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.users (id int PRIMARY KEY, name text, doc text)")
	s, pipeline := e.cdcSource("")
	f := follow(t, s, pipeline, map[string]string{"users": ""})
	f.await(t, "a resume point", func() bool { return f.last("users") != "" })
	if ok, err := s.Resplit(t.Context(), "users", source.WholeStream); ok || err != nil {
		t.Errorf("split a cdc stream: %v, %v", ok, err)
	}
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
}

// A composite key's position leaves more of its first column's value to read:
// the lower part keeps them.
func TestResplitACompositeKey(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (a int, b int, n int NOT NULL, PRIMARY KEY (a, b))")
	e.exec("INSERT INTO $schema.t SELECT i / 10, i % 10, i + 1 FROM generate_series(0, 99) i")
	s := e.source(`, "chunkRows": 25`)
	rc := &splitCapture{col: "n"}
	var asked atomic.Bool
	rc.onBatch = func(stream, split string) {
		if split == source.WholeStream && !asked.Swap(true) {
			if ok, err := s.Resplit(t.Context(), stream, split); err != nil || !ok {
				t.Errorf("asked to split: %v, %v", ok, err)
			}
		}
	}
	e.mustRead(s, 2, rc, source.ConfiguredStream{Name: "t"})
	sameValues(t, rc.values(), oneToN(100))
	events := rc.of(source.WholeStream)
	st := parseState(t, events[1].state)
	// Read up to (2, 4); the rest of a = 2 stays below.
	if !slices.Equal(st.After, []string{"2", "4"}) || st.Hi == nil || *st.Hi != "5" {
		t.Errorf("the lower part: %s", events[1].state)
	}
	if got := string(rc.added()[0].State); got != `{"lo":"5"}` {
		t.Errorf("the new split: %s", got)
	}
}

// An incremental read's position isn't on the key: rows of any key can follow
// it, so both parts keep it, and share its floor and ceiling.
func TestResplitAnIncrementalRead(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, seq int, n int)")
	e.exec("INSERT INTO $schema.t SELECT i, (i * 37) % 100, i FROM generate_series(1, 400) i")
	s := e.source(`, "chunkRows": 30`)
	cs := source.ConfiguredStream{Name: "t", SyncMode: source.Incremental, Cursor: "seq"}
	rc := &splitCapture{col: "n"}
	var asked atomic.Int64
	rc.onBatch = func(stream, split string) {
		if asked.Add(1) <= 6 {
			if _, err := s.Resplit(t.Context(), stream, split); err != nil {
				t.Error(err)
			}
		}
	}
	e.mustRead(s, 3, rc, cs)
	sameValues(t, rc.values(), oneToN(400))
	if len(rc.added()) == 0 {
		t.Fatal("no split was added")
	}
	for _, a := range rc.added() {
		st := parseState(t, string(a.State))
		if st.Until == nil || *st.Until != "99" || st.Lo == nil || len(st.After) != 2 {
			t.Errorf("the new split %q: %s", a.ID, a.State)
		}
		events := rc.of(a.ID)
		if last := events[len(events)-1]; !last.complete || last.state != `{"since":"99"}` {
			t.Errorf("the new split %q ended with %+v", a.ID, last)
		}
	}
}

// Splits ask to split each other at random throughout a concurrent read, of
// every key type that divides, and every row is read once. A restart from a
// checkpoint taken part-way reads exactly the rest.
func TestResplitAtRandom(t *testing.T) {
	for name, c := range map[string]struct{ cols, key, row string }{
		"int":      {"id int", "id", "i"},
		"sparse":   {"id bigint", "id", "i::bigint * i * i"},
		"text":     {"id text", "id", "md5(i::text)"},
		"uuid":     {"id uuid", "id", "md5(i::text)::uuid"},
		"int text": {"a int, b text", "a, b", "i % 300, md5(i::text)"},
	} {
		t.Run(name, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec("CREATE TABLE $schema.t (" + c.cols + ", n int NOT NULL, PRIMARY KEY (" + c.key + "))")
			e.exec("INSERT INTO $schema.t SELECT " + c.row + ", i FROM generate_series(1, 3000) i")
			e.exec("ANALYZE $schema.t")
			s := e.source(`, "chunkRows": 40`)
			cs := source.ConfiguredStream{Name: "t"}
			splits := e.mustSplits(s, cs, nil)
			rc := &splitCapture{col: "n"}
			rng := rand.New(rand.NewPCG(1, 2))
			var mu sync.Mutex
			known := map[string]bool{}
			for _, sp := range splits {
				known[sp.ID] = true
			}
			var accepted atomic.Int64
			rc.onBatch = func(stream, split string) {
				mu.Lock()
				defer mu.Unlock()
				known[split] = true
				ids := slices.Sorted(maps.Keys(known))
				pick := ids[rng.IntN(len(ids))]
				if ok, err := s.Resplit(t.Context(), stream, pick); err != nil {
					t.Error(err)
				} else if ok {
					accepted.Add(1)
				}
			}
			e.mustRead(s, 4, rc, inSplits(cs, splits))
			sameValues(t, rc.values(), oneToN(3000))
			added := rc.added()
			if len(added) == 0 || accepted.Load() == 0 {
				t.Errorf("%d asks accepted, %d splits added", accepted.Load(), len(added))
			}
			seen := map[string]bool{}
			for _, sp := range splits {
				seen[sp.ID] = true
			}
			for _, a := range added {
				if seen[a.ID] {
					t.Errorf("split %q added twice", a.ID)
				}
				seen[a.ID] = true
			}

			// A checkpoint part-way: each split's first resume point, and the
			// splits those added.
			var again []source.Split
			var before []string
			var walk func(id string, state []byte)
			walk = func(id string, state []byte) {
				events := rc.of(id)
				at := slices.IndexFunc(events, func(e splitEvent) bool { return e.resume })
				if at < 0 {
					again = append(again, source.Split{ID: id, State: state})
					return
				}
				before = append(before, valuesOf(events[:at])...)
				again = append(again, source.Split{ID: id, State: []byte(events[at].state)})
				for _, a := range events[at].added {
					walk(a.ID, a.State)
				}
			}
			for _, sp := range splits {
				walk(sp.ID, sp.State)
			}
			rest := &splitCapture{col: "n"}
			e.mustRead(s, 4, rest, inSplits(cs, again))
			sameValues(t, append(before, rest.values()...), oneToN(3000))
		})
	}
}

// A connection that fails a split's read fails the read, with the others
// stopped.
func TestSplitsStopOnAFailure(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, n int)")
	e.exec("INSERT INTO $schema.t SELECT i, i FROM generate_series(1, 200) i")
	s := e.source(`, "chunkRows": 20`)
	bad := source.Split{ID: "bad", State: []byte(`{"after":["1","2","3"]}`)}
	good := source.Split{ID: "good", State: []byte(`{"lo":"100"}`)}
	err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "t", Splits: []source.Split{good, bad}}}, Lanes: 2}, &splitCapture{col: "n"})
	if err == nil || !strings.Contains(err.Error(), "3 key columns") {
		t.Errorf("read with a bad state: %v", err)
	}
	if err := s.Read(context.Background(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "missing"}}}, &splitCapture{col: "n"}); err == nil {
		t.Error("read a missing table")
	}
}

// The connector says it splits and re-splits, and does both over the protocol.
func TestSourceServesSplits(t *testing.T) {
	e := newSourceEnv(t)
	// Enough that flow control keeps the source from finishing before the
	// asking gets to it.
	e.exec("CREATE TABLE $schema.t (id int PRIMARY KEY, n int, v text)")
	e.exec("INSERT INTO $schema.t SELECT i, i, repeat('x', 500) FROM generate_series(1, 100000) i")
	e.exec("ANALYZE $schema.t")
	launch := conformance.Exec(conformance.Build(t, "../cmd/source-postgres"))
	p, err := launch(t.Context(), fmt.Appendf(nil, `{"dsn": %q, "schema": %q, "chunkRows": 1000}`, dsn(), e.schema))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	spec, err := protocolv1.NewConnectorServiceClient(p.Conn()).Spec(t.Context(), &protocolv1.SpecRequest{Host: conformance.Host})
	if err != nil {
		t.Fatal(err)
	}
	if c := spec.GetSource(); !c.GetSplits() || !c.GetResplit() || !c.GetAcknowledges() {
		t.Errorf("capabilities %+v", c)
	}
	client := protocolv1.NewSourceServiceClient(p.Conn())
	cs := &protocolv1.ConfiguredStream{Name: "t", SyncMode: protocolv1.SyncMode_SYNC_MODE_FULL_REFRESH}
	splits, err := client.Splits(t.Context(), &protocolv1.SplitsRequest{Config: p.Config(), Stream: cs})
	if err != nil {
		t.Fatal(err)
	}
	if len(splits.GetSplits()) != maxKeyRanges {
		t.Fatalf("%d splits", len(splits.GetSplits()))
	}
	cs.Splits = splits.GetSplits()
	read, err := client.Read(t.Context(), &protocolv1.ReadRequest{Config: p.Config(), Streams: []*protocolv1.ConfiguredStream{cs}, Lanes: 3})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	ended := map[string]bool{}
	var added []*protocolv1.Split
	asked := false
	for {
		m, err := read.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case m.GetBatch() != nil:
			rec, err := arrowipc.DecodeBatch(m.GetBatch().GetArrow(), memory.DefaultAllocator)
			if err != nil {
				t.Fatal(err)
			}
			col := rec.Column(rec.Schema().FieldIndices("n")[0])
			for i := range col.Len() {
				got = append(got, col.ValueStr(i))
			}
			rec.Release()
			if !asked {
				r, err := client.Resplit(t.Context(), &protocolv1.ResplitRequest{Stream: "t", Split: m.GetBatch().GetSplit()})
				if err != nil {
					t.Fatal(err)
				}
				asked = r.GetAccepted()
			}
		case m.GetResumePoint() != nil:
			added = append(added, m.GetResumePoint().GetAdded()...)
		case m.GetComplete() != nil:
			ended[m.GetComplete().GetSplit()] = true
		}
	}
	sameValues(t, got, oneToN(100000))
	if !asked {
		t.Error("no ask to split was accepted")
	}
	if len(ended) != maxKeyRanges+len(added) {
		t.Errorf("%d splits ended, with %d added", len(ended), len(added))
	}
}
