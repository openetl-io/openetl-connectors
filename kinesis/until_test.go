//go:build integration

package kinesis

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// covered is a capture that also keeps how many rows came before each
// resume point.
type covered struct {
	*capture
	at []int
}

func (c *covered) ResumePoint(stream string, state []byte) error {
	c.mu.Lock()
	c.at = append(c.at, len(c.rows[stream]))
	c.mu.Unlock()
	return c.capture.ResumePoint(stream, state)
}

func (c *covered) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(c, stream, id)
}

// A read given an end, a resume point of an earlier read, reads every shard
// up to it, across a split, then sends a resume point there and completes
// there, with nothing after it.
func TestSourceStopsAtUntil(t *testing.T) {
	stream := uniq("until")
	createStream(t, stream, 1)
	putJSON(t, stream, 0, 120)
	parent := "shardId-000000000000"
	split(t, stream, parent)
	putJSON(t, stream, 120, 80)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	first := &covered{capture: newCapture()}
	readWith(t, s, first, source.ConfiguredStream{Name: stream, SyncMode: source.Stream})
	if n := len(first.rows[stream]); n != 200 {
		t.Fatalf("read %d rows", n)
	}
	points := first.states[stream]
	if len(points) < 3 {
		t.Fatalf("%d resume points", len(points))
	}
	for i, until := range points {
		c := newCapture()
		readWith(t, s, c, source.ConfiguredStream{Name: stream, SyncMode: source.Stream, Until: until})
		want := rowIDs(t, first.rows[stream][:first.at[i]])
		got := rowIDs(t, c.rows[stream])
		if len(got) != len(want) || len(c.rows[stream]) != first.at[i] {
			t.Errorf("until resume point %d: %d rows, want %d", i, len(c.rows[stream]), first.at[i])
		}
		for id := range want {
			if got[id] != 1 {
				t.Errorf("until resume point %d: id %d read %d times", i, id, got[id])
			}
		}
		states := c.states[stream]
		if len(states) == 0 || !bytes.Equal(states[len(states)-1], until) || !bytes.Equal(c.complete[stream], until) {
			t.Errorf("until resume point %d: ended at %s, completed at %s, want %s", i, states[len(states)-1], c.complete[stream], until)
		}
		if first.at[i] > 120 {
			checkOrder(t, c.rows[stream], parent)
		}
	}

	// From a resume point to a later one.
	c := newCapture()
	readWith(t, s, c, source.ConfiguredStream{Name: stream, SyncMode: source.Stream, State: points[0], Until: points[len(points)-1]})
	if got, want := len(c.rows[stream]), first.at[len(points)-1]-first.at[0]; got != want {
		t.Errorf("between two resume points: %d rows, want %d", got, want)
	}
}

// A split given an end reads up to it and completes there, adding no
// children, even when the end is its end.
func TestSourceSplitStopsAtUntil(t *testing.T) {
	stream := uniq("until_split")
	createStream(t, stream, 1)
	putJSON(t, stream, 0, 60)
	parent := "shardId-000000000000"
	split(t, stream, parent)
	putJSON(t, stream, 60, 20)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	splits, err := s.Splits(t.Context(), source.ConfiguredStream{Name: stream, SyncMode: source.Stream}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) != 1 || splits[0].ID != parent {
		t.Fatalf("splits %v", splits)
	}
	all := newSplitCapture(map[string][]source.Split{stream: splits})
	readWith(t, s, all, source.ConfiguredStream{Name: stream, SyncMode: source.Stream, Splits: splits})
	rows := all.splits[stream][parent].rows
	if len(rows) != 60 {
		t.Fatalf("the parent has %d rows", len(rows))
	}
	for _, tc := range []struct {
		name string
		n    int
		end  shardState
	}{
		{"partway", 25, shardState{After: rows[24][sequenceColumn].(string)}},
		{"at its end", 60, shardState{After: rows[59][sequenceColumn].(string), Done: true}},
		{"before anything", 0, shardState{}},
	} {
		until, _ := json.Marshal(tc.end)
		sp := []source.Split{{ID: parent, Until: until}}
		c := newSplitCapture(map[string][]source.Split{stream: sp})
		readWith(t, s, c, source.ConfiguredStream{Name: stream, SyncMode: source.Stream, Splits: sp})
		rec := c.splits[stream][parent]
		wantIDs(t, rowIDs(t, rec.rows), 0, tc.n)
		if !rec.complete || !bytes.Equal(rec.state, until) || len(rec.added) > 0 || len(c.splits[stream]) != 1 {
			t.Errorf("%s: completed %v at %s, adding %v, want at %s and none", tc.name, rec.complete, rec.state, rec.added, until)
		}
	}
}

// Splits read together: one given an end stops there while one without
// reads to where it catches up; and a parent given its end, done, lets a
// child among the read's splits read to the child's end after it.
func TestSourceSplitsStopAtUntilTogether(t *testing.T) {
	two := uniq("until_two")
	createStream(t, two, 2)
	putJSON(t, two, 0, 80)
	s := newSource(t, map[string]any{"streams": []string{two}})
	splits, err := s.Splits(t.Context(), source.ConfiguredStream{Name: two, SyncMode: source.Stream}, nil)
	if err != nil || len(splits) != 2 {
		t.Fatalf("splits %v: %v", splits, err)
	}
	all := newSplitCapture(map[string][]source.Split{two: splits})
	readWith(t, s, all, source.ConfiguredStream{Name: two, SyncMode: source.Stream, Splits: splits})
	a, b := splits[0].ID, splits[1].ID
	rowsA, rowsB := all.splits[two][a].rows, all.splits[two][b].rows
	if len(rowsA) < 2 || len(rowsB) == 0 {
		t.Fatalf("the shards have %d and %d rows", len(rowsA), len(rowsB))
	}
	k := len(rowsA) / 2
	until, _ := json.Marshal(shardState{After: rowsA[k-1][sequenceColumn].(string)})
	sp := []source.Split{{ID: a, Until: until}, {ID: b}}
	c := newSplitCapture(map[string][]source.Split{two: sp})
	readWith(t, s, c, source.ConfiguredStream{Name: two, SyncMode: source.Stream, Splits: sp})
	if got := len(c.splits[two][a].rows); got != k || !bytes.Equal(c.splits[two][a].state, until) {
		t.Errorf("the split given an end read %d rows, want %d, and completed at %s", got, k, c.splits[two][a].state)
	}
	if got := len(c.splits[two][b].rows); got != len(rowsB) || !c.splits[two][b].complete {
		t.Errorf("the split without an end read %d rows, want %d", got, len(rowsB))
	}

	stream := uniq("until_child")
	createStream(t, stream, 1)
	putJSON(t, stream, 0, 60)
	parent := "shardId-000000000000"
	split(t, stream, parent)
	putJSON(t, stream, 60, 40)
	s = newSource(t, map[string]any{"streams": []string{stream}})
	start := []source.Split{{ID: parent}}
	all = newSplitCapture(map[string][]source.Split{stream: start})
	readWith(t, s, all, source.ConfiguredStream{Name: stream, SyncMode: source.Stream, Splits: start})
	var child string
	for id, rec := range all.splits[stream] {
		if id != parent && len(rec.rows) >= 2 {
			child = id
		}
	}
	if child == "" {
		t.Fatal("no child has two rows")
	}
	parentRows, childRows := all.splits[stream][parent].rows, all.splits[stream][child].rows
	parentEnd, _ := json.Marshal(shardState{After: parentRows[len(parentRows)-1][sequenceColumn].(string), Done: true})
	k = len(childRows) / 2
	childEnd, _ := json.Marshal(shardState{After: childRows[k-1][sequenceColumn].(string)})
	sp = []source.Split{{ID: parent, Until: parentEnd}, {ID: child, Until: childEnd}}
	c = newSplitCapture(map[string][]source.Split{stream: sp})
	readWith(t, s, c, source.ConfiguredStream{Name: stream, SyncMode: source.Stream, Splits: sp})
	p, ch := c.splits[stream][parent], c.splits[stream][child]
	if len(p.rows) != 60 || !bytes.Equal(p.state, parentEnd) || len(p.added) > 0 {
		t.Errorf("the parent read %d rows, completed at %s adding %v", len(p.rows), p.state, p.added)
	}
	if len(ch.rows) != k || !bytes.Equal(ch.state, childEnd) || ch.firstRow < p.completed {
		t.Errorf("the child read %d rows, want %d, completed at %s, its first row at %d, the parent completed at %d", len(ch.rows), k, ch.state, ch.firstRow, p.completed)
	}
	if len(c.splits[stream]) != 2 {
		t.Errorf("splits %v", c.states(stream, false))
	}
}

func readWith(t *testing.T, s *Source, out source.Emitter, st source.ConfiguredStream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err := s.Read(ctx, source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{st}}, out); err != nil {
		t.Fatal(err)
	}
}

// A shard idle for longer than half the retention, synced less often than
// that but within it, keeps its position: each sync that finds it caught up
// records so, and the next doesn't take it for lost, even with nothing left
// in the shard to show where the position was.
func TestSourceIdleShardKeepsItsPosition(t *testing.T) {
	stream := uniq("idle")
	createStream(t, stream, 1)
	shard := "shardId-000000000000"
	s := newSource(t, map[string]any{"streams": []string{stream}})
	retention := 24 * time.Hour
	at := encodeState(map[string]shardState{shard: {Since: time.Now().Add(-retention * 5 / 12)}})
	for sync := range 3 {
		start := time.Now()
		c := read(t, s, stream, at)
		var st state
		if err := json.Unmarshal(c.complete[stream], &st); err != nil {
			t.Fatal(err)
		}
		if since := st.Shards[shard].Since; since.Before(start) {
			t.Fatalf("sync %d: the caught-up shard's Since is %v, before the sync started", sync, since)
		}
		// The next sync, 20 hours on.
		sh := st.Shards[shard]
		sh.Since = sh.Since.Add(-20 * time.Hour)
		at = encodeState(map[string]shardState{shard: sh})
	}
}
