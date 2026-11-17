package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestProgress(t *testing.T) {
	now := time.Now()
	r := &topicRead{name: "t", next: map[int32]int64{0: 5, 1: 9}, hwm: map[int32]int64{0: 8, 1: 9}, last: now.Add(-time.Hour), retention: 7 * 24 * time.Hour}
	p := r.progress(now)
	if *p.LagRecords != 3 || *p.Lag != time.Hour || p.RetentionLeft == nil || *p.RetentionLeft != 7*24*time.Hour-time.Hour {
		t.Errorf("behind: %+v", p)
	}
	r.next[0] = 8
	if p := r.progress(now); *p.LagRecords != 0 || *p.RetentionLeft != 7*24*time.Hour {
		t.Errorf("caught up: %+v", p)
	}
	r.next[0], r.last = 5, time.Time{}
	if p := r.progress(now); p.RetentionLeft != nil {
		t.Errorf("behind, with no record read: %+v", p)
	}
	r.retention = 0
	if p := r.progress(now); p.RetentionLeft != nil {
		t.Errorf("unknown retention: %+v", p)
	}
}

func TestSplitOffsets(t *testing.T) {
	got, err := splitOffsets([]source.Split{{ID: "p0", State: encodeOffset(5)}, {ID: "p12", State: encodeOffset(0)}, {ID: "p3"}})
	if err != nil || !maps.Equal(got, map[int32]int64{0: 5, 12: 0}) {
		t.Errorf("splits: %v, %v", got, err)
	}
	got, err = splitOffsets([]source.Split{{ID: source.WholeStream, State: encodeState(map[int32]int64{1: 7, 2: 9})}})
	if err != nil || !maps.Equal(got, map[int32]int64{1: 7, 2: 9}) {
		t.Errorf("whole: %v, %v", got, err)
	}
	got, err = splitOffsets(nil)
	if err != nil || len(got) != 0 {
		t.Errorf("never read: %v, %v", got, err)
	}
	for _, id := range []string{"0", "px", "shardId-1"} {
		if _, err := splitOffsets([]source.Split{{ID: id}}); err == nil {
			t.Errorf("split %q isn't a partition", id)
		}
	}
	if _, err := splitOffsets([]source.Split{{ID: "p1", State: []byte("{")}}); err == nil {
		t.Error("a broken state")
	}
}

// readEvents reads cs to its end, and returns what each split emitted, as
// splitEvents spells it.
func readEvents(t *testing.T, s *Source, cs source.ConfiguredStream) map[string][]string {
	t.Helper()
	out := &splitEvents{events: map[string][]string{}}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Pipeline: "p", Lanes: 1}, out); err != nil {
		t.Fatal(err)
	}
	return out.events
}

// rowsIn are the rows of events, sorted.
func rowsIn(events []string) []string {
	var out []string
	for _, e := range events {
		if e[0] == 'p' {
			out = append(out, e)
		}
	}
	slices.Sort(out)
	return out
}

// lastIs says events end with want, after any resume points at the same
// state.
func lastIs(events []string, want string) bool {
	if len(events) == 0 || events[len(events)-1] != want {
		return false
	}
	resume := "resume " + strings.TrimPrefix(want, "complete ")
	for _, e := range slices.Backward(events[:len(events)-1]) {
		if e[0] == 'p' || strings.HasPrefix(e, "complete") {
			break
		}
		if e != resume && strings.HasPrefix(e, "resume") {
			return false
		}
	}
	return true
}

// A read given where a split, or the whole stream, completed before reads
// up to there and completes there again, though the topic has moved on.
func TestASourceCompletesWhereItIsAsked(t *testing.T) {
	addrs := fake(t)
	topic := "events"
	produceRecords(t, addrs, topic, 2, 30, 10)
	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	first := readEvents(t, s, source.ConfiguredStream{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(5)}, {ID: "p1", State: encodeOffset(0)}}})
	end := "complete " + string(encodeOffset(30))
	if !lastIs(first["p0"], end) || !lastIs(first["p1"], end) {
		t.Fatalf("the first read ended with %v and %v", first["p0"], first["p1"])
	}
	produceMore(t, addrs, topic, 0, 5)
	produceMore(t, addrs, topic, 1, 5)

	got := readEvents(t, s, source.ConfiguredStream{Name: topic, Splits: []source.Split{
		{ID: "p0", State: encodeOffset(5), CompleteAt: encodeOffset(30)},
		{ID: "p1", State: encodeOffset(0), CompleteAt: encodeOffset(30)},
	}})
	if r := rowsIn(got["p0"]); !slices.Equal(r, rowsIn(at(0, 5, 30))) {
		t.Errorf("split p0 read %v", r)
	}
	if r := rowsIn(got["p1"]); !slices.Equal(r, rowsIn(at(1, 0, 30))) {
		t.Errorf("split p1 read %v", r)
	}
	if !lastIs(got["p0"], end) || !lastIs(got["p1"], end) {
		t.Errorf("the splits ended with %v and %v", got["p0"], got["p1"])
	}

	from, completed := encodeState(map[int32]int64{0: 2, 1: 3}), encodeState(map[int32]int64{0: 30, 1: 30})
	got = readEvents(t, s, source.ConfiguredStream{Name: topic, State: from, CompleteAt: completed})
	ev := got[source.WholeStream]
	if r := rowsIn(ev); !slices.Equal(r, rowsIn(append(at(0, 2, 30), at(1, 3, 30)...))) {
		t.Errorf("the whole stream read %v", r)
	}
	if !lastIs(ev, "complete "+string(completed)) {
		t.Errorf("the whole stream ended with %v", ev)
	}

	// A continuous read has no end to complete at, so it doesn't stop there.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	out := &splitEvents{events: map[string][]string{}}
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(28), CompleteAt: encodeOffset(30)}}}}, Pipeline: "p", Lanes: 1, Continuous: true}, out)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		out.mu.Lock()
		n := len(rowsIn(out.events["p0"]))
		out.mu.Unlock()
		if n == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a continuous read read %d rows of 7", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
}

// Where the offsets just below a stop no longer return records, as once log
// cleanup has removed an aborted transaction and its marker, a read stops
// there all the same, at the first record past it, and completes at the
// stop rather than at the last record it read.
func TestASourceStopsWhereCleanupRemovedTheLastRecords(t *testing.T) {
	c := fakeCluster(t)
	addrs := c.ListenAddrs()
	topic := "compacted"
	if err := c.CreateTopic(topic, 1, map[string]string{"cleanup.policy": "compact"}); err != nil {
		t.Fatal(err)
	}
	produceKeyed := func(opts []kgo.Opt, keys ...string) {
		t.Helper()
		cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(addrs...), kgo.DefaultProduceTopic(topic)}, opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		var recs []*kgo.Record
		for _, k := range keys {
			recs = append(recs, &kgo.Record{Key: []byte(k), Value: fmt.Appendf(nil, `{"k":%q}`, k)})
		}
		if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	produceKeyed(nil, "a", "b", "c", "d", "e")
	tx, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.DefaultProduceTopic(topic), kgo.TransactionalID("aborts"))
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if err := tx.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := tx.ProduceSync(t.Context(), &kgo.Record{Key: []byte("x"), Value: []byte(`{"k":"x"}`)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := tx.EndTransaction(t.Context(), kgo.TryAbort); err != nil {
		t.Fatal(err)
	}

	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	first := readEvents(t, s, source.ConfiguredStream{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(0)}}})
	want := rowsIn(at(0, 0, 5))
	if r := rowsIn(first["p0"]); !slices.Equal(r, want) {
		t.Fatalf("the first read read %v", r)
	}
	ev := first["p0"]
	end := strings.TrimPrefix(ev[len(ev)-1], "complete ")
	var st splitState
	if err := json.Unmarshal([]byte(end), &st); err != nil || st.Offset <= 6 {
		t.Fatalf("the first read completed at %s, not past the aborted transaction's marker", end)
	}

	// The topic moves on, and compaction removes the aborted transaction.
	produceKeyed(nil, "f")
	c.Compact()
	if got := offsets(t, addrs, topic, st.Offset); !slices.Equal(got, []int64{0, 1, 2, 3, 4, st.Offset}) {
		t.Fatalf("after compaction the partition returns offsets %v", got)
	}

	for _, sp := range []source.Split{
		{ID: "p0", State: encodeOffset(0), CompleteAt: []byte(end)},
		{ID: "p0", State: encodeOffset(0), Until: []byte(end)},
	} {
		begun := time.Now()
		got := readEvents(t, s, source.ConfiguredStream{Name: topic, Splits: []source.Split{sp}})
		if r := rowsIn(got["p0"]); !slices.Equal(r, want) {
			t.Errorf("a read to %s read %v", end, r)
		}
		if !lastIs(got["p0"], "complete "+end) {
			t.Errorf("a read to %s ended with %v", end, got["p0"])
		}
		if sp.Until != nil && !slices.Contains(got["p0"], "resume "+end) {
			t.Errorf("a read until %s sent no resume point there: %v", end, got["p0"])
		}
		if d := time.Since(begun); d >= idleAfter {
			t.Errorf("a read to %s waited %v for the records cleanup removed", end, d)
		}
	}
	from, whole := encodeState(map[int32]int64{0: 0}), encodeState(map[int32]int64{0: st.Offset})
	got := readEvents(t, s, source.ConfiguredStream{Name: topic, State: from, CompleteAt: whole})
	if r := rowsIn(got[source.WholeStream]); !slices.Equal(r, want) || !lastIs(got[source.WholeStream], "complete "+string(whole)) {
		t.Errorf("the whole stream read %v, and ended with %v", r, got[source.WholeStream])
	}
}

// offsets are the offsets a read-committed fetch of partition 0 returns up
// to last, control records among them.
func offsets(t *testing.T, addrs []string, topic string, last int64) []int64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords(), kgo.FetchMaxWait(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var out []int64
	for ctx.Err() == nil && !slices.ContainsFunc(out, func(o int64) bool { return o >= last }) {
		for _, r := range cl.PollFetches(ctx).Records() {
			out = append(out, r.Offset)
		}
	}
	return out
}
