package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
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
	got, err := splitOffsets([]source.Split{{ID: "p0", State: encodeOffset(5, "")}, {ID: "p12", State: encodeOffset(0, "")}, {ID: "p3"}})
	if err != nil || !maps.Equal(got, map[int32]int64{0: 5, 12: 0}) {
		t.Errorf("splits: %v, %v", got, err)
	}
	got, err = splitOffsets([]source.Split{{ID: source.WholeStream, State: encodeState(map[int32]int64{1: 7, 2: 9}, "")}})
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
	first := readEvents(t, s, source.ConfiguredStream{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(5, "")}, {ID: "p1", State: encodeOffset(0, "")}}})
	end := "complete " + string(encodeOffset(30, ""))
	if !lastIs(first["p0"], end) || !lastIs(first["p1"], end) {
		t.Fatalf("the first read ended with %v and %v", first["p0"], first["p1"])
	}
	produceMore(t, addrs, topic, 0, 5)
	produceMore(t, addrs, topic, 1, 5)

	got := readEvents(t, s, source.ConfiguredStream{Name: topic, Splits: []source.Split{
		{ID: "p0", State: encodeOffset(5, ""), CompleteAt: encodeOffset(30, "")},
		{ID: "p1", State: encodeOffset(0, ""), CompleteAt: encodeOffset(30, "")},
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

	from, completed := encodeState(map[int32]int64{0: 2, 1: 3}, ""), encodeState(map[int32]int64{0: 30, 1: 30}, "")
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
		done <- s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(28, ""), CompleteAt: encodeOffset(30, "")}}}}, Pipeline: "p", Lanes: 1, Continuous: true}, out)
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
// there all the same, and completes at the stop rather than at the last
// record it read: at the first record past it that it fetches, or, if it
// fetches none, once a fetch of its own shows nothing left below the stop.
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
	first := readEvents(t, s, source.ConfiguredStream{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(0, "")}}})
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
		{ID: "p0", State: encodeOffset(0, ""), CompleteAt: []byte(end)},
		{ID: "p0", State: encodeOffset(0, ""), Until: []byte(end)},
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
	from, whole := encodeState(map[int32]int64{0: 0}, ""), encodeState(map[int32]int64{0: st.Offset}, "")
	got := readEvents(t, s, source.ConfiguredStream{Name: topic, State: from, CompleteAt: whole})
	if r := rowsIn(got[source.WholeStream]); !slices.Equal(r, want) || !lastIs(got[source.WholeStream], "complete "+string(whole)) {
		t.Errorf("the whole stream read %v, and ended with %v", r, got[source.WholeStream])
	}

	// The read's own fetches return nothing, so only a fetch of the
	// source's own, which the consumer's long polls aren't, can show that
	// nothing is left between the last record and the stop.
	shortWaits(t, 100*time.Millisecond, 10*time.Second)
	c.ControlKey(int16(kmsg.Fetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if req.(*kmsg.FetchRequest).MaxWaitMillis == 0 {
			return nil, nil, false
		}
		return nil, errors.New("held"), true
	})
	got = readEvents(t, s, source.ConfiguredStream{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(5, ""), CompleteAt: []byte(end)}}})
	if r := rowsIn(got["p0"]); len(r) > 0 || !lastIs(got["p0"], "complete "+end) {
		t.Errorf("with the read's fetches held, a read from 5 to %s read %v, and ended with %v", end, r, got["p0"])
	}
}

// A topic deleted and created again, whose partitions end before the
// offsets a read resumes from, has lost the read's position: the read and
// the splits fail with POSITION_LOST rather than skip the new topic's
// records up to the old offsets. A partition that ends at its offset still
// completes there.
func TestASourceRefusesAPositionPastThePartitionsEnd(t *testing.T) {
	addrs := fake(t)
	topic := "events"
	produceRecords(t, addrs, topic, 1, 30, 10)
	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	got := readEvents(t, s, source.ConfiguredStream{Name: topic, State: encodeState(map[int32]int64{0: 30}, "")})
	if !lastIs(got[source.WholeStream], "complete "+string(encodeState(map[int32]int64{0: 30}, ""))) {
		t.Fatalf("a read from the partition's end ended with %v", got[source.WholeStream])
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).DeleteTopic(t.Context(), topic); err != nil {
		t.Fatal(err)
	}
	produceRecords(t, addrs, topic, 1, 5, 5)

	lost := func(what string, err error) {
		t.Helper()
		var coded interface{ ErrorCode() protocolv1.ErrorCode }
		if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
			t.Errorf("%s: %v, want POSITION_LOST", what, err)
		}
	}
	for _, cs := range []source.ConfiguredStream{
		{Name: topic, State: encodeState(map[int32]int64{0: 30}, "")},
		{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(30, "")}}},
	} {
		for _, continuous := range []bool{false, true} {
			out := &splitEvents{events: map[string][]string{}}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			err := s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Pipeline: "p", Lanes: 1, Continuous: continuous}, out)
			cancel()
			lost(fmt.Sprintf("a read (continuous %v) from offset 30 of a partition ending at 5", continuous), err)
			if len(out.events) > 0 {
				t.Errorf("the read emitted %v", out.events)
			}
		}
	}
	_, err = s.Splits(t.Context(), source.ConfiguredStream{Name: topic}, []source.Split{{ID: "p0", State: encodeOffset(30, "")}})
	lost("splits from offset 30 of a partition ending at 5", err)
}

// A read again that stops at a state, as OpenETL's redo of an epoch does,
// ends with that state as it was given, with the topic's ID or, from
// before 0.2.0, without one: OpenETL finds the position it asked for by
// its bytes.
func TestARedoEndsWithTheStateItWasGiven(t *testing.T) {
	addrs := fake(t)
	topic := "events"
	produceRecords(t, addrs, topic, 1, 30, 10)
	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	read := func(cs source.ConfiguredStream) map[string][]byte {
		t.Helper()
		out := &splitEvents{events: map[string][]string{}}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		if err := s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Pipeline: "p", Lanes: 1}, out); err != nil {
			t.Fatal(err)
		}
		return out.states
	}
	id := topicIDOf(read(source.ConfiguredStream{Name: topic})[source.WholeStream])
	if id == "" {
		t.Fatal("a read's state has no topic ID")
	}
	counted := func(o int64, tid string, n int32) []byte {
		b, err := json.Marshal(splitState{Offset: o, TopicID: tid, Partitions: n})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for _, tid := range []string{"", id} {
		from, stop := encodeState(map[int32]int64{0: 2}, tid), encodeState(map[int32]int64{0: 8}, tid)
		fromP0, stopP0 := encodeOffset(2, tid), encodeOffset(8, tid)
		for _, cs := range []source.ConfiguredStream{
			{Name: topic, State: from, Until: stop},
			{Name: topic, State: from, CompleteAt: stop},
			{Name: topic, Splits: []source.Split{{ID: "p0", State: fromP0, Until: stopP0}}},
			{Name: topic, Splits: []source.Split{{ID: "p0", State: fromP0, CompleteAt: stopP0}}},
			// Partition 0's count of the partitions with splits, which a
			// continuous read raises, is the stop's.
			{Name: topic, Splits: []source.Split{{ID: "p0", State: counted(2, tid, 1), Until: counted(8, tid, 3)}}},
			{Name: topic, Splits: []source.Split{{ID: "p0", State: counted(2, tid, 1), CompleteAt: counted(8, tid, 3)}}},
			{Name: topic, Splits: []source.Split{{ID: "p0", State: counted(2, tid, 1), Until: stopP0}}},
		} {
			split, want := source.WholeStream, stop
			if cs.Splits != nil {
				split, want = "p0", stopOf(cs.Splits[0].Until, cs.Splits[0].CompleteAt)
			}
			if got := read(cs)[split]; string(got) != string(want) {
				t.Errorf("a read to %s ended with %s", want, got)
			}
		}
	}
}

// A topic deleted and created again is another topic, even once it has
// grown past the read's offsets: a state that names the old topic's ID has
// lost its position. A state without an ID, from before 0.2.0, reads on
// and takes the topic's ID.
func TestASourceRefusesATopicCreatedAgain(t *testing.T) {
	addrs := fake(t)
	topic := "events"
	produceRecords(t, addrs, topic, 1, 10, 10)
	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	read := func(cs source.ConfiguredStream, continuous bool) (*splitEvents, error) {
		out := &splitEvents{events: map[string][]string{}}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		err := s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Pipeline: "p", Lanes: 1, Continuous: continuous}, out)
		return out, err
	}
	first, err := read(source.ConfiguredStream{Name: topic}, false)
	if err != nil {
		t.Fatal(err)
	}
	whole := first.states[source.WholeStream]
	old := topicIDOf(whole)
	if old == "" {
		t.Fatalf("the state %s has no topic ID", whole)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).DeleteTopic(t.Context(), topic); err != nil {
		t.Fatal(err)
	}
	produceRecords(t, addrs, topic, 1, 15, 15)

	for _, cs := range []source.ConfiguredStream{
		{Name: topic, State: whole},
		{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(10, old)}}},
	} {
		for _, continuous := range []bool{false, true} {
			out, err := read(cs, continuous)
			if codeOf(err) != "ERROR_CODE_POSITION_LOST" || len(out.events) > 0 {
				t.Errorf("a read (continuous %v) of a topic created again: %v, emitting %v", continuous, err, out.events)
			}
		}
	}
	_, err = s.Splits(t.Context(), source.ConfiguredStream{Name: topic}, []source.Split{{ID: "p0", State: encodeOffset(10, old)}})
	if codeOf(err) != "ERROR_CODE_POSITION_LOST" {
		t.Errorf("splits of a topic created again: %v", err)
	}

	out, err := read(source.ConfiguredStream{Name: topic, State: encodeState(map[int32]int64{0: 10}, "")}, false)
	if err != nil || len(rowsIn(out.events[source.WholeStream])) != 5 {
		t.Fatalf("a state without an ID: %v, emitting %v", err, out.events)
	}
	if id := topicIDOf(out.states[source.WholeStream]); id == "" || id == old {
		t.Errorf("a state without an ID took ID %q, the old topic's %q", id, old)
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

// shortWaits makes a bounded read count a partition idle, and a stopped one
// stalled, sooner.
func shortWaits(t *testing.T, idle, stall time.Duration) {
	idleAfter, stallAfter = idle, stall
	t.Cleanup(func() { idleAfter, stallAfter = 10*time.Second, 2*time.Minute })
}

// A partition short of where its read stops, whose broker doesn't answer its
// fetches, doesn't complete at the stop, which would skip the records below
// it: the read fails, to be tried again. One whose broker answers again
// reads them, and completes at the stop.
func TestAStopIsReachedOnlyWhenTheBrokerShowsIt(t *testing.T) {
	shortWaits(t, 100*time.Millisecond, time.Second)
	c := fakeCluster(t)
	addrs := c.ListenAddrs()
	topic := "held"
	produceRecords(t, addrs, topic, 1, 30, 10)
	var held atomic.Bool
	held.Store(true)
	fetches := atomic.Int64{}
	c.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !held.Load() {
			return nil, nil, false
		}
		fetches.Add(1)
		return nil, errors.New("held"), true
	})
	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	for _, sp := range []source.Split{
		{ID: "p0", State: encodeOffset(0, ""), Until: encodeOffset(30, "")},
		{ID: "p0", State: encodeOffset(0, ""), CompleteAt: encodeOffset(30, "")},
	} {
		out := &splitEvents{events: map[string][]string{}}
		err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: topic, Splits: []source.Split{sp}}}, Pipeline: "p", Lanes: 1}, out)
		var coded interface{ ErrorCode() protocolv1.ErrorCode }
		if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
			t.Errorf("with the broker not answering, the read ended with %v and %v", err, out.events["p0"])
		}
		if slices.ContainsFunc(out.events["p0"], func(e string) bool { return strings.HasPrefix(e, "complete") }) {
			t.Errorf("with the broker not answering, the split completed: %v", out.events["p0"])
		}
	}
	if fetches.Load() == 0 {
		t.Fatal("no fetch was held")
	}

	shortWaits(t, 100*time.Millisecond, 10*time.Second)
	time.AfterFunc(time.Second, func() { held.Store(false) })
	got := readEvents(t, s, source.ConfiguredStream{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(0, ""), CompleteAt: encodeOffset(30, "")}}})
	if r := rowsIn(got["p0"]); !slices.Equal(r, rowsIn(at(0, 0, 30))) || !lastIs(got["p0"], "complete "+string(encodeOffset(30, ""))) {
		t.Errorf("once the broker answered, the split read %v, and ended with %v", r, got["p0"])
	}
}

// shortChecks makes a continuous read look for new partitions every d.
func shortChecks(t *testing.T, d time.Duration) {
	every := partitionsEvery
	partitionsEvery = d
	t.Cleanup(func() { partitionsEvery = every })
}

// waitFor fails t unless ok holds within 20 seconds.
func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); !ok(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s", what)
		}
	}
}

// follow reads splits of topic on continuous reads, one each, as an
// attempt's workers do. during runs once every read has rows, and the
// reads end once ready says so of their rows, and they have looked for new
// partitions a few times more.
func follow(t *testing.T, s *Source, topic string, splits [][]source.Split, ready func(rows func(id string) int) bool, during func(outs []*splitEvents)) []*splitEvents {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	outs := make([]*splitEvents, len(splits))
	done := make(chan error, len(splits))
	for i, ss := range splits {
		outs[i] = &splitEvents{events: map[string][]string{}}
		go func() {
			done <- s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: topic, Splits: ss}}, Pipeline: "p", Lanes: 1, Continuous: true}, outs[i])
		}()
	}
	rows := func(id string) int {
		n := 0
		for _, out := range outs {
			out.mu.Lock()
			n += len(rowsIn(out.events[id]))
			out.mu.Unlock()
		}
		return n
	}
	waitFor(t, "the partitions' first records", func() bool { return rows("p0") > 0 && rows("p1") > 0 })
	during(outs)
	waitFor(t, "the new partition's records", func() bool { return ready(rows) })
	time.Sleep(5 * partitionsEvery)
	cancel()
	for range splits {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("a read ended with %v", err)
		}
	}
	return outs
}

// growTopic adds n partitions to topic, and 4 records to each of them.
func growTopic(t *testing.T, addrs []string, topic string, n int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	tds, err := kadm.NewClient(cl).ListTopics(t.Context(), topic)
	if err != nil {
		t.Fatal(err)
	}
	had := len(tds[topic].Partitions)
	if _, err := kadm.NewClient(cl).CreatePartitions(t.Context(), n, topic); err != nil {
		t.Fatal(err)
	}
	for p := had; p < had+n; p++ {
		produceMore(t, addrs, topic, int32(p), 4)
	}
}

// rowsOf are partition p's rows that any of outs read, sorted.
func rowsOf(outs []*splitEvents, p int32) []string {
	var got []string
	for _, out := range outs {
		got = append(got, rowsIn(out.events[splitID(p)])...)
	}
	slices.Sort(got)
	return got
}

// The splits of a stream's read can be spread over several reads, each of
// which has only its own: a partition that has no split, whether added to
// the topic during the reads or before they began, joins exactly one of
// them, the read of partition 0's split, and no read takes another's
// partitions, so every record is read once.
func TestAPartitionWithoutASplitJoinsOneRead(t *testing.T) {
	shortChecks(t, 100*time.Millisecond)
	for _, during := range []bool{true, false} {
		t.Run(fmt.Sprintf("added during the reads %v", during), func(t *testing.T) {
			addrs := fake(t)
			topic := "events"
			produceRecords(t, addrs, topic, 3, 5, 5)
			s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
			if err != nil {
				t.Fatal(err)
			}
			splits, err := s.Splits(t.Context(), source.ConfiguredStream{Name: topic}, nil)
			if err != nil || len(splits) != 3 {
				t.Fatalf("splits %v, %v", splits, err)
			}
			add := func([]*splitEvents) { growTopic(t, addrs, topic, 1) }
			if !during {
				add(nil)
				add = func([]*splitEvents) {}
			}
			outs := follow(t, s, topic, [][]source.Split{{splits[0], splits[2]}, {splits[1]}}, func(rows func(string) int) bool { return rows("p3") >= 4 }, add)
			if a, b := outs[0].added, outs[1].added; !slices.Equal(a, []string{"p3"}) || len(b) > 0 {
				t.Errorf("the read of p0 and p2 added %v, the read of p1 %v", a, b)
			}
			for p, n := range map[int32]int64{0: 5, 1: 5, 2: 5, 3: 4} {
				if got := rowsOf(outs, p); !slices.Equal(got, rowsIn(at(p, 0, n))) {
					t.Errorf("partition %d's rows: %v", p, got)
				}
			}
			if n := partitionsOf(outs[0].states["p0"]); n != 4 {
				t.Errorf("partition 0's split counts %d partitions, of 4", n)
			}
		})
	}
}

// Partition 0's split, as 0.1.0 wrote it, holds no count of the partitions
// with splits, so its read takes every partition it wasn't given, as
// 0.1.0's did, and holds their count from then on.
func TestAReadFromSplitsBefore020TakesThePartitionsItWasntGiven(t *testing.T) {
	shortChecks(t, 100*time.Millisecond)
	addrs := fake(t)
	topic := "events"
	produceRecords(t, addrs, topic, 2, 5, 5)
	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	old := []source.Split{{ID: "p0", State: []byte(`{"offset":0}`)}, {ID: "p1", State: []byte(`{"offset":0}`)}}
	counted := func(outs []*splitEvents, n int32) func() bool {
		return func() bool {
			outs[0].mu.Lock()
			defer outs[0].mu.Unlock()
			return partitionsOf(outs[0].states["p0"]) == n
		}
	}
	outs := follow(t, s, topic, [][]source.Split{old}, func(rows func(string) int) bool { return rows("p2") >= 4 }, func(outs []*splitEvents) {
		waitFor(t, "a count of the partitions read", counted(outs, 2))
		growTopic(t, addrs, topic, 1)
	})
	if !slices.Equal(outs[0].added, []string{"p2"}) {
		t.Errorf("the read added %v", outs[0].added)
	}
	for p, n := range map[int32]int64{0: 5, 1: 5, 2: 4} {
		if got := rowsOf(outs, p); !slices.Equal(got, rowsIn(at(p, 0, n))) {
			t.Errorf("partition %d's rows: %v", p, got)
		}
	}
	if !counted(outs, 3)() {
		t.Errorf("partition 0's split ended at %s", outs[0].states["p0"])
	}
}

// A topic read whole with no state, from the latest offsets, starts where
// the topic ended when the read began, which a read again of its first
// epoch, as OpenETL's redo is, has no state to say: its states hold it, so
// that read starts where the first did, though the topic has moved on, and
// returns the same rows up to the same end.
func TestARedoOfAReadFromLatestStartsWhereItDid(t *testing.T) {
	addrs := fake(t)
	topic := "events"
	produceRecords(t, addrs, topic, 2, 5, 5)
	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}, "startOffset": "latest"}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := &splitEvents{events: map[string][]string{}}
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: topic}}, Pipeline: "p", Lanes: 1, Continuous: true}, first)
	}()
	rows := func() []string {
		first.mu.Lock()
		defer first.mu.Unlock()
		return rowsIn(first.events[source.WholeStream])
	}
	// The first read began at the end it found, so it leaves out the
	// records added before then, and reads those added after.
	starts := map[int32]int64{}
	waitFor(t, "the first read's rows", func() bool {
		produceMore(t, addrs, topic, 0, 1)
		produceMore(t, addrs, topic, 1, 1)
		time.Sleep(100 * time.Millisecond)
		for _, r := range rows() {
			var p int32
			var o int64
			if _, err := fmt.Sscanf(r, "p%d@%d", &p, &o); err != nil {
				t.Fatal(err)
			}
			if s, ok := starts[p]; !ok || o < s {
				starts[p] = o
			}
		}
		return len(starts) == 2
	})
	n := len(rows())
	time.Sleep(2 * time.Second)
	if len(rows()) != n {
		t.Fatalf("the first read went on from %d rows to %d", n, len(rows()))
	}
	cancel()
	<-done
	want, end := rows(), first.states[source.WholeStream]
	produceMore(t, addrs, topic, 0, 3)
	produceMore(t, addrs, topic, 1, 3)

	for _, cs := range []source.ConfiguredStream{
		{Name: topic, Until: end},
		{Name: topic, CompleteAt: end},
		// A state as 0.1.0 wrote it, which held no start: the first
		// read's, given as where the read again starts.
		{Name: topic, State: fmt.Appendf(nil, `{"offsets":{"0":%d,"1":%d}}`, starts[0], starts[1]), Until: end},
	} {
		got := readEvents(t, s, cs)[source.WholeStream]
		if r := rowsIn(got); !slices.Equal(r, want) {
			t.Errorf("a read to %s read %v, and the first read %v", end, r, want)
		}
		if !lastIs(got, "complete "+topicIDField.ReplaceAllString(string(end), "")) {
			t.Errorf("a read to %s ended with %v", end, got)
		}
	}
}

// A bounded read that doesn't learn where a partition ends, because the
// brokers answer for it with an error or not at all, fails as a transient
// failure that OpenETL tries again, rather than complete the partition
// unread.
func TestAPartitionWhoseEndIsUnknownFailsTheRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		// The answer for partition 1, false for none.
		answer func(*kmsg.ListOffsetsResponseTopicPartition) bool
	}{
		{"with an error", func(p *kmsg.ListOffsetsResponseTopicPartition) bool {
			p.ErrorCode = kerr.OffsetNotAvailable.Code
			return true
		}},
		{"with no answer", func(*kmsg.ListOffsetsResponseTopicPartition) bool { return false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fakeCluster(t)
			addrs := c.ListenAddrs()
			topic := "events"
			produceRecords(t, addrs, topic, 2, 5, 5)
			// Where the read starts is listed as usual; where it ends, read
			// committed, isn't for partition 1.
			c.ControlKey(int16(kmsg.ListOffsets), func(req kmsg.Request) (kmsg.Response, error, bool) {
				c.KeepControl()
				r := req.(*kmsg.ListOffsetsRequest)
				if r.IsolationLevel != 1 {
					return nil, nil, false
				}
				resp := r.ResponseKind().(*kmsg.ListOffsetsResponse)
				for _, rt := range r.Topics {
					t := kmsg.NewListOffsetsResponseTopic()
					t.Topic = rt.Topic
					for _, rp := range rt.Partitions {
						p := kmsg.NewListOffsetsResponseTopicPartition()
						p.Partition, p.Offset, p.Timestamp = rp.Partition, 5, -1
						if rp.Partition != 1 || tc.answer(&p) {
							t.Partitions = append(t.Partitions, p)
						}
					}
					resp.Topics = append(resp.Topics, t)
				}
				return resp, nil, true
			})
			s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
			if err != nil {
				t.Fatal(err)
			}
			for _, cs := range []source.ConfiguredStream{
				{Name: topic},
				{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(0, "")}, {ID: "p1", State: encodeOffset(0, "")}}},
			} {
				out := &splitEvents{events: map[string][]string{}}
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				err := s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Pipeline: "p", Lanes: 1}, out)
				cancel()
				if codeOf(err) != "ERROR_CODE_TRANSIENT" {
					t.Errorf("a read (in splits %v) whose partition 1's end is unknown: %v", cs.Splits != nil, err)
				}
				for id, ev := range out.events {
					if slices.ContainsFunc(ev, func(e string) bool { return strings.HasPrefix(e, "complete") }) {
						t.Errorf("split %q completed: %v", id, ev)
					}
				}
			}
		})
	}
}
