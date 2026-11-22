package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// fake starts an in-process broker with transactions, and returns its
// address.
func fake(t *testing.T) []string {
	t.Helper()
	return fakeCluster(t).ListenAddrs()
}

func fakeCluster(t *testing.T) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.DefaultNumPartitions(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func fakeConfig(t *testing.T, addrs []string, extra map[string]any) []byte {
	t.Helper()
	m := map[string]any{"brokers": addrs}
	for k, v := range extra {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// lanes is a pipeline's destination on a fake broker, writing table.
type lanes struct {
	t        *testing.T
	addrs    []string
	d        *Destination
	pipeline string
	table    string
}

func newLanes(t *testing.T) *lanes {
	t.Helper()
	addrs := fake(t)
	d, err := NewDestination(fakeConfig(t, addrs, nil))
	if err != nil {
		t.Fatal(err)
	}
	return &lanes{t: t, addrs: addrs, d: d, pipeline: "6f1c2b9e-0d4a-4c51-9a37-2f1e8b7d3c10", table: "items"}
}

// open opens writer lane lane of generation gen, which an attempt closes
// on its own.
func (l *lanes) open(gen uint64, lane int) *writer {
	l.t.Helper()
	cfg := destination.OpenConfig{
		Pipeline: l.pipeline, Generation: gen, Scope: destination.ScopePipeline, CommitInterval: time.Second, Lane: lane,
		Streams: []destination.Stream{{Name: l.table, Mode: destination.Append, PrimaryKey: []string{"id"}}},
	}
	w, err := l.d.Open(l.t.Context(), cfg)
	if err != nil {
		l.t.Fatal(err)
	}
	l.t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w.(*writer)
}

// stage writes ids from..to-1 into w's part of e and pre-commits it.
func (l *lanes) stage(w *writer, e destination.Epoch, from, to int) destination.Staged {
	l.t.Helper()
	b := idRows(l.table, from, to)
	defer b.Release()
	if err := w.Write(l.t.Context(), e, l.table, b); err != nil {
		l.t.Fatal(err)
	}
	st, err := w.PreCommit(l.t.Context(), e)
	if err != nil {
		l.t.Fatal(err)
	}
	return st
}

func (l *lanes) commit(w *writer, e destination.Epoch, st destination.Staged) {
	l.t.Helper()
	if _, err := w.Commit(l.t.Context(), e, []destination.Staged{st}); err != nil {
		l.t.Fatalf("lane %d's commit of epoch %d: %v", w.cfg.Lane, e.N, err)
	}
}

func (l *lanes) recover(w *writer, gen uint64, e destination.Epoch, st destination.Staged) destination.Recovery {
	l.t.Helper()
	r, err := w.Recover(l.t.Context(), gen, e, []destination.Staged{st})
	if err != nil {
		l.t.Fatalf("lane %d's recovery of epoch %d: %v", w.cfg.Lane, e.N, err)
	}
	return r
}

// want checks that the table holds ids from..to-1 once each, committed.
func (l *lanes) want(when string, from, to int) {
	l.t.Helper()
	got := l.committed()
	bad := len(got) != to-from
	for i := from; i < to && !bad; i++ {
		bad = got[int64(i)] != 1
	}
	if bad {
		l.t.Errorf("%s: ids %v, want %d to %d once each", when, got, from, to-1)
	}
}

// committed counts the table's committed records by id.
func (l *lanes) committed() map[int64]int {
	l.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(l.addrs...))
	if err != nil {
		l.t.Fatal(err)
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListCommittedOffsets(l.t.Context(), l.table)
	if err != nil {
		l.t.Fatal(err)
	}
	end, ok := ends.Lookup(l.table, 0)
	if !ok || end.Err != nil || end.Offset == 0 {
		return map[int64]int{}
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(l.addrs...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{l.table: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())
	if err != nil {
		l.t.Fatal(err)
	}
	defer consumer.Close()
	ctx, cancel := context.WithTimeout(l.t.Context(), 10*time.Second)
	defer cancel()
	out := map[int64]int{}
	for done := false; !done; {
		fs := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			l.t.Fatalf("reading %s to %d: %v", l.table, end.Offset, ctx.Err())
		}
		for _, r := range fs.Records() {
			done = done || r.Offset+1 >= end.Offset
			if r.Attrs.IsControl() {
				continue
			}
			var v struct {
				ID int64 `json:"id"`
			}
			if err := json.Unmarshal(r.Value, &v); err != nil {
				l.t.Fatalf("value %q: %v", r.Value, err)
			}
			out[v.ID]++
		}
	}
	return out
}

// idRows is a batch of ids from..to-1 for table, with the metadata columns
// the engine adds.
func idRows(table string, from, to int) arrow.RecordBatch {
	md := arrow.NewMetadata([]string{records.TableKey}, []string{table})
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: opColumn, Type: arrow.BinaryTypes.String},
	}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	for i := from; i < to; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append("insert")
	}
	return b.NewRecordBatch()
}

func epochOf(gen, n uint64) destination.Epoch { return destination.Epoch{Generation: gen, N: n} }

func TestTheDestinationTakesLaneCommits(t *testing.T) {
	c := DestinationCapabilities
	if !c.LaneCommits || c.Lanes() < 2 {
		t.Errorf("capabilities %+v", c)
	}
}

// Each lane's producers have transactional IDs of their own, numbered by
// lane and slot, the same in every generation. Lane 0's are those of the
// destination's one writer lane before lane commits.
func TestEachLaneHasItsOwnTransactionalIDs(t *testing.T) {
	l := newLanes(t)
	var ids []string
	for gen, lane := range []int{0, 1, 2, 1} {
		w := l.open(uint64(gen+1), lane)
		for _, s := range w.slots {
			ids = append(ids, s.id)
		}
	}
	p := "openetl-" + l.pipeline + "-"
	var want []string
	for _, n := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 3, 4, 5} {
		want = append(want, p+strconv.Itoa(n))
	}
	if !slices.Equal(ids, want) {
		t.Errorf("transactional IDs %v, want %v", ids, want)
	}
}

// Two lanes write an epoch at once and commit it on their own: each lane's
// rows show once its commit is done, and a commit repeated changes nothing.
func TestLanesCommitAnEpochEachOnItsOwn(t *testing.T) {
	l := newLanes(t)
	e1 := epochOf(1, 1)
	w0, w1 := l.open(1, 0), l.open(1, 1)
	for _, w := range []*writer{w0, w1} {
		if r, err := w.Recover(t.Context(), 1, destination.Epoch{}, nil); err != nil || r != destination.Applied {
			t.Fatalf("Recover = %v, %v", r, err)
		}
	}
	// Lane 1's records come first in the topic, so read_committed sees them
	// while lane 0's transaction is open.
	st1, st0 := l.stage(w1, e1, 10, 25), l.stage(w0, e1, 0, 10)
	l.want("before the commits", 0, 0)
	l.commit(w1, e1, st1)
	l.want("after lane 1's commit", 10, 25)
	l.commit(w0, e1, st0)
	l.want("after both lanes' commits", 0, 25)
	for range 2 {
		l.commit(w0, e1, st0)
		l.commit(w1, e1, st1)
	}
	l.want("after the commits again", 0, 25)
	e2 := epochOf(1, 2)
	st20, st21 := l.stage(w0, e2, 25, 30), l.stage(w1, e2, 30, 40)
	l.commit(w0, e2, st20)
	l.commit(w1, e2, st21)
	l.want("after a second epoch", 0, 40)
	// And on writers that didn't stage them.
	l.commit(l.open(1, 0), e1, st0)
	l.commit(l.open(1, 1), e2, st21)
	l.want("after commits on other writers", 0, 40)
}

// A lane's writer of a newer generation fences that lane's older writer,
// whose epoch it finds lost, and no other lane's: a lane staged and not
// committed while another settles still commits.
func TestALanesNewWriterFencesOnlyItsOwnLane(t *testing.T) {
	l := newLanes(t)
	e1 := epochOf(1, 1)
	old0, old1 := l.open(1, 0), l.open(1, 1)
	st0, st1 := l.stage(old0, e1, 0, 10), l.stage(old1, e1, 10, 20)

	new0 := l.open(2, 0)
	if r := l.recover(new0, 2, e1, st0); r != destination.Lost {
		t.Errorf("lane 0 of epoch 1, whose writer was fenced before its commit, recovered %s", r)
	}
	if _, err := old0.Commit(t.Context(), e1, []destination.Staged{st0}); !errors.Is(err, destination.ErrFenced) {
		t.Errorf("the fenced lane 0's commit: %v", err)
	}
	l.commit(old1, e1, st1)
	l.want("after lane 1's commit, behind lane 0's new writer", 10, 20)

	new1 := l.open(2, 1)
	if r := l.recover(new1, 2, e1, st1); r != destination.Applied {
		t.Errorf("lane 1 of epoch 1, committed, recovered %s", r)
	}
	// Its later epoch on the old lane 1 is fenced now.
	e2 := epochOf(1, 2)
	b := idRows(l.table, 100, 105)
	defer b.Release()
	if err := old1.Write(t.Context(), e2, l.table, b); err == nil {
		if st, err := old1.PreCommit(t.Context(), e2); err == nil {
			if _, err := old1.Commit(t.Context(), e2, []destination.Staged{st}); !errors.Is(err, destination.ErrFenced) {
				t.Errorf("the fenced lane 1's later commit: %v", err)
			}
		}
	}
	l.want("after the fenced lane wrote on", 10, 20)
}

// A lane settled lost is written again on the writer that settled it, as
// the epoch's, and committed. Settling it again from the record, which
// still names the first transaction, finds the redo's marker; a redo that
// wasn't committed is lost again, and written once more.
func TestALostLaneIsWrittenAgain(t *testing.T) {
	l := newLanes(t)
	e1 := epochOf(1, 1)
	a0, a1 := l.open(1, 0), l.open(1, 1)
	st0, st1 := l.stage(a0, e1, 0, 10), l.stage(a1, e1, 10, 20)
	l.commit(a1, e1, st1)

	// Generation 2 settles lane 0, and its redo dies before its commit.
	b0 := l.open(2, 0)
	if r := l.recover(b0, 2, e1, st0); r != destination.Lost {
		t.Fatalf("lane 0 recovered %s", r)
	}
	l.stage(b0, e1, 0, 10)
	l.want("after a redo pre-committed", 10, 20)

	// Generation 3 settles it again, and commits its redo.
	c0 := l.open(3, 0)
	if r := l.recover(c0, 3, e1, st0); r != destination.Lost {
		t.Fatalf("lane 0, whose redo wasn't committed, recovered %s", r)
	}
	again := l.stage(c0, e1, 0, 10)
	l.commit(c0, e1, again)
	l.commit(c0, e1, again)
	l.want("after the redo's commit", 0, 20)

	// Generation 4 settles both lanes from the record.
	if r := l.recover(l.open(4, 0), 4, e1, st0); r != destination.Applied {
		t.Errorf("lane 0, redone, recovered %s", r)
	}
	if r := l.recover(l.open(4, 1), 4, e1, st1); r != destination.Applied {
		t.Errorf("lane 1 recovered %s", r)
	}
	l.commit(l.open(4, 0), e1, st0)
	l.want("after settling again", 0, 20)

	// The lanes go on with the next epoch.
	e2 := epochOf(4, 2)
	d0, d1 := l.open(5, 0), l.open(5, 1)
	st0, st1 = l.stage(d0, e2, 20, 30), l.stage(d1, e2, 30, 35)
	l.commit(d0, e2, st0)
	l.commit(d1, e2, st1)
	l.want("after the next epoch", 0, 35)
}

// A lane settles without waiting for another lane's transaction, open
// with a marker ahead of the lane's in the marker topic's partition, which
// holds back what a read_committed read of the partition gets.
func TestSettlingIsNotHeldUpByAnotherLanesTransaction(t *testing.T) {
	l := newLanes(t)
	e1, e2 := epochOf(1, 1), epochOf(1, 2)
	a0, a1 := l.open(1, 0), l.open(1, 1)
	l.stage(a1, e1, 10, 20)
	st0 := l.stage(a0, e1, 0, 10)
	l.stage(a1, e2, 20, 30)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	b0 := l.open(2, 0)
	r, err := b0.Recover(ctx, 2, e1, []destination.Staged{st0})
	if err != nil || r != destination.Lost {
		t.Fatalf("lane 0 recovered %v, %v, with lane 1's transactions open", r, err)
	}
}

// A writer settles and commits only its own lane's committable.
func TestALaneTakesOnlyItsOwnCommittable(t *testing.T) {
	l := newLanes(t)
	e1 := epochOf(1, 1)
	st0 := l.stage(l.open(1, 0), e1, 0, 5)
	w1 := l.open(2, 1)
	if _, err := w1.Recover(t.Context(), 2, e1, []destination.Staged{st0}); err == nil {
		t.Error("lane 1 settled lane 0's committable")
	}
	if _, err := w1.Commit(t.Context(), e1, []destination.Staged{st0}); err == nil {
		t.Error("lane 1 committed lane 0's committable")
	}
	if _, err := l.d.Open(t.Context(), destination.OpenConfig{Pipeline: l.pipeline, Scope: destination.ScopePipeline, Lane: DestinationCapabilities.WriterLanes}); err == nil {
		t.Error("opened a lane past the most")
	}
}

// splitEvents is what a read emitted of each split, by split: "pP@O" for
// the row of partition P's offset O, "resume S" and "complete S".
type splitEvents struct {
	mu     sync.Mutex
	events map[string][]string
	// Each split's last state; events show states without their topic
	// IDs, which a fake cluster makes up.
	states map[string][]byte
	// The IDs of the splits the read added, in order.
	added []string
}

func (c *splitEvents) add(id string, events ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events[id] = append(c.events[id], events...)
}

var topicIDField = regexp.MustCompile(`,"topicId":"[^"]*"`)

func (c *splitEvents) state(id, event string, state []byte) {
	c.mu.Lock()
	if c.states == nil {
		c.states = map[string][]byte{}
	}
	c.states[id] = state
	c.mu.Unlock()
	c.add(id, event+" "+topicIDField.ReplaceAllString(string(state), ""))
}

func (c *splitEvents) Rows(stream string, rows []json.RawMessage) error {
	return c.Split(stream, source.WholeStream).Rows(rows)
}
func (c *splitEvents) Records(stream string, b arrow.RecordBatch) error {
	return c.Split(stream, source.WholeStream).Records(b)
}
func (c *splitEvents) ResumePoint(stream string, state []byte) error {
	return c.Split(stream, source.WholeStream).ResumePoint(state)
}
func (c *splitEvents) Complete(stream string, next []byte) error {
	return c.Split(stream, source.WholeStream).Complete(next)
}
func (c *splitEvents) Split(_, id string) source.SplitEmitter { return splitLog{c, id} }

type splitLog struct {
	c  *splitEvents
	id string
}

func (s splitLog) Rows(rows []json.RawMessage) error {
	for _, r := range rows {
		var v struct {
			P int32 `json:"_kafka_partition"`
			O int64 `json:"_kafka_offset"`
		}
		if err := json.Unmarshal(r, &v); err != nil {
			return err
		}
		s.c.add(s.id, fmt.Sprintf("p%d@%d", v.P, v.O))
	}
	return nil
}
func (s splitLog) Records(arrow.RecordBatch) error { return errors.New("records") }
func (s splitLog) ResumePoint(state []byte, added ...source.Split) error {
	s.c.state(s.id, "resume", state)
	s.c.mu.Lock()
	for _, a := range added {
		s.c.added = append(s.c.added, a.ID)
	}
	s.c.mu.Unlock()
	return nil
}
func (s splitLog) Complete(next []byte, _ ...source.Split) error {
	s.c.state(s.id, "complete", next)
	return nil
}

// produceRecords writes n records to each of topic's partitions, in
// batches of batch.
func produceRecords(t *testing.T, addrs []string, topic string, partitions int32, n, batch int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopic(t.Context(), partitions, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	for p := range partitions {
		for i := 0; i < n; i += batch {
			var recs []*kgo.Record
			for j := i; j < min(i+batch, n); j++ {
				recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Value: fmt.Appendf(nil, `{"n":%d}`, j)})
			}
			if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// at is a split's events for offsets from..to-1 of partition p.
func at(p int32, from, to int64) []string {
	var out []string
	for o := from; o < to; o++ {
		out = append(out, fmt.Sprintf("p%d@%d", p, o))
	}
	return out
}

// A read that says where each split ends, or where the whole stream does,
// reads up to there and no further, whatever it fetched, then gives a
// resume point there and completes, so a read again ends where the first
// read's resume point was.
func TestASourceReadEndsWhereItIsAsked(t *testing.T) {
	addrs := fake(t)
	topic := "events"
	produceRecords(t, addrs, topic, 2, 30, 10)
	s, err := NewSource(fakeConfig(t, addrs, map[string]any{"topics": []string{topic}}))
	if err != nil {
		t.Fatal(err)
	}
	read := func(cs source.ConfiguredStream) map[string][]string {
		t.Helper()
		out := &splitEvents{events: map[string][]string{}}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		if err := s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Pipeline: "p", Lanes: 1}, out); err != nil {
			t.Fatal(err)
		}
		return out.events
	}
	collapse := func(events []string) []string {
		var out []string
		for _, e := range events {
			if len(out) == 0 || e != out[len(out)-1] {
				out = append(out, e)
			}
		}
		return out
	}

	got := read(source.ConfiguredStream{Name: topic, Splits: []source.Split{
		{ID: "p0", State: encodeOffset(5, ""), Until: encodeOffset(17, "")},
		{ID: "p1", State: encodeOffset(0, ""), Until: encodeOffset(10, "")},
	}})
	var rows0, rows1 []string
	for _, e := range got["p0"] {
		if e[0] == 'p' {
			rows0 = append(rows0, e)
		}
	}
	for _, e := range got["p1"] {
		if e[0] == 'p' {
			rows1 = append(rows1, e)
		}
	}
	if !slices.Equal(rows0, at(0, 5, 17)) || !slices.Equal(rows1, at(1, 0, 10)) {
		t.Errorf("splits read %v and %v", rows0, rows1)
	}
	for id, end := range map[string]int64{"p0": 17, "p1": 10} {
		ev := collapse(got[id])
		if n := len(ev); n < 2 || ev[n-2] != "resume "+string(encodeOffset(end, "")) || ev[n-1] != "complete "+string(encodeOffset(end, "")) {
			t.Errorf("split %s ended with %v", id, ev)
		}
	}

	from, until := encodeState(map[int32]int64{0: 2, 1: 3}, ""), encodeState(map[int32]int64{0: 8, 1: 5}, "")
	got = read(source.ConfiguredStream{Name: topic, State: from, Until: until})
	ev := got[source.WholeStream]
	var rows []string
	for _, e := range ev {
		if e[0] == 'p' {
			rows = append(rows, e)
		}
	}
	slices.Sort(rows)
	want := append(at(0, 2, 8), at(1, 3, 5)...)
	slices.Sort(want)
	if !slices.Equal(rows, want) {
		t.Errorf("the whole stream read %v", rows)
	}
	if n := len(ev); n < 2 || ev[n-2] != "resume "+string(until) || ev[n-1] != "complete "+string(until) {
		t.Errorf("the whole stream ended with %v", ev)
	}

	// Without an end, the read goes to the topic's, and completes where it
	// ended. Its completion's state, given as an end, ends a read there too.
	got = read(source.ConfiguredStream{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(5, "")}}})
	ev = got["p0"]
	if ev[len(ev)-1] != "complete "+string(encodeOffset(30, "")) {
		t.Errorf("a split without an end ended with %v", ev)
	}
	produceMore(t, addrs, topic, 0, 5)
	got = read(source.ConfiguredStream{Name: topic, Splits: []source.Split{{ID: "p0", State: encodeOffset(5, ""), Until: encodeOffset(30, "")}}})
	rows0 = nil
	for _, e := range got["p0"] {
		if e[0] == 'p' {
			rows0 = append(rows0, e)
		}
	}
	if ev := collapse(got["p0"]); !slices.Equal(rows0, at(0, 5, 30)) || ev[len(ev)-1] != "complete "+string(encodeOffset(30, "")) {
		t.Errorf("a split read to its completion's state read %v, and ended with %v", rows0, ev)
	}
}

// produceMore adds n records to partition p of topic.
func produceMore(t *testing.T, addrs []string, topic string, p int32, n int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var recs []*kgo.Record
	for i := range n {
		recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Value: fmt.Appendf(nil, `{"more":%d}`, i)})
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}
