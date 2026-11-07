//go:build integration

package kafka

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Runs against the compose broker and schema registry (make deps-up), or
// OPENETL_TEST_KAFKA_BROKERS and OPENETL_TEST_SCHEMA_REGISTRY.
func brokers() string { return cmp.Or(os.Getenv("OPENETL_TEST_KAFKA_BROKERS"), "127.0.0.1:9092") }
func registryURL() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_SCHEMA_REGISTRY"), "http://127.0.0.1:8081")
}

func uniq(prefix string) string { return prefix + "_" + strings.ToLower(rand.Text())[:8] }

func config(t *testing.T, extra map[string]any) []byte {
	t.Helper()
	m := map[string]any{"brokers": []string{brokers()}}
	for k, v := range extra {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newDest(t *testing.T, extra map[string]any) *Destination {
	t.Helper()
	d, err := NewDestination(config(t, extra))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Check(t.Context()); err != nil {
		t.Fatalf("check (is the broker running? make deps-up): %v", err)
	}
	return d
}

func openWriter(t *testing.T, d *Destination, pipeline string, gen uint64, streams ...string) destination.Writer {
	t.Helper()
	cfg := destination.OpenConfig{Pipeline: pipeline, Generation: gen, Scope: destination.ScopePipeline, CommitInterval: time.Second}
	for _, s := range streams {
		cfg.Streams = append(cfg.Streams, destination.Stream{Name: s, Mode: destination.Append, PrimaryKey: []string{"id"}})
	}
	w, err := d.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

// rows is a batch of ids from..to-1 for table, with the metadata columns
// the engine adds.
func rows(t *testing.T, table string, from, to int) arrow.RecordBatch {
	t.Helper()
	md := arrow.NewMetadata([]string{records.TableKey}, []string{table})
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: idColumn, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: arrow.NewMetadata([]string{records.ExtensionNameKey}, []string{records.UUIDExtension})},
		{Name: opColumn, Type: arrow.BinaryTypes.String},
	}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	for i := from; i < to; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append("n" + strconv.Itoa(i))
		u := uuid.New()
		b.Field(2).(*array.FixedSizeBinaryBuilder).Append(u[:])
		b.Field(3).(*array.StringBuilder).Append("insert")
	}
	return b.NewRecordBatch()
}

func write(t *testing.T, w destination.Writer, e destination.Epoch, table string, from, to int) {
	t.Helper()
	b := rows(t, table, from, to)
	defer b.Release()
	if err := w.Write(t.Context(), e, table, b); err != nil {
		t.Fatal(err)
	}
}

// committed reads every committed record of topic.
func committed(t *testing.T, topic string) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListCommittedOffsets(t.Context(), topic)
	if err != nil {
		t.Fatal(err)
	}
	starts := map[string]map[int32]kgo.Offset{topic: {}}
	left := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			starts[topic][o.Partition] = kgo.NewOffset().AtStart()
			left[o.Partition] = o.Offset
		}
	})
	if len(left) == 0 {
		return nil
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers()), kgo.ConsumePartitions(starts), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(left) > 0 {
		fs := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("reading %s: %v; partitions left %v", topic, ctx.Err(), left)
		}
		for _, r := range fs.Records() {
			if r.Offset+1 >= left[r.Partition] {
				delete(left, r.Partition)
			}
			if !r.Attrs.IsControl() {
				out = append(out, r)
			}
		}
	}
	return out
}

func ids(t *testing.T, recs []*kgo.Record) map[int64]int {
	t.Helper()
	out := map[int64]int{}
	for _, r := range recs {
		var v struct {
			ID int64 `json:"id"`
		}
		if err := json.Unmarshal(r.Value, &v); err != nil {
			t.Fatalf("value %q: %v", r.Value, err)
		}
		out[v.ID]++
	}
	return out
}

func wantIDs(t *testing.T, got map[int64]int, from, to int) {
	t.Helper()
	if len(got) != to-from {
		t.Errorf("%d distinct ids, want %d", len(got), to-from)
	}
	for i := from; i < to; i++ {
		if got[int64(i)] != 1 {
			t.Errorf("id %d appears %d times", i, got[int64(i)])
			return
		}
	}
}

func TestDestinationCommitsEpochs(t *testing.T) {
	d := newDest(t, nil)
	pipeline, table := uniq("p"), uniq("items")
	w := openWriter(t, d, pipeline, 1, table)
	if r, err := w.Recover(t.Context(), 1, destination.Epoch{}, nil); err != nil || r != destination.Applied {
		t.Fatalf("Recover = %v, %v", r, err)
	}
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e1, table, 0, 50)
	st, err := w.PreCommit(t.Context(), e1)
	if err != nil {
		t.Fatal(err)
	}
	// Rows of the next epoch arrive while the first commits.
	e2 := destination.Epoch{Generation: 1, N: 2}
	write(t, w, e2, table, 50, 80)
	if n := len(committed(t, table)); n != 0 {
		t.Fatalf("%d records visible before the commit", n)
	}
	for range 2 {
		if _, err := w.Commit(t.Context(), e1, []destination.Staged{st}); err != nil {
			t.Fatal(err)
		}
	}
	recs := committed(t, table)
	wantIDs(t, ids(t, recs), 0, 50)
	r := recs[0]
	if len(r.Key) == 0 || !strings.HasPrefix(string(r.Key), `{"id":`) {
		t.Errorf("key %q", r.Key)
	}
	if len(r.Headers) != 1 || r.Headers[0].Key != idHeader || !strings.Contains(string(r.Value), string(r.Headers[0].Value)) {
		t.Errorf("headers %v, value %s", r.Headers, r.Value)
	}
	st2, err := w.PreCommit(t.Context(), e2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), e2, []destination.Staged{st2}); err != nil {
		t.Fatal(err)
	}
	wantIDs(t, ids(t, committed(t, table)), 0, 80)
}

// A replacement's Open fences the old attempt's producers: the epoch the
// old one had pre-committed is aborted and reported lost, and the old
// attempt can't commit it.
func TestDestinationRecoversAndFences(t *testing.T) {
	d := newDest(t, nil)
	pipeline, table := uniq("p"), uniq("items")
	old := openWriter(t, d, pipeline, 1, table)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, old, e1, table, 0, 20)
	st1, err := old.PreCommit(t.Context(), e1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Commit(t.Context(), e1, []destination.Staged{st1}); err != nil {
		t.Fatal(err)
	}
	write(t, old, e2, table, 20, 40)
	st2, err := old.PreCommit(t.Context(), e2)
	if err != nil {
		t.Fatal(err)
	}

	next := openWriter(t, d, pipeline, 2, table)
	if r, err := next.Recover(t.Context(), 2, e2, []destination.Staged{st2}); err != nil || r != destination.Lost {
		t.Errorf("Recover(2) = %v, %v; want lost", r, err)
	}
	if r, err := next.Recover(t.Context(), 2, e1, []destination.Staged{st1}); err != nil || r != destination.Applied {
		t.Errorf("Recover(1) = %v, %v; want applied", r, err)
	}
	if _, err := old.Commit(t.Context(), e2, []destination.Staged{st2}); !errors.Is(err, destination.ErrFenced) {
		t.Errorf("the old attempt's Commit = %v, want fenced", err)
	}
	wantIDs(t, ids(t, committed(t, table)), 0, 20)

	// The replacement goes on from epoch 1.
	e := destination.Epoch{Generation: 2, N: 2}
	write(t, next, e, table, 20, 40)
	st, err := next.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := next.Commit(t.Context(), e, []destination.Staged{st}); err != nil {
		t.Fatal(err)
	}
	if r, err := next.Recover(t.Context(), 2, e, []destination.Staged{st}); err != nil || r != destination.Applied {
		t.Errorf("Recover(g2, 2) = %v, %v; want applied", r, err)
	}
	wantIDs(t, ids(t, committed(t, table)), 0, 40)
}

func TestDestinationRefusesTransactionsTheBrokersCant(t *testing.T) {
	d := newDest(t, nil)
	cfg := destination.OpenConfig{Pipeline: uniq("p"), Generation: 1, Scope: destination.ScopePipeline, CommitInterval: 10 * time.Minute,
		Streams: []destination.Stream{{Name: "s", Mode: destination.Append}}}
	_, err := d.Open(t.Context(), cfg)
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID || !strings.Contains(err.Error(), "transaction.max.timeout.ms") {
		t.Errorf("Open = %v", err)
	}
}

// capture is an Emitter that keeps what a read emits.
type capture struct {
	mu       sync.Mutex
	rows     map[string][]map[string]any
	states   map[string][]byte
	complete map[string][]byte
	progress []source.Progress
}

func newCapture() *capture {
	return &capture{rows: map[string][]map[string]any{}, states: map[string][]byte{}, complete: map[string][]byte{}}
}

func (c *capture) Rows(stream string, rows []json.RawMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range rows {
		var m map[string]any
		if err := json.Unmarshal(r, &m); err != nil {
			return err
		}
		c.rows[stream] = append(c.rows[stream], m)
	}
	return nil
}

func (c *capture) Records(string, arrow.RecordBatch) error { return errors.New("no typed batches") }

func (c *capture) ResumePoint(stream string, state []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.states[stream] = state
	return nil
}

func (c *capture) Split(stream, id string) source.SplitEmitter {
	return source.WholeSplit(c, stream, id)
}

func (c *capture) Complete(stream string, next []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.complete[stream] = next
	return nil
}

func (c *capture) Progress(p source.Progress) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.progress = append(c.progress, p)
	return nil
}

func (c *capture) count(stream string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.rows[stream])
}

func newSource(t *testing.T, extra map[string]any) *Source {
	t.Helper()
	s, err := NewSource(config(t, extra))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func read(t *testing.T, s *Source, topic string, state []byte) *capture {
	t.Helper()
	c := newCapture()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	req := source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: topic, SyncMode: source.Stream, State: state}}}
	if err := s.Read(ctx, req, c); err != nil {
		t.Fatal(err)
	}
	if c.complete[topic] == nil {
		t.Fatalf("%s didn't complete", topic)
	}
	return c
}

// produce writes n JSON records, ids from..from+n-1, outside a transaction.
func produce(t *testing.T, topic string, from, n int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()), kgo.DefaultProduceTopic(topic), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var recs []*kgo.Record
	for i := from; i < from+n; i++ {
		recs = append(recs, &kgo.Record{Key: []byte("k" + strconv.Itoa(i)), Value: []byte(`{"id":` + strconv.Itoa(i) + `,"name":"n"}`),
			Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("v")}}})
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// transaction writes ids in a transaction it commits or aborts.
func transaction(t *testing.T, topic string, from, n int, commit bool) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()), kgo.DefaultProduceTopic(topic), kgo.TransactionalID(uniq("txn")))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for i := from; i < from+n; i++ {
		if err := cl.ProduceSync(t.Context(), &kgo.Record{Value: []byte(`{"id":` + strconv.Itoa(i) + `}`)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := cl.EndTransaction(t.Context(), kgo.TransactionEndTry(commit)); err != nil {
		t.Fatal(err)
	}
}

func createTopic(t *testing.T, topic string, partitions int32) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).CreateTopic(t.Context(), partitions, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
}

func rowIDs(t *testing.T, rows []map[string]any) map[int64]int {
	t.Helper()
	out := map[int64]int{}
	for _, r := range rows {
		id, ok := r["id"].(float64)
		if !ok {
			t.Fatalf("row %v has no id", r)
		}
		out[int64(id)]++
	}
	return out
}

// A bounded read stops at the end it saw when it started, skips aborted
// transactions, and the next resumes exactly after it.
func TestSourceReadsToTheEndAndResumes(t *testing.T) {
	topic := uniq("events")
	createTopic(t, topic, 3)
	produce(t, topic, 0, 100)
	transaction(t, topic, 1000, 5, false)
	transaction(t, topic, 100, 5, true)
	s := newSource(t, map[string]any{"topics": []string{topic}})
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Streams) != 1 || cat.Streams[0].Name != topic || cat.Streams[0].Replay != source.Exact || cat.Streams[0].CursorField != offsetColumn {
		t.Fatalf("catalog %+v", cat)
	}
	c := read(t, s, topic, nil)
	wantRowIDs(t, rowIDs(t, c.rows[topic]), 0, 105)
	r := c.rows[topic][0]
	for _, col := range []string{partitionColumn, offsetColumn, timestampColumn, keyColumn, headersColumn, "name"} {
		if _, ok := r[col]; !ok {
			t.Errorf("row %v lacks %s", r, col)
		}
	}
	if h, ok := r[headersColumn].(map[string]any); !ok || h["h"] != "v" {
		t.Errorf("headers %v", r[headersColumn])
	}

	produce(t, topic, 105, 20)
	again := read(t, s, topic, c.complete[topic])
	wantRowIDs(t, rowIDs(t, again.rows[topic]), 105, 125)
	// Nothing new: the read completes at once with the same offsets.
	idle := read(t, s, topic, again.complete[topic])
	if n := len(idle.rows[topic]); n != 0 {
		t.Errorf("%d rows from a read with nothing new", n)
	}
}

func wantRowIDs(t *testing.T, got map[int64]int, from, to int) {
	t.Helper()
	wantIDs(t, got, from, to)
}

func TestSourceFollowsATopic(t *testing.T) {
	topic := uniq("live")
	createTopic(t, topic, 2)
	produce(t, topic, 0, 10)
	s := newSource(t, map[string]any{"topics": []string{topic}})
	c := newCapture()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Pipeline: "p", Continuous: true, Streams: []source.ConfiguredStream{{Name: topic, SyncMode: source.Stream}}}, c)
	}()
	await(t, "the first records", func() bool { return c.count(topic) == 10 })
	produce(t, topic, 10, 10)
	await(t, "records produced while following", func() bool { return c.count(topic) == 20 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Read = %v", err)
	}
	if c.complete[topic] != nil {
		t.Error("a continuous read completed")
	}
	if err := s.Committed(t.Context(), map[string][]byte{topic: c.states[topic]}, nil); err != nil {
		t.Fatal(err)
	}
	cl, _ := kgo.NewClient(kgo.SeedBrokers(brokers()))
	defer cl.Close()
	offs, err := kadm.NewClient(cl).FetchOffsets(t.Context(), "openetl-p")
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	offs.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic {
			total += o.At
		}
	})
	if total != 20 {
		t.Errorf("the group's offsets add to %d, want 20", total)
	}
}

// A topic read whole takes a new partition into its state, from its start.
func TestSourceFollowsNewPartitions(t *testing.T) {
	topic := uniq("grows_whole")
	createTopic(t, topic, 1)
	produceTo(t, topic, 0, 0, 5)
	s := newSource(t, map[string]any{"topics": []string{topic}})
	c := newCapture()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Pipeline: "p", Continuous: true, Streams: []source.ConfiguredStream{{Name: topic, SyncMode: source.Stream}}}, c)
	}()
	await(t, "the first records", func() bool { return c.count(topic) == 5 })
	addPartitions(t, topic, 1)
	produceTo(t, topic, 1, 5, 5)
	awaitFor(t, "the new partition's records", 2*partitionsEvery, func() bool { return c.count(topic) == 10 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Read = %v", err)
	}
	wantIDs(t, rowIDs(t, c.rows[topic]), 0, 10)
	if next, err := parseState(c.states[topic]); err != nil || !maps.Equal(next, map[int32]int64{0: 5, 1: 5}) {
		t.Errorf("state %s: %v", c.states[topic], err)
	}
}

func await(t *testing.T, what string, ok func() bool) {
	t.Helper()
	awaitFor(t, what, 30*time.Second, ok)
}

func awaitFor(t *testing.T, what string, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSourcePositionLost(t *testing.T) {
	topic := uniq("retained")
	createTopic(t, topic, 1)
	produce(t, topic, 0, 10)
	cl, _ := kgo.NewClient(kgo.SeedBrokers(brokers()))
	defer cl.Close()
	del := kadm.Offsets{}
	del.Add(kadm.Offset{Topic: topic, Partition: 0, At: 6})
	if _, err := kadm.NewClient(cl).DeleteRecords(t.Context(), del); err != nil {
		t.Fatal(err)
	}
	s := newSource(t, map[string]any{"topics": []string{topic}})
	err := s.Read(t.Context(), source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: topic, SyncMode: source.Stream, State: []byte(`{"offsets":{"0":3}}`)}}}, newCapture())
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("Read = %v, want POSITION_LOST", err)
	}
}

// What the destination writes in Avro and Protobuf, the source reads back
// with the registry's schemas, and declares their types.
func TestRegistryFormats(t *testing.T) {
	for _, format := range []string{records.Avro, records.Protobuf} {
		t.Run(format, func(t *testing.T) {
			extra := map[string]any{"format": format, "schemaRegistryUrl": registryURL()}
			d := newDest(t, extra)
			pipeline, table := uniq("p"), uniq("typed")
			createTopic(t, table, 4)
			w := openWriter(t, d, pipeline, 1, table)
			e := destination.Epoch{Generation: 1, N: 1}
			write(t, w, e, table, 0, 40)
			st, err := w.PreCommit(t.Context(), e)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Commit(t.Context(), e, []destination.Staged{st}); err != nil {
				t.Fatal(err)
			}
			s := newSource(t, map[string]any{"format": format, "schemaRegistryUrl": registryURL(), "topics": []string{table}})
			cat, err := s.Discover(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			declared := map[string]string{}
			for _, f := range cat.Streams[0].Schema.Fields() {
				declared[f.Name] = f.Type.String()
			}
			if declared["id"] != "int64" || declared["name"] != "utf8" {
				t.Errorf("declared %v", declared)
			}
			c := read(t, s, table, nil)
			got := map[int64]int{}
			for _, r := range c.rows[table] {
				switch id := r["id"].(type) {
				case float64:
					got[int64(id)]++
				case string:
					n, _ := strconv.ParseInt(id, 10, 64)
					got[n]++
				}
				if !strings.HasPrefix(r[keyColumn].(string), `{"id":`) {
					t.Errorf("key %v", r[keyColumn])
				}
			}
			wantIDs(t, got, 0, 40)

			// Splits decode concurrently, from an empty schema cache.
			cold := newSource(t, map[string]any{"format": format, "schemaRegistryUrl": registryURL(), "topics": []string{table}})
			sc := readSplits(t, cold, table, splitsOf(t, cold, table, nil))
			if n := sc.count(); n != 40 {
				t.Errorf("read %d rows in splits, want 40", n)
			}
		})
	}
}

// splitCapture is an Emitter that keeps what a read emits per split, and holds
// a source to what the platform takes: emits on a split the stream has, none
// after it completes, one goroutine at a time.
type splitCapture struct {
	*capture
	mu     sync.Mutex
	splits map[string]map[string]*splitRecord
	total  int
	// Emits fail with errStopped once this many rows are in, if any.
	stopAt int
}

var errStopped = errors.New("stopped")

type splitRecord struct {
	busy     atomic.Bool
	rows     []map[string]any
	state    []byte
	covered  int
	complete bool
	added    []source.Split
}

func newSplitCapture(streams map[string][]source.Split) *splitCapture {
	c := &splitCapture{capture: newCapture(), splits: map[string]map[string]*splitRecord{}}
	for stream, splits := range streams {
		c.splits[stream] = map[string]*splitRecord{}
		for _, sp := range splits {
			c.splits[stream][sp.ID] = &splitRecord{state: sp.State}
		}
	}
	return c
}

func (c *splitCapture) Split(stream, id string) source.SplitEmitter {
	if id == source.WholeStream {
		return source.WholeSplit(c.capture, stream, id)
	}
	return splitEmit{c, stream, id}
}

type splitEmit struct {
	c          *splitCapture
	stream, id string
}

func (e splitEmit) emit(f func(*splitRecord) error) error {
	e.c.mu.Lock()
	rec := e.c.splits[e.stream][e.id]
	e.c.mu.Unlock()
	if rec == nil {
		return fmt.Errorf("emitted on split %s of %s, which it doesn't have", e.id, e.stream)
	}
	if !rec.busy.CompareAndSwap(false, true) {
		return fmt.Errorf("two goroutines emit on split %s of %s", e.id, e.stream)
	}
	defer rec.busy.Store(false)
	time.Sleep(time.Millisecond)
	e.c.mu.Lock()
	defer e.c.mu.Unlock()
	if rec.complete {
		return fmt.Errorf("emitted on split %s of %s after completing it", e.id, e.stream)
	}
	if e.c.stopAt > 0 && e.c.total >= e.c.stopAt {
		return errStopped
	}
	return f(rec)
}

func (e splitEmit) Rows(rows []json.RawMessage) error {
	return e.emit(func(rec *splitRecord) error {
		for _, r := range rows {
			var m map[string]any
			if err := json.Unmarshal(r, &m); err != nil {
				return err
			}
			rec.rows = append(rec.rows, m)
		}
		e.c.total += len(rows)
		return nil
	})
}

func (e splitEmit) Records(arrow.RecordBatch) error { return errors.New("no typed batches") }

func (e splitEmit) add(rec *splitRecord, added []source.Split) error {
	for _, sp := range added {
		if e.c.splits[e.stream][sp.ID] != nil {
			return fmt.Errorf("added split %s of %s, which it already has", sp.ID, e.stream)
		}
		e.c.splits[e.stream][sp.ID] = &splitRecord{state: sp.State}
		rec.added = append(rec.added, sp)
	}
	return nil
}

func (e splitEmit) ResumePoint(state []byte, added ...source.Split) error {
	return e.emit(func(rec *splitRecord) error {
		rec.state, rec.covered = state, len(rec.rows)
		return e.add(rec, added)
	})
}

func (e splitEmit) Complete(next []byte, added ...source.Split) error {
	return e.emit(func(rec *splitRecord) error {
		rec.state, rec.covered, rec.complete = next, len(rec.rows), true
		return e.add(rec, added)
	})
}

func (c *splitCapture) has(stream, id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.splits[stream][id] != nil
}

func (c *splitCapture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// states are the stream's splits where the read got to, as the platform's
// record has them.
func (c *splitCapture) states(stream string) []source.Split {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []source.Split
	for _, id := range slices.Sorted(maps.Keys(c.splits[stream])) {
		out = append(out, source.Split{ID: id, State: c.splits[stream][id].state})
	}
	return out
}

// rowsOf are the rows of the stream's splits. resumed: only those their last
// resume points cover.
func (c *splitCapture) rowsOf(stream string, resumed bool) []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []map[string]any
	for _, rec := range c.splits[stream] {
		if resumed {
			out = append(out, rec.rows[:rec.covered]...)
		} else {
			out = append(out, rec.rows...)
		}
	}
	return out
}

func splitsOf(t *testing.T, s *Source, topic string, ended []source.Split) []source.Split {
	t.Helper()
	splits, err := s.Splits(t.Context(), source.ConfiguredStream{Name: topic, SyncMode: source.Stream}, ended)
	if err != nil {
		t.Fatal(err)
	}
	return splits
}

// readSplits reads the splits, in a read that ends once every one completes.
func readSplits(t *testing.T, s *Source, topic string, splits []source.Split) *splitCapture {
	t.Helper()
	c := newSplitCapture(map[string][]source.Split{topic: splits})
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	req := source.ReadRequest{Pipeline: "p", Lanes: 2, Streams: []source.ConfiguredStream{{Name: topic, SyncMode: source.Stream, Splits: splits}}}
	if err := s.Read(ctx, req, c); err != nil {
		t.Fatal(err)
	}
	for id, rec := range c.splits[topic] {
		if !rec.complete {
			t.Errorf("split %s didn't complete", id)
		}
	}
	return c
}

// produceTo writes ids from..from+n-1 to one partition.
func produceTo(t *testing.T, topic string, partition int32, from, n int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()), kgo.DefaultProduceTopic(topic), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var recs []*kgo.Record
	for i := from; i < from+n; i++ {
		recs = append(recs, &kgo.Record{Partition: partition, Value: []byte(`{"id":` + strconv.Itoa(i) + `}`)})
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func addPartitions(t *testing.T, topic string, add int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	tds, err := adm.ListTopics(t.Context(), topic)
	if err != nil {
		t.Fatal(err)
	}
	want := len(tds[topic].Partitions) + add
	if _, err := adm.CreatePartitions(t.Context(), add, topic); err != nil {
		t.Fatal(err)
	}
	await(t, "the new partitions", func() bool {
		tds, err := adm.ListTopics(t.Context(), topic)
		return err == nil && len(tds[topic].Partitions) == want
	})
}

func groupOffsets(t *testing.T, group, topic string) map[int32]int64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	offs, err := kadm.NewClient(cl).FetchOffsets(t.Context(), group)
	if err != nil {
		t.Fatal(err)
	}
	out := map[int32]int64{}
	offs.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic {
			out[o.Partition] = o.At
		}
	})
	return out
}

// A partition is a split: each reads its own records, in order, completes
// where its partition ended, and the next read goes on from there.
func TestSourceReadsPartitionsAsSplits(t *testing.T) {
	topic := uniq("splits")
	createTopic(t, topic, 3)
	produce(t, topic, 0, 300)
	transaction(t, topic, 1000, 5, false)
	transaction(t, topic, 300, 5, true)
	s := newSource(t, map[string]any{"topics": []string{topic}})
	splits := splitsOf(t, s, topic, nil)
	if len(splits) != 3 || splits[0].ID != "p0" || splits[1].ID != "p1" || splits[2].ID != "p2" {
		t.Fatalf("splits %v", splits)
	}
	c := readSplits(t, s, topic, splits)
	wantIDs(t, rowIDs(t, c.rowsOf(topic, false)), 0, 305)
	for id, rec := range c.splits[topic] {
		p, err := parsePartition(id)
		if err != nil {
			t.Fatal(err)
		}
		offset := int64(-1)
		for _, r := range rec.rows {
			if int32(r[partitionColumn].(float64)) != p {
				t.Errorf("split %s has a row of partition %v", id, r[partitionColumn])
			}
			o := int64(r[offsetColumn].(float64))
			if o <= offset {
				t.Errorf("split %s: offset %d after %d", id, o, offset)
			}
			offset = o
		}
		next, err := splitOffsets([]source.Split{{ID: id, State: rec.state}})
		if err != nil {
			t.Fatal(err)
		}
		if next[p] <= offset {
			t.Errorf("split %s completes at %d, its last row's offset %d", id, next[p], offset)
		}
	}

	produce(t, topic, 305, 20)
	again := readSplits(t, s, topic, splitsOf(t, s, topic, c.states(topic)))
	wantIDs(t, rowIDs(t, again.rowsOf(topic, false)), 305, 325)
	idle := readSplits(t, s, topic, splitsOf(t, s, topic, again.states(topic)))
	if n := idle.count(); n != 0 {
		t.Errorf("%d rows from a read with nothing new", n)
	}
	if !slices.EqualFunc(idle.states(topic), again.states(topic), func(a, b source.Split) bool { return a.ID == b.ID && bytes.Equal(a.State, b.State) }) {
		t.Errorf("a read with nothing new moved: %s, from %s", idle.states(topic), again.states(topic))
	}
}

// A retry reads only the splits that haven't completed, each from its state,
// and nothing else: not the partition that completed, nor any record twice.
func TestSourceResumesAMixOfSplits(t *testing.T) {
	topic := uniq("mix")
	createTopic(t, topic, 3)
	produce(t, topic, 0, 300)
	s := newSource(t, map[string]any{"topics": []string{topic}})
	first := readSplits(t, s, topic, splitsOf(t, s, topic, nil))
	want := map[int64]int{}
	midway := first.splits[topic]["p1"].rows[len(first.splits[topic]["p1"].rows)/2]
	mid := int64(midway[offsetColumn].(float64))
	for _, r := range first.rowsOf(topic, false) {
		switch int32(r[partitionColumn].(float64)) {
		case 1:
			if int64(r[offsetColumn].(float64)) >= mid {
				want[int64(r["id"].(float64))]++
			}
		case 2:
			want[int64(r["id"].(float64))]++
		}
	}
	c := readSplits(t, s, topic, []source.Split{{ID: "p1", State: encodeOffset(mid)}, {ID: "p2"}})
	if c.has(topic, "p0") {
		t.Error("read a split that had completed")
	}
	if got := rowIDs(t, c.rowsOf(topic, false)); !maps.Equal(got, want) {
		t.Errorf("read %v, want %v", got, want)
	}
}

// A read that stops partway restarts from its splits' states: every record is
// in what those cover or what the restart reads, exactly once.
func TestSourceRestartsFromSplitStates(t *testing.T) {
	topic := uniq("restart")
	createTopic(t, topic, 4)
	for i := range 12 {
		produce(t, topic, i*1000, 1000)
	}
	s := newSource(t, map[string]any{"topics": []string{topic}})
	splits := splitsOf(t, s, topic, nil)
	c := newSplitCapture(map[string][]source.Split{topic: splits})
	c.stopAt = pollRecords + 1000
	req := source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: topic, SyncMode: source.Stream, Splits: splits}}}
	if err := s.Read(t.Context(), req, c); !errors.Is(err, errStopped) {
		t.Fatalf("Read = %v", err)
	}
	covered := len(c.rowsOf(topic, true))
	if covered == 0 || covered == 12000 {
		t.Errorf("the states cover %d rows of 12000", covered)
	}
	again := readSplits(t, s, topic, c.states(topic))
	got := rowIDs(t, c.rowsOf(topic, true))
	for id, n := range rowIDs(t, again.rowsOf(topic, false)) {
		got[id] += n
	}
	wantIDs(t, got, 0, 12000)
}

// A partition added during a continuous read joins at a resume point of a
// running split, at its start, and reads, reports and commits like the rest.
func TestSourceSplitsGainNewPartitions(t *testing.T) {
	topic := uniq("grows")
	createTopic(t, topic, 2)
	produceTo(t, topic, 0, 0, 10)
	produceTo(t, topic, 1, 10, 10)
	s := newSource(t, map[string]any{"topics": []string{topic}})
	splits := splitsOf(t, s, topic, nil)
	c := newSplitCapture(map[string][]source.Split{topic: splits})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Pipeline: uniq("p"), Continuous: true, Lanes: 2, Streams: []source.ConfiguredStream{{Name: topic, SyncMode: source.Stream, Splits: splits}}}, c)
	}()
	await(t, "the first records", func() bool { return c.count() == 20 })
	addPartitions(t, topic, 1)
	produceTo(t, topic, 2, 20, 5)
	awaitFor(t, "the new partition", 2*partitionsEvery, func() bool { return c.has(topic, "p2") })
	await(t, "the new partition's records", func() bool { return c.count() == 25 })
	produceTo(t, topic, 0, 25, 5)
	await(t, "records after it joined", func() bool { return c.count() == 30 })
	awaitFor(t, "a progress report", 2*progressEvery, func() bool {
		c.capture.mu.Lock()
		defer c.capture.mu.Unlock()
		return len(c.capture.progress) > 0
	})
	c.mu.Lock()
	rec := c.splits[topic]["p2"]
	added := append(slices.Clone(c.splits[topic]["p0"].added), c.splits[topic]["p1"].added...)
	nrows := len(rec.rows)
	c.mu.Unlock()
	if len(added) != 1 || added[0].ID != "p2" || string(added[0].State) != string(encodeOffset(0)) {
		t.Errorf("added %v", added)
	}
	if nrows != 5 {
		t.Errorf("the new split has %d rows, want 5", nrows)
	}
	progress := func() source.Progress {
		c.capture.mu.Lock()
		defer c.capture.mu.Unlock()
		return c.capture.progress[0]
	}()
	if progress.Stream != topic || progress.LagRecords == nil {
		t.Errorf("progress %+v", progress)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Read = %v", err)
	}
	wantIDs(t, rowIDs(t, c.rowsOf(topic, false)), 0, 30)

	// The checkpoint's offsets go to the group, by split, and a restart from
	// them reads what follows, in the new partition too.
	group := uniq("group")
	acker := newSource(t, map[string]any{"topics": []string{topic}, "group": group})
	if err := acker.Committed(t.Context(), nil, map[string][]source.Split{topic: c.states(topic)}); err != nil {
		t.Fatal(err)
	}
	if got := groupOffsets(t, group, topic); !maps.Equal(got, map[int32]int64{0: 15, 1: 10, 2: 5}) {
		t.Errorf("the group's offsets %v", got)
	}
	produceTo(t, topic, 2, 30, 5)
	again := readSplits(t, s, topic, c.states(topic))
	wantIDs(t, rowIDs(t, again.rowsOf(topic, false)), 30, 35)
}

// A record from before splits continues whole, in the state it has. Once that
// read ends, the next is in splits, from its offsets, beside a topic read
// whole in the same request.
func TestSourceWholeStatesKeepWorking(t *testing.T) {
	topic, other := uniq("was_whole"), uniq("stays_whole")
	createTopic(t, topic, 2)
	createTopic(t, other, 1)
	produce(t, topic, 0, 50)
	produce(t, other, 0, 10)
	s := newSource(t, map[string]any{"topics": []string{topic, other}})
	first := read(t, s, topic, nil)
	wantIDs(t, rowIDs(t, first.rows[topic]), 0, 50)
	offsets, err := parseState(first.complete[topic])
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 {
		t.Fatalf("whole state %s", first.complete[topic])
	}
	produce(t, topic, 50, 10)
	continued := read(t, s, topic, first.complete[topic])
	wantIDs(t, rowIDs(t, continued.rows[topic]), 50, 60)
	if _, err := parseState(continued.states[topic]); err != nil || len(continued.states[topic]) == 0 {
		t.Errorf("whole resume point %s: %v", continued.states[topic], err)
	}

	produce(t, topic, 60, 10)
	ended := []source.Split{{ID: source.WholeStream, State: continued.complete[topic]}}
	splits := splitsOf(t, s, topic, ended)
	if len(splits) != 2 || splits[0].ID != "p0" || splits[1].ID != "p1" {
		t.Fatalf("splits %v", splits)
	}
	c := newSplitCapture(map[string][]source.Split{topic: splits})
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	req := source.ReadRequest{Pipeline: "p", Lanes: 2, Streams: []source.ConfiguredStream{{Name: topic, SyncMode: source.Stream, Splits: splits}, {Name: other, SyncMode: source.Stream}}}
	if err := s.Read(ctx, req, c); err != nil {
		t.Fatal(err)
	}
	wantIDs(t, rowIDs(t, c.rowsOf(topic, false)), 60, 70)
	wantIDs(t, rowIDs(t, c.rows[other]), 0, 10)
	if c.complete[other] == nil {
		t.Errorf("%s didn't complete", other)
	}
	for _, sp := range c.states(topic) {
		if !c.splits[topic][sp.ID].complete {
			t.Errorf("split %s didn't complete", sp.ID)
		}
	}

	// A group commit takes both kinds of state at once.
	group := uniq("group")
	acker := newSource(t, map[string]any{"topics": []string{topic, other}, "group": group})
	if err := acker.Committed(t.Context(), map[string][]byte{other: c.complete[other]}, map[string][]source.Split{topic: c.states(topic)}); err != nil {
		t.Fatal(err)
	}
	if got := groupOffsets(t, group, other); !maps.Equal(got, map[int32]int64{0: 10}) {
		t.Errorf("the group's offsets of %s: %v", other, got)
	}
	var sum int64
	for _, o := range groupOffsets(t, group, topic) {
		sum += o
	}
	if sum != 70 {
		t.Errorf("the group's offsets of %s add to %d, want 70", topic, sum)
	}
}

// startOffset latest starts a topic never read at its end; a partition added
// after that read starts at its start, since it's all new.
func TestSourceSplitsStartAtLatest(t *testing.T) {
	topic := uniq("latest")
	createTopic(t, topic, 2)
	produce(t, topic, 0, 20)
	s := newSource(t, map[string]any{"topics": []string{topic}, "startOffset": "latest"})
	c := readSplits(t, s, topic, splitsOf(t, s, topic, nil))
	if n := c.count(); n != 0 {
		t.Errorf("read %d rows from before the first read", n)
	}
	produce(t, topic, 20, 20)
	addPartitions(t, topic, 1)
	produceTo(t, topic, 2, 40, 5)
	again := readSplits(t, s, topic, splitsOf(t, s, topic, c.states(topic)))
	wantIDs(t, rowIDs(t, again.rowsOf(topic, false)), 20, 45)
}

// An offset below its partition's start is lost, when asked for the splits
// and when read; a split at its beginning reads what's left.
func TestSourceSplitPositionLost(t *testing.T) {
	topic := uniq("retained")
	createTopic(t, topic, 1)
	produce(t, topic, 0, 10)
	cl, _ := kgo.NewClient(kgo.SeedBrokers(brokers()))
	defer cl.Close()
	del := kadm.Offsets{}
	del.Add(kadm.Offset{Topic: topic, Partition: 0, At: 6})
	if _, err := kadm.NewClient(cl).DeleteRecords(t.Context(), del); err != nil {
		t.Fatal(err)
	}
	s := newSource(t, map[string]any{"topics": []string{topic}})
	lost := []source.Split{{ID: "p0", State: encodeOffset(3)}}
	wantLost := func(err error) {
		t.Helper()
		var coded interface{ ErrorCode() protocolv1.ErrorCode }
		if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
			t.Errorf("got %v, want POSITION_LOST", err)
		}
	}
	_, err := s.Splits(t.Context(), source.ConfiguredStream{Name: topic}, lost)
	wantLost(err)
	wantLost(s.Read(t.Context(), source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: topic, SyncMode: source.Stream, Splits: lost}}}, newSplitCapture(map[string][]source.Split{topic: lost})))
	c := readSplits(t, s, topic, []source.Split{{ID: "p0"}})
	if n := c.count(); n != 4 {
		t.Errorf("read %d rows of the 4 left", n)
	}
}
