//go:build integration

package kafka

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
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
	md := arrow.NewMetadata([]string{tableKey}, []string{table})
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: idColumn, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: meta(uuidExtension)},
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
		if _, err := w.Commit(t.Context(), e1, st); err != nil {
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
	if _, err := w.Commit(t.Context(), e2, st2); err != nil {
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
	if _, err := old.Commit(t.Context(), e1, st1); err != nil {
		t.Fatal(err)
	}
	write(t, old, e2, table, 20, 40)
	st2, err := old.PreCommit(t.Context(), e2)
	if err != nil {
		t.Fatal(err)
	}

	next := openWriter(t, d, pipeline, 2, table)
	if r, err := next.Recover(t.Context(), 2, e2, st2); err != nil || r != destination.Lost {
		t.Errorf("Recover(2) = %v, %v; want lost", r, err)
	}
	if r, err := next.Recover(t.Context(), 2, e1, st1); err != nil || r != destination.Applied {
		t.Errorf("Recover(1) = %v, %v; want applied", r, err)
	}
	if _, err := old.Commit(t.Context(), e2, st2); !errors.Is(err, destination.ErrFenced) {
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
	if _, err := next.Commit(t.Context(), e, st); err != nil {
		t.Fatal(err)
	}
	if r, err := next.Recover(t.Context(), 2, e, st); err != nil || r != destination.Applied {
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
	if err := s.Committed(t.Context(), map[string][]byte{topic: c.states[topic]}); err != nil {
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

func await(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
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
	for _, format := range []string{formatAvro, formatProtobuf} {
		t.Run(format, func(t *testing.T) {
			extra := map[string]any{"format": format, "schemaRegistryUrl": registryURL()}
			d := newDest(t, extra)
			pipeline, table := uniq("p"), uniq("typed")
			w := openWriter(t, d, pipeline, 1, table)
			e := destination.Epoch{Generation: 1, N: 1}
			write(t, w, e, table, 0, 10)
			st, err := w.PreCommit(t.Context(), e)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Commit(t.Context(), e, st); err != nil {
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
			wantIDs(t, got, 0, 10)
		})
	}
}

func TestRegistryRefusesAnIncompatibleSchema(t *testing.T) {
	d := newDest(t, map[string]any{"format": formatAvro, "schemaRegistryUrl": registryURL()})
	reg, err := d.cfg.registry()
	if err != nil {
		t.Fatal(err)
	}
	subject := uniq("drift") + "-value"
	if _, err := newRowEncoder(t.Context(), formatAvro, reg, subject, "t", []col{{name: "id", kind: kInt64}}); err != nil {
		t.Fatal(err)
	}
	if _, err := newRowEncoder(t.Context(), formatAvro, reg, subject, "t", []col{{name: "id", kind: kInt64}, {name: "extra", kind: kString}}); err != nil {
		t.Errorf("an added column: %v", err)
	}
	_, err = newRowEncoder(t.Context(), formatAvro, reg, subject, "t", []col{{name: "id", kind: kBool}})
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT {
		t.Errorf("a changed type: %v", err)
	}

}
