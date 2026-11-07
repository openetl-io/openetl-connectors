//go:build integration

package kinesis

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
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
	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/google/uuid"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Runs against the compose kinesis-mock and schema registry (make
// deps-up), or OPENETL_TEST_KINESIS_ENDPOINT and
// OPENETL_TEST_SCHEMA_REGISTRY.
func endpoint() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_KINESIS_ENDPOINT"), "http://127.0.0.1:4568")
}

func registryURL() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_SCHEMA_REGISTRY"), "http://127.0.0.1:8081")
}

func uniq(prefix string) string { return prefix + "_" + strings.ToLower(rand.Text())[:8] }

func config(t *testing.T, extra map[string]any) []byte {
	t.Helper()
	m := map[string]any{"region": "us-east-1", "endpoint": endpoint(), "accessKeyId": "openetl", "secretAccessKey": "openetl"}
	for k, v := range extra {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func client(t *testing.T) *awskinesis.Client {
	t.Helper()
	c := Connection{Region: "us-east-1", Endpoint: endpoint()}
	if err := json.Unmarshal(config(t, nil), &c); err != nil {
		t.Fatal(err)
	}
	cl, err := c.client(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func active(t *testing.T, cl *awskinesis.Client, name string) {
	t.Helper()
	await(t, name+" to be active", func() bool {
		d, err := cl.DescribeStreamSummary(t.Context(), &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
		return err == nil && d.StreamDescriptionSummary.StreamStatus == types.StreamStatusActive
	})
}

func createStream(t *testing.T, name string, shards int32) {
	t.Helper()
	cl := client(t)
	if _, err := cl.CreateStream(t.Context(), &awskinesis.CreateStreamInput{StreamName: aws.String(name), ShardCount: aws.Int32(shards)}); err != nil {
		t.Fatal(err)
	}
	active(t, cl, name)
}

// putJSON puts ids from..from+n-1, each keyed k<id mod 10>.
func putJSON(t *testing.T, stream string, from, n int) {
	t.Helper()
	cl := client(t)
	for i := from; i < from+n; i += maxRequestRecords {
		var entries []types.PutRecordsRequestEntry
		for j := i; j < min(i+maxRequestRecords, from+n); j++ {
			entries = append(entries, types.PutRecordsRequestEntry{
				Data: []byte(`{"id":` + strconv.Itoa(j) + `,"name":"n"}`), PartitionKey: aws.String("k" + strconv.Itoa(j%10)),
			})
		}
		res, err := cl.PutRecords(t.Context(), &awskinesis.PutRecordsInput{StreamName: aws.String(stream), Records: entries})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToInt32(res.FailedRecordCount) > 0 {
			t.Fatalf("%d records failed", aws.ToInt32(res.FailedRecordCount))
		}
	}
}

// split splits a shard in the middle of its hash keys.
func split(t *testing.T, stream, shard string) {
	t.Helper()
	cl := client(t)
	half := new(big.Int).Lsh(big.NewInt(1), 127)
	if _, err := cl.SplitShard(t.Context(), &awskinesis.SplitShardInput{StreamName: aws.String(stream), ShardToSplit: aws.String(shard), NewStartingHashKey: aws.String(half.String())}); err != nil {
		t.Fatal(err)
	}
	active(t, cl, stream)
}

// capture is an Emitter that keeps what a read emits, in order.
type capture struct {
	mu       sync.Mutex
	rows     map[string][]map[string]any
	states   map[string][][]byte
	complete map[string][]byte
	progress []source.Progress
}

func newCapture() *capture {
	return &capture{rows: map[string][]map[string]any{}, states: map[string][][]byte{}, complete: map[string][]byte{}}
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
	c.states[stream] = append(c.states[stream], state)
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

func read(t *testing.T, s *Source, stream string, state []byte) *capture {
	t.Helper()
	c := newCapture()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	req := source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream, State: state}}}
	if err := s.Read(ctx, req, c); err != nil {
		t.Fatal(err)
	}
	if c.complete[stream] == nil {
		t.Fatalf("%s didn't complete", stream)
	}
	return c
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

func wantIDs(t *testing.T, got map[int64]int, from, to int) {
	t.Helper()
	bad := 0
	for i := from; i < to; i++ {
		if got[int64(i)] != 1 {
			bad++
			if bad < 5 {
				t.Errorf("id %d: %d times, want once", i, got[int64(i)])
			}
		}
	}
	if len(got) != to-from {
		t.Errorf("%d distinct ids, want %d", len(got), to-from)
	}
}

// A bounded read reads every shard to where it caught up, and the next
// resumes exactly after it.
func TestSourceReadsToTheEndAndResumes(t *testing.T) {
	stream := uniq("events")
	createStream(t, stream, 2)
	putJSON(t, stream, 0, 100)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Streams) != 1 || cat.Streams[0].Name != stream || cat.Streams[0].Replay != source.Exact || cat.Streams[0].CursorField != sequenceColumn {
		t.Fatalf("catalog %+v", cat)
	}
	c := read(t, s, stream, nil)
	wantIDs(t, rowIDs(t, c.rows[stream]), 0, 100)
	r := c.rows[stream][0]
	for _, col := range []string{shardColumn, sequenceColumn, arrivalColumn, partitionKeyColumn, "name"} {
		if _, ok := r[col]; !ok {
			t.Errorf("row %v lacks %s", r, col)
		}
	}
	putJSON(t, stream, 100, 50)
	c = read(t, s, stream, c.complete[stream])
	wantIDs(t, rowIDs(t, c.rows[stream]), 100, 150)
	c = read(t, s, stream, c.complete[stream])
	if n := c.count(stream); n != 0 {
		t.Errorf("a read with nothing new read %d rows", n)
	}
}

// checkOrder fails unless every row of the parent comes before every row of
// its children, and each partition key's ids increase.
func checkOrder(t *testing.T, rows []map[string]any, parent string) {
	t.Helper()
	lastParent, firstChild := -1, len(rows)
	byKey := map[string]float64{}
	for i, r := range rows {
		if r[shardColumn] == parent {
			lastParent = i
		} else if firstChild == len(rows) {
			firstChild = i
		}
		k := r[partitionKeyColumn].(string)
		if id := r["id"].(float64); id < byKey[k] {
			t.Fatalf("key %s: id %v after %v", k, id, byKey[k])
		} else {
			byKey[k] = id
		}
	}
	if firstChild < lastParent {
		t.Errorf("a child's row at %d, before the parent's last at %d", firstChild, lastParent)
	}
	if firstChild == len(rows) {
		t.Error("no rows from the children")
	}
}

// After a split, a read reads the parent to its end before its children,
// from the start and from a position partway through the parent.
func TestSourceReadsParentsBeforeChildren(t *testing.T) {
	stream := uniq("resharded")
	createStream(t, stream, 1)
	// Past one GetRecords, so the parent takes two.
	putJSON(t, stream, 0, 12000)
	parent := "shardId-000000000000"
	split(t, stream, parent)
	putJSON(t, stream, 12000, 200)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	c := read(t, s, stream, nil)
	wantIDs(t, rowIDs(t, c.rows[stream]), 0, 12200)
	checkOrder(t, c.rows[stream], parent)
	var st state
	if err := json.Unmarshal(c.complete[stream], &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Shards) != 3 || !st.Shards[parent].Done {
		t.Errorf("state %s", c.complete[stream])
	}

	first := c.states[stream][0]
	if err := json.Unmarshal(first, &st); err != nil || st.Shards[parent].Done || st.Shards[parent].After == "" {
		t.Fatalf("the first resume point %s: %v", first, err)
	}
	c = read(t, s, stream, first)
	got := rowIDs(t, c.rows[stream])
	wantIDs(t, got, 12200-len(got), 12200)
	if len(got) <= 200 {
		t.Errorf("resuming partway through the parent read %d rows", len(got))
	}
	checkOrder(t, c.rows[stream], parent)
}

// A continuous read follows a split: the parent's records, then its
// children's, each once, and it reports its lag and retention left.
func TestSourceFollowsASplit(t *testing.T) {
	stream := uniq("live")
	createStream(t, stream, 1)
	putJSON(t, stream, 0, 100)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	c := newCapture()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Pipeline: "p", Continuous: true, Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream}}}, c)
	}()
	await(t, "the first records", func() bool { return c.count(stream) == 100 })
	split(t, stream, "shardId-000000000000")
	putJSON(t, stream, 100, 100)
	await(t, "the children's records", func() bool { return c.count(stream) == 200 })
	await(t, "a progress report", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.progress) > 0
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Read = %v", err)
	}
	wantIDs(t, rowIDs(t, c.rows[stream]), 0, 200)
	checkOrder(t, c.rows[stream], "shardId-000000000000")
	p := c.progress[0]
	if p.Stream != stream || p.Lag == nil || p.RetentionLeft == nil || *p.RetentionLeft != 24*time.Hour {
		t.Errorf("progress %+v", p)
	}
}

func await(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// startPosition latest skips what the stream held before the first read.
func TestSourceStartsAtLatest(t *testing.T) {
	stream := uniq("latest")
	createStream(t, stream, 2)
	putJSON(t, stream, 0, 20)
	s := newSource(t, map[string]any{"streams": []string{stream}, "startPosition": "latest"})
	c := read(t, s, stream, nil)
	if n := c.count(stream); n != 0 {
		t.Errorf("read %d rows from before the first read", n)
	}
	putJSON(t, stream, 20, 20)
	c = read(t, s, stream, c.complete[stream])
	wantIDs(t, rowIDs(t, c.rows[stream]), 20, 40)
}

func wantCode(t *testing.T, err error, code protocolv1.ErrorCode) {
	t.Helper()
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if !errors.As(err, &coded) || coded.ErrorCode() != code {
		t.Errorf("err = %v, want %v", err, code)
	}
}

// A position older than retention is lost unless the shard still holds its
// record; a shard Kinesis no longer lists is lost only if it could have
// held records the read hadn't reached.
func TestSourcePositionLost(t *testing.T) {
	stream := uniq("retained")
	createStream(t, stream, 1)
	putJSON(t, stream, 0, 10)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	rows := read(t, s, stream, nil).rows[stream]
	shard, oldest := rows[0][shardColumn].(string), rows[0][sequenceColumn].(string)
	old := time.Now().Add(-25 * time.Hour)
	at := func(shards map[string]shardState) []byte { return encodeState(shards) }

	c := read(t, s, stream, at(map[string]shardState{shard: {After: oldest, Since: old}}))
	wantIDs(t, rowIDs(t, c.rows[stream]), 1, 10)

	req := func(st []byte) source.ReadRequest {
		return source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream, State: st}}}
	}
	err := s.Read(t.Context(), req(at(map[string]shardState{shard: {After: "1", Since: old}})), newCapture())
	wantCode(t, err, protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST)
	err = s.Read(t.Context(), req(at(map[string]shardState{shard: {After: rows[9][sequenceColumn].(string)}, "shardId-000000000099": {After: "1", Since: old}})), newCapture())
	wantCode(t, err, protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST)

	c = read(t, s, stream, at(map[string]shardState{shard: {After: rows[4][sequenceColumn].(string), Since: time.Now()}, "shardId-000000000099": {After: "1", Since: time.Now()}}))
	wantIDs(t, rowIDs(t, c.rows[stream]), 5, 10)
	var st state
	if err := json.Unmarshal(c.complete[stream], &st); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Shards["shardId-000000000099"]; ok || len(st.Shards) != 1 {
		t.Errorf("state %s keeps a shard Kinesis doesn't list", c.complete[stream])
	}
}

func newDest(t *testing.T, extra map[string]any) *Destination {
	t.Helper()
	d, err := NewDestination(config(t, extra))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Check(t.Context()); err != nil {
		t.Fatalf("check (is kinesis-mock running? make deps-up): %v", err)
	}
	return d
}

func openWriter(t *testing.T, d *Destination, pipeline, namespace string, streams map[string][]string) destination.Writer {
	t.Helper()
	cfg := destination.OpenConfig{Pipeline: pipeline, Generation: 1, Scope: destination.ScopeStream, Namespace: namespace}
	for s, key := range streams {
		cfg.Streams = append(cfg.Streams, destination.Stream{Name: s, Mode: destination.Append, PrimaryKey: key})
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
		{Name: "_openetl_op", Type: arrow.BinaryTypes.String},
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

// everything reads every record of a stream from each shard's start.
func everything(t *testing.T, stream string) []types.Record {
	t.Helper()
	cl := client(t)
	shards, err := cl.ListShards(t.Context(), &awskinesis.ListShardsInput{StreamName: aws.String(stream)})
	if err != nil {
		t.Fatal(err)
	}
	var out []types.Record
	for _, sh := range shards.Shards {
		it, err := cl.GetShardIterator(t.Context(), &awskinesis.GetShardIteratorInput{StreamName: aws.String(stream), ShardId: sh.ShardId, ShardIteratorType: types.ShardIteratorTypeTrimHorizon})
		if err != nil {
			t.Fatal(err)
		}
		next := it.ShardIterator
		for next != nil {
			res, err := cl.GetRecords(t.Context(), &awskinesis.GetRecordsInput{ShardIterator: next})
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, res.Records...)
			if len(res.Records) == 0 && aws.ToInt64(res.MillisBehindLatest) == 0 {
				break
			}
			next = res.NextShardIterator
		}
	}
	return out
}

// The destination creates each stream, and PreCommit returns once every
// record is in it: keyed by the primary key, or else by _openetl_id.
func TestDestinationDelivers(t *testing.T) {
	d := newDest(t, nil)
	ns := uniq("ns")
	w := openWriter(t, d, uniq("p"), ns, map[string][]string{"keyed": {"id"}, "keyless": nil})
	e := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e, "keyed", 0, 1200)
	write(t, w, e, "keyless", 0, 30)
	if _, err := w.PreCommit(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), e, nil); err != nil {
		t.Fatal(err)
	}
	if r, err := w.Recover(t.Context(), 2, e, nil); err != nil || r != destination.Applied {
		t.Errorf("Recover = %v, %v", r, err)
	}
	for table, n := range map[string]int{"keyed": 1200, "keyless": 30} {
		recs := everything(t, ns+"."+table)
		got := map[int64]int{}
		for _, r := range recs {
			var v map[string]any
			if err := json.Unmarshal(r.Data, &v); err != nil {
				t.Fatal(err)
			}
			id := int64(v["id"].(float64))
			got[id]++
			want := strconv.FormatInt(id, 10)
			if table == "keyless" {
				want = v[idColumn].(string)
			}
			if k := aws.ToString(r.PartitionKey); k != want {
				t.Fatalf("%s: partition key %q, want %q", table, k, want)
			}
			if _, ok := v["_openetl_op"]; !ok {
				t.Fatalf("%s: data %s lacks _openetl_op", table, r.Data)
			}
		}
		wantIDs(t, got, 0, n)
	}
}

// What the destination writes in each format, the source reads back, with
// the registry's schemas for Avro and Protobuf.
func TestFormatsRoundTrip(t *testing.T) {
	for _, format := range []string{records.JSON, records.Avro, records.Protobuf} {
		t.Run(format, func(t *testing.T) {
			extra := map[string]any{"format": format, "schemaRegistryUrl": registryURL()}
			d := newDest(t, extra)
			table := uniq("typed")
			w := openWriter(t, d, uniq("p"), "", map[string][]string{table: {"id"}})
			e := destination.Epoch{Generation: 1, N: 1}
			write(t, w, e, table, 0, 10)
			if _, err := w.PreCommit(t.Context(), e); err != nil {
				t.Fatal(err)
			}
			extra["streams"] = []string{table}
			s := newSource(t, extra)
			cat, err := s.Discover(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			declared := map[string]string{}
			for _, f := range cat.Streams[0].Schema.Fields() {
				declared[f.Name] = f.Type.String()
			}
			if format != records.JSON && (declared["id"] != "int64" || declared["name"] != "utf8") {
				t.Errorf("declared %v", declared)
			}
			c := read(t, s, table, nil)
			got := map[int64]int{}
			for _, r := range c.rows[table] {
				var n int64
				switch id := r["id"].(type) {
				case float64:
					n = int64(id)
				case string:
					n, _ = strconv.ParseInt(id, 10, 64)
				}
				got[n]++
				if k := strconv.FormatInt(n, 10); r[partitionKeyColumn] != k {
					t.Errorf("partition key %v, want %s", r[partitionKeyColumn], k)
				}
			}
			wantIDs(t, got, 0, 10)
		})
	}
}

// merge merges two adjacent shards.
func merge(t *testing.T, stream, a, b string) {
	t.Helper()
	cl := client(t)
	if _, err := cl.MergeShards(t.Context(), &awskinesis.MergeShardsInput{StreamName: aws.String(stream), ShardToMerge: aws.String(a), AdjacentShardToMerge: aws.String(b)}); err != nil {
		t.Fatal(err)
	}
	active(t, cl, stream)
}

// splitCapture is an Emitter that keeps what a read emits per split, and holds
// a source to what the platform takes: emits on a split the stream has, none
// after it completes, one goroutine at a time.
type splitCapture struct {
	*capture
	mu     sync.Mutex
	splits map[string]map[string]*splitRecord
	seq    int
}

type splitRecord struct {
	busy     atomic.Bool
	rows     []map[string]any
	state    []byte
	covered  int
	complete bool
	added    []source.Split
	// When its first and last rows came, and when it completed.
	firstRow, lastRow, completed int
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
	e.c.seq++
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
		if rec.firstRow == 0 {
			rec.firstRow = e.c.seq
		}
		rec.lastRow = e.c.seq
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
		rec.state, rec.covered, rec.complete, rec.completed = next, len(rec.rows), true, e.c.seq
		return e.add(rec, added)
	})
}

// states are the stream's splits where the read got to. open: only those a
// retry reads.
func (c *splitCapture) states(stream string, open bool) []source.Split {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []source.Split
	for _, id := range slices.Sorted(maps.Keys(c.splits[stream])) {
		if !open || !c.splits[stream][id].complete {
			out = append(out, source.Split{ID: id, State: c.splits[stream][id].state})
		}
	}
	return out
}

func (c *splitCapture) ids(t *testing.T, stream string, resumed bool) map[int64]int {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var rows []map[string]any
	for _, rec := range c.splits[stream] {
		if resumed {
			rows = append(rows, rec.rows[:rec.covered]...)
		} else {
			rows = append(rows, rec.rows...)
		}
	}
	return rowIDs(t, rows)
}

func (c *splitCapture) total(stream string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, rec := range c.splits[stream] {
		n += len(rec.rows)
	}
	return n
}

func (c *splitCapture) has(stream, id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.splits[stream][id] != nil
}

func (c *splitCapture) record(stream, id string) *splitRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.splits[stream][id]
}

// wantAfter fails unless every row of the child comes after every row of the
// parent, whose split has completed by then.
func (c *splitCapture) wantAfter(t *testing.T, stream, parent, child string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ch := c.splits[stream][parent], c.splits[stream][child]
	if p == nil || ch == nil {
		t.Fatalf("splits %s and %s: %v, %v", parent, child, p, ch)
	}
	if ch.firstRow != 0 && (ch.firstRow < p.lastRow || !p.complete || ch.firstRow < p.completed) {
		t.Errorf("%s's first row at %d, %s's last at %d, completed at %d", child, ch.firstRow, parent, p.lastRow, p.completed)
	}
}

func splitsOf(t *testing.T, s *Source, stream string, ended []source.Split) []source.Split {
	t.Helper()
	splits, err := s.Splits(t.Context(), source.ConfiguredStream{Name: stream, SyncMode: source.Stream}, ended)
	if err != nil {
		t.Fatal(err)
	}
	return splits
}

func wantSplits(t *testing.T, splits []source.Split, ids ...string) {
	t.Helper()
	got := make([]string, len(splits))
	for i, sp := range splits {
		got[i] = sp.ID
	}
	if !slices.Equal(got, ids) {
		t.Errorf("splits %v, want %v", got, ids)
	}
}

// readSplits reads the splits, in a read that ends once every one completes.
func readSplits(t *testing.T, s *Source, stream string, splits []source.Split) *splitCapture {
	t.Helper()
	c := newSplitCapture(map[string][]source.Split{stream: splits})
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	req := source.ReadRequest{Pipeline: "p", Lanes: 2, Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream, Splits: splits}}}
	if err := s.Read(ctx, req, c); err != nil {
		t.Fatal(err)
	}
	for id, rec := range c.splits[stream] {
		if !rec.complete {
			t.Errorf("split %s didn't complete", id)
		}
	}
	return c
}

// A shard is a split: each reads its own records, completes where it stopped,
// and the next read goes on from there. A stream read whole last time splits
// from the whole state.
func TestSourceReadsShardsAsSplits(t *testing.T) {
	stream := uniq("splits")
	createStream(t, stream, 2)
	putJSON(t, stream, 0, 100)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	const a, b = "shardId-000000000000", "shardId-000000000001"
	splits := splitsOf(t, s, stream, nil)
	wantSplits(t, splits, a, b)
	c := readSplits(t, s, stream, splits)
	wantIDs(t, c.ids(t, stream, false), 0, 100)
	for id, rec := range c.splits[stream] {
		var st shardState
		if err := json.Unmarshal(rec.state, &st); err != nil {
			t.Fatal(err)
		}
		if len(rec.rows) == 0 || st.Done || st.After != rec.rows[len(rec.rows)-1][sequenceColumn] {
			t.Errorf("split %s: state %s after %d rows", id, rec.state, len(rec.rows))
		}
		for _, r := range rec.rows {
			if r[shardColumn] != id {
				t.Errorf("split %s has a row of shard %v", id, r[shardColumn])
			}
		}
	}
	putJSON(t, stream, 100, 50)
	again := readSplits(t, s, stream, splitsOf(t, s, stream, c.states(stream, false)))
	wantIDs(t, again.ids(t, stream, false), 100, 150)
	idle := readSplits(t, s, stream, splitsOf(t, s, stream, again.states(stream, false)))
	if n := idle.total(stream); n != 0 {
		t.Errorf("a read with nothing new read %d rows", n)
	}

	// A record from before splits continues whole, and splits once it ends.
	whole := read(t, s, stream, nil)
	wantIDs(t, rowIDs(t, whole.rows[stream]), 0, 150)
	putJSON(t, stream, 150, 10)
	whole = read(t, s, stream, whole.complete[stream])
	wantIDs(t, rowIDs(t, whole.rows[stream]), 150, 160)
	putJSON(t, stream, 160, 10)
	fromWhole := splitsOf(t, s, stream, []source.Split{{ID: source.WholeStream, State: whole.complete[stream]}})
	wantSplits(t, fromWhole, a, b)
	wantIDs(t, readSplits(t, s, stream, fromWhole).ids(t, stream, false), 160, 170)
}

// A split reads its parent first: the children join with its completion, and
// the reads after it know it's done without being told, however far back.
func TestSourceSplitsJoinAfterTheirParent(t *testing.T) {
	stream := uniq("resharded")
	createStream(t, stream, 1)
	putJSON(t, stream, 0, 300)
	const parent, c1, c2 = "shardId-000000000000", "shardId-000000000001", "shardId-000000000002"
	split(t, stream, parent)
	putJSON(t, stream, 300, 100)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	splits := splitsOf(t, s, stream, nil)
	wantSplits(t, splits, parent)
	c := readSplits(t, s, stream, splits)
	wantIDs(t, c.ids(t, stream, false), 0, 400)
	wantSplits(t, c.splits[stream][parent].added, c1, c2)
	c.wantAfter(t, stream, parent, c1)
	c.wantAfter(t, stream, parent, c2)
	var st shardState
	if err := json.Unmarshal(c.splits[stream][parent].state, &st); err != nil || !st.Done {
		t.Errorf("the parent's state %s: %v", c.splits[stream][parent].state, err)
	}

	// From a position partway through the parent.
	rows := c.splits[stream][parent].rows
	mid := rows[len(rows)/2][sequenceColumn].(string)
	midway := readSplits(t, s, stream, []source.Split{{ID: parent, State: encodeShard(shardState{After: mid, Since: time.Now()})}})
	if n := midway.total(stream); n != 100+len(rows)-len(rows)/2-1 {
		t.Errorf("resuming partway through the parent read %d rows, of %d in it", n, len(rows))
	}
	wantSplits(t, midway.splits[stream][parent].added, c1, c2)
	midway.wantAfter(t, stream, parent, c1)

	next := splitsOf(t, s, stream, c.states(stream, false))
	wantSplits(t, next, c1, c2)
	putJSON(t, stream, 400, 20)
	second := readSplits(t, s, stream, next)
	wantIDs(t, second.ids(t, stream, false), 400, 420)
	if second.has(stream, parent) {
		t.Error("the second read read the parent")
	}
	third := splitsOf(t, s, stream, second.states(stream, false))
	wantSplits(t, third, c1, c2)
	if n := readSplits(t, s, stream, third).total(stream); n != 0 {
		t.Errorf("the third read read %d rows", n)
	}
}

// A merged child waits for both parents: it joins with the last one's
// completion, however the read got there.
func TestSourceMergedSplitWaitsForBothParents(t *testing.T) {
	stream := uniq("merged")
	createStream(t, stream, 2)
	putJSON(t, stream, 0, 200)
	const a, b, m = "shardId-000000000000", "shardId-000000000001", "shardId-000000000002"
	merge(t, stream, a, b)
	putJSON(t, stream, 200, 100)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	splits := splitsOf(t, s, stream, nil)
	wantSplits(t, splits, a, b)
	first := readSplits(t, s, stream, splits)
	wantIDs(t, first.ids(t, stream, false), 0, 300)
	first.wantAfter(t, stream, a, m)
	first.wantAfter(t, stream, b, m)
	added := append(slices.Clone(first.splits[stream][a].added), first.splits[stream][b].added...)
	wantSplits(t, added, m)

	bRows := first.splits[stream][b].rows
	midB := bRows[len(bRows)/2][sequenceColumn].(string)
	midState := encodeShard(shardState{After: midB, Since: time.Now()})
	rest := len(bRows) - len(bRows)/2 - 1
	mRows := len(first.splits[stream][m].rows)
	done := first.splits[stream][a].state

	t.Run("one parent completed in an earlier read", func(t *testing.T) {
		ended := []source.Split{{ID: a, State: done}, {ID: b, State: midState}}
		next := splitsOf(t, s, stream, ended)
		wantSplits(t, next, a, b)
		c := readSplits(t, s, stream, next)
		if n := len(c.splits[stream][a].rows); n != 0 {
			t.Errorf("the done parent read %d rows", n)
		}
		if n := c.total(stream); n != rest+mRows {
			t.Errorf("read %d rows, want %d", n, rest+mRows)
		}
		wantSplits(t, c.splits[stream][b].added, m)
		c.wantAfter(t, stream, b, m)
		wantSplits(t, splitsOf(t, s, stream, c.states(stream, false)), m)
	})

	t.Run("a retry of a bounded read leaves the child to the next read", func(t *testing.T) {
		c := readSplits(t, s, stream, []source.Split{{ID: b, State: midState}})
		if n := c.total(stream); n != rest {
			t.Errorf("read %d rows, want %d", n, rest)
		}
		if c.has(stream, m) {
			t.Error("read a child, one parent's end unknown")
		}
		ended := append([]source.Split{{ID: a, State: done}}, c.states(stream, false)...)
		next := splitsOf(t, s, stream, ended)
		wantSplits(t, next, m)
		if n := readSplits(t, s, stream, next).total(stream); n != mRows {
			t.Errorf("the next read read %d rows, want %d", n, mRows)
		}
	})

	t.Run("a retry of a continuous read adds the child", func(t *testing.T) {
		splits := []source.Split{{ID: b, State: midState}}
		c := newSplitCapture(map[string][]source.Split{stream: splits})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		errc := make(chan error, 1)
		go func() {
			errc <- s.Read(ctx, source.ReadRequest{Pipeline: "p", Continuous: true, Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream, Splits: splits}}}, c)
		}()
		await(t, "what's left of the parent and the child", func() bool { return c.total(stream) == rest+mRows })
		cancel()
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Errorf("Read = %v", err)
		}
		if c.has(stream, a) {
			t.Error("read a parent that had completed")
		}
		wantSplits(t, c.record(stream, b).added, m)
		c.wantAfter(t, stream, b, m)
	})

	// A barrier can land between one parent's last resume point and its
	// completion, with the other's completion and the child in the record.
	t.Run("a child in the record before its parent's end", func(t *testing.T) {
		aRows := first.splits[stream][a].rows
		midA := aRows[len(aRows)-1][sequenceColumn].(string)
		child := source.Split{ID: m, State: encodeShard(shardState{Since: time.Now()})}
		c := readSplits(t, s, stream, []source.Split{{ID: a, State: encodeShard(shardState{After: midA, Since: time.Now()})}, child})
		if n := len(c.splits[stream][a].added); n != 0 {
			t.Errorf("added %v, which the read has", c.splits[stream][a].added)
		}
		if n := c.total(stream); n != mRows {
			t.Errorf("read %d rows, want %d", n, mRows)
		}
		c.wantAfter(t, stream, a, m)
	})
}

// A continuous read follows a reshard split by split, and reports on them; a
// restart reads what its states don't cover, of the splits that haven't
// completed, and adds nothing the record has.
func TestSourceSplitsFollowAReshard(t *testing.T) {
	stream := uniq("live")
	createStream(t, stream, 1)
	putJSON(t, stream, 0, 100)
	const parent, c1, c2 = "shardId-000000000000", "shardId-000000000001", "shardId-000000000002"
	s := newSource(t, map[string]any{"streams": []string{stream}})
	splits := splitsOf(t, s, stream, nil)
	c := newSplitCapture(map[string][]source.Split{stream: splits})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Pipeline: "p", Continuous: true, Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream, Splits: splits}}}, c)
	}()
	await(t, "the first records", func() bool { return c.total(stream) == 100 })
	split(t, stream, parent)
	putJSON(t, stream, 100, 100)
	await(t, "the children's records", func() bool { return c.total(stream) == 200 })
	await(t, "a progress report", func() bool {
		c.capture.mu.Lock()
		defer c.capture.mu.Unlock()
		return len(c.capture.progress) > 0
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Read = %v", err)
	}
	wantIDs(t, c.ids(t, stream, false), 0, 200)
	wantSplits(t, c.splits[stream][parent].added, c1, c2)
	c.wantAfter(t, stream, parent, c1)
	c.wantAfter(t, stream, parent, c2)
	if p := c.capture.progress[0]; p.Stream != stream || p.Lag == nil || p.RetentionLeft == nil {
		t.Errorf("progress %+v", p)
	}

	putJSON(t, stream, 200, 50)
	open := c.states(stream, true)
	wantSplits(t, open, c1, c2)
	again := newSplitCapture(map[string][]source.Split{stream: open})
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	go func() {
		done <- s.Read(ctx2, source.ReadRequest{Pipeline: "p", Continuous: true, Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream, Splits: open}}}, again)
	}()
	await(t, "what the states don't cover", func() bool { return again.total(stream) >= 50 })
	cancel2()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Read = %v", err)
	}
	if again.has(stream, parent) {
		t.Error("the restart read the parent")
	}
	got := c.ids(t, stream, true)
	for id, n := range again.ids(t, stream, false) {
		got[id] += n
	}
	wantIDs(t, got, 0, 250)
}

// A shard Kinesis no longer lists is lost only if it could have held records
// the read hadn't reached; otherwise its split completes at once, and the next
// read drops it.
func TestSourceSplitPositionLost(t *testing.T) {
	stream := uniq("retained")
	createStream(t, stream, 1)
	putJSON(t, stream, 0, 10)
	const shard, gone = "shardId-000000000000", "shardId-000000000099"
	s := newSource(t, map[string]any{"streams": []string{stream}})
	first := readSplits(t, s, stream, splitsOf(t, s, stream, nil))
	at := first.splits[stream][shard].state
	old := encodeShard(shardState{After: "1", Since: time.Now().Add(-25 * time.Hour)})
	lost := []source.Split{{ID: shard, State: at}, {ID: gone, State: old}}
	_, err := s.Splits(t.Context(), source.ConfiguredStream{Name: stream}, lost)
	wantCode(t, err, protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST)
	err = s.Read(t.Context(), source.ReadRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream, Splits: lost}}}, newSplitCapture(map[string][]source.Split{stream: lost}))
	wantCode(t, err, protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST)

	recent := []source.Split{{ID: shard, State: at}, {ID: gone, State: encodeShard(shardState{After: "1", Since: time.Now()})}}
	wantSplits(t, splitsOf(t, s, stream, recent), shard)
	c := readSplits(t, s, stream, recent)
	if n := c.total(stream); n != 0 {
		t.Errorf("read %d rows", n)
	}
	var st shardState
	if err := json.Unmarshal(c.splits[stream][gone].state, &st); err != nil || !st.Done {
		t.Errorf("the expired shard's state %s: %v", c.splits[stream][gone].state, err)
	}
	wantSplits(t, splitsOf(t, s, stream, c.states(stream, false)), shard)
}

// startPosition latest skips what the stream held before the first read, and
// a sync that reads nothing keeps its place.
func TestSourceSplitsStartAtLatest(t *testing.T) {
	stream := uniq("latest")
	createStream(t, stream, 2)
	putJSON(t, stream, 0, 20)
	s := newSource(t, map[string]any{"streams": []string{stream}, "startPosition": "latest"})
	c := readSplits(t, s, stream, splitsOf(t, s, stream, nil))
	if n := c.total(stream); n != 0 {
		t.Errorf("read %d rows from before the first read", n)
	}
	putJSON(t, stream, 20, 20)
	wantIDs(t, readSplits(t, s, stream, splitsOf(t, s, stream, c.states(stream, false))).ids(t, stream, false), 20, 40)
}
