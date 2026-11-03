//go:build integration

package kinesis

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/google/uuid"

	"github.com/unsuitable001/openetl-connectors/records"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
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
