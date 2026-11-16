//go:build integration

package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/openetl-io/openetl-connectors/records"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// startPosition timestamp reads what arrived from startTimestamp on, and
// resumes from there.
func TestSourceStartsAtTimestamp(t *testing.T) {
	stream := uniq("at")
	createStream(t, stream, 2)
	putJSON(t, stream, 0, 20)
	time.Sleep(1500 * time.Millisecond)
	from := time.Now().UTC()
	time.Sleep(500 * time.Millisecond)
	putJSON(t, stream, 20, 20)
	s := newSource(t, map[string]any{"streams": []string{stream}, "startPosition": "timestamp", "startTimestamp": from.Format(time.RFC3339Nano)})
	c := read(t, s, stream, nil)
	wantIDs(t, rowIDs(t, c.rows[stream]), 20, 40)
	putJSON(t, stream, 40, 10)
	c = read(t, s, stream, c.complete[stream])
	wantIDs(t, rowIDs(t, c.rows[stream]), 40, 50)
}

// laneCapture counts the splits emitting at once.
type laneCapture struct {
	mu       sync.Mutex
	active   int
	most     int
	rows     int
	complete map[string]bool
}

func (c *laneCapture) Rows(string, []json.RawMessage) error { return errors.New("whole stream") }
func (c *laneCapture) Records(string, arrow.RecordBatch) error {
	return errors.New("no typed batches")
}
func (c *laneCapture) ResumePoint(string, []byte) error { return nil }
func (c *laneCapture) Complete(string, []byte) error    { return nil }
func (c *laneCapture) Split(_, id string) source.SplitEmitter {
	return laneEmit{c: c, id: id}
}

type laneEmit struct {
	c  *laneCapture
	id string
}

func (e laneEmit) Rows(rows []json.RawMessage) error {
	c := e.c
	c.mu.Lock()
	c.active++
	c.most = max(c.most, c.active)
	c.rows += len(rows)
	c.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	c.mu.Lock()
	c.active--
	c.mu.Unlock()
	return nil
}
func (e laneEmit) Records(arrow.RecordBatch) error               { return errors.New("no typed batches") }
func (e laneEmit) ResumePoint(_ []byte, _ ...source.Split) error { return nil }
func (e laneEmit) Complete(_ []byte, _ ...source.Split) error {
	e.c.mu.Lock()
	defer e.c.mu.Unlock()
	e.c.complete[e.id] = true
	return nil
}

// A read's splits fetch and emit no more at once than its lanes.
func TestSourceLanesBoundSplits(t *testing.T) {
	stream := uniq("lanes")
	createStream(t, stream, 4)
	putJSON(t, stream, 0, 400)
	s := newSource(t, map[string]any{"streams": []string{stream}, "polling": map[string]any{"maxRecords": 10}})
	splits := splitsOf(t, s, stream, nil)
	for _, lanes := range []int{1, 4} {
		c := &laneCapture{complete: map[string]bool{}}
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		err := s.Read(ctx, source.ReadRequest{Pipeline: "p", Lanes: lanes, Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream, Splits: splits}}}, c)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if c.rows != 400 || len(c.complete) != 4 {
			t.Fatalf("lanes %d: %d rows, %d splits complete", lanes, c.rows, len(c.complete))
		}
		if lanes == 1 && c.most != 1 || lanes == 4 && c.most < 2 {
			t.Errorf("lanes %d: %d splits emitted at once", lanes, c.most)
		}
	}
}

// A record the Kinesis Producer Library aggregated is read as its user
// records, and the next read resumes after it.
func TestSourceDeaggregates(t *testing.T) {
	stream := uniq("kpl")
	createStream(t, stream, 1)
	cl := client(t)
	var users []userRecord
	for i := range 5 {
		users = append(users, userRecord{partitionKey: "k" + strconv.Itoa(i%2), data: []byte(`{"id":` + strconv.Itoa(i) + `}`)})
	}
	if _, err := cl.PutRecord(t.Context(), &awskinesis.PutRecordInput{StreamName: aws.String(stream), PartitionKey: aws.String("agg"), Data: aggregate([]string{"k0", "k1"}, users)}); err != nil {
		t.Fatal(err)
	}
	putJSON(t, stream, 5, 3)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	c := read(t, s, stream, nil)
	wantIDs(t, rowIDs(t, c.rows[stream]), 0, 8)
	for _, r := range c.rows[stream] {
		id := int(r["id"].(float64))
		if id < 5 && (r[subsequenceColumn] != float64(id) || r[partitionKeyColumn] != "k"+strconv.Itoa(id%2)) {
			t.Errorf("user record %d: %v", id, r)
		}
	}
	putJSON(t, stream, 8, 2)
	c = read(t, s, stream, c.complete[stream])
	wantIDs(t, rowIDs(t, c.rows[stream]), 8, 10)
}

// The pipeline's efo consumer is registered on the first read and
// deregistered at teardown.
func TestSourceConsumerLifecycle(t *testing.T) {
	stream := uniq("efo")
	createStream(t, stream, 1)
	s := newSource(t, map[string]any{"streams": []string{stream}, "consumer": map[string]any{"type": "efo"}})
	cl := client(t)
	sum, err := cl.DescribeStreamSummary(t.Context(), &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(stream)})
	if err != nil {
		t.Fatal(err)
	}
	arn := aws.ToString(sum.StreamDescriptionSummary.StreamARN)
	name, err := s.cfg.consumerName("orders")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		consumer, err := s.register(t.Context(), cl, arn, name)
		if err != nil {
			t.Fatal(err)
		}
		if !regexp.MustCompile(`/consumer/openetl-orders:`).MatchString(consumer) {
			t.Errorf("consumer %s", consumer)
		}
	}
	r := check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "orders", Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream}}}))
	if err := r.Err(); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	got := itemsOf(r)
	if it := wantItem(t, got, "stream."+check.Name(stream)+".consumer", check.Passed); it.Message != name+" is registered and active." {
		t.Errorf("the registered consumer: %+v", it)
	}
	wantItem(t, got, "stream."+check.Name(stream)+".consumer-limit", check.Skipped)
	for range 2 {
		if err := s.Teardown(t.Context(), "orders"); err != nil {
			t.Fatal(err)
		}
	}
	d, err := cl.DescribeStreamConsumer(t.Context(), &awskinesis.DescribeStreamConsumerInput{StreamARN: aws.String(arn), ConsumerName: aws.String(name)})
	if err == nil && d.ConsumerDescription.ConsumerStatus != types.ConsumerStatusDeleting {
		t.Errorf("consumer still %s after teardown", d.ConsumerDescription.ConsumerStatus)
	}
}

// The source's check reaches Kinesis and finds streams to offer, warns when
// streams and streamPattern match none, and fails fast when the endpoint
// doesn't answer.
func TestSourceCheck(t *testing.T) {
	stream := uniq("chk")
	createStream(t, stream, 1)
	r := check.Run(t.Context(), newSource(t, map[string]any{"streams": []string{stream}}).Checks())
	got := itemsOf(r)
	for _, n := range []string{"net.dns", "net.connect", "kinesis.sign-in", "streams.offered"} {
		wantItem(t, got, n, check.Passed)
	}
	if r.Err() != nil {
		t.Errorf("check: %v", r.Err())
	}
	r = check.Run(t.Context(), newSource(t, map[string]any{"streams": []string{stream, "missing-" + stream}}).Checks())
	if it := wantItem(t, itemsOf(r), "streams.offered", check.Warning); it.Field != "streams" || !strings.Contains(it.Message, "missing-"+stream) {
		t.Errorf("a stream that's missing: %+v", it)
	}
	r = check.Run(t.Context(), newSource(t, map[string]any{"streamPattern": "^nothing-matches-" + stream + "$"}).Checks())
	if it := wantItem(t, itemsOf(r), "streams.offered", check.Warning); it.Field != "streamPattern" {
		t.Errorf("field %q", it.Field)
	}
	if r.Err() != nil {
		t.Errorf("a pattern that matches nothing fails: %v", r.Err())
	}
	start := time.Now()
	r = check.Run(t.Context(), newSource(t, map[string]any{"endpoint": "http://127.0.0.1:1"}).Checks())
	got = itemsOf(r)
	if it := wantItem(t, got, "net.connect", check.Failed); it.Field != "endpoint" || it.Failure.Reason != "connection_refused" {
		t.Errorf("unreachable: %+v", it)
	}
	wantItem(t, got, "kinesis.sign-in", check.Skipped)
	wantCode(t, r.Err(), protocolv1.ErrorCode_ERROR_CODE_TRANSIENT)
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("an endpoint that refuses took %s", d)
	}
}

// The source's preflight passes each stream that's there and readable,
// fails one that isn't, or that the pipeline reads in a sync mode Kinesis
// has no use for, and warns when the schedule or startTimestamp outruns
// retention.
func TestSourcePreflight(t *testing.T) {
	stream := uniq("pre")
	createStream(t, stream, 2)
	missing, other := uniq("missing"), uniq("other")
	createStream(t, other, 1)
	s := newSource(t, map[string]any{"streams": []string{stream, missing, other}})
	r := check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "p", Interval: 48 * time.Hour, Streams: []source.ConfiguredStream{
		{Name: stream, SyncMode: source.Stream}, {Name: missing, SyncMode: source.Stream}, {Name: other, SyncMode: source.Incremental},
	}}))
	got := itemsOf(r)
	base := func(n string) string { return "stream." + check.Name(n) }
	for _, what := range []string{"mode", "describe", "shards", "read"} {
		wantItem(t, got, base(stream)+"."+what, check.Passed)
	}
	wantItem(t, got, base(stream)+".schedule", check.Warning)
	if it := wantItem(t, got, base(missing)+".describe", check.Failed); it.Field != "streams" || it.Failure == nil || it.Failure.Fix == "" {
		t.Errorf("missing stream: %+v", it)
	}
	wantItem(t, got, base(other)+".mode", check.Failed)
	wantItem(t, got, base(other)+".describe", check.Skipped)
	wantCode(t, r.Err(), protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID)

	old := time.Now().Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339)
	efo := newSource(t, map[string]any{"streams": []string{stream}, "startPosition": "timestamp", "startTimestamp": old, "consumer": map[string]any{"type": "efo"}})
	r = check.Run(t.Context(), efo.Preflight(source.PreflightRequest{Pipeline: "p", Continuous: true, Streams: []source.ConfiguredStream{{Name: stream, SyncMode: source.Stream}}}))
	got = itemsOf(r)
	wantItem(t, got, "start-timestamp", check.Passed)
	wantItem(t, got, base(stream)+".start", check.Warning)
	if _, ok := got[base(stream)+".schedule"]; ok {
		t.Error("a continuous pipeline's schedule outruns retention")
	}
	wantItem(t, got, base(stream)+".consumer", check.Passed)
	wantItem(t, got, base(stream)+".consumer-limit", check.Passed)
	wantItem(t, got, base(stream)+".subscribe", check.Skipped)
	if r.Err() != nil {
		t.Errorf("preflight: %v", r.Err())
	}

	// Once the pipeline's consumer is one of a stream's 20, it takes no new
	// place. A stream with 20 others may have no room for it: a warning.
	cl := client(t)
	sum, err := cl.DescribeStreamSummary(t.Context(), &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(other)})
	if err != nil {
		t.Fatal(err)
	}
	arn := aws.ToString(sum.StreamDescriptionSummary.StreamARN)
	for i := range consumerLimit - 1 {
		if _, err := cl.RegisterStreamConsumer(t.Context(), &awskinesis.RegisterStreamConsumerInput{StreamARN: aws.String(arn), ConsumerName: aws.String("c" + strconv.Itoa(i))}); err != nil {
			t.Fatal(err)
		}
	}
	preflightOther := func() map[string]check.Item {
		return itemsOf(check.Run(t.Context(), efo.Preflight(source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: other, SyncMode: source.Stream}}})))
	}
	wantItem(t, preflightOther(), base(other)+".consumer-limit", check.Passed)
	name, err := efo.cfg.consumerName("p")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := efo.register(t.Context(), cl, arn, name); err != nil {
		t.Fatal(err)
	}
	got = preflightOther()
	wantItem(t, got, base(other)+".consumer", check.Passed)
	wantItem(t, got, base(other)+".consumer-limit", check.Skipped)
	if err := efo.deregister(t.Context(), cl, arn, name); err != nil {
		t.Fatal(err)
	}
	await(t, name+" to be gone", func() bool {
		_, err := cl.DescribeStreamConsumer(t.Context(), &awskinesis.DescribeStreamConsumerInput{StreamARN: aws.String(arn), ConsumerName: aws.String(name)})
		return apiCode(err) == "ResourceNotFoundException"
	})
	if _, err := cl.RegisterStreamConsumer(t.Context(), &awskinesis.RegisterStreamConsumerInput{StreamARN: aws.String(arn), ConsumerName: aws.String("c-last")}); err != nil {
		t.Fatal(err)
	}
	if it := wantItem(t, preflightOther(), base(other)+".consumer-limit", check.Warning); it.Failure == nil || it.Failure.Reason != "consumer_limit" {
		t.Errorf("full: %+v", it)
	}
}

// A stream the destination creates gets the config's capacity mode, shards
// and retention; an existing one is left as it is.
func TestDestinationCreatesStreams(t *testing.T) {
	ns := uniq("ns")
	createStream(t, ns+".old", 1)
	d := newDest(t, map[string]any{"streamMode": "provisioned", "shards": 3, "retentionHours": 48})
	w := openWriter(t, d, uniq("p"), ns, map[string][]string{"new": {"id"}, "old": {"id"}})
	e := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e, "new", 0, 10)
	write(t, w, e, "old", 0, 10)
	if _, err := w.PreCommit(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	cl := client(t)
	for name, want := range map[string][2]int32{"new": {3, 48}, "old": {1, 24}} {
		sum, err := cl.DescribeStreamSummary(t.Context(), &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(ns + "." + name)})
		if err != nil {
			t.Fatal(err)
		}
		s := sum.StreamDescriptionSummary
		if aws.ToInt32(s.OpenShardCount) != want[0] || aws.ToInt32(s.RetentionPeriodHours) != want[1] {
			t.Errorf("%s: %d shards, %dh retention; want %v", name, aws.ToInt32(s.OpenShardCount), aws.ToInt32(s.RetentionPeriodHours), want)
		}
		if n := len(everything(t, ns+"."+name)); n != 10 {
			t.Errorf("%s: %d records", name, n)
		}
		tags, err := cl.ListTagsForStream(t.Context(), &awskinesis.ListTagsForStreamInput{StreamName: aws.String(ns + "." + name)})
		if err != nil {
			t.Fatal(err)
		}
		for _, tag := range tags.Tags {
			if aws.ToString(tag.Key) == setupTag {
				t.Errorf("%s keeps its setup tag", name)
			}
		}
	}
}

// schemaOf is a stream's declared columns, with its table.
func schemaOf(table string) *arrow.Schema {
	md := arrow.NewMetadata([]string{records.TableKey}, []string{table})
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}}, &md)
}

// The destination's preflight names each stream's Kinesis stream as the
// writer does, says how a missing one will be created, warns of an
// existing one that differs from the config, and fails a write mode other
// than append and partition keys the stream can't make.
func TestDestinationPreflight(t *testing.T) {
	ns := uniq("ns")
	createStream(t, ns+".old", 1)
	d := newDest(t, map[string]any{"streamMode": "provisioned", "shards": 3, "retentionHours": 48})
	req := destination.PreflightRequest{Pipeline: "p", Namespace: ns,
		Streams: []destination.Stream{{Name: "public.old", Mode: destination.Append, PrimaryKey: []string{"id"}}, {Name: "public.new", Mode: destination.Append}},
		Schemas: map[string]*arrow.Schema{"public.old": schemaOf("old"), "public.new": schemaOf("new")},
	}
	r := check.Run(t.Context(), d.Preflight(req))
	got := itemsOf(r)
	if it := wantItem(t, got, "stream.public-old.target", check.Warning); !strings.Contains(it.Message, "1 open shards, not 3") {
		t.Errorf("old: %s", it.Message)
	}
	if it := wantItem(t, got, "stream.public-new.target", check.Skipped); !strings.Contains(it.Message, "provisioned with 3 shards, with 48 hours of retention") || !strings.Contains(it.Message, "kinesis:CreateStream") || !strings.Contains(it.Message, "stream/"+ns+".new") {
		t.Errorf("new: %+v", it)
	}
	wantItem(t, got, "stream.public-old.keys", check.Passed)
	wantItem(t, got, "stream.public-new.keys", check.Passed)
	wantItem(t, got, "put", check.Skipped)
	if r.Err() != nil {
		t.Errorf("preflight: %v", r.Err())
	}

	off := newDest(t, map[string]any{"createStreams": false, "partitionKey": map[string]any{"mode": "fields", "fields": []string{"nope"}}})
	req.Streams[0].Mode = destination.Merge
	r = check.Run(t.Context(), off.Preflight(req))
	got = itemsOf(r)
	wantItem(t, got, "stream.public-old.mode", check.Failed)
	wantItem(t, got, "stream.public-old.keys", check.Failed)
	wantItem(t, got, "stream.public-old.target", check.Warning)
	wantItem(t, got, "stream.public-new.target", check.Failed)
	wantCode(t, r.Err(), protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID)
}

// partitionKey.mode fields keys records by those columns; random by random
// keys.
func TestDestinationPartitionModes(t *testing.T) {
	for _, c := range []struct {
		mode string
		want func(i int, key string) bool
	}{
		{"fields", func(i int, key string) bool { return key == "n"+strconv.Itoa(i) }},
		{"random", func(_ int, key string) bool { return regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(key) }},
	} {
		pk := map[string]any{"mode": c.mode}
		if c.mode == "fields" {
			pk["fields"] = []string{"name"}
		}
		d := newDest(t, map[string]any{"partitionKey": pk, "ordering": "none"})
		ns := uniq("ns")
		w := openWriter(t, d, uniq("p"), ns, map[string][]string{"t": {"id"}})
		e := destination.Epoch{Generation: 1, N: 1}
		write(t, w, e, "t", 0, 20)
		if _, err := w.PreCommit(t.Context(), e); err != nil {
			t.Fatal(err)
		}
		recs := everything(t, ns+".t")
		if len(recs) != 20 {
			t.Fatalf("%s: %d records", c.mode, len(recs))
		}
		keys := map[string]bool{}
		for _, r := range recs {
			var row struct {
				ID int `json:"id"`
			}
			if err := json.Unmarshal(r.Data, &row); err != nil {
				t.Fatal(err)
			}
			k := aws.ToString(r.PartitionKey)
			if !c.want(row.ID, k) {
				t.Errorf("%s: row %d has key %q", c.mode, row.ID, k)
			}
			keys[k] = true
		}
		if len(keys) != 20 {
			t.Errorf("%s: %d distinct keys", c.mode, len(keys))
		}
	}
	d := newDest(t, map[string]any{"partitionKey": map[string]any{"mode": "fields", "fields": []string{"nope"}}})
	w := openWriter(t, d, uniq("p"), uniq("ns"), map[string][]string{"t": {"id"}})
	b := rows(t, "t", 0, 1)
	defer b.Release()
	wantCode(t, w.Write(t.Context(), destination.Epoch{Generation: 1, N: 1}, "t", b), protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID)
}
