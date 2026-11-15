//go:build integration

package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/preflight"
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
	var r preflight.Report
	s.Preflight(t.Context(), &r)
	if err := r.Err(); err != nil {
		t.Fatalf("pre-flight: %v", err)
	}
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

func items(r *preflight.Report) map[string]preflight.Item {
	out := map[string]preflight.Item{}
	for _, it := range r.Items() {
		out[it.Name] = it
	}
	return out
}

func wantItem(t *testing.T, got map[string]preflight.Item, name string, status preflight.Status) preflight.Item {
	t.Helper()
	it, ok := got[name]
	if !ok {
		t.Errorf("no item %s", name)
		return it
	}
	if it.Status != status {
		t.Errorf("%s: %s (%s), want %s", name, it.Status, it.Message, status)
	}
	return it
}

// The source's pre-flight passes each stream that's there, and fails one
// that isn't, naming streams.
func TestSourcePreflight(t *testing.T) {
	stream := uniq("pre")
	createStream(t, stream, 2)
	missing := uniq("missing")
	s := newSource(t, map[string]any{"streams": []string{stream, missing}})
	var r preflight.Report
	s.Preflight(t.Context(), &r)
	got := items(&r)
	wantItem(t, got, "connect", preflight.Passed)
	for _, what := range []string{"describe", "status", "shards", "read"} {
		wantItem(t, got, "stream."+stream+"."+what, preflight.Passed)
	}
	it := wantItem(t, got, "stream."+missing+".describe", preflight.Failed)
	if it.Field != "streams" || it.Fix == "" {
		t.Errorf("missing stream: field %q, fix %q", it.Field, it.Fix)
	}
	wantCode(t, r.Err(), protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID)

	r = preflight.Report{}
	newSource(t, map[string]any{"streamPattern": "^nothing-matches-" + stream + "$"}).Preflight(t.Context(), &r)
	wantItem(t, items(&r), "streams", preflight.Warning)
	if r.Err() != nil {
		t.Errorf("a pattern that matches nothing fails: %v", r.Err())
	}

	r = preflight.Report{}
	old := time.Now().Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339)
	newSource(t, map[string]any{"streams": []string{stream}, "startPosition": "timestamp", "startTimestamp": old, "consumer": map[string]any{"type": "efo"}}).Preflight(t.Context(), &r)
	got = items(&r)
	wantItem(t, got, "stream."+stream+".start", preflight.Warning)
	wantItem(t, got, "stream."+stream+".consumers", preflight.Passed)
	wantItem(t, got, "stream."+stream+".subscribe", preflight.Skipped)
}

// A stream the destination creates gets the config's capacity mode, shards
// and retention; an existing one is left as it is, and the pre-flight says
// how it differs.
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
	}
	var r preflight.Report
	d2, err := NewDestination(config(t, map[string]any{"namespace": ns, "streamMode": "provisioned", "shards": 3, "retentionHours": 48}))
	if err != nil {
		t.Fatal(err)
	}
	d2.Preflight(t.Context(), &r)
	got := items(&r)
	wantItem(t, got, "existing", preflight.Passed)
	wantItem(t, got, "stream."+ns+".old.settings", preflight.Warning)
	if _, ok := got["stream."+ns+".new.settings"]; ok {
		t.Errorf("the created stream differs: %+v", got["stream."+ns+".new.settings"])
	}
	wantItem(t, got, "create", preflight.Skipped)
	wantItem(t, got, "put", preflight.Skipped)
	if r.Err() != nil {
		t.Errorf("pre-flight: %v", r.Err())
	}
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
