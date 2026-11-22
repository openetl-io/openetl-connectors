package kinesis

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/configschema"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestSchemasLint(t *testing.T) {
	for name, schema := range map[string][]byte{"source": SourceConfigSchema, "destination": DestinationConfigSchema} {
		t.Run(name, func(t *testing.T) {
			for _, err := range configschema.Lint(schema) {
				t.Error(err)
			}
			form, err := configschema.ReadForm(schema)
			if err != nil {
				t.Fatal(err)
			}
			if u := configschema.Undescribed(form); len(u) > 0 {
				t.Errorf("fields without a description: %v", u)
			}
			for _, f := range form.Fields {
				if f.Group == "" {
					t.Errorf("%s has no x-group", f.Path)
				}
			}
		})
	}
}

// The schemas have every field the configs decode, and nothing else.
func TestSchemasMatchConfigs(t *testing.T) {
	for name, c := range map[string]struct {
		schema []byte
		config any
	}{"source": {SourceConfigSchema, SourceConfig{}}, "destination": {DestinationConfigSchema, DestinationConfig{}}} {
		t.Run(name, func(t *testing.T) {
			form, err := configschema.ReadForm(c.schema)
			if err != nil {
				t.Fatal(err)
			}
			var inSchema []string
			var walk func([]configschema.Field)
			walk = func(fs []configschema.Field) {
				for _, f := range fs {
					inSchema = append(inSchema, f.Path)
					walk(f.Fields)
				}
			}
			walk(form.Fields)
			got := jsonPaths(reflect.TypeOf(c.config), "")
			slices.Sort(inSchema)
			slices.Sort(got)
			if !slices.Equal(inSchema, got) {
				t.Errorf("the schema has %v,\nthe config %v", inSchema, got)
			}
		})
	}
}

func jsonPaths(t reflect.Type, prefix string) []string {
	var out []string
	for i := range t.NumField() {
		f := t.Field(i)
		tag, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if f.Anonymous && tag == "" {
			out = append(out, jsonPaths(ft, prefix)...)
			continue
		}
		if tag == "" || tag == "-" {
			continue
		}
		out = append(out, prefix+tag)
		if ft.Kind() == reflect.Struct && ft != reflect.TypeFor[time.Time]() {
			out = append(out, jsonPaths(ft, prefix+tag+".")...)
		}
	}
	return out
}

func compile(t *testing.T, schema []byte) *jsonschema.Schema {
	t.Helper()
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("schema.json", doc); err != nil {
		t.Fatal(err)
	}
	sch, err := c.Compile("schema.json")
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

func validates(sch *jsonschema.Schema, config string) error {
	v, err := jsonschema.UnmarshalJSON(strings.NewReader(config))
	if err != nil {
		return err
	}
	return sch.Validate(v)
}

func TestSourceConfig(t *testing.T) {
	sch := compile(t, SourceConfigSchema)
	for _, c := range []struct {
		config string
		ok     bool
	}{
		{`{}`, true},
		{`{"startPosition": "latest"}`, true},
		{`{"startPosition": "timestamp", "startTimestamp": "2026-11-01T00:00:00Z"}`, true},
		{`{"startPosition": "timestamp"}`, false},
		{`{"startTimestamp": "2026-11-01T00:00:00Z"}`, false},
		{`{"startPosition": "middle"}`, false},
		{`{"consumer": {"type": "efo", "name": "orders.reader-1"}}`, true},
		{`{"consumer": {"type": "efo", "name": "has space"}}`, false},
		{`{"consumer": {"name": "c"}}`, false},
		{`{"consumer": {"type": "fanout"}}`, false},
		{`{"polling": {"maxRecords": 500, "interval": "500ms", "idleInterval": "2s"}}`, true},
		{`{"polling": {"interval": "100ms"}}`, false},
		{`{"polling": {"maxRecords": 10001}}`, false},
		{`{"polling": {"interval": "2s", "idleInterval": "1s"}}`, false},
		{`{"polling": {"interval": "2s"}}`, true},
		{`{"startPosition": "timestamp", "startTimestamp": "2999-01-01T00:00:00Z"}`, true},
		{`{"polling": {"interval": "soon"}}`, false},
		{`{"retry": {"initialBackoff": "50ms", "maxBackoff": "2s", "maxAttempts": 3, "maxElapsed": "1m"}}`, true},
		{`{"retry": {"initialBackoff": "5s", "maxBackoff": "1s"}}`, false},
		{`{"retry": {"maxAttempts": 101}}`, false},
		{`{"retry": {"maxElapsed": "10ms"}}`, false},
		{`{"deaggregate": false}`, true},
	} {
		_, err := NewSource([]byte(c.config))
		if (err == nil) != c.ok {
			t.Errorf("%s: %v", c.config, err)
		}
		if err != nil && codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID {
			t.Errorf("%s: %v isn't CONFIG_INVALID", c.config, err)
		}
		// The schema accepts what the source does, but for the checks
		// across fields that it leaves to the source.
		if c.ok {
			if err := validates(sch, c.config); err != nil {
				t.Errorf("the schema refuses %s: %v", c.config, err)
			}
		}
	}
	s, err := NewSource([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	p := s.cfg.Polling
	if p.MaxRecords != 10000 || p.Interval != Duration(200*time.Millisecond) || p.IdleInterval != Duration(time.Second) || s.cfg.Consumer.Type != consumerPolling || !s.cfg.deaggregate() {
		t.Errorf("defaults: %+v, %+v", p, s.cfg.Consumer)
	}
	s, err = NewSource([]byte(`{"polling": {"interval": "2s"}}`))
	if err != nil || s.cfg.Polling.IdleInterval != Duration(2*time.Second) {
		t.Errorf("idleInterval without one, after interval 2s: %v, %v", s.cfg.Polling.IdleInterval, err)
	}
	if r := s.cfg.Retry; r.InitialBackoff != defaultInitialBackoff || r.MaxBackoff != defaultMaxBackoff || r.MaxAttempts != defaultMaxAttempts || r.MaxElapsed != defaultMaxElapsed {
		t.Errorf("retry's defaults: %+v", r)
	}
}

// A pipeline's name is its consumer's as it is where it can be; where it
// can't, a hash of it keeps pipelines' consumers apart.
func TestConsumerName(t *testing.T) {
	cfg := SourceConfig{}
	hashed := regexp.MustCompile(`-[0-9a-f]{8}$`)
	for pipeline, want := range map[string]string{
		"orders":                         "openetl-orders",
		"0b0c5a6e-1c4e-4f42-9a54-5e7f1d": "openetl-0b0c5a6e-1c4e-4f42-9a54-5e7f1d",
		strings.Repeat("a", 120):         "openetl-" + strings.Repeat("a", 120),
		"workspaces/w/pipelines/orders":  "openetl-workspaces-w-pipelines-orders-",
		"émoji ok":                       "openetl--moji-ok-",
		strings.Repeat("a", 121):         "openetl-" + strings.Repeat("a", 111) + "-",
		strings.Repeat("a", 200):         "openetl-" + strings.Repeat("a", 111) + "-",
	} {
		got, err := cfg.consumerName(pipeline)
		if err != nil || !validConsumerName(got) || len(got) > 128 {
			t.Errorf("%q: %q, %v", pipeline, got, err)
			continue
		}
		if strings.HasSuffix(want, "-") {
			if !strings.HasPrefix(got, want) || !hashed.MatchString(got) || len(got) != len(want)+8 {
				t.Errorf("%q: %q, want %s and a hash", pipeline, got, want)
			}
		} else if got != want {
			t.Errorf("%q: %q, want %q", pipeline, got, want)
		}
	}
	seen := map[string]string{}
	for _, p := range []string{"a/b", "a-b", "a b", "a.b", strings.Repeat("x", 200) + "1", strings.Repeat("x", 200) + "2"} {
		n, _ := cfg.consumerName(p)
		if other, ok := seen[n]; ok {
			t.Errorf("%q and %q both read with %q", p, other, n)
		}
		seen[n] = p
	}
	if _, err := cfg.consumerName(""); err == nil {
		t.Error("no pipeline and no name: no error")
	}
	cfg.Consumer.Name = "mine"
	if got, _ := cfg.consumerName("p"); got != "mine" {
		t.Errorf("consumer.name: %q", got)
	}
}

func TestDestinationConfig(t *testing.T) {
	sch := compile(t, DestinationConfigSchema)
	for _, c := range []struct {
		config string
		ok     bool
	}{
		{`{}`, true},
		{`{"streamMode": "provisioned", "shards": 4}`, true},
		{`{"streamMode": "provisioned"}`, false},
		{`{"shards": 4}`, false},
		{`{"streamMode": "serverless"}`, false},
		{`{"retentionHours": 168, "kmsKeyId": "alias/aws/kinesis"}`, true},
		{`{"retentionHours": 12}`, false},
		{`{"retentionHours": 9000}`, false},
		{`{"partitionKey": {"mode": "fields", "fields": ["customer_id"]}}`, true},
		{`{"partitionKey": {"mode": "fields"}}`, false},
		{`{"partitionKey": {"mode": "random"}}`, true},
		{`{"partitionKey": {"fields": ["a"]}}`, false},
		{`{"partitionKey": {"mode": "hash"}}`, false},
		{`{"ordering": "none"}`, true},
		{`{"ordering": "strict"}`, false},
		{`{"rateLimit": {"recordsPerSecond": 1000, "bytesPerSecond": 1048576}}`, true},
		{`{"rateLimit": {"recordsPerSecond": -1}}`, false},
	} {
		_, err := NewDestination([]byte(c.config))
		if (err == nil) != c.ok {
			t.Errorf("%s: %v", c.config, err)
		}
		if c.ok {
			if err := validates(sch, c.config); err != nil {
				t.Errorf("the schema refuses %s: %v", c.config, err)
			}
		}
	}
	d, err := NewDestination([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if d.cfg.StreamMode != streamOnDemand || d.cfg.PartitionKey.Mode != partitionByKey || d.cfg.Ordering != orderingKey || d.records != nil || d.bytes != nil {
		t.Errorf("defaults: %+v", d.cfg)
	}
}

func TestBackoff(t *testing.T) {
	r := Retry{InitialBackoff: Duration(100 * time.Millisecond), MaxBackoff: Duration(time.Second)}
	for n, ceil := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 4: 800 * time.Millisecond, 5: time.Second, 100: time.Second} {
		for range 100 {
			if d := r.backoff(n); d <= 0 || d > ceil {
				t.Fatalf("backoff(%d) = %s, over %s", n, d, ceil)
			}
		}
	}
}

func TestThrottleGivesUp(t *testing.T) {
	r := Retry{InitialBackoff: Duration(time.Millisecond), MaxBackoff: Duration(5 * time.Millisecond), MaxElapsed: Duration(50 * time.Millisecond)}
	th := throttle{retry: &r}
	start := time.Now()
	var err error
	for err == nil {
		err = th.wait(t.Context(), errors.New("throttled"))
	}
	if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED {
		t.Fatalf("%v isn't RATE_LIMITED", err)
	}
	if d := time.Since(start); d < 50*time.Millisecond {
		t.Errorf("gave up after %s", d)
	}
	th.ok()
	if err := th.wait(t.Context(), errors.New("throttled")); err != nil {
		t.Errorf("after ok: %v", err)
	}
}

func entries(keys ...string) []entry {
	out := make([]entry, len(keys))
	for i, k := range keys {
		out[i] = entry{key: k, data: []byte{byte(i)}}
	}
	return out
}

func keysOf(es []entry) string {
	var b strings.Builder
	for _, e := range es {
		b.WriteString(e.key)
	}
	return b.String()
}

func TestTake(t *testing.T) {
	w := &writer{d: &Destination{cfg: DestinationConfig{Ordering: orderingKey}}}
	q := &queue{entries: entries("a", "b", "a", "c", "b", "a"), bytes: 12}
	var got []string
	for len(q.entries) > 0 {
		got = append(got, keysOf(w.take(q)))
	}
	if want := []string{"abc", "ab", "a"}; !slices.Equal(got, want) {
		t.Errorf("ordering key: %v, want %v", got, want)
	}
	if q.bytes != 0 {
		t.Errorf("bytes %d", q.bytes)
	}
	w.d.cfg.Ordering = orderingNone
	q = &queue{entries: entries("a", "b", "a", "c", "b", "a")}
	if got := keysOf(w.take(q)); got != "abacba" || len(q.entries) != 0 {
		t.Errorf("ordering none: %s, left %d", got, len(q.entries))
	}
	q = &queue{entries: entries(slices.Repeat([]string{"k", "j"}, maxRequestRecords)...)}
	w.d.cfg.Ordering = orderingKey
	if n := len(w.take(q)); n != 2 {
		t.Errorf("two hot keys: %d in the first request", n)
	}
	w.d.cfg.Ordering = orderingNone
	if n := len(w.take(q)); n != maxRequestRecords {
		t.Errorf("ordering none: %d in a request", n)
	}
}

// fakePutter fails each record whose data is in fail the first time it
// sees it, and keeps what it accepts in order.
type fakePutter struct {
	mu       sync.Mutex
	fail     map[byte]bool
	accepted []entry
	calls    int
	throttle bool
}

func (f *fakePutter) PutRecords(_ context.Context, in *awskinesis.PutRecordsInput, _ ...func(*awskinesis.Options)) (*awskinesis.PutRecordsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.throttle {
		return nil, &fakeAPIError{code: "ProvisionedThroughputExceededException"}
	}
	out := &awskinesis.PutRecordsOutput{Records: make([]types.PutRecordsResultEntry, len(in.Records))}
	failed := int32(0)
	for i, r := range in.Records {
		if b := r.Data[0]; f.fail[b] {
			delete(f.fail, b)
			out.Records[i] = types.PutRecordsResultEntry{ErrorCode: aws.String("ProvisionedThroughputExceededException")}
			failed++
			continue
		}
		f.accepted = append(f.accepted, entry{key: aws.ToString(r.PartitionKey), data: r.Data})
	}
	out.FailedRecordCount = aws.Int32(failed)
	return out, nil
}

type fakeAPIError struct{ code string }

func (e *fakeAPIError) Error() string     { return e.code }
func (e *fakeAPIError) ErrorCode() string { return e.code }
func (e *fakeAPIError) ErrorMessage() string {
	return e.code
}
func (e *fakeAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

func testWriter(ordering string, p putter) *writer {
	d := &Destination{cfg: DestinationConfig{Ordering: ordering}}
	d.cfg.Retry = Retry{InitialBackoff: Duration(time.Millisecond), MaxBackoff: Duration(2 * time.Millisecond), MaxAttempts: 1, MaxElapsed: Duration(100 * time.Millisecond)}
	return &writer{d: d, put: p, pending: map[string]*queue{}}
}

// With ordering key, a record Kinesis fails and that's sent again stays
// ahead of its key's later records; with none, it doesn't.
func TestPartialFailureKeepsKeyOrder(t *testing.T) {
	keys := []string{"a", "b", "a", "a", "b", "c"}
	for _, c := range []struct {
		ordering string
		inOrder  bool
	}{{orderingKey, true}, {orderingNone, false}} {
		f := &fakePutter{fail: map[byte]bool{0: true, 1: true}}
		w := testWriter(c.ordering, f)
		q := &queue{}
		for _, e := range entries(keys...) {
			if err := w.add(t.Context(), "s", q, e); err != nil {
				t.Fatal(err)
			}
		}
		w.pending["s"] = q
		if _, err := w.PreCommit(t.Context(), destination.Epoch{}); err != nil {
			t.Fatal(err)
		}
		if len(f.accepted) != len(keys) {
			t.Fatalf("%s: accepted %d of %d", c.ordering, len(f.accepted), len(keys))
		}
		inOrder := true
		last := map[string]byte{}
		for _, e := range f.accepted {
			if p, ok := last[e.key]; ok && e.data[0] < p {
				inOrder = false
			}
			last[e.key] = e.data[0]
		}
		if inOrder != c.inOrder {
			t.Errorf("ordering %s: in order %v, accepted %v", c.ordering, inOrder, f.accepted)
		}
	}
}

func TestPutGivesUpWhenThrottled(t *testing.T) {
	f := &fakePutter{throttle: true}
	w := testWriter(orderingKey, f)
	err := w.putAll(t.Context(), "s", entries("a"))
	if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED {
		t.Fatalf("%v isn't RATE_LIMITED", err)
	}
	if f.calls < 2 {
		t.Errorf("%d calls", f.calls)
	}
}

// aggregate builds a record the Kinesis Producer Library aggregated.
func aggregate(keys []string, recs []userRecord) []byte {
	var body []byte
	for _, k := range keys {
		body = protowire.AppendTag(body, 1, protowire.BytesType)
		body = protowire.AppendString(body, k)
	}
	for _, r := range recs {
		var sub []byte
		sub = protowire.AppendTag(sub, 1, protowire.VarintType)
		sub = protowire.AppendVarint(sub, uint64(slices.Index(keys, r.partitionKey)))
		sub = protowire.AppendTag(sub, 3, protowire.BytesType)
		sub = protowire.AppendBytes(sub, r.data)
		body = protowire.AppendTag(body, 3, protowire.BytesType)
		body = protowire.AppendBytes(body, sub)
	}
	sum := md5.Sum(body)
	return append(append(append([]byte{}, kplMagic...), body...), sum[:]...)
}

func TestDeaggregate(t *testing.T) {
	users := []userRecord{{"k1", []byte(`{"id":1}`)}, {"k2", []byte(`{"id":2}`)}, {"k1", []byte(`{"id":3}`)}}
	data := aggregate([]string{"k1", "k2"}, users)
	if !aggregated(data) {
		t.Fatal("not aggregated")
	}
	got, err := deaggregate(data)
	if err != nil || !reflect.DeepEqual(got, users) {
		t.Fatalf("%v, %v", got, err)
	}
	broken := slices.Clone(data)
	broken[len(broken)-1] ^= 1
	if aggregated(broken) {
		t.Error("a bad checksum counts as aggregated")
	}
	if aggregated([]byte(`{"id":1}`)) {
		t.Error("JSON counts as aggregated")
	}

	s := &Source{cfg: SourceConfig{}, values: records.NewDecoder(records.JSON, nil)}
	at := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	recs := []types.Record{
		{SequenceNumber: aws.String("1"), PartitionKey: aws.String("agg"), Data: data, ApproximateArrivalTimestamp: &at},
		{SequenceNumber: aws.String("2"), PartitionKey: aws.String("k9"), Data: []byte(`{"id":4}`), ApproximateArrivalTimestamp: &at},
	}
	rows, err := s.rows(t.Context(), "shard-0", recs)
	if err != nil {
		t.Fatal(err)
	}
	var want []map[string]any
	for _, w := range []struct {
		id       float64
		seq, key string
		sub      float64
	}{{1, "1", "k1", 0}, {2, "1", "k2", 1}, {3, "1", "k1", 2}, {4, "2", "k9", 0}} {
		want = append(want, map[string]any{"id": w.id, shardColumn: "shard-0", sequenceColumn: w.seq, subsequenceColumn: w.sub, partitionKeyColumn: w.key, arrivalColumn: "2026-11-15T00:00:00Z"})
	}
	var got2 []map[string]any
	for _, r := range rows {
		var m map[string]any
		if err := json.Unmarshal(r, &m); err != nil {
			t.Fatalf("%s: %v", r, err)
		}
		got2 = append(got2, m)
	}
	if !reflect.DeepEqual(got2, want) {
		t.Errorf("rows\n%v\nwant\n%v", got2, want)
	}

	off := false
	s.cfg.Deaggregate = &off
	if _, err := s.rows(t.Context(), "shard-0", recs[:1]); codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID || !strings.Contains(err.Error(), "deaggregate is off") {
		t.Errorf("deaggregate off: %v", err)
	}
}

// fakeEvents is one subscription: its events, then its end.
type fakeEvents struct {
	ch  chan types.SubscribeToShardEventStream
	err error
}

func (f *fakeEvents) Events() <-chan types.SubscribeToShardEventStream { return f.ch }
func (f *fakeEvents) Err() error                                       { return f.err }
func (f *fakeEvents) Close() error                                     { return nil }

func subscription(err error, events ...types.SubscribeToShardEvent) *fakeEvents {
	ch := make(chan types.SubscribeToShardEventStream, len(events))
	for _, e := range events {
		ch <- &types.SubscribeToShardEventStreamMemberSubscribeToShardEvent{Value: e}
	}
	close(ch)
	return &fakeEvents{ch: ch, err: err}
}

func record(seq string, at time.Time) types.Record {
	return types.Record{SequenceNumber: aws.String(seq), PartitionKey: aws.String("k"), Data: []byte(`{"seq":` + seq + `}`), ApproximateArrivalTimestamp: aws.Time(at)}
}

// unitEmitter keeps a split's rows and states.
type unitEmitter struct {
	mu     sync.Mutex
	rows   []json.RawMessage
	states [][]byte
}

func (e *unitEmitter) Rows(rows []json.RawMessage) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rows = append(e.rows, rows...)
	return nil
}
func (e *unitEmitter) Records(arrow.RecordBatch) error { return errors.New("no batches") }
func (e *unitEmitter) ResumePoint(state []byte, _ ...source.Split) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.states = append(e.states, state)
	return nil
}
func (e *unitEmitter) Complete([]byte, ...source.Split) error { return nil }

// An efo read handles each subscription's events, subscribes again from the
// last continuation when one ends, waits out a subscription still in use,
// and ends with the shard.
func TestSubscribe(t *testing.T) {
	at := time.Now().Add(-time.Minute)
	subs := []struct {
		es  *fakeEvents
		err error
	}{
		{es: subscription(nil,
			types.SubscribeToShardEvent{Records: []types.Record{record("1", at), record("2", at)}, ContinuationSequenceNumber: aws.String("2"), MillisBehindLatest: aws.Int64(5)},
			types.SubscribeToShardEvent{ContinuationSequenceNumber: aws.String("3"), MillisBehindLatest: aws.Int64(0)})},
		{err: &fakeAPIError{code: "ResourceInUseException"}},
		{es: subscription(nil,
			types.SubscribeToShardEvent{Records: []types.Record{record("4", at)}, ContinuationSequenceNumber: aws.String("4"), MillisBehindLatest: aws.Int64(0)},
			types.SubscribeToShardEvent{ChildShards: []types.ChildShard{{ShardId: aws.String("child")}}})},
	}
	var starts []string
	r := &streamRead{name: "s", consumer: "arn:consumer", retention: 24 * time.Hour, splits: map[string]bool{"shard-0": true}, shards: map[string]shardState{}, behind: map[string]time.Duration{}}
	rd := &reading{
		s:     &Source{cfg: SourceConfig{Connection: Connection{Retry: Retry{InitialBackoff: Duration(time.Millisecond), MaxBackoff: Duration(time.Millisecond), MaxElapsed: Duration(time.Minute)}}}, values: records.NewDecoder(records.JSON, nil)},
		lanes: make(chan struct{}, 1),
		start: time.Now(),
	}
	rd.subscribe = func(_ context.Context, in *awskinesis.SubscribeToShardInput) (eventStream, error) {
		p := in.StartingPosition
		starts = append(starts, string(p.Type)+" "+aws.ToString(p.SequenceNumber))
		s := subs[0]
		subs = subs[1:]
		if s.err != nil {
			return nil, s.err
		}
		return s.es, nil
	}
	em := &unitEmitter{}
	sr := &shardRead{rd: rd, r: r, shard: "shard-0", em: em, st: shardState{After: "0"}, th: throttle{retry: &rd.s.cfg.Retry}}
	over, err := sr.subscribe(t.Context())
	if err != nil || !over || !sr.ended {
		t.Fatalf("over %v, ended %v, %v", over, sr.ended, err)
	}
	if want := []string{"AFTER_SEQUENCE_NUMBER 0", "AFTER_SEQUENCE_NUMBER 3", "AFTER_SEQUENCE_NUMBER 3"}; !slices.Equal(starts, want) {
		t.Errorf("subscribed at %v, want %v", starts, want)
	}
	if len(em.rows) != 3 || sr.st.After != "4" {
		t.Errorf("%d rows, after %q", len(em.rows), sr.st.After)
	}
	if len(em.states) == 0 {
		t.Error("no resume points")
	}
	if r.behind["shard-0"] != 0 {
		t.Errorf("behind %s", r.behind["shard-0"])
	}
}

// A subscription that fails for good fails the read.
func TestSubscribeFails(t *testing.T) {
	r := &streamRead{name: "s", consumer: "arn:consumer", shards: map[string]shardState{}, behind: map[string]time.Duration{}}
	rd := &reading{s: &Source{cfg: SourceConfig{Connection: Connection{Retry: Retry{InitialBackoff: Duration(time.Millisecond), MaxBackoff: Duration(time.Millisecond), MaxElapsed: Duration(time.Minute)}}}}, lanes: make(chan struct{}, 1)}
	rd.subscribe = func(context.Context, *awskinesis.SubscribeToShardInput) (eventStream, error) {
		return nil, &fakeAPIError{code: "AccessDeniedException"}
	}
	sr := &shardRead{rd: rd, r: r, shard: "shard-0", em: &unitEmitter{}, th: throttle{retry: &rd.s.cfg.Retry}}
	if _, err := sr.subscribe(t.Context()); codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Fatalf("%v isn't PERMISSION_DENIED", err)
	}
}

// The preflight names streams as the writer does.
func TestTarget(t *testing.T) {
	md := arrow.NewMetadata([]string{records.TableKey}, []string{"orders"})
	withTable := arrow.NewSchema(nil, &md)
	for _, c := range []struct {
		pipeline, config, stream string
		sc                       *arrow.Schema
		want                     string
	}{
		{"", "", "public.orders", withTable, "orders"},
		{"cdc", "", "public.orders", withTable, "cdc.orders"},
		{"cdc", "cfg", "public.orders", withTable, "cdc.orders"},
		{"", "cfg", "public.orders", withTable, "cfg.orders"},
		{"", "", "public.orders", nil, records.AvroName("public.orders")},
	} {
		if got := target(c.pipeline, c.config, tableOf(c.stream, c.sc)); got != c.want {
			t.Errorf("%+v: %s", c, got)
		}
	}
}

func TestKeyColumns(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "name", Type: arrow.BinaryTypes.String}}, nil)
	for _, c := range []struct {
		mode    string
		primary []string
		fields  []string
		hasID   bool
		want    []string
		ok      bool
	}{
		{partitionByKey, []string{"id"}, nil, false, []string{"id"}, true},
		{partitionByKey, nil, nil, true, nil, true},
		{partitionByKey, nil, nil, false, nil, false},
		{partitionByKey, []string{"nope"}, nil, true, nil, false},
		{partitionByFields, nil, []string{"name"}, false, []string{"name"}, true},
		{partitionByFields, nil, []string{"name", "nope"}, true, nil, false},
		{partitionRandom, nil, nil, false, nil, true},
	} {
		got, err := keyColumns(c.mode, sc, c.primary, c.fields, c.hasID)
		if (err == nil) != c.ok || c.ok && !slices.Equal(got, c.want) {
			t.Errorf("%+v: %v, %v", c, got, err)
		}
	}
}
