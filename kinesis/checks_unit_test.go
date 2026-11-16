package kinesis

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl-connectors/records"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

const fakeAccount = "arn:aws:kinesis:us-east-1:123456789012:"

// fakeKinesis answers Kinesis's JSON API with handle, and keeps the
// operations it was called with and their inputs.
type fakeKinesis struct {
	mu     sync.Mutex
	calls  []string
	inputs map[string][]map[string]any
	// A status and a body; a nil body is {}.
	handle func(op string, in map[string]any) (int, any)
}

func newFake(t *testing.T, handle func(op string, in map[string]any) (int, any)) (*fakeKinesis, Connection) {
	t.Helper()
	f := &fakeKinesis{handle: handle, inputs: map[string][]map[string]any{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "Kinesis_20131202.")
		var in map[string]any
		_ = json.NewDecoder(r.Body).Decode(&in)
		f.mu.Lock()
		f.calls = append(f.calls, op)
		f.inputs[op] = append(f.inputs[op], in)
		f.mu.Unlock()
		status, body := f.handle(op, in)
		if body == nil {
			body = map[string]any{}
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)
	return f, Connection{
		Region: "us-east-1", Endpoint: srv.URL, AccessKeyID: sdk.NewSecret("AKIDFAKE"), SecretAccessKey: sdk.NewSecret("fake-secret"),
		Config: records.Config{Format: records.JSON},
		Retry:  Retry{InitialBackoff: Duration(time.Millisecond), MaxBackoff: Duration(time.Millisecond), MaxAttempts: 1, MaxElapsed: Duration(time.Second)},
	}
}

func (f *fakeKinesis) called(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.inputs[op])
}

func apiError(code, message string) (int, any) {
	return http.StatusBadRequest, map[string]any{"__type": code, "message": message}
}

func denial(action, resource string) (int, any) {
	return apiError("AccessDeniedException", "User: arn:aws:iam::123456789012:user/etl is not authorized to perform: "+action+" on resource: "+resource+" because no identity-based policy allows the "+action+" action")
}

func summary(name string, retention, consumers int) map[string]any {
	return map[string]any{"StreamDescriptionSummary": map[string]any{
		"StreamName": name, "StreamARN": fakeAccount + "stream/" + name, "StreamStatus": "ACTIVE",
		"RetentionPeriodHours": retention, "OpenShardCount": 1, "ConsumerCount": consumers, "EncryptionType": "NONE",
		"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"}, "StreamCreationTimestamp": 1.7e9, "EnhancedMonitoring": []any{},
	}}
}

func itemsOf(r check.Report) map[string]check.Item {
	out := map[string]check.Item{}
	for _, it := range r.Items {
		out[it.Name] = it.Rendered()
	}
	return out
}

func wantItem(t *testing.T, got map[string]check.Item, name string, status check.Status) check.Item {
	t.Helper()
	it, ok := got[name]
	if !ok {
		names := make([]string, 0, len(got))
		for n := range got {
			names = append(names, n)
		}
		slices.Sort(names)
		t.Errorf("no item %s among %v", name, names)
		return it
	}
	if it.Status != status {
		t.Errorf("%s: %s (%s), want %s", name, it.Status, it.Message, status)
	}
	return it
}

func failureOf(t *testing.T, it check.Item) *check.Failure {
	t.Helper()
	if it.Failure == nil {
		t.Fatalf("%s has no failure: %s", it.Name, it.Message)
	}
	return it.Failure
}

// Every list the connectors make is one the SDK runs: names valid and apart,
// even for streams whose names fit a check's name the same way.
func TestChecksAreValid(t *testing.T) {
	src, err := NewSource([]byte(`{"endpoint": "http://127.0.0.1:4568", "format": "avro", "schemaRegistryUrl": "http://u:p@registry:8081",
		"startPosition": "timestamp", "startTimestamp": "2026-01-01T00:00:00Z", "consumer": {"type": "efo"}}`))
	if err != nil {
		t.Fatal(err)
	}
	checks := src.Checks()
	if err := check.Validate(checks); err != nil {
		t.Error(err)
	}
	var names []string
	for _, c := range checks {
		names = append(names, c.Name)
	}
	if want := []string{"net.dns", "net.connect", "kinesis.sign-in", "registry.reach", "streams.offered"}; !slices.Equal(names, want) {
		t.Errorf("checks %v, want %v", names, want)
	}
	if c := checks[3]; c.Params["registry"] != "registry:8081" {
		t.Errorf("the registry's param %q holds more than its host", c.Params["registry"])
	}
	streams := []source.ConfiguredStream{{Name: "a.b"}, {Name: "a_b"}, {Name: "A-B"}, {Name: "a.b"}, {Name: "other"}}
	pre := src.Preflight(source.PreflightRequest{Pipeline: "p", Interval: time.Hour, Streams: streams})
	if err := check.Validate(pre); err != nil {
		t.Fatal(err)
	}
	describes := 0
	for _, c := range pre {
		if strings.HasSuffix(c.Name, ".describe") {
			describes++
		}
	}
	if describes != 4 {
		t.Errorf("%d streams described, want 4", describes)
	}
	if !slices.ContainsFunc(pre, func(c check.Check) bool { return c.Name == "stream.other.describe" }) {
		t.Error("a stream whose name no other shares has a hash in its checks' names")
	}

	plain, err := NewSource([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if c := plain.Checks(); len(c) != 2 || c[0].Name != "kinesis.sign-in" || c[0].Needs != nil {
		t.Errorf("without an endpoint or a registry: %+v", c)
	}

	d, err := NewDestination([]byte(`{"retentionHours": 48, "partitionKey": {"mode": "fields", "fields": ["name"]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Validate(d.Checks()); err != nil {
		t.Error(err)
	}
	req := destination.PreflightRequest{Pipeline: "p", Namespace: "ns",
		Streams: []destination.Stream{{Name: "a.b", Mode: destination.Append}, {Name: "a_b", Mode: destination.Merge}},
		Schemas: map[string]*arrow.Schema{"a.b": schemaWith("a_b"), "a_b": schemaWith("ab")},
	}
	if err := check.Validate(d.Preflight(req)); err != nil {
		t.Error(err)
	}
}

func schemaWith(table string) *arrow.Schema {
	md := arrow.NewMetadata([]string{records.TableKey}, []string{table})
	return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}}, &md)
}

// Signing in fails as the reason AWS's error has, with the action and
// resource it names; a destination needn't list streams.
func TestSignIn(t *testing.T) {
	var answer func() (int, any)
	_, c := newFake(t, func(op string, _ map[string]any) (int, any) {
		if op != "ListStreams" {
			return apiError("UnknownOperationException", op)
		}
		return answer()
	})
	run := func(destination bool) check.Item {
		k := newConn(&c)
		r := check.Run(t.Context(), []check.Check{k.signIn(destination)})
		return itemsOf(r)["kinesis.sign-in"]
	}

	answer = func() (int, any) {
		return http.StatusOK, map[string]any{"StreamNames": []string{"s"}, "HasMoreStreams": false}
	}
	if it := run(false); it.Status != check.Passed || it.Message != "Kinesis in us-east-1 accepts the credentials." || it.Field != "accessKeyId" {
		t.Errorf("signed in: %+v", it)
	}

	answer = func() (int, any) { return denial("kinesis:ListStreams", fakeAccount+"stream/*") }
	it := run(false)
	f := failureOf(t, it)
	if it.Status != check.Failed || f.Reason != "privilege_missing" || f.Code != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED ||
		it.Params["privilege"] != "kinesis:ListStreams" || it.Params["object"] != fakeAccount+"stream/*" || it.Params["principal"] != "arn:aws:iam::123456789012:user/etl" {
		t.Errorf("denied: %+v %+v", it, f)
	}
	if !strings.Contains(f.Fix, "Allow kinesis:ListStreams on "+fakeAccount+"stream/*") {
		t.Errorf("fix %q", f.Fix)
	}
	if it := run(true); it.Status != check.Passed || !strings.Contains(it.Message, "which the destination doesn't need") {
		t.Errorf("a destination denied listing: %+v", it)
	}

	answer = func() (int, any) {
		return apiError("UnrecognizedClientException", "The security token included in the request is invalid.")
	}
	if f := failureOf(t, run(true)); f.Reason != "credentials_rejected" || f.Code != protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID || !strings.Contains(f.Fix, "accessKeyId") {
		t.Errorf("rejected: %+v", f)
	}

	answer = func() (int, any) { return apiError("LimitExceededException", "Rate exceeded") }
	if f := failureOf(t, run(false)); f.Reason != "rate_limited" {
		t.Errorf("throttled: %+v", f)
	}

	none := Connection{Region: "", Endpoint: c.Endpoint, AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey}
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/none")
	k := newConn(&none)
	r := check.Run(t.Context(), []check.Check{k.signIn(false)})
	if f := failureOf(t, itemsOf(r)["kinesis.sign-in"]); f.Reason != "region_missing" {
		t.Errorf("no region: %+v", f)
	}
}

// fakeStreams answers a source's or a destination's preflight: streams by
// name, with their retention and consumer counts; denied's describe is
// refused; consumers registered by name.
type fakeStreams struct {
	retention, consumers map[string]int
	registered           map[string]string
	tags                 map[string]map[string]string
}

func (s *fakeStreams) handle(op string, in map[string]any) (int, any) {
	name, _ := in["StreamName"].(string)
	switch op {
	case "DescribeStreamSummary":
		if name == "denied" {
			return denial("kinesis:DescribeStreamSummary", fakeAccount+"stream/denied")
		}
		h, ok := s.retention[name]
		if !ok {
			return apiError("ResourceNotFoundException", "Stream "+name+" under account 123456789012 not found.")
		}
		return http.StatusOK, summary(name, h, s.consumers[name])
	case "ListShards":
		return http.StatusOK, map[string]any{"Shards": []any{map[string]any{"ShardId": "shardId-000000000000",
			"HashKeyRange":        map[string]any{"StartingHashKey": "0", "EndingHashKey": "1"},
			"SequenceNumberRange": map[string]any{"StartingSequenceNumber": "1"}}}}
	case "GetShardIterator":
		return http.StatusOK, map[string]any{"ShardIterator": "it"}
	case "GetRecords":
		return http.StatusOK, map[string]any{"Records": []any{}, "MillisBehindLatest": 0, "NextShardIterator": "it"}
	case "DescribeStreamConsumer":
		arn, _ := in["StreamARN"].(string)
		c, _ := in["ConsumerName"].(string)
		status, ok := s.registered[c]
		if !ok {
			return apiError("ResourceNotFoundException", "Consumer "+c+" not found.")
		}
		return http.StatusOK, map[string]any{"ConsumerDescription": map[string]any{"ConsumerName": c, "ConsumerARN": arn + "/consumer/" + c + ":1",
			"ConsumerStatus": status, "ConsumerCreationTimestamp": 1.7e9, "StreamARN": arn}}
	case "ListTagsForStream":
		var tags []any
		for k, v := range s.tags[name] {
			tags = append(tags, map[string]any{"Key": k, "Value": v})
		}
		return http.StatusOK, map[string]any{"Tags": tags, "HasMoreTags": false}
	}
	return apiError("UnknownOperationException", op)
}

// The source's preflight passes a stream that's there and readable, fails
// one that isn't, a sync mode other than stream and a start in the future,
// warns of retention the schedule outruns and of a stream full of
// consumers, and skips what registering a consumer would change.
func TestSourcePreflightItems(t *testing.T) {
	fs := &fakeStreams{retention: map[string]int{"s": 24, "full": 24}, consumers: map[string]int{"full": 20}, registered: map[string]string{}}
	_, c := newFake(t, fs.handle)
	s := &Source{cfg: SourceConfig{Connection: c, StartPosition: startEarliest, Consumer: Consumer{Type: consumerEFO}}}
	req := source.PreflightRequest{Pipeline: "orders", Interval: 48 * time.Hour, Streams: []source.ConfiguredStream{
		{Name: "s", SyncMode: source.Stream}, {Name: "missing", SyncMode: source.Stream}, {Name: "denied", SyncMode: source.Stream},
		{Name: "inc", SyncMode: source.Incremental}, {Name: "full", SyncMode: source.Stream},
	}}
	r := check.Run(t.Context(), s.Preflight(req))
	got := itemsOf(r)
	for _, n := range []string{"mode", "describe", "shards", "read", "consumer"} {
		wantItem(t, got, "stream.s."+n, check.Passed)
	}
	if it := got["stream.s.describe"]; it.Message != "s is active, with 1 open shards, 24 hours of retention and no encryption." {
		t.Errorf("describe: %q", it.Message)
	}
	if it := wantItem(t, got, "stream.s.schedule", check.Warning); !strings.Contains(it.Message, "can go 48h between syncs, longer than s's retention of 24h") {
		t.Errorf("schedule: %q", it.Message)
	}
	if it := got["stream.s.consumer"]; it.Message != "openetl-orders isn't registered yet: the first read registers it." {
		t.Errorf("consumer: %q", it.Message)
	}
	wantItem(t, got, "stream.s.consumer-limit", check.Passed)
	if it := wantItem(t, got, "stream.s.subscribe", check.Skipped); !strings.Contains(it.Message, "kinesis:RegisterStreamConsumer on "+fakeAccount+"stream/s,") ||
		!strings.Contains(it.Message, "kinesis:SubscribeToShard on "+fakeAccount+"stream/s/consumer/openetl-orders:*") {
		t.Errorf("subscribe: %q", it.Message)
	}

	it := wantItem(t, got, "stream.missing.describe", check.Failed)
	if f := failureOf(t, it); f.Reason != "not_found" || it.Message != "missing doesn't exist in us-east-1." || f.Fix != "Create missing in us-east-1, or take it out of the pipeline." || it.Field != "streams" {
		t.Errorf("missing: %+v %+v", it, f)
	}
	wantItem(t, got, "stream.missing.shards", check.Skipped)
	it = wantItem(t, got, "stream.denied.describe", check.Failed)
	if it.Params["privilege"] != "kinesis:DescribeStreamSummary" || it.Params["object"] != fakeAccount+"stream/denied" {
		t.Errorf("denied: %+v", it.Params)
	}
	wantItem(t, got, "stream.inc.mode", check.Failed)
	wantItem(t, got, "stream.inc.describe", check.Skipped)
	if it := wantItem(t, got, "stream.full.consumer-limit", check.Warning); !strings.Contains(it.Message, "full has 20 enhanced fan-out consumers") {
		t.Errorf("full: %q", it.Message)
	}
	if code := codeOf(r.Err()); code != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID {
		t.Errorf("the preflight's error %v isn't CONFIG_INVALID", r.Err())
	}

	// A consumer that's registered takes no new place; one the preflight
	// can't name is a warning.
	fs.registered["openetl-orders"] = "ACTIVE"
	r = check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "orders", Streams: []source.ConfiguredStream{{Name: "full", SyncMode: source.Stream}}}))
	got = itemsOf(r)
	wantItem(t, got, "stream.full.consumer", check.Passed)
	wantItem(t, got, "stream.full.consumer-limit", check.Skipped)
	r = check.Run(t.Context(), s.Preflight(source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "s", SyncMode: source.Stream}}}))
	got = itemsOf(r)
	if it := wantItem(t, got, "stream.s.consumer", check.Warning); it.Title != "Find the pipeline's consumer of s" {
		t.Errorf("unnamed: %+v", it)
	}
	wantItem(t, got, "stream.s.consumer-limit", check.Passed)
	if !r.OK() {
		t.Errorf("warnings fail the preflight: %v", r.Err())
	}

	// startTimestamp in the future fails; one older than retention warns.
	for _, at := range []struct {
		start      time.Time
		item, each check.Status
	}{{time.Now().Add(time.Hour), check.Failed, check.Skipped}, {time.Now().Add(-72 * time.Hour), check.Passed, check.Warning}, {time.Now().Add(-time.Hour), check.Passed, check.Passed}} {
		s.cfg.StartPosition, s.cfg.StartTimestamp, s.cfg.Consumer.Type = startTimestamp, &at.start, consumerPolling
		got := itemsOf(check.Run(t.Context(), s.Preflight(source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "s", SyncMode: source.Stream}}})))
		if it := wantItem(t, got, "start-timestamp", at.item); at.item == check.Failed && (it.Field != "startTimestamp" || it.Failure.Reason != "start_in_future") {
			t.Errorf("future: %+v", it)
		}
		wantItem(t, got, "stream.s.start", at.each)
	}
}

// The destination's preflight names each target as the writer does, skips
// creating a missing one with the actions it takes, warns of an existing one
// that differs, and fails
// a write mode other than append and partition keys the stream can't make.
func TestDestinationPreflightItems(t *testing.T) {
	fs := &fakeStreams{retention: map[string]int{"ns.old": 24}}
	_, c := newFake(t, fs.handle)
	d := &Destination{cfg: DestinationConfig{Connection: c, StreamMode: streamOnDemand, RetentionHours: 48, PartitionKey: PartitionKey{Mode: partitionByFields, Fields: []string{"name"}}}}
	req := destination.PreflightRequest{Pipeline: "p", Namespace: "ns",
		Streams: []destination.Stream{{Name: "public.old", Mode: destination.Append}, {Name: "public.new", Mode: destination.Merge}},
		Schemas: map[string]*arrow.Schema{"public.old": schemaWith("old"),
			"public.new": arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, func() *arrow.Metadata { m := arrow.NewMetadata([]string{records.TableKey}, []string{"new"}); return &m }())},
	}
	r := check.Run(t.Context(), d.Preflight(req))
	got := itemsOf(r)
	if it := wantItem(t, got, "stream.public-old.target", check.Warning); !strings.Contains(it.Message, "ns.old differs from the config: 24h of retention, not 48h.") {
		t.Errorf("old: %q", it.Message)
	}
	it := wantItem(t, got, "stream.public-new.target", check.Skipped)
	for _, want := range []string{"on-demand, with 48 hours of retention, unencrypted", "kinesis:IncreaseStreamRetentionPeriod", "on " + fakeAccount + "stream/ns.new."} {
		if !strings.Contains(it.Message, want) {
			t.Errorf("new: %q lacks %q", it.Message, want)
		}
	}
	wantItem(t, got, "stream.public-new.mode", check.Failed)
	if it := wantItem(t, got, "stream.public-new.keys", check.Failed); it.Field != "partitionKey.fields" || it.Failure.Reason != "partition_key_unusable" {
		t.Errorf("keys: %+v", it)
	}
	wantItem(t, got, "stream.public-old.keys", check.Passed)
	if it := wantItem(t, got, "put", check.Skipped); !strings.Contains(it.Message, "kinesis:PutRecords on "+fakeAccount+"stream/ns.*") {
		t.Errorf("put: %q", it.Message)
	}

	off := false
	d.cfg.CreateStreams = &off
	got = itemsOf(check.Run(t.Context(), d.Preflight(req)))
	if it := wantItem(t, got, "stream.public-new.target", check.Failed); it.Field != "createStreams" || it.Message != "ns.new doesn't exist in us-east-1, and createStreams is off." {
		t.Errorf("createStreams off: %+v", it)
	}
}

func TestHuman(t *testing.T) {
	for d, want := range map[time.Duration]string{48 * time.Hour: "48h", 90 * time.Minute: "1h30m", 90 * time.Second: "1m30s", 30 * time.Minute: "30m", 10 * time.Second: "10s"} {
		if got := human(d); got != want {
			t.Errorf("%s: %q", d, got)
		}
	}
}
