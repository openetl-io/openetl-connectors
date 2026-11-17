package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/smithy-go"

	"github.com/openetl-io/openetl-connectors/sourcetest"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// unprocessed leaves the first item of each request unprocessed, the first
// times times.
type unprocessed struct {
	times int
	calls int
	got   []string
}

func (u *unprocessed) BatchWriteItem(_ context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	u.calls++
	reqs := in.RequestItems["t"]
	if len(reqs) > 25 {
		return nil, errors.New("more than 25 requests")
	}
	out := &dynamodb.BatchWriteItemOutput{}
	if u.calls <= u.times {
		out.UnprocessedItems = map[string][]types.WriteRequest{"t": reqs[:1]}
		reqs = reqs[1:]
	}
	for _, r := range reqs {
		u.got = append(u.got, stringOf(r.PutRequest.Item["k"]))
	}
	return out, nil
}

func TestWriteBatchesRetriesUnprocessed(t *testing.T) {
	pause := unprocessedPause
	unprocessedPause = func(int) time.Duration { return time.Millisecond }
	t.Cleanup(func() { unprocessedPause = pause })
	var reqs []types.WriteRequest
	for i := range 60 {
		reqs = append(reqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"k": str(strconv.Itoa(i))}}})
	}
	u := &unprocessed{times: 4}
	if err := writeBatches(t.Context(), u, "t", reqs); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(u.got, func(a, b string) int { x, _ := strconv.Atoi(a); y, _ := strconv.Atoi(b); return x - y })
	if len(u.got) != 60 || u.got[0] != "0" || u.got[59] != "59" || slices.Compact(slices.Clone(u.got))[59] != "59" {
		t.Errorf("wrote %d items, want each of 60 once: %v", len(u.got), u.got)
	}
	if u.calls != 3+4 {
		t.Errorf("%d calls, want 3 batches and 4 retries", u.calls)
	}
}

func TestUnprocessedPause(t *testing.T) {
	if unprocessedPause(0) != 50*time.Millisecond || unprocessedPause(3) != 400*time.Millisecond || unprocessedPause(100) != 5*time.Second {
		t.Errorf("pauses %v %v %v", unprocessedPause(0), unprocessedPause(3), unprocessedPause(100))
	}
}

// fakeStream is a stream of shards, each with records, read in pages of
// one record, its GetRecords failing with fail where it's set.
type fakeStream struct {
	mu      sync.Mutex
	shards  []streamtypes.Shard
	records map[string][]streamtypes.Record
	fail    map[string]error
	// How many of each shard's oldest records the stream has trimmed.
	trim map[string]int
	// The shards in the order their reads started, and the order they were
	// read to their ends.
	started, ended []string
}

func (f *fakeStream) DescribeStream(_ context.Context, in *dynamodbstreams.DescribeStreamInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error) {
	// A page of one shard, so that listing pages.
	i := 0
	if in.ExclusiveStartShardId != nil {
		i = slices.IndexFunc(f.shards, func(s streamtypes.Shard) bool { return *s.ShardId == *in.ExclusiveStartShardId }) + 1
	}
	d := &streamtypes.StreamDescription{Shards: f.shards[i : i+1]}
	if i+1 < len(f.shards) {
		d.LastEvaluatedShardId = f.shards[i].ShardId
	}
	return &dynamodbstreams.DescribeStreamOutput{StreamDescription: d}, nil
}

// An iterator is SHARD/POSITION.
func (f *fakeStream) GetShardIterator(_ context.Context, in *dynamodbstreams.GetShardIteratorInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetShardIteratorOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := *in.ShardId
	f.started = append(f.started, id)
	pos := f.trim[id]
	if in.ShardIteratorType == streamtypes.ShardIteratorTypeAfterSequenceNumber {
		pos = slices.IndexFunc(f.records[id], func(r streamtypes.Record) bool { return *r.Dynamodb.SequenceNumber == *in.SequenceNumber }) + 1
		if pos <= f.trim[id] {
			return nil, &smithy.GenericAPIError{Code: "TrimmedDataAccessException", Message: "trimmed"}
		}
	}
	return &dynamodbstreams.GetShardIteratorOutput{ShardIterator: aws.String(fmt.Sprintf("%s/%d", id, pos))}, nil
}

func (f *fakeStream) GetRecords(_ context.Context, in *dynamodbstreams.GetRecordsInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var id string
	var pos int
	if _, err := fmt.Sscanf(*in.ShardIterator, "%1s/%d", &id, &pos); err != nil {
		return nil, err
	}
	if err := f.fail[id]; err != nil {
		return nil, err
	}
	if pos < f.trim[id] {
		return nil, &smithy.GenericAPIError{Code: "TrimmedDataAccessException", Message: "trimmed"}
	}
	recs := f.records[id]
	out := &dynamodbstreams.GetRecordsOutput{}
	if pos < len(recs) {
		out.Records = recs[pos : pos+1]
		pos++
	}
	closed := slices.ContainsFunc(f.shards, func(s streamtypes.Shard) bool {
		return *s.ShardId == id && s.SequenceNumberRange.EndingSequenceNumber != nil
	})
	if closed && pos >= len(recs) {
		f.ended = append(f.ended, id)
		return out, nil
	}
	out.NextShardIterator = aws.String(fmt.Sprintf("%s/%d", id, pos))
	return out, nil
}

// trimBefore trims the records created before cut, as the stream does those
// past its 24 hours.
func (f *fakeStream) trimBefore(cut time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.trim = map[string]int{}
	for id, recs := range f.records {
		f.trim[id] = len(recs) - len(slices.DeleteFunc(slices.Clone(recs), func(r streamtypes.Record) bool {
			return r.Dynamodb.ApproximateCreationDateTime.Before(cut)
		}))
	}
}

func shard(id, parent string, closed bool) streamtypes.Shard {
	s := streamtypes.Shard{ShardId: aws.String(id), SequenceNumberRange: &streamtypes.SequenceNumberRange{StartingSequenceNumber: aws.String("1")}}
	if parent != "" {
		s.ParentShardId = aws.String(parent)
	}
	if closed {
		s.SequenceNumberRange.EndingSequenceNumber = aws.String("9")
	}
	return s
}

func insert(seq, id int, at time.Time) streamtypes.Record {
	return streamtypes.Record{EventName: streamtypes.OperationTypeInsert, Dynamodb: &streamtypes.StreamRecord{
		SequenceNumber: aws.String(fmt.Sprintf("%021d", seq)), ApproximateCreationDateTime: aws.Time(at),
		Keys:     map[string]streamtypes.AttributeValue{"id": &streamtypes.AttributeValueMemberN{Value: strconv.Itoa(id)}},
		NewImage: map[string]streamtypes.AttributeValue{"id": &streamtypes.AttributeValueMemberN{Value: strconv.Itoa(id)}},
	}}
}

func lineageRead(f *fakeStream, rec *sourcetest.Recorder, st cdcState) *cdcRead {
	return &cdcRead{
		s: &Source{}, streams: f, cs: source.ConfiguredStream{Name: "t", SyncMode: source.CDC},
		t: &table{name: "t", keys: []string{"id"}}, out: rec.Split("t", source.WholeStream), start: now(), st: st,
	}
}

// A child shard is read once its parents are read to their ends, and
// shards a split or a merge made after the read's state are read too.
func TestShardLineage(t *testing.T) {
	at := time.Now().Add(-time.Minute)
	// a splits into b and c, which merge into d, which is open.
	f := &fakeStream{
		shards: []streamtypes.Shard{shard("d", "b", false), shard("c", "a", true), shard("b", "a", true), shard("a", "", true)},
		records: map[string][]streamtypes.Record{
			"a": {insert(1, 1, at), insert(2, 1, at)},
			"b": {insert(3, 2, at)}, "c": {insert(4, 3, at)},
			"d": {insert(5, 4, at)},
		},
	}
	rec := &sourcetest.Recorder{}
	r := lineageRead(f, rec, cdcState{Stream: "arn", G: 1, Skip: at.Add(-time.Hour), Shards: map[string]shardState{"a": {Since: at.Add(-time.Hour)}}})
	if err := r.follow(t.Context()); err != nil {
		t.Fatal(err)
	}
	pos := func(id string) int { return slices.Index(f.started, id) }
	if pos("a") < 0 || pos("b") < pos("a") || pos("c") < pos("a") || pos("d") < pos("b") || pos("d") < pos("c") {
		t.Errorf("shards started in the order %v, children before parents", f.started)
	}
	if !slices.Contains(f.ended, "a") || slices.Index(f.ended, "b") < 0 || slices.Index(f.ended, "c") < 0 {
		t.Errorf("closed shards read to their ends: %v", f.ended)
	}
	var ids []string
	for _, row := range rec.RowsOf("t") {
		ids = append(ids, row["id"])
	}
	if len(ids) != 5 || ids[0] != "1" || ids[1] != "1" || ids[4] != "4" {
		t.Errorf("rows by id %v, want a's two, b's and c's, then d's", ids)
	}
	state, complete := rec.Last("t")
	if !complete {
		t.Fatal("the read didn't complete")
	}
	var st cdcState
	if err := json.Unmarshal(state, &st); err != nil {
		t.Fatal(err)
	}
	if st.Shards["d"].After != fmt.Sprintf("%021d", 5) || st.Shards["d"].Done || !st.Shards["c"].Done {
		t.Errorf("state's shards %+v", st.Shards)
	}
	// The next read starts after d's last record, and reads nothing.
	rec2 := &sourcetest.Recorder{}
	if err := lineageRead(f, rec2, st).follow(t.Context()); err != nil {
		t.Fatal(err)
	}
	if rows := rec2.RowsOf("t"); len(rows) != 0 {
		t.Errorf("a read from the state read %v again", rows)
	}
}

func TestTrimmedPositionIsLost(t *testing.T) {
	at := time.Now().Add(-time.Minute)
	f := &fakeStream{
		shards:  []streamtypes.Shard{shard("a", "", false)},
		records: map[string][]streamtypes.Record{"a": {insert(1, 1, at)}},
		fail:    map[string]error{"a": &smithy.GenericAPIError{Code: "TrimmedDataAccessException", Message: "trimmed"}},
	}
	err := lineageRead(f, &sourcetest.Recorder{}, cdcState{Stream: "arn", G: 1, Skip: at, Shards: map[string]shardState{"a": {After: "x"}}}).follow(t.Context())
	if sourcetest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a trimmed position: %v (%v), want POSITION_LOST", err, sourcetest.CodeOf(err))
	}
	// A shard the state hadn't read to its end that the stream no longer
	// lists.
	err = lineageRead(f, &sourcetest.Recorder{}, cdcState{Stream: "arn", G: 1, Skip: at, Shards: map[string]shardState{"gone": {After: "1"}}}).follow(t.Context())
	if sourcetest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a shard gone: %v (%v), want POSITION_LOST", err, sourcetest.CodeOf(err))
	}
	// A shard gone whose position is within 24 hours had nothing after it:
	// the read goes on without it.
	if err := lineageRead(f, &sourcetest.Recorder{}, cdcState{Stream: "arn", G: 1, Skip: at, Shards: map[string]shardState{
		"gone": {After: "1", Since: time.Now().Add(-23 * time.Hour)}, "a": {Since: at},
	}}).follow(t.Context()); sourcetest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST || !strings.Contains(err.Error(), "shard a ") {
		t.Errorf("a shard gone within 24 hours: %v, want the read to go on to a's trimmed position", err)
	}
	// A shard read from its start that has records older than 24 hours.
	err = lineageRead(f, &sourcetest.Recorder{}, cdcState{Stream: "arn", G: 1, Skip: at, Shards: map[string]shardState{"a": {Since: time.Now().Add(-25 * time.Hour)}}}).follow(t.Context())
	if sourcetest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a position past 24 hours: %v (%v), want POSITION_LOST", err, sourcetest.CodeOf(err))
	}
}

func TestSeq(t *testing.T) {
	r := &cdcRead{st: cdcState{G: 12}}
	a, err := r.seq("000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	if got := a.BigInt().String(); got != "12"+fmt.Sprintf("%029d", 1) {
		t.Errorf("seq = %s", got)
	}
	snap, _ := r.seq("")
	if snap.Cmp(a) >= 0 {
		t.Error("a snapshot's seq isn't below its stream's")
	}
	next := &cdcRead{st: cdcState{G: 13}}
	b, _ := next.seq("")
	if b.Cmp(a) <= 0 {
		t.Error("a later snapshot's seq isn't above the stream before it")
	}
	if _, err := r.seq("1" + fmt.Sprintf("%029d", 0)); sourcetest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED {
		t.Errorf("a 30-digit sequence number: %v", err)
	}
}

func TestRetention(t *testing.T) {
	now := time.Now()
	cdc, _ := json.Marshal(cdcState{Stream: "arn", Skip: now.Add(-time.Hour), Shards: map[string]shardState{
		"a": {Since: now.Add(-2 * time.Hour)}, "b": {Since: now.Add(-23 * time.Hour), Done: true},
	}})
	scan, _ := json.Marshal(segState{})
	got, err := (&Source{}).Retention(t.Context(), "p", map[string][]byte{"c": cdc, "s": scan}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Stream != "c" || *got[0].Lag < 2*time.Hour || *got[0].RetentionLeft > 22*time.Hour || *got[0].RetentionLeft < 21*time.Hour {
		t.Errorf("retention %+v", got)
	}
}

func TestHighestCursor(t *testing.T) {
	var cur *value
	for _, v := range []types.AttributeValue{str("z"), num("5"), num("12"), str("zz"), &types.AttributeValueMemberBOOL{Value: true}, num("7")} {
		cur = highest(cur, v)
	}
	if n, ok := cur.AttributeValue.(*types.AttributeValueMemberN); !ok || n.Value != "12" {
		t.Errorf("highest = %v, want N 12", cur.AttributeValue)
	}
}

// Every role's checks and preflights are lists the SDK runs: names it
// takes, needs that come before, and fields of the config schema.
func TestCheckLists(t *testing.T) {
	src, err := NewSource([]byte(`{"region": "us-east-1", "tables": ["a.b", "c_d"]}`))
	if err != nil {
		t.Fatal(err)
	}
	dst, err := NewDestination([]byte(`{"region": "us-east-1", "endpoint": "http://127.0.0.1:8000", "tablePrefix": "x_"}`))
	if err != nil {
		t.Fatal(err)
	}
	lists := map[string][]check.Check{
		"source checks": src.Checks(),
		"source preflight": src.Preflight(source.PreflightRequest{Interval: 30 * time.Hour, Streams: []source.ConfiguredStream{
			{Name: "a.b", SyncMode: source.CDC}, {Name: "c_d", SyncMode: source.Incremental},
		}}),
		"destination checks": dst.Checks(),
		"destination preflight": dst.Preflight(destination.PreflightRequest{Streams: []destination.Stream{
			{Name: "a", Mode: destination.Merge, PrimaryKey: []string{"id"}},
		}}),
	}
	fields := map[string]bool{}
	for _, s := range [][]byte{SourceConfigSchema, DestinationConfigSchema} {
		var schema struct{ Properties map[string]any }
		if err := json.Unmarshal(s, &schema); err != nil {
			t.Fatal(err)
		}
		for k := range schema.Properties {
			fields[k] = true
		}
	}
	for name, l := range lists {
		if err := check.Validate(l); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		for _, c := range l {
			if c.Field != "" && !fields[c.Field] {
				t.Errorf("%s: %s's field %q isn't the config's", name, c.Name, c.Field)
			}
		}
	}
}

func TestPreflightRetention(t *testing.T) {
	src, _ := NewSource([]byte(`{"region": "us-east-1"}`))
	report := check.Run(t.Context(), slices.DeleteFunc(src.Preflight(source.PreflightRequest{Interval: 30 * time.Hour, Streams: []source.ConfiguredStream{
		{Name: "t", SyncMode: source.CDC},
	}}), func(c check.Check) bool { return c.Name != "stream.t.retention" }))
	b, _ := json.Marshal(report)
	if len(report.Items) != 1 || report.OK() {
		t.Errorf("a pipeline syncing every 30 hours passed its retention check: %s", b)
	}
}

// A read from each resume point until the next reads the records between
// them across the lineage, sends a resume point at until, and completes
// there.
func TestShardUntil(t *testing.T) {
	at := time.Now().Add(-time.Minute)
	f := &fakeStream{
		shards: []streamtypes.Shard{shard("d", "b", false), shard("c", "a", true), shard("b", "a", true), shard("a", "", true)},
		records: map[string][]streamtypes.Record{
			"a": {insert(1, 1, at), insert(2, 1, at)},
			"b": {insert(3, 2, at), insert(4, 3, at)}, "c": {insert(5, 4, at)},
			"d": {insert(6, 5, at), insert(7, 6, at)},
		},
	}
	first := cdcState{Stream: "arn", G: 1, Skip: at.Add(-time.Hour), Shards: map[string]shardState{"a": {Since: at.Add(-time.Hour)}}}
	start, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	rec := &sourcetest.Recorder{}
	if err := lineageRead(f, rec, first).follow(t.Context()); err != nil {
		t.Fatal(err)
	}
	from, rows := start, map[string]int{}
	points := 0
	for _, e := range rec.Snapshot() {
		for _, row := range e.Rows {
			rows[row["id"]]++
		}
		if !e.Resume {
			continue
		}
		points++
		var st, until cdcState
		if err := json.Unmarshal(from, &st); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(e.State, &until); err != nil {
			t.Fatal(err)
		}
		again := &sourcetest.Recorder{}
		r := lineageRead(f, again, st)
		r.until, r.untilRaw = &until, e.State
		if err := r.follow(t.Context()); err != nil {
			t.Fatal(err)
		}
		got := map[string]int{}
		for _, row := range again.RowsOf("t") {
			got[row["id"]]++
		}
		if !maps.Equal(got, rows) {
			t.Errorf("until resume point %d: read %v, want %v", points, got, rows)
		}
		events := again.Snapshot()
		if n := len(events); n < 2 || !events[n-2].Resume || string(events[n-2].State) != string(e.State) || !events[n-1].Complete || string(events[n-1].State) != string(e.State) {
			t.Errorf("until resume point %d: didn't end with a resume point and a completion at it", points)
		}
		from, rows = e.State, map[string]int{}
	}
	if points < 5 {
		t.Errorf("%d resume points, want one a record", points)
	}
}

// Bounded reads 11.9 hours after the last record, then 12.2 hours apart,
// of idle shards, one with a record the
// stream trims, one with none, lose nothing; a read more than 24 hours
// after the last, of records written between, has lost them.
func TestIdleShardsAcrossSyncs(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := t0
	defer func(n func() time.Time) { now = n }(now)
	now = func() time.Time { return clock }
	f := &fakeStream{
		shards:  []streamtypes.Shard{shard("a", "", false), shard("b", "", false)},
		records: map[string][]streamtypes.Record{"a": {insert(1, 1, t0)}},
	}
	read := func(st cdcState) (cdcState, error) {
		f.trimBefore(clock.Add(-retention))
		rec := &sourcetest.Recorder{}
		if err := lineageRead(f, rec, st).follow(t.Context()); err != nil {
			return st, err
		}
		state, complete := rec.Last("t")
		if !complete {
			t.Fatal("the read didn't complete")
		}
		var next cdcState
		if err := json.Unmarshal(state, &next); err != nil {
			t.Fatal(err)
		}
		return next, nil
	}
	st := cdcState{Stream: "arn", G: 1, Skip: t0, Shards: map[string]shardState{"a": {Since: t0}, "b": {Since: t0}}}
	// The first sync ends with Since 11.9 hours old, the rest come 12.2
	// hours apart.
	clock = t0.Add(11*time.Hour + 54*time.Minute)
	st, err := read(st)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		clock = clock.Add(12*time.Hour + 12*time.Minute)
		if st, err = read(st); err != nil {
			t.Fatalf("sync %d, 12.2 hours after the last: %v", i+2, err)
		}
	}
	f.mu.Lock()
	f.records["a"] = append(f.records["a"], insert(2, 2, clock.Add(time.Hour)))
	f.mu.Unlock()
	clock = clock.Add(25*time.Hour + 30*time.Minute)
	if _, err := read(st); sourcetest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a read 25.5 hours after the last, of a record written between that the stream trimmed: %v (%v), want POSITION_LOST", err, sourcetest.CodeOf(err))
	}
}
