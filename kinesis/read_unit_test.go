package kinesis

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/openetl-io/openetl-connectors/records"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// fakeShard is one shard of fakeStream: its records, oldest first, from
// which trimming removes.
type fakeShard struct {
	id, parent string
	closed     bool
	seqs       []string
	arrivals   []time.Time
}

// fakeStream answers a source's reads of one stream, "s", with 24 hours of
// retention. A GetRecords returns at most perCall records. Iterators are
// "SHARD/AFTER", the records after sequence number AFTER, so one after a
// record retention removed reads on from the next one, as Kinesis's does.
type fakeStream struct {
	mu      sync.Mutex
	shards  []*fakeShard
	perCall int
	// Each op's answer instead, when it returns ok.
	override func(op string, in map[string]any) (int, any, bool)
}

// put adds n records to shard, numbered on from the last, arrived at.
func (f *fakeStream) put(shard *fakeShard, n int, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	next := 100
	if len(shard.seqs) > 0 {
		next, _ = strconv.Atoi(shard.seqs[len(shard.seqs)-1])
		next++
	}
	for i := range n {
		shard.seqs = append(shard.seqs, strconv.Itoa(next+i))
		shard.arrivals = append(shard.arrivals, at)
	}
}

// trim removes a shard's records up to and including seq.
func (f *fakeStream) trim(shard *fakeShard, seq string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := 0
	for i < len(shard.seqs) && cmpSeq(shard.seqs[i], seq) <= 0 {
		i++
	}
	shard.seqs, shard.arrivals = shard.seqs[i:], shard.arrivals[i:]
}

func (f *fakeStream) shard(id string) *fakeShard {
	for _, sh := range f.shards {
		if sh.id == id {
			return sh
		}
	}
	return nil
}

func (f *fakeStream) handle(op string, in map[string]any) (int, any) {
	if f.override != nil {
		if status, body, ok := f.override(op, in); ok {
			return status, body
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch op {
	case "DescribeStreamSummary":
		return http.StatusOK, summary("s", 24, 0)
	case "ListShards":
		var out []any
		for _, sh := range f.shards {
			rng := map[string]any{"StartingSequenceNumber": "1"}
			if sh.closed {
				rng["EndingSequenceNumber"] = "99999"
			}
			m := map[string]any{"ShardId": sh.id, "HashKeyRange": map[string]any{"StartingHashKey": "0", "EndingHashKey": "1"}, "SequenceNumberRange": rng}
			if sh.parent != "" {
				m["ParentShardId"] = sh.parent
			}
			out = append(out, m)
		}
		return http.StatusOK, map[string]any{"Shards": out}
	case "GetShardIterator":
		id, _ := in["ShardId"].(string)
		sh := f.shard(id)
		after := ""
		switch in["ShardIteratorType"] {
		case "AFTER_SEQUENCE_NUMBER":
			after, _ = in["StartingSequenceNumber"].(string)
		case "AT_TIMESTAMP":
			ts, _ := in["Timestamp"].(float64)
			at := time.UnixMilli(int64(ts * 1000))
			for i, a := range sh.arrivals {
				if a.Before(at) {
					after = sh.seqs[i]
				}
			}
		}
		return http.StatusOK, map[string]any{"ShardIterator": id + "/" + after}
	case "GetRecords":
		it, _ := in["ShardIterator"].(string)
		id, after, _ := strings.Cut(it, "/")
		sh := f.shard(id)
		var recs []any
		last := after
		for i, seq := range sh.seqs {
			if after != "" && cmpSeq(seq, after) <= 0 || len(recs) == f.perCall {
				continue
			}
			recs = append(recs, map[string]any{"SequenceNumber": seq, "PartitionKey": "k", "Data": []byte(`{"seq":` + seq + `}`),
				"ApproximateArrivalTimestamp": float64(sh.arrivals[i].UnixMilli()) / 1000})
			last = seq
		}
		behind := 0
		if len(sh.seqs) > 0 && (last == "" || cmpSeq(last, sh.seqs[len(sh.seqs)-1]) < 0) {
			behind = 1000
		}
		out := map[string]any{"Records": recs, "MillisBehindLatest": behind}
		if !sh.closed || behind > 0 {
			out["NextShardIterator"] = id + "/" + last
		}
		return http.StatusOK, out
	}
	return apiError("UnknownOperationException", op)
}

// newReadSource is a source reading stream "s" of the fake.
func newReadSource(t *testing.T, f *fakeStream) *Source {
	t.Helper()
	_, c := newFake(t, f.handle)
	c.Retry.MaxAttempts = 3
	cfg := SourceConfig{Connection: c, Streams: []string{"s"}, StartPosition: startEarliest}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Polling.Interval, cfg.Polling.IdleInterval = Duration(minPollInterval), Duration(minPollInterval)
	return &Source{cfg: cfg, values: records.NewDecoder(records.JSON, nil)}
}

// eventLog is an Emitter that keeps what a read emits, in order: "SPLIT rows
// SEQ...", "SPLIT resume STATE" and "SPLIT complete STATE +ADDED...".
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) Rows(stream string, rows []json.RawMessage) error {
	return l.Split(stream, source.WholeStream).Rows(rows)
}
func (l *eventLog) Records(string, arrow.RecordBatch) error { return fmt.Errorf("no typed batches") }
func (l *eventLog) ResumePoint(stream string, state []byte) error {
	return l.Split(stream, source.WholeStream).ResumePoint(state)
}
func (l *eventLog) Complete(stream string, next []byte) error {
	return l.Split(stream, source.WholeStream).Complete(next)
}
func (l *eventLog) Split(_, id string) source.SplitEmitter { return splitLog{l: l, id: id} }

type splitLog struct {
	l  *eventLog
	id string
}

func (s splitLog) Rows(rows []json.RawMessage) error {
	seqs := make([]string, len(rows))
	for i, r := range rows {
		var m map[string]any
		if err := json.Unmarshal(r, &m); err != nil {
			return err
		}
		seqs[i], _ = m[sequenceColumn].(string)
	}
	s.l.add(s.id + " rows " + strings.Join(seqs, " "))
	return nil
}
func (s splitLog) Records(arrow.RecordBatch) error { return fmt.Errorf("no typed batches") }
func (s splitLog) ResumePoint(state []byte, added ...source.Split) error {
	s.l.add(s.id + " resume " + string(state) + addedOf(added))
	return nil
}
func (s splitLog) Complete(next []byte, added ...source.Split) error {
	s.l.add(s.id + " complete " + string(next) + addedOf(added))
	return nil
}

func addedOf(added []source.Split) string {
	out := ""
	for _, a := range added {
		out += " +" + a.ID
	}
	return out
}

// rowSeqs are the sequence numbers of the rows the log has, in order.
func (l *eventLog) rowSeqs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, e := range l.events {
		if _, seqs, ok := strings.Cut(e, " rows "); ok {
			out = append(out, strings.Fields(seqs)...)
		}
	}
	return out
}

func (l *eventLog) last(n int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events[max(0, len(l.events)-n):])
}

func readOnce(t *testing.T, s *Source, cs source.ConfiguredStream) (*eventLog, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	log := &eventLog{}
	cs.Name, cs.SyncMode = "s", source.Stream
	err := s.Read(ctx, source.ReadRequest{Pipeline: "p", Lanes: 1, Streams: []source.ConfiguredStream{cs}}, log)
	return log, err
}

func seqRange(from, to int) []string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, strconv.Itoa(i))
	}
	return out
}

// A bounded read given an end, a resume point's state or a completion's,
// reads each shard up to and including the end's position, nothing of a
// shard the end doesn't have, and ends with a resume point at the end and
// completes there.
func TestReadStopsAtItsEnd(t *testing.T) {
	old := time.Now().Add(-time.Hour)
	f := &fakeStream{perCall: 3, shards: []*fakeShard{{id: "shardId-0"}, {id: "shardId-1"}}}
	f.put(f.shards[0], 10, old)
	f.put(f.shards[1], 4, old)
	s := newReadSource(t, f)
	end := encodeState(map[string]shardState{"shardId-0": {After: "106", Since: old}})
	for _, cs := range []source.ConfiguredStream{{Until: end}, {CompleteAt: end}} {
		log, err := readOnce(t, s, cs)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := log.rowSeqs(), seqRange(100, 106); !slices.Equal(got, want) {
			t.Errorf("until %v, complete_at %v: rows %v, want %v", cs.Until != nil, cs.CompleteAt != nil, got, want)
		}
		if got, want := log.last(2), []string{" resume " + string(end), " complete " + string(end)}; !slices.Equal(got, want) {
			t.Errorf("until %v, complete_at %v: ended with %q, want %q", cs.Until != nil, cs.CompleteAt != nil, got, want)
		}
	}

	from := encodeState(map[string]shardState{"shardId-0": {After: "102", Since: old}, "shardId-1": {After: "101", Since: old}})
	end = encodeState(map[string]shardState{"shardId-0": {After: "104", Since: old}, "shardId-1": {After: "101", Since: old}})
	log, err := readOnce(t, s, source.ConfiguredStream{State: from, Until: end})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := log.rowSeqs(), seqRange(103, 104); !slices.Equal(got, want) {
		t.Errorf("from a resume point: rows %v, want %v", got, want)
	}
}

// A split given an end reads up to it and completes there, adding none of
// its children, even where the end is the shard's end.
func TestSplitStopsAtItsEnd(t *testing.T) {
	old := time.Now().Add(-time.Hour)
	f := &fakeStream{perCall: 3, shards: []*fakeShard{{id: "shardId-0", closed: true}, {id: "shardId-1", parent: "shardId-0"}}}
	f.put(f.shards[0], 8, old)
	f.put(f.shards[1], 3, old)
	s := newReadSource(t, f)
	for _, c := range []struct {
		end  shardState
		want []string
	}{
		{shardState{After: "104", Since: old}, seqRange(102, 104)},
		{shardState{After: "107", Since: old, Done: true}, seqRange(102, 107)},
	} {
		end := encodeShard(c.end)
		for _, sp := range []source.Split{{Until: end}, {CompleteAt: end}} {
			sp.ID, sp.State = "shardId-0", encodeShard(shardState{After: "101", Since: old})
			log, err := readOnce(t, s, source.ConfiguredStream{Splits: []source.Split{sp}})
			if err != nil {
				t.Fatal(err)
			}
			if got := log.rowSeqs(); !slices.Equal(got, c.want) {
				t.Errorf("end %s: rows %v, want %v", end, got, c.want)
			}
			if got, want := log.last(2), []string{"shardId-0 resume " + string(end), "shardId-0 complete " + string(end)}; !slices.Equal(got, want) {
				t.Errorf("end %s: ended with %q, want %q", end, got, want)
			}
		}
	}
}

// A redo of an epoch that 0.1.0 read, whose states have no "checked", ends
// with exactly the state 0.1.0 sent, the read and each split caught up.
func TestRedoesAnEpochOf010(t *testing.T) {
	old := time.Now().Add(-time.Hour)
	f := &fakeStream{perCall: 3, shards: []*fakeShard{{id: "shardId-0"}}}
	f.put(f.shards[0], 10, old)
	s := newReadSource(t, f)
	since := old.Add(time.Minute).UTC().Format(time.RFC3339Nano)
	shard := []byte(`{"after":"109","since":"` + since + `"}`)
	whole := []byte(`{"shards":{"shardId-0":` + string(shard) + `}}`)
	for _, c := range []struct {
		cs   source.ConfiguredStream
		end  []byte
		name string
	}{
		{source.ConfiguredStream{Until: whole}, whole, ""},
		{source.ConfiguredStream{CompleteAt: whole}, whole, ""},
		{source.ConfiguredStream{Splits: []source.Split{{ID: "shardId-0", State: []byte(`{}`), Until: shard}}}, shard, "shardId-0"},
		{source.ConfiguredStream{Splits: []source.Split{{ID: "shardId-0", State: []byte(`{}`), CompleteAt: shard}}}, shard, "shardId-0"},
	} {
		log, err := readOnce(t, s, c.cs)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := log.last(2), []string{c.name + " resume " + string(c.end), c.name + " complete " + string(c.end)}; !slices.Equal(got, want) {
			t.Errorf("ended with %q, want %q", got, want)
		}
	}
}

// A read whose end retention removed fails with POSITION_LOST rather than
// completing at an end it never reached.
func TestReadToAnEndRetentionRemoved(t *testing.T) {
	old := time.Now().Add(-time.Hour)
	f := &fakeStream{perCall: 3, shards: []*fakeShard{{id: "shardId-0"}}}
	f.put(f.shards[0], 10, old)
	f.trim(f.shards[0], "105")
	s := newReadSource(t, f)
	for _, end := range []shardState{{After: "104", Since: old}, {After: "105", Since: old}} {
		_, err := readOnce(t, s, source.ConfiguredStream{Until: encodeState(map[string]shardState{"shardId-0": end})})
		if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
			t.Errorf("an end at %s, removed: %v isn't POSITION_LOST", end.After, err)
		}
	}
	f.trim(f.shards[0], "109")
	_, err := readOnce(t, s, source.ConfiguredStream{Until: encodeState(map[string]shardState{"shardId-0": {After: "107", Since: old}})})
	if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a shard emptied past its end: %v isn't POSITION_LOST", err)
	}
}

// A bounded read from its last resume point completes where the read that
// emitted it did, as a redo of a part that ended at a completion reads.
func TestReadCompletesAgainWhereItDid(t *testing.T) {
	f := &fakeStream{perCall: 3, shards: []*fakeShard{{id: "shardId-0"}, {id: "shardId-1"}}}
	f.put(f.shards[0], 7, time.Now().Add(-2*time.Minute))
	s := newReadSource(t, f)
	first, err := readOnce(t, s, source.ConfiguredStream{})
	if err != nil {
		t.Fatal(err)
	}
	var lastResume, completion string
	for _, e := range first.last(len(first.events)) {
		if st, ok := strings.CutPrefix(e, " resume "); ok {
			lastResume = st
		}
		if st, ok := strings.CutPrefix(e, " complete "); ok {
			completion = st
		}
	}
	again, err := readOnce(t, s, source.ConfiguredStream{State: []byte(lastResume)})
	if err != nil {
		t.Fatal(err)
	}
	if got := again.last(1); len(got) != 1 || got[0] != " complete "+completion {
		t.Errorf("read again from %s, completed with %q, not at %s", lastResume, got, completion)
	}
}

// Kinesis refuses a start at a time later than its clock. A start at a time
// the runner's clock has passed, as startPosition latest's is, is tried
// again until Kinesis's clock passes it too: polling, and with enhanced
// fan-out.
func TestStartsDespiteClockSkew(t *testing.T) {
	refused := map[string]int{}
	f := &fakeStream{perCall: 3, shards: []*fakeShard{{id: "shardId-0"}}}
	f.override = func(op string, in map[string]any) (int, any, bool) {
		if op != "GetShardIterator" || in["ShardIteratorType"] != "AT_TIMESTAMP" || refused[op] == 2 {
			return 0, nil, false
		}
		refused[op]++
		status, body := apiError("InvalidArgumentException", "The timestampInMillis parameter cannot be greater than the currentTimestampInMillis.")
		return status, body, true
	}
	s := newReadSource(t, f)
	s.cfg.StartPosition = startLatest
	if _, err := readOnce(t, s, source.ConfiguredStream{}); err != nil {
		t.Fatalf("polling: %v", err)
	}
	if refused["GetShardIterator"] != 2 {
		t.Errorf("refused %d times", refused["GetShardIterator"])
	}

	rd := &reading{s: s, lanes: make(chan struct{}, 1), start: time.Now()}
	subscribed := 0
	rd.subscribe = func(_ context.Context, in *awskinesis.SubscribeToShardInput) (eventStream, error) {
		if subscribed++; subscribed <= 2 {
			return nil, &fakeAPIError{code: "InvalidArgumentException"}
		}
		return subscription(nil, types.SubscribeToShardEvent{ChildShards: []types.ChildShard{{ShardId: aws.String("child")}}}), nil
	}
	r := &streamRead{name: "s", consumer: "arn:consumer", retention: 24 * time.Hour, splits: map[string]bool{"shard-0": true}, shards: map[string]shardState{}, behind: map[string]time.Duration{}}
	sr := &shardRead{rd: rd, r: r, shard: "shard-0", em: &unitEmitter{}, st: shardState{From: time.Now(), Since: time.Now()}, th: throttle{retry: &s.cfg.Retry}}
	if over, err := sr.subscribe(t.Context()); err != nil || !over {
		t.Errorf("enhanced fan-out: over %v, %v", over, err)
	}
}

// A read further behind than the stream's retention that needs a new
// iterator, its last having expired, or a new subscription, fails with
// POSITION_LOST if retention removed what follows its position: Kinesis
// would read on from the oldest record left, past the gap, without a word.
func TestReadBehindRetentionFailsOnANewIterator(t *testing.T) {
	retention := 24 * time.Hour
	// Within retention when the read starts, past it once the iterator
	// expires.
	since := time.Now().Add(-retention + 2*time.Second)
	f := &fakeStream{perCall: 3, shards: []*fakeShard{{id: "shardId-0"}}}
	f.put(f.shards[0], 10, since)
	f.trim(f.shards[0], "105")
	var expired atomic.Bool
	f.override = func(op string, _ map[string]any) (int, any, bool) {
		if op != "GetRecords" || !expired.CompareAndSwap(false, true) {
			return 0, nil, false
		}
		time.Sleep(time.Until(since.Add(retention + 10*time.Millisecond)))
		status, body := apiError("ExpiredIteratorException", "Iterator expired.")
		return status, body, true
	}
	s := newReadSource(t, f)
	log, err := readOnce(t, s, source.ConfiguredStream{State: encodeState(map[string]shardState{"shardId-0": {After: "102", Since: since}})})
	if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST || !expired.Load() {
		t.Errorf("polling: %v isn't POSITION_LOST after the iterator expired (%v), and the read emitted %v", err, expired.Load(), log.rowSeqs())
	}

	cl, err := s.cfg.client(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	rd := &reading{s: s, cl: cl, lanes: make(chan struct{}, 1), start: time.Now()}
	rd.subscribe = func(context.Context, *awskinesis.SubscribeToShardInput) (eventStream, error) {
		return subscription(nil, types.SubscribeToShardEvent{ContinuationSequenceNumber: aws.String("102"), MillisBehindLatest: aws.Int64(1000)}), nil
	}
	r := &streamRead{name: "s", consumer: "arn:consumer", retention: retention, splits: map[string]bool{"shardId-0": true}, shards: map[string]shardState{}, behind: map[string]time.Duration{}}
	sr := &shardRead{rd: rd, r: r, shard: "shardId-0", em: &unitEmitter{}, st: shardState{After: "102", Since: since}, th: throttle{retry: &s.cfg.Retry}}
	time.Sleep(time.Until(since.Add(retention)))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := sr.subscribe(ctx); codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("enhanced fan-out: %v isn't POSITION_LOST", err)
	}
}

// Streams are listed by NextToken, or after the last name where a page says
// there are more but has no token, as emulators answer: for discovery, and
// for the streams teardown deregisters the consumer from.
func TestListsEveryStream(t *testing.T) {
	_, c := newFake(t, func(op string, in map[string]any) (int, any) {
		switch {
		case op != "ListStreams":
			return apiError("UnknownOperationException", op)
		case in["NextToken"] == "t":
			return http.StatusOK, map[string]any{"StreamNames": []string{"c"}, "HasMoreStreams": true}
		case in["ExclusiveStartStreamName"] == "c":
			return http.StatusOK, map[string]any{"StreamNames": []string{"d"}, "HasMoreStreams": false}
		}
		return http.StatusOK, map[string]any{"StreamNames": []string{"a", "b"}, "HasMoreStreams": true, "NextToken": "t"}
	})
	s := &Source{cfg: SourceConfig{Connection: c}, values: records.NewDecoder(records.JSON, nil)}
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, st := range cat.Streams {
		got = append(got, st.Name)
	}
	want := []string{"a", "b", "c", "d"}
	if !slices.Equal(got, want) {
		t.Errorf("discovered %v, want %v", got, want)
	}
	cl, err := c.client(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.streams(t.Context(), cl, 0); err != nil || !slices.Equal(got, want) {
		t.Errorf("listed %v, %v; want %v", got, err, want)
	}
}

// A continuous read lists the stream's shards again until a shard it read
// to its end has children, however long the listing stays without them,
// and reads them: read whole, and as splits.
func TestContinuousReadListsChildrenAgain(t *testing.T) {
	for _, c := range []struct {
		name   string
		splits []source.Split
		child  string
	}{
		{name: "whole", child: ""},
		{name: "splits", splits: []source.Split{{ID: "p"}}, child: "c"},
	} {
		t.Run(c.name, func(t *testing.T) {
			old := time.Now().Add(-time.Hour)
			f := &fakeStream{perCall: 3, shards: []*fakeShard{{id: "p", closed: true}, {id: "c", parent: "p"}}}
			f.put(f.shards[0], 3, old)
			f.put(f.shards[1], 2, old)
			f.shards[1].seqs = []string{"200", "201"}
			// The read's first listing and its first tries after p ends
			// don't have c.
			var lists atomic.Int32
			f.override = func(op string, _ map[string]any) (int, any, bool) {
				if op != "ListShards" || lists.Add(1) > listTries+2 {
					return 0, nil, false
				}
				p := map[string]any{"ShardId": "p", "HashKeyRange": map[string]any{"StartingHashKey": "0", "EndingHashKey": "1"},
					"SequenceNumberRange": map[string]any{"StartingSequenceNumber": "1", "EndingSequenceNumber": "99999"}}
				return http.StatusOK, map[string]any{"Shards": []any{p}}, true
			}
			s := newReadSource(t, f)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			log := &eventLog{}
			done := make(chan error, 1)
			go func() {
				done <- s.Read(ctx, source.ReadRequest{Pipeline: "p", Lanes: 1, Continuous: true, Streams: []source.ConfiguredStream{{Name: "s", SyncMode: source.Stream, Splits: c.splits}}}, log)
			}()
			want := []string{"100", "101", "102", "200", "201"}
			for ctx.Err() == nil && !slices.Equal(log.rowSeqs(), want) {
				time.Sleep(50 * time.Millisecond)
			}
			cancel()
			<-done
			if got := log.rowSeqs(); !slices.Equal(got, want) {
				t.Fatalf("read %v, want %v, after %d listings", got, want, lists.Load())
			}
			if c.child != "" && !slices.ContainsFunc(log.last(len(log.events)), func(e string) bool { return strings.HasPrefix(e, "p complete ") && strings.HasSuffix(e, " +c") }) {
				t.Errorf("p didn't add c: %v", log.events)
			}
		})
	}
}

// A continuous read's splits take no lane: on a read of one lane, two
// splits emit at once.
func TestContinuousSplitsTakeNoLanes(t *testing.T) {
	if readLanes(source.ReadRequest{Lanes: 1, Continuous: true}) != nil {
		t.Error("a continuous read has lanes")
	}
	if l := readLanes(source.ReadRequest{Lanes: 3}); cap(l) != 3 {
		t.Errorf("a bounded read of 3 lanes has %d", cap(l))
	}
	f := &fakeStream{perCall: 5, shards: []*fakeShard{{id: "a"}, {id: "b"}}}
	old := time.Now().Add(-time.Hour)
	f.put(f.shards[0], 2, old)
	f.put(f.shards[1], 2, old)
	s := newReadSource(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	em := &meetEmitter{in: map[string]bool{}, met: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Pipeline: "p", Lanes: 1, Continuous: true,
			Streams: []source.ConfiguredStream{{Name: "s", SyncMode: source.Stream, Splits: []source.Split{{ID: "a"}, {ID: "b"}}}}}, em)
	}()
	select {
	case <-em.met:
	case err := <-done:
		t.Fatalf("the read ended before both splits emitted: %v", err)
	}
	cancel()
	<-done
}

// meetEmitter's splits each wait in Rows, for up to 5 seconds, until another
// split is in Rows too, and fail if none comes.
type meetEmitter struct {
	mu  sync.Mutex
	in  map[string]bool
	met chan struct{}
}

func (e *meetEmitter) Rows(string, []json.RawMessage) error {
	return fmt.Errorf("whole stream")
}
func (e *meetEmitter) Records(string, arrow.RecordBatch) error { return fmt.Errorf("no typed batches") }
func (e *meetEmitter) ResumePoint(string, []byte) error        { return nil }
func (e *meetEmitter) Complete(string, []byte) error           { return nil }
func (e *meetEmitter) Split(_, id string) source.SplitEmitter  { return meetSplit{e: e, id: id} }

type meetSplit struct {
	e  *meetEmitter
	id string
}

func (s meetSplit) Rows([]json.RawMessage) error {
	e := s.e
	e.mu.Lock()
	if !e.in[s.id] {
		e.in[s.id] = true
		if len(e.in) == 2 {
			close(e.met)
		}
	}
	e.mu.Unlock()
	select {
	case <-e.met:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("split %s emitted alone", s.id)
	}
}
func (s meetSplit) Records(arrow.RecordBatch) error               { return fmt.Errorf("no typed batches") }
func (s meetSplit) ResumePoint(_ []byte, _ ...source.Split) error { return nil }
func (s meetSplit) Complete(_ []byte, _ ...source.Split) error    { return nil }
