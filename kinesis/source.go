package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/openetl-io/openetl-connectors/records"
	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type SourceConfig struct {
	Connection
	// Empty: every stream the credentials can list.
	Streams       []string `json:"streams"`
	StreamPattern string   `json:"streamPattern"`
	// earliest (default) or latest.
	StartPosition string `json:"startPosition"`
}

// The columns every row has, besides its data's fields.
const (
	shardColumn        = "_kinesis_shard"
	sequenceColumn     = "_kinesis_sequence"
	arrivalColumn      = "_kinesis_arrival"
	partitionKeyColumn = "_kinesis_partition_key"
)

const (
	getRecordsLimit = 10000
	// Kinesis allows five GetRecords a second per shard.
	minPoll  = 200 * time.Millisecond
	idlePoll = time.Second
	// How often a caught-up stream's state is emitted, to keep its time
	// current.
	idleResumeEvery = time.Minute
	progressEvery   = 10 * time.Second
	// How many empty GetRecords the retention check follows from a shard's
	// trim horizon before it counts the shard as empty.
	trimHorizonCalls = 20
)

type Source struct {
	cfg     SourceConfig
	pattern *regexp.Regexp
	reg     *sr.Client
	values  *records.Decoder
}

var (
	_ source.Source  = (*Source)(nil)
	_ source.Checker = (*Source)(nil)
)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{StartPosition: "earliest"}
	if err := decodeConfig(config, &cfg); err != nil {
		return nil, err
	}
	invalid := func(err error) error { return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err) }
	if err := cfg.validate(records.JSON, records.Avro, records.Protobuf, records.String); err != nil {
		return nil, invalid(err)
	}
	if cfg.StartPosition != "earliest" && cfg.StartPosition != "latest" {
		return nil, invalid(fmt.Errorf("kinesis: startPosition %q is neither earliest nor latest", cfg.StartPosition))
	}
	s := &Source{cfg: cfg}
	if cfg.StreamPattern != "" {
		p, err := regexp.Compile(cfg.StreamPattern)
		if err != nil {
			return nil, invalid(fmt.Errorf("kinesis: streamPattern: %w", err))
		}
		s.pattern = p
	}
	reg, err := cfg.Registry()
	if err != nil {
		return nil, err
	}
	s.reg = reg
	s.values = records.NewDecoder(cfg.Format, reg)
	return s, nil
}

func (s *Source) Check(ctx context.Context) error { return check(ctx, &s.cfg.Connection) }

func (s *Source) offered(name string) bool {
	if len(s.cfg.Streams) > 0 && !slices.Contains(s.cfg.Streams, name) {
		return false
	}
	return s.pattern == nil || s.pattern.MatchString(name)
}

func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	cl, err := s.cfg.client(ctx)
	if err != nil {
		return source.Catalog{}, err
	}
	var cat source.Catalog
	p := awskinesis.NewListStreamsPaginator(cl, &awskinesis.ListStreamsInput{})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return source.Catalog{}, classify(fmt.Errorf("kinesis: list streams: %w", err))
		}
		for _, name := range page.StreamNames {
			if !s.offered(name) {
				continue
			}
			sc, err := s.declared(ctx, name)
			if err != nil {
				return source.Catalog{}, err
			}
			cat.Streams = append(cat.Streams, source.CatalogStream{
				Name: name, CursorField: sequenceColumn, Replay: source.Exact, Schema: sc,
				SyncModes: []source.SyncMode{source.Stream},
			})
		}
	}
	return cat, nil
}

// declared is a stream's columns: the record's own, and the data's fields
// as the registry's latest value schema has them.
func (s *Source) declared(ctx context.Context, stream string) (*arrow.Schema, error) {
	fields := []arrow.Field{
		{Name: shardColumn, Type: arrow.BinaryTypes.String},
		{Name: sequenceColumn, Type: arrow.BinaryTypes.String},
		{Name: arrivalColumn, Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: partitionKeyColumn, Type: arrow.BinaryTypes.String},
	}
	values, err := records.ValueFields(ctx, s.reg, s.cfg.Format, stream+"-value")
	if err != nil {
		return nil, fmt.Errorf("kinesis: %w", err)
	}
	own := map[string]bool{}
	for _, f := range fields {
		own[f.Name] = true
	}
	for _, f := range values {
		if !own[f.Name] {
			fields = append(fields, f)
		}
	}
	return arrow.NewSchema(fields, nil), nil
}

// shardState is where a shard's read is.
type shardState struct {
	// The last sequence number read. Empty: from From, or else the shard's
	// start.
	After string `json:"after,omitempty"`
	// startPosition latest: records that arrived after this.
	From time.Time `json:"from,omitzero"`
	// Read to its end.
	Done bool `json:"done,omitempty"`
	// Nothing after the position arrived before this: the last record's
	// arrival, or when the read last caught up.
	Since time.Time `json:"since,omitzero"`
}

// state is a stream's resume point: each shard's position.
type state struct {
	Shards map[string]shardState `json:"shards"`
}

func parseState(b []byte) (map[string]shardState, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("kinesis: state: %w", err)
	}
	return st.Shards, nil
}

func encodeState(shards map[string]shardState) []byte {
	b, _ := json.Marshal(state{Shards: shards})
	return b
}

// cmpSeq compares two sequence numbers, decimals without leading zeros.
func cmpSeq(a, b string) int {
	if len(a) != len(b) {
		return len(a) - len(b)
	}
	return strings.Compare(a, b)
}

func positionLost(err error) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, err)
}

// streamRead is one stream's read.
type streamRead struct {
	name      string
	retention time.Duration
	shards    map[string]shardState
	// The stream's shards as last listed.
	listed map[string]types.Shard
	// Shards being polled, and shards whose read has stopped for this read.
	running map[string]bool
	stopped map[string]bool
	// Each running shard's last MillisBehindLatest.
	behind       map[string]time.Duration
	lastResume   time.Time
	complete     bool
	sinceChanged bool
}

// event is what a shard's poller saw in one GetRecords.
type event struct {
	stream, shard string
	records       []types.Record
	behind        time.Duration
	at            time.Time
	// The shard has no more records: it was closed, and they are read.
	ended bool
	// Bounded reads: the shard has caught up, or reached a record that
	// arrived after the read started.
	stop bool
	err  error
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	if len(req.Streams) == 0 {
		return nil
	}
	cl, err := s.cfg.client(ctx)
	if err != nil {
		return err
	}
	start := time.Now()
	bounded := !req.Continuous
	reads := map[string]*streamRead{}
	var names []string
	for _, st := range req.Streams {
		shards, err := parseState(st.State)
		if err != nil {
			return err
		}
		r := &streamRead{name: st.Name, shards: shards, running: map[string]bool{}, stopped: map[string]bool{}, behind: map[string]time.Duration{}}
		if err := s.position(ctx, cl, r, start); err != nil {
			return err
		}
		reads[st.Name] = r
		names = append(names, st.Name)
	}
	pctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()
	events := make(chan event)
	poll := func(r *streamRead, shard string) {
		r.running[shard] = true
		st := r.shards[shard]
		closed := r.listed[shard].SequenceNumberRange != nil && r.listed[shard].SequenceNumberRange.EndingSequenceNumber != nil
		wg.Go(func() { s.poll(pctx, cl, r.name, shard, st, closed, bounded, start, events) })
	}
	for _, t := range names {
		for _, id := range s.runnable(reads[t]) {
			poll(reads[t], id)
		}
	}
	progress, _ := out.(source.Progresser)
	tick := time.NewTicker(progressEvery)
	defer tick.Stop()
	for {
		if bounded {
			for _, t := range names {
				r := reads[t]
				if !r.complete && len(r.running) == 0 {
					r.complete = true
					if err := out.Complete(t, encodeState(r.shards)); err != nil {
						return err
					}
				}
			}
			if !slices.ContainsFunc(names, func(t string) bool { return !reads[t].complete }) {
				return nil
			}
		}
		var ev event
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if progress != nil {
				for _, t := range names {
					if err := progress.Progress(reads[t].progress(time.Now())); err != nil {
						return err
					}
				}
			}
			continue
		case ev = <-events:
		}
		if ev.err != nil {
			return ev.err
		}
		r := reads[ev.stream]
		st := r.shards[ev.shard]
		r.behind[ev.shard] = ev.behind
		if len(ev.records) > 0 {
			rows := make([]json.RawMessage, 0, len(ev.records))
			for i := range ev.records {
				row, err := s.row(ctx, ev.shard, &ev.records[i])
				if err != nil {
					return err
				}
				rows = append(rows, row)
			}
			last := ev.records[len(ev.records)-1]
			st.After = aws.ToString(last.SequenceNumber)
			if a := aws.ToTime(last.ApproximateArrivalTimestamp); a.After(st.Since) {
				st.Since = a
			}
			if err := out.Rows(ev.stream, rows); err != nil {
				return err
			}
		}
		if ev.behind == 0 && len(ev.records) == 0 && ev.at.After(st.Since) {
			st.Since = ev.at
			r.sinceChanged = true
		}
		if ev.ended {
			st.Done = true
		}
		r.shards[ev.shard] = st
		if ev.ended || ev.stop {
			delete(r.running, ev.shard)
			delete(r.behind, ev.shard)
			r.stopped[ev.shard] = true
		}
		if ev.ended {
			if err := s.list(ctx, cl, r); err != nil {
				return err
			}
			for _, id := range s.runnable(r) {
				poll(r, id)
			}
		}
		if len(ev.records) > 0 || ev.ended || (r.sinceChanged && time.Since(r.lastResume) > idleResumeEvery) {
			r.lastResume, r.sinceChanged = time.Now(), false
			if err := out.ResumePoint(ev.stream, encodeState(r.shards)); err != nil {
				return err
			}
		}
	}
}

// position settles where each of the stream's shards starts, and fails if
// retention may have discarded records after a shard's position.
func (s *Source) position(ctx context.Context, cl *awskinesis.Client, r *streamRead, start time.Time) error {
	sum, err := cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(r.name)})
	if err != nil {
		if apiCode(err) == "ResourceNotFoundException" {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("kinesis: stream %s doesn't exist", r.name))
		}
		return classify(fmt.Errorf("kinesis: describe stream %s: %w", r.name, err))
	}
	r.retention = time.Duration(aws.ToInt32(sum.StreamDescriptionSummary.RetentionPeriodHours)) * time.Hour
	if err := s.list(ctx, cl, r); err != nil {
		return err
	}
	fresh := r.shards == nil
	if fresh {
		r.shards = map[string]shardState{}
	}
	for _, id := range slices.Sorted(maps.Keys(r.shards)) {
		st := r.shards[id]
		if _, ok := r.listed[id]; ok {
			if !st.Done && start.Sub(st.Since) > r.retention {
				if err := s.checkRetained(ctx, cl, r, id, st); err != nil {
					return err
				}
			}
			continue
		}
		// Kinesis no longer lists the shard: it expired a retention period
		// after it closed, so whatever came after the position arrived
		// before then.
		if !st.Done && start.Sub(st.Since) > r.retention {
			return positionLost(fmt.Errorf("kinesis: %s's shard %s expired before the read reached its end, at %q: retention discarded records it hadn't read; the stream needs a re-sync", r.name, id, st.After))
		}
		delete(r.shards, id)
	}
	for id, sh := range r.listed {
		if _, ok := r.shards[id]; ok {
			continue
		}
		st := shardState{Since: start}
		if fresh && s.cfg.StartPosition == "latest" {
			if sh.SequenceNumberRange != nil && sh.SequenceNumberRange.EndingSequenceNumber != nil {
				st.Done = true
			} else {
				st.From = start
			}
		}
		r.shards[id] = st
	}
	return nil
}

// checkRetained fails unless a shard still holds the record at its
// position, so nothing after it can be gone.
func (s *Source) checkRetained(ctx context.Context, cl *awskinesis.Client, r *streamRead, id string, st shardState) error {
	it, err := cl.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{StreamName: aws.String(r.name), ShardId: aws.String(id), ShardIteratorType: types.ShardIteratorTypeTrimHorizon})
	if err != nil {
		return classify(fmt.Errorf("kinesis: %s's shard %s: %w", r.name, id, err))
	}
	oldest := ""
	next := it.ShardIterator
	for range trimHorizonCalls {
		if next == nil {
			break
		}
		res, err := cl.GetRecords(ctx, &awskinesis.GetRecordsInput{ShardIterator: next, Limit: aws.Int32(1)})
		if err != nil {
			return classify(fmt.Errorf("kinesis: %s's shard %s: %w", r.name, id, err))
		}
		if len(res.Records) > 0 {
			oldest = aws.ToString(res.Records[0].SequenceNumber)
			break
		}
		if aws.ToInt64(res.MillisBehindLatest) == 0 {
			break
		}
		next = res.NextShardIterator
	}
	if st.After != "" && oldest != "" && cmpSeq(oldest, st.After) <= 0 {
		return nil
	}
	return positionLost(fmt.Errorf("kinesis: %s's shard %s: the read's position (%q, nothing after it before %s) is older than the stream's %v retention, and the shard no longer holds it: retention may have discarded records the read hadn't reached; the stream needs a re-sync",
		r.name, id, st.After, st.Since.UTC().Format(time.RFC3339), r.retention))
}

// list lists the stream's shards.
func (s *Source) list(ctx context.Context, cl *awskinesis.Client, r *streamRead) error {
	listed := map[string]types.Shard{}
	in := &awskinesis.ListShardsInput{StreamName: aws.String(r.name)}
	for {
		res, err := cl.ListShards(ctx, in)
		if err != nil {
			return classify(fmt.Errorf("kinesis: list %s's shards: %w", r.name, err))
		}
		for _, sh := range res.Shards {
			listed[aws.ToString(sh.ShardId)] = sh
		}
		if res.NextToken == nil {
			break
		}
		in = &awskinesis.ListShardsInput{NextToken: res.NextToken}
	}
	r.listed = listed
	return nil
}

// runnable are the shards to start polling: not done, not running or
// stopped in this read, and whose parents are done or gone.
func (s *Source) runnable(r *streamRead) []string {
	var out []string
	for _, id := range slices.Sorted(maps.Keys(r.listed)) {
		st := r.shards[id]
		if st.Done || r.running[id] || r.stopped[id] {
			continue
		}
		sh := r.listed[id]
		if !s.parentDone(r, sh.ParentShardId) || !s.parentDone(r, sh.AdjacentParentShardId) {
			continue
		}
		if _, ok := r.shards[id]; !ok {
			r.shards[id] = shardState{Since: time.Now()}
		}
		out = append(out, id)
	}
	return out
}

func (s *Source) parentDone(r *streamRead, parent *string) bool {
	if parent == nil {
		return true
	}
	if _, listed := r.listed[*parent]; !listed {
		return true
	}
	return r.shards[*parent].Done
}

// poll reads one shard, sending what each GetRecords returns, until the
// shard ends, a bounded read stops, or ctx is done.
func (s *Source) poll(ctx context.Context, cl *awskinesis.Client, stream, shard string, st shardState, closed, bounded bool, start time.Time, events chan<- event) {
	send := func(ev event) bool {
		ev.stream, ev.shard = stream, shard
		select {
		case events <- ev:
			return true
		case <-ctx.Done():
			return false
		}
	}
	fail := func(err error) {
		send(event{err: classify(fmt.Errorf("kinesis: %s's shard %s: %w", stream, shard, err))})
	}
	after := st.After
	iterator := func() (*string, error) {
		in := &awskinesis.GetShardIteratorInput{StreamName: aws.String(stream), ShardId: aws.String(shard)}
		switch {
		case after != "":
			in.ShardIteratorType, in.StartingSequenceNumber = types.ShardIteratorTypeAfterSequenceNumber, aws.String(after)
		case !st.From.IsZero():
			in.ShardIteratorType, in.Timestamp = types.ShardIteratorTypeAtTimestamp, aws.Time(st.From)
		default:
			in.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
		}
		res, err := cl.GetShardIterator(ctx, in)
		if err != nil {
			return nil, err
		}
		return res.ShardIterator, nil
	}
	it, err := iterator()
	if err != nil {
		fail(err)
		return
	}
	for {
		at := time.Now()
		res, err := cl.GetRecords(ctx, &awskinesis.GetRecordsInput{ShardIterator: it, Limit: aws.Int32(getRecordsLimit)})
		if ctx.Err() != nil {
			return
		}
		if apiCode(err) == "ExpiredIteratorException" {
			if it, err = iterator(); err != nil {
				fail(err)
				return
			}
			continue
		}
		if err != nil {
			fail(err)
			return
		}
		ev := event{records: res.Records, behind: time.Duration(aws.ToInt64(res.MillisBehindLatest)) * time.Millisecond, at: at}
		if bounded {
			if i := slices.IndexFunc(ev.records, func(r types.Record) bool { return aws.ToTime(r.ApproximateArrivalTimestamp).After(start) }); i >= 0 {
				ev.records, ev.stop = ev.records[:i], true
			}
		}
		if !ev.stop {
			ev.ended = res.NextShardIterator == nil
			ev.stop = bounded && !closed && ev.behind == 0 && len(res.Records) == 0
		}
		if n := len(ev.records); n > 0 {
			after = aws.ToString(ev.records[n-1].SequenceNumber)
		}
		if !send(ev) || ev.ended || ev.stop {
			return
		}
		it = res.NextShardIterator
		wait := minPoll - time.Since(at)
		if len(res.Records) == 0 {
			wait = idlePoll
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (r *streamRead) progress(now time.Time) source.Progress {
	var lag time.Duration
	left := r.retention
	for id, b := range r.behind {
		lag = max(lag, b)
		if b > 0 {
			left = min(left, max(0, r.retention-now.Sub(r.shards[id].Since)))
		}
	}
	return source.Progress{Stream: r.name, Lag: &lag, RetentionLeft: &left}
}

// kplMagic starts a record the Kinesis Producer Library aggregated.
var kplMagic = []byte{0xf3, 0x89, 0x9a, 0xc2}

// row is a record as a JSON object: its data's fields, or its data as
// "value", and the record's own columns.
func (s *Source) row(ctx context.Context, shard string, rec *types.Record) (json.RawMessage, error) {
	seq := aws.ToString(rec.SequenceNumber)
	b := make([]byte, 0, len(rec.Data)+160)
	b = append(b, '{')
	if len(rec.Data) > 0 {
		v, err := s.values.Decode(ctx, rec.Data)
		if err != nil {
			if len(rec.Data) >= len(kplMagic) && string(rec.Data[:len(kplMagic)]) == string(kplMagic) {
				err = errors.New("a record the Kinesis Producer Library aggregated, which the source doesn't deaggregate yet")
			}
			return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, fmt.Errorf("kinesis: shard %s sequence %s: %w", shard, seq, err))
		}
		b = records.AppendFields(b, v)
	}
	for _, kv := range [][2]string{
		{shardColumn, shard},
		{sequenceColumn, seq},
		{arrivalColumn, aws.ToTime(rec.ApproximateArrivalTimestamp).UTC().Format(time.RFC3339Nano)},
		{partitionKeyColumn, aws.ToString(rec.PartitionKey)},
	} {
		b = records.AppendJSONString(b, kv[0])
		b = append(b, ':')
		b = records.AppendJSONString(b, kv[1])
		b = append(b, ',')
	}
	b[len(b)-1] = '}'
	return b, nil
}
