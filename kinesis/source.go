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
	// How many listings a shard that ended waits for its children through.
	listTries = 5
)

type Source struct {
	cfg     SourceConfig
	pattern *regexp.Regexp
	reg     *sr.Client
	values  *records.Decoder
}

var (
	_ source.Source   = (*Source)(nil)
	_ source.Checker  = (*Source)(nil)
	_ source.Splitter = (*Source)(nil)
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

// state is the resume point of a stream read whole: each shard's position. A
// shard read as a split has its shardState alone.
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

func encodeShard(st shardState) []byte {
	b, _ := json.Marshal(st)
	return b
}

// splitShards are the positions splits hold, or a whole stream's state;
// nil if there are none. A split at its beginning starts at start.
func splitShards(splits []source.Split, start time.Time) (map[string]shardState, error) {
	if len(splits) == 0 {
		return nil, nil
	}
	if len(splits) == 1 && splits[0].ID == source.WholeStream {
		return parseState(splits[0].State)
	}
	out := make(map[string]shardState, len(splits))
	for _, sp := range splits {
		st := shardState{Since: start}
		if len(sp.State) > 0 {
			if err := json.Unmarshal(sp.State, &st); err != nil {
				return nil, fmt.Errorf("kinesis: state of split %s: %w", sp.ID, err)
			}
		}
		out[sp.ID] = st
	}
	return out, nil
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
	// Where it's read in splits, the shards the read was given; nil where
	// it's read whole.
	splits map[string]bool
	// Orders shards' ends, and every emit of a stream read whole, which a
	// resume point of any shard must cover.
	serial sync.Mutex
	// Guards what's below.
	mu     sync.Mutex
	shards map[string]shardState
	// The stream's shards as last listed.
	listed map[string]types.Shard
	// Shards being polled, and shards whose read has stopped for this read.
	running map[string]bool
	stopped map[string]bool
	// Each running shard's last MillisBehindLatest.
	behind map[string]time.Duration
}

// reading is one Read: what its shards' goroutines share.
type reading struct {
	s       *Source
	cl      *awskinesis.Client
	out     source.Emitter
	bounded bool
	start   time.Time
	wg      sync.WaitGroup
	fail    context.CancelCauseFunc
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	if len(req.Streams) == 0 {
		return nil
	}
	cl, err := s.cfg.client(ctx)
	if err != nil {
		return err
	}
	rd := &reading{s: s, cl: cl, out: out, bounded: !req.Continuous, start: time.Now()}
	var reads []*streamRead
	for _, st := range req.Streams {
		r := &streamRead{name: st.Name, running: map[string]bool{}, stopped: map[string]bool{}, behind: map[string]time.Duration{}}
		if len(st.Splits) > 0 {
			r.shards, err = splitShards(st.Splits, rd.start)
			r.splits = make(map[string]bool, len(r.shards))
			for id := range r.shards {
				r.splits[id] = true
			}
		} else {
			r.shards, err = parseState(st.State)
		}
		if err != nil {
			return err
		}
		if err := s.position(ctx, cl, r, rd.start); err != nil {
			return err
		}
		if r.splits != nil {
			r.settleSplits(rd.bounded)
		}
		reads = append(reads, r)
	}
	pctx, cancel := context.WithCancelCause(ctx)
	rd.fail = cancel
	defer func() {
		cancel(nil)
		rd.wg.Wait()
	}()
	for _, r := range reads {
		if err := rd.begin(pctx, r); err != nil {
			return err
		}
	}
	done := make(chan struct{})
	go func() {
		rd.wg.Wait()
		close(done)
	}()
	progress, _ := out.(source.Progresser)
	tick := time.NewTicker(progressEvery)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if progress != nil {
				for _, r := range reads {
					if err := progress.Progress(r.progress(time.Now())); err != nil {
						return err
					}
				}
			}
		case <-done:
			if err := context.Cause(pctx); err != nil {
				return err
			}
			if rd.bounded {
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		}
	}
}

// begin starts the shards the read can start with: the splits to read now,
// and those already done, which only have to complete.
func (rd *reading) begin(ctx context.Context, r *streamRead) error {
	r.mu.Lock()
	var start []string
	for _, id := range r.runnable() {
		if _, known := r.shards[id]; known {
			start = append(start, id)
		}
	}
	for id := range r.splits {
		if r.shards[id].Done {
			start = append(start, id)
		}
	}
	for _, id := range start {
		r.running[id] = true
	}
	r.mu.Unlock()
	if rd.bounded && len(start) == 0 {
		return rd.complete(r)
	}
	for _, id := range start {
		rd.spawn(ctx, r, id)
	}
	return nil
}

func (rd *reading) spawn(ctx context.Context, r *streamRead, shard string) {
	rd.wg.Go(func() {
		if err := rd.poll(ctx, r, shard); err != nil {
			rd.fail(err)
		}
	})
}

// emitter is what a shard emits on: its split, or the whole stream.
func (rd *reading) emitter(r *streamRead, shard string) source.SplitEmitter {
	if r.splits == nil {
		shard = source.WholeStream
	}
	return rd.out.Split(r.name, shard)
}

// stateOf is the state of a resume point of shard.
func (r *streamRead) stateOf(shard string) []byte {
	if r.splits == nil {
		return encodeState(r.shards)
	}
	return encodeShard(r.shards[shard])
}

// emit sends what one GetRecords of shard returned, and moves it to st.
func (r *streamRead) emit(em source.SplitEmitter, shard string, st shardState, behind time.Duration, rows []json.RawMessage, resume bool) error {
	if r.splits == nil {
		r.serial.Lock()
		defer r.serial.Unlock()
	}
	if len(rows) > 0 {
		if err := em.Rows(rows); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.shards[shard], r.behind[shard] = st, behind
	var state []byte
	if resume {
		state = r.stateOf(shard)
	}
	r.mu.Unlock()
	if resume {
		return em.ResumePoint(state)
	}
	return nil
}

// finish ends a shard's read at st: read to its end if ended, or else as far
// as a bounded read goes. Shards end one at a time, each completing before
// the next counts as done, so a shard that a last parent adds has no parent
// still to complete once it has emitted anything.
func (rd *reading) finish(ctx context.Context, r *streamRead, shard string, st shardState, ended bool) error {
	r.serial.Lock()
	defer r.serial.Unlock()
	st.Done = ended
	if ended {
		if err := rd.listChildren(ctx, r, shard); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.shards[shard] = st
	delete(r.running, shard)
	delete(r.behind, shard)
	r.stopped[shard] = true
	var start []string
	var added []source.Split
	if ended {
		start, added = r.take()
	}
	state := r.stateOf(shard)
	last := rd.bounded && len(r.running) == 0
	r.mu.Unlock()
	em := rd.emitter(r, shard)
	var err error
	if r.splits == nil {
		err = em.ResumePoint(state)
	} else {
		err = em.Complete(state, added...)
	}
	if err != nil {
		return err
	}
	if last {
		if err := rd.complete(r); err != nil {
			return err
		}
	}
	for _, id := range start {
		rd.spawn(ctx, r, id)
	}
	return nil
}

// complete ends a bounded read of a stream once nothing's running, so nothing
// else touches its state: the stream read whole, or the splits that never
// started, their parents not read to their ends, which keep their states.
func (rd *reading) complete(r *streamRead) error {
	if r.splits == nil {
		return rd.out.Split(r.name, source.WholeStream).Complete(encodeState(r.shards))
	}
	for _, id := range slices.Sorted(maps.Keys(r.splits)) {
		if !r.stopped[id] {
			if err := rd.out.Split(r.name, id).Complete(encodeShard(r.shards[id])); err != nil {
				return err
			}
		}
	}
	return nil
}

// listChildren lists the stream's shards. A closed shard's children exist
// before it ends, so a listing without them is stale.
func (rd *reading) listChildren(ctx context.Context, r *streamRead, shard string) error {
	for range listTries {
		if err := rd.s.list(ctx, rd.cl, r); err != nil {
			return err
		}
		r.mu.Lock()
		childless := r.childless(shard)
		r.mu.Unlock()
		if !childless {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(idlePoll):
		}
	}
	return nil
}

func (r *streamRead) childless(shard string) bool {
	if _, listed := r.listed[shard]; !listed {
		return false
	}
	return !slices.ContainsFunc(slices.Collect(maps.Values(r.listed)), func(sh types.Shard) bool { return slices.Contains(parents(sh), shard) })
}

func parents(sh types.Shard) []string {
	var out []string
	for _, p := range []*string{sh.ParentShardId, sh.AdjacentParentShardId} {
		if p != nil {
			out = append(out, *p)
		}
	}
	return out
}

// settleSplits places the listed shards that aren't the read's splits, which
// are all it knows of them. One below a split waits for it; any other
// completed earlier in the read, or is above what the read has reached.
//
// What completed earlier in a bounded read may have stopped short of its end,
// so a shard waiting for one of those waits for the next read too.
func (r *streamRead) settleSplits(bounded bool) {
	earlier := map[string]bool{}
	for id := range r.listed {
		if !r.splits[id] && !r.below(id, r.splits) {
			earlier[id] = true
			r.shards[id] = shardState{Done: true}
		}
	}
	if !bounded {
		return
	}
	for id, sh := range r.listed {
		if !r.splits[id] && !earlier[id] && slices.ContainsFunc(parents(sh), func(p string) bool { return earlier[p] }) {
			r.stopped[id] = true
		}
	}
}

// below says a shard descends from one of shards.
func (r *streamRead) below(id string, shards map[string]bool) bool {
	sh, listed := r.listed[id]
	if !listed {
		return false
	}
	return slices.ContainsFunc(parents(sh), func(p string) bool { return shards[p] || r.below(p, shards) })
}

// doneAbove counts what's above a shard the read has reached as done: a child
// is read only after its parents, and done shards stop being splits.
func (r *streamRead) doneAbove(id string) {
	sh, listed := r.listed[id]
	if !listed {
		return
	}
	for _, p := range parents(sh) {
		if _, known := r.shards[p]; known {
			continue
		}
		if _, listed := r.listed[p]; listed {
			r.shards[p] = shardState{Done: true}
			r.doneAbove(p)
		}
	}
}

// splitSet is a read's splits, once every shard has a position: those
// runnable now, and those done that a child still waits for or may not have
// appeared under, so the next read knows them. Other done shards aren't
// splits, and the next read finds them above their children.
func (r *streamRead) splitSet() []source.Split {
	runnable := map[string]bool{}
	for _, id := range r.runnable() {
		runnable[id] = true
	}
	var out []source.Split
	for _, id := range slices.Sorted(maps.Keys(r.listed)) {
		st := r.shards[id]
		if runnable[id] || st.Done && !r.settled(id, runnable) {
			out = append(out, source.Split{ID: id, State: encodeShard(st)})
		}
	}
	return out
}

// settled says a done shard has children, all of them runnable or done.
func (r *streamRead) settled(id string, runnable map[string]bool) bool {
	children := 0
	for c, sh := range r.listed {
		if slices.Contains(parents(sh), id) {
			if !runnable[c] && !r.shards[c].Done {
				return false
			}
			children++
		}
	}
	return children > 0
}

// Splits are the stream's shards that can be read now, from where the last
// read ended. A shard whose parents aren't read to their ends isn't a split
// until they are: it joins with its last parent's completion.
func (s *Source) Splits(ctx context.Context, stream source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	start := time.Now()
	shards, err := splitShards(ended, start)
	if err != nil {
		return nil, err
	}
	cl, err := s.cfg.client(ctx)
	if err != nil {
		return nil, err
	}
	r := &streamRead{name: stream.Name, shards: shards}
	if err := s.position(ctx, cl, r, start); err != nil {
		return nil, err
	}
	return r.splitSet(), nil
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
		if r.splits != nil {
			st.Done = true
			r.shards[id] = st
			continue
		}
		delete(r.shards, id)
	}
	if r.splits != nil {
		return nil
	}
	for _, id := range slices.Sorted(maps.Keys(r.shards)) {
		r.doneAbove(id)
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
	r.mu.Lock()
	r.listed = listed
	r.mu.Unlock()
	return nil
}

// runnable are the shards that can start polling: not done, not running or
// stopped in this read, and whose parents are done or gone.
func (r *streamRead) runnable() []string {
	var out []string
	for _, id := range slices.Sorted(maps.Keys(r.listed)) {
		st := r.shards[id]
		if st.Done || r.running[id] || r.stopped[id] {
			continue
		}
		sh := r.listed[id]
		if !r.parentDone(sh.ParentShardId) || !r.parentDone(sh.AdjacentParentShardId) {
			continue
		}
		out = append(out, id)
	}
	return out
}

// take marks the runnable shards as running. Those new to the read join it.
func (r *streamRead) take() ([]string, []source.Split) {
	start := r.runnable()
	var added []source.Split
	for _, id := range start {
		r.running[id] = true
		if _, known := r.shards[id]; !known {
			st := shardState{Since: time.Now()}
			r.shards[id] = st
			added = append(added, source.Split{ID: id, State: encodeShard(st)})
		}
	}
	return start, added
}

func (r *streamRead) parentDone(parent *string) bool {
	if parent == nil {
		return true
	}
	if _, listed := r.listed[*parent]; !listed && !r.running[*parent] {
		return true
	}
	return r.shards[*parent].Done
}

// poll reads one shard on its own goroutine, emitting what each GetRecords
// returns, until the shard ends, a bounded read stops, or ctx is done.
func (rd *reading) poll(ctx context.Context, r *streamRead, shard string) error {
	r.mu.Lock()
	st := r.shards[shard]
	sh := r.listed[shard]
	r.mu.Unlock()
	if st.Done {
		return rd.finish(ctx, r, shard, st, true)
	}
	closed := sh.SequenceNumberRange != nil && sh.SequenceNumberRange.EndingSequenceNumber != nil
	failed := func(err error) error {
		return classify(fmt.Errorf("kinesis: %s's shard %s: %w", r.name, shard, err))
	}
	iterator := func() (*string, error) {
		in := &awskinesis.GetShardIteratorInput{StreamName: aws.String(r.name), ShardId: aws.String(shard)}
		switch {
		case st.After != "":
			in.ShardIteratorType, in.StartingSequenceNumber = types.ShardIteratorTypeAfterSequenceNumber, aws.String(st.After)
		case !st.From.IsZero():
			in.ShardIteratorType, in.Timestamp = types.ShardIteratorTypeAtTimestamp, aws.Time(st.From)
		default:
			in.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
		}
		res, err := rd.cl.GetShardIterator(ctx, in)
		if err != nil {
			return nil, err
		}
		return res.ShardIterator, nil
	}
	it, err := iterator()
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return failed(err)
	}
	em := rd.emitter(r, shard)
	var lastResume time.Time
	sinceChanged := false
	for {
		at := time.Now()
		res, err := rd.cl.GetRecords(ctx, &awskinesis.GetRecordsInput{ShardIterator: it, Limit: aws.Int32(getRecordsLimit)})
		if ctx.Err() != nil {
			return nil
		}
		if apiCode(err) == "ExpiredIteratorException" {
			if it, err = iterator(); err != nil {
				return failed(err)
			}
			continue
		}
		if err != nil {
			return failed(err)
		}
		records := res.Records
		behind := time.Duration(aws.ToInt64(res.MillisBehindLatest)) * time.Millisecond
		stop, ended := false, false
		if rd.bounded {
			if i := slices.IndexFunc(records, func(rec types.Record) bool { return aws.ToTime(rec.ApproximateArrivalTimestamp).After(rd.start) }); i >= 0 {
				records, stop = records[:i], true
			}
		}
		if !stop {
			ended = res.NextShardIterator == nil
			stop = rd.bounded && !closed && behind == 0 && len(res.Records) == 0
		}
		rows := make([]json.RawMessage, 0, len(records))
		for i := range records {
			row, err := rd.s.row(ctx, shard, &records[i])
			if err != nil {
				return err
			}
			rows = append(rows, row)
		}
		if n := len(records); n > 0 {
			last := records[n-1]
			st.After = aws.ToString(last.SequenceNumber)
			if a := aws.ToTime(last.ApproximateArrivalTimestamp); a.After(st.Since) {
				st.Since = a
			}
		}
		if behind == 0 && len(records) == 0 && at.After(st.Since) {
			st.Since = at
			sinceChanged = true
		}
		resume := !ended && !stop && (len(records) > 0 || sinceChanged && time.Since(lastResume) > idleResumeEvery)
		if err := r.emit(em, shard, st, behind, rows, resume); err != nil {
			return err
		}
		if ended || stop {
			return rd.finish(ctx, r, shard, st, ended)
		}
		if resume {
			lastResume, sinceChanged = time.Now(), false
		}
		it = res.NextShardIterator
		wait := minPoll - time.Since(at)
		if len(res.Records) == 0 {
			wait = idlePoll
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

func (r *streamRead) progress(now time.Time) source.Progress {
	r.mu.Lock()
	defer r.mu.Unlock()
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
