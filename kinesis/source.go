package kinesis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
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
	// Where a shard's first read starts: earliest (default), latest or
	// timestamp, at StartTimestamp.
	StartPosition  string     `json:"startPosition"`
	StartTimestamp *time.Time `json:"startTimestamp"`
	Consumer       Consumer   `json:"consumer"`
	Polling        Polling    `json:"polling"`
	// Split records the Kinesis Producer Library aggregated into their user
	// records. Default true.
	Deaggregate *bool `json:"deaggregate"`
}

// Consumer is how shards are read.
type Consumer struct {
	// polling (default): GetRecords, sharing each shard's 2 MB/s with other
	// readers. efo: enhanced fan-out, a registered consumer with 2 MB/s of
	// its own per shard, pushed over SubscribeToShard.
	Type string `json:"type"`
	// The efo consumer's name. Empty: openetl-PIPELINE.
	Name string `json:"name"`
	// Deregister the efo consumer once the pipeline is deleted. Default
	// true.
	DeregisterOnTeardown *bool `json:"deregisterOnTeardown"`
}

// Polling is how often GetRecords is called, per shard.
type Polling struct {
	MaxRecords   int32    `json:"maxRecords"`
	Interval     Duration `json:"interval"`
	IdleInterval Duration `json:"idleInterval"`
}

const (
	startEarliest  = "earliest"
	startLatest    = "latest"
	startTimestamp = "timestamp"

	consumerPolling = "polling"
	consumerEFO     = "efo"
)

// The columns every row has, besides its data's fields.
const (
	shardColumn        = "_kinesis_shard"
	sequenceColumn     = "_kinesis_sequence"
	subsequenceColumn  = "_kinesis_subsequence"
	arrivalColumn      = "_kinesis_arrival"
	partitionKeyColumn = "_kinesis_partition_key"
)

const (
	maxGetRecords = 10000
	// Kinesis allows five GetRecords a second per shard.
	minPollInterval = 200 * time.Millisecond
	// How often a shard that ended waits between listings for its children.
	listEvery = time.Second
	// How often a caught-up stream's state is emitted, to keep its time
	// current.
	idleResumeEvery = time.Minute
	progressEvery   = 10 * time.Second
	// How many empty GetRecords the retention check follows from a shard's
	// trim horizon before it counts the shard as empty.
	trimHorizonCalls = 20
	// How many listings a shard that ended waits for its children through.
	listTries = 5
	// The most rows one Rows call carries.
	rowsPerEmit = 5000
	// How far behind the runner's clock Kinesis's may be: a start at a time
	// the runner's clock has passed, which Kinesis refuses as later than its
	// own, is tried again every skewEvery until then. AWS refuses requests
	// signed further off.
	maxClockSkew = 5 * time.Minute
	skewEvery    = time.Second
)

type Source struct {
	cfg     SourceConfig
	pattern *regexp.Regexp
	reg     *sr.Client
	values  *records.Decoder
}

var (
	_ source.Source     = (*Source)(nil)
	_ source.Splitter   = (*Source)(nil)
	_ source.Teardowner = (*Source)(nil)
)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{StartPosition: startEarliest}
	if err := decodeConfig(config, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, invalid(err)
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

// validate checks the config and fills in its defaults.
func (c *SourceConfig) validate() error {
	if err := c.Connection.validate(records.JSON, records.Avro, records.Protobuf, records.String); err != nil {
		return err
	}
	switch c.StartPosition {
	case startEarliest, startLatest:
		if c.StartTimestamp != nil {
			return fmt.Errorf("kinesis: startTimestamp needs startPosition timestamp, not %s", c.StartPosition)
		}
	case startTimestamp:
		if c.StartTimestamp == nil {
			return errors.New("kinesis: startPosition timestamp needs startTimestamp")
		}
	default:
		return fmt.Errorf("kinesis: startPosition %q isn't earliest, latest or timestamp", c.StartPosition)
	}
	switch c.Consumer.Type {
	case "":
		c.Consumer.Type = consumerPolling
	case consumerPolling, consumerEFO:
	default:
		return fmt.Errorf("kinesis: consumer.type %q isn't polling or efo", c.Consumer.Type)
	}
	if c.Consumer.Name != "" && !validConsumerName(c.Consumer.Name) {
		return fmt.Errorf("kinesis: consumer.name %q isn't 1 to 128 of a-z, A-Z, 0-9, _, . and -", c.Consumer.Name)
	}
	if c.Consumer.Type != consumerEFO && c.Consumer.Name != "" {
		return errors.New("kinesis: consumer.name needs consumer.type efo")
	}
	p := &c.Polling
	if p.MaxRecords == 0 {
		p.MaxRecords = maxGetRecords
	}
	if p.Interval == 0 {
		p.Interval = Duration(minPollInterval)
	}
	if p.IdleInterval == 0 {
		p.IdleInterval = max(Duration(time.Second), p.Interval)
	}
	switch {
	case p.MaxRecords < 1 || p.MaxRecords > maxGetRecords:
		return fmt.Errorf("kinesis: polling.maxRecords %d isn't between 1 and %d", p.MaxRecords, maxGetRecords)
	case time.Duration(p.Interval) < minPollInterval:
		return fmt.Errorf("kinesis: polling.interval %s is under %s, Kinesis's five GetRecords a second per shard", time.Duration(p.Interval), minPollInterval)
	case p.IdleInterval < p.Interval:
		return errors.New("kinesis: polling.idleInterval is under polling.interval")
	}
	return nil
}

func (c *SourceConfig) deaggregate() bool { return c.Deaggregate == nil || *c.Deaggregate }

// consumerName is the efo consumer a pipeline reads with: consumer.name, or
// openetl- and the pipeline's name. Where the pipeline's name has characters
// a consumer's can't, or is too long, they're replaced or cut, and a hash of
// the whole name follows, so two pipelines' names don't make one consumer's.
func (c *SourceConfig) consumerName(pipeline string) (string, error) {
	if c.Consumer.Name != "" {
		return c.Consumer.Name, nil
	}
	if pipeline == "" {
		return "", invalid(errors.New("kinesis: consumer.type efo needs consumer.name, or a pipeline to name the consumer after"))
	}
	const prefix, most = "openetl-", 128
	b := []byte(prefix)
	for _, r := range pipeline {
		if r < 0x80 && consumerNameChar(byte(r)) {
			b = append(b, byte(r))
		} else {
			b = append(b, '-')
		}
	}
	if string(b[len(prefix):]) == pipeline && len(b) <= most {
		return string(b), nil
	}
	sum := sha256.Sum256([]byte(pipeline))
	tag := "-" + hex.EncodeToString(sum[:4])
	return string(b[:min(len(b), most-len(tag))]) + tag, nil
}

func validConsumerName(n string) bool {
	if n == "" || len(n) > 128 {
		return false
	}
	for i := range len(n) {
		if !consumerNameChar(n[i]) {
			return false
		}
	}
	return true
}

func consumerNameChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-'
}

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
	names, err := s.streams(ctx, cl, 0)
	if err != nil {
		return source.Catalog{}, err
	}
	var cat source.Catalog
	for _, name := range names {
		sc, err := s.declared(ctx, name)
		if err != nil {
			return source.Catalog{}, err
		}
		cat.Streams = append(cat.Streams, source.CatalogStream{
			Name: name, CursorField: sequenceColumn, Replay: source.Exact, Schema: sc,
			SyncModes: []source.SyncMode{source.Stream},
		})
	}
	return cat, nil
}

// declared is a stream's columns: the record's own, and the data's fields
// as the registry's latest value schema has them.
func (s *Source) declared(ctx context.Context, stream string) (*arrow.Schema, error) {
	fields := []arrow.Field{
		{Name: shardColumn, Type: arrow.BinaryTypes.String},
		{Name: sequenceColumn, Type: arrow.BinaryTypes.String},
		{Name: subsequenceColumn, Type: arrow.PrimitiveTypes.Int64},
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
	// startPosition latest or timestamp: records that arrived at or after
	// this.
	From time.Time `json:"from,omitzero"`
	// Read to its end.
	Done bool `json:"done,omitempty"`
	// Nothing after the position arrived before this: the last record's
	// arrival, or when the read last caught up.
	Since time.Time `json:"since,omitzero"`
	// Since is when the read last caught up. A bounded read moves it again
	// only once it's a minute old, so a read again from where one stopped
	// soon after completes where that one did.
	Checked bool `json:"checked,omitempty"`
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

// upTo is how many of recs, what a fetch returned after the sequence number
// after, a read that stops at end keeps, and whether it reaches end with
// them. A record past end that doesn't follow end itself shows that end is
// gone: lost.
func upTo(recs []types.Record, after, end string) (keep int, reached, lost bool) {
	i := slices.IndexFunc(recs, func(rec types.Record) bool { return cmpSeq(aws.ToString(rec.SequenceNumber), end) > 0 })
	if i < 0 {
		n := len(recs)
		return n, n > 0 && aws.ToString(recs[n-1].SequenceNumber) == end, false
	}
	if i > 0 {
		after = aws.ToString(recs[i-1].SequenceNumber)
	}
	if after != end {
		return 0, false, true
	}
	return i, true, false
}

// stopOf is where a read that isn't continuous stops: its until, or its
// complete_at, which is never set with it. Both are states this source
// emitted, at a resume point or a completion. Nil: where the read would end.
func stopOf(until, completeAt []byte) []byte {
	if until != nil {
		return until
	}
	return completeAt
}

func positionLost(err error) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, err)
}

// streamRead is one stream's read.
type streamRead struct {
	name      string
	arn       string
	retention time.Duration
	// The efo consumer's ARN; empty when polling.
	consumer string
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
	// Where a bounded read stops, where the request says: each shard's
	// position, read up to and including its After. A stream read whole
	// reads nothing of a shard its stop doesn't have. Nil: where the read
	// catches up.
	until map[string]shardState
	// The stops as the request gave them, which the read completes at: the
	// whole stream's, or each split's by ID.
	untilWhole []byte
	untilSplit map[string][]byte
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
	// A token for each split that may fetch and emit at once: the read's
	// lanes. A stream read whole is one lane, its shards fetching together.
	lanes chan struct{}
	// SubscribeToShard, for efo.
	subscribe func(context.Context, *awskinesis.SubscribeToShardInput) (eventStream, error)
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	if len(req.Streams) == 0 {
		return nil
	}
	cl, err := s.cfg.client(ctx)
	if err != nil {
		return err
	}
	rd := &reading{s: s, cl: cl, out: out, bounded: !req.Continuous, start: time.Now(), lanes: make(chan struct{}, max(req.Lanes, 1)), subscribe: subscriber(cl)}
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
		if rd.bounded {
			if err := r.stops(st); err != nil {
				return err
			}
		}
		if err := s.position(ctx, cl, r, rd.start); err != nil {
			return err
		}
		if r.splits != nil {
			r.settleSplits(rd.bounded)
		}
		if s.cfg.Consumer.Type == consumerEFO {
			name, err := s.cfg.consumerName(req.Pipeline)
			if err != nil {
				return err
			}
			if r.consumer, err = s.register(ctx, cl, r.arn, name); err != nil {
				return err
			}
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

// stops takes where a bounded read stops from the request.
func (r *streamRead) stops(st source.ConfiguredStream) error {
	if r.splits == nil {
		stop := stopOf(st.Until, st.CompleteAt)
		if stop == nil {
			return nil
		}
		shards, err := parseState(stop)
		if err != nil {
			return err
		}
		r.until, r.untilWhole = map[string]shardState{}, stop
		maps.Copy(r.until, shards)
		return nil
	}
	for _, sp := range st.Splits {
		stop := stopOf(sp.Until, sp.CompleteAt)
		if stop == nil {
			continue
		}
		var u shardState
		if err := json.Unmarshal(stop, &u); err != nil {
			return fmt.Errorf("kinesis: where split %s stops: %w", sp.ID, err)
		}
		if r.until == nil {
			r.until, r.untilSplit = map[string]shardState{}, map[string][]byte{}
		}
		r.until[sp.ID], r.untilSplit[sp.ID] = u, stop
	}
	return nil
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

// emit sends what one fetch of shard returned, and moves it to st.
func (r *streamRead) emit(em source.SplitEmitter, shard string, st shardState, behind time.Duration, rows []json.RawMessage, resume bool) error {
	if r.splits == nil {
		r.serial.Lock()
		defer r.serial.Unlock()
	}
	for len(rows) > 0 {
		n := min(len(rows), rowsPerEmit)
		if err := em.Rows(rows[:n]); err != nil {
			return err
		}
		rows = rows[n:]
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
	stop := r.untilSplit[shard]
	r.mu.Lock()
	r.shards[shard] = st
	delete(r.running, shard)
	delete(r.behind, shard)
	r.stopped[shard] = true
	var start []string
	var added []source.Split
	switch {
	case ended && stop != nil:
		// A split that stops where the request says adds no splits: only
		// the read's own start.
		for _, id := range r.runnable() {
			if r.splits[id] {
				r.running[id] = true
				start = append(start, id)
			}
		}
	case ended:
		start, added = r.take()
	}
	state := r.stateOf(shard)
	last := rd.bounded && len(r.running) == 0
	r.mu.Unlock()
	em := rd.emitter(r, shard)
	var err error
	switch {
	case stop != nil:
		err = completeAt(em, stop)
	case r.splits == nil:
		err = em.ResumePoint(state)
	default:
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
		em := rd.out.Split(r.name, source.WholeStream)
		if r.untilWhole != nil {
			return completeAt(em, r.untilWhole)
		}
		return em.Complete(encodeState(r.shards))
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

// completeAt completes a read at the stop its request gave, after a resume
// point there.
func completeAt(em source.SplitEmitter, stop []byte) error {
	if err := em.ResumePoint(stop); err != nil {
		return err
	}
	return em.Complete(stop)
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
		case <-time.After(listEvery):
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
	r.arn = aws.ToString(sum.StreamDescriptionSummary.StreamARN)
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
		if u, ok := r.until[id]; ok {
			// Where the read that reached the stop started the shard.
			st.From = u.From
		} else if fresh {
			switch s.cfg.StartPosition {
			case startLatest:
				if sh.SequenceNumberRange != nil && sh.SequenceNumberRange.EndingSequenceNumber != nil {
					st.Done = true
				} else {
					st.From = start
				}
			case startTimestamp:
				st.From = *s.cfg.StartTimestamp
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
	th := throttle{retry: &s.cfg.Retry}
	for {
		res, err := cl.ListShards(ctx, in)
		if throttled(err) {
			if err := th.wait(ctx, err); err != nil {
				return classify(fmt.Errorf("kinesis: list %s's shards: %w", r.name, err))
			}
			continue
		}
		if err != nil {
			return classify(fmt.Errorf("kinesis: list %s's shards: %w", r.name, err))
		}
		th.ok()
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
		if _, ok := r.until[id]; r.untilWhole != nil && !ok {
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

// lane waits for a lane for one fetch of a split, and returns its release.
// The shards of a stream read whole share the stream's lane, so they don't
// wait.
func (rd *reading) lane(ctx context.Context, r *streamRead) (func(), error) {
	if r.splits == nil {
		return func() {}, nil
	}
	select {
	case rd.lanes <- struct{}{}:
		return func() { <-rd.lanes }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// shardRead is one shard's read, on its own goroutine.
type shardRead struct {
	rd    *reading
	r     *streamRead
	shard string
	em    source.SplitEmitter
	// Where the read is.
	st shardState
	// The shard was closed when listed, so a bounded read goes to its end.
	closed bool
	// The shard has no more records.
	ended bool

	lastResume   time.Time
	sinceChanged bool
	th           throttle
}

// poll reads one shard, emitting what each fetch returns, until the shard
// ends, a bounded read stops, or ctx is done.
func (rd *reading) poll(ctx context.Context, r *streamRead, shard string) error {
	r.mu.Lock()
	st := r.shards[shard]
	sh := r.listed[shard]
	r.mu.Unlock()
	if st.Done {
		return rd.finish(ctx, r, shard, st, true)
	}
	// The read is at its stop, or the read that set it read nothing of the
	// shard.
	if u, ok := r.until[shard]; ok && (u.After == "" || st.After != "" && cmpSeq(st.After, u.After) >= 0) {
		return rd.finish(ctx, r, shard, st, u.Done)
	}
	sr := &shardRead{
		rd: rd, r: r, shard: shard, em: rd.emitter(r, shard), st: st,
		closed: sh.SequenceNumberRange != nil && sh.SequenceNumberRange.EndingSequenceNumber != nil,
		th:     throttle{retry: &rd.s.cfg.Retry},
	}
	var over bool
	var err error
	if r.consumer != "" {
		over, err = sr.subscribe(ctx)
	} else {
		over, err = sr.getRecords(ctx)
	}
	switch {
	case ctx.Err() != nil:
		return nil
	case err != nil:
		return err
	case over:
		return rd.finish(ctx, r, shard, sr.st, sr.ended)
	}
	return nil
}

// startRefused is err, or if Kinesis refused to start the shard's read at
// startTimestamp, later than the runner's clock, a config error that says
// so: Kinesis refuses a time later than its clock.
func (sr *shardRead) startRefused(err error, at types.ShardIteratorType) error {
	if at != types.ShardIteratorTypeAtTimestamp || apiCode(err) != "InvalidArgumentException" || !sr.st.From.After(time.Now()) {
		return err
	}
	return invalid(fmt.Errorf("startTimestamp %s is later than Kinesis's clock, and a read starts only at a time that has passed: set startTimestamp to a past time, or startPosition to latest: %w", sr.st.From.UTC().Format(time.RFC3339), err))
}

// skewed says Kinesis refused to start the shard's read at a time that the
// runner's clock passed less than maxClockSkew ago: Kinesis's clock is
// behind the runner's, and takes the time once it has passed it too.
func (sr *shardRead) skewed(err error, at types.ShardIteratorType) bool {
	return at == types.ShardIteratorTypeAtTimestamp && apiCode(err) == "InvalidArgumentException" &&
		!sr.st.From.After(time.Now()) && time.Since(sr.st.From) < maxClockSkew
}

func (sr *shardRead) failed(err error) error {
	if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
		return fmt.Errorf("kinesis: %s's shard %s: %w", sr.r.name, sr.shard, err)
	}
	return classify(fmt.Errorf("kinesis: %s's shard %s: %w", sr.r.name, sr.shard, err))
}

// batch handles what one fetch at at returned: it emits the records and
// moves the shard's position. ended: the shard has nothing after them. It
// reports whether the shard's read is over.
func (sr *shardRead) batch(ctx context.Context, at time.Time, recs []types.Record, behind time.Duration, ended bool) (bool, error) {
	rd := sr.rd
	stop := false
	u, stops := sr.r.until[sr.shard]
	switch {
	case stops:
		keep, reached, lost := upTo(recs, sr.st.After, u.After)
		if lost || !reached && (ended || !sr.closed && behind == 0 && len(recs) == 0) {
			return false, positionLost(fmt.Errorf("kinesis: %s's shard %s no longer holds record %s, where the read stops: retention removed it", sr.r.name, sr.shard, u.After))
		}
		recs, stop = recs[:keep], reached
		ended = reached && u.Done
	case rd.bounded:
		if i := slices.IndexFunc(recs, func(rec types.Record) bool { return aws.ToTime(rec.ApproximateArrivalTimestamp).After(rd.start) }); i >= 0 {
			recs, stop, ended = recs[:i], true, false
		}
		if !stop {
			stop = !sr.closed && behind == 0 && len(recs) == 0
		}
	}
	rows, err := rd.s.rows(ctx, sr.shard, recs)
	if err != nil {
		return false, err
	}
	st := &sr.st
	if n := len(recs); n > 0 {
		last := recs[n-1]
		st.After = aws.ToString(last.SequenceNumber)
		if a := aws.ToTime(last.ApproximateArrivalTimestamp); a.After(st.Since) {
			st.Since, st.Checked = a, false
		}
	}
	if behind == 0 && len(recs) == 0 && at.After(st.Since) && (!rd.bounded || !st.Checked || at.Sub(st.Since) > idleResumeEvery) {
		st.Since, st.Checked = at, true
		sr.sinceChanged = true
	}
	resume := !ended && !stop && (len(recs) > 0 || sr.sinceChanged && time.Since(sr.lastResume) > idleResumeEvery)
	if err := sr.r.emit(sr.em, sr.shard, *st, behind, rows, resume); err != nil {
		return false, err
	}
	if resume {
		sr.lastResume, sr.sinceChanged = time.Now(), false
	}
	sr.ended = ended
	return ended || stop, nil
}

// iterator is a shard iterator from where the read is.
func (sr *shardRead) iterator(ctx context.Context) (*string, error) {
	in := &awskinesis.GetShardIteratorInput{StreamName: aws.String(sr.r.name), ShardId: aws.String(sr.shard)}
	switch {
	case sr.st.After != "":
		in.ShardIteratorType, in.StartingSequenceNumber = types.ShardIteratorTypeAfterSequenceNumber, aws.String(sr.st.After)
	case !sr.st.From.IsZero():
		in.ShardIteratorType, in.Timestamp = types.ShardIteratorTypeAtTimestamp, aws.Time(sr.st.From)
	default:
		in.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
	}
	for {
		res, err := sr.rd.cl.GetShardIterator(ctx, in)
		if throttled(err) {
			if err := sr.th.wait(ctx, err); err != nil {
				return nil, sr.failed(err)
			}
			continue
		}
		if sr.skewed(err, in.ShardIteratorType) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(skewEvery):
			}
			continue
		}
		if err != nil {
			return nil, sr.failed(sr.startRefused(err, in.ShardIteratorType))
		}
		sr.th.ok()
		return res.ShardIterator, nil
	}
}

// getRecords polls the shard with GetRecords.
func (sr *shardRead) getRecords(ctx context.Context) (bool, error) {
	p := sr.rd.s.cfg.Polling
	it, err := sr.iterator(ctx)
	if err != nil {
		return false, err
	}
	for {
		release, err := sr.rd.lane(ctx, sr.r)
		if err != nil {
			return false, nil
		}
		at := time.Now()
		res, err := sr.rd.cl.GetRecords(ctx, &awskinesis.GetRecordsInput{ShardIterator: it, Limit: aws.Int32(p.MaxRecords)})
		if err != nil {
			release()
			switch {
			case ctx.Err() != nil:
				return false, nil
			case apiCode(err) == "ExpiredIteratorException":
				if it, err = sr.iterator(ctx); err != nil {
					return false, err
				}
			case throttled(err):
				if err := sr.th.wait(ctx, err); err != nil {
					return false, sr.failed(err)
				}
			default:
				return false, sr.failed(err)
			}
			continue
		}
		sr.th.ok()
		over, err := sr.batch(ctx, at, res.Records, time.Duration(aws.ToInt64(res.MillisBehindLatest))*time.Millisecond, res.NextShardIterator == nil)
		release()
		if err != nil || over {
			return over, err
		}
		it = res.NextShardIterator
		wait := time.Duration(p.Interval) - time.Since(at)
		if len(res.Records) == 0 {
			wait = time.Duration(p.IdleInterval)
		}
		select {
		case <-ctx.Done():
			return false, nil
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

// rows are records as rows: each record's, or each of its user records' if
// the Kinesis Producer Library aggregated it.
func (s *Source) rows(ctx context.Context, shard string, recs []types.Record) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(recs))
	for i := range recs {
		rec := &recs[i]
		if aggregated(rec.Data) {
			if !s.cfg.deaggregate() {
				return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, fmt.Errorf("kinesis: shard %s sequence %s: a record the Kinesis Producer Library aggregated, and deaggregate is off", shard, aws.ToString(rec.SequenceNumber)))
			}
			users, err := deaggregate(rec.Data)
			if err != nil {
				return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, fmt.Errorf("kinesis: shard %s sequence %s: an aggregated record: %w", shard, aws.ToString(rec.SequenceNumber), err))
			}
			for j, u := range users {
				row, err := s.row(ctx, shard, rec, j, u.partitionKey, u.data)
				if err != nil {
					return nil, err
				}
				out = append(out, row)
			}
			continue
		}
		row, err := s.row(ctx, shard, rec, 0, aws.ToString(rec.PartitionKey), rec.Data)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

// row is a record, or its user record sub, as a JSON object: its data's
// fields, or its data as "value", and the record's own columns.
func (s *Source) row(ctx context.Context, shard string, rec *types.Record, sub int, key string, data []byte) (json.RawMessage, error) {
	seq := aws.ToString(rec.SequenceNumber)
	b := make([]byte, 0, len(data)+180)
	b = append(b, '{')
	if len(data) > 0 {
		v, err := s.values.Decode(ctx, data)
		if err != nil {
			return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, fmt.Errorf("kinesis: shard %s sequence %s: %w", shard, seq, err))
		}
		b = records.AppendFields(b, v)
	}
	for _, kv := range [][2]string{
		{shardColumn, shard},
		{sequenceColumn, seq},
		{arrivalColumn, aws.ToTime(rec.ApproximateArrivalTimestamp).UTC().Format(time.RFC3339Nano)},
		{partitionKeyColumn, key},
	} {
		b = records.AppendJSONString(b, kv[0])
		b = append(b, ':')
		b = records.AppendJSONString(b, kv[1])
		b = append(b, ',')
	}
	b = records.AppendJSONString(b, subsequenceColumn)
	b = append(b, ':')
	b = strconv.AppendInt(b, int64(sub), 10)
	b = append(b, '}')
	return b, nil
}
