package dynamodb

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"golang.org/x/sync/errgroup"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

const (
	// How long a DynamoDB stream keeps a record.
	retention = 24 * time.Hour
	// Records this long before a snapshot starts are read from the stream
	// again: a record's ApproximateCreationDateTime is rounded down, to the
	// minute on DynamoDB Local, and clocks differ.
	skipMargin = 5 * time.Minute
	// A seq is a snapshot's generation times 10^seqDigits, plus the
	// sequence number; 38 digits in all.
	seqDigits = 29
	// A bounded read counts an open shard as read once this many
	// GetRecords in a row return nothing: an empty page doesn't prove
	// there's nothing after it, but what the read leaves the next one
	// reads.
	emptyPages = 3
	// Pauses between GetRecords: DynamoDB Streams allows five a second per
	// shard.
	minPoll, idlePoll = 250 * time.Millisecond, time.Second
	// How often a continuous read lists shards when none is open, and moves
	// a quiet stream's resume point.
	listEvery, idleResumeEvery = 10 * time.Second, 30 * time.Second
	progressEvery              = 10 * time.Second
)

// epoch2020 starts the clock of a snapshot's generation.
var epoch2020 = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// streamsAPI is what a cdc read calls DynamoDB Streams with.
type streamsAPI interface {
	DescribeStream(context.Context, *dynamodbstreams.DescribeStreamInput, ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error)
	GetShardIterator(context.Context, *dynamodbstreams.GetShardIteratorInput, ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetShardIteratorOutput, error)
	GetRecords(context.Context, *dynamodbstreams.GetRecordsInput, ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error)
}

// cdcState is where a cdc read is: its snapshot's segments while the
// snapshot runs, then each shard's position.
type cdcState struct {
	// The stream's ARN: another means the stream was turned off and on, and
	// what came between is gone.
	Stream string `json:"stream"`
	// The snapshot's generation: seconds from 2020 to its start. Every seq
	// of this snapshot and the stream after it starts with it, so a later
	// snapshot's rows win over this one's changes.
	G int64 `json:"g"`
	// Stream records created before this are in the snapshot.
	Skip time.Time `json:"skip"`
	// The snapshot's segments, while it runs.
	Snapshot []segState            `json:"snapshot,omitempty"`
	Shards   map[string]shardState `json:"shards,omitempty"`
}

type shardState struct {
	// The last sequence number read. Empty: from the shard's oldest record.
	After string `json:"after,omitempty"`
	// Read to its end.
	Done bool `json:"done,omitempty"`
	// Nothing after the position was created before this.
	Since time.Time `json:"since,omitzero"`
	// Since is when a read found nothing after the position.
	Checked bool `json:"checked,omitempty"`
}

// shardInfo is a listed shard.
type shardInfo struct {
	parent string
	closed bool
}

// cdcRead reads one stream with cdc: the snapshot, then the stream's
// shards, each on a goroutine of its own once its parent is read to its
// end. Its emits are serial, each resume point covering every shard.
type cdcRead struct {
	s          *Source
	db         dbAPI
	streams    streamsAPI
	cs         source.ConfiguredStream
	t          *table
	out        source.SplitEmitter
	progress   source.Progresser
	continuous bool
	start      time.Time

	// A read until a resume point's state: where it stops, and that state.
	until    *cdcState
	untilRaw []byte

	mu       sync.Mutex
	st       cdcState
	listed   map[string]shardInfo
	running  map[string]bool
	stopped  map[string]bool
	lastEmit time.Time
}

func (s *Source) readCDC(ctx context.Context, cl *clients, cs source.ConfiguredStream, out source.Emitter, continuous bool) error {
	t, err := describe(ctx, cl, cs.Name)
	if err != nil {
		return err
	}
	if t.streamARN == "" {
		return configInvalid(fmt.Errorf("dynamodb: %s has no stream enabled, which cdc reads: enable one, with the view type NEW_AND_OLD_IMAGES", cs.Name))
	}
	r := &cdcRead{s: s, db: cl.db, streams: cl.streams, cs: cs, t: t, out: out.Split(cs.Name, source.WholeStream), continuous: continuous, start: now()}
	r.progress, _ = out.(source.Progresser)
	if cs.Until != nil && !continuous {
		r.until, r.untilRaw = &cdcState{}, cs.Until
		if err := json.Unmarshal(cs.Until, r.until); err != nil {
			return fmt.Errorf("dynamodb: until of %s: %w", cs.Name, err)
		}
	}
	switch {
	case len(cs.State) > 0:
		if err := json.Unmarshal(cs.State, &r.st); err != nil {
			return fmt.Errorf("dynamodb: state of %s: %w", cs.Name, err)
		}
		if r.st.Stream != t.streamARN {
			return positionLost(fmt.Errorf("dynamodb: %s's stream is %s, not the %s the read was reading: the stream was turned off and on, and the changes between are gone; the stream needs a re-sync", cs.Name, t.streamARN, r.st.Stream))
		}
	case r.until != nil:
		// The read that sent until began here: its snapshot's generation
		// and skip, so that this one sends the same rows.
		r.st = cdcState{Stream: r.until.Stream, G: r.until.G, Skip: r.until.Skip}
		r.st.Snapshot = make([]segState, cmp.Or(len(r.until.Snapshot), s.segments(t)))
		if r.st.Stream != t.streamARN {
			return positionLost(fmt.Errorf("dynamodb: %s's stream is %s, not the %s the read was reading: the stream needs a re-sync", cs.Name, t.streamARN, r.st.Stream))
		}
		if err := r.list(ctx); err != nil {
			return err
		}
		r.st.Shards = map[string]shardState{}
		for id := range r.listed {
			r.st.Shards[id] = shardState{Since: r.st.Skip}
		}
	default:
		r.st = cdcState{Stream: t.streamARN, G: int64(r.start.Sub(epoch2020) / time.Second), Skip: r.start.Add(-skipMargin)}
		r.st.Snapshot = make([]segState, s.segments(t))
		if err := r.list(ctx); err != nil {
			return err
		}
		r.st.Shards = map[string]shardState{}
		for id := range r.listed {
			r.st.Shards[id] = shardState{Since: r.st.Skip}
		}
	}
	if len(r.st.Snapshot) > 0 {
		if err := r.snapshot(ctx); err != nil {
			return err
		}
		if r.until != nil && len(r.until.Snapshot) > 0 {
			return completeAt(r.out, r.untilRaw)
		}
	}
	return r.follow(ctx)
}

func positionLost(err error) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, err)
}

// seq is a record's seq: the generation, then the sequence number, which
// orders a key's changes; 0 for the snapshot's rows.
func (r *cdcRead) seq(sequence string) (decimal128.Num, error) {
	n := new(big.Int)
	if sequence != "" {
		if _, ok := n.SetString(sequence, 10); !ok || n.Sign() < 0 || len(n.String()) > seqDigits {
			return decimal128.Num{}, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED,
				fmt.Errorf("dynamodb: %s's stream has a sequence number %q of more than %d digits, which the seq doesn't hold", r.cs.Name, sequence, seqDigits))
		}
	}
	g := new(big.Int).Mul(big.NewInt(r.st.G), new(big.Int).Exp(big.NewInt(10), big.NewInt(seqDigits), nil))
	return decimal128.FromBigInt(g.Add(g, n)), nil
}

// emit sends a batch's rows and a resume point with the state as it is
// after them, which update moves. Callers hold no lock.
func (r *cdcRead) emit(b *batcher, update func()) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := b.Flush(r.out); err != nil {
		return err
	}
	update()
	state, err := json.Marshal(r.st)
	if err != nil {
		return err
	}
	r.lastEmit = time.Now()
	return r.out.ResumePoint(state)
}

// snapshot scans the table, its segments in parallel, with consistent
// reads, each row an insert at the generation's seq.
func (r *cdcRead) snapshot(ctx context.Context) error {
	if now().Sub(r.st.Skip) > retention {
		return positionLost(fmt.Errorf("dynamodb: %s's snapshot started more than %v ago, and the stream no longer has every change since: the stream needs a re-sync", r.cs.Name, retention))
	}
	seq, err := r.seq("")
	if err != nil {
		return err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	for i, seg := range r.st.Snapshot {
		var until segState
		if r.until != nil && len(r.until.Snapshot) > 0 {
			until = r.until.Snapshot[i]
			at := until.After.attrs()
			if !until.Done && (at == nil || seg.After != nil && keyText(seg.After.attrs(), at) == keyText(at, at)) {
				continue
			}
		}
		if seg.Done {
			continue
		}
		g.Go(func() error {
			b := r.s.newBatcher(r.cs, r.t, true)
			defer b.Release()
			return r.s.scanPages(gctx, &clients{db: r.db}, r.t.name, i, len(r.st.Snapshot), seg.After.attrs(), nil, true, func(items []map[string]types.AttributeValue, last map[string]types.AttributeValue) (bool, error) {
				reached := false
				if until.After != nil {
					var n int
					n, reached = untilCut(items, last, until.After.attrs())
					items = items[:n]
				}
				if reached {
					last = until.After.attrs()
				}
				for _, it := range items {
					if err := b.add(it, source.OpInsert, seq, true); err != nil {
						return false, err
					}
				}
				return reached, r.emit(b, func() {
					r.st.Snapshot[i] = segState{After: itemOf(last), Done: last == nil}
				})
			})
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	r.mu.Lock()
	r.st.Snapshot = nil
	r.mu.Unlock()
	return nil
}

// list lists the stream's shards, and checks every shard the state has
// that isn't read to its end is still there.
func (r *cdcRead) list(ctx context.Context) error {
	listed := map[string]shardInfo{}
	in := &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(r.st.Stream)}
	for {
		res, err := r.streams.DescribeStream(ctx, in)
		if err != nil {
			if apiCode(err) == "ResourceNotFoundException" {
				return positionLost(fmt.Errorf("dynamodb: %s's stream %s is gone: the stream needs a re-sync", r.cs.Name, r.st.Stream))
			}
			return classify(fmt.Errorf("dynamodb: describe %s's stream: %w", r.cs.Name, err))
		}
		d := res.StreamDescription
		for _, sh := range d.Shards {
			listed[aws.ToString(sh.ShardId)] = shardInfo{
				parent: aws.ToString(sh.ParentShardId),
				closed: sh.SequenceNumberRange != nil && sh.SequenceNumberRange.EndingSequenceNumber != nil,
			}
		}
		if d.LastEvaluatedShardId == nil {
			break
		}
		in.ExclusiveStartShardId = d.LastEvaluatedShardId
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listed = listed
	for id, sh := range r.st.Shards {
		if _, ok := listed[id]; ok {
			continue
		}
		// A shard leaves the list once its records are past the stream's
		// 24 hours, and nothing after its position was created before
		// Since: if that's within 24 hours, there was nothing after it.
		if !sh.Done && now().Sub(sh.Since) >= retention {
			return positionLost(fmt.Errorf("dynamodb: %s's stream no longer has shard %s, which the read hadn't read to its end (at %q), and its changes since %s are gone; the stream needs a re-sync",
				r.cs.Name, id, sh.After, sh.Since.UTC().Format(time.RFC3339)))
		}
		delete(r.st.Shards, id)
	}
	if r.st.Shards == nil {
		return nil
	}
	// A shard new to the read has nothing from before its parent's
	// position; one without a parent the read knows, nothing from before
	// the oldest position the read has.
	oldest := r.st.Skip
	for _, sh := range r.st.Shards {
		if !sh.Done && sh.Since.Before(oldest) {
			oldest = sh.Since
		}
	}
	for _, id := range slices.Sorted(maps.Keys(listed)) {
		if _, ok := r.st.Shards[id]; ok {
			continue
		}
		since := oldest
		if p, ok := r.st.Shards[listed[id].parent]; ok {
			since = p.Since
		}
		r.st.Shards[id] = shardState{Since: since}
	}
	return nil
}

// runnable are the shards that can start: not read to their ends, not
// running or stopped, and whose parent is read to its end or gone.
func (r *cdcRead) runnable() []string {
	var out []string
	for _, id := range slices.Sorted(maps.Keys(r.st.Shards)) {
		sh := r.st.Shards[id]
		if sh.Done || r.running[id] || r.stopped[id] {
			continue
		}
		if u, ok := r.untilShard(id); ok && !u.Done && (u.After == "" || u.After == sh.After) {
			continue
		}
		if p, ok := r.st.Shards[r.listed[id].parent]; ok && !p.Done {
			continue
		}
		out = append(out, id)
	}
	return out
}

type shardEnd struct {
	id    string
	ended bool
	err   error
}

// follow reads the stream's shards, a child once its parent is read to its
// end. A bounded read completes once no shard can run; a continuous one
// lists shards again while none is open.
func (r *cdcRead) follow(ctx context.Context) error {
	if err := r.list(ctx); err != nil {
		return err
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	// Cancelled before the wait, so that shards still reading stop.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ends := make(chan shardEnd)
	r.running, r.stopped = map[string]bool{}, map[string]bool{}
	tick := time.NewTicker(listEvery)
	defer tick.Stop()
	report := time.NewTicker(progressEvery)
	defer report.Stop()
	for {
		r.mu.Lock()
		start := r.runnable()
		for _, id := range start {
			r.running[id] = true
		}
		idle := len(r.running) == 0
		r.mu.Unlock()
		for _, id := range start {
			wg.Go(func() {
				ended, err := r.poll(ctx, id)
				select {
				case ends <- shardEnd{id, ended, err}:
				case <-ctx.Done():
				}
			})
		}
		if idle && !r.continuous {
			return r.complete()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-ends:
			if e.err != nil {
				return e.err
			}
			r.mu.Lock()
			delete(r.running, e.id)
			if !e.ended {
				r.stopped[e.id] = true
			}
			r.mu.Unlock()
			if e.ended {
				if err := r.list(ctx); err != nil {
					return err
				}
			}
		case <-tick.C:
			if !r.continuous {
				continue
			}
			if idle {
				if err := r.list(ctx); err != nil {
					return err
				}
			}
			if err := r.idleResume(); err != nil {
				return err
			}
		case <-report.C:
			if r.progress != nil {
				if err := r.progress.Progress(r.lag(time.Now())); err != nil {
					return err
				}
			}
		}
	}
}

// idleResume sends a resume point when none has gone for a while, so the
// state's times, which retention is reckoned from, move with a quiet
// stream.
func (r *cdcRead) idleResume() error {
	r.mu.Lock()
	quiet := time.Since(r.lastEmit) > idleResumeEvery
	r.mu.Unlock()
	if !quiet {
		return nil
	}
	b := &batcher{}
	return r.emit(b, func() {})
}

// untilShard is where a read until stops in shard id: ok, with a zero
// position for a shard the read until doesn't read.
func (r *cdcRead) untilShard(id string) (shardState, bool) {
	if r.until == nil {
		return shardState{}, false
	}
	return r.until.Shards[id], true
}

func (r *cdcRead) complete() error {
	if r.until != nil {
		return completeAt(r.out, r.untilRaw)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := json.Marshal(r.st)
	if err != nil {
		return err
	}
	return r.out.Complete(state)
}

func (r *cdcRead) lag(now time.Time) source.Progress {
	r.mu.Lock()
	defer r.mu.Unlock()
	lag := time.Duration(0)
	for _, sh := range r.st.Shards {
		if !sh.Done {
			lag = max(lag, now.Sub(sh.Since))
		}
	}
	left := max(0, retention-lag)
	return source.Progress{Stream: r.cs.Name, Lag: &lag, RetentionLeft: &left}
}

// poll reads one shard until it ends, a bounded read has read it, or ctx is
// done; ended says it was read to its end.
func (r *cdcRead) poll(ctx context.Context, id string) (ended bool, err error) {
	r.mu.Lock()
	st := r.st.Shards[id]
	closed := r.listed[id].closed
	until, bounded := r.untilShard(id)
	r.mu.Unlock()
	bounded = bounded && !until.Done
	if st.After == "" && now().Sub(st.Since) > retention {
		return false, positionLost(fmt.Errorf("dynamodb: %s's shard %s has records from before %s that the read hasn't read, and the stream keeps records for %v: some may be gone; the stream needs a re-sync",
			r.cs.Name, id, st.Since.UTC().Format(time.RFC3339), retention))
	}
	failed := func(err error) error {
		if trimmed(err) {
			return positionLost(fmt.Errorf("dynamodb: %s's shard %s no longer has the records after %q, and some created since %s may be gone: the stream needs a re-sync: %w",
				r.cs.Name, id, st.After, st.Since.UTC().Format(time.RFC3339), err))
		}
		return classify(fmt.Errorf("dynamodb: %s's shard %s: %w", r.cs.Name, id, err))
	}
	// A position the stream has trimmed loses nothing where nothing after it
	// was created before Since, within the stream's 24 hours: the read goes
	// on from the shard's oldest record, past the position.
	horizon, skipTo, retries := false, "", 0
	iterator := func() (*string, error) {
		in := &dynamodbstreams.GetShardIteratorInput{StreamArn: aws.String(r.st.Stream), ShardId: aws.String(id), ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon}
		if st.After != "" && !horizon {
			in.ShardIteratorType, in.SequenceNumber = streamtypes.ShardIteratorTypeAfterSequenceNumber, aws.String(st.After)
		}
		res, err := r.streams.GetShardIterator(ctx, in)
		if err != nil {
			return nil, err
		}
		return res.ShardIterator, nil
	}
	resume := func(err error) (*string, error) {
		if !trimmed(err) || now().Sub(st.Since) > retention || retries >= 3 {
			return nil, failed(err)
		}
		horizon, skipTo, retries = true, st.After, retries+1
		it, err := iterator()
		if err != nil {
			return nil, failed(err)
		}
		return it, nil
	}
	it, err := iterator()
	if err != nil {
		if ctx.Err() != nil {
			return false, nil
		}
		if it, err = resume(err); err != nil {
			return false, err
		}
	}
	b := r.s.newBatcher(r.cs, r.t, true)
	defer b.Release()
	b.gone = r.gone(ctx)
	empty := 0
	for {
		at, polled := now(), time.Now()
		if it == nil {
			return true, r.emit(b, func() { r.setShard(id, shardState{After: st.After, Done: true, Since: st.Since}) })
		}
		res, err := r.streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: it, Limit: aws.Int32(1000)})
		if ctx.Err() != nil {
			return false, nil
		}
		if apiCode(err) == "ExpiredIteratorException" {
			if it, err = iterator(); err != nil {
				if it, err = resume(err); err != nil {
					return false, err
				}
			}
			continue
		}
		if trimmed(err) {
			if it, err = resume(err); err != nil {
				return false, err
			}
			continue
		}
		if err != nil {
			return false, failed(err)
		}
		recs, stop := res.Records, false
		if horizon && skipTo != "" {
			recs = slices.DeleteFunc(slices.Clone(recs), func(rec streamtypes.Record) bool {
				return !seqAfter(aws.ToString(rec.Dynamodb.SequenceNumber), skipTo)
			})
		}
		if bounded {
			if i := slices.IndexFunc(recs, func(rec streamtypes.Record) bool {
				return aws.ToString(rec.Dynamodb.SequenceNumber) == until.After
			}); i >= 0 {
				recs, stop = recs[:i+1], true
			}
		} else if !r.continuous {
			// A bounded read reads what was there when it started.
			if i := slices.IndexFunc(recs, func(rec streamtypes.Record) bool {
				return rec.Dynamodb != nil && aws.ToTime(rec.Dynamodb.ApproximateCreationDateTime).After(r.start)
			}); i >= 0 {
				recs, stop = recs[:i], true
			}
		}
		for _, rec := range recs {
			if err := r.change(ctx, b, rec); err != nil {
				return false, err
			}
			horizon, retries = false, 0
			st.After = aws.ToString(rec.Dynamodb.SequenceNumber)
			if c := aws.ToTime(rec.Dynamodb.ApproximateCreationDateTime); c.After(st.Since) {
				st.Since, st.Checked = c, false
			}
		}
		checked := false
		if len(res.Records) == 0 {
			empty++
			// The read found nothing after the position up to now, which
			// it records, with a resume point, unless a read recorded so
			// within the last minute: so a read again soon after completes
			// where this one did, and Since lags by at most the minute.
			if empty >= emptyPages && at.After(st.Since) && (!st.Checked || at.Sub(st.Since) > checkEvery) {
				st.Since, st.Checked, checked = at, true, true
			}
		} else {
			empty = 0
		}
		next := shardState{After: st.After, Since: st.Since, Checked: st.Checked}
		if len(recs) > 0 || b.Rows > 0 || checked {
			if err := r.emit(b, func() { r.setShard(id, next) }); err != nil {
				return false, err
			}
		} else {
			r.mu.Lock()
			r.setShard(id, next)
			r.mu.Unlock()
		}
		if stop || !bounded && !r.continuous && !closed && empty >= emptyPages {
			return false, nil
		}
		it = res.NextShardIterator
		wait := minPoll - time.Since(polled)
		if len(res.Records) == 0 && it != nil {
			wait = idlePoll
			if !r.continuous {
				wait = minPoll
			}
		}
		select {
		case <-ctx.Done():
			return false, nil
		case <-time.After(wait):
		}
	}
}

// trimmed says the stream refused a read of records it has trimmed.
func trimmed(err error) bool { return apiCode(err) == "TrimmedDataAccessException" }

// seqAfter says sequence number a comes after b.
func seqAfter(a, b string) bool {
	return len(a) > len(b) || len(a) == len(b) && a > b
}

// checkEvery is how often a read records that it found nothing after a
// shard's position.
const checkEvery = time.Minute

// now is the clock retention is reckoned by.
var now = time.Now

// setShard sets a shard's position; the caller holds mu.
func (r *cdcRead) setShard(id string, st shardState) { r.st.Shards[id] = st }

// change adds a stream record's row: an insert's or a modify's item as the
// change left it, from its new image, or where the stream's view has none,
// read from the table, skipped if the item is gone, as a later remove
// says; a remove's keys, with the item it removed where the view has old
// images. Records from before the snapshot are skipped.
func (r *cdcRead) change(ctx context.Context, b *batcher, rec streamtypes.Record) error {
	d := rec.Dynamodb
	if d == nil {
		return nil
	}
	if c := d.ApproximateCreationDateTime; c != nil && c.Before(r.st.Skip) {
		return nil
	}
	seq, err := r.seq(aws.ToString(d.SequenceNumber))
	if err != nil {
		return err
	}
	keys, err := attributevalue.FromDynamoDBStreamsMap(d.Keys)
	if err != nil {
		return fmt.Errorf("dynamodb: a record of %s's stream: %w", r.cs.Name, err)
	}
	if rec.EventName == streamtypes.OperationTypeRemove {
		img, whole := keys, false
		if d.OldImage != nil {
			if img, err = attributevalue.FromDynamoDBStreamsMap(d.OldImage); err != nil {
				return fmt.Errorf("dynamodb: a record of %s's stream: %w", r.cs.Name, err)
			}
			whole = true
		}
		return b.add(img, source.OpDelete, seq, whole)
	}
	op := source.OpInsert
	if rec.EventName == streamtypes.OperationTypeModify {
		op = source.OpUpdate
	}
	var img map[string]types.AttributeValue
	if d.NewImage != nil {
		if img, err = attributevalue.FromDynamoDBStreamsMap(d.NewImage); err != nil {
			return fmt.Errorf("dynamodb: a record of %s's stream: %w", r.cs.Name, err)
		}
	} else if img, err = r.current(ctx, keys); err != nil || img == nil {
		return err
	}
	return b.add(img, op, seq, true)
}

// current is the item with keys as it is now; nil if there's none.
func (r *cdcRead) current(ctx context.Context, keys map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	res, err := r.db.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(r.t.name), Key: keys, ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, classify(fmt.Errorf("dynamodb: read an item of %s: %w", r.cs.Name, err))
	}
	return res.Item, nil
}

// gone says, for an attribute the run doesn't declare, whether the item no
// longer has it: then discovery, which reads the table, can't declare it
// for its sake, and the row leaves it out. Where it has, discovery finds
// it, so the read fails and starts again.
func (r *cdcRead) gone(ctx context.Context) func(map[string]types.AttributeValue, string) (bool, error) {
	return func(it map[string]types.AttributeValue, attr string) (bool, error) {
		keys := map[string]types.AttributeValue{}
		for _, k := range r.t.keys {
			keys[k] = it[k]
		}
		now, err := r.current(ctx, keys)
		if err != nil {
			return false, err
		}
		_, has := now[attr]
		return !has, nil
	}
}
