package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/openetl-io/openetl-connectors/records"
	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type SourceConfig struct {
	Connection
	// Empty: every topic but internal ones.
	Topics       []string `json:"topics"`
	TopicPattern string   `json:"topicPattern"`
	// earliest (default) or latest.
	StartOffset string `json:"startOffset"`
	// Default openetl-<pipeline>.
	Group string `json:"group"`
}

// The columns every row has, besides its value's fields.
const (
	partitionColumn = "_kafka_partition"
	offsetColumn    = "_kafka_offset"
	timestampColumn = "_kafka_timestamp"
	keyColumn       = "_kafka_key"
	headersColumn   = "_kafka_headers"
)

const (
	progressEvery = 10 * time.Second
	// How often a continuous read looks for new partitions.
	partitionsEvery = 30 * time.Second
	// The most records one poll takes, per lane.
	pollRecords = 5000
)

// Variables, for tests.
var (
	// A bounded read counts a partition with nothing left before its end
	// as caught up after this long without records.
	idleAfter = 10 * time.Second
	// A partition that stops at an offset, and has had no records for this
	// long without the brokers showing that nothing is left below it, fails
	// the read.
	stallAfter = 2 * time.Minute
)

type Source struct {
	cfg     SourceConfig
	pattern *regexp.Regexp
	reg     *sr.Client
	values  *records.Decoder
	keys    *records.Decoder

	mu       sync.Mutex
	pipeline string
	acker    *kgo.Client
}

var (
	_ source.Source       = (*Source)(nil)
	_ source.Acknowledger = (*Source)(nil)
	_ source.Splitter     = (*Source)(nil)
)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{StartOffset: "earliest"}
	if err := decodeConfig(config, &cfg); err != nil {
		return nil, err
	}
	invalid := func(err error) error { return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err) }
	if err := cfg.validate(records.JSON, records.Avro, records.Protobuf, records.String); err != nil {
		return nil, invalid(err)
	}
	if cfg.StartOffset != "earliest" && cfg.StartOffset != "latest" {
		return nil, invalid(fmt.Errorf("kafka: startOffset %q is neither earliest nor latest", cfg.StartOffset))
	}
	s := &Source{cfg: cfg}
	if cfg.TopicPattern != "" {
		p, err := regexp.Compile(cfg.TopicPattern)
		if err != nil {
			return nil, invalid(fmt.Errorf("kafka: topicPattern: %w", err))
		}
		s.pattern = p
	}
	reg, err := cfg.Registry()
	if err != nil {
		return nil, err
	}
	s.reg = reg
	s.values = records.NewDecoder(cfg.Format, reg)
	s.keys = records.NewDecoder(cfg.Format, reg)
	return s, nil
}

func (s *Source) offered(topic string) bool {
	if strings.HasPrefix(topic, "__") || topic == "_schemas" || topic == defaultMarkerTopic {
		return false
	}
	if len(s.cfg.Topics) > 0 && !slices.Contains(s.cfg.Topics, topic) {
		return false
	}
	return s.pattern == nil || s.pattern.MatchString(topic)
}

func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	cl, err := s.cfg.client()
	if err != nil {
		return source.Catalog{}, err
	}
	defer cl.Close()
	tds, err := kadm.NewClient(cl).ListTopics(ctx)
	if err != nil {
		return source.Catalog{}, classify(fmt.Errorf("kafka: list topics: %w", err))
	}
	tds.FilterInternal()
	var cat source.Catalog
	for _, topic := range tds.Names() {
		if !s.offered(topic) {
			continue
		}
		sc, err := s.declared(ctx, topic)
		if err != nil {
			return source.Catalog{}, err
		}
		cat.Streams = append(cat.Streams, source.CatalogStream{
			Name: topic, CursorField: offsetColumn, Replay: source.Exact, Schema: sc,
			SyncModes: []source.SyncMode{source.Stream},
		})
	}
	return cat, nil
}

// declared is a topic's columns: the record's own, and the value's fields
// as the registry's latest value schema has them.
func (s *Source) declared(ctx context.Context, topic string) (*arrow.Schema, error) {
	fields := []arrow.Field{
		{Name: partitionColumn, Type: arrow.PrimitiveTypes.Int32},
		{Name: offsetColumn, Type: arrow.PrimitiveTypes.Int64},
		{Name: timestampColumn, Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: keyColumn, Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: headersColumn, Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.NewMetadata([]string{records.ExtensionNameKey}, []string{records.JSONExtension})},
	}
	own := map[string]bool{}
	for _, f := range fields {
		own[f.Name] = true
	}
	values, err := records.ValueFields(ctx, s.reg, s.cfg.Format, topic+"-value")
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	for _, f := range values {
		if !own[f.Name] {
			fields = append(fields, f)
		}
	}
	return arrow.NewSchema(fields, nil), nil
}

// state is the resume point of a stream read whole: each partition's next
// offset, and the topic's ID, which a topic created again doesn't have.
// Brokers without topic IDs, and states from before 0.2.0, have none.
type state struct {
	Offsets map[string]int64 `json:"offsets"`
	TopicID string           `json:"topicId,omitempty"`
}

// splitState is the resume point of a partition read as a split: its next
// offset, and the topic's ID, as state has it.
type splitState struct {
	Offset  int64  `json:"offset"`
	TopicID string `json:"topicId,omitempty"`
}

// topicIDOf is the topic ID that the first of states to hold one holds,
// whole streams' and splits' alike.
func topicIDOf(states ...[]byte) string {
	for _, b := range states {
		var st struct {
			TopicID string `json:"topicId"`
		}
		if json.Unmarshal(b, &st) == nil && st.TopicID != "" {
			return st.TopicID
		}
	}
	return ""
}

func splitStates(splits []source.Split) [][]byte {
	out := make([][]byte, len(splits))
	for i, sp := range splits {
		out[i] = sp.State
	}
	return out
}

func splitID(p int32) string { return "p" + strconv.Itoa(int(p)) }

func parsePartition(id string) (int32, error) {
	n, ok := strings.CutPrefix(id, "p")
	if !ok {
		return 0, fmt.Errorf("kafka: split %q isn't a partition", id)
	}
	p, err := strconv.ParseInt(n, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("kafka: split %q isn't a partition", id)
	}
	return int32(p), nil
}

func encodeOffset(o int64, topicID string) []byte {
	b, _ := json.Marshal(splitState{Offset: o, TopicID: topicID})
	return b
}

// splitOffsets are the next offsets that splits hold: the whole stream's
// state, or partitions' own. A split at its beginning has none.
func splitOffsets(splits []source.Split) (map[int32]int64, error) {
	out := map[int32]int64{}
	for _, sp := range splits {
		if sp.ID == source.WholeStream {
			whole, err := parseState(sp.State)
			if err != nil {
				return nil, err
			}
			maps.Copy(out, whole)
			continue
		}
		p, err := parsePartition(sp.ID)
		if err != nil {
			return nil, err
		}
		if len(sp.State) == 0 {
			continue
		}
		var st splitState
		if err := json.Unmarshal(sp.State, &st); err != nil {
			return nil, fmt.Errorf("kafka: state of split %s: %w", sp.ID, err)
		}
		out[p] = st.Offset
	}
	return out, nil
}

func parseState(b []byte) (map[int32]int64, error) {
	out := map[int32]int64{}
	if len(b) == 0 {
		return out, nil
	}
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("kafka: state: %w", err)
	}
	for k, v := range st.Offsets {
		p, err := strconv.ParseInt(k, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("kafka: state: partition %q", k)
		}
		out[int32(p)] = v
	}
	return out, nil
}

func encodeState(next map[int32]int64, topicID string) []byte {
	st := state{Offsets: make(map[string]int64, len(next)), TopicID: topicID}
	for p, o := range next {
		st.Offsets[strconv.Itoa(int(p))] = o
	}
	b, _ := json.Marshal(st)
	return b
}

// topicRead is one stream's read.
type topicRead struct {
	name string
	// The topic's ID: the state's, then the brokers', if they have IDs.
	id string
	// A read again to a state from before 0.2.0, which has no ID: its
	// states keep none, so they are the first read's.
	bare bool
	// Where it's read whole, its emitter; else its partitions' splits. Only
	// splits' partitions are read.
	whole  source.SplitEmitter
	splits map[int32]source.SplitEmitter
	// The topic's partitions as last listed.
	partitions []int32
	next       map[int32]int64
	// Bounded reads: where each partition ends, and the partitions done.
	end  map[int32]int64
	done map[int32]bool
	// Bounded reads: the partitions whose read stops at an offset the
	// request gives, its until or complete_at, reading no record from it
	// on. Such a partition completes at that offset.
	until map[int32]int64
	// Bounded reads: when each partition last moved.
	moved map[int32]time.Time
	hwm   map[int32]int64
	// The newest record's time.
	last time.Time
	// The topic's retention.ms; 0 if unknown or unlimited.
	retention time.Duration
	complete  bool
}

// stopOf is where a read that isn't continuous stops: its until, or its
// complete_at, which is never set with it. Both are offsets this source
// sent, at a resume point or a completion. Nil: where the read would end.
func stopOf(until, completeAt []byte) []byte {
	if until != nil {
		return until
	}
	return completeAt
}

func positionLost(err error) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST, err)
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	if len(req.Streams) == 0 {
		return nil
	}
	s.mu.Lock()
	s.pipeline = req.Pipeline
	s.mu.Unlock()
	cl, err := s.cfg.client()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	reads := map[string]*topicRead{}
	var topics []string
	for _, st := range req.Streams {
		r := &topicRead{name: st.Name, end: map[int32]int64{}, done: map[int32]bool{}, moved: map[int32]time.Time{}, hwm: map[int32]int64{}}
		if len(st.Splits) == 0 {
			r.whole, r.id = out.Split(st.Name, source.WholeStream), topicIDOf(st.State)
			if r.next, err = parseState(st.State); err != nil {
				return err
			}
			if stop := stopOf(st.Until, st.CompleteAt); stop != nil && !req.Continuous {
				if r.until, err = parseState(stop); err != nil {
					return err
				}
				r.bare = topicIDOf(stop) == ""
			}
		} else {
			if r.next, err = splitOffsets(st.Splits); err != nil {
				return err
			}
			r.id = topicIDOf(splitStates(st.Splits)...)
			r.splits = make(map[int32]source.SplitEmitter, len(st.Splits))
			for _, sp := range st.Splits {
				p, err := parsePartition(sp.ID)
				if err != nil {
					return err
				}
				r.splits[p] = out.Split(st.Name, sp.ID)
				if stop := stopOf(sp.Until, sp.CompleteAt); stop != nil && !req.Continuous {
					var u splitState
					if err := json.Unmarshal(stop, &u); err != nil {
						return fmt.Errorf("kafka: where split %s stops: %w", sp.ID, err)
					}
					if r.until == nil {
						r.until = map[int32]int64{}
					}
					r.until[p] = u.Offset
					r.bare = r.bare || u.TopicID == ""
				}
			}
		}
		reads[st.Name] = r
		topics = append(topics, st.Name)
	}
	if err := s.position(ctx, adm, reads, topics); err != nil {
		return err
	}
	for _, r := range reads {
		if r.whole != nil && r.until != nil {
			// The partitions the read's end has, as it had them.
			for p := range r.next {
				if _, ok := r.until[p]; !ok {
					delete(r.next, p)
				}
			}
		}
	}
	retentions(ctx, adm, reads, topics)
	bounded := !req.Continuous
	if bounded {
		ends, err := adm.ListCommittedOffsets(ctx, topics...)
		if err != nil {
			return classify(fmt.Errorf("kafka: list end offsets: %w", err))
		}
		ends.Each(func(o kadm.ListedOffset) {
			if r := reads[o.Topic]; r != nil && o.Err == nil {
				r.end[o.Partition] = o.Offset
			}
		})
		for _, r := range reads {
			maps.Copy(r.end, r.until)
		}
	}
	consume := map[string]map[int32]kgo.Offset{}
	now := time.Now()
	for _, r := range reads {
		consume[r.name] = map[int32]kgo.Offset{}
		for p, o := range r.next {
			r.moved[p] = now
			if bounded && o >= r.end[p] {
				continue
			}
			consume[r.name][p] = kgo.NewOffset().At(o)
		}
	}
	consumer, err := s.cfg.client(
		kgo.ConsumePartitions(consume),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.KeepControlRecords(),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.FetchMaxWait(time.Second),
	)
	if err != nil {
		return err
	}
	defer consumer.Close()
	progress, _ := out.(source.Progresser)
	lastProgress, lastPartitions := now, now
	for {
		if bounded {
			for _, t := range topics {
				if err := reads[t].finish(consumer, func(p int32, from, u int64) (bool, error) {
					return reached(ctx, adm, cl, t, p, from, u)
				}); err != nil {
					return err
				}
			}
			if !slices.ContainsFunc(topics, func(t string) bool { return !reads[t].complete }) {
				return nil
			}
		} else if time.Since(lastPartitions) > partitionsEvery {
			lastPartitions = time.Now()
			if err := s.newPartitions(ctx, adm, consumer, out, reads, topics); err != nil {
				return err
			}
		}
		if progress != nil && time.Since(lastProgress) > progressEvery {
			lastProgress = time.Now()
			for _, t := range topics {
				if err := progress.Progress(reads[t].progress(time.Now())); err != nil {
					return err
				}
			}
		}
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		fs := consumer.PollRecords(pctx, pollRecords*max(req.Lanes, 1))
		cancel()
		if err := ctx.Err(); err != nil {
			return err
		}
		var ferr error
		fs.EachError(func(topic string, p int32, err error) {
			switch {
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			case errors.Is(err, kerr.OffsetOutOfRange):
				ferr = errors.Join(ferr, positionLost(fmt.Errorf("kafka: %s partition %d no longer has offset %d: retention deleted records the read hadn't reached, or the topic was deleted and created again, or the brokers lost records; the stream needs a re-sync", topic, p, reads[topic].next[p])))
			default:
				ferr = errors.Join(ferr, classify(fmt.Errorf("kafka: fetch %s partition %d: %w", topic, p, err)))
			}
		})
		if ferr != nil {
			return ferr
		}
		fs.EachPartition(func(fp kgo.FetchTopicPartition) {
			if r := reads[fp.Topic]; r != nil {
				r.hwm[fp.Partition] = fp.HighWatermark
			}
		})
		batches := map[batchKey]*batch{}
		for _, rec := range fs.Records() {
			r := reads[rec.Topic]
			if r == nil || r.done[rec.Partition] || rec.Offset < r.next[rec.Partition] {
				continue
			}
			if u, ok := r.until[rec.Partition]; ok && rec.Offset >= u {
				// Every record before u has been fetched: the read reached u,
				// even where the fetch no longer returns the last of them.
				r.next[rec.Partition] = max(r.next[rec.Partition], u)
				continue
			}
			r.next[rec.Partition] = rec.Offset + 1
			r.moved[rec.Partition] = time.Now()
			k := r.batchKey(rec.Partition)
			b := batches[k]
			if b == nil {
				b = &batch{}
				batches[k] = b
			}
			if rec.Attrs.IsControl() {
				continue
			}
			r.last = rec.Timestamp
			b.records = append(b.records, rec)
		}
		var wg sync.WaitGroup
		for k, b := range batches {
			em, resume := reads[k.topic].resumePoint(k.partition)
			wg.Go(func() { b.err = s.emit(ctx, em, b.records, resume) })
		}
		wg.Wait()
		var eerr error
		for _, b := range batches {
			eerr = errors.Join(eerr, b.err)
		}
		if eerr != nil {
			return eerr
		}
	}
}

// batchKey is what one resume point covers: a partition's split, or the
// whole topic, whose partition is -1.
type batchKey struct {
	topic     string
	partition int32
}

// batch is what one poll read of a batchKey's.
type batch struct {
	records []*kgo.Record
	err     error
}

func (r *topicRead) batchKey(p int32) batchKey {
	if r.splits == nil {
		p = -1
	}
	return batchKey{r.name, p}
}

// resumePoint is the emitter and state of a batchKey, at the offsets reached.
func (r *topicRead) resumePoint(p int32) (source.SplitEmitter, []byte) {
	if r.splits == nil {
		return r.whole, encodeState(r.next, r.id)
	}
	return r.splits[p], encodeOffset(r.next[p], r.id)
}

// emit decodes one poll's records of a split, or of a topic read whole, and
// emits them with the resume point after them. Different splits' run
// concurrently.
func (s *Source) emit(ctx context.Context, em source.SplitEmitter, records []*kgo.Record, resume []byte) error {
	if len(records) > 0 {
		rows := make([]json.RawMessage, 0, len(records))
		for _, rec := range records {
			row, err := s.row(ctx, rec)
			if err != nil {
				return err
			}
			rows = append(rows, row)
		}
		if err := em.Rows(rows); err != nil {
			return err
		}
	}
	return em.ResumePoint(resume)
}

// finish stops reading the partitions of a bounded read that have caught up,
// completing their splits, and the topic once all of them have. A partition
// that stops at an offset completes there once it has read up to it, or
// once reached shows that nothing it could return is left below it.
func (r *topicRead) finish(consumer *kgo.Client, reached func(p int32, from, u int64) (bool, error)) error {
	if r.complete {
		return nil
	}
	var pause []int32
	for p := range r.next {
		if r.done[p] || (r.next[p] < r.end[p] && time.Since(r.moved[p]) <= idleAfter) {
			continue
		}
		u, until := r.until[p]
		if until && r.next[p] < u {
			// Short of the stop, which an earlier read reached: what's
			// below it is gone, as an aborted transaction and its marker
			// once compaction has removed them, or not fetched yet.
			ok, err := reached(p, r.next[p], u)
			if !ok {
				if idle := time.Since(r.moved[p]); idle > stallAfter {
					why := "it has records there it hasn't returned"
					if err != nil {
						why = err.Error()
					}
					return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("kafka: %s partition %d: the read stops at offset %d and is at %d, and for %v the brokers haven't shown that nothing is left between: %s", r.name, p, u, r.next[p], idle.Round(time.Second), why))
				}
				continue
			}
			r.next[p] = u
		}
		r.done[p] = true
		pause = append(pause, p)
		if r.splits != nil {
			if err := completeAt(r.splits[p], encodeOffset(r.next[p], r.id), until); err != nil {
				return err
			}
		}
	}
	if len(pause) > 0 {
		consumer.PauseFetchPartitions(map[string][]int32{r.name: pause})
	}
	if len(r.done) < len(r.next) {
		return nil
	}
	r.complete = true
	if r.splits == nil {
		return completeAt(r.whole, encodeState(r.next, r.id), r.until != nil)
	}
	return nil
}

// reached says whether a partition holds no record a read-committed fetch
// returns, control records among them, from offset from up to u: one fetch
// of its own from the partition's leader passes u, or returns nothing below
// it while the partition is stable past it. It's false if the fetch returns
// a record below u, and fails if the brokers don't answer.
func reached(ctx context.Context, adm *kadm.Client, cl *kgo.Client, topic string, p int32, from, u int64) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		tds, err := adm.ListTopics(ctx, topic)
		if err != nil {
			return false, err
		}
		td := tds[topic]
		pd, ok := td.Partitions[p]
		switch {
		case td.Err != nil:
			return false, td.Err
		case !ok:
			return false, fmt.Errorf("no partition %d", p)
		case pd.Err != nil:
			return false, pd.Err
		}
		req := kmsg.NewPtrFetchRequest()
		req.ReplicaID, req.MaxWaitMillis, req.MinBytes, req.MaxBytes = -1, 0, 1, 1<<20
		req.IsolationLevel, req.SessionEpoch = 1, -1
		rt := kmsg.NewFetchRequestTopic()
		rt.Topic, rt.TopicID = topic, td.ID
		rp := kmsg.NewFetchRequestTopicPartition()
		rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes = p, from, 1<<20
		rp.CurrentLeaderEpoch, rp.LogStartOffset = -1, -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		resp, err := cl.Broker(int(pd.Leader)).Request(ctx, req)
		if err != nil {
			return false, err
		}
		fr := resp.(*kmsg.FetchResponse)
		if err := kerr.ErrorForCode(fr.ErrorCode); err != nil {
			return false, err
		}
		var got *kmsg.FetchResponseTopicPartition
		for i := range fr.Topics {
			if ft := &fr.Topics[i]; ft.Topic == topic || ft.TopicID == td.ID {
				for j := range ft.Partitions {
					if ft.Partitions[j].Partition == p {
						got = &ft.Partitions[j]
					}
				}
			}
		}
		if got == nil {
			return false, fmt.Errorf("the fetch of partition %d had no answer for it", p)
		}
		fp, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{
			KeepControlRecords: true, Offset: from, IsolationLevel: kgo.ReadCommitted(), Topic: topic, Partition: p,
		}, got, kgo.DefaultDecompressor(), nil)
		if fp.Err != nil {
			return false, fp.Err
		}
		if len(fp.Records) > 0 {
			return fp.Records[0].Offset >= u, nil
		}
		switch {
		case next >= u:
			return true, nil
		case next == from:
			// Nothing at or after from up to where the partition is stable.
			if fp.LastStableOffset >= u {
				return true, nil
			}
			return false, fmt.Errorf("partition %d is stable only up to %d", p, fp.LastStableOffset)
		}
		from = next
	}
}

// completeAt completes a split, or the whole stream, at state, after a
// resume point there if the request said where its read stops.
func completeAt(em source.SplitEmitter, state []byte, until bool) error {
	if until {
		if err := em.ResumePoint(state); err != nil {
			return err
		}
	}
	return em.Complete(state)
}

// position gives each partition read its next offset: the state's, or else
// its start's, or for a topic never read, the configured start's. A state's
// offset below its partition's start means retention deleted records the
// read never reached; one past its partition's end, or of a partition the
// topic no longer has, means the topic was deleted and created again, or
// the brokers lost records the read had read. So does a topic ID other
// than the state's.
func (s *Source) position(ctx context.Context, adm *kadm.Client, reads map[string]*topicRead, topics []string) error {
	tds, err := adm.ListTopics(ctx, topics...)
	if err != nil {
		return classify(fmt.Errorf("kafka: describe topics: %w", err))
	}
	for _, t := range topics {
		if !tds.Has(t) {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("kafka: topic %s doesn't exist", t))
		}
	}
	starts, err := adm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return classify(fmt.Errorf("kafka: list start offsets: %w", err))
	}
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return classify(fmt.Errorf("kafka: list end offsets: %w", err))
	}
	firsts := starts
	if s.cfg.StartOffset == "latest" {
		firsts = ends
	}
	for _, t := range topics {
		r := reads[t]
		if id := tds[t].ID; id != (kadm.TopicID{}) {
			if r.id != "" && r.id != id.String() {
				return positionLost(fmt.Errorf("kafka: %s has ID %s, and the read was of %s: the topic was deleted and created again; the stream needs a re-sync", t, id, r.id))
			}
			if !r.bare {
				r.id = id.String()
			}
		}
		fresh := r.splits == nil && len(r.next) == 0
		r.partitions = tds[t].Partitions.Numbers()
		for p, o := range r.next {
			if !slices.Contains(r.partitions, p) {
				return positionLost(fmt.Errorf("kafka: %s has no partition %d, which the read was at offset %d of: the topic was deleted and created again; the stream needs a re-sync", t, p, o))
			}
		}
		for _, p := range r.partitions {
			if r.splits != nil && r.splits[p] == nil {
				continue
			}
			start, ok := starts.Lookup(t, p)
			if !ok || start.Err != nil {
				return classify(fmt.Errorf("kafka: %s partition %d's start: %w", t, p, start.Err))
			}
			if o, ok := r.next[p]; ok {
				if o < start.Offset {
					return positionLost(fmt.Errorf("kafka: %s partition %d starts at %d, past the read's offset %d: retention deleted records the read hadn't reached; the stream needs a re-sync", t, p, start.Offset, o))
				}
				end, ok := ends.Lookup(t, p)
				if !ok || end.Err != nil {
					return classify(fmt.Errorf("kafka: %s partition %d's end: %w", t, p, end.Err))
				}
				if o > end.Offset {
					return positionLost(fmt.Errorf("kafka: %s partition %d ends at %d, before the read's offset %d: the topic was deleted and created again, or the brokers lost records the read had read; the stream needs a re-sync", t, p, end.Offset, o))
				}
				continue
			}
			first := start
			if fresh {
				first, _ = firsts.Lookup(t, p)
			}
			r.next[p] = max(first.Offset, start.Offset)
		}
	}
	return nil
}

// Splits are a topic's partitions, each from where the last read ended. A
// partition that read didn't have is new since, so it starts at its start,
// and every partition of a topic never read at the configured start.
func (s *Source) Splits(ctx context.Context, stream source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	next, err := splitOffsets(ended)
	if err != nil {
		return nil, err
	}
	cl, err := s.cfg.client()
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	r := &topicRead{name: stream.Name, next: next, id: topicIDOf(splitStates(ended)...)}
	if err := s.position(ctx, kadm.NewClient(cl), map[string]*topicRead{r.name: r}, []string{r.name}); err != nil {
		return nil, err
	}
	out := make([]source.Split, 0, len(r.partitions))
	for _, p := range r.partitions {
		out = append(out, source.Split{ID: splitID(p), State: encodeOffset(r.next[p], r.id)})
	}
	return out, nil
}

// newPartitions starts reading partitions added to the topics since the
// read began, from their start. A topic read in splits gains them at a
// resume point of one of its splits, none of which complete in a continuous
// read, which is the only one that looks.
func (s *Source) newPartitions(ctx context.Context, adm *kadm.Client, consumer *kgo.Client, out source.Emitter, reads map[string]*topicRead, topics []string) error {
	tds, err := adm.ListTopics(ctx, topics...)
	if err != nil {
		slog.Warn("kafka: looking for new partitions failed", "err", err)
		return nil
	}
	fresh := map[string][]int32{}
	for _, t := range topics {
		for _, p := range tds[t].Partitions.Numbers() {
			if _, ok := reads[t].next[p]; !ok {
				fresh[t] = append(fresh[t], p)
			}
		}
	}
	if len(fresh) == 0 {
		return nil
	}
	starts, err := adm.ListStartOffsets(ctx, slices.Collect(maps.Keys(fresh))...)
	if err != nil {
		slog.Warn("kafka: looking for new partitions failed", "err", err)
		return nil
	}
	for t, ps := range fresh {
		r := reads[t]
		add := map[int32]kgo.Offset{}
		var added []source.Split
		var running source.SplitEmitter
		var resume []byte
		if r.splits != nil {
			p := slices.Min(slices.Collect(maps.Keys(r.splits)))
			running, resume = r.splits[p], encodeOffset(r.next[p], r.id)
		}
		for _, p := range ps {
			start, ok := starts.Lookup(t, p)
			if !ok || start.Err != nil {
				slog.Warn("kafka: looking for new partitions failed", "topic", t, "partition", p, "err", start.Err)
				continue
			}
			r.next[p] = start.Offset
			add[p] = kgo.NewOffset().At(start.Offset)
			if r.splits != nil {
				r.splits[p] = out.Split(t, splitID(p))
				added = append(added, source.Split{ID: splitID(p), State: encodeOffset(start.Offset, r.id)})
			}
		}
		if len(added) > 0 {
			if err := running.ResumePoint(resume, added...); err != nil {
				return err
			}
		}
		if len(add) > 0 {
			consumer.AddConsumePartitions(map[string]map[int32]kgo.Offset{t: add})
		}
	}
	return nil
}

// retentions reads the topics' retention.ms. Credentials that may not
// describe topics' configs leave it unknown.
func retentions(ctx context.Context, adm *kadm.Client, reads map[string]*topicRead, topics []string) {
	rcs, err := adm.DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		return
	}
	for _, rc := range rcs {
		r := reads[rc.Name]
		if r == nil || rc.Err != nil {
			continue
		}
		for _, c := range rc.Configs {
			if c.Key != "retention.ms" || c.Value == nil {
				continue
			}
			if ms, err := strconv.ParseInt(*c.Value, 10, 64); err == nil && ms > 0 {
				r.retention = time.Duration(ms) * time.Millisecond
			}
		}
	}
}

// progress is the read's lag, and the retention left: the topic's
// retention less the age of the newest record read, since the records
// after it are newer, or all of it once caught up. Retention by size isn't
// counted.
func (r *topicRead) progress(now time.Time) source.Progress {
	var lag uint64
	for p, o := range r.next {
		if h, ok := r.hwm[p]; ok && h > o {
			lag += uint64(h - o)
		}
	}
	behind := time.Duration(0)
	if lag > 0 && !r.last.IsZero() {
		behind = now.Sub(r.last)
	}
	p := source.Progress{Stream: r.name, LagRecords: &lag, Lag: &behind}
	if r.retention > 0 && (lag == 0 || !r.last.IsZero()) {
		left := r.retention
		if lag > 0 {
			left = max(0, r.retention-behind)
		}
		p.RetentionLeft = &left
	}
	return p
}

// row is a record as a JSON object: its value's fields, or its value as
// "value", and the record's own columns.
func (s *Source) row(ctx context.Context, rec *kgo.Record) (json.RawMessage, error) {
	b := make([]byte, 0, len(rec.Value)+128)
	b = append(b, '{')
	if rec.Value != nil {
		v, err := s.values.Decode(ctx, rec.Value)
		if err != nil {
			return nil, decodeFailed(rec, "value", err)
		}
		b = records.AppendFields(b, v)
	}
	b = records.AppendJSONString(b, partitionColumn)
	b = append(b, ':')
	b = strconv.AppendInt(b, int64(rec.Partition), 10)
	b = append(b, ',')
	b = records.AppendJSONString(b, offsetColumn)
	b = append(b, ':')
	b = strconv.AppendInt(b, rec.Offset, 10)
	b = append(b, ',')
	b = records.AppendJSONString(b, timestampColumn)
	b = append(b, ':')
	b = records.AppendJSONString(b, rec.Timestamp.UTC().Format(time.RFC3339Nano))
	b = append(b, ',')
	b = records.AppendJSONString(b, keyColumn)
	b = append(b, ':')
	k, err := s.key(ctx, rec.Key)
	if err != nil {
		return nil, decodeFailed(rec, "key", err)
	}
	b = append(b, k...)
	b = append(b, ',')
	b = records.AppendJSONString(b, headersColumn)
	b = append(b, ':')
	if len(rec.Headers) == 0 {
		b = append(b, "null"...)
	} else {
		// A key's last value, as text, or base64 if it isn't UTF-8.
		h := map[string]json.RawMessage{}
		for _, hd := range rec.Headers {
			h[hd.Key] = records.JSONText(hd.Value)
		}
		hb, err := json.Marshal(h)
		if err != nil {
			return nil, err
		}
		b = append(b, hb...)
	}
	return append(b, '}'), nil
}

// key is a record's key as a JSON string: a string key's text, a structured
// one's JSON, or null. A key that doesn't decode is its bytes' text, unless
// the schema registry didn't answer for it.
func (s *Source) key(ctx context.Context, k []byte) ([]byte, error) {
	if k == nil {
		return []byte("null"), nil
	}
	v, err := s.keys.Decode(ctx, k)
	switch {
	case err != nil && records.Unanswered(err):
		return nil, err
	case err != nil:
		return records.JSONText(k), nil
	case len(v) > 0 && v[0] == '"':
		return v, nil
	}
	return records.AppendJSONString(nil, string(v)), nil
}

// decodeFailed is the error of a record's value or key that didn't decode:
// the schema registry's, if it didn't answer, else invalid data.
func decodeFailed(rec *kgo.Record, part string, err error) error {
	code := protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID
	if records.Unanswered(err) {
		code = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
		if re, ok := errors.AsType[*sr.ResponseError](err); ok {
			switch re.StatusCode {
			case http.StatusUnauthorized:
				code = protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID
			case http.StatusForbidden:
				code = protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
			}
			err = fmt.Errorf("the schema registry answered %d: %w", re.StatusCode, err)
		}
	}
	return sdk.WithCode(code, fmt.Errorf("kafka: %s partition %d offset %d: %s: %w", rec.Topic, rec.Partition, rec.Offset, part, err))
}

// Committed commits the checkpoint's offsets to the consumer group, so
// Kafka's tools show the read's lag. The checkpoint, not the group, is
// where reads resume.
func (s *Source) Committed(ctx context.Context, states map[string][]byte, splits map[string][]source.Split) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acker == nil {
		cl, err := s.cfg.client()
		if err != nil {
			return err
		}
		s.acker = cl
	}
	group := s.cfg.Group
	if group == "" {
		group = "openetl-" + s.pipeline
	}
	offsets := kadm.Offsets{}
	add := func(t string, next map[int32]int64) {
		for p, o := range next {
			offsets.Add(kadm.Offset{Topic: t, Partition: p, At: o, LeaderEpoch: -1})
		}
	}
	for _, t := range slices.Sorted(maps.Keys(states)) {
		next, err := parseState(states[t])
		if err != nil {
			return err
		}
		add(t, next)
	}
	for _, t := range slices.Sorted(maps.Keys(splits)) {
		next, err := splitOffsets(splits[t])
		if err != nil {
			return err
		}
		add(t, next)
	}
	res, err := kadm.NewClient(s.acker).CommitOffsets(ctx, group, offsets)
	if err == nil {
		err = res.Error()
	}
	if err != nil {
		return classify(fmt.Errorf("kafka: commit offsets to %s: %w", group, err))
	}
	return nil
}
