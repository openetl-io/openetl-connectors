package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
	// A bounded read counts a partition with nothing left before its end
	// as caught up after this long without records.
	idleAfter     = 10 * time.Second
	progressEvery = 10 * time.Second
	// How often a continuous read looks for new partitions.
	partitionsEvery = 30 * time.Second
	// The most records one poll takes, per lane.
	pollRecords = 5000
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
	_ source.Checker      = (*Source)(nil)
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

func (s *Source) Check(ctx context.Context) error { return check(ctx, &s.cfg.Connection) }

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
// offset.
type state struct {
	Offsets map[string]int64 `json:"offsets"`
}

// splitState is the resume point of a partition read as a split: its next
// offset.
type splitState struct {
	Offset int64 `json:"offset"`
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

func encodeOffset(o int64) []byte {
	b, _ := json.Marshal(splitState{Offset: o})
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

func encodeState(next map[int32]int64) []byte {
	st := state{Offsets: make(map[string]int64, len(next))}
	for p, o := range next {
		st.Offsets[strconv.Itoa(int(p))] = o
	}
	b, _ := json.Marshal(st)
	return b
}

// topicRead is one stream's read.
type topicRead struct {
	name string
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
	// Bounded reads: when each partition last moved.
	moved map[int32]time.Time
	hwm   map[int32]int64
	// The newest record's time.
	last time.Time
	// The topic's retention.ms; 0 if unknown or unlimited.
	retention time.Duration
	complete  bool
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
			r.whole = out.Split(st.Name, source.WholeStream)
			if r.next, err = parseState(st.State); err != nil {
				return err
			}
		} else {
			if r.next, err = splitOffsets(st.Splits); err != nil {
				return err
			}
			r.splits = make(map[int32]source.SplitEmitter, len(st.Splits))
			for _, sp := range st.Splits {
				p, err := parsePartition(sp.ID)
				if err != nil {
					return err
				}
				r.splits[p] = out.Split(st.Name, sp.ID)
			}
		}
		reads[st.Name] = r
		topics = append(topics, st.Name)
	}
	if err := s.position(ctx, adm, reads, topics); err != nil {
		return err
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
				if err := reads[t].finish(consumer); err != nil {
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
				ferr = errors.Join(ferr, positionLost(fmt.Errorf("kafka: %s partition %d: retention deleted records the read hadn't reached (offset %d); the stream needs a re-sync", topic, p, reads[topic].next[p])))
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
		return r.whole, encodeState(r.next)
	}
	return r.splits[p], encodeOffset(r.next[p])
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
// completing their splits, and the topic once all of them have.
func (r *topicRead) finish(consumer *kgo.Client) error {
	if r.complete {
		return nil
	}
	var pause []int32
	for p := range r.next {
		if r.done[p] || (r.next[p] < r.end[p] && time.Since(r.moved[p]) <= idleAfter) {
			continue
		}
		r.done[p] = true
		pause = append(pause, p)
		if r.splits != nil {
			if err := r.splits[p].Complete(encodeOffset(r.next[p])); err != nil {
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
		return r.whole.Complete(encodeState(r.next))
	}
	return nil
}

// position gives each partition read its next offset: the state's, or else
// its start's, or for a topic never read, the configured start's. A state's
// offset below its partition's start means retention deleted records the
// read never reached.
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
	firsts := starts
	if s.cfg.StartOffset == "latest" {
		if firsts, err = adm.ListEndOffsets(ctx, topics...); err != nil {
			return classify(fmt.Errorf("kafka: list end offsets: %w", err))
		}
	}
	for _, t := range topics {
		r := reads[t]
		fresh := r.splits == nil && len(r.next) == 0
		r.partitions = tds[t].Partitions.Numbers()
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
	r := &topicRead{name: stream.Name, next: next}
	if err := s.position(ctx, kadm.NewClient(cl), map[string]*topicRead{r.name: r}, []string{r.name}); err != nil {
		return nil, err
	}
	out := make([]source.Split, 0, len(r.partitions))
	for _, p := range r.partitions {
		out = append(out, source.Split{ID: splitID(p), State: encodeOffset(r.next[p])})
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
			running, resume = r.splits[p], encodeOffset(r.next[p])
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
				added = append(added, source.Split{ID: splitID(p), State: encodeOffset(start.Offset)})
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
			return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, fmt.Errorf("kafka: %s partition %d offset %d: %w", rec.Topic, rec.Partition, rec.Offset, err))
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
	b = append(b, s.key(ctx, rec.Key)...)
	b = append(b, ',')
	b = records.AppendJSONString(b, headersColumn)
	b = append(b, ':')
	if len(rec.Headers) == 0 {
		b = append(b, "null"...)
	} else {
		h := map[string]string{}
		for _, hd := range rec.Headers {
			h[hd.Key] = string(hd.Value)
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
// one's JSON, or null.
func (s *Source) key(ctx context.Context, k []byte) []byte {
	if k == nil {
		return []byte("null")
	}
	v, err := s.keys.Decode(ctx, k)
	if err != nil {
		return records.JSONText(k)
	}
	if len(v) > 0 && v[0] == '"' {
		return v
	}
	return records.AppendJSONString(nil, string(v))
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
