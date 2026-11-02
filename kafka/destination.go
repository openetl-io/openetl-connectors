package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

type DestinationConfig struct {
	Connection
	// Topics are named namespace.table when the pipeline or this sets a
	// namespace, and table otherwise.
	Namespace string `json:"namespace"`
	// Create missing topics. Default true.
	CreateTopics *bool `json:"createTopics"`
	// For created topics; 0: the broker's default.
	Partitions        int32 `json:"partitions"`
	ReplicationFactor int16 `json:"replicationFactor"`
	// The compacted topic holding each epoch's marker. Default
	// _openetl_markers.
	MarkerTopic string `json:"markerTopic"`
	// Transactional IDs are this, the pipeline, and the producer's slot.
	// Default openetl-.
	TransactionalIDPrefix string `json:"transactionalIdPrefix"`
	// Follow each delete with a tombstone for its key.
	Tombstones bool `json:"tombstones"`
}

const (
	defaultMarkerTopic = "_openetl_markers"
	// Epoch N's transaction is on producer N mod slots: while N commits,
	// N+1 can be pre-committed and N+2 written.
	slots = 3
	// Without the platform's interval, as a continuous pipeline commits.
	defaultInterval = 10 * time.Second
	idHeader        = "openetl-id"
	opColumn        = "_openetl_op"
	idColumn        = "_openetl_id"
)

type Destination struct {
	cfg DestinationConfig
}

var _ destination.Checker = (*Destination)(nil)

func NewDestination(config []byte) (*Destination, error) {
	cfg := DestinationConfig{MarkerTopic: defaultMarkerTopic, TransactionalIDPrefix: "openetl-"}
	if err := decodeConfig(config, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.validate(formatJSON, formatAvro, formatProtobuf); err != nil {
		return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
	}
	return &Destination{cfg: cfg}, nil
}

func decodeConfig(config []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("kafka: config: %w", err))
	}
	return nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

// Check reaches the brokers and, if there is one, the schema registry.
func (d *Destination) Check(ctx context.Context) error {
	return check(ctx, &d.cfg.Connection)
}

func check(ctx context.Context, c *Connection) error {
	cl, err := c.client()
	if err != nil {
		return err
	}
	defer cl.Close()
	if _, err := kadm.NewClient(cl).ListBrokers(ctx); err != nil {
		return classify(fmt.Errorf("kafka: list brokers: %w", err))
	}
	reg, err := c.registry()
	if err != nil || reg == nil {
		return err
	}
	if _, err := reg.Subjects(ctx); err != nil {
		return fmt.Errorf("kafka: schema registry: %w", err)
	}
	return nil
}

// classify gives Kafka's errors their taxonomy codes.
func classify(err error) error {
	var ke *kerr.Error
	switch {
	case err == nil:
		return nil
	case errors.As(err, &ke) && (ke == kerr.SaslAuthenticationFailed || ke == kerr.IllegalSaslState || ke == kerr.UnsupportedSaslMechanism):
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, err)
	case errors.As(err, &ke) && (ke == kerr.TopicAuthorizationFailed || ke == kerr.GroupAuthorizationFailed ||
		ke == kerr.ClusterAuthorizationFailed || ke == kerr.TransactionalIDAuthorizationFailed):
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, err)
	case fenced(err):
		return fmt.Errorf("%w: %w", destination.ErrFenced, err)
	case errors.As(err, &ke) && ke.Retriable:
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	return err
}

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if !DestinationCapabilities.SupportsMode(s.Mode) {
			return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("kafka: stream %q: Kafka only appends", s.Name))
		}
	}
	if cfg.Scope != destination.ScopePipeline {
		return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, errors.New("kafka: an epoch is one transaction across topics; set delivery.commit.scope to pipeline"))
	}
	reg, err := d.cfg.registry()
	if err != nil {
		return nil, err
	}
	plain, err := d.cfg.client()
	if err != nil {
		return nil, err
	}
	wctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w := &writer{
		d: d, cfg: cfg, reg: reg, plain: plain, admin: kadm.NewClient(plain),
		ctx: wctx, cancel: cancel,
		streams: map[string]destination.Stream{},
		encs:    map[string]*streamEncoder{},
		topics:  map[string]bool{},
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	if err := w.open(ctx); err != nil {
		_ = w.Close(ctx)
		return nil, err
	}
	return w, nil
}

type writer struct {
	d     *Destination
	cfg   destination.OpenConfig
	reg   *sr.Client
	plain *kgo.Client
	admin *kadm.Client
	// Produces outlive the Write that started them.
	ctx    context.Context
	cancel context.CancelFunc

	slots   [slots]*slot
	streams map[string]destination.Stream
	// Only the goroutine that calls Write and PreCommit uses these.
	encs   map[string]*streamEncoder
	topics map[string]bool
}

// slot is one transactional producer.
type slot struct {
	id string
	cl *kgo.Client

	mu sync.Mutex
	// The epoch whose transaction is open; zero for none.
	epoch destination.Epoch
	// The transaction's first failed produce.
	err error
}

func (s *slot) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}

// timeout is how long an epoch's transaction may stay open: it opens with
// the epoch's first row, and commits after the next barrier, the record
// write and the previous epoch's commit.
func timeout(interval time.Duration) time.Duration {
	if interval <= 0 {
		interval = defaultInterval
	}
	return 2*interval + time.Minute
}

func (w *writer) open(ctx context.Context) error {
	if err := w.checkTimeout(ctx); err != nil {
		return err
	}
	if err := w.ensureTopic(ctx, w.d.cfg.MarkerTopic, map[string]*string{"cleanup.policy": kadm.StringPtr("compact")}); err != nil {
		return err
	}
	for i := range w.slots {
		s, err := w.newSlot(ctx, i)
		if err != nil {
			return err
		}
		w.slots[i] = s
	}
	return nil
}

// newSlot starts slot i's producer. Initializing it fences the producer an
// earlier attempt had with its transactional ID, and settles that
// producer's transaction: an open one is aborted, one being committed
// finishes.
func (w *writer) newSlot(ctx context.Context, i int) (*slot, error) {
	id := w.d.cfg.TransactionalIDPrefix + w.cfg.Pipeline + "-" + strconv.Itoa(i)
	cl, err := w.d.cfg.client(
		kgo.TransactionalID(id),
		kgo.TransactionTimeout(timeout(w.cfg.CommitInterval)),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordRetries(10),
	)
	if err != nil {
		return nil, err
	}
	if _, _, err := cl.ProducerID(ctx); err != nil {
		cl.Close()
		return nil, classify(fmt.Errorf("kafka: initialize producer %s: %w", id, err))
	}
	return &slot{id: id, cl: cl}, nil
}

// checkTimeout fails if the brokers would refuse the transactions'
// timeout. Credentials that may not describe the brokers' config skip it.
func (w *writer) checkTimeout(ctx context.Context) error {
	brokers, err := w.admin.ListBrokers(ctx)
	if err != nil {
		return classify(fmt.Errorf("kafka: list brokers: %w", err))
	}
	if len(brokers) == 0 {
		return nil
	}
	rcs, err := w.admin.DescribeBrokerConfigs(ctx, brokers[0].NodeID)
	if err != nil || len(rcs) == 0 || rcs[0].Err != nil {
		return nil
	}
	for _, c := range rcs[0].Configs {
		if c.Key != "transaction.max.timeout.ms" || c.Value == nil {
			continue
		}
		ms, err := strconv.ParseInt(*c.Value, 10, 64)
		if err != nil {
			return nil
		}
		if limit, t := time.Duration(ms)*time.Millisecond, timeout(w.cfg.CommitInterval); t > limit {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf(
				"kafka: a commit interval of %v needs transactions of %v, above the brokers' transaction.max.timeout.ms, %v; lower delivery.commit.interval or raise the brokers' limit",
				w.cfg.CommitInterval, t, limit))
		}
	}
	return nil
}

// ensureTopic creates topic unless it exists or createTopics is off.
func (w *writer) ensureTopic(ctx context.Context, topic string, configs map[string]*string) error {
	if w.topics[topic] {
		return nil
	}
	td, err := w.admin.ListTopics(ctx, topic)
	if err != nil {
		return classify(fmt.Errorf("kafka: describe topic %s: %w", topic, err))
	}
	if !td.Has(topic) {
		if c := w.d.cfg.CreateTopics; c != nil && !*c {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("kafka: topic %s doesn't exist, and createTopics is off", topic))
		}
		partitions, rf := w.d.cfg.Partitions, w.d.cfg.ReplicationFactor
		if partitions == 0 {
			partitions = -1
		}
		if rf == 0 {
			rf = -1
		}
		if _, err := w.admin.CreateTopic(ctx, partitions, rf, configs, topic); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return classify(fmt.Errorf("kafka: create topic %s: %w", topic, err))
		}
	}
	w.topics[topic] = true
	return nil
}

// streamEncoder encodes one stream's rows while its schema stays the same.
type streamEncoder struct {
	schema   *arrow.Schema
	topic    string
	cols     []col
	key      []int
	keyEnc   rowEncoder
	valueEnc rowEncoder
	id, op   int
}

func (w *writer) topic(table string) string {
	ns := w.cfg.Namespace
	if ns == "" {
		ns = w.d.cfg.Namespace
	}
	if ns == "" {
		return table
	}
	return ns + "." + table
}

func (w *writer) encoder(ctx context.Context, stream string, sc *arrow.Schema) (*streamEncoder, error) {
	if e := w.encs[stream]; e != nil && e.schema.Equal(sc) {
		return e, nil
	}
	table, ok := sc.Metadata().GetValue(tableKey)
	if !ok {
		table = avroName(stream)
	}
	e := &streamEncoder{schema: sc, topic: w.topic(table), id: -1, op: -1}
	if err := w.ensureTopic(ctx, e.topic, nil); err != nil {
		return nil, err
	}
	cols, err := colsOf(sc)
	if err != nil {
		return nil, fmt.Errorf("kafka: stream %q: %w", stream, err)
	}
	e.cols = cols
	for i := range cols {
		switch cols[i].name {
		case idColumn:
			e.id = i
		case opColumn:
			e.op = i
		}
	}
	names, err := keyColumns(sc, w.streams[stream].PrimaryKey)
	if err != nil {
		return nil, fmt.Errorf("kafka: stream %q: %w", stream, err)
	}
	var keyCols []col
	for _, n := range names {
		i := sc.FieldIndices(n)[0]
		e.key = append(e.key, i)
		keyCols = append(keyCols, cols[i])
	}
	f := w.d.cfg.Format
	if e.valueEnc, err = newRowEncoder(ctx, f, w.reg, e.topic+"-value", table, cols); err != nil {
		return nil, err
	}
	if len(keyCols) > 0 {
		if e.keyEnc, err = newRowEncoder(ctx, f, w.reg, e.topic+"-key", table+"_key", keyCols); err != nil {
			return nil, err
		}
	}
	w.encs[stream] = e
	return e, nil
}

const sourceNamesKey = "openetl.source_names"

// keyColumns returns the destination names of a stream's primary key, which
// the catalog gives by source name.
func keyColumns(sc *arrow.Schema, key []string) ([]string, error) {
	renamed := map[string]string{}
	if v, ok := sc.Metadata().GetValue(sourceNamesKey); ok {
		var names map[string]string
		if err := json.Unmarshal([]byte(v), &names); err != nil {
			return nil, fmt.Errorf("its %s metadata: %w", sourceNamesKey, err)
		}
		for dst, src := range names {
			renamed[src] = dst
		}
	}
	out := make([]string, len(key))
	for i, k := range key {
		out[i] = k
		if dst, ok := renamed[k]; ok {
			out[i] = dst
		}
		if len(sc.FieldIndices(out[i])) == 0 {
			return nil, fmt.Errorf("its key has %q, which its batches don't have", k)
		}
	}
	return out, nil
}

// txn returns epoch e's slot, with its transaction begun.
func (w *writer) txn(e destination.Epoch) (*slot, error) {
	s := w.slots[e.N%slots]
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch == e {
		return s, nil
	}
	if !s.epoch.IsZero() {
		return nil, fmt.Errorf("kafka: producer %s still has epoch %d open, writing %d", s.id, s.epoch.N, e.N)
	}
	if err := s.cl.BeginTransaction(); err != nil {
		return nil, fmt.Errorf("kafka: begin a transaction on %s: %w", s.id, err)
	}
	s.epoch, s.err = e, nil
	return s, nil
}

func (w *writer) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	if _, ok := w.streams[stream]; !ok {
		return fmt.Errorf("kafka: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	enc, err := w.encoder(ctx, stream, batch.Schema())
	if err != nil {
		return err
	}
	s, err := w.txn(e)
	if err != nil {
		return err
	}
	vals := make([]any, len(enc.cols))
	keyVals := make([]any, len(enc.key))
	promise := func(_ *kgo.Record, err error) {
		if err != nil {
			s.fail(err)
		}
	}
	for i := range int(batch.NumRows()) {
		for j := range enc.cols {
			if vals[j], err = enc.cols[j].at(batch.Column(j), i); err != nil {
				return fmt.Errorf("kafka: stream %q: %w", stream, err)
			}
		}
		rec := &kgo.Record{Topic: enc.topic}
		if rec.Value, err = enc.valueEnc.encode(vals); err != nil {
			return fmt.Errorf("kafka: stream %q: %w", stream, err)
		}
		if enc.keyEnc != nil {
			for j, k := range enc.key {
				keyVals[j] = vals[k]
			}
			if rec.Key, err = enc.keyEnc.encode(keyVals); err != nil {
				return fmt.Errorf("kafka: stream %q: key: %w", stream, err)
			}
		}
		if enc.id >= 0 {
			if id, ok := vals[enc.id].(string); ok {
				rec.Headers = []kgo.RecordHeader{{Key: idHeader, Value: []byte(id)}}
			}
		}
		s.cl.Produce(w.ctx, rec, promise)
		if w.d.cfg.Tombstones && rec.Key != nil && enc.op >= 0 && vals[enc.op] == "delete" {
			s.cl.Produce(w.ctx, &kgo.Record{Topic: enc.topic, Key: rec.Key}, promise)
		}
	}
	return nil
}

// marker is each epoch's record on the marker topic, keyed by the pipeline.
type marker struct {
	Pipeline   string `json:"pipeline"`
	Generation uint64 `json:"generation"`
	Epoch      uint64 `json:"epoch"`
}

type staged struct {
	Slot       int    `json:"slot"`
	Generation uint64 `json:"generation"`
	Epoch      uint64 `json:"epoch"`
	// Where the marker is.
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// PreCommit adds the epoch's marker to its transaction and flushes it.
func (w *writer) PreCommit(ctx context.Context, e destination.Epoch) (destination.Staged, error) {
	s, err := w.txn(e)
	if err != nil {
		return nil, err
	}
	value, err := json.Marshal(marker{Pipeline: w.cfg.Pipeline, Generation: e.Generation, Epoch: e.N})
	if err != nil {
		return nil, err
	}
	res := s.cl.ProduceSync(ctx, &kgo.Record{Topic: w.d.cfg.MarkerTopic, Key: []byte(w.cfg.Pipeline), Value: value})
	if err := s.cl.Flush(ctx); err != nil {
		return nil, err
	}
	rec, err := res.First()
	if err != nil {
		return nil, classify(fmt.Errorf("kafka: epoch %d's marker: %w", e.N, err))
	}
	s.mu.Lock()
	err = s.err
	s.mu.Unlock()
	if err != nil {
		return nil, classify(fmt.Errorf("kafka: epoch %d: %w", e.N, err))
	}
	return json.Marshal(staged{Slot: int(e.N % slots), Generation: e.Generation, Epoch: e.N, Partition: rec.Partition, Offset: rec.Offset})
}

// Commit commits the epoch's transaction. A fenced producer means a newer
// attempt owns the pipeline. A commit franz-go can't confirm is settled by
// aborting it on the same producer, which leaves a commit that reached the
// broker in place; Commit then looks for the epoch's marker, as it does
// without a transaction open for the epoch, after an earlier Commit.
func (w *writer) Commit(ctx context.Context, e destination.Epoch, st destination.Staged) (destination.Committed, error) {
	var desc staged
	if err := json.Unmarshal(st, &desc); err != nil {
		return destination.Committed{}, fmt.Errorf("kafka: staged descriptor: %w", err)
	}
	s := w.slots[desc.Slot%slots]
	s.mu.Lock()
	open := s.epoch == e
	s.mu.Unlock()
	if open {
		err := s.cl.EndTransaction(ctx, kgo.TryCommit)
		if err != nil && !fenced(err) {
			err = s.cl.EndTransaction(ctx, kgo.TryAbort)
		}
		s.mu.Lock()
		s.epoch = destination.Epoch{}
		s.mu.Unlock()
		switch {
		case err == nil:
			return destination.Committed{}, nil
		case fenced(err):
			return destination.Committed{}, fmt.Errorf("%w: kafka: commit epoch %d: %w", destination.ErrFenced, e.N, err)
		}
	}
	ok, err := w.applied(ctx, e, desc)
	if err != nil {
		return destination.Committed{}, err
	}
	if !ok {
		return destination.Committed{}, fmt.Errorf("%w: kafka: epoch %d's transaction was aborted", destination.ErrFenced, e.N)
	}
	return destination.Committed{}, nil
}

func fenced(err error) bool {
	return errors.Is(err, kerr.ProducerFenced) || errors.Is(err, kerr.InvalidProducerEpoch)
}

// Recover reports whether the epoch's marker is visible. Open has settled
// every transaction an earlier attempt left, so the answer is final.
func (w *writer) Recover(ctx context.Context, _ uint64, e destination.Epoch, st destination.Staged) (destination.Recovery, error) {
	if e.IsZero() {
		return destination.Applied, nil
	}
	var desc staged
	if err := json.Unmarshal(st, &desc); err != nil {
		return 0, fmt.Errorf("kafka: staged descriptor: %w", err)
	}
	ok, err := w.applied(ctx, e, desc)
	switch {
	case err != nil:
		return 0, err
	case ok:
		return destination.Applied, nil
	}
	return destination.Lost, nil
}

// applied reads the marker topic from the epoch's marker, read_committed:
// the marker comes first if its transaction committed, and otherwise a later
// record, or the abort's control record, does.
func (w *writer) applied(ctx context.Context, e destination.Epoch, desc staged) (bool, error) {
	cl, err := w.d.cfg.client(
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{w.d.cfg.MarkerTopic: {desc.Partition: kgo.NewOffset().At(desc.Offset)}}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.KeepControlRecords(),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
	)
	if err != nil {
		return false, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return false, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("kafka: epoch %d's marker: %w", e.N, err))
		}
		var ferr error
		fs.EachError(func(_ string, _ int32, err error) { ferr = errors.Join(ferr, err) })
		if ferr != nil {
			if errors.Is(ferr, kerr.OffsetOutOfRange) {
				return false, nil
			}
			return false, classify(fmt.Errorf("kafka: epoch %d's marker: %w", e.N, ferr))
		}
		for _, r := range fs.Records() {
			if r.Offset < desc.Offset {
				continue
			}
			if r.Offset > desc.Offset || r.Attrs.IsControl() {
				return false, nil
			}
			var m marker
			if err := json.Unmarshal(r.Value, &m); err != nil {
				return false, nil
			}
			return m == marker{Pipeline: w.cfg.Pipeline, Generation: e.Generation, Epoch: e.N}, nil
		}
	}
}

func (w *writer) Close(context.Context) error {
	w.cancel()
	for _, s := range w.slots {
		if s != nil {
			s.cl.Close()
		}
	}
	w.plain.Close()
	return nil
}
