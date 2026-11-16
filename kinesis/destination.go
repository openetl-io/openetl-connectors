package kinesis

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
	"github.com/twmb/franz-go/pkg/sr"
	"golang.org/x/time/rate"

	"github.com/openetl-io/openetl-connectors/records"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type DestinationConfig struct {
	Connection
	// Streams are named namespace.table when the pipeline or this sets a
	// namespace, and table otherwise.
	Namespace string `json:"namespace"`
	// Create missing streams. Default true.
	CreateStreams *bool `json:"createStreams"`
	// Created streams' capacity mode: onDemand (default) or provisioned,
	// with Shards shards.
	StreamMode string `json:"streamMode"`
	Shards     int32  `json:"shards"`
	// Created streams' retention, in hours; 0: Kinesis's 24.
	RetentionHours int32 `json:"retentionHours"`
	// Created streams are encrypted with this KMS key; empty: not encrypted.
	KMSKeyID     string       `json:"kmsKeyId"`
	PartitionKey PartitionKey `json:"partitionKey"`
	// key (default): a request holds one record of each partition key, so a
	// retry can't reorder a key's records. none: requests are filled
	// freely.
	Ordering  string    `json:"ordering"`
	RateLimit RateLimit `json:"rateLimit"`
}

// PartitionKey is what a record's partition key is made of.
type PartitionKey struct {
	// key (default): the row's primary key, or its _openetl_id. fields: the
	// columns in Fields. random: a random key, spreading rows evenly over
	// shards with no order between a key's rows.
	Mode   string   `json:"mode"`
	Fields []string `json:"fields"`
}

// RateLimit caps what one destination process sends, on average; 0: no
// cap.
type RateLimit struct {
	RecordsPerSecond int `json:"recordsPerSecond"`
	BytesPerSecond   int `json:"bytesPerSecond"`
}

const (
	streamOnDemand    = "onDemand"
	streamProvisioned = "provisioned"

	partitionByKey    = "key"
	partitionByFields = "fields"
	partitionRandom   = "random"

	orderingKey  = "key"
	orderingNone = "none"
)

// Kinesis's limits.
const (
	maxRequestRecords  = 500
	maxRequestBytes    = 5 << 20
	maxRecordBytes     = 1 << 20
	maxPartitionKeyLen = 256
	minRetentionHours  = 24
	maxRetentionHours  = 8760
	idColumn           = "_openetl_id"
)

type Destination struct {
	cfg DestinationConfig
	// Shared by every writer of the process; nil: no cap.
	records, bytes *rate.Limiter
}

func NewDestination(config []byte) (*Destination, error) {
	var cfg DestinationConfig
	if err := decodeConfig(config, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, invalid(err)
	}
	d := &Destination{cfg: cfg}
	if n := cfg.RateLimit.RecordsPerSecond; n > 0 {
		d.records = rate.NewLimiter(rate.Limit(n), max(n, maxRequestRecords))
	}
	if n := cfg.RateLimit.BytesPerSecond; n > 0 {
		d.bytes = rate.NewLimiter(rate.Limit(n), max(n, maxRequestBytes))
	}
	return d, nil
}

// validate checks the config and fills in its defaults.
func (c *DestinationConfig) validate() error {
	if err := c.Connection.validate(records.JSON, records.Avro, records.Protobuf); err != nil {
		return err
	}
	switch c.StreamMode {
	case "", streamOnDemand:
		c.StreamMode = streamOnDemand
		if c.Shards != 0 {
			return errors.New("kinesis: shards needs streamMode provisioned")
		}
	case streamProvisioned:
		if c.Shards < 1 {
			return fmt.Errorf("kinesis: streamMode provisioned needs shards, 1 or more, not %d", c.Shards)
		}
	default:
		return fmt.Errorf("kinesis: streamMode %q isn't onDemand or provisioned", c.StreamMode)
	}
	if h := c.RetentionHours; h != 0 && (h < minRetentionHours || h > maxRetentionHours) {
		return fmt.Errorf("kinesis: retentionHours %d isn't between %d and %d", h, minRetentionHours, maxRetentionHours)
	}
	switch c.PartitionKey.Mode {
	case "", partitionByKey, partitionRandom:
		if c.PartitionKey.Mode == "" {
			c.PartitionKey.Mode = partitionByKey
		}
		if len(c.PartitionKey.Fields) > 0 {
			return errors.New("kinesis: partitionKey.fields needs partitionKey.mode fields")
		}
	case partitionByFields:
		if len(c.PartitionKey.Fields) == 0 {
			return errors.New("kinesis: partitionKey.mode fields needs partitionKey.fields")
		}
	default:
		return fmt.Errorf("kinesis: partitionKey.mode %q isn't key, fields or random", c.PartitionKey.Mode)
	}
	switch c.Ordering {
	case "":
		c.Ordering = orderingKey
	case orderingKey, orderingNone:
	default:
		return fmt.Errorf("kinesis: ordering %q isn't key or none", c.Ordering)
	}
	if c.RateLimit.RecordsPerSecond < 0 || c.RateLimit.BytesPerSecond < 0 {
		return errors.New("kinesis: rateLimit's values are negative")
	}
	return nil
}

func (c *DestinationConfig) createsStreams() bool { return c.CreateStreams == nil || *c.CreateStreams }

// setup says the streams the destination creates need more than
// CreateStream gives them: retention or encryption.
func (c *DestinationConfig) setup() bool {
	return c.RetentionHours > minRetentionHours || c.KMSKeyID != ""
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

// putter is the part of the Kinesis client a writer sends with.
type putter interface {
	PutRecords(ctx context.Context, in *awskinesis.PutRecordsInput, opts ...func(*awskinesis.Options)) (*awskinesis.PutRecordsOutput, error)
}

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if !DestinationCapabilities.SupportsMode(s.Mode) {
			return nil, invalid(fmt.Errorf("kinesis: stream %q: Kinesis only appends", s.Name))
		}
	}
	reg, err := d.cfg.Registry()
	if err != nil {
		return nil, err
	}
	cl, err := d.cfg.client(ctx)
	if err != nil {
		return nil, err
	}
	w := &writer{d: d, cfg: cfg, reg: reg, cl: cl, put: cl, streams: map[string]destination.Stream{}, encs: map[string]*streamEncoder{}, ready: map[string]bool{}, pending: map[string]*queue{}}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	return w, nil
}

type writer struct {
	d   *Destination
	cfg destination.OpenConfig
	reg *sr.Client
	cl  *awskinesis.Client
	put putter

	streams map[string]destination.Stream
	encs    map[string]*streamEncoder
	// Kinesis streams that exist and are active.
	ready map[string]bool
	// Each Kinesis stream's records not sent yet, in order.
	pending map[string]*queue
}

type entry struct {
	data []byte
	key  string
}

func (e entry) size() int { return len(e.data) + len(e.key) }

type queue struct {
	entries []entry
	bytes   int
}

// streamEncoder encodes one stream's rows while its schema stays the same.
type streamEncoder struct {
	schema  *arrow.Schema
	target  string
	cols    []records.Col
	mode    string
	key     []int
	keyCols []records.Col
	enc     records.Encoder
	id      int
}

func (w *writer) target(table string) string {
	return target(w.cfg.Namespace, w.d.cfg.Namespace, table)
}

// target is the Kinesis stream a table goes to: namespace.table, with the
// pipeline's namespace or else the config's, or table without either.
func target(pipeline, config, table string) string {
	ns := pipeline
	if ns == "" {
		ns = config
	}
	if ns == "" {
		return table
	}
	return ns + "." + table
}

// tableOf is the table a stream's schema names, or its name made a table.
func tableOf(stream string, sc *arrow.Schema) string {
	if sc != nil {
		if t, ok := sc.Metadata().GetValue(records.TableKey); ok {
			return t
		}
	}
	return records.AvroName(stream)
}

func (w *writer) encoder(ctx context.Context, stream string, sc *arrow.Schema) (*streamEncoder, error) {
	if e := w.encs[stream]; e != nil && e.schema.Equal(sc) {
		return e, nil
	}
	table := tableOf(stream, sc)
	e := &streamEncoder{schema: sc, target: w.target(table), id: -1, mode: w.d.cfg.PartitionKey.Mode}
	if err := w.ensureStream(ctx, e.target); err != nil {
		return nil, err
	}
	cols, err := records.Cols(sc)
	if err != nil {
		return nil, fmt.Errorf("kinesis: stream %q: %w", stream, err)
	}
	e.cols = cols
	for i := range cols {
		if cols[i].Name() == idColumn {
			e.id = i
		}
	}
	if err := e.keys(sc, w.streams[stream].PrimaryKey, w.d.cfg.PartitionKey.Fields); err != nil {
		return nil, fmt.Errorf("kinesis: stream %q: %w", stream, err)
	}
	if e.enc, err = records.NewEncoder(ctx, w.d.cfg.Format, w.reg, e.target+"-value", table, cols, w.streams[stream].AllowTypeChanges); err != nil {
		return nil, fmt.Errorf("kinesis: stream %q: %w", stream, err)
	}
	w.encs[stream] = e
	return e, nil
}

// keys settles the columns partition keys are made of.
func (e *streamEncoder) keys(sc *arrow.Schema, primary, fields []string) error {
	names, err := keyColumns(e.mode, sc, primary, fields, e.id >= 0)
	if err != nil {
		return err
	}
	for _, n := range names {
		i := sc.FieldIndices(n)[0]
		e.key = append(e.key, i)
		e.keyCols = append(e.keyCols, e.cols[i])
	}
	return nil
}

// keyColumns are the columns of sc partition keys are made of in mode;
// none for random keys, or for _openetl_id's, which hasID says sc has.
func keyColumns(mode string, sc *arrow.Schema, primary, fields []string, hasID bool) ([]string, error) {
	switch mode {
	case partitionByKey:
		names, err := records.KeyColumns(sc, primary)
		if err != nil {
			return nil, invalid(err)
		}
		if len(names) == 0 && !hasID {
			return nil, fmt.Errorf("neither a primary key nor %s for partition keys", idColumn)
		}
		return names, nil
	case partitionByFields:
		for _, n := range fields {
			if len(sc.FieldIndices(n)) == 0 {
				return nil, invalid(fmt.Errorf("no column %s, which partitionKey.fields names", n))
			}
		}
		return fields, nil
	}
	return nil, nil
}

// setupTag marks a stream the destination created and hasn't finished
// setting up: CreateStream sets it, and it's removed once the stream has
// the config's retention and encryption.
const (
	setupTag     = "openetl.io/setup"
	setupPending = "pending"
)

// ensureStream waits for a Kinesis stream to be active, creating it unless
// it exists or createStreams is off. Only a stream it creates gets the
// config's mode, retention and encryption; an existing one is left as it
// is, unless the destination created it and its setup didn't finish, which
// ensureStream then finishes.
func (w *writer) ensureStream(ctx context.Context, name string) error {
	if w.ready[name] {
		return nil
	}
	cfg := &w.d.cfg
	sum, err := w.cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
	created := false
	if apiCode(err) == "ResourceNotFoundException" {
		sum = nil
		if !cfg.createsStreams() {
			return invalid(fmt.Errorf("kinesis: stream %s doesn't exist, and createStreams is off", name))
		}
		in := &awskinesis.CreateStreamInput{StreamName: aws.String(name), StreamModeDetails: &types.StreamModeDetails{StreamMode: types.StreamModeOnDemand}}
		if cfg.StreamMode == streamProvisioned {
			in.ShardCount, in.StreamModeDetails = aws.Int32(cfg.Shards), &types.StreamModeDetails{StreamMode: types.StreamModeProvisioned}
		}
		if cfg.setup() {
			in.Tags = map[string]string{setupTag: setupPending}
		}
		_, err = w.cl.CreateStream(ctx, in)
		switch apiCode(err) {
		case "":
			created = err == nil
		case "ResourceInUseException":
			err = nil
		}
	}
	if err != nil {
		return classify(fmt.Errorf("kinesis: stream %s: %w", name, err))
	}
	if err := w.active(ctx, name); err != nil {
		return err
	}
	if cfg.setup() {
		pending := created
		if !created {
			var err error
			pending, err = pendingSetup(ctx, w.cl, name)
			switch {
			case accessDenied(apiCode(err)) && sum != nil:
				// Without its tags, the stream counts as one the
				// destination didn't create.
			case err != nil:
				return classify(fmt.Errorf("kinesis: stream %s: list its tags: %w", name, err))
			}
		}
		if pending {
			if err := w.configure(ctx, name); err != nil {
				return err
			}
			sum = nil
		}
	}
	if sum != nil {
		if diff := differs(cfg, sum.StreamDescriptionSummary); diff != "" {
			slog.WarnContext(ctx, "kinesis: an existing stream differs from the config, which applies only to streams the destination creates", "stream", name, "differs", diff)
		}
	}
	w.ready[name] = true
	return nil
}

// tagLister is the part of the Kinesis client that lists a stream's tags.
type tagLister interface {
	ListTagsForStream(ctx context.Context, in *awskinesis.ListTagsForStreamInput, opts ...func(*awskinesis.Options)) (*awskinesis.ListTagsForStreamOutput, error)
}

// pendingSetup says the destination created the stream and hasn't finished
// setting it up.
func pendingSetup(ctx context.Context, cl tagLister, name string) (bool, error) {
	in := &awskinesis.ListTagsForStreamInput{StreamName: aws.String(name), Limit: aws.Int32(50)}
	for {
		res, err := cl.ListTagsForStream(ctx, in)
		if err != nil {
			return false, err
		}
		for _, t := range res.Tags {
			if aws.ToString(t.Key) == setupTag {
				return aws.ToString(t.Value) == setupPending, nil
			}
		}
		if !aws.ToBool(res.HasMoreTags) || len(res.Tags) == 0 {
			return false, nil
		}
		in.ExclusiveStartTagKey = res.Tags[len(res.Tags)-1].Key
	}
}

func (w *writer) active(ctx context.Context, name string) error {
	waiter := awskinesis.NewStreamExistsWaiter(w.cl, func(o *awskinesis.StreamExistsWaiterOptions) { o.MinDelay = time.Second })
	if err := waiter.Wait(ctx, &awskinesis.DescribeStreamInput{StreamName: aws.String(name)}, 5*time.Minute); err != nil {
		return classify(fmt.Errorf("kinesis: stream %s didn't become active: %w", name, err))
	}
	return nil
}

// configure gives a stream the destination created the config's retention
// and encryption, what it doesn't have of them yet, then takes away its
// setup tag.
func (w *writer) configure(ctx context.Context, name string) error {
	cfg := &w.d.cfg
	res, err := w.cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
	if err != nil {
		return classify(fmt.Errorf("kinesis: stream %s: %w", name, err))
	}
	sum := res.StreamDescriptionSummary
	if h := cfg.RetentionHours; h > aws.ToInt32(sum.RetentionPeriodHours) {
		if _, err := w.cl.IncreaseStreamRetentionPeriod(ctx, &awskinesis.IncreaseStreamRetentionPeriodInput{StreamName: aws.String(name), RetentionPeriodHours: aws.Int32(h)}); err != nil {
			return classify(fmt.Errorf("kinesis: stream %s: set retention: %w", name, err))
		}
		if err := w.active(ctx, name); err != nil {
			return err
		}
	}
	if k := cfg.KMSKeyID; k != "" && (sum.EncryptionType != types.EncryptionTypeKms || aws.ToString(sum.KeyId) != k) {
		if _, err := w.cl.StartStreamEncryption(ctx, &awskinesis.StartStreamEncryptionInput{StreamName: aws.String(name), EncryptionType: types.EncryptionTypeKms, KeyId: aws.String(k)}); err != nil {
			return classify(fmt.Errorf("kinesis: stream %s: start encryption: %w", name, err))
		}
		if err := w.active(ctx, name); err != nil {
			return err
		}
	}
	if _, err := w.cl.RemoveTagsFromStream(ctx, &awskinesis.RemoveTagsFromStreamInput{StreamName: aws.String(name), TagKeys: []string{setupTag}}); err != nil {
		return classify(fmt.Errorf("kinesis: stream %s: remove its setup tag: %w", name, err))
	}
	return nil
}

// differs says how an existing stream differs from what the config would
// create; empty if it doesn't.
func differs(cfg *DestinationConfig, sum *types.StreamDescriptionSummary) string {
	var out []string
	if m := sum.StreamModeDetails; m != nil {
		switch {
		case cfg.StreamMode == streamOnDemand && m.StreamMode == types.StreamModeProvisioned:
			out = append(out, "provisioned, not on-demand")
		case cfg.StreamMode == streamProvisioned && m.StreamMode == types.StreamModeOnDemand:
			out = append(out, "on-demand, not provisioned")
		case cfg.StreamMode == streamProvisioned && aws.ToInt32(sum.OpenShardCount) != cfg.Shards:
			out = append(out, fmt.Sprintf("%d open shards, not %d", aws.ToInt32(sum.OpenShardCount), cfg.Shards))
		}
	}
	if h := cfg.RetentionHours; h != 0 && aws.ToInt32(sum.RetentionPeriodHours) != h {
		out = append(out, fmt.Sprintf("%dh of retention, not %dh", aws.ToInt32(sum.RetentionPeriodHours), h))
	}
	if cfg.KMSKeyID != "" && (sum.EncryptionType != types.EncryptionTypeKms || aws.ToString(sum.KeyId) != cfg.KMSKeyID) {
		out = append(out, "not encrypted with kmsKeyId")
	}
	return strings.Join(out, ", ")
}

// partitionKey is a row's partition key, hashed if it's longer than Kinesis
// allows.
func (e *streamEncoder) partitionKey(vals []any) (string, error) {
	var k string
	switch {
	case e.mode == partitionRandom:
		var b [16]byte
		_, _ = rand.Read(b[:])
		return hex.EncodeToString(b[:]), nil
	case len(e.key) > 0:
		kv := make([]any, len(e.key))
		for j, i := range e.key {
			kv[j] = vals[i]
		}
		var err error
		if k, err = records.KeyText(e.keyCols, kv); err != nil {
			return "", err
		}
	default:
		k, _ = vals[e.id].(string)
	}
	if k == "" {
		k = "null"
	}
	if utf8.RuneCountInString(k) > maxPartitionKeyLen {
		h := sha256.Sum256([]byte(k))
		k = hex.EncodeToString(h[:])
	}
	return k, nil
}

func (w *writer) Write(ctx context.Context, _ destination.Epoch, stream string, batch arrow.RecordBatch) error {
	if _, ok := w.streams[stream]; !ok {
		return fmt.Errorf("kinesis: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	enc, err := w.encoder(ctx, stream, batch.Schema())
	if err != nil {
		return err
	}
	q := w.pending[enc.target]
	if q == nil {
		q = &queue{}
		w.pending[enc.target] = q
	}
	vals := make([]any, len(enc.cols))
	for i := range int(batch.NumRows()) {
		for j := range enc.cols {
			if vals[j], err = enc.cols[j].At(batch.Column(j), i); err != nil {
				return fmt.Errorf("kinesis: stream %q: %w", stream, err)
			}
		}
		data, err := enc.enc.Encode(vals)
		if err != nil {
			return fmt.Errorf("kinesis: stream %q: %w", stream, err)
		}
		key, err := enc.partitionKey(vals)
		if err != nil {
			return fmt.Errorf("kinesis: stream %q: partition key: %w", stream, err)
		}
		if err := w.add(ctx, enc.target, q, entry{data: data, key: key}); err != nil {
			return fmt.Errorf("kinesis: stream %q: %w", stream, err)
		}
	}
	return nil
}

// add queues e, and sends requests while the queue holds a full one.
func (w *writer) add(ctx context.Context, target string, q *queue, e entry) error {
	if e.size() > maxRecordBytes {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, fmt.Errorf("a row of %d bytes, over Kinesis's 1 MiB a record", e.size()))
	}
	q.entries = append(q.entries, e)
	q.bytes += e.size()
	for len(q.entries) >= maxRequestRecords || q.bytes >= maxRequestBytes {
		if err := w.putAll(ctx, target, w.take(q)); err != nil {
			return err
		}
	}
	return nil
}

// take takes the next request's entries from q. With ordering key, a request
// holds one entry of each partition key, and the entries of a key it
// already holds wait, in order, for a later one.
func (w *writer) take(q *queue) []entry {
	var out []entry
	var keys map[string]bool
	if w.d.cfg.Ordering == orderingKey {
		keys = map[string]bool{}
	}
	size := 0
	var rest []entry
	for i, e := range q.entries {
		if len(out) == maxRequestRecords || size+e.size() > maxRequestBytes {
			rest = append(rest, q.entries[i:]...)
			break
		}
		if keys[e.key] {
			rest = append(rest, e)
			continue
		}
		if keys != nil {
			keys[e.key] = true
		}
		out = append(out, e)
		size += e.size()
	}
	q.entries, q.bytes = rest, q.bytes-size
	return out
}

// putAll sends entries to a Kinesis stream, and sends the ones it fails
// again, with backoff, until Kinesis has accepted all of them. Throttled for
// longer than retry.maxElapsed, it fails with RATE_LIMITED.
func (w *writer) putAll(ctx context.Context, target string, entries []entry) error {
	th := throttle{retry: &w.d.cfg.Retry}
	failed := func(err error) error { return classify(fmt.Errorf("kinesis: put records to %s: %w", target, err)) }
	for len(entries) > 0 {
		if err := w.limit(ctx, entries); err != nil {
			return err
		}
		in := &awskinesis.PutRecordsInput{StreamName: aws.String(target), Records: make([]types.PutRecordsRequestEntry, len(entries))}
		for i, e := range entries {
			in.Records[i] = types.PutRecordsRequestEntry{Data: e.data, PartitionKey: aws.String(e.key)}
		}
		res, err := w.put.PutRecords(ctx, in)
		if throttled(err) {
			if err := th.wait(ctx, err); err != nil {
				return failed(err)
			}
			continue
		}
		if err != nil {
			return failed(err)
		}
		if aws.ToInt32(res.FailedRecordCount) == 0 {
			return nil
		}
		var again []entry
		var last error
		for i, r := range res.Records {
			switch code := aws.ToString(r.ErrorCode); code {
			case "":
			case "ProvisionedThroughputExceededException", "InternalFailure":
				again = append(again, entries[i])
				last = &smithy.GenericAPIError{Code: code, Message: aws.ToString(r.ErrorMessage)}
			default:
				return failed(&smithy.GenericAPIError{Code: code, Message: aws.ToString(r.ErrorMessage)})
			}
		}
		if len(again) == 0 {
			return nil
		}
		if len(again) < len(entries) {
			th.ok()
		}
		entries = again
		if err := th.wait(ctx, last); err != nil {
			return failed(err)
		}
	}
	return nil
}

// limit waits until the rate limits let entries go.
func (w *writer) limit(ctx context.Context, entries []entry) error {
	if w.d.records != nil {
		if err := w.d.records.WaitN(ctx, len(entries)); err != nil {
			return err
		}
	}
	if w.d.bytes != nil {
		n := 0
		for _, e := range entries {
			n += e.size()
		}
		if err := w.d.bytes.WaitN(ctx, n); err != nil {
			return err
		}
	}
	return nil
}

// PreCommit sends what's left, and returns once Kinesis has accepted every
// record of the epoch.
func (w *writer) PreCommit(ctx context.Context, _ destination.Epoch) (destination.Staged, error) {
	for target, q := range w.pending {
		for len(q.entries) > 0 {
			if err := w.putAll(ctx, target, w.take(q)); err != nil {
				return nil, err
			}
		}
		delete(w.pending, target)
	}
	return nil, nil
}

func (w *writer) Commit(context.Context, destination.Epoch, []destination.Staged) (destination.Committed, error) {
	return destination.Committed{}, nil
}

// Recover: an epoch in the checkpoint log was accepted in full.
func (w *writer) Recover(context.Context, uint64, destination.Epoch, []destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (w *writer) Close(context.Context) error { return nil }
