package kinesis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/unsuitable001/openetl-connectors/records"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

type DestinationConfig struct {
	Connection
	// Streams are named namespace.table when the pipeline or this sets a
	// namespace, and table otherwise.
	Namespace string `json:"namespace"`
	// Create missing streams. Default true.
	CreateStreams *bool `json:"createStreams"`
	// Created streams' shards, provisioned; 0: on-demand.
	Shards int32 `json:"shards"`
}

// Kinesis's limits.
const (
	maxRequestRecords  = 500
	maxRequestBytes    = 5 << 20
	maxRecordBytes     = 1 << 20
	maxPartitionKeyLen = 256
	idColumn           = "_openetl_id"
)

type Destination struct {
	cfg DestinationConfig
}

var _ destination.Checker = (*Destination)(nil)

func NewDestination(config []byte) (*Destination, error) {
	var cfg DestinationConfig
	if err := decodeConfig(config, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.validate(records.JSON, records.Avro, records.Protobuf); err != nil {
		return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
	}
	if cfg.Shards < 0 {
		return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("kinesis: shards %d is below 1", cfg.Shards))
	}
	return &Destination{cfg: cfg}, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

// Check lists streams and, if there is one, the schema registry's subjects.
func (d *Destination) Check(ctx context.Context) error { return check(ctx, &d.cfg.Connection) }

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if !DestinationCapabilities.SupportsMode(s.Mode) {
			return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("kinesis: stream %q: Kinesis only appends", s.Name))
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
	w := &writer{d: d, cfg: cfg, reg: reg, cl: cl, streams: map[string]destination.Stream{}, encs: map[string]*streamEncoder{}, ready: map[string]bool{}, pending: map[string]*request{}}
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

	streams map[string]destination.Stream
	encs    map[string]*streamEncoder
	// Kinesis streams that exist and are active.
	ready map[string]bool
	// Each Kinesis stream's request being filled.
	pending map[string]*request
}

type request struct {
	entries []types.PutRecordsRequestEntry
	bytes   int
}

// streamEncoder encodes one stream's rows while its schema stays the same.
type streamEncoder struct {
	schema  *arrow.Schema
	target  string
	cols    []records.Col
	key     []int
	keyCols []records.Col
	enc     records.Encoder
	id      int
}

func (w *writer) target(table string) string {
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
	table, ok := sc.Metadata().GetValue(records.TableKey)
	if !ok {
		table = records.AvroName(stream)
	}
	e := &streamEncoder{schema: sc, target: w.target(table), id: -1}
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
	names, err := records.KeyColumns(sc, w.streams[stream].PrimaryKey)
	if err != nil {
		return nil, fmt.Errorf("kinesis: stream %q: %w", stream, err)
	}
	for _, n := range names {
		i := sc.FieldIndices(n)[0]
		e.key = append(e.key, i)
		e.keyCols = append(e.keyCols, cols[i])
	}
	if len(e.key) == 0 && e.id < 0 {
		return nil, fmt.Errorf("kinesis: stream %q has neither a primary key nor %s for partition keys", stream, idColumn)
	}
	if e.enc, err = records.NewEncoder(ctx, w.d.cfg.Format, w.reg, e.target+"-value", table, cols); err != nil {
		return nil, fmt.Errorf("kinesis: stream %q: %w", stream, err)
	}
	w.encs[stream] = e
	return e, nil
}

// ensureStream waits for a Kinesis stream to be active, creating it unless
// it exists or createStreams is off.
func (w *writer) ensureStream(ctx context.Context, name string) error {
	if w.ready[name] {
		return nil
	}
	_, err := w.cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
	if apiCode(err) == "ResourceNotFoundException" {
		if c := w.d.cfg.CreateStreams; c != nil && !*c {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("kinesis: stream %s doesn't exist, and createStreams is off", name))
		}
		in := &awskinesis.CreateStreamInput{StreamName: aws.String(name), StreamModeDetails: &types.StreamModeDetails{StreamMode: types.StreamModeOnDemand}}
		if w.d.cfg.Shards > 0 {
			in.ShardCount, in.StreamModeDetails = aws.Int32(w.d.cfg.Shards), &types.StreamModeDetails{StreamMode: types.StreamModeProvisioned}
		}
		if _, err = w.cl.CreateStream(ctx, in); apiCode(err) == "ResourceInUseException" {
			err = nil
		}
	}
	if err != nil {
		return classify(fmt.Errorf("kinesis: stream %s: %w", name, err))
	}
	waiter := awskinesis.NewStreamExistsWaiter(w.cl, func(o *awskinesis.StreamExistsWaiterOptions) { o.MinDelay = time.Second })
	if err := waiter.Wait(ctx, &awskinesis.DescribeStreamInput{StreamName: aws.String(name)}, 5*time.Minute); err != nil {
		return classify(fmt.Errorf("kinesis: stream %s didn't become active: %w", name, err))
	}
	w.ready[name] = true
	return nil
}

// partitionKey is a row's key as text, or its _openetl_id, hashed if it's
// longer than Kinesis allows.
func (e *streamEncoder) partitionKey(vals []any) (string, error) {
	var k string
	if len(e.key) > 0 {
		kv := make([]any, len(e.key))
		for j, i := range e.key {
			kv[j] = vals[i]
		}
		var err error
		if k, err = records.KeyText(e.keyCols, kv); err != nil {
			return "", err
		}
	} else {
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
		size := len(data) + len(key)
		if size > maxRecordBytes {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, fmt.Errorf("kinesis: stream %q: a row of %d bytes, over Kinesis's 1 MiB a record", stream, size))
		}
		r := w.pending[enc.target]
		if r != nil && (len(r.entries) == maxRequestRecords || r.bytes+size > maxRequestBytes) {
			if err := w.put(ctx, enc.target, r.entries); err != nil {
				return err
			}
			r = nil
		}
		if r == nil {
			r = &request{}
			w.pending[enc.target] = r
		}
		r.entries = append(r.entries, types.PutRecordsRequestEntry{Data: data, PartitionKey: aws.String(key)})
		r.bytes += size
	}
	return nil
}

// put sends entries to a Kinesis stream, and sends the ones it fails again,
// with backoff, until Kinesis has accepted all of them.
func (w *writer) put(ctx context.Context, target string, entries []types.PutRecordsRequestEntry) error {
	backoff := 100 * time.Millisecond
	for len(entries) > 0 {
		res, err := w.cl.PutRecords(ctx, &awskinesis.PutRecordsInput{StreamName: aws.String(target), Records: entries})
		if err != nil {
			return classify(fmt.Errorf("kinesis: put records to %s: %w", target, err))
		}
		if aws.ToInt32(res.FailedRecordCount) == 0 {
			return nil
		}
		var failed []types.PutRecordsRequestEntry
		for i, r := range res.Records {
			switch code := aws.ToString(r.ErrorCode); code {
			case "":
			case "ProvisionedThroughputExceededException", "InternalFailure":
				failed = append(failed, entries[i])
			default:
				return classify(fmt.Errorf("kinesis: put records to %s: %w", target, &smithy.GenericAPIError{Code: code, Message: aws.ToString(r.ErrorMessage)}))
			}
		}
		entries = failed
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 5*time.Second)
	}
	return nil
}

// PreCommit sends what's left, and returns once Kinesis has accepted every
// record of the epoch.
func (w *writer) PreCommit(ctx context.Context, _ destination.Epoch) (destination.Staged, error) {
	for target, r := range w.pending {
		if err := w.put(ctx, target, r.entries); err != nil {
			return nil, err
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
