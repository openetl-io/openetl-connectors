// Package s3 has the Parquet on S3 destination: each stream's table is a
// directory of Parquet files. Writes stream each stream's batches into
// Parquet files staged under the pipeline's _openetl/{pipeline}/staging,
// which PreCommit lists in the writer lane's staged descriptor. Commit copies
// every lane's to names derived from the pipeline, the epoch, its generation
// and the lane, then writes the stream's marker. A repeated or stale commit copies the same files to the
// same names, so it needs no fence: an epoch is only committed once the
// checkpoint log has recorded it, under the generation in its names.
package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type DestinationConfig struct {
	// s3://bucket/prefix, or file:// and an absolute path.
	URL    string `json:"url"`
	Region string `json:"region"`
	// An S3-compatible endpoint, with path-style addressing.
	Endpoint string `json:"endpoint"`
	// Without a key, the AWS default credentials.
	AccessKeyID     sdk.Secret `json:"accessKeyId"`
	SecretAccessKey sdk.Secret `json:"secretAccessKey"`
	// zstd, snappy, gzip or none. Default zstd.
	Compression string `json:"compression"`
	// Bytes a file reaches before the next starts. Default 256 MiB.
	FileSize int64 `json:"fileSize"`
}

const (
	defaultFileSize = 256 << 20
	rowGroupSize    = 64 << 20
	// What the destination keeps besides tables, under the URL.
	reserved = "_openetl"
)

var compressions = map[string]compress.Compression{
	"zstd": compress.Codecs.Zstd, "snappy": compress.Codecs.Snappy, "gzip": compress.Codecs.Gzip, "none": compress.Codecs.Uncompressed,
}

type Destination struct {
	cfg    DestinationConfig
	u      *url.URL
	codec  compress.Compression
	prefix string
}

var (
	_ destination.Destination = (*Destination)(nil)
	_ destination.Checker     = (*Destination)(nil)
)

func NewDestination(config []byte) (*Destination, error) {
	cfg := DestinationConfig{Compression: "zstd", FileSize: defaultFileSize}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("s3: config: %w", err)
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("s3: url: %w", err)
	}
	d := &Destination{cfg: cfg, u: u}
	switch {
	case u.Scheme == "s3" && u.Host != "" && u.RawQuery == "":
		d.prefix = strings.Trim(u.Path, "/")
	case u.Scheme == "file" && (u.Host == "" || u.Host == "localhost") && path.IsAbs(u.Path) && u.RawQuery == "":
	default:
		return nil, fmt.Errorf("s3: url %q must be s3://bucket/prefix, or file:// and an absolute path", cfg.URL)
	}
	codec, ok := compressions[cfg.Compression]
	if !ok {
		return nil, fmt.Errorf("s3: compression %q is none of zstd, snappy, gzip and none", cfg.Compression)
	}
	d.codec = codec
	if cfg.FileSize < 1<<20 {
		return nil, fmt.Errorf("s3: fileSize %d is under 1 MiB", cfg.FileSize)
	}
	if cfg.AccessKeyID.IsZero() != cfg.SecretAccessKey.IsZero() {
		return nil, errors.New("s3: accessKeyId and secretAccessKey go together")
	}
	return d, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

func (d *Destination) bucket(ctx context.Context) (*blob.Bucket, error) {
	var b *blob.Bucket
	var err error
	if d.u.Scheme == "file" {
		b, err = fileblob.OpenBucket(d.u.Path, &fileblob.Options{CreateDir: true, NoTempDir: true})
	} else {
		var opts []func(*awsconfig.LoadOptions) error
		if d.cfg.Region != "" {
			opts = append(opts, awsconfig.WithRegion(d.cfg.Region))
		}
		if !d.cfg.AccessKeyID.IsZero() {
			opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(d.cfg.AccessKeyID.Reveal(), d.cfg.SecretAccessKey.Reveal(), "")))
		}
		ac, lerr := awsconfig.LoadDefaultConfig(ctx, opts...)
		if lerr != nil {
			return nil, fmt.Errorf("s3: AWS config: %w", lerr)
		}
		client := awss3.NewFromConfig(ac, func(o *awss3.Options) {
			if d.cfg.Endpoint != "" {
				o.BaseEndpoint = aws.String(d.cfg.Endpoint)
				o.UsePathStyle = true
			}
		})
		b, err = s3blob.OpenBucket(ctx, client, d.u.Host, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("s3: open %s: %w", d.cfg.URL, err)
	}
	if d.prefix != "" {
		b = blob.PrefixedBucket(b, d.prefix+"/")
	}
	return b, nil
}

// Check writes a file under the URL and deletes it.
func (d *Destination) Check(ctx context.Context) (err error) {
	defer func() { err = classify(err) }()
	b, err := d.bucket(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = b.Close() }()
	key := reserved + "/check-" + uuid.NewString()
	if err := b.WriteAll(ctx, key, nil, nil); err != nil {
		return fmt.Errorf("s3: write %s: %w", key, err)
	}
	if err := b.Delete(ctx, key); err != nil {
		return fmt.Errorf("s3: delete %s: %w", key, err)
	}
	return nil
}

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if s.Mode != destination.Append {
			return nil, fmt.Errorf("s3: stream %q: only append writes are supported", s.Name)
		}
	}
	if cfg.Pipeline == "" || strings.ContainsAny(cfg.Pipeline, "/.") {
		return nil, fmt.Errorf("s3: pipeline %q can't name files", cfg.Pipeline)
	}
	b, err := d.bucket(ctx)
	if err != nil {
		return nil, err
	}
	wctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &writer{
		d: d, b: b, cfg: cfg, ns: strings.Trim(cfg.Namespace, "/"),
		ctx: wctx, cancel: cancel,
		parts: map[uint64]map[string]*epochFiles{},
	}, nil
}

type writer struct {
	d   *Destination
	b   *blob.Bucket
	cfg destination.OpenConfig
	ns  string
	// Files are written on it, across Write calls.
	ctx    context.Context
	cancel context.CancelFunc
	// Only Write, PreCommit and Close use it.
	parts map[uint64]map[string]*epochFiles
}

// staged is the descriptor PreCommit returns. Paths are relative to the URL.
type staged struct {
	Generation uint64         `json:"generation"`
	Epoch      uint64         `json:"epoch"`
	Streams    []stagedStream `json:"streams"`
}

type stagedStream struct {
	Name   string       `json:"name"`
	Marker string       `json:"marker"`
	Files  []stagedFile `json:"files"`
}

type stagedFile struct {
	Staged string `json:"staged"`
	Target string `json:"target"`
	Rows   int64  `json:"rows"`
}

func (w *writer) staging() string { return reserved + "/" + w.cfg.Pipeline + "/staging/" }

func (w *writer) table(sc *arrow.Schema, stream string) string {
	name := stream
	if t, ok := sc.Metadata().GetValue("openetl.table"); ok {
		name = t
	}
	if w.ns != "" {
		return w.ns + "/" + name
	}
	return name
}

// epochFiles is what a stream wrote in an epoch: its closed files, and the
// one being written.
type epochFiles struct {
	table string
	files []stagedFile
	open  *part
}

type part struct {
	file   stagedFile
	schema *arrow.Schema
	fw     *pqarrow.FileWriter
	cancel context.CancelFunc
}

func (w *writer) startPart(e destination.Epoch, ef *epochFiles, sc *arrow.Schema) (*part, error) {
	k := fmt.Sprintf("%05d", len(ef.files))
	if w.cfg.Lane > 0 {
		k = fmt.Sprintf("l%d-%s", w.cfg.Lane, k)
	}
	p := &part{schema: sc, file: stagedFile{
		Staged: fmt.Sprintf("%s%d/%d/%s/part-%s.parquet", w.staging(), e.Generation, e.N, ef.table, k),
		Target: fmt.Sprintf("%s/%s-%020d-g%020d-%s.parquet", ef.table, w.cfg.Pipeline, e.N, e.Generation, k),
	}}
	ctx, cancel := context.WithCancel(w.ctx)
	bw, err := w.b.NewWriter(ctx, p.file.Staged, &blob.WriterOptions{ContentType: "application/vnd.apache.parquet"})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("s3: stage %s: %w", p.file.Staged, err)
	}
	props := parquet.NewWriterProperties(parquet.WithCompression(w.d.codec), parquet.WithVersion(parquet.V2_LATEST))
	p.fw, err = pqarrow.NewFileWriter(sc, bw, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		cancel()
		_ = bw.Close()
		return nil, fmt.Errorf("s3: stage %s: %w", p.file.Staged, err)
	}
	p.cancel = cancel
	return p, nil
}

// finish closes the part's file, which makes it durable.
func (p *part) finish() (stagedFile, error) {
	defer p.cancel()
	p.file.Rows = int64(p.fw.NumRows())
	if err := p.fw.Close(); err != nil {
		return p.file, fmt.Errorf("s3: stage %s: %w", p.file.Staged, err)
	}
	return p.file, nil
}

// abort drops the part's file, which was never written.
func (p *part) abort() {
	p.cancel()
	_ = p.fw.Close()
}

func (w *writer) Write(_ context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) (err error) {
	defer func() { err = classify(err) }()
	if !slices.ContainsFunc(w.cfg.Streams, func(s destination.Stream) bool { return s.Name == stream }) {
		return fmt.Errorf("s3: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	byStream := w.parts[e.N]
	if byStream == nil {
		byStream = map[string]*epochFiles{}
		w.parts[e.N] = byStream
	}
	ef := byStream[stream]
	if ef == nil {
		ef = &epochFiles{table: w.table(batch.Schema(), stream)}
		byStream[stream] = ef
	}
	if ef.open != nil && !ef.open.schema.Equal(batch.Schema()) {
		if err := w.close(ef); err != nil {
			return err
		}
	}
	if ef.open == nil {
		p, err := w.startPart(e, ef, batch.Schema())
		if err != nil {
			return err
		}
		ef.open = p
	}
	fw := ef.open.fw
	if err := fw.WriteBuffered(batch); err != nil {
		return fmt.Errorf("s3: stream %q: %w", stream, err)
	}
	if fw.RowGroupTotalBytesWritten() >= rowGroupSize {
		fw.NewBufferedRowGroup()
	}
	if fw.TotalBytesWritten()+fw.RowGroupTotalBytesWritten() >= w.d.cfg.FileSize {
		return w.close(ef)
	}
	return nil
}

func (w *writer) close(ef *epochFiles) error {
	f, err := ef.open.finish()
	ef.open = nil
	if err != nil {
		return err
	}
	ef.files = append(ef.files, f)
	return nil
}

// PreCommit closes the epoch's files. They are durable once closed, and
// outside every table until Commit copies them in.
func (w *writer) PreCommit(_ context.Context, e destination.Epoch) (_ destination.Staged, err error) {
	defer func() { err = classify(err) }()
	desc := staged{Generation: e.Generation, Epoch: e.N}
	byStream := w.parts[e.N]
	delete(w.parts, e.N)
	for _, s := range w.cfg.Streams {
		ef := byStream[s.Name]
		if ef == nil {
			continue
		}
		if ef.open != nil {
			if err := w.close(ef); err != nil {
				return nil, err
			}
		}
		desc.Streams = append(desc.Streams, stagedStream{
			Name:   s.Name,
			Marker: fmt.Sprintf("%s/%s/loads/%s/%020d-g%020d.json", reserved, w.cfg.Pipeline, ef.table, e.N, e.Generation),
			Files:  ef.files,
		})
	}
	return json.Marshal(desc)
}

// descriptor merges the epoch's descriptors, one per lane of the attempt that
// staged it, by stream.
func (w *writer) descriptor(e destination.Epoch, committables []destination.Staged) (staged, error) {
	out := staged{Generation: e.Generation, Epoch: e.N}
	if len(committables) == 0 {
		return out, errors.New("s3: no staged descriptor")
	}
	at := map[string]int{}
	for _, c := range committables {
		var d staged
		if err := json.Unmarshal(c, &d); err != nil {
			return out, fmt.Errorf("s3: staged descriptor: %w", err)
		}
		if d.Generation != e.Generation || d.Epoch != e.N {
			return out, fmt.Errorf("s3: the staged descriptor is for epoch %d-g%d, not %d-g%d", d.Epoch, d.Generation, e.N, e.Generation)
		}
		for _, st := range d.Streams {
			if i, ok := at[st.Name]; ok {
				out.Streams[i].Files = append(out.Streams[i].Files, st.Files...)
				continue
			}
			at[st.Name] = len(out.Streams)
			out.Streams = append(out.Streams, st)
		}
	}
	return out, nil
}

func (w *writer) Commit(ctx context.Context, e destination.Epoch, committables []destination.Staged) (_ destination.Committed, err error) {
	defer func() { err = classify(err) }()
	d, err := w.descriptor(e, committables)
	if err != nil {
		return destination.Committed{}, err
	}
	for _, st := range d.Streams {
		if err := w.apply(ctx, st); err != nil {
			return destination.Committed{}, err
		}
	}
	w.dropStaged(d)
	return destination.Committed{}, nil
}

// apply copies each staged file to its target, then writes the stream's
// marker. A target whose staged file is gone was copied by an earlier apply.
func (w *writer) apply(ctx context.Context, st stagedStream) error {
	if done, err := w.b.Exists(ctx, st.Marker); err != nil || done {
		return err
	}
	for _, f := range st.Files {
		err := w.b.Copy(ctx, f.Target, f.Staged, nil)
		if gcerrors.Code(err) == gcerrors.NotFound {
			if ok, xerr := w.b.Exists(ctx, f.Target); xerr == nil && ok {
				err = nil
			}
		}
		if err != nil {
			return fmt.Errorf("s3: apply %s: %w", st.Name, err)
		}
	}
	marker, err := json.Marshal(st.Files)
	if err != nil {
		return err
	}
	if err := w.b.WriteAll(ctx, st.Marker, marker, &blob.WriterOptions{ContentType: "application/json"}); err != nil {
		return fmt.Errorf("s3: write the marker of %s: %w", st.Name, err)
	}
	return nil
}

// dropStaged is best effort: a staged file left behind is never applied
// again, because its stream's marker exists.
func (w *writer) dropStaged(d staged) {
	for _, st := range d.Streams {
		for _, f := range st.Files {
			_ = w.b.Delete(w.ctx, f.Staged)
		}
	}
}

func (w *writer) Recover(ctx context.Context, gen uint64, e destination.Epoch, committables []destination.Staged) (_ destination.Recovery, err error) {
	defer func() { err = classify(err) }()
	r, err := w.recover(ctx, e, committables)
	if err != nil {
		return 0, err
	}
	// Nothing an earlier generation staged can be applied any more.
	w.dropGenerations(ctx, gen)
	return r, nil
}

func (w *writer) recover(ctx context.Context, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	if e.IsZero() {
		return destination.Applied, nil
	}
	d, err := w.descriptor(e, committables)
	if err != nil {
		return 0, err
	}
	var missing []stagedStream
	lost := 0
	for _, st := range d.Streams {
		done, err := w.b.Exists(ctx, st.Marker)
		if err != nil {
			return 0, err
		}
		if done {
			continue
		}
		missing = append(missing, st)
		have := 0
		for _, f := range st.Files {
			staged, err := w.b.Exists(ctx, f.Staged)
			if err != nil {
				return 0, err
			}
			copied, err := w.b.Exists(ctx, f.Target)
			if err != nil {
				return 0, err
			}
			if staged || copied {
				have++
			}
		}
		switch have {
		case len(st.Files):
		case 0:
			lost++
		default:
			return 0, fmt.Errorf("s3: epoch %d-g%d: stream %s is partly staged", d.Epoch, d.Generation, st.Name)
		}
	}
	switch {
	case len(missing) == 0:
		w.dropStaged(d)
		return destination.Applied, nil
	case lost == 0:
		for _, st := range missing {
			if err := w.apply(ctx, st); err != nil {
				return 0, err
			}
		}
		w.dropStaged(d)
		return destination.Redone, nil
	case lost == len(d.Streams):
		return destination.Lost, nil
	default:
		return 0, fmt.Errorf("s3: epoch %d-g%d is partly applied and the staging for the rest is gone", d.Epoch, d.Generation)
	}
}

// dropGenerations deletes what generations before gen staged. Best effort.
func (w *writer) dropGenerations(ctx context.Context, gen uint64) {
	it := w.b.List(&blob.ListOptions{Prefix: w.staging(), Delimiter: "/"})
	for {
		obj, err := it.Next(ctx)
		if err != nil {
			return
		}
		g, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(obj.Key, w.staging()), "/"), 10, 64)
		if !obj.IsDir || err != nil || g >= gen {
			continue
		}
		files := w.b.List(&blob.ListOptions{Prefix: obj.Key})
		for {
			f, err := files.Next(ctx)
			if err != nil {
				break
			}
			_ = w.b.Delete(ctx, f.Key)
		}
	}
}

// Close drops the files of epochs that were never pre-committed.
func (w *writer) Close(context.Context) error {
	for _, byStream := range w.parts {
		for _, ef := range byStream {
			if ef.open != nil {
				ef.open.abort()
			}
			for _, f := range ef.files {
				_ = w.b.Delete(w.ctx, f.Staged)
			}
		}
	}
	w.cancel()
	return w.b.Close()
}

// classify makes failures to reach the store TRANSIENT.
func classify(err error) error {
	if _, ok := errors.AsType[net.Error](err); ok && !errors.Is(err, context.Canceled) {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	return err
}
