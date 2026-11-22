package redshift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// stagingTable is a stream's staging of an epoch on one lane: a file per
// staging table, each holding the batches, in order, that have its columns.
type stagingTable struct {
	sqlstage.Table
	parts []*part
}

// part is one staging table's file, written until PreCommit loads it.
type part struct {
	name   string
	cols   []column
	schema *arrow.Schema
	file   *os.File
	fw     *pqarrow.FileWriter
	// Rows written; each row's _openetl_ord is its place, from 1.
	rows   int64
	closed bool
}

// The temporary table COPY loads a file into, and its columns' names, which
// are the file's.
const loadTable = "_openetl_load"

func loadName(i int) string { return "c" + strconv.Itoa(i) }

func (w *writer) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	cfg, ok := w.streams[stream]
	if !ok {
		return fmt.Errorf("redshift: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	if cfg.Dedupe != destination.DedupeNone && len(batch.Schema().FieldIndices(idColumn)) == 0 {
		return fmt.Errorf("redshift: stream %q is deduped at apply, but its batches have no %s", stream, idColumn)
	}
	if _, ok := w.Starts[stream]; cfg.Mode == destination.Overwrite && !ok {
		return fmt.Errorf("redshift: stream %q overwrites, but its read has no start", stream)
	}
	var key []string
	if cfg.Mode == destination.Merge {
		var err error
		if key, err = keyColumns(batch.Schema(), cfg.PrimaryKey); err != nil {
			return fmt.Errorf("redshift: stream %q: %w", stream, err)
		}
	}
	st, err := w.stage(w.Staging, stagingName(w.cfg.Pipeline, stream, e, w.cfg.Lane), e, stream, targetTable(batch.Schema(), stream), key, batch)
	if err != nil {
		return err
	}
	w.SetTable(stream, st.Target)
	for _, f := range batch.Schema().Fields() {
		if v, _ := f.Metadata.GetValue(destination.HeldKey); v == "last" && !slices.Contains(st.Last, f.Name) {
			st.Last = append(st.Last, f.Name)
		}
	}
	return nil
}

// Patch stages patches in a table of their own, which Commit applies after
// the epoch's rows, in the same transaction and under the same marker.
func (w *writer) Patch(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	cfg, ok := w.streams[stream]
	switch {
	case !ok:
		return fmt.Errorf("redshift: unknown stream %q", stream)
	case cfg.Mode != destination.Merge && cfg.Mode != destination.Append:
		return fmt.Errorf("redshift: stream %q overwrites, and takes no patches", stream)
	case batch.NumRows() == 0:
		return nil
	case len(batch.Schema().FieldIndices(idColumn)) == 0 || len(batch.Schema().FieldIndices(heldColumn)) == 0:
		return fmt.Errorf("redshift: patches for %q need %s and %s", stream, idColumn, heldColumn)
	}
	var key []string
	if cfg.Mode == destination.Merge {
		var err error
		if key, err = keyColumns(batch.Schema(), cfg.PrimaryKey); err != nil {
			return fmt.Errorf("redshift: patches for %q: %w", stream, err)
		}
	}
	target := w.Table(stream)
	if target == "" {
		target = targetTable(batch.Schema(), stream)
	}
	st, err := w.stage(w.Patches, patchName(w.cfg.Pipeline, stream, e, w.cfg.Lane), e, stream, target, key, batch)
	if err != nil {
		return err
	}
	for _, f := range batch.Schema().Fields() {
		if held, ok := f.Metadata.GetValue(destination.FillsKey); ok {
			if st.Fills == nil {
				st.Fills = map[string]string{}
			}
			st.Fills[f.Name] = held
		}
	}
	return nil
}

// stage writes a batch to the stream's staging for epoch e in tables: to its
// file, or, if the batch's columns differ from the file's, to a new one,
// which is the next staging table.
func (w *writer) stage(tables map[uint64]map[string]*stagingTable, name string, e destination.Epoch, stream, target string, key []string, batch arrow.RecordBatch) (*stagingTable, error) {
	cols, err := columnsOf(batch.Schema())
	if err != nil {
		return nil, err
	}
	byStream := tables[e.N]
	if byStream == nil {
		byStream = map[string]*stagingTable{}
		tables[e.N] = byStream
	}
	st := byStream[stream]
	switch {
	case st == nil:
		st = &stagingTable{Table: sqlstage.Table{Name: name, Target: target, Key: key}}
		byStream[stream] = st
	case !sameColumns(st.parts[len(st.parts)-1].cols, cols):
		if err := st.parts[len(st.parts)-1].close(); err != nil {
			return nil, fmt.Errorf("redshift: stage %s: %w", stream, err)
		}
		st.Earlier = append(st.Earlier, st.Name)
		st.Name = name + "_" + strconv.Itoa(len(st.Earlier)+1)
	}
	if len(st.parts) == 0 || st.parts[len(st.parts)-1].name != st.Name {
		p, err := newPart(w.dir, st.Name, cols)
		if err != nil {
			return nil, fmt.Errorf("redshift: stage %s: %w", stream, err)
		}
		st.parts = append(st.parts, p)
	}
	if err := st.parts[len(st.parts)-1].write(batch, cols); err != nil {
		return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, fmt.Errorf("redshift: stage %s: %w", stream, err))
	}
	st.Rows += uint64(batch.NumRows())
	return st, nil
}

func sameColumns(a, b []column) bool {
	return slices.EqualFunc(a, b, func(x, y column) bool {
		return x.name == y.name && x.typ == y.typ && arrow.TypeEqual(x.load, y.load)
	})
}

func newPart(dir, name string, cols []column) (*part, error) {
	fields := make([]arrow.Field, 0, len(cols)+1)
	for i, c := range cols {
		fields = append(fields, arrow.Field{Name: loadName(i), Type: c.load, Nullable: true})
	}
	fields = append(fields, arrow.Field{Name: ordColumn, Type: arrow.PrimitiveTypes.Int64})
	schema := arrow.NewSchema(fields, nil)
	f, err := os.CreateTemp(dir, "*.parquet")
	if err != nil {
		return nil, err
	}
	fw, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy)), pqarrow.DefaultWriterProps())
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, err
	}
	return &part{name: name, cols: cols, schema: schema, file: f, fw: fw}, nil
}

// write appends a batch with the part's columns to its file, as a row group.
// cols are the batch's, which convert its arrays: a part's columns take
// batches of different Arrow types, such as int8 and int16 for smallint.
func (p *part) write(batch arrow.RecordBatch, cols []column) error {
	n := int(batch.NumRows())
	b := array.NewRecordBuilder(memory.DefaultAllocator, p.schema)
	defer b.Release()
	var ids arrow.Array
	if i := batch.Schema().FieldIndices(idColumn); len(i) > 0 {
		ids = batch.Column(i[0])
	}
	for j, c := range cols {
		a, fb := batch.Column(j), b.Field(j)
		fb.Reserve(n)
		for i := range n {
			var err error
			switch {
			case a.IsNull(i):
				fb.AppendNull()
			case c.name == heldColumn:
				err = appendHeld(fb, a, ids, i)
			case c.name == unchangedColumn:
				var names []string
				if names, err = listValues(a, i); err == nil {
					err = appendJSON(fb, nameList(names))
				}
			case c.put == nil:
				fb.AppendNull()
			default:
				err = c.put(fb, a, i)
			}
			if err != nil {
				return fmt.Errorf("column %q: %w", c.name, err)
			}
		}
	}
	ord := b.Field(len(p.cols)).(*array.Int64Builder)
	for range n {
		p.rows++
		ord.Append(p.rows)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	return p.fw.Write(rec)
}

// appendHeld appends row i's held columns as the object _openetl_held
// holds: each to the row's _openetl_id; null for none.
func appendHeld(b array.Builder, a, ids arrow.Array, i int) error {
	names, err := listValues(a, i)
	if err != nil || len(names) == 0 {
		b.AppendNull()
		return err
	}
	id, err := rowID(ids, i)
	if err != nil {
		return err
	}
	return appendJSON(b, heldObject(names, id))
}

func appendJSON(b array.Builder, s string) error {
	if len(s) > maxVarchar {
		return fmt.Errorf("%d bytes of names, and Redshift's VARCHAR holds at most %d", len(s), maxVarchar)
	}
	b.(*array.StringBuilder).Append(s)
	return nil
}

// rowID is row i's _openetl_id, as text.
func rowID(ids arrow.Array, i int) (string, error) {
	if ids == nil || ids.IsNull(i) {
		return "", fmt.Errorf("held cells, and no %s to hold them by", idColumn)
	}
	fb, ok := storage(ids).(*array.FixedSizeBinary)
	if !ok {
		return "", fmt.Errorf("%s is a %s, not a UUID", idColumn, ids.DataType())
	}
	u, err := uuid.FromBytes(fb.Value(i))
	return u.String(), err
}

// close finishes the part's file.
func (p *part) close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	err := p.fw.Close()
	if cerr := p.file.Close(); cerr != nil && !errors.Is(cerr, os.ErrClosed) && err == nil {
		err = cerr
	}
	return err
}

// discard closes the part's file and removes it.
func (p *part) discard() {
	_ = p.close()
	_ = os.Remove(p.file.Name())
}

// PreCommit loads each of the epoch's files into its staging table.
func (w *writer) PreCommit(ctx context.Context, e destination.Epoch) (destination.Staged, error) {
	for _, tables := range []map[string]*stagingTable{w.Staging[e.N], w.Patches[e.N]} {
		for _, stream := range slices.Sorted(maps.Keys(tables)) {
			st := tables[stream]
			label := stagingLabel{Pipeline: w.cfg.Pipeline, Generation: e.Generation, Epoch: e.N}
			for _, p := range st.parts {
				if err := w.load(ctx, p, label); err != nil {
					return nil, fmt.Errorf("redshift: stage %s: %w", stream, err)
				}
			}
		}
	}
	return w.Describe(w.cfg, e, readName)
}

// load uploads p's file and, in one transaction, creates its staging table,
// labelled, and fills it: COPY loads the file into a temporary table, whose
// columns are cast to the staging's types. The file, and the object once
// COPY has read it, are removed.
func (w *writer) load(ctx context.Context, p *part, label stagingLabel) error {
	if err := p.close(); err != nil {
		return err
	}
	defer p.discard()
	key := w.store.key(p.name)
	if err := w.store.upload(ctx, key, p.file.Name()); err != nil {
		return err
	}
	defer w.store.remove(context.WithoutCancel(ctx), key)
	b, err := json.Marshal(label)
	if err != nil {
		return err
	}
	loads := make([]string, 0, len(p.cols)+1)
	sel := make([]string, 0, len(p.cols)+1)
	for i, c := range p.cols {
		loads = append(loads, quoteIdent(loadName(i))+" "+loadType(c.load))
		expr := quoteIdent(loadName(i))
		if c.cast != nil {
			expr = c.cast(expr)
		}
		sel = append(sel, expr)
	}
	ord := quoteIdent(ordColumn)
	loads, sel = append(loads, ord+" bigint NOT NULL"), append(sel, ord)
	// Without the target's NOT NULL: gather fills a null _openetl_deleted.
	defs := make([]string, len(p.cols))
	for i, c := range p.cols {
		defs[i] = quoteIdent(c.name) + " " + c.typ
	}
	stmts := []string{
		"DROP TABLE IF EXISTS " + w.ident(p.name),
		"CREATE TABLE " + w.ident(p.name) + " (" + strings.Join(defs, ", ") + ", " + ord + " bigint NOT NULL)",
		"COMMENT ON TABLE " + w.ident(p.name) + " IS " + w.lit(string(b)),
		"CREATE TEMP TABLE " + quoteIdent(loadTable) + " (" + strings.Join(loads, ", ") + ")",
		"COPY " + quoteIdent(loadTable) + " FROM " + w.lit(w.store.url(key)) + " " + w.d.copyAuth(w.lit) + " FORMAT AS PARQUET",
		"INSERT INTO " + w.ident(p.name) + " (" + quoteAll(append(names(p.cols), ordColumn)) + ") SELECT " + strings.Join(sel, ", ") + " FROM " + quoteIdent(loadTable),
		"DROP TABLE " + quoteIdent(loadTable),
	}
	return w.inTx(ctx, func(tx pgx.Tx) error {
		for _, q := range stmts {
			if _, err := tx.Exec(ctx, q); err != nil {
				return err
			}
		}
		return nil
	})
}

// copyAuth is how COPY reads the staged files: as the IAM role, the only
// authorization COPY takes for Parquet.
func (d *Destination) copyAuth(lit func(string) string) string {
	if d.cfg.IAMRole == "default" {
		return "IAM_ROLE default"
	}
	return "IAM_ROLE " + lit(d.cfg.IAMRole)
}

// store is the bucket the staged files wait in.
type store struct {
	client *s3.Client
	up     *transfermanager.Client
	bucket string
	prefix string
}

func (d *Destination) store(ctx context.Context) (*store, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if d.cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(d.cfg.Region))
	}
	if !d.cfg.AccessKeyID.IsZero() {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			d.cfg.AccessKeyID.Reveal(), d.cfg.SecretAccessKey.Reveal(), d.cfg.SessionToken.Reveal())))
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("redshift: AWS config: %w", err))
	}
	if ac.Region == "" {
		return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, errors.New("redshift: no region for the bucket: set region in the config, or AWS_REGION"))
	}
	client := s3.NewFromConfig(ac)
	return &store{client: client, up: transfermanager.New(client), bucket: d.cfg.Bucket, prefix: strings.Trim(d.cfg.Prefix, "/")}, nil
}

// key is a new object for a staging table's file. Its random part keeps it
// from another deployment's, whose pipeline of the same name stages tables
// of the same names; COPY reads every object whose key starts with the one
// it names, and the part's fixed length keeps any key from starting with
// another.
func (s *store) key(name string) string {
	key := name + "." + uuid.NewString() + ".parquet"
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

func (s *store) url(key string) string { return "s3://" + s.bucket + "/" + key }

func (s *store) upload(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = s.up.UploadObject(ctx, &transfermanager.UploadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: f})
	if err != nil {
		return classifyS3(fmt.Errorf("upload s3://%s/%s: %w", s.bucket, key, err))
	}
	return nil
}

// remove is best effort: a bucket lifecycle rule ends what's left behind.
func (s *store) remove(ctx context.Context, key string) {
	_, _ = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
}

// classifyS3 gives S3's refusals their taxonomy codes.
func classifyS3(err error) error {
	ae, ok := errors.AsType[smithy.APIError](err)
	if !ok {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	switch ae.ErrorCode() {
	case "AccessDenied", "Forbidden", "AllAccessDisabled":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, err)
	case "InvalidAccessKeyId", "SignatureDoesNotMatch":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, err)
	case "ExpiredToken", "TokenRefreshRequired":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED, err)
	case "NoSuchBucket", "PermanentRedirect", "AuthorizationHeaderMalformed":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
	case "SlowDown", "ServiceUnavailable", "InternalError", "RequestTimeout":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	return err
}
