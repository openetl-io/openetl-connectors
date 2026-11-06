// Package iceberg has the Iceberg destination: append, overwrite and merge
// writes into Iceberg tables through a REST or Glue catalog. Writes stream
// each stream's batches into Parquet data files under its table's location,
// which PreCommit lists in the staged descriptor. Commit adds them in one
// snapshot per table, with the epoch's marker in the snapshot summary and in
// a table property, on the snapshot it checked the fence and the marker on:
// the catalog refuses the commit if another landed first. Overwrite keeps a
// read's files out of the table until the read ends, then replaces the
// table's files with them. Merge reads its files back at Commit, and adds
// equality deletes of the keys they change with the keys' new rows. It never
// changes a column's type, and takes holds: patches are staged like rows,
// and Commit rewrites the rows they fill, by key or by _openetl_id, with
// equality deletes in the same snapshot.
package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/google/uuid"

	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

type DestinationConfig struct {
	// rest or glue. Default rest.
	Catalog string `json:"catalog"`
	// The REST catalog's URI.
	URI string `json:"uri"`
	// The REST catalog's warehouse, or Glue's location for new tables.
	Warehouse string `json:"warehouse"`
	// Used when the pipeline names no namespace. Default "default".
	Namespace string `json:"namespace"`
	// REST: a bearer token, or OAuth2 client credentials as id:secret.
	Token      sdk.Secret `json:"token"`
	Credential sdk.Secret `json:"credential"`
	// AWS, for Glue and S3. Without a key, the AWS default credentials.
	Region          string     `json:"region"`
	AccessKeyID     sdk.Secret `json:"accessKeyId"`
	SecretAccessKey sdk.Secret `json:"secretAccessKey"`
	S3Endpoint      string     `json:"s3Endpoint"`
	// More catalog and file IO properties.
	Properties map[string]string `json:"properties"`
}

type Destination struct {
	cfg   DestinationConfig
	props ice.Properties
}

var (
	_ destination.Destination   = (*Destination)(nil)
	_ destination.Checker       = (*Destination)(nil)
	_ destination.SchemaApplier = (*writer)(nil)
	_ destination.ReadTracker   = (*writer)(nil)
	_ destination.Patcher       = (*writer)(nil)
)

func NewDestination(config []byte) (*Destination, error) {
	cfg := DestinationConfig{Catalog: "rest", Namespace: "default"}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("iceberg: config: %w", err)
	}
	switch cfg.Catalog {
	case "rest":
		if cfg.URI == "" {
			return nil, errors.New("iceberg: a REST catalog needs a uri")
		}
	case "glue":
	default:
		return nil, fmt.Errorf("iceberg: catalog %q is neither rest nor glue", cfg.Catalog)
	}
	if cfg.AccessKeyID.IsZero() != cfg.SecretAccessKey.IsZero() {
		return nil, errors.New("iceberg: accessKeyId and secretAccessKey go together")
	}
	props := ice.Properties{}
	for k, v := range cfg.Properties {
		props[k] = v
	}
	if cfg.S3Endpoint != "" {
		props[iceio.S3EndpointURL] = cfg.S3Endpoint
	}
	if cfg.Region != "" {
		props[iceio.S3Region] = cfg.Region
	}
	if !cfg.AccessKeyID.IsZero() {
		props[iceio.S3AccessKeyID] = cfg.AccessKeyID.Reveal()
		props[iceio.S3SecretAccessKey] = cfg.SecretAccessKey.Reveal()
	}
	return &Destination{cfg: cfg, props: props}, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

func (d *Destination) catalog(ctx context.Context) (catalog.Catalog, error) {
	if d.cfg.Catalog == "glue" {
		var opts []func(*awsconfig.LoadOptions) error
		if d.cfg.Region != "" {
			opts = append(opts, awsconfig.WithRegion(d.cfg.Region))
		}
		if !d.cfg.AccessKeyID.IsZero() {
			opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(d.cfg.AccessKeyID.Reveal(), d.cfg.SecretAccessKey.Reveal(), "")))
		}
		ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("iceberg: AWS config: %w", err)
		}
		if ep := d.props["glue.endpoint"]; ep != "" {
			ac.BaseEndpoint = aws.String(ep)
		}
		c, err := glue.NewCatalog(glue.WithAwsConfig(ac), glue.WithAwsProperties(glue.AwsProperties(d.props)))
		if err != nil {
			return nil, fmt.Errorf("iceberg: glue: %w", err)
		}
		return c, nil
	}
	opts := []rest.Option{rest.WithAdditionalProps(d.props)}
	if d.cfg.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(d.cfg.Warehouse))
	}
	if !d.cfg.Token.IsZero() {
		opts = append(opts, rest.WithOAuthToken(d.cfg.Token.Reveal()))
	}
	if !d.cfg.Credential.IsZero() {
		opts = append(opts, rest.WithCredential(d.cfg.Credential.Reveal()))
	}
	c, err := rest.NewCatalog(ctx, "openetl", d.cfg.URI, opts...)
	if err != nil {
		return nil, unavailable(fmt.Errorf("iceberg: catalog %s: %w", d.cfg.URI, err))
	}
	return c, nil
}

// unavailable classifies an error reaching the catalog or the store as the
// destination being unavailable.
func unavailable(err error) error {
	if _, ok := errors.AsType[net.Error](err); ok && !errors.Is(err, context.Canceled) {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE, err)
	}
	return err
}

// Check confirms the catalog answers and lists namespaces.
func (d *Destination) Check(ctx context.Context) error {
	c, err := d.catalog(ctx)
	if err != nil {
		return err
	}
	if _, err := c.ListNamespaces(ctx, nil); err != nil {
		return unavailable(fmt.Errorf("iceberg: list namespaces: %w", err))
	}
	return nil
}

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if !DestinationCapabilities.SupportsMode(s.Mode) {
			return nil, fmt.Errorf("iceberg: stream %q: only append, overwrite and merge writes are supported", s.Name)
		}
		if s.Mode == destination.Merge && len(s.PrimaryKey) == 0 {
			return nil, fmt.Errorf("iceberg: stream %q merges, and has no primary key", s.Name)
		}
	}
	if cfg.Scope == destination.ScopePipeline && d.cfg.Catalog == "glue" {
		return nil, errors.New("iceberg: Glue commits one table at a time; set delivery.commit.scope to stream")
	}
	c, err := d.catalog(ctx)
	if err != nil {
		return nil, err
	}
	ns := cfg.Namespace
	if ns == "" {
		ns = d.cfg.Namespace
	}
	wctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w := &writer{
		d: d, cat: c, cfg: cfg, ns: strings.Split(ns, "."),
		ctx: wctx, cancel: cancel,
		streams: map[string]destination.Stream{},
		tables:  map[string]*table.Table{},
		names:   map[string]string{},
		keys:    map[string][]string{},
		parts:   map[uint64]map[string]*epochFiles{},
		patches: map[uint64]map[string]*epochFiles{},
		ended:   map[uint64][]string{},
		reads:   map[string]*read{},
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	return w, nil
}

type writer struct {
	d       *Destination
	cat     catalog.Catalog
	cfg     destination.OpenConfig
	ns      table.Identifier
	streams map[string]destination.Stream
	// Data files are written on it, across Write calls.
	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex
	// Each stream's table, as Write and ApplySchema, or Patch, last made it
	// hold their schema, and its name.
	tables           map[string]*table.Table
	written, patched map[string]ensured
	names            map[string]string
	// Merge: each stream's key under destination names.
	keys map[string][]string
	// Columns whose values Write or Patch left out, since the table
	// couldn't take their type, until ApplySchema refuses them.
	dropped map[string]map[string]bool

	// Only Write, Patch, StartReads, EndRead, PreCommit and Recover use
	// these.
	parts   map[uint64]map[string]*epochFiles
	patches map[uint64]map[string]*epochFiles
	starts  map[string]time.Time
	ended   map[uint64][]string
	// Overwrite: each stream's read in progress.
	reads map[string]*read
}

// read is an overwrite read whose files stay out of its table until it ends.
type read struct {
	start int64
	table string
	files []string
}

// staged is the descriptor PreCommit returns.
type staged struct {
	Generation uint64         `json:"generation"`
	Epoch      uint64         `json:"epoch"`
	Namespace  []string       `json:"namespace"`
	Streams    []stagedStream `json:"streams"`
}

const (
	modeOverwrite = "overwrite"
	modeMerge     = "merge"
)

type stagedStream struct {
	Name  string `json:"name"`
	Table string `json:"table"`
	// The epoch's data files.
	Files []string `json:"files,omitempty"`
	// Empty for append.
	Mode string `json:"mode,omitempty"`
	// Overwrite: the read's start in microseconds, its earlier epochs' files,
	// and whether it ended in this epoch, which replaces the table's files
	// with the read's.
	Read    int64    `json:"read,omitempty"`
	Earlier []string `json:"earlier,omitempty"`
	Ended   bool     `json:"ended,omitempty"`
	// Merge: the key's columns, and whether a delete removes the row. The
	// files are read back at Commit, and no snapshot holds them.
	Key         []string `json:"key,omitempty"`
	HardDeletes bool     `json:"hard_deletes,omitempty"`
	// Merge: columns whose held cells keep the table's value.
	Last []string `json:"last,omitempty"`
	// The epoch's patches' files, in the table's schema, read back at
	// Commit; and the columns in them that fill another held column, by
	// that column.
	Patches []string          `json:"patches,omitempty"`
	Fills   map[string]string `json:"fills,omitempty"`
}

// applies reports whether committing the stream changes its table, and
// returns the files it needs: those it adds, and the patches.
func (s stagedStream) applies() ([]string, bool) {
	if s.Mode != modeOverwrite {
		files := append(slices.Clone(s.Files), s.Patches...)
		return files, len(files) > 0
	}
	if !s.Ended {
		return nil, false
	}
	return append(slices.Clone(s.Earlier), s.Files...), true
}

// adds is the files committing the stream adds to its table as they are.
func (s stagedStream) adds() []string {
	if s.Mode == modeOverwrite {
		return append(slices.Clone(s.Earlier), s.Files...)
	}
	return s.Files
}

const (
	commitRetries = 10
	// The snapshot summary's marker.
	pipelineKey   = "openetl.pipeline"
	streamKey     = "openetl.stream"
	generationKey = "openetl.generation"
	epochKey      = "openetl.epoch"
)

// The table properties: the generation that last recovered the pipeline,
// and the last epoch it committed, as "N:g".
func fenceKey(pipeline string) string  { return "openetl.fence." + pipeline }
func loadedKey(pipeline string) string { return "openetl.loaded." + pipeline }

func (w *writer) ident(ns []string, name string) table.Identifier {
	return append(slices.Clone(ns), name)
}

// ensured is a schema a table was made to hold, and its columns whose type
// the table can't take.
type ensured struct {
	schema  *arrow.Schema
	refused []destination.RefusedColumn
}

func (w *writer) setTable(stream string, tbl *table.Table, e ensured, patch bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tables[stream] = tbl
	if w.written == nil {
		w.written, w.patched = map[string]ensured{}, map[string]ensured{}
	}
	if patch {
		w.patched[stream] = e
	} else {
		w.written[stream] = e
	}
	w.names[stream] = tbl.Identifier()[len(tbl.Identifier())-1]
}

func (w *writer) name(stream string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.names[stream]
}

// tableFor returns the stream's table once it holds every column of sc,
// with the columns of sc whose type it can't take. patch: sc is a patch's.
func (w *writer) tableFor(ctx context.Context, stream string, sc *arrow.Schema, patch bool) (*table.Table, []destination.RefusedColumn, error) {
	w.mu.Lock()
	tbl, have := w.tables[stream], w.written[stream]
	if patch {
		have = w.patched[stream]
	}
	w.mu.Unlock()
	if tbl != nil && have.schema != nil && have.schema.Equal(sc) {
		return tbl, have.refused, nil
	}
	tbl, refused, err := w.ensure(ctx, targetTable(sc, stream), sc)
	if err != nil {
		return nil, nil, err
	}
	w.setTable(stream, tbl, ensured{schema: sc, refused: refused}, patch)
	return tbl, refused, nil
}

// ensure creates the table with sc's columns, or adds the columns it
// lacks. It never changes a column's type: it leaves the columns whose type
// can't take sc's as they are, and returns them. It isn't fenced: a column
// added for an epoch that never commits stays empty.
func (w *writer) ensure(ctx context.Context, name string, sc *arrow.Schema) (*table.Table, []destination.RefusedColumn, error) {
	want, err := icebergSchema(sc)
	if err != nil {
		return nil, nil, err
	}
	ident := w.ident(w.ns, name)
	for attempt := 0; ; attempt++ {
		tbl, err := w.cat.LoadTable(ctx, ident)
		if errors.Is(err, catalog.ErrNoSuchTable) {
			if tbl, err = w.create(ctx, ident, want); errors.Is(err, catalog.ErrTableAlreadyExists) && attempt < commitRetries {
				continue
			}
			return tbl, nil, err
		}
		if err != nil {
			return nil, nil, fmt.Errorf("iceberg: load %s: %w", strings.Join(ident, "."), err)
		}
		refused, add := compareSchemas(ident, tbl.Schema(), want)
		tx := tbl.NewTransaction()
		if err := tx.UpdateSchema(true, false).UnionByNameWith(add).Commit(); err != nil {
			return nil, nil, fmt.Errorf("iceberg: %s: %w", strings.Join(ident, "."), err)
		}
		// A transaction without changes commits nothing.
		tbl, err = tx.Commit(ctx)
		if err != nil && conflict(err) && attempt < commitRetries {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("iceberg: change the schema of %s: %w", strings.Join(ident, "."), err)
		}
		return tbl, refused, nil
	}
}

// compareSchemas returns the columns of want whose type have's can't take,
// and what want adds to have: new columns, and new fields of structs. A
// column keeps whether it's required.
func compareSchemas(ident table.Identifier, have, want *ice.Schema) ([]destination.RefusedColumn, *ice.Schema) {
	var refused []destination.RefusedColumn
	var keep []ice.NestedField
	for _, f := range want.Fields() {
		h, ok := have.FindFieldByName(f.Name)
		switch {
		case !ok:
			keep = append(keep, f)
			continue
		case holds(h.Type, f.Type):
			if !holds(f.Type, h.Type) {
				f.Required = h.Required
				keep = append(keep, f)
			}
			continue
		}
		r := destination.RefusedColumn{Column: f.Name, FromType: h.Type.String(), ToType: f.Type.String()}
		if promotes(h.Type, f.Type) {
			r.Reason = "it doesn't change a column's type"
			r.Fix = "ALTER TABLE " + sparkIdent(ident...) + " ALTER COLUMN " + sparkIdent(f.Name) + " TYPE " + sparkType(f.Type)
		} else {
			r.Reason = fmt.Sprintf("it doesn't change a column's type, and Iceberg can't change %s to %s", h.Type, f.Type)
		}
		refused = append(refused, r)
	}
	return refused, ice.NewSchema(0, keep...)
}

// limits are what tbl can't take: partition columns can't be held, and
// required columns can't take a null.
func limits(tbl *table.Table) destination.TableLimits {
	var l destination.TableLimits
	sc := tbl.Schema()
	for _, f := range sc.Fields() {
		if f.Required && !slices.Contains(metadataColumns, f.Name) && f.Name != heldColumn && f.Name != unchangedColumn {
			l.NotNull = append(l.NotNull, f.Name)
		}
	}
	spec := tbl.Spec()
	for _, pf := range spec.Fields() {
		for _, id := range pf.SourceIDs {
			if name, ok := sc.FindColumnName(id); ok && !slices.Contains(l.Unholdable, name) {
				l.Unholdable = append(l.Unholdable, name)
			}
		}
	}
	return l
}

func (w *writer) create(ctx context.Context, ident table.Identifier, sc *ice.Schema) (*table.Table, error) {
	ns := ident[:len(ident)-1]
	if err := w.cat.CreateNamespace(ctx, ns, nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		return nil, fmt.Errorf("iceberg: create namespace %s: %w", strings.Join(ns, "."), err)
	}
	opts := []catalog.CreateTableOpt{catalog.WithProperties(ice.Properties{
		table.PropertyFormatVersion: "2",
		fenceKey(w.cfg.Pipeline):    strconv.FormatUint(w.cfg.Generation, 10),
	})}
	if w.d.cfg.Catalog == "glue" && w.d.cfg.Warehouse != "" {
		opts = append(opts, catalog.WithLocation(strings.TrimSuffix(w.d.cfg.Warehouse, "/")+"/"+strings.Join(ident, "/")))
	}
	tbl, err := w.cat.CreateTable(ctx, ident, sc, opts...)
	if err != nil {
		return nil, fmt.Errorf("iceberg: create %s: %w", strings.Join(ident, "."), err)
	}
	return tbl, nil
}

// ApplySchema creates each stream's table and adds its new columns. It
// never changes a column's type: it refuses a column whose type changed,
// even where Iceberg could promote it, and the platform holds the column
// until the user changes it. It reports what each table can't take.
func (w *writer) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	out := destination.SchemaApplied{Limits: make(map[string]destination.TableLimits, len(streams))}
	for _, s := range streams {
		// Not from the cache: the user may have changed the table.
		tbl, refused, err := w.ensure(ctx, targetTable(s.Schema, s.Name), s.Schema)
		if err != nil {
			return destination.SchemaApplied{}, err
		}
		w.setTable(s.Name, tbl, ensured{schema: s.Schema, refused: refused}, false)
		w.mu.Lock()
		dropped := w.dropped[s.Name]
		delete(w.dropped, s.Name)
		w.mu.Unlock()
		for col := range dropped {
			if !slices.ContainsFunc(refused, func(r destination.RefusedColumn) bool { return r.Column == col }) {
				return destination.SchemaApplied{}, fmt.Errorf("iceberg: %s's column %q changed type while the epoch was written, and its values were left out; read the epoch again", s.Name, col)
			}
		}
		for _, r := range refused {
			r.Stream = s.Name
			out.Refused = append(out.Refused, r)
		}
		out.Limits[s.Name] = limits(tbl)
	}
	return out, nil
}

func (w *writer) StartReads(_ context.Context, starts map[string]time.Time) error {
	w.starts = starts
	for name, r := range w.reads {
		if start, ok := starts[name]; !ok || start.UnixMicro() != r.start {
			w.dropFiles(w.ns, r.table, r.files)
			delete(w.reads, name)
		}
	}
	return nil
}

func (w *writer) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	w.ended[e.N] = append(w.ended[e.N], stream)
	return nil
}

// epochFiles is what a stream wrote in an epoch, or its patches: its closed
// parts' files, and the part being written.
type epochFiles struct {
	files []string
	open  *part
	n     int
	// Columns marked to keep the table's value in held cells.
	last []string
	// Patches: columns that fill another held column, by that column.
	fills map[string]string
}

// part streams batches into data files in one table schema.
type part struct {
	schemaID int
	arrow    *arrow.Schema
	ch       chan arrow.RecordBatch
	done     chan struct{}
	files    []string
	err      error
}

func (w *writer) startPart(tbl *table.Table, e destination.Epoch, stream string, k int, patch bool) (*part, error) {
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("iceberg: %w", err)
	}
	p := &part{schemaID: tbl.Schema().ID, arrow: sc, ch: make(chan arrow.RecordBatch, 1), done: make(chan struct{})}
	// Data file names come from it, so each generation and epoch has its own.
	kind := ""
	if patch {
		kind = "patch/"
	}
	id := uuid.NewSHA1(uuid.NameSpaceURL, fmt.Appendf(nil, "openetl:%s%s/%s/%d/%d/%d", kind, w.cfg.Pipeline, stream, e.Generation, e.N, k))
	batches := func(yield func(arrow.RecordBatch, error) bool) {
		for b := range p.ch {
			if !yield(b, nil) {
				return
			}
		}
	}
	go func() {
		defer close(p.done)
		for df, err := range table.WriteRecords(w.ctx, tbl, sc, batches, table.WithWriteUUID(id), table.WithMaxWriteWorkers(1)) {
			if err != nil {
				p.err = fmt.Errorf("iceberg: write %s: %w", stream, err)
				return
			}
			p.files = append(p.files, df.FilePath())
		}
	}()
	return p, nil
}

func (p *part) send(b arrow.RecordBatch) error {
	select {
	case p.ch <- b:
		return nil
	case <-p.done:
		b.Release()
		return p.err
	}
}

// finish closes the part's last file, and returns all of them.
func (p *part) finish() ([]string, error) {
	close(p.ch)
	<-p.done
	for b := range p.ch {
		b.Release()
	}
	return p.files, p.err
}

func (w *writer) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	cfg, ok := w.streams[stream]
	if !ok {
		return fmt.Errorf("iceberg: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	if _, ok := w.starts[stream]; cfg.Mode == destination.Overwrite && !ok {
		return fmt.Errorf("iceberg: stream %q overwrites, but its read has no start", stream)
	}
	if _, ok := w.keys[stream]; cfg.Mode == destination.Merge && !ok {
		key, err := keyColumns(batch.Schema(), cfg.PrimaryKey)
		if err != nil {
			return fmt.Errorf("iceberg: stream %q: %w", stream, err)
		}
		w.keys[stream] = key
	}
	ef, err := w.stage(ctx, w.parts, e, stream, batch, false)
	if err != nil {
		return err
	}
	for _, f := range batch.Schema().Fields() {
		if v, _ := f.Metadata.GetValue(destination.HeldKey); v == "last" && !slices.Contains(ef.last, f.Name) {
			ef.last = append(ef.last, f.Name)
		}
	}
	return nil
}

// Patch stages patches in files of their own, which Commit reads back and
// applies after the epoch's rows, in the same snapshot.
func (w *writer) Patch(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	cfg, ok := w.streams[stream]
	switch {
	case !ok:
		return fmt.Errorf("iceberg: unknown stream %q", stream)
	case cfg.Mode != destination.Merge && cfg.Mode != destination.Append:
		return fmt.Errorf("iceberg: stream %q overwrites, and takes no patches", stream)
	case batch.NumRows() == 0:
		return nil
	case !batch.Schema().HasField(idColumn) || !batch.Schema().HasField(heldColumn):
		return fmt.Errorf("iceberg: patches for %q need %s and %s", stream, idColumn, heldColumn)
	}
	if _, ok := w.keys[stream]; cfg.Mode == destination.Merge && !ok {
		key, err := keyColumns(batch.Schema(), cfg.PrimaryKey)
		if err != nil {
			return fmt.Errorf("iceberg: patches for %q: %w", stream, err)
		}
		w.keys[stream] = key
	}
	ef, err := w.stage(ctx, w.patches, e, stream, batch, true)
	if err != nil {
		return err
	}
	for _, f := range batch.Schema().Fields() {
		if held, ok := f.Metadata.GetValue(destination.FillsKey); ok {
			if ef.fills == nil {
				ef.fills = map[string]string{}
			}
			ef.fills[f.Name] = held
		}
	}
	return nil
}

// stage streams batch into the stream's files for epoch e in parts. A
// column whose type the table can't take is left out: ApplySchema refuses
// it before the epoch is recorded.
func (w *writer) stage(ctx context.Context, parts map[uint64]map[string]*epochFiles, e destination.Epoch, stream string, batch arrow.RecordBatch, patch bool) (*epochFiles, error) {
	tbl, refused, err := w.tableFor(ctx, stream, batch.Schema(), patch)
	if err != nil {
		return nil, err
	}
	if len(refused) > 0 {
		w.mu.Lock()
		if w.dropped == nil {
			w.dropped = map[string]map[string]bool{}
		}
		if w.dropped[stream] == nil {
			w.dropped[stream] = map[string]bool{}
		}
		for _, r := range refused {
			w.dropped[stream][r.Column] = true
		}
		w.mu.Unlock()
		batch = without(batch, refused)
		defer batch.Release()
	}
	byStream := parts[e.N]
	if byStream == nil {
		byStream = map[string]*epochFiles{}
		parts[e.N] = byStream
	}
	ef := byStream[stream]
	if ef == nil {
		ef = &epochFiles{}
		byStream[stream] = ef
	}
	if ef.open != nil && ef.open.schemaID != tbl.Schema().ID {
		files, err := ef.open.finish()
		ef.open = nil
		if err != nil {
			return nil, err
		}
		ef.files = append(ef.files, files...)
	}
	if ef.open == nil {
		if ef.open, err = w.startPart(tbl, e, stream, ef.n, patch); err != nil {
			return nil, err
		}
		ef.n++
	}
	b, err := conform(ctx, batch, ef.open.arrow)
	if err != nil {
		return nil, fmt.Errorf("iceberg: stream %q: %w", stream, err)
	}
	return ef, ef.open.send(b)
}

// without is batch without the refused columns, retained.
func without(batch arrow.RecordBatch, refused []destination.RefusedColumn) arrow.RecordBatch {
	var fields []arrow.Field
	var cols []arrow.Array
	for i, f := range batch.Schema().Fields() {
		if !slices.ContainsFunc(refused, func(r destination.RefusedColumn) bool { return r.Column == f.Name }) {
			fields, cols = append(fields, f), append(cols, batch.Column(i))
		}
	}
	md := batch.Schema().Metadata()
	return array.NewRecordBatch(arrow.NewSchema(fields, &md), cols, batch.NumRows())
}

// finish closes the files' last part, and returns all of them.
func (ef *epochFiles) finish() ([]string, error) {
	if ef == nil {
		return nil, nil
	}
	files := ef.files
	if ef.open != nil {
		more, err := ef.open.finish()
		ef.open = nil
		if err != nil {
			return nil, err
		}
		files = append(files, more...)
	}
	return files, nil
}

// PreCommit closes the epoch's data files. They are durable once closed,
// and invisible until Commit adds them to their tables.
func (w *writer) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	desc := staged{Generation: e.Generation, Epoch: e.N, Namespace: w.ns}
	byStream, patches := w.parts[e.N], w.patches[e.N]
	delete(w.parts, e.N)
	delete(w.patches, e.N)
	ended := w.ended[e.N]
	delete(w.ended, e.N)
	for _, s := range w.cfg.Streams {
		ef, pf := byStream[s.Name], patches[s.Name]
		files, err := ef.finish()
		if err != nil {
			return nil, err
		}
		patched, err := pf.finish()
		if err != nil {
			return nil, err
		}
		isEnd := s.Mode == destination.Overwrite && slices.Contains(ended, s.Name)
		if len(files) == 0 && len(patched) == 0 && !isEnd {
			continue
		}
		ss := stagedStream{Name: s.Name, Table: w.name(s.Name), Files: files, Patches: patched}
		if pf != nil {
			ss.Fills = pf.fills
		}
		if s.Mode == destination.Merge {
			ss.Mode, ss.Key, ss.HardDeletes = modeMerge, w.keys[s.Name], s.Deletes == destination.HardDelete
			if ef != nil {
				ss.Last = ef.last
			}
		}
		if s.Mode == destination.Overwrite {
			r := w.reads[s.Name]
			if r == nil {
				r = &read{start: w.starts[s.Name].UnixMicro(), table: ss.Table}
				w.reads[s.Name] = r
			}
			ss.Mode, ss.Read, ss.Earlier, ss.Ended = modeOverwrite, r.start, slices.Clone(r.files), isEnd
			r.files = append(r.files, files...)
			if isEnd {
				delete(w.reads, s.Name)
			}
		}
		desc.Streams = append(desc.Streams, ss)
	}
	return json.Marshal(desc)
}

func (w *writer) Commit(ctx context.Context, _ destination.Epoch, committables []destination.Staged) (destination.Committed, error) {
	s, err := destination.One(committables)
	if err != nil {
		return destination.Committed{}, err
	}
	var desc staged
	if err := json.Unmarshal(s, &desc); err != nil {
		return destination.Committed{}, fmt.Errorf("iceberg: staged descriptor: %w", err)
	}
	return destination.Committed{}, w.apply(ctx, desc, desc.Streams)
}

// pending is a table's transaction, with the files it wrote, which no
// snapshot holds unless it commits, and a merge's staged files, which it
// makes unneeded once it does.
type pending struct {
	tx      *table.Transaction
	ns      []string
	table   string
	written []string
	staged  []string
}

func (w *writer) discard(ps []pending) {
	for _, p := range ps {
		w.dropFiles(p.ns, p.table, p.written)
	}
}

func (w *writer) committed(p pending) { w.dropFiles(p.ns, p.table, p.staged) }

// apply commits each stream that changes its table and has no marker yet:
// all in one catalog transaction, or with stream scope one per table. A
// conflict means another commit landed first, so it checks again.
func (w *writer) apply(ctx context.Context, desc staged, streams []stagedStream) error {
	for attempt := 0; ; attempt++ {
		txs, err := w.prepare(ctx, desc, streams)
		if err != nil || len(txs) == 0 {
			return err
		}
		if testHookBeforeCommit != nil {
			testHookBeforeCommit(w)
		}
		err = w.commit(ctx, txs)
		if err == nil {
			return nil
		}
		if !conflict(err) || attempt >= commitRetries {
			return fmt.Errorf("iceberg: commit epoch %d-g%d: %w", desc.Epoch, desc.Generation, err)
		}
		select {
		case <-time.After(time.Duration(min(attempt+1, 20)) * 100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// conflict reports whether a commit lost to another. Catalogs that check
// requirements themselves, such as Glue's, return a failed one unwrapped.
func conflict(err error) bool {
	return errors.Is(err, table.ErrCommitFailed) || strings.Contains(err.Error(), "requirement failed")
}

// testHookBeforeCommit runs between checking the tables and committing.
var testHookBeforeCommit func(*writer)

// commit commits txs. It drops the files of those the catalog refused or
// that it never sent, but not of one whose outcome it doesn't know.
func (w *writer) commit(ctx context.Context, txs []pending) error {
	if w.cfg.Scope == destination.ScopePipeline && len(txs) > 1 {
		m, err := catalog.NewMultiTableTransaction(w.cat)
		if err != nil {
			w.discard(txs)
			return err
		}
		for _, p := range txs {
			if err := m.AddTransaction(p.tx); err != nil {
				w.discard(txs)
				return err
			}
		}
		if err := m.Commit(ctx); err != nil {
			if conflict(err) {
				w.discard(txs)
			}
			return err
		}
		for _, p := range txs {
			w.committed(p)
		}
		return nil
	}
	for i, p := range txs {
		if _, err := p.tx.Commit(ctx); err != nil {
			if conflict(err) {
				w.discard(txs[i : i+1])
			}
			w.discard(txs[i+1:])
			return err
		}
		w.committed(p)
	}
	return nil
}

// prepare loads each stream's table, and returns a transaction for each one
// still to apply, on the snapshot it checked.
func (w *writer) prepare(ctx context.Context, desc staged, streams []stagedStream) (txs []pending, err error) {
	defer func() {
		if err != nil {
			w.discard(txs)
			txs = nil
		}
	}()
	for _, s := range streams {
		files, ok := s.applies()
		if !ok {
			continue
		}
		ident := w.ident(desc.Namespace, s.Table)
		if s.Table == "" {
			if len(files) > 0 {
				return nil, fmt.Errorf("iceberg: stream %q staged files for no table", s.Name)
			}
			continue
		}
		tbl, err := w.cat.LoadTable(ctx, ident)
		if errors.Is(err, catalog.ErrNoSuchTable) && len(files) == 0 {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("iceberg: load %s: %w", strings.Join(ident, "."), err)
		}
		if err := w.checkFence(tbl.Properties(), w.cfg.Generation); err != nil {
			return nil, err
		}
		if applied(tbl.Properties(), w.cfg.Pipeline, desc) {
			continue
		}
		tx, written, err := w.transaction(ctx, tbl, desc, s, s.adds())
		if err != nil {
			w.dropFiles(desc.Namespace, s.Table, written)
			return txs, fmt.Errorf("iceberg: %s: %w", strings.Join(ident, "."), err)
		}
		p := pending{tx: tx, ns: desc.Namespace, table: s.Table, written: written, staged: s.Patches}
		if s.Mode == modeMerge {
			p.staged = append(slices.Clone(s.Files), s.Patches...)
		}
		txs = append(txs, p)
	}
	return txs, nil
}

// transaction stages the stream's epoch in a transaction on tbl. It returns
// the files it wrote, also on failure.
func (w *writer) transaction(ctx context.Context, tbl *table.Table, desc staged, s stagedStream, paths []string) (*table.Transaction, []string, error) {
	// Without it, a table that sets commit.retry.num-retries has iceberg-go
	// replay the commit on top of a conflicting one.
	tx := tbl.NewTransaction()
	if err := tx.AssertRefSnapshotID(""); err != nil {
		return nil, nil, err
	}
	marker := ice.Properties{
		pipelineKey: w.cfg.Pipeline, streamKey: s.Name,
		generationKey: strconv.FormatUint(desc.Generation, 10), epochKey: strconv.FormatUint(desc.Epoch, 10),
	}
	var written []string
	var err error
	if s.Mode == modeMerge {
		written, err = w.mergeInto(ctx, tx, tbl, s, marker)
	} else {
		written, err = w.addFiles(ctx, tx, tbl, s, paths, marker)
	}
	if err != nil {
		return nil, written, err
	}
	if err := tx.SetProperties(ice.Properties{loadedKey(w.cfg.Pipeline): fmt.Sprintf("%d:%d", desc.Epoch, desc.Generation)}); err != nil {
		return nil, written, err
	}
	return tx, written, nil
}

// addFiles stages an append or an overwrite of the staged files in tx,
// and an append's patches. It returns the files it wrote.
func (w *writer) addFiles(ctx context.Context, tx *table.Transaction, tbl *table.Table, s stagedStream, paths []string, marker ice.Properties) ([]string, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	files := make([]ice.DataFile, len(paths))
	for i, p := range paths {
		if files[i], err = table.FileToDataFile(ctx, fs, p, tbl.Schema(), tbl.Spec(), 0, tbl.Properties()); err != nil {
			return nil, fmt.Errorf("staged file %s: %w", p, err)
		}
	}
	if s.Mode == "" && len(s.Patches) > 0 {
		return w.appendPatched(ctx, tx, tbl, s, files, marker)
	}
	var current []ice.DataFile
	if s.Mode == modeOverwrite && tbl.CurrentSnapshot() != nil {
		tasks, err := tbl.Scan().PlanFiles(ctx)
		if err != nil {
			return nil, fmt.Errorf("plan its files: %w", err)
		}
		for _, t := range tasks {
			current = append(current, t.File)
		}
	}
	switch {
	case len(current) > 0:
		err = tx.ReplaceDataFilesWithDataFiles(ctx, current, files, marker, table.WithoutAutoNameMapping())
	case len(files) > 0:
		err = tx.AddDataFiles(ctx, files, marker, table.WithoutAutoNameMapping())
	}
	return nil, err
}

func (w *writer) checkFence(props ice.Properties, gen uint64) error {
	if v, ok := props[fenceKey(w.cfg.Pipeline)]; ok {
		fence, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return fmt.Errorf("iceberg: fence %q: %w", v, err)
		}
		if fence > gen {
			return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, gen)
		}
	}
	return nil
}

// applied reports whether the table holds the epoch: its last committed
// epoch is this one or a later one. Epochs commit in order, and only the
// one at the log's HEAD is ever recovered.
func applied(props ice.Properties, pipeline string, desc staged) bool {
	v, ok := props[loadedKey(pipeline)]
	if !ok {
		return false
	}
	n, g, ok := strings.Cut(v, ":")
	if !ok {
		return false
	}
	epoch, err1 := strconv.ParseUint(n, 10, 64)
	gen, err2 := strconv.ParseUint(g, 10, 64)
	if err1 != nil || err2 != nil {
		return false
	}
	return epoch > desc.Epoch || (epoch == desc.Epoch && gen == desc.Generation)
}

// raiseFence sets each table's fence to gen, and fails if one is higher.
func (w *writer) raiseFence(ctx context.Context, ns []string, names []string, gen uint64) error {
	for _, name := range names {
		ident := w.ident(ns, name)
		for attempt := 0; ; attempt++ {
			tbl, err := w.cat.LoadTable(ctx, ident)
			if errors.Is(err, catalog.ErrNoSuchTable) {
				break
			}
			if err != nil {
				return fmt.Errorf("iceberg: load %s: %w", strings.Join(ident, "."), err)
			}
			if err := w.checkFence(tbl.Properties(), gen); err != nil {
				return err
			}
			if tbl.Properties()[fenceKey(w.cfg.Pipeline)] == strconv.FormatUint(gen, 10) {
				break
			}
			tx := tbl.NewTransaction()
			if err := tx.SetProperties(ice.Properties{fenceKey(w.cfg.Pipeline): strconv.FormatUint(gen, 10)}); err != nil {
				return err
			}
			if _, err = tx.Commit(ctx); err == nil {
				break
			}
			if !conflict(err) || attempt >= commitRetries {
				return fmt.Errorf("iceberg: raise the fence of %s: %w", strings.Join(ident, "."), err)
			}
		}
	}
	return nil
}

func (w *writer) Recover(ctx context.Context, gen uint64, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	s, err := destination.One(committables)
	if err != nil {
		return 0, err
	}
	var desc staged
	if !e.IsZero() {
		if err := json.Unmarshal(s, &desc); err != nil {
			return 0, fmt.Errorf("iceberg: staged descriptor: %w", err)
		}
	}
	w.mu.Lock()
	known := make([]string, 0, len(w.names))
	for _, n := range w.names {
		known = append(known, n)
	}
	w.mu.Unlock()
	if err := w.raiseFence(ctx, w.ns, known, gen); err != nil {
		return 0, err
	}
	if e.IsZero() {
		return destination.Applied, nil
	}
	var names []string
	for _, st := range desc.Streams {
		if st.Table != "" {
			names = append(names, st.Table)
		}
	}
	if err := w.raiseFence(ctx, desc.Namespace, names, gen); err != nil {
		return 0, err
	}

	var missing []stagedStream
	applying, needs, present := 0, 0, 0
	for _, st := range desc.Streams {
		files, ok := st.applies()
		if !ok || st.Table == "" {
			continue
		}
		applying++
		tbl, err := w.cat.LoadTable(ctx, w.ident(desc.Namespace, st.Table))
		if errors.Is(err, catalog.ErrNoSuchTable) {
			tbl = nil
		} else if err != nil {
			return 0, fmt.Errorf("iceberg: load %s: %w", st.Table, err)
		}
		if tbl != nil && applied(tbl.Properties(), w.cfg.Pipeline, desc) {
			if st.Mode == modeMerge {
				w.dropFiles(desc.Namespace, st.Table, st.Files)
			}
			w.dropFiles(desc.Namespace, st.Table, st.Patches)
			continue
		}
		missing = append(missing, st)
		if len(files) == 0 {
			continue
		}
		needs++
		if tbl != nil && w.allExist(ctx, tbl, files) {
			present++
		}
	}
	var r destination.Recovery
	switch {
	case len(missing) == 0:
		r = destination.Applied
	case present == needs:
		if err := w.apply(ctx, desc, missing); err != nil {
			return 0, err
		}
		r = destination.Redone
	case present == 0 && len(missing) == applying:
		r = destination.Lost
		for _, st := range desc.Streams {
			w.dropFiles(desc.Namespace, st.Table, st.Files)
			w.dropFiles(desc.Namespace, st.Table, st.Patches)
		}
	default:
		return 0, fmt.Errorf("iceberg: epoch %d-g%d is partly applied and the staged files for the rest are gone", desc.Epoch, desc.Generation)
	}
	// A read that hasn't ended continues, unless StartReads says it began
	// again.
	for _, st := range desc.Streams {
		if st.Mode != modeOverwrite || st.Ended {
			continue
		}
		files := st.Earlier
		if r != destination.Lost {
			files = append(slices.Clone(files), st.Files...)
		}
		w.reads[st.Name] = &read{start: st.Read, table: st.Table, files: files}
	}
	return r, nil
}

// allExist reports whether every file can be opened.
func (w *writer) allExist(ctx context.Context, tbl *table.Table, paths []string) bool {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return false
	}
	for _, p := range paths {
		f, err := fs.Open(p)
		if err != nil {
			return false
		}
		_ = f.Close()
	}
	return true
}

// dropFiles deletes data files that no table holds. Best effort: whatever
// stays behind is an orphan, which Iceberg's orphan-file cleanup removes.
func (w *writer) dropFiles(ns []string, name string, paths []string) {
	if len(paths) == 0 || name == "" {
		return
	}
	tbl, err := w.cat.LoadTable(w.ctx, w.ident(ns, name))
	if err != nil {
		return
	}
	fs, err := tbl.FS(w.ctx)
	if err != nil {
		return
	}
	for _, p := range paths {
		_ = fs.Remove(p)
	}
}

// Close drops the files of epochs that were never pre-committed.
func (w *writer) Close(context.Context) error {
	for _, parts := range []map[uint64]map[string]*epochFiles{w.parts, w.patches} {
		for _, byStream := range parts {
			for stream, ef := range byStream {
				files := ef.files
				if ef.open != nil {
					more, _ := ef.open.finish()
					files = append(files, more...)
				}
				w.dropFiles(w.ns, w.name(stream), files)
			}
		}
	}
	w.cancel()
	return nil
}
