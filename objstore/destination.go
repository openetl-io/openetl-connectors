// Package objstore is what the files connectors share: the source and the
// destination of Parquet, Avro, CSV and JSON Lines files in a bucket, which
// each connector's Store opens, or on a local filesystem.
//
// The destination writes each stream's table as a directory of Parquet, CSV
// or JSON Lines files. Writes stream each stream's batches into files staged
// under the pipeline's _openetl/{pipeline}/staging, which PreCommit lists in
// the writer lane's staged descriptor. Commit copies every lane's to names
// derived from the pipeline, the epoch, its generation and the lane, then
// writes the stream's marker. A repeated or stale commit of an append copies
// the same files to the same names, so it needs no fence: an epoch is only
// committed once the checkpoint log has recorded it, under the generation in
// its names. An overwrite's files go to a directory of the read's, and the
// epoch the read ends in swaps the table's manifest to them with the
// store's conditional write, which refuses to move it back to an older
// epoch's.
package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/openetl-io/openetl/sdk/go/destination"

	"github.com/openetl-io/openetl-connectors/records"
)

// DestinationConfig is where the destination writes, and how: the settings
// of every connector's destination, besides its store's.
type DestinationConfig struct {
	// SCHEME://BUCKET/PREFIX, or file:// and an absolute path.
	URL string `json:"url"`
	// parquet, csv or jsonl. Default parquet.
	Format string `json:"format"`
	// Parquet: zstd, snappy, gzip or none, default zstd. CSV and JSON Lines:
	// gzip, zstd or none, default none.
	Compression string `json:"compression"`
	// Bytes a file reaches, compressed, before the next starts. Default 256
	// MiB.
	FileSize int64 `json:"fileSize"`
}

const (
	defaultFileSize = 256 << 20
	// What the destination keeps besides tables, under the URL.
	reserved = "_openetl"
	// Below an overwritten table's directory: the reads' files, and the
	// manifest that lists the last read's, as Delta Lake's symlink manifest
	// does, for Athena, Trino, Spark and the like.
	readsDir     = "_reads"
	manifestPath = "_symlink_format_manifest/manifest"
)

// DestinationCapabilities: files appear one at a time as Commit copies them,
// so an epoch is never visible at once across streams.
var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.StageAndApply,
	Scopes:                []destination.Scope{destination.ScopeStream},
	DefaultCommitInterval: time.Minute,
	MinCommitInterval:     time.Second,
	NanosecondTimestamps:  true,
	WriteModes:            []destination.WriteMode{destination.Append, destination.Overwrite},
	WriterLanes:           16,
}

type Destination struct {
	cfg   DestinationConfig
	store *store
}

var (
	_ destination.Destination = (*Destination)(nil)
	_ destination.CheckLister = (*Destination)(nil)
	_ destination.Preflighter = (*Destination)(nil)
	_ destination.ReadTracker = (*writer)(nil)
)

// NewDestination is the destination of config, whose store's settings
// decode into settings, which store makes the store of.
func NewDestination(config []byte, settings any, store NewStore) (*Destination, error) {
	cfg := DestinationConfig{Format: formatParquet, FileSize: defaultFileSize}
	if err := decode(config, &cfg, settings); err != nil {
		return nil, err
	}
	st, err := newStore(cfg.URL, store)
	if err != nil {
		return nil, err
	}
	switch cfg.Format {
	case formatParquet:
		if cfg.Compression == "" {
			cfg.Compression = "zstd"
		}
		if _, ok := parquetCodecs[cfg.Compression]; !ok {
			return nil, fmt.Errorf("objstore: Parquet compression %q is none of zstd, snappy, gzip and none", cfg.Compression)
		}
	case formatCSV, formatJSONL:
		if cfg.Compression == "" {
			cfg.Compression = "none"
		}
		if cfg.Compression != "gzip" && cfg.Compression != "zstd" && cfg.Compression != "none" {
			return nil, fmt.Errorf("objstore: %s compression %q is none of gzip, zstd and none", cfg.Format, cfg.Compression)
		}
	default:
		return nil, fmt.Errorf("objstore: format %q is none of parquet, csv and jsonl", cfg.Format)
	}
	if cfg.FileSize < 1<<20 {
		return nil, fmt.Errorf("objstore: fileSize %d is under 1 MiB", cfg.FileSize)
	}
	// Commit copies each file, and S3 copies at most 5 GiB at once; a file
	// can pass fileSize by a batch.
	if cfg.FileSize > 4<<30 {
		return nil, fmt.Errorf("objstore: fileSize %d is over 4 GiB", cfg.FileSize)
	}
	return &Destination{cfg: cfg, store: st}, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

func (d *Destination) bucket(ctx context.Context) (*blob.Bucket, error) { return d.store.open(ctx) }

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if s.Mode != destination.Append && s.Mode != destination.Overwrite {
			return nil, fmt.Errorf("objstore: stream %q: only append and overwrite writes are supported", s.Name)
		}
	}
	if cfg.Pipeline == "" || strings.ContainsAny(cfg.Pipeline, "/.") {
		return nil, fmt.Errorf("objstore: pipeline %q can't name files", cfg.Pipeline)
	}
	b, err := d.bucket(ctx)
	if err != nil {
		return nil, d.store.classify(err)
	}
	wctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &writer{
		d: d, b: b, cfg: cfg, ns: strings.Trim(cfg.Namespace, "/"),
		ctx: wctx, cancel: cancel,
		parts: map[uint64]map[string]*epochFiles{}, ended: map[uint64][]string{},
		tables: map[string]string{},
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
	// Only Write, StartReads, EndRead, PreCommit and Close use these.
	parts  map[uint64]map[string]*epochFiles
	starts map[string]time.Time
	ended  map[uint64][]string
	// Each stream's table, as its batches or schemas name it.
	mu     sync.Mutex
	tables map[string]string
}

// staged is the descriptor PreCommit returns. Paths are relative to the URL.
type staged struct {
	Generation uint64         `json:"generation"`
	Epoch      uint64         `json:"epoch"`
	Streams    []stagedStream `json:"streams"`
}

type stagedStream struct {
	Name string `json:"name"`
	// Empty when the lane doesn't know it, having had no rows of the stream.
	Table string       `json:"table,omitempty"`
	Files []stagedFile `json:"files"`
	// Overwrite: the read's directory, and whether the read ended in the
	// epoch.
	Read  string `json:"read,omitempty"`
	Ended bool   `json:"ended,omitempty"`
}

type stagedFile struct {
	Staged string `json:"staged"`
	Target string `json:"target"`
	Rows   int64  `json:"rows"`
}

func (w *writer) staging() string { return reserved + "/" + w.cfg.Pipeline + "/staging/" }

// marker is the key whose object says the stream's part of the epoch is
// applied.
func (w *writer) marker(stream string, epoch, gen uint64) string {
	return fmt.Sprintf("%s/%s/loads/%s/%020d-g%020d.json", reserved, w.cfg.Pipeline, url.PathEscape(stream), epoch, gen)
}

// tableOf is the stream's table directory, as sc names it.
func (w *writer) tableOf(sc *arrow.Schema, stream string) string {
	name := stream
	if t, ok := sc.Metadata().GetValue("openetl.table"); ok {
		name = t
	}
	if w.ns != "" {
		name = w.ns + "/" + name
	}
	w.mu.Lock()
	w.tables[stream] = name
	w.mu.Unlock()
	return name
}

// knownTable is the table of a stream the writer saw a batch or schema of,
// or the stream's name.
func (w *writer) knownTable(stream string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if t, ok := w.tables[stream]; ok {
		return t
	}
	if w.ns != "" {
		return w.ns + "/" + stream
	}
	return stream
}

// readDir is where an overwrite read that started at start keeps its files.
func readDir(table string, start time.Time) string {
	return fmt.Sprintf("%s/%s/%020d/", table, readsDir, start.UnixMicro())
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
	fw     fileWriter
	cancel context.CancelFunc
}

func (w *writer) startPart(e destination.Epoch, stream string, ef *epochFiles, sc *arrow.Schema) (*part, error) {
	k := fmt.Sprintf("%05d", len(ef.files))
	if w.cfg.Lane > 0 {
		k = fmt.Sprintf("l%d-%s", w.cfg.Lane, k)
	}
	ext := extension(w.d.cfg.Format, w.d.cfg.Compression)
	dir := ef.table + "/"
	if start, ok := w.starts[stream]; ok && w.mode(stream) == destination.Overwrite {
		dir = readDir(ef.table, start)
	}
	p := &part{schema: sc, file: stagedFile{
		Staged: fmt.Sprintf("%s%d/%d/%s/part-%s%s", w.staging(), e.Generation, e.N, ef.table, k, ext),
		Target: fmt.Sprintf("%s%s-%020d-g%020d-%s%s", dir, w.cfg.Pipeline, e.N, e.Generation, k, ext),
	}}
	ctx, cancel := context.WithCancel(w.ctx)
	bw, err := w.b.NewWriter(ctx, p.file.Staged, w.d.store.writeOptions(w.b, blob.WriterOptions{ContentType: contentType(w.d.cfg.Format)}, w.ctx))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("objstore: stage %s: %w", p.file.Staged, err)
	}
	p.fw, err = newFileWriter(w.d.cfg.Format, w.d.cfg.Compression, sc, bw)
	if err != nil {
		cancel()
		_ = bw.Close()
		return nil, fmt.Errorf("objstore: stage %s: %w", p.file.Staged, err)
	}
	p.cancel = cancel
	return p, nil
}

func contentType(format string) string {
	switch format {
	case formatCSV:
		return "text/csv"
	case formatJSONL:
		return "application/jsonl"
	}
	return "application/vnd.apache.parquet"
}

func (w *writer) mode(stream string) destination.WriteMode {
	for _, s := range w.cfg.Streams {
		if s.Name == stream {
			return s.Mode
		}
	}
	return 0
}

// finish closes the part's file, which makes it durable.
func (p *part) finish() (stagedFile, error) {
	defer p.cancel()
	p.file.Rows = p.fw.rows()
	if err := p.fw.close(); err != nil {
		return p.file, fmt.Errorf("objstore: stage %s: %w", p.file.Staged, err)
	}
	return p.file, nil
}

// abort drops the part's file, which was never written.
func (p *part) abort() {
	p.cancel()
	_ = p.fw.close()
}

func (w *writer) StartReads(_ context.Context, starts map[string]time.Time) error {
	w.starts = starts
	return nil
}

func (w *writer) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	w.ended[e.N] = append(w.ended[e.N], stream)
	return nil
}

func (w *writer) Write(_ context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) (err error) {
	defer func() { err = w.d.store.classify(err) }()
	mode := w.mode(stream)
	if mode == 0 {
		return fmt.Errorf("objstore: unknown stream %q", stream)
	}
	if _, ok := w.starts[stream]; mode == destination.Overwrite && !ok {
		return fmt.Errorf("objstore: stream %q overwrites, but its read has no start", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	if w.d.cfg.Format == formatParquet {
		// Parquet has no interval of nanoseconds.
		batch = records.IntervalsAsStructs(batch)
		defer batch.Release()
	}
	byStream := w.parts[e.N]
	if byStream == nil {
		byStream = map[string]*epochFiles{}
		w.parts[e.N] = byStream
	}
	ef := byStream[stream]
	if ef == nil {
		ef = &epochFiles{table: w.tableOf(batch.Schema(), stream)}
		byStream[stream] = ef
	}
	if ef.open != nil && !ef.open.schema.Equal(batch.Schema()) {
		if err := w.close(ef); err != nil {
			return err
		}
	}
	if ef.open == nil {
		p, err := w.startPart(e, stream, ef, batch.Schema())
		if err != nil {
			return err
		}
		ef.open = p
	}
	if err := ef.open.fw.write(batch); err != nil {
		return fmt.Errorf("objstore: stream %q: %w", stream, err)
	}
	if ef.open.fw.size() >= w.d.cfg.FileSize {
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
	defer func() { err = w.d.store.classify(err) }()
	desc := staged{Generation: e.Generation, Epoch: e.N}
	// Until it's staged, the epoch's files stay where Close drops them.
	byStream, ended := w.parts[e.N], w.ended[e.N]
	for _, s := range w.cfg.Streams {
		ef := byStream[s.Name]
		end := s.Mode == destination.Overwrite && slices.Contains(ended, s.Name)
		if ef == nil && !end {
			continue
		}
		ss := stagedStream{Name: s.Name, Ended: end}
		if ef != nil {
			if ef.open != nil {
				if err := w.close(ef); err != nil {
					return nil, err
				}
			}
			ss.Table, ss.Files = ef.table, ef.files
		}
		if s.Mode == destination.Overwrite {
			if _, ok := w.starts[s.Name]; !ok {
				return nil, fmt.Errorf("objstore: stream %q overwrites, but its read has no start", s.Name)
			}
			ss.Read = strconv.FormatInt(w.starts[s.Name].UnixMicro(), 10)
		}
		desc.Streams = append(desc.Streams, ss)
	}
	delete(w.parts, e.N)
	delete(w.ended, e.N)
	return json.Marshal(desc)
}

// descriptor merges the epoch's descriptors, one per lane of the attempt that
// staged it, by stream. A stream's table is the one a lane with its rows
// names, or else the one the writer knows.
func (w *writer) descriptor(e destination.Epoch, committables []destination.Staged) (staged, error) {
	out := staged{Generation: e.Generation, Epoch: e.N}
	if len(committables) == 0 {
		return out, errors.New("objstore: no staged descriptor")
	}
	at := map[string]int{}
	for _, c := range committables {
		var d staged
		if err := json.Unmarshal(c, &d); err != nil {
			return out, fmt.Errorf("objstore: staged descriptor: %w", err)
		}
		if d.Generation != e.Generation || d.Epoch != e.N {
			return out, fmt.Errorf("objstore: the staged descriptor is for epoch %d-g%d, not %d-g%d", d.Epoch, d.Generation, e.N, e.Generation)
		}
		for _, st := range d.Streams {
			i, ok := at[st.Name]
			if !ok {
				at[st.Name] = len(out.Streams)
				out.Streams = append(out.Streams, st)
				continue
			}
			m := &out.Streams[i]
			m.Files = append(m.Files, st.Files...)
			m.Ended = m.Ended || st.Ended
			if m.Table == "" {
				m.Table = st.Table
			}
			if m.Read != st.Read {
				return out, fmt.Errorf("objstore: epoch %d-g%d: the lanes' descriptors name different reads of %s", e.N, e.Generation, st.Name)
			}
		}
	}
	for i := range out.Streams {
		if out.Streams[i].Table == "" {
			out.Streams[i].Table = w.knownTable(out.Streams[i].Name)
		}
	}
	return out, nil
}

func (w *writer) Commit(ctx context.Context, e destination.Epoch, committables []destination.Staged) (_ destination.Committed, err error) {
	defer func() { err = w.d.store.classify(err) }()
	d, err := w.descriptor(e, committables)
	if err != nil {
		return destination.Committed{}, err
	}
	for _, st := range d.Streams {
		if err := w.apply(ctx, d, st); err != nil {
			return destination.Committed{}, err
		}
	}
	w.dropStaged(ctx, d)
	return destination.Committed{}, nil
}

// apply copies each staged file to its target, swaps an ended read's
// manifest in, then writes the stream's marker. A target whose staged file
// is gone was copied by an earlier apply.
func (w *writer) apply(ctx context.Context, d staged, st stagedStream) error {
	marker := w.marker(st.Name, d.Epoch, d.Generation)
	if done, err := w.b.Exists(ctx, marker); err != nil || done {
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
			return fmt.Errorf("objstore: apply %s: %w", st.Name, err)
		}
	}
	if st.Ended {
		if err := w.publish(ctx, d, st); err != nil {
			return err
		}
	}
	body, err := json.Marshal(st.Files)
	if err != nil {
		return err
	}
	if err := w.b.WriteAll(ctx, marker, body, w.d.store.writeOptions(w.b, blob.WriterOptions{ContentType: "application/json"}, nil)); err != nil {
		return fmt.Errorf("objstore: write the marker of %s: %w", st.Name, err)
	}
	return nil
}

// manifestState is what a table's manifest records of the epoch that wrote
// it, in its object's metadata.
type manifestState struct {
	epoch, generation uint64
	// The read's start, in microseconds.
	read int64
}

func (m manifestState) before(n manifestState) bool {
	return m.epoch < n.epoch || m.epoch == n.epoch && m.generation < n.generation
}

func (w *writer) manifest(ctx context.Context, table string) (*version, manifestState, error) {
	a, err := w.b.Attributes(ctx, table+"/"+manifestPath)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return nil, manifestState{}, nil
	}
	if err != nil {
		return nil, manifestState{}, err
	}
	var m manifestState
	m.epoch, _ = strconv.ParseUint(a.Metadata["epoch"], 10, 64)
	m.generation, _ = strconv.ParseUint(a.Metadata["generation"], 10, 64)
	m.read, _ = strconv.ParseInt(a.Metadata["read"], 10, 64)
	return w.d.store.version(a), m, nil
}

// publishAttempts is how many times publish writes a manifest that keeps
// changing, or that the store keeps refusing as changed.
const publishAttempts = 5

// publish makes the read's files the table's: it writes the manifest that
// lists them, unless an epoch as late or later wrote it, then drops older
// reads. The conditional write keeps a stale attempt from moving the
// manifest back.
func (w *writer) publish(ctx context.Context, d staged, st stagedStream) error {
	read, err := strconv.ParseInt(st.Read, 10, 64)
	if err != nil {
		return fmt.Errorf("objstore: stream %s's read %q: %w", st.Name, st.Read, err)
	}
	ours := manifestState{epoch: d.Epoch, generation: d.Generation, read: read}
	key := st.Table + "/" + manifestPath
	for attempt := 1; ; attempt++ {
		was, cur, err := w.manifest(ctx, st.Table)
		if err != nil {
			return fmt.Errorf("objstore: read %s: %w", key, err)
		}
		if was != nil && !cur.before(ours) {
			ours = cur
			break
		}
		dir := readDir(st.Table, time.UnixMicro(read))
		var lines []byte
		it := w.b.List(&blob.ListOptions{Prefix: dir})
		for {
			obj, err := it.Next(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					return fmt.Errorf("objstore: list %s: %w", dir, err)
				}
				break
			}
			lines = append(append(lines, w.d.store.location(obj.Key)...), '\n')
		}
		md := map[string]string{
			"epoch": strconv.FormatUint(d.Epoch, 10), "generation": strconv.FormatUint(d.Generation, 10), "read": st.Read,
		}
		err = w.d.store.swap(ctx, w.b, key, lines, md, was, func() (*version, error) {
			v, _, err := w.manifest(ctx, st.Table)
			return v, err
		})
		// A store may refuse a write as it does a conflict for as long as
		// something else, such as an Azure lease, holds the file.
		if errors.Is(err, errConflict) && attempt < publishAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("objstore: write %s: %w", key, err)
		}
		break
	}
	w.dropReads(ctx, st.Table, ours.read)
	return nil
}

// dropReads deletes the table's reads that started before the one its
// manifest lists, which nothing lists again. Best effort.
func (w *writer) dropReads(ctx context.Context, table string, current int64) {
	prefix := table + "/" + readsDir + "/"
	it := w.b.List(&blob.ListOptions{Prefix: prefix, Delimiter: "/"})
	for {
		obj, err := it.Next(ctx)
		if err != nil {
			return
		}
		start, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(obj.Key, prefix), "/"), 10, 64)
		if !obj.IsDir || err != nil || start >= current {
			continue
		}
		w.deleteAll(ctx, obj.Key)
	}
}

func (w *writer) deleteAll(ctx context.Context, prefix string) {
	it := w.b.List(&blob.ListOptions{Prefix: prefix})
	for {
		f, err := it.Next(ctx)
		if err != nil {
			return
		}
		_ = w.b.Delete(ctx, f.Key)
	}
}

// dropStaged is best effort, within ctx and cleanupTimeout: a staged file
// left behind is never applied again, because its stream's marker exists,
// and DropStale or Recover drops it later.
func (w *writer) dropStaged(ctx context.Context, d staged) {
	ctx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	for _, st := range d.Streams {
		for _, f := range st.Files {
			_ = w.b.Delete(ctx, f.Staged)
		}
	}
}

func (w *writer) Recover(ctx context.Context, gen uint64, e destination.Epoch, committables []destination.Staged) (_ destination.Recovery, err error) {
	defer func() { err = w.d.store.classify(err) }()
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
	needs, present, copied := 0, 0, 0
	for _, st := range d.Streams {
		done, err := w.b.Exists(ctx, w.marker(st.Name, d.Epoch, d.Generation))
		if err != nil {
			return 0, err
		}
		if done {
			continue
		}
		missing = append(missing, st)
		for _, f := range st.Files {
			needs++
			staged, err := w.b.Exists(ctx, f.Staged)
			if err != nil {
				return 0, err
			}
			target, err := w.b.Exists(ctx, f.Target)
			if err != nil {
				return 0, err
			}
			if target {
				copied++
			}
			if staged || target {
				present++
			}
		}
	}
	switch {
	case len(missing) == 0:
		w.dropStaged(ctx, d)
		return destination.Applied, nil
	case present == needs:
		for _, st := range missing {
			if err := w.apply(ctx, d, st); err != nil {
				return 0, err
			}
		}
		w.dropStaged(ctx, d)
		return destination.Redone, nil
	case copied == 0 && len(missing) == len(d.Streams):
		return destination.Lost, nil
	default:
		return 0, fmt.Errorf("objstore: epoch %d-g%d is partly applied and the staging for the rest is gone", d.Epoch, d.Generation)
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
		w.deleteAll(ctx, obj.Key)
	}
}

// The longest a cleanup takes, Close's or a commit's drop of its staged
// files, whose contexts OpenETL gives no end.
const cleanupTimeout = 30 * time.Second

// Close aborts open parts and drops the files of epochs that were never
// pre-committed, within ctx and cleanupTimeout: what a store that stops
// answering keeps stays for a later Recover or DropStale, as after a crash.
func (w *writer) Close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	// A part's abort, as of a multipart upload, ends with the writer's
	// context, as its write's options give it to the store.
	defer context.AfterFunc(ctx, w.cancel)()
	for _, byStream := range w.parts {
		for _, ef := range byStream {
			if ef.open != nil {
				ef.open.abort()
			}
			for _, f := range ef.files {
				_ = w.b.Delete(ctx, f.Staged)
			}
		}
	}
	w.cancel()
	return w.b.Close()
}
