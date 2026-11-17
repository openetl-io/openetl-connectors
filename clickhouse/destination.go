package clickhouse

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

type DestinationConfig struct {
	connConfig
}

// Destination writes each stream to a table of the pipeline's namespace,
// a database. Each lane's batches go into staging tables of their own
// shape. Commit builds a table of the epoch's rows as the target would hold
// them, under a name of its own once it's whole, and moves its parts into
// the target: a partition moves at once, and a move of parts already gone
// moves nothing, so a commit cut off anywhere is redone by running it again.
// Append and overwrite targets are MergeTree tables; merge targets are
// ReplacingMergeTree, versioned by _openetl_seq.
type Destination struct {
	cfg DestinationConfig
}

var (
	_ destination.Destination   = (*Destination)(nil)
	_ destination.SchemaApplier = (*writer)(nil)
	_ destination.ReadTracker   = (*writer)(nil)
	_ destination.Patcher       = (*writer)(nil)
)

func NewDestination(config []byte) (*Destination, error) {
	cfg := DestinationConfig{connConfig{User: "default", Database: "default"}}
	if err := decodeConfig(config, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.check("destination"); err != nil {
		return nil, err
	}
	return &Destination{cfg: cfg}, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

// connect connects without a default database, which may not exist yet:
// the destination names its database in each statement.
func (d *Destination) connect(ctx context.Context, conns int) (driver.Conn, error) {
	c := d.cfg.connConfig
	c.Database = ""
	return c.connect(ctx, conns, writeSettings)
}

// writeSettings are sent with each of the destination's queries: an insert
// is acknowledged once it's in a part, and a mutation once it's done.
var writeSettings = ch.Settings{"async_insert": 0, "mutations_sync": 2}

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if s.Mode == destination.Merge && len(s.PrimaryKey) == 0 {
			return nil, fmt.Errorf("clickhouse: stream %q merges, and has no primary key", s.Name)
		}
	}
	conn, err := d.connect(ctx, 4)
	if err != nil {
		return nil, err
	}
	w := &writer{
		conn:    conn,
		cfg:     cfg,
		db:      cmp.Or(cfg.Namespace, d.cfg.Database),
		staging: map[uint64]map[string]*laneStaging{},
		patches: map[uint64]map[string]*laneStaging{},
		ended:   map[uint64][]string{},
		streams: map[string]destination.Stream{},
		tables:  map[string]string{},
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	return w, nil
}

type writer struct {
	conn    driver.Conn
	cfg     destination.OpenConfig
	db      string
	streams map[string]destination.Stream
	// Only Write, Patch, StartReads, EndRead and PreCommit use these.
	staging map[uint64]map[string]*laneStaging
	patches map[uint64]map[string]*laneStaging
	starts  map[string]time.Time
	ended   map[uint64][]string // streams whose read ends in each epoch
	// Each stream's target, from its batches and schemas.
	mu     sync.Mutex
	tables map[string]string
	// Held by ApplySchema, and by a commit from its build to its move, which
	// a change to the target's columns mustn't come between.
	ddl sync.Mutex
}

// laneStaging is one stream's staging of one epoch on this lane: a table
// for each shape its batches came in, the rows numbered in the order they
// came.
type laneStaging struct {
	base   string
	target string
	// Merge: the key's columns.
	key    []string
	tables []rawTable
	rows   uint64
	// Merge: columns whose held cells keep the target's value.
	last []string
	// Patches: columns that fill another held column, by that column.
	fills map[string]string
	// Batches not yet inserted, all of one schema.
	pending      []arrow.RecordBatch
	pendingBytes int64
}

type rawTable struct {
	name   string
	schema *arrow.Schema
	cols   []column
}

// How many bytes of batches a lane keeps before it inserts them, so that
// each insert makes a part of a reasonable size.
const flushBytes = 32 << 20

const (
	idColumn      = "_openetl_id"
	opColumn      = "_openetl_op"
	seqColumn     = "_openetl_seq"
	deletedColumn = "_openetl_deleted"
	syncedColumn  = "_openetl_synced_at"
	// A changelog's: the columns an update didn't send. In a merge target,
	// those it never received.
	unchangedColumn = "_openetl_unchanged"
	// A merge target's ReplacingMergeTree's is_deleted: 1 for a row a hard
	// delete removed, which FINAL leaves out.
	isDeletedColumn = "_openetl_is_deleted"
	// A merge target's version, from a seq that is a Decimal of scale 0.
	versionColumn = "_openetl_version"
	// A row's held columns, a list; in the target, a map from each to the
	// hex of the _openetl_id of the version whose value is held.
	heldColumn = destination.HeldColumn
	heldType   = "Map(String, String)"
	// The order a lane staged its rows in, and the lane, in staging.
	rowColumn  = "_openetl_staged_row"
	laneColumn = "_openetl_lane"
	// The engine names renamed columns' source names in each schema's
	// metadata: a JSON object from destination name to source name.
	sourceNamesKey = "openetl.source_names"
)

var metadataColumns = []string{idColumn, opColumn, seqColumn, syncedColumn, deletedColumn, unchangedColumn, isDeletedColumn, versionColumn, heldColumn}

func isMetadata(c string) bool { return slices.Contains(metadataColumns, c) }

func hashName(prefix string, parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + hex.EncodeToString(h[:12])
}

func epochParts(pipeline, stream string, e destination.Epoch) []string {
	return []string{pipeline, stream, strconv.FormatUint(e.Generation, 10), strconv.FormatUint(e.N, 10)}
}

func stagingName(pipeline, stream string, e destination.Epoch, lane int) string {
	return hashName("_openetl_stage_", append(epochParts(pipeline, stream, e), strconv.Itoa(lane))...)
}

func patchName(pipeline, stream string, e destination.Epoch, lane int) string {
	return stagingName(pipeline, stream+"\x00patch", e, lane)
}

// joinName is the Join table the epoch's patches of a stream are looked up
// in. Its prefix is staging's, so Recover drops what's left of it.
func joinName(pipeline, stream string, gen, n uint64) string {
	return hashName("_openetl_stage_", append(epochParts(pipeline, stream, destination.Epoch{Generation: gen, N: n}), "join")...)
}

// readyName is the table the epoch's rows of a stream are built in, whole.
func readyName(pipeline, stream string, gen, n uint64) string {
	return hashName("_openetl_ready_", epochParts(pipeline, stream, destination.Epoch{Generation: gen, N: n})...)
}

func readName(pipeline, stream string, start time.Time) string {
	return hashName("_openetl_read_", pipeline, stream, strconv.FormatInt(start.UnixMicro(), 10))
}

// label is each table the destination keeps for a pipeline's comment, so
// Recover can find what earlier generations left.
type label struct {
	Pipeline   string `json:"pipeline"`
	Generation uint64 `json:"generation,omitempty"`
	Epoch      uint64 `json:"epoch,omitempty"`
	// A read's.
	Stream string `json:"stream,omitempty"`
}

func (l label) comment() string {
	b, _ := json.Marshal(l)
	return string(b)
}

func (w *writer) table(stream string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.tables[stream]
}

func (w *writer) setTable(stream, table string) {
	w.mu.Lock()
	w.tables[stream] = table
	w.mu.Unlock()
}

func (w *writer) q(table string) string { return qualified(w.db, table) }

func (w *writer) StartReads(_ context.Context, starts map[string]time.Time) error {
	w.starts = starts
	return nil
}

func (w *writer) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	w.ended[e.N] = append(w.ended[e.N], stream)
	return nil
}

func targetTable(schema *arrow.Schema, stream string) string {
	if t, ok := schema.Metadata().GetValue(tableKey); ok && t != "" {
		return t
	}
	return stream
}

// keyColumns returns the destination names of a stream's primary key, which
// the catalog gives by source name.
func keyColumns(schema *arrow.Schema, key []string) ([]string, error) {
	renamed := map[string]string{}
	if v, ok := schema.Metadata().GetValue(sourceNamesKey); ok {
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
		if len(schema.FieldIndices(out[i])) == 0 {
			return nil, fmt.Errorf("it merges on %q, which its batches don't have", k)
		}
	}
	return out, nil
}

func (w *writer) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	cfg, ok := w.streams[stream]
	if !ok {
		return fmt.Errorf("clickhouse: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	if cfg.Dedupe != destination.DedupeNone && len(batch.Schema().FieldIndices(idColumn)) == 0 {
		return fmt.Errorf("clickhouse: stream %q is deduped at apply, but its batches have no %s", stream, idColumn)
	}
	if _, ok := w.starts[stream]; cfg.Mode == destination.Overwrite && !ok {
		return fmt.Errorf("clickhouse: stream %q overwrites, but its read has no start", stream)
	}
	var key []string
	if cfg.Mode == destination.Merge {
		var err error
		if key, err = keyColumns(batch.Schema(), cfg.PrimaryKey); err != nil {
			return fmt.Errorf("clickhouse: stream %q: %w", stream, err)
		}
	}
	ls := w.laneStaging(w.staging, stagingName(w.cfg.Pipeline, stream, e, w.cfg.Lane), e, stream, targetTable(batch.Schema(), stream), key)
	w.setTable(stream, ls.target)
	for _, f := range batch.Schema().Fields() {
		if v, _ := f.Metadata.GetValue(destination.HeldKey); v == "last" && !slices.Contains(ls.last, f.Name) {
			ls.last = append(ls.last, f.Name)
		}
	}
	return w.add(ctx, e, ls, batch)
}

// Patch stages patches in tables of their own, which Commit applies after
// the epoch's rows.
func (w *writer) Patch(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	cfg, ok := w.streams[stream]
	switch {
	case !ok:
		return fmt.Errorf("clickhouse: unknown stream %q", stream)
	case cfg.Mode == destination.Overwrite:
		return fmt.Errorf("clickhouse: stream %q overwrites, and takes no patches", stream)
	case batch.NumRows() == 0:
		return nil
	case len(batch.Schema().FieldIndices(idColumn)) == 0 || len(batch.Schema().FieldIndices(heldColumn)) == 0:
		return fmt.Errorf("clickhouse: patches for %q need %s and %s", stream, idColumn, heldColumn)
	}
	var key []string
	if cfg.Mode == destination.Merge {
		var err error
		if key, err = keyColumns(batch.Schema(), cfg.PrimaryKey); err != nil {
			return fmt.Errorf("clickhouse: patches for %q: %w", stream, err)
		}
	}
	ls := w.laneStaging(w.patches, patchName(w.cfg.Pipeline, stream, e, w.cfg.Lane), e, stream, targetTable(batch.Schema(), stream), key)
	for _, f := range batch.Schema().Fields() {
		if held, ok := f.Metadata.GetValue(destination.FillsKey); ok {
			if ls.fills == nil {
				ls.fills = map[string]string{}
			}
			ls.fills[f.Name] = held
		}
	}
	return w.add(ctx, e, ls, batch)
}

func (w *writer) laneStaging(tables map[uint64]map[string]*laneStaging, base string, e destination.Epoch, stream, target string, key []string) *laneStaging {
	byStream := tables[e.N]
	if byStream == nil {
		byStream = map[string]*laneStaging{}
		tables[e.N] = byStream
	}
	ls := byStream[stream]
	if ls == nil {
		ls = &laneStaging{base: base, target: target, key: key}
		byStream[stream] = ls
	}
	return ls
}

// add keeps a batch until the lane has enough of one schema to insert.
func (w *writer) add(ctx context.Context, e destination.Epoch, ls *laneStaging, batch arrow.RecordBatch) error {
	if len(ls.pending) > 0 && !ls.pending[0].Schema().Equal(batch.Schema()) {
		if err := w.flush(ctx, e, ls); err != nil {
			return err
		}
	}
	batch.Retain()
	ls.pending = append(ls.pending, batch)
	for _, c := range batch.Columns() {
		for _, b := range c.Data().Buffers() {
			if b != nil {
				ls.pendingBytes += int64(b.Len())
			}
		}
	}
	if ls.pendingBytes >= flushBytes {
		return w.flush(ctx, e, ls)
	}
	return nil
}

func (ls *laneStaging) release() {
	for _, b := range ls.pending {
		b.Release()
	}
	ls.pending, ls.pendingBytes = nil, 0
}

// flush inserts the lane's pending batches into the staging table of their
// schema, which it creates, labelled.
func (w *writer) flush(ctx context.Context, e destination.Epoch, ls *laneStaging) error {
	if len(ls.pending) == 0 {
		return nil
	}
	defer ls.release()
	schema := ls.pending[0].Schema()
	i := slices.IndexFunc(ls.tables, func(t rawTable) bool { return t.schema.Equal(schema) })
	if i < 0 {
		cols, err := columnsOf(schema, isMetadata)
		if err != nil {
			return err
		}
		t := rawTable{name: ls.base + "_" + strconv.Itoa(len(ls.tables)), schema: schema, cols: cols}
		l := label{Pipeline: w.cfg.Pipeline, Generation: e.Generation, Epoch: e.N}
		defs := append(columnDefs(cols), ident(rowColumn)+" UInt64")
		if err := w.exec(ctx, "CREATE TABLE IF NOT EXISTS "+w.q(t.name)+" ("+strings.Join(defs, ", ")+") ENGINE = MergeTree ORDER BY "+ident(rowColumn)+
			" COMMENT "+literal(l.comment())); err != nil {
			return fmt.Errorf("clickhouse: create staging: %w", err)
		}
		ls.tables = append(ls.tables, t)
		i = len(ls.tables) - 1
	}
	t := ls.tables[i]
	b, err := w.conn.PrepareBatch(ctx, "INSERT INTO "+w.q(t.name)+" ("+quoteAll(append(names(t.cols), rowColumn))+")")
	if err != nil {
		return classify(fmt.Errorf("clickhouse: stage: %w", err))
	}
	defer func() { _ = b.Close() }()
	for _, rec := range ls.pending {
		n := int(rec.NumRows())
		for j, c := range t.cols {
			col, arr := b.Column(j), rec.Column(j)
			for r := range n {
				if err := col.AppendRow(c.value(arr, r)); err != nil {
					return fmt.Errorf("clickhouse: stage column %q: %w", c.name, err)
				}
			}
		}
		col := b.Column(len(t.cols))
		for range n {
			if err := col.AppendRow(ls.rows); err != nil {
				return err
			}
			ls.rows++
		}
	}
	if err := b.Send(); err != nil {
		return classify(fmt.Errorf("clickhouse: stage: %w", err))
	}
	return nil
}

func columnDefs(cols []column) []string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = ident(c.name) + " " + c.typ
	}
	return defs
}

// staged is the descriptor PreCommit returns.
type staged struct {
	Generation uint64         `json:"generation"`
	Epoch      uint64         `json:"epoch"`
	Streams    []stagedStream `json:"streams"`
}

const (
	modeOverwrite = "overwrite"
	modeMerge     = "merge"
)

type stagedStream struct {
	Name  string `json:"name"`
	Table string `json:"table"`
	// The lane's staging tables, in the order its rows came; none when an
	// overwrite read ended in the epoch without rows in it.
	Staging []string `json:"staging,omitempty"`
	Rows    uint64   `json:"rows,omitempty"`
	// One row per _openetl_id in the epoch, and only IDs the target doesn't
	// have yet.
	Dedupe bool `json:"dedupe,omitempty"`
	// One row per _openetl_id in the epoch.
	DedupeEpoch bool `json:"dedupe_epoch,omitempty"`
	// Empty for append.
	Mode string `json:"mode,omitempty"`
	// Merge: the key's columns, and whether a delete removes the row.
	Key         []string `json:"key,omitempty"`
	HardDeletes bool     `json:"hard_deletes,omitempty"`
	// Overwrite: the read's table, which takes the epoch's rows and, once
	// the read has ended in it, replaces the target's.
	Read  string `json:"read,omitempty"`
	Ended bool   `json:"ended,omitempty"`
	// Merge: columns whose held cells keep the target's value.
	Last []string `json:"last,omitempty"`
	// The lane's patches' staging, and the columns in it that fill another
	// held column, by that column.
	Patch []string          `json:"patch,omitempty"`
	Fills map[string]string `json:"fills,omitempty"`
	// Staging and Patch by lane, once mergeLanes has merged the lanes'
	// descriptors.
	stagings, patches [][]string
}

func (s stagedStream) deduped() bool { return s.Dedupe || s.DedupeEpoch }

// into is where the epoch's rows go: the read's table for overwrite.
func (s stagedStream) into() string {
	if s.Mode == modeOverwrite {
		return s.Read
	}
	return s.Table
}

func (s stagedStream) staged() []string {
	return slices.Concat(slices.Concat(s.stagings...), slices.Concat(s.patches...))
}

// PreCommit inserts what the lane still keeps; then every row of the epoch
// is in staging.
func (w *writer) PreCommit(ctx context.Context, e destination.Epoch) (destination.Staged, error) {
	desc := staged{Generation: e.Generation, Epoch: e.N}
	byStream, patches := w.staging[e.N], w.patches[e.N]
	defer func() {
		for _, ls := range byStream {
			ls.release()
		}
		for _, ls := range patches {
			ls.release()
		}
		delete(w.staging, e.N)
		delete(w.patches, e.N)
		delete(w.ended, e.N)
	}()
	for _, s := range w.cfg.Streams {
		st, pt := byStream[s.Name], patches[s.Name]
		ended := s.Mode == destination.Overwrite && slices.Contains(w.ended[e.N], s.Name)
		if st == nil && pt == nil && !ended {
			continue
		}
		ss := stagedStream{
			Name: s.Name, Table: w.table(s.Name),
			Dedupe: s.Dedupe == destination.DedupeTarget, DedupeEpoch: s.Dedupe == destination.DedupeEpoch,
		}
		var key []string
		if st != nil {
			if err := w.flush(ctx, e, st); err != nil {
				return nil, err
			}
			ss.Table, ss.Rows, ss.Last, key = st.target, st.rows, st.last, st.key
			for _, t := range st.tables {
				ss.Staging = append(ss.Staging, t.name)
			}
		}
		if pt != nil {
			if err := w.flush(ctx, e, pt); err != nil {
				return nil, err
			}
			ss.Table, ss.Fills = cmp.Or(ss.Table, pt.target), pt.fills
			if key == nil {
				key = pt.key
			}
			for _, t := range pt.tables {
				ss.Patch = append(ss.Patch, t.name)
			}
		}
		switch s.Mode {
		case destination.Overwrite:
			ss.Mode, ss.Read, ss.Ended = modeOverwrite, readName(w.cfg.Pipeline, s.Name, w.starts[s.Name]), ended
		case destination.Merge:
			ss.Mode, ss.Key, ss.HardDeletes = modeMerge, key, s.Deletes == destination.HardDelete
		}
		desc.Streams = append(desc.Streams, ss)
	}
	return json.Marshal(desc)
}

func (w *writer) Commit(ctx context.Context, _ destination.Epoch, committables []destination.Staged) (destination.Committed, error) {
	desc, err := w.mergeLanes(committables)
	if err != nil {
		return destination.Committed{}, err
	}
	c := destination.Committed{Collapsed: map[string]uint64{}}
	for _, s := range desc.Streams {
		n, err := w.apply(ctx, desc, s)
		if n > 0 {
			c.Collapsed[s.Name] = n
		}
		if err != nil {
			return c, err
		}
	}
	return c, nil
}

// mergeLanes merges the epoch's descriptors, one per lane of the attempt
// that staged it, by stream. A lane that didn't know a read ended, or had no
// rows of a stream, says less: its table can come from the schemas.
func (w *writer) mergeLanes(committables []destination.Staged) (staged, error) {
	var out staged
	at := map[string]int{}
	if len(committables) == 0 {
		return out, errors.New("clickhouse: no staged descriptor")
	}
	for i, c := range committables {
		var desc staged
		if err := json.Unmarshal(c, &desc); err != nil {
			return out, fmt.Errorf("clickhouse: staged descriptor: %w", err)
		}
		if i == 0 {
			out.Generation, out.Epoch = desc.Generation, desc.Epoch
		} else if desc.Generation != out.Generation || desc.Epoch != out.Epoch {
			return out, fmt.Errorf("clickhouse: staged descriptors of epochs %d-g%d and %d-g%d", out.Epoch, out.Generation, desc.Epoch, desc.Generation)
		}
		for _, s := range desc.Streams {
			j, ok := at[s.Name]
			if !ok {
				j = len(out.Streams)
				at[s.Name] = j
				out.Streams = append(out.Streams, stagedStream{
					Name: s.Name, Dedupe: s.Dedupe, DedupeEpoch: s.DedupeEpoch, Mode: s.Mode, HardDeletes: s.HardDeletes, Read: s.Read,
				})
			}
			m := &out.Streams[j]
			if m.Read != s.Read {
				return out, fmt.Errorf("clickhouse: two reads of %s in epoch %d-g%d", s.Name, out.Epoch, out.Generation)
			}
			m.Table, m.Rows, m.Ended = cmp.Or(m.Table, s.Table), m.Rows+s.Rows, m.Ended || s.Ended
			if len(m.Key) == 0 {
				m.Key = s.Key
			}
			if len(s.Staging) > 0 {
				m.stagings = append(m.stagings, s.Staging)
			}
			if len(s.Patch) > 0 {
				m.patches = append(m.patches, s.Patch)
			}
			for _, c := range s.Last {
				if !slices.Contains(m.Last, c) {
					m.Last = append(m.Last, c)
				}
			}
			for c, held := range s.Fills {
				if m.Fills == nil {
					m.Fills = map[string]string{}
				}
				m.Fills[c] = held
			}
		}
	}
	for i := range out.Streams {
		if s := &out.Streams[i]; s.Table == "" {
			s.Table = w.table(s.Name)
		}
	}
	return out, nil
}

func (w *writer) exec(ctx context.Context, query string) error {
	return classify(w.conn.Exec(ctx, query))
}

// marked reports whether the stream's marker for the epoch is there.
func (w *writer) marked(ctx context.Context, desc staged, stream string) (bool, error) {
	var n uint64
	err := w.conn.QueryRow(ctx, "SELECT count() FROM "+w.q(loadsTable)+" WHERE pipeline = ? AND stream = ? AND generation = ? AND epoch = ?",
		w.cfg.Pipeline, stream, desc.Generation, desc.Epoch).Scan(&n)
	if err != nil {
		return false, classify(fmt.Errorf("clickhouse: read the marker of %s: %w", stream, err))
	}
	return n > 0, nil
}

const (
	fenceTable = "_openetl_fence"
	loadsTable = "_openetl_loads"
)

// fence is the pipeline's highest generation, 0 if none has recovered.
func (w *writer) fence(ctx context.Context) (uint64, error) {
	var g uint64
	if err := w.conn.QueryRow(ctx, "SELECT max(generation) FROM "+w.q(fenceTable)+" WHERE pipeline = ?", w.cfg.Pipeline).Scan(&g); err != nil {
		return 0, classify(fmt.Errorf("clickhouse: read the fence: %w", err))
	}
	return g, nil
}

func (w *writer) checkFence(ctx context.Context) error {
	g, err := w.fence(ctx)
	switch {
	case err != nil:
		return err
	case g == 0:
		return errors.New("clickhouse: the fence is missing; Recover must run first")
	case g > w.cfg.Generation:
		return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, g, w.cfg.Generation)
	}
	return nil
}

// apply makes one stream's part of the epoch visible, unless its marker is
// there, and returns the staged rows it didn't insert, as Dedupe skipped
// them. Each step can run again after any other was cut off: the epoch's
// rows are built in a table that takes its name once whole, its parts move
// into the target, which moves nothing a second time, and then the marker
// is written.
func (w *writer) apply(ctx context.Context, desc staged, s stagedStream) (uint64, error) {
	if err := w.checkFence(ctx); err != nil {
		return 0, err
	}
	if done, err := w.marked(ctx, desc, s.Name); err != nil || done {
		return 0, err
	}
	w.ddl.Lock()
	defer w.ddl.Unlock()
	ready := readyName(w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch)
	have, err := w.exists(ctx, ready)
	if err != nil {
		return 0, err
	}
	var collapsed uint64
	if !have && len(s.stagings) > 0 {
		n, err := w.build(ctx, desc, s, ready)
		if err != nil {
			return 0, fmt.Errorf("clickhouse: apply %s: %w", s.Name, err)
		}
		if s.deduped() && s.Rows > n {
			collapsed = s.Rows - n
		}
		have = true
	}
	if have {
		// Another applier may have moved, marked and dropped the epoch's
		// table between the check above and the rename that made this one,
		// or a newer attempt replaced this one.
		if err := w.checkFence(ctx); err != nil {
			return 0, err
		}
		if done, err := w.marked(ctx, desc, s.Name); err != nil || done {
			if done {
				w.drop(ctx, ready)
			}
			return 0, err
		}
		if err := w.move(ctx, ready, s.into()); err != nil {
			return 0, fmt.Errorf("clickhouse: apply %s: %w", s.Name, err)
		}
	}
	if len(s.patches) > 0 {
		if err := w.patch(ctx, desc, s); err != nil {
			return 0, fmt.Errorf("clickhouse: patch %s: %w", s.Name, err)
		}
	}
	if s.Mode == modeOverwrite {
		if err := w.dropAbandonedReads(ctx, func(l label, table string) bool { return l.Stream == s.Name && table != s.Read }); err != nil {
			return 0, err
		}
		if s.Ended {
			if err := w.replace(ctx, s); err != nil {
				return 0, fmt.Errorf("clickhouse: replace %s: %w", s.Name, err)
			}
		}
	}
	if err := w.exec(ctx, "INSERT INTO "+w.q(loadsTable)+" (pipeline, stream, generation, epoch) VALUES ("+
		literal(w.cfg.Pipeline)+", "+literal(s.Name)+", "+strconv.FormatUint(desc.Generation, 10)+", "+strconv.FormatUint(desc.Epoch, 10)+")"); err != nil {
		return 0, fmt.Errorf("clickhouse: write the marker of %s: %w", s.Name, err)
	}
	w.drop(ctx, append(s.staged(), ready, joinName(w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch))...)
	if s.Mode == modeOverwrite && s.Ended {
		w.drop(ctx, s.Read)
	}
	return collapsed, nil
}

// drop is best effort: what's left behind is never applied twice, as its
// marker is there, and Recover drops it.
func (w *writer) drop(ctx context.Context, tables ...string) {
	for _, t := range tables {
		_ = w.conn.Exec(ctx, "DROP TABLE IF EXISTS "+w.q(t)+" SYNC")
	}
}

func (w *writer) exists(ctx context.Context, table string) (bool, error) {
	var n uint64
	if err := w.conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", w.db, table).Scan(&n); err != nil {
		return false, classify(err)
	}
	return n > 0, nil
}
