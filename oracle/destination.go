package oracle

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	go_ora "github.com/sijms/go-ora/v2"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

type DestinationConfig struct {
	// oracle://user:password@host:port/service, with go-ora's options.
	DSN sdk.Secret `json:"dsn"`
}

// Destination writes append, overwrite and merge streams to tables in a
// schema, the pipeline's namespace or the user's own, staged into tables of
// the user's own schema, one per stream, epoch and writer lane, and applied
// as one behind a fence row with a marker per stream in _openetl_loads.
//
// Oracle commits a transaction before and after each DDL statement, so the
// destination does every DDL an apply needs before the apply's transaction,
// which only changes rows: the fence check, the stream's rows and its marker
// commit together or not at all.
type Destination struct {
	cfg DestinationConfig
	// The dsn's host:port, empty if it names none; its user and service.
	addr, user, service string
}

var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.StageAndApply,
	Scopes:                []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
	DedupeAtApply:         true,
	DefaultCommitInterval: 10 * time.Second,
	MinCommitInterval:     time.Second,
	MaxIdentifierLength:   128,
	NanosecondTimestamps:  true,
	WriteModes:            []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge},
	Holds:                 destination.Holds{ByKey: true, ByID: true, Last: true, Guarded: true},
	WriterLanes:           16,
}

var (
	_ destination.Destination   = (*Destination)(nil)
	_ destination.SchemaApplier = (*writer)(nil)
	_ destination.ReadTracker   = (*writer)(nil)
	_ destination.Patcher       = (*writer)(nil)
)

func NewDestination(config []byte) (*Destination, error) {
	var cfg DestinationConfig
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("oracle: config: %w", err)
	}
	if cfg.DSN.IsZero() {
		return nil, errors.New("oracle: config needs a dsn")
	}
	if err := parseDSN(cfg.DSN.Reveal()); err != nil {
		return nil, err
	}
	d := &Destination{cfg: cfg}
	if u, err := url.Parse(cfg.DSN.Reveal()); err == nil {
		d.user, d.service = u.User.Username(), strings.Trim(u.Path, "/")
		if h := u.Hostname(); h != "" {
			d.addr = net.JoinHostPort(h, cmpOr(u.Port(), "1521"))
		}
	}
	return d, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if s.Mode == destination.Merge && len(s.PrimaryKey) == 0 {
			return nil, fmt.Errorf("oracle: stream %q merges, and has no primary key", s.Name)
		}
	}
	// Only lane 0 commits alongside its writes, and a fenced drop takes two
	// connections.
	conns := 4
	if cfg.Lane > 0 {
		conns = 1
	}
	db, err := open(ctx, d.cfg.DSN.Reveal(), conns)
	if err != nil {
		return nil, err
	}
	user, _, compatible, err := server(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	w := &writer{
		db:      db,
		cfg:     cfg,
		user:    user,
		schema:  cmp.Or(cfg.Namespace, user),
		boolean: major(compatible) >= 23,
		staging: map[uint64]map[string]*stagingTable{},
		patches: map[uint64]map[string]*stagingTable{},
		ended:   map[uint64][]string{},
		streams: map[string]destination.Stream{},
		tables:  map[string]string{},
		indexed: map[string]bool{},
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	return w, nil
}

// server is the session's user, and the database's version and COMPATIBLE
// setting.
func server(ctx context.Context, db *sql.DB) (user, version, compatible string, err error) {
	if err := db.QueryRowContext(ctx, "SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM dual").Scan(&user); err != nil {
		return "", "", "", classify(fmt.Errorf("oracle: read the session's user: %w", err))
	}
	if _, err := db.ExecContext(ctx, "BEGIN DBMS_UTILITY.DB_VERSION(:1, :2); END;",
		go_ora.Out{Dest: &version, Size: 100}, go_ora.Out{Dest: &compatible, Size: 100}); err != nil {
		return "", "", "", classify(fmt.Errorf("oracle: read the database's version: %w", err))
	}
	return user, version, compatible, nil
}

// major is a version's first number; 0 if it has none.
func major(version string) int {
	n, _ := strconv.Atoi(strings.SplitN(version, ".", 2)[0])
	return n
}

type writer struct {
	db  *sql.DB
	cfg destination.OpenConfig
	// The session's user, whose schema holds the destination's own tables,
	// and the schema the targets are in.
	user, schema string
	// The server takes BOOLEAN columns.
	boolean bool
	streams map[string]destination.Stream
	// Only Write, Patch, StartReads, EndRead and PreCommit use these.
	staging map[uint64]map[string]*stagingTable
	patches map[uint64]map[string]*stagingTable
	starts  map[string]time.Time
	ended   map[uint64][]string // streams whose read ends in each epoch
	// Each stream's target, from its batches and schemas.
	mu     sync.Mutex
	tables map[string]string
	// Only Commit, Recover and ApplySchema use these: indexes known to
	// exist, by name, and whether the target schema is.
	indexed map[string]bool
	ready   bool
}

type stagingTable struct {
	name   string
	target string
	cols   []dcol
	rows   uint64
	// The next row's _openetl_ord.
	ord int64
	// Merge: the key's columns.
	key []string
	// Columns whose held cells keep the target's value.
	last []string
	// Patches: columns that fill another held column, by that column.
	fills map[string]string
	// Tables of the epoch before name, each narrower than the next: a batch
	// wider than a table starts the next one, as no table's column is ever
	// changed to a wider type.
	earlier []string
}

// all are the staging's tables, in order.
func (st *stagingTable) all() []string { return append(slices.Clone(st.earlier), st.name) }

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
	// Empty when an overwrite read ended in the epoch without rows in it.
	Staging string `json:"staging"`
	// The lane's later staging tables of the epoch, each taking batches
	// wider than the one before.
	Wider []string `json:"wider,omitempty"`
	// One row per _openetl_id in the epoch, and only IDs the target doesn't
	// have yet.
	Dedupe bool `json:"dedupe,omitempty"`
	// One row per _openetl_id in the epoch.
	DedupeEpoch bool `json:"dedupe_epoch,omitempty"`
	// Staged rows, to count those the apply skips.
	Rows uint64 `json:"rows,omitempty"`
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
	// The epoch's patches' staging, and the columns in it that fill
	// another held column.
	Patch string            `json:"patch,omitempty"`
	Fills map[string]string `json:"fills,omitempty"`
	// As Wider, for Patch.
	WiderPatches []string `json:"wider_patches,omitempty"`
	// Staging and Patch, of every lane that has them, once mergeLanes has
	// merged the lanes' descriptors.
	stagings, patches []string
}

func (s stagedStream) deduped() bool { return s.Dedupe || s.DedupeEpoch }

// target is a table of the target schema; own one of the user's.
func (w *writer) target(name string) string { return quoteIdent(w.schema) + "." + quoteIdent(name) }
func (w *writer) own(name string) string    { return quoteIdent(w.user) + "." + quoteIdent(name) }

// into is where the epoch's rows go: the read's table for overwrite.
func (w *writer) into(s stagedStream) string {
	if s.Mode == modeOverwrite {
		return w.own(s.Read)
	}
	return w.target(s.Table)
}

const (
	fenceTable  = "_openetl_fence"
	loadsTable  = "_openetl_loads"
	stagePrefix = "_openetl_stage_"
	readPrefix  = "_openetl_read_"
)

func digest(n int, parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(h[:])[:n]
}

// stageName names a staging table of a pipeline's generation gen, which
// dropStale reads back from it.
func stageName(pipeline string, gen uint64, parts ...string) string {
	return stagePrefix + digest(16, pipeline) + "_" + strconv.FormatUint(gen, 10) + "_" + digest(24, append([]string{pipeline}, parts...)...)
}

func stagingName(pipeline, stream string, e destination.Epoch, lane int) string {
	return stageName(pipeline, e.Generation, stream, strconv.FormatUint(e.N, 10), strconv.Itoa(lane))
}

func patchName(pipeline, stream string, e destination.Epoch, lane int) string {
	return stagingName(pipeline, stream+"\x00patch", e, lane)
}

// readName names an overwrite read's table by its pipeline and stream, which
// dropAbandonedReads reads back from it.
func readName(pipeline, stream string, start time.Time) string {
	return readPrefix + digest(16, pipeline) + "_" + digest(16, stream) + "_" + digest(16, strconv.FormatInt(start.UnixMicro(), 10))
}

func indexName(schema, table string) string    { return "_openetl_id_" + digest(24, schema, table) }
func keyIndexName(schema, table string) string { return "_openetl_key_" + digest(24, schema, table) }

func (w *writer) StartReads(_ context.Context, starts map[string]time.Time) error {
	w.starts = starts
	return nil
}

func (w *writer) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	w.ended[e.N] = append(w.ended[e.N], stream)
	return nil
}

func (w *writer) setTable(stream, table string) {
	w.mu.Lock()
	w.tables[stream] = table
	w.mu.Unlock()
}

func (w *writer) table(stream string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.tables[stream]
}

func (w *writer) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	cfg, ok := w.streams[stream]
	if !ok {
		return fmt.Errorf("oracle: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	if cfg.Dedupe != destination.DedupeNone && len(batch.Schema().FieldIndices(colID)) == 0 {
		return fmt.Errorf("oracle: stream %q is deduped at apply, but its batches have no %s", stream, colID)
	}
	if _, ok := w.starts[stream]; cfg.Mode == destination.Overwrite && !ok {
		return fmt.Errorf("oracle: stream %q overwrites, but its read has no start", stream)
	}
	var key []string
	if cfg.Mode == destination.Merge {
		var err error
		if key, err = keyColumns(batch.Schema(), cfg.PrimaryKey); err != nil {
			return fmt.Errorf("oracle: stream %q: %w", stream, err)
		}
	}
	st, err := w.stage(ctx, w.staging, stagingName(w.cfg.Pipeline, stream, e, w.cfg.Lane), e, stream, targetTable(batch.Schema(), stream), key, batch)
	if err != nil {
		return err
	}
	w.setTable(stream, st.target)
	for _, f := range batch.Schema().Fields() {
		if v, _ := f.Metadata.GetValue(destination.HeldKey); v == "last" && !slices.Contains(st.last, f.Name) {
			st.last = append(st.last, f.Name)
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
		return fmt.Errorf("oracle: unknown stream %q", stream)
	case cfg.Mode != destination.Merge && cfg.Mode != destination.Append:
		return fmt.Errorf("oracle: stream %q overwrites, and takes no patches", stream)
	case batch.NumRows() == 0:
		return nil
	case len(batch.Schema().FieldIndices(colID)) == 0 || len(batch.Schema().FieldIndices(colHeld)) == 0:
		return fmt.Errorf("oracle: patches for %q need %s and %s", stream, colID, colHeld)
	}
	var key []string
	if cfg.Mode == destination.Merge {
		var err error
		if key, err = keyColumns(batch.Schema(), cfg.PrimaryKey); err != nil {
			return fmt.Errorf("oracle: patches for %q: %w", stream, err)
		}
	}
	target := w.table(stream)
	if target == "" {
		target = targetTable(batch.Schema(), stream)
	}
	st, err := w.stage(ctx, w.patches, patchName(w.cfg.Pipeline, stream, e, w.cfg.Lane), e, stream, target, key, batch)
	if err != nil {
		return err
	}
	for _, f := range batch.Schema().Fields() {
		if held, ok := f.Metadata.GetValue(destination.FillsKey); ok {
			if st.fills == nil {
				st.fills = map[string]string{}
			}
			st.fills[f.Name] = held
		}
	}
	return nil
}

// stage inserts a batch into the stream's staging table for epoch e in
// tables, which it creates or widens.
func (w *writer) stage(ctx context.Context, tables map[uint64]map[string]*stagingTable, name string, e destination.Epoch, stream, target string, key []string, batch arrow.RecordBatch) (*stagingTable, error) {
	if err := validName("table", target); err != nil {
		return nil, err
	}
	cols, err := destColumns(batch.Schema(), key, w.boolean)
	if err != nil {
		return nil, err
	}
	byStream := tables[e.N]
	if byStream == nil {
		byStream = map[string]*stagingTable{}
		tables[e.N] = byStream
	}
	st := byStream[stream]
	if st == nil {
		st = &stagingTable{name: name, target: target, key: key}
		if err := w.createStaging(ctx, st.name, cols); err != nil {
			return nil, fmt.Errorf("oracle: create staging for %s: %w", stream, err)
		}
		st.cols = cols
		byStream[stream] = st
	} else if wider, err := w.addColumns(ctx, st, cols); err != nil {
		return nil, err
	} else if wider != nil {
		st.earlier = append(st.earlier, st.name)
		st.name = name + "_" + strconv.Itoa(len(st.earlier)+1)
		if err := w.createStaging(ctx, st.name, wider); err != nil {
			return nil, fmt.Errorf("oracle: create staging for %s: %w", stream, err)
		}
		st.cols = wider
	}
	if err := w.insertRows(ctx, st, cols, batch); err != nil {
		return nil, classify(fmt.Errorf("oracle: stage %s: %w", stream, err))
	}
	st.rows += uint64(batch.NumRows())
	return st, nil
}

// Rows and bytes of bound values per INSERT: go-ora binds each column's
// values as one array.
const (
	chunkRows  = 5000
	chunkBytes = 8 << 20
)

// insertRows inserts a batch into st in one transaction, a chunk of rows
// per INSERT. Each value takes st's column type, which may be wider.
func (w *writer) insertRows(ctx context.Context, st *stagingTable, cols []dcol, batch arrow.RecordBatch) error {
	n := int(batch.NumRows())
	arrs := batch.Columns()
	into := typesOf(st.cols)
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for lo := 0; lo < n; {
		vals := make([][]any, len(cols))
		widths := make([]int, len(cols))
		var ords []any
		hi, size := lo, 0
		for ; hi < n && hi-lo < chunkRows && size < chunkBytes; hi++ {
			for j, c := range cols {
				var v any
				if arrs[j].IsNull(hi) || c.value == nil {
					if c.key {
						return fmt.Errorf("column %q is in the merge key, and row %d has no value in it", c.name, hi)
					}
				} else if v, err = c.bind(arrs[j], hi); err != nil {
					return fmt.Errorf("column %q, row %d: %w", c.name, hi, err)
				}
				k := bound(v)
				widths[j], size = max(widths[j], k), size+k
				vals[j] = append(vals[j], v)
			}
			ords = append(ords, st.ord)
			st.ord++
		}
		// go-ora binds a value longer than 32767 bytes as a LONG, which must
		// come after every other bind of the statement.
		order := make([]int, len(cols))
		for j := range order {
			order[j] = j
		}
		slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(widths[a], widths[b]) })
		names := []string{quoteIdent(colOrd)}
		exprs := []string{":1"}
		args := []any{ords}
		for _, j := range order {
			c := cols[j]
			bind := ":" + strconv.Itoa(len(args)+1)
			if c.conv != "" {
				bind = fmt.Sprintf(c.conv, bind)
			}
			names = append(names, quoteIdent(c.name))
			exprs = append(exprs, convert(bind, c.typ, into[c.name]))
			args = append(args, vals[j])
		}
		q := "INSERT INTO " + w.own(st.name) + " (" + strings.Join(names, ", ") + ") VALUES (" + strings.Join(exprs, ", ") + ")"
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return err
		}
		lo = hi
	}
	return tx.Commit()
}

// bound is about how many bytes a bound value takes.
func bound(v any) int {
	switch x := v.(type) {
	case string:
		return len(x)
	case []byte:
		return len(x)
	}
	return 22
}

// keyColumns returns the destination names of a stream's primary key, which
// the catalog gives by source name.
func keyColumns(schema *arrow.Schema, key []string) ([]string, error) {
	renamed := map[string]string{}
	if v, ok := schema.Metadata().GetValue(metaSourceNames); ok {
		var names map[string]string
		if err := json.Unmarshal([]byte(v), &names); err != nil {
			return nil, fmt.Errorf("its %s metadata: %w", metaSourceNames, err)
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

// validName refuses a name Oracle can't quote.
func validName(what, name string) error {
	if name == "" || strings.ContainsAny(name, "\"\x00") {
		return fmt.Errorf("oracle: the %s name %q can't be an Oracle name, which is never empty and holds no double quote or NUL", what, name)
	}
	return nil
}

// createStaging creates a staging table in the user's schema, with
// _openetl_ord.
func (w *writer) createStaging(ctx context.Context, name string, cols []dcol) error {
	defs := append([]string{quoteIdent(colOrd) + " NUMBER(19) NOT NULL"}, columnDefs(cols)...)
	return w.ddl(ctx, "CREATE TABLE "+w.own(name)+" ("+strings.Join(defs, ", ")+")", 955)
}

// addColumns adds columns that appeared since the staging table was created.
// If a column's type widened, it returns the columns a table must have to take
// the batch, and the table is left as it is. An older batch's narrower columns
// fit.
func (w *writer) addColumns(ctx context.Context, st *stagingTable, cols []dcol) ([]dcol, error) {
	wider := slices.Clone(st.cols)
	changed := false
	for _, c := range cols {
		i := slices.IndexFunc(st.cols, func(x dcol) bool { return x.name == c.name })
		switch {
		case i < 0:
			if err := w.ddl(ctx, "ALTER TABLE "+w.own(st.name)+" ADD ("+columnDef(c)+")"); err != nil {
				return nil, fmt.Errorf("oracle: add column %q to staging: %w", c.name, err)
			}
			st.cols = append(st.cols, c)
			wider = append(wider, c)
		case st.cols[i].typ == c.typ, c.value == nil, fits(c.typ, st.cols[i].typ):
		case fits(st.cols[i].typ, c.typ):
			wider[i], changed = c, true
		default:
			return nil, fmt.Errorf("oracle: column %q changed from %s to %s within an epoch", c.name, st.cols[i].typ, c.typ)
		}
	}
	if !changed {
		return nil, nil
	}
	return wider, nil
}

// columnDef is a column's definition. _openetl_deleted is false in rows from
// before it was added.
func columnDef(c dcol) string {
	def := quoteIdent(c.name) + " " + c.typ
	if c.name == colDeleted {
		if c.typ == "BOOLEAN" {
			return def + " DEFAULT FALSE NOT NULL"
		}
		return def + " DEFAULT 0 NOT NULL"
	}
	return def
}

func columnDefs(cols []dcol) []string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = columnDef(c)
	}
	return defs
}

func names(cols []dcol) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.name
	}
	return out
}

func typesOf(cols []dcol) map[string]string {
	out := make(map[string]string, len(cols))
	for _, c := range cols {
		out[c.name] = c.typ
	}
	return out
}

// PreCommit has nothing to flush: every Write committed its rows to staging.
func (w *writer) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	desc := staged{Generation: e.Generation, Epoch: e.N}
	byStream, patches := w.staging[e.N], w.patches[e.N]
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
		key := []string(nil)
		if pt != nil {
			tables := pt.all()
			ss.Table, ss.Patch, ss.WiderPatches, ss.Fills, key = pt.target, tables[0], tables[1:], pt.fills, pt.key
		}
		if st != nil {
			tables := st.all()
			ss.Table, ss.Staging, ss.Wider, ss.Rows, ss.Last, key = st.target, tables[0], tables[1:], st.rows, st.last, st.key
		}
		switch s.Mode {
		case destination.Overwrite:
			// A read that ends without rows in the epoch still replaces the
			// target's.
			ss.Mode, ss.Read, ss.Ended = modeOverwrite, readName(w.cfg.Pipeline, s.Name, w.starts[s.Name]), ended
		case destination.Merge:
			ss.Mode, ss.Key, ss.HardDeletes = modeMerge, key, s.Deletes == destination.HardDelete
		}
		desc.Streams = append(desc.Streams, ss)
	}
	delete(w.staging, e.N)
	delete(w.patches, e.N)
	delete(w.ended, e.N)
	return json.Marshal(desc)
}

func (w *writer) Commit(ctx context.Context, _ destination.Epoch, committables []destination.Staged) (destination.Committed, error) {
	desc, err := w.mergeLanes(committables)
	if err != nil {
		return destination.Committed{}, err
	}
	c := destination.Committed{Collapsed: map[string]uint64{}}
	return c, w.apply(ctx, desc, desc.Streams, c.Collapsed)
}

// mergeLanes merges the epoch's descriptors, one per lane of the attempt
// that staged it, by stream. A lane that didn't know a read ended, or had no
// rows of a stream, says less: its table can come from the schemas.
func (w *writer) mergeLanes(committables []destination.Staged) (staged, error) {
	var out staged
	at := map[string]int{}
	if len(committables) == 0 {
		return out, errors.New("oracle: no staged descriptor")
	}
	for i, c := range committables {
		var desc staged
		if err := json.Unmarshal(c, &desc); err != nil {
			return out, fmt.Errorf("oracle: staged descriptor: %w", err)
		}
		if i == 0 {
			out.Generation, out.Epoch = desc.Generation, desc.Epoch
		} else if desc.Generation != out.Generation || desc.Epoch != out.Epoch {
			return out, fmt.Errorf("oracle: staged descriptors of epochs %d-g%d and %d-g%d", out.Epoch, out.Generation, desc.Epoch, desc.Generation)
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
				return out, fmt.Errorf("oracle: two reads of %s in epoch %d-g%d", s.Name, out.Epoch, out.Generation)
			}
			m.Table, m.Rows, m.Ended = cmp.Or(m.Table, s.Table), m.Rows+s.Rows, m.Ended || s.Ended
			if len(m.Key) == 0 {
				m.Key = s.Key
			}
			for _, c := range s.Last {
				if !slices.Contains(m.Last, c) {
					m.Last = append(m.Last, c)
				}
			}
			if len(s.Fills) > 0 && m.Fills == nil {
				m.Fills = map[string]string{}
			}
			maps.Copy(m.Fills, s.Fills)
			if s.Staging != "" {
				m.stagings = append(append(m.stagings, s.Staging), s.Wider...)
			}
			if s.Patch != "" {
				m.patches = append(append(m.patches, s.Patch), s.WiderPatches...)
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

// apply applies each stream that has no marker yet, then drops its staging.
// It adds the rows each deduped stream skipped to collapsed.
func (w *writer) apply(ctx context.Context, desc staged, streams []stagedStream, collapsed map[string]uint64) error {
	todo, err := w.unapplied(ctx, desc, streams)
	if err != nil {
		return err
	}
	if len(todo) == 0 {
		// Applied, perhaps by a newer generation's Recover, which then stops
		// this one.
		return w.fencedOut(ctx)
	}
	prep := make(map[string]prepared, len(todo))
	for _, s := range todo {
		if prep[s.Name], err = w.prepare(ctx, desc, s); err != nil {
			return err
		}
	}
	if slices.ContainsFunc(todo, func(s stagedStream) bool { return s.Mode == modeOverwrite }) {
		err := w.fenced(ctx, func() error {
			return w.dropAbandonedReads(ctx, func(stream, table string) bool {
				return slices.ContainsFunc(todo, func(s stagedStream) bool {
					return s.Mode == modeOverwrite && digest(16, s.Name) == stream && table != s.Read
				})
			})
		})
		if err != nil {
			return err
		}
	}
	if w.cfg.Scope == destination.ScopeStream {
		for _, s := range todo {
			if err := w.applyTx(ctx, desc, []stagedStream{s}, prep, collapsed); err != nil {
				return err
			}
		}
	} else if err := w.applyTx(ctx, desc, todo, prep, collapsed); err != nil {
		return err
	}
	for _, s := range todo {
		// The read's rows are the target's now.
		if s.Ended && s.Read != "" {
			w.dropOwn(ctx, s.Read)
		}
		for _, t := range prep[s.Name].made {
			w.dropOwn(ctx, t)
		}
	}
	w.dropStaging(ctx, todo)
	return nil
}

// fencedOut fails if a newer generation raised the fence.
func (w *writer) fencedOut(ctx context.Context) error {
	var fence uint64
	err := w.db.QueryRowContext(ctx, "SELECT generation FROM "+w.own(fenceTable)+" WHERE pipeline = :1", w.cfg.Pipeline).Scan(&fence)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err == nil && fence > w.cfg.Generation:
		return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, w.cfg.Generation)
	}
	return classify(err)
}

// fence reads the pipeline's fence in tx, locking its row, and fails if a
// newer generation raised it. Oracle has no shared row lock, so applies of
// one pipeline take turns.
func (w *writer) fence(ctx context.Context, tx *sql.Tx) error {
	var fence uint64
	err := tx.QueryRowContext(ctx, "SELECT generation FROM "+w.own(fenceTable)+" WHERE pipeline = :1 FOR UPDATE", w.cfg.Pipeline).Scan(&fence)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("oracle: the fence row is missing; Recover must run first")
	}
	if err != nil {
		return classify(fmt.Errorf("oracle: lock fence: %w", err))
	}
	if fence > w.cfg.Generation {
		return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, w.cfg.Generation)
	}
	return nil
}

// fenced runs fn, which may run DDL on other connections, while a
// transaction holds the fence row, so that a newer generation's Recover
// waits for it and a fenced writer can't run it.
func (w *writer) fenced(ctx context.Context, fn func() error) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := w.fence(ctx, tx); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	return tx.Commit()
}

func (w *writer) applyTx(ctx context.Context, desc staged, streams []stagedStream, prep map[string]prepared, collapsed map[string]uint64) error {
	skipped := map[string]uint64{}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := w.fence(ctx, tx); err != nil {
		return err
	}
	for _, s := range streams {
		var done int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+w.own(loadsTable)+" WHERE pipeline = :1 AND stream = :2 AND generation = :3 AND epoch = :4",
			w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch).Scan(&done); err != nil {
			return classify(fmt.Errorf("oracle: read marker for %s: %w", s.Name, err))
		}
		if done > 0 {
			continue
		}
		n, err := w.applyStream(ctx, tx, s, prep[s.Name])
		if err != nil {
			return classify(fmt.Errorf("oracle: apply %s: %w", s.Name, err))
		}
		if s.deduped() && s.Rows > n {
			skipped[s.Name] = s.Rows - n
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+w.own(loadsTable)+" (pipeline, stream, generation, epoch) VALUES (:1, :2, :3, :4)",
			w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch); err != nil {
			return classify(fmt.Errorf("oracle: write marker for %s: %w", s.Name, err))
		}
	}
	if err := tx.Commit(); err != nil {
		return classify(err)
	}
	for name, n := range skipped {
		collapsed[name] += n
	}
	return nil
}

// applyStream applies one stream's staging in tx, and returns the rows it
// inserted.
func (w *writer) applyStream(ctx context.Context, tx *sql.Tx, s stagedStream, p prepared) (uint64, error) {
	n, err := w.applyRows(ctx, tx, s, p)
	if err != nil {
		return 0, err
	}
	for _, q := range w.patch(s, p) {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return 0, fmt.Errorf("patch: %w", err)
		}
	}
	return n, nil
}

func (w *writer) applyRows(ctx context.Context, tx *sql.Tx, s stagedStream, p prepared) (uint64, error) {
	if s.Mode == modeMerge {
		if len(s.stagings) == 0 {
			return 0, nil
		}
		for _, q := range w.merge(s, p) {
			if _, err := tx.ExecContext(ctx, q); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	var n uint64
	if len(s.stagings) > 0 {
		res, err := tx.ExecContext(ctx, w.insert(s, p))
		if err != nil {
			return 0, err
		}
		affected, err := res.RowsAffected()
		if err != nil || affected < 0 {
			return 0, fmt.Errorf("oracle: the insert reported %d rows: %w", affected, err)
		}
		n = uint64(affected)
	}
	if s.Ended {
		return n, w.replace(ctx, tx, s, p)
	}
	return n, nil
}

// replace makes the read's rows the target's. Readers see the old rows
// until the transaction commits; apply drops the read's table after it.
func (w *writer) replace(ctx context.Context, tx *sql.Tx, s stagedStream, p prepared) error {
	switch {
	case s.Table == "" && len(p.read) > 0:
		return fmt.Errorf("its read has rows in %s, and no target table", s.Read)
	case s.Table == "" || p.target == nil:
		// The stream never had a schema, so it has no table to empty.
		return nil
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+w.target(s.Table)); err != nil {
		return err
	}
	if len(p.read) == 0 {
		return nil
	}
	sel := make([]string, len(p.read))
	for i, c := range p.read {
		sel[i] = val("r", c, p.into, p.target)
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO "+w.target(s.Table)+" ("+quoteAll(p.read)+") SELECT "+strings.Join(sel, ", ")+" FROM "+w.own(s.Read)+" r")
	return err
}

func quoteAll(names []string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = quoteIdent(n)
	}
	return strings.Join(out, ", ")
}

// prepared is what a stream's apply reads before its transaction.
type prepared struct {
	// The staging's columns; none if the epoch staged nothing for it.
	cols []string
	// The table with every lane's staging, as acrossLanes makes it, and its
	// columns' types.
	rows  string
	types map[string]string
	// The column types of the table the rows go into: the read's table for
	// overwrite, else the target; nil if it doesn't exist.
	into map[string]string
	// Overwrite: the read table's columns once it can take the staging's;
	// none if the read has no table. The target's column types; nil if it
	// doesn't exist.
	read   []string
	target map[string]string
	// Merge: the columns an update in the staging didn't send, which the
	// apply resolves, and every name in the staging's _openetl_unchanged.
	unchanged, everUnchanged []string
	// Every name in the staging's _openetl_held.
	held []string
	// The patches' staging's columns; none if there's nothing to patch.
	patch []string
	// The table with every lane's patches, and its columns' types.
	patches    string
	patchTypes map[string]string
	// Tables acrossLanes made for this apply alone.
	made []string
}

// prepare makes the tables a stream's apply writes hold the staging's
// columns, with the indexes it needs.
func (w *writer) prepare(ctx context.Context, desc staged, s stagedStream) (prepared, error) {
	var p prepared
	var cols []dcol
	if len(s.stagings) > 0 {
		var err error
		if cols, p.rows, err = w.acrossLanes(ctx, desc, s.stagings, s.Name, &p.made); err != nil {
			return p, err
		}
		if len(cols) == 0 {
			// A newer generation's Recover drops what older ones staged.
			if err := w.fencedOut(ctx); err != nil {
				return p, err
			}
			return p, fmt.Errorf("oracle: staging for %s in epoch %d-g%d is missing", s.Name, desc.Epoch, desc.Generation)
		}
		p.cols, p.types = names(cols), typesOf(cols)
	}
	if s.Mode == modeOverwrite {
		if cols != nil {
			if err := w.ensureRead(ctx, s, cols); err != nil {
				return p, err
			}
		}
		have, err := w.columnTypes(ctx, w.user, s.Read)
		if err != nil {
			return p, err
		}
		p.read = names(have)
		if len(have) > 0 {
			p.into = typesOf(have)
		}
		if s.Ended && len(have) > 0 && s.Table != "" {
			if err := w.ensureTable(ctx, s.Name, s.Table, have); err != nil {
				return p, err
			}
		}
		if s.Ended && s.Table != "" {
			if p.target, err = w.targetTypes(ctx, s.Table); err != nil {
				return p, err
			}
		}
	} else if cols != nil {
		if err := w.ensureTable(ctx, s.Name, s.Table, cols); err != nil {
			return p, err
		}
	}
	if s.Mode != modeOverwrite && s.Table != "" {
		var err error
		if p.into, err = w.targetTypes(ctx, s.Table); err != nil {
			return p, err
		}
	}
	if len(s.patches) > 0 {
		var err error
		if p.patch, p.patches, p.patchTypes, err = w.preparePatch(ctx, desc, s, p.into, &p.made); err != nil {
			return p, err
		}
	}
	if s.Dedupe && cols != nil {
		owner, table := w.schema, s.Table
		if s.Mode == modeOverwrite {
			owner, table = w.user, s.Read
		}
		if err := w.ensureIndex(ctx, owner, table, []string{colID}); err != nil {
			return p, fmt.Errorf("oracle: index %s on %s: %w", colID, table, err)
		}
	}
	var err error
	if slices.Contains(p.cols, colHeld) {
		if p.held, err = w.jsonNames(ctx, p.rows, colHeld); err != nil {
			return p, err
		}
	}
	if s.Mode == modeMerge && (cols != nil || p.patch != nil) {
		if err := w.ensureKey(ctx, s.Table, s.Key); err != nil {
			return p, err
		}
		if slices.Contains(p.cols, colUnchanged) {
			if p.everUnchanged, err = w.jsonNames(ctx, p.rows, colUnchanged); err != nil {
				return p, err
			}
			p.unchanged = slices.DeleteFunc(slices.Clone(p.everUnchanged), func(c string) bool { return !slices.Contains(p.cols, c) || slices.Contains(s.Key, c) })
		}
	}
	return p, nil
}

// targetTypes are a target's column types; nil if it doesn't exist.
func (w *writer) targetTypes(ctx context.Context, table string) (map[string]string, error) {
	cols, err := w.columnTypes(ctx, w.schema, table)
	if err != nil || len(cols) == 0 {
		return nil, err
	}
	return typesOf(cols), nil
}

// jsonNames returns the distinct strings in the JSON arrays of rel's col.
func (w *writer) jsonNames(ctx context.Context, rel, col string) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, "SELECT DISTINCT j.c FROM "+rel+" x, JSON_TABLE(x."+quoteIdent(col)+", '$[*]' COLUMNS (c VARCHAR2(512) PATH '$')) j WHERE j.c IS NOT NULL ORDER BY j.c")
	if err != nil {
		return nil, classify(fmt.Errorf("oracle: read %s of the staging: %w", col, err))
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// acrossLanes returns the columns of tables, each staging of one stream on
// a lane, at each column's widest type, and the table that holds their rows:
// the one there is, or a table of this applier's that it fills with every
// one's rows, in order, and adds to made. No columns if one of them doesn't
// exist.
func (w *writer) acrossLanes(ctx context.Context, desc staged, tables []string, of string, made *[]string) ([]dcol, string, error) {
	each := make([][]dcol, len(tables))
	var cols []dcol
	for i, t := range tables {
		var err error
		if each[i], err = w.columnTypes(ctx, w.user, t); err != nil {
			return nil, "", err
		}
		if len(each[i]) == 0 {
			return nil, "", nil
		}
		for _, c := range each[i] {
			j := slices.IndexFunc(cols, func(x dcol) bool { return x.name == c.name })
			switch {
			case j < 0:
				cols = append(cols, c)
			case fits(c.typ, cols[j].typ):
			case fits(cols[j].typ, c.typ):
				cols[j] = c
			default:
				return nil, "", fmt.Errorf("oracle: column %q is %s on one lane, and %s on another", c.name, cols[j].typ, c.typ)
			}
		}
	}
	if len(tables) == 1 {
		return cols, w.own(tables[0]), nil
	}
	// Named for the applier, so that a fenced one's can't be another's.
	all := stageName(w.cfg.Pipeline, w.cfg.Generation, of, "lanes", tables[0], strconv.FormatUint(desc.Epoch, 10))
	if err := w.ddl(ctx, "DROP TABLE "+w.own(all)+" PURGE", 942); err != nil {
		return nil, "", err
	}
	if err := w.createStaging(ctx, all, cols); err != nil {
		return nil, "", fmt.Errorf("oracle: create the staging of every lane: %w", err)
	}
	*made = append(*made, all)
	to := typesOf(cols)
	var off int64
	for i, t := range tables {
		from := typesOf(each[i])
		list := names(each[i])
		sel := make([]string, len(list))
		for j, c := range list {
			sel[j] = val("x", c, from, to)
		}
		res, err := w.db.ExecContext(ctx, "INSERT INTO "+w.own(all)+" ("+quoteIdent(colOrd)+", "+quoteAll(list)+") SELECT ROW_NUMBER() OVER (ORDER BY x."+
			quoteIdent(colOrd)+") + :1, "+strings.Join(sel, ", ")+" FROM "+w.own(t)+" x", off)
		if err != nil {
			return nil, "", classify(fmt.Errorf("oracle: gather the staging of every lane: %w", err))
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, "", err
		}
		off += n
	}
	return cols, w.own(all), nil
}

// preparePatch returns the columns of the stream's patches, none if its
// target doesn't exist, _openetl_synced_at only if the target has it, and
// gives an append target the _openetl_id index patches match on.
func (w *writer) preparePatch(ctx context.Context, desc staged, s stagedStream, target map[string]string, made *[]string) ([]string, string, map[string]string, error) {
	cols, rows, err := w.acrossLanes(ctx, desc, s.patches, s.Name+"\x00patch", made)
	if err != nil {
		return nil, rows, nil, err
	}
	if len(cols) == 0 {
		return nil, rows, nil, fmt.Errorf("oracle: patches for %s in epoch %d-g%d are missing", s.Name, desc.Epoch, desc.Generation)
	}
	if target == nil {
		return nil, rows, nil, nil
	}
	if s.Mode != modeMerge {
		if err := w.ensureIndex(ctx, w.schema, s.Table, []string{colID}); err != nil {
			return nil, rows, nil, fmt.Errorf("oracle: index %s on %s: %w", colID, s.Table, err)
		}
	}
	out := names(cols)
	if _, ok := target[colSynced]; !ok {
		out = slices.DeleteFunc(out, func(c string) bool { return c == colSynced })
	}
	return out, rows, typesOf(cols), nil
}

// ensureRead creates the read's table, in the user's schema, with the
// target's types where it has the column and they fit, or adds its columns to
// hold cols. The rows take the target's types when they replace its rows.
func (w *writer) ensureRead(ctx context.Context, s stagedStream, cols []dcol) error {
	have, err := w.catalog(ctx, w.user, []string{s.Read})
	if err != nil {
		return err
	}
	if have[s.Read] != nil {
		refused, err := w.alter(ctx, w.user, s.Read, cols, have[s.Read], true)
		if err == nil && len(refused) > 0 {
			err = fmt.Errorf("oracle: column %q of the read table for %s: %s", refused[0].Column, s.Name, refused[0].Reason)
		}
		return err
	}
	var target map[string]string
	if s.Table != "" {
		if target, err = w.targetTypes(ctx, s.Table); err != nil {
			return err
		}
	}
	want := slices.Clone(cols)
	for i, c := range want {
		if t, ok := target[c.name]; ok && fits(c.typ, t) {
			want[i].typ = t
		}
	}
	if err := w.ddl(ctx, "CREATE TABLE "+w.own(s.Read)+" ("+strings.Join(columnDefs(want), ", ")+")", 955); err != nil {
		return fmt.Errorf("oracle: create the read table for %s: %w", s.Name, err)
	}
	return nil
}

// ensureIndex gives a table an index on cols, in the user's schema, unless
// it has the one this names.
func (w *writer) ensureIndex(ctx context.Context, owner, table string, cols []string) error {
	name := indexName(owner, table)
	if w.indexed[name] {
		return nil
	}
	var n int
	if err := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM all_indexes WHERE owner = :1 AND index_name = :2", w.user, name).Scan(&n); err != nil {
		return classify(err)
	}
	if n == 0 {
		// 1408: another index has these columns.
		if err := w.ddl(ctx, "CREATE INDEX "+w.own(name)+" ON "+quoteIdent(owner)+"."+quoteIdent(table)+" ("+quoteAll(cols)+")", 955, 1408); err != nil {
			return err
		}
	}
	w.indexed[name] = true
	return nil
}

// ensureKey gives a merge target a unique index on its key, unless it has
// one on those columns whole, so a merge finds a key's row by it and the
// table keeps one row per key.
func (w *writer) ensureKey(ctx context.Context, table string, key []string) error {
	name := keyIndexName(w.schema, table)
	if w.indexed[name] {
		return nil
	}
	rows, err := w.db.QueryContext(ctx, `SELECT i.owner || '.' || i.index_name, c.column_name FROM all_indexes i
		JOIN all_ind_columns c ON c.index_owner = i.owner AND c.index_name = i.index_name
		WHERE i.table_owner = :1 AND i.table_name = :2 AND i.uniqueness = 'UNIQUE' ORDER BY 1, c.column_position`, w.schema, table)
	if err != nil {
		return classify(fmt.Errorf("oracle: read the indexes of %s: %w", table, err))
	}
	indexes := map[string][]string{}
	for rows.Next() {
		var index, col string
		if err := rows.Scan(&index, &col); err != nil {
			_ = rows.Close()
			return err
		}
		indexes[index] = append(indexes[index], col)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	want := slices.Sorted(slices.Values(key))
	for _, cols := range indexes {
		if slices.Equal(slices.Sorted(slices.Values(cols)), want) {
			w.indexed[name] = true
			return nil
		}
	}
	// 1408: a non-unique index has the key's columns, which serves the
	// merge.
	err = w.ddl(ctx, "CREATE UNIQUE INDEX "+w.own(name)+" ON "+w.target(table)+" ("+quoteAll(key)+")", 955, 1408)
	switch oraCode(err) {
	case 0:
		w.indexed[name] = true
		return nil
	case 1452:
		return fmt.Errorf("oracle: merging into %s needs one row per primary key (%s), and it has repeats; dedupe it or write to a new table: %w", table, strings.Join(key, ", "), err)
	}
	return fmt.Errorf("oracle: unique index on the key of %s: %w", table, err)
}

// ApplySchema creates each stream's table and adds its new columns, in one
// statement per table. A column it can't add, or whose type changed, is
// refused, and the rest applied. It reports what each table can't take.
func (w *writer) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	var out destination.SchemaApplied
	tables := make([]string, len(streams))
	want := make([][]dcol, len(streams))
	for i, s := range streams {
		var key []string
		if cfg := w.streams[s.Name]; cfg.Mode == destination.Merge {
			key, _ = keyColumns(s.Schema, cfg.PrimaryKey)
		}
		cols, err := destColumns(s.Schema, key, w.boolean)
		if err != nil {
			return out, err
		}
		tables[i], want[i] = targetTable(s.Schema, s.Name), cols
		if err := validName("table", tables[i]); err != nil {
			return out, err
		}
	}
	have, err := w.catalog(ctx, w.schema, tables)
	if err != nil {
		return out, err
	}
	out.Limits = make(map[string]destination.TableLimits, len(streams))
	for i, s := range streams {
		t := have[tables[i]]
		refused, err := w.alter(ctx, w.schema, tables[i], want[i], t, w.streams[s.Name].AllowTypeChanges)
		if err != nil {
			return destination.SchemaApplied{}, err
		}
		for _, r := range refused {
			r.Stream = s.Name
			out.Refused = append(out.Refused, r)
		}
		if t == nil {
			t = &tableInfo{}
			for _, c := range want[i] {
				t.cols = append(t.cols, tableColumn{dcol: c})
			}
		}
		out.Limits[s.Name] = t.limits()
		w.setTable(s.Name, tables[i])
	}
	return out, nil
}

// ensureTable creates stream's table with cols, or adds its columns to hold
// them. A column already wider is left alone.
func (w *writer) ensureTable(ctx context.Context, stream, table string, cols []dcol) error {
	have, err := w.catalog(ctx, w.schema, []string{table})
	if err != nil {
		return err
	}
	refused, err := w.alter(ctx, w.schema, table, cols, have[table], w.streams[stream].AllowTypeChanges)
	if err != nil {
		return err
	}
	if len(refused) > 0 {
		return fmt.Errorf("oracle: column %q of %s: %s", refused[0].Column, table, refused[0].Reason)
	}
	return nil
}

type tableColumn struct {
	dcol
	notNull bool
}

// tableInfo is a table as the catalog has it.
type tableInfo struct {
	cols []tableColumn
}

func (t *tableInfo) limits() destination.TableLimits {
	l := destination.TableLimits{MaxColumns: maxCols}
	if t == nil {
		return l
	}
	l.Columns = len(t.cols)
	for _, c := range t.cols {
		if !slices.Contains(metaColumns, c.name) && c.notNull {
			l.NotNull = append(l.NotNull, c.name)
		}
	}
	return l
}

// catalog reads the columns of owner's tables, their types and which can't
// take a null. A table that doesn't exist is missing.
func (w *writer) catalog(ctx context.Context, owner string, tables []string) (map[string]*tableInfo, error) {
	out := map[string]*tableInfo{}
	// Oracle takes 1000 expressions in a list.
	for chunk := range slices.Chunk(tables, 500) {
		args := []any{owner}
		binds := make([]string, len(chunk))
		for i, t := range chunk {
			args = append(args, t)
			binds[i] = ":" + strconv.Itoa(i+2)
		}
		rows, err := w.db.QueryContext(ctx, `SELECT table_name, column_name, data_type, data_precision, data_scale, data_length, char_length, char_used, nullable
			FROM all_tab_columns WHERE owner = :1 AND table_name IN (`+strings.Join(binds, ", ")+`) ORDER BY table_name, column_id`, args...)
		if err != nil {
			return nil, classify(fmt.Errorf("oracle: read the columns of %s: %w", strings.Join(chunk, ", "), err))
		}
		for rows.Next() {
			var table, dataType, nullable string
			var precision, scale sql.NullInt64
			var length, charLength int64
			var charUsed sql.NullString
			var c tableColumn
			if err := rows.Scan(&table, &c.name, &dataType, &precision, &scale, &length, &charLength, &charUsed, &nullable); err != nil {
				_ = rows.Close()
				return nil, err
			}
			c.typ = catalogType(dataType, ptr(precision), ptr(scale), length, charLength, charUsed.String)
			c.notNull = nullable == "N"
			t := out[table]
			if t == nil {
				t = &tableInfo{}
				out[table] = t
			}
			t.cols = append(t.cols, c)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return nil, classify(err)
		}
	}
	return out, nil
}

func ptr(n sql.NullInt64) *int64 {
	if !n.Valid {
		return nil
	}
	return &n.Int64
}

// columnTypes returns a table's columns in order, but _openetl_ord, or none
// if it doesn't exist.
func (w *writer) columnTypes(ctx context.Context, owner, table string) ([]dcol, error) {
	have, err := w.catalog(ctx, owner, []string{table})
	if err != nil || have[table] == nil {
		return nil, err
	}
	var cols []dcol
	for _, c := range have[table].cols {
		if c.name != colOrd {
			cols = append(cols, c.dcol)
		}
	}
	return cols, nil
}

// modify is what makes a column of type from take type to: ALTER TABLE
// MODIFY where Oracle changes the type in place, else a new column that
// takes the old one's values and its place, which loses its NOT NULL and
// default.
func modify(table, col, from, to string) string {
	c := quoteIdent(col)
	if alterable(from, to) {
		return "ALTER TABLE " + table + " MODIFY (" + c + " " + to + ")"
	}
	tmp := quoteIdent("_openetl_new")
	return "ALTER TABLE " + table + " ADD (" + tmp + " " + to + "); UPDATE " + table + " SET " + tmp + " = " + convert(c, from, to) +
		"; COMMIT; ALTER TABLE " + table + " DROP COLUMN " + c + "; ALTER TABLE " + table + " RENAME COLUMN " + tmp + " TO " + c + ";"
}

func full(c dcol, why string) destination.RefusedColumn {
	return destination.RefusedColumn{Column: c.name, Reason: "it takes no more columns: " + why, ToType: c.typ}
}

// plan is what alter makes of a table that exists, have, to hold cols: the
// columns to add, those whose type it changes, by their index in have.cols,
// and those it refuses, with why. ref is the table, as a Fix names it.
func plan(ref string, have *tableInfo, cols []dcol, allow bool) ([]dcol, map[int]dcol, []destination.RefusedColumn) {
	var adds []dcol
	var refused []destination.RefusedColumn
	widen := map[int]dcol{}
	n := len(have.cols)
	for _, c := range cols {
		i := slices.IndexFunc(have.cols, func(x tableColumn) bool { return x.name == c.name })
		switch {
		case i < 0 && n >= maxCols:
			refused = append(refused, full(c, fmt.Sprintf("the table has the %d columns Oracle takes", maxCols)))
			continue
		case i < 0:
			adds = append(adds, c)
			n++
			continue
		}
		from := have.cols[i].typ
		switch {
		case from == c.typ || fits(c.typ, from):
		case !fits(from, c.typ):
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: fmt.Sprintf("it can't change a column from %s to %s", from, c.typ), FromType: from, ToType: c.typ,
			})
		case allow && alterable(from, c.typ):
			widen[i] = c
		default:
			reason := "it doesn't change a column's type unless the pipeline's typeChanges allows it"
			if !alterable(from, c.typ) {
				reason = fmt.Sprintf("Oracle can't change a column that has values from %s to %s in place", from, c.typ)
			}
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: reason, FromType: from, ToType: c.typ, Fix: modify(ref, c.name, from, c.typ),
			})
		}
	}
	return adds, widen, refused
}

// alter creates owner's table with cols if it doesn't exist, or adds its new
// columns in one ALTER TABLE, and returns the columns it refused, with why.
// It changes a column's type only if allow, and Oracle can in place. have is
// updated to match.
func (w *writer) alter(ctx context.Context, owner, table string, cols []dcol, have *tableInfo, allow bool) ([]destination.RefusedColumn, error) {
	ref := quoteIdent(owner) + "." + quoteIdent(table)
	if have == nil {
		if owner == w.schema {
			if err := w.ensureSchema(ctx); err != nil {
				return nil, err
			}
		}
		if err := w.ddl(ctx, "CREATE TABLE "+ref+" ("+strings.Join(columnDefs(cols), ", ")+")", 955); err != nil {
			return nil, fmt.Errorf("oracle: create %s: %w", table, err)
		}
		return nil, nil
	}
	adds, widen, refused := plan(ref, have, cols, allow)
	for _, i := range slices.Sorted(maps.Keys(widen)) {
		c := widen[i]
		if err := w.ddl(ctx, "ALTER TABLE "+ref+" MODIFY ("+quoteIdent(c.name)+" "+c.typ+")"); err != nil {
			return nil, fmt.Errorf("oracle: change the type of column %q of %s to %s: %w", c.name, table, c.typ, err)
		}
		have.cols[i].typ = c.typ
	}
	if len(adds) == 0 {
		return refused, nil
	}
	err := w.ddl(ctx, "ALTER TABLE "+ref+" ADD ("+strings.Join(columnDefs(adds), ", ")+")")
	if oraCode(err) == 1792 {
		// Too many columns: add those that fit.
		for _, c := range adds {
			err := w.ddl(ctx, "ALTER TABLE "+ref+" ADD ("+columnDef(c)+")")
			switch {
			case oraCode(err) == 1792:
				refused = append(refused, full(c, err.Error()))
			case err != nil:
				return nil, fmt.Errorf("oracle: add column %q to %s: %w", c.name, table, err)
			default:
				have.cols = append(have.cols, tableColumn{dcol: c})
			}
		}
		return refused, nil
	}
	if err != nil {
		return nil, fmt.Errorf("oracle: add columns to %s: %w", table, err)
	}
	for _, c := range adds {
		have.cols = append(have.cols, tableColumn{dcol: c})
	}
	return refused, nil
}

// How long a schema change waits for its table's lock before it gives up
// and tries again, and how often it tries.
var (
	lockTimeout = 5 * time.Second
	ddlAttempts = 5
)

// ddl runs a DDL statement, on a connection of its own, as DDL commits the
// transaction it's in. An error with one of the codes in ok counts as done,
// as when a table it creates exists.
func (w *writer) ddl(ctx context.Context, stmt string, ok ...int) error {
	backoff := lockTimeout / 5
	var err error
	for range ddlAttempts {
		err = w.ddlOnce(ctx, stmt)
		code := oraCode(err)
		switch {
		case err == nil || slices.Contains(ok, code):
			return nil
		case code != 54 && code != 4021:
			return classify(err)
		}
		t := time.NewTimer(backoff)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
		backoff *= 2
	}
	return transient("oracle: waited for a lock %d times: %w", ddlAttempts, err)
}

func (w *writer) ddlOnce(ctx context.Context, stmt string) error {
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	// It changes how long DDL waits for a lock, and nothing else.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER SESSION SET DDL_LOCK_TIMEOUT = %d", int(lockTimeout.Seconds()))); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, stmt)
	return err
}

// dropOwn drops a table of the user's. Best effort: staging left behind is
// never applied twice, because its marker exists.
func (w *writer) dropOwn(ctx context.Context, table string) {
	_ = w.ddl(ctx, "DROP TABLE "+w.own(table)+" PURGE", 942)
}

func (w *writer) unapplied(ctx context.Context, desc staged, streams []stagedStream) ([]stagedStream, error) {
	var todo []stagedStream
	for _, s := range streams {
		var done int
		if err := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+w.own(loadsTable)+" WHERE pipeline = :1 AND stream = :2 AND generation = :3 AND epoch = :4",
			w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch).Scan(&done); err != nil {
			return nil, classify(fmt.Errorf("oracle: read marker for %s: %w", s.Name, err))
		}
		if done == 0 {
			todo = append(todo, s)
		}
	}
	return todo, nil
}

func (w *writer) dropStaging(ctx context.Context, streams []stagedStream) {
	for _, s := range streams {
		for _, t := range slices.Concat(s.stagings, s.patches) {
			w.dropOwn(ctx, t)
		}
	}
}

// ensureSchema creates the target schema if it's missing: a user that can't
// sign in, with a quota on the database's default tablespace.
func (w *writer) ensureSchema(ctx context.Context) error {
	if w.ready || w.schema == w.user {
		return nil
	}
	var n int
	if err := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM all_users WHERE username = :1", w.schema).Scan(&n); err != nil {
		return classify(fmt.Errorf("oracle: schema %s: %w", w.schema, err))
	}
	if n == 0 {
		var tablespace string
		if err := w.db.QueryRowContext(ctx, "SELECT property_value FROM database_properties WHERE property_name = 'DEFAULT_PERMANENT_TABLESPACE'").Scan(&tablespace); err != nil {
			return classify(fmt.Errorf("oracle: read the default tablespace: %w", err))
		}
		// 1920: it exists now.
		if err := w.ddl(ctx, "CREATE USER "+quoteIdent(w.schema)+" NO AUTHENTICATION QUOTA UNLIMITED ON "+quoteIdent(tablespace), 1920); err != nil {
			return fmt.Errorf("oracle: create schema %s: %w", w.schema, err)
		}
	}
	w.ready = true
	return nil
}

func (w *writer) Recover(ctx context.Context, gen uint64, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	for _, q := range []string{
		"CREATE TABLE " + w.own(fenceTable) + " (pipeline VARCHAR2(512 BYTE) PRIMARY KEY, generation NUMBER(20) NOT NULL)",
		"CREATE TABLE " + w.own(loadsTable) + ` (pipeline VARCHAR2(512 BYTE) NOT NULL, stream VARCHAR2(2000 BYTE) NOT NULL,
			generation NUMBER(20) NOT NULL, epoch NUMBER(20) NOT NULL, loaded_at TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
			PRIMARY KEY (pipeline, stream, generation, epoch))`,
	} {
		if err := w.ddl(ctx, q, 955); err != nil {
			return 0, fmt.Errorf("oracle: prepare the schema %s: %w", w.user, err)
		}
	}
	// Waits for applies that hold the fence row. Two that insert it at once
	// collide, and the second updates it when it tries again.
	raise := "MERGE INTO " + w.own(fenceTable) + ` f USING (SELECT :1 AS pipeline, :2 AS generation FROM dual) s ON (f.pipeline = s.pipeline)
		WHEN MATCHED THEN UPDATE SET f.generation = GREATEST(f.generation, s.generation)
		WHEN NOT MATCHED THEN INSERT (pipeline, generation) VALUES (s.pipeline, s.generation)`
	_, err := w.db.ExecContext(ctx, raise, w.cfg.Pipeline, gen)
	if oraCode(err) == 1 {
		_, err = w.db.ExecContext(ctx, raise, w.cfg.Pipeline, gen)
	}
	if err != nil {
		return 0, classify(fmt.Errorf("oracle: raise fence: %w", err))
	}
	var raised uint64
	if err := w.db.QueryRowContext(ctx, "SELECT generation FROM "+w.own(fenceTable)+" WHERE pipeline = :1", w.cfg.Pipeline).Scan(&raised); err != nil {
		return 0, classify(fmt.Errorf("oracle: raise fence: %w", err))
	}
	if raised != gen {
		return 0, fmt.Errorf("%w: recovering as generation %d, and the fence is %d", destination.ErrFenced, gen, raised)
	}
	if e.IsZero() {
		w.dropRemovedReads(ctx)
		return destination.Applied, nil
	}

	desc, err := w.mergeLanes(committables)
	if err != nil {
		return 0, err
	}
	missing, err := w.unapplied(ctx, desc, desc.Streams)
	if err != nil {
		return 0, err
	}
	// An overwrite read that ended without rows in the epoch needs no
	// staging.
	needs, present := 0, 0
	for _, m := range missing {
		for _, t := range slices.Concat(m.stagings, m.patches) {
			needs++
			var n int
			if err := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2", w.user, t).Scan(&n); err != nil {
				return 0, classify(err)
			}
			present += n
		}
	}
	var r destination.Recovery
	switch {
	case len(missing) == 0:
		w.dropStaging(ctx, desc.Streams)
		r = destination.Applied
	case present == needs:
		if err := w.apply(ctx, desc, missing, map[string]uint64{}); err != nil {
			return 0, err
		}
		r = destination.Redone
	case len(missing) == len(desc.Streams):
		// Nothing of it is visible, and some lane's staging is gone.
		r = destination.Lost
	default:
		return 0, fmt.Errorf("oracle: epoch %d-g%d is partly applied and the staging for the rest is gone", desc.Epoch, desc.Generation)
	}
	if r != destination.Lost {
		w.trimMarkers(ctx, desc.Epoch)
	}
	w.dropStale(ctx, gen)
	w.dropRemovedReads(ctx)
	return r, nil
}

// trimMarkers deletes the pipeline's markers below an applied epoch. Recovery
// checks only HEAD's, and HEAD never moves back past an applied epoch; a lost
// one moves it back to the previous record, whose markers it then checks.
// Best effort, like dropStaging.
func (w *writer) trimMarkers(ctx context.Context, applied uint64) {
	_, _ = w.db.ExecContext(ctx, "DELETE FROM "+w.own(loadsTable)+" WHERE pipeline = :1 AND epoch < :2", w.cfg.Pipeline, applied)
}

// ownTables returns the user's tables whose names start with prefix.
func (w *writer) ownTables(ctx context.Context, prefix string) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, "SELECT table_name FROM user_tables WHERE SUBSTR(table_name, 1, :1) = :2", len(prefix), prefix)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// dropStale drops what earlier generations of the pipeline staged. None of
// it can be applied once HEAD's epoch is recovered: the fence stops their
// commits. Best effort, like dropStaging.
func (w *writer) dropStale(ctx context.Context, gen uint64) {
	prefix := stagePrefix + digest(16, w.cfg.Pipeline) + "_"
	tables, err := w.ownTables(ctx, prefix)
	if err != nil {
		return
	}
	for _, t := range tables {
		g, _, _ := strings.Cut(strings.TrimPrefix(t, prefix), "_")
		if n, err := strconv.ParseUint(g, 10, 64); err == nil && n < gen {
			w.dropOwn(ctx, t)
		}
	}
}

// dropAbandonedReads drops the pipeline's read tables that drop says were
// abandoned, given the digest of their stream's name. Callers hold the
// fence, so a zombie can't drop a read that continues.
func (w *writer) dropAbandonedReads(ctx context.Context, drop func(stream, table string) bool) error {
	prefix := readPrefix + digest(16, w.cfg.Pipeline) + "_"
	tables, err := w.ownTables(ctx, prefix)
	if err != nil {
		return classify(fmt.Errorf("oracle: find abandoned reads: %w", err))
	}
	for _, t := range tables {
		stream, _, _ := strings.Cut(strings.TrimPrefix(t, prefix), "_")
		if drop(stream, t) {
			if err := w.ddl(ctx, "DROP TABLE "+w.own(t)+" PURGE", 942); err != nil {
				return fmt.Errorf("oracle: drop an abandoned read: %w", err)
			}
		}
	}
	return nil
}

// dropRemovedReads drops the read tables of streams that no longer
// overwrite, once Recover has raised the fence. Best effort, like
// dropStaging.
func (w *writer) dropRemovedReads(ctx context.Context) {
	_ = w.dropAbandonedReads(ctx, func(stream, _ string) bool {
		for name, s := range w.streams {
			if digest(16, name) == stream {
				return s.Mode != destination.Overwrite
			}
		}
		return true
	})
}

func (w *writer) Close(context.Context) error {
	return w.db.Close()
}
