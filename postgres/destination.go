// Package postgres has the Postgres source, which reads tables in full or
// their changes through a replication slot, and the Postgres destination:
// append, overwrite and merge writes,
// staged into one table per stream, epoch and writer lane, and applied as one
// behind a fence row with a marker per stream in _openetl_loads. Deduped
// streams insert one row per _openetl_id in the epoch, and with DedupeTarget
// only IDs the target doesn't have yet. Overwrite applies a read's epochs to a
// table of its own, which replaces the target's rows when the read ends. Merge
// upserts each key's latest version, guarded by _openetl_seq.
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type DestinationConfig struct {
	// A connection URL or a keyword/value connection string.
	DSN sdk.Secret `json:"dsn"`
	// Used when the pipeline names no namespace. Default "public".
	Schema string `json:"schema"`
	TLS
}

type Destination struct {
	cfg  DestinationConfig
	pool *pgxpool.Config
}

var (
	_ destination.Destination   = (*Destination)(nil)
	_ destination.SchemaApplier = (*writer)(nil)
	_ destination.ReadTracker   = (*writer)(nil)
	_ destination.Patcher       = (*writer)(nil)
)

func NewDestination(config []byte) (*Destination, error) {
	cfg := DestinationConfig{Schema: "public"}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("postgres: config: %w", err)
	}
	if cfg.DSN.IsZero() {
		return nil, errors.New("postgres: config needs a dsn")
	}
	pc, err := pgxpool.ParseConfig(cfg.DSN.Reveal())
	if err != nil {
		return nil, fmt.Errorf("postgres: dsn: %w", err)
	}
	if err := cfg.TLS.apply(&pc.ConnConfig.Config); err != nil {
		return nil, err
	}
	pc.MaxConns = 4
	// COPY encodes values for the column types a statement description
	// gives. A cached one is stale once a column widens, and would encode the
	// old type without an error.
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	return &Destination{cfg: cfg, pool: pc}, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if s.Mode == destination.Merge && len(s.PrimaryKey) == 0 {
			return nil, fmt.Errorf("postgres: stream %q merges, and has no primary key", s.Name)
		}
	}
	pc := d.pool.Copy()
	if cfg.Lane > 0 {
		// Only lane 0 commits alongside its writes.
		pc.MaxConns = 1
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, classify(fmt.Errorf("postgres: connect: %w", err))
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, classify(fmt.Errorf("postgres: connect: %w", err))
	}
	w := &writer{
		pool:     pool,
		cfg:      cfg,
		schema:   cfg.Namespace,
		Epochs:   sqlstage.NewEpochs[*stagingTable](),
		streams:  map[string]destination.Stream{},
		indexed:  map[string]bool{},
		unlogged: cfg.Scope != destination.ScopeStream,
	}
	if w.schema == "" {
		w.schema = d.cfg.Schema
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	w.Dialect = sqlstage.Dialect[*stagingTable]{Name: "postgres", Open: cfg, Streams: w.streams,
		StagingName: sqlstage.StagingName, ReadName: sqlstage.ReadName, TargetTable: sqlstage.TargetTable, SQL: w}
	return w, nil
}

type writer struct {
	pool    *pgxpool.Pool
	cfg     destination.OpenConfig
	schema  string
	streams map[string]destination.Stream
	sqlstage.Epochs[*stagingTable]
	// Only Commit and Recover use it: tables with an _openetl_id index, or
	// one on their key.
	indexed map[string]bool
	// Stage creates unlogged staging: with pipeline scope, where an epoch is
	// applied whole or not at all, so one whose staging a crash emptied is
	// lost whole and read again.
	unlogged bool
	// The generation whose alive table Stage made sure of; 0 for none.
	alive uint64
}

type stagingTable struct {
	sqlstage.Table
	cols []column
}

type (
	staged       = sqlstage.Staged
	stagedStream = sqlstage.Stream
)

const (
	modeOverwrite = sqlstage.ModeOverwrite
	modeMerge     = sqlstage.ModeMerge
)

func (w *writer) ident(name string) string { return pgx.Identifier{w.schema, name}.Sanitize() }

const (
	idColumn      = "_openetl_id"
	opColumn      = "_openetl_op"
	seqColumn     = "_openetl_seq"
	deletedColumn = "_openetl_deleted"
	// A changelog's: the columns an update didn't send, as a JSON array,
	// whose values are null. In a merge's target, those it never received.
	unchangedColumn = "_openetl_unchanged"
	// A row's held columns, as a JSON array; in the target, a JSON object
	// from each to the _openetl_id of the version whose value is held.
	heldColumn   = destination.HeldColumn
	syncedColumn = "_openetl_synced_at"
	// The engine names renamed columns' source names in each schema's
	// metadata: a JSON object from destination name to source name.
	sourceNamesKey = "openetl.source_names"
)

var metadataColumns = []string{idColumn, opColumn, seqColumn, syncedColumn, deletedColumn, unchangedColumn, heldColumn}

// Stage copies a batch into the stream's staging table for epoch e in
// tables, which it creates, labelled, or widens.
func (w *writer) Stage(ctx context.Context, tables map[uint64]map[string]*stagingTable, name string, e destination.Epoch, stream, target string, key []string, batch arrow.RecordBatch) (*stagingTable, error) {
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
	if st == nil {
		st = &stagingTable{Table: sqlstage.Table{Name: name, Target: target, Key: key}}
		if err := w.createStaging(ctx, st.Name, e, cols); err != nil {
			return nil, fmt.Errorf("postgres: create staging for %s: %w", stream, err)
		}
		st.cols = cols
		byStream[stream] = st
	} else if wider, err := w.addColumns(ctx, st, cols); err != nil {
		return nil, err
	} else if wider != nil {
		st.Earlier = append(st.Earlier, st.Name)
		st.Name = name + "_" + strconv.Itoa(len(st.Earlier)+1)
		if err := w.createStaging(ctx, st.Name, e, wider); err != nil {
			return nil, fmt.Errorf("postgres: create staging for %s: %w", stream, err)
		}
		st.cols = wider
	}

	n := int(batch.NumRows())
	if len(cols) == 0 {
		if _, err := w.pool.Exec(ctx, "INSERT INTO "+w.ident(st.Name)+" SELECT FROM generate_series(1, $1)", n); err != nil {
			return nil, err
		}
		if n > 0 {
			st.Rows += uint64(n)
		}
		return st, nil
	}
	arrs := batch.Columns()
	row := make([]any, len(cols))
	src := pgx.CopyFromSlice(n, func(i int) ([]any, error) {
		for j, c := range cols {
			if arrs[j].IsNull(i) || c.value == nil {
				row[j] = nil
				continue
			}
			v := c.value(arrs[j], i)
			if err, ok := v.(error); ok {
				return nil, fmt.Errorf("postgres: column %q: %w", c.name, err)
			}
			row[j] = v
		}
		return row, nil
	})
	if st.narrower(cols) {
		err = w.copyCast(ctx, st, cols, src)
	} else {
		_, err = w.pool.CopyFrom(ctx, pgx.Identifier{w.schema, st.Name}, names(cols), src)
	}
	if err != nil {
		return nil, stageError(stream, cols, err)
	}
	if n > 0 {
		st.Rows += uint64(n)
	}
	return st, nil
}

// stageError is the error of staging stream's cols. A string or JSON value
// that text and jsonb can't hold, with a NUL or bytes that aren't UTF-8, is
// DATA_INVALID, naming its column, which COPY's context ends with.
func stageError(stream string, cols []column, err error) error {
	pe := (*pgconn.PgError)(nil)
	if !errors.As(err, &pe) || pe.Code != "22021" && pe.Code != "22P05" {
		return fmt.Errorf("postgres: stage %s: %w", stream, err)
	}
	for _, c := range cols {
		if strings.HasSuffix(pe.Where, ", column "+c.name) {
			err = fmt.Errorf("column %q: %w", c.name, err)
			break
		}
	}
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID, fmt.Errorf("postgres: stage %s: %w", stream, err))
}

// createLabelled runs create, which creates the table name if it doesn't
// exist, and labels the table.
func (w *writer) createLabelled(ctx context.Context, create, name string, label any) error {
	b, err := json.Marshal(label)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, create); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "COMMENT ON TABLE "+w.ident(name)+" IS "+quoteLiteral(string(b)))
		return err
	})
}

// createStaging creates a staging table of epoch e, unlogged once its
// generation's alive table is there to say whether a crash emptied it.
func (w *writer) createStaging(ctx context.Context, name string, e destination.Epoch, cols []column) error {
	if w.unlogged {
		if err := w.ensureAlive(ctx, e.Generation); err != nil {
			return err
		}
	}
	label := sqlstage.StagingLabel{Pipeline: w.cfg.Pipeline, Generation: e.Generation, Epoch: e.N}
	return w.createLabelled(ctx, stagingDDL(w.ident(name), cols, w.unlogged), name, label)
}

// stagingDDL creates a staging table. Unlogged, its rows write no WAL, nor
// do the hint bits the apply's read sets, and checkpoints leave its pages.
// Autovacuum never takes it: it lives until its epoch is applied, and a
// worker on it would hold up the DROP that follows.
func stagingDDL(table string, cols []column, unlogged bool) string {
	persistence := ""
	if unlogged {
		persistence = "UNLOGGED "
	}
	return "CREATE " + persistence + "TABLE IF NOT EXISTS " + table + " (" + columnDefs(cols) + ") WITH (autovacuum_enabled = false)"
}

// aliveName names the unlogged table whose one row says that a generation's
// unlogged staging still holds what was staged. A crash, or a failover to a
// standby, empties every unlogged table, it among them, and nothing writes
// its row again.
func aliveName(pipeline string, gen uint64) string {
	return sqlstage.HashName("_openetl_alive_", pipeline, strconv.FormatUint(gen, 10))
}

// ensureAlive creates generation gen's alive table, with its row, before any
// of gen's unlogged staging. One that exists, emptied or not, stays as it is.
func (w *writer) ensureAlive(ctx context.Context, gen uint64) error {
	if w.alive == gen {
		return nil
	}
	name := aliveName(w.cfg.Pipeline, gen)
	label := sqlstage.StagingLabel{Pipeline: w.cfg.Pipeline, Generation: gen}
	err := w.createLabelled(ctx, "CREATE UNLOGGED TABLE IF NOT EXISTS "+w.ident(name)+" AS SELECT true AS alive", name, label)
	// Another lane created it at the same time, and committed.
	if pe, ok := errors.AsType[*pgconn.PgError](err); ok && (pe.Code == "23505" || pe.Code == "42P07") {
		err = nil
	}
	if err != nil {
		return fmt.Errorf("postgres: create %s: %w", name, err)
	}
	w.alive = gen
	return nil
}

// intact reports whether generation gen's unlogged staging holds what was
// staged: its alive table has its row. In an apply's transaction, it holds
// for what the transaction reads, since a crash would end it.
func (w *writer) intact(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, gen uint64) (bool, error) {
	name := w.ident(aliveName(w.cfg.Pipeline, gen))
	var ok bool
	if err := q.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&ok); err != nil || !ok {
		return false, err
	}
	err := q.QueryRow(ctx, "SELECT EXISTS (SELECT FROM "+name+")").Scan(&ok)
	return ok, err
}

// quoteLiteral quotes s as Postgres's quote_literal does, whatever
// standard_conforming_strings is.
func quoteLiteral(s string) string {
	q := "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''") + "'"
	if strings.Contains(s, `\`) {
		q = "E" + q
	}
	return q
}

// narrower reports whether a batch's cols have a column of a narrower type
// than the staging's: it's older, as a lane fed by several can get.
func (st *stagingTable) narrower(cols []column) bool {
	return slices.ContainsFunc(cols, func(c column) bool {
		i := slices.IndexFunc(st.cols, func(x column) bool { return x.name == c.name })
		return c.value != nil && st.cols[i].typ != c.typ
	})
}

// copyCast copies a batch narrower than the staging through a table of its
// own types, and casts its columns as widening the staging would have.
func (w *writer) copyCast(ctx context.Context, st *stagingTable, cols []column, src pgx.CopyFromSource) error {
	tmp := st.Name + "_older"
	sel := make([]string, len(cols))
	for i, c := range cols {
		q := pgx.Identifier{c.name}.Sanitize()
		sel[i] = q
		if j := slices.IndexFunc(st.cols, func(x column) bool { return x.name == c.name }); st.cols[j].typ != c.typ {
			sel[i] = q + "::" + st.cols[j].typ
		}
	}
	// Unlogged: it's dropped in the transaction that creates it.
	return pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "CREATE UNLOGGED TABLE "+w.ident(tmp)+" ("+columnDefs(cols)+")"); err != nil {
			return err
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{w.schema, tmp}, names(cols), src); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO "+w.ident(st.Name)+" ("+quoteAll(names(cols))+") SELECT "+strings.Join(sel, ", ")+" FROM "+w.ident(tmp)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "DROP TABLE "+w.ident(tmp))
		return err
	})
}

// addColumns adds columns that appeared since the staging table was created.
// If a column's type widened, it returns the columns a table must have to take
// the batch, and the table is left as it is. An older batch's narrower columns
// fit.
func (w *writer) addColumns(ctx context.Context, st *stagingTable, cols []column) ([]column, error) {
	wider := slices.Clone(st.cols)
	changed := false
	for _, c := range cols {
		i := slices.IndexFunc(st.cols, func(x column) bool { return x.name == c.name })
		switch {
		case i < 0:
			if _, err := w.pool.Exec(ctx, "ALTER TABLE "+w.ident(st.Name)+" ADD COLUMN IF NOT EXISTS "+columnDef(c)); err != nil {
				return nil, fmt.Errorf("postgres: add column %q to staging: %w", c.name, err)
			}
			st.cols = append(st.cols, c)
			wider = append(wider, c)
		case st.cols[i].typ == c.typ, c.value == nil, widens(c.typ, st.cols[i].typ):
		case widens(st.cols[i].typ, c.typ):
			wider[i], changed = c, true
		default:
			return nil, fmt.Errorf("postgres: column %q changed from %s to %s within an epoch", c.name, st.cols[i].typ, c.typ)
		}
	}
	if !changed {
		return nil, nil
	}
	return wider, nil
}

// columnDef is a column's definition. _openetl_deleted is false in rows from
// before it was added.
func columnDef(c column) string {
	def := pgx.Identifier{c.name}.Sanitize() + " " + c.typ
	if c.name == deletedColumn {
		def += " NOT NULL DEFAULT false"
	}
	return def
}

func columnDefs(cols []column) string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = columnDef(c)
	}
	return strings.Join(defs, ", ")
}

// prepared is what a stream's apply reads before its transaction.
type prepared struct {
	// The staging's columns, if the epoch staged rows for it.
	cols []string
	// The relation that reads every lane's staging, as acrossLanes makes it.
	rows lanesRows
	// Overwrite: the read table's columns once it can take the staging's,
	// if hasRead: the read has a table.
	read    []string
	hasRead bool
	// Merge: the columns an update in the staging didn't send.
	unchanged []string
	// The patches' staging's columns; none if there's nothing to patch.
	patch []string
	// The relation that reads every lane's patches.
	patches lanesRows
}

// lanesRows is a relation over a stream's staging on every lane.
type lanesRows struct {
	rel string
	// It's more than one lane's, so a row's place is its lane and its ctid.
	lanes bool
}

// place is the columns of rel, this relation under an alias, that order its
// rows as they were staged, ctid being what rel calls the row's ctid.
func (r lanesRows) place(rel, ctid string) string {
	if r.lanes {
		return rel + "._openetl_lane, " + rel + "." + ctid
	}
	return rel + "." + ctid
}

// latestFirst orders rel's rows from the last staged.
func (r lanesRows) latestFirst(rel string) string {
	if r.lanes {
		return rel + "._openetl_lane DESC, " + rel + ".ctid DESC"
	}
	return rel + ".ctid DESC"
}

// Apply applies each stream that has no marker yet, then drops its staging.
// It adds the rows each deduped stream skipped to collapsed.
func (w *writer) Apply(ctx context.Context, desc staged, streams []stagedStream, collapsed map[string]uint64) error {
	todo, err := w.Unapplied(ctx, desc, streams)
	if err != nil {
		return err
	}
	if len(todo) == 0 {
		// Applied, maybe by a newer generation's Recover: this one is then
		// fenced, and says so.
		return w.unfenced(ctx, w.pool, "")
	}
	prep := make(map[string]prepared, len(todo))
	for _, s := range todo {
		if prep[s.Name], err = w.prepare(ctx, desc, s); err != nil {
			return err
		}
	}
	unlogged, err := w.unloggedStaging(ctx, todo)
	if err != nil {
		return err
	}
	// Unlogged staging, from an attempt with pipeline scope, is applied in
	// one transaction whatever the scope: a crash between two would leave
	// the epoch partly applied, and the rest's staging emptied.
	if w.cfg.Scope == destination.ScopeStream && !unlogged {
		for _, s := range todo {
			if err := w.applyTx(ctx, desc, []stagedStream{s}, prep, "FOR SHARE", false, collapsed); err != nil {
				return err
			}
		}
	} else if err := w.applyTx(ctx, desc, todo, prep, "FOR UPDATE", unlogged, collapsed); err != nil {
		return err
	}
	w.DropStaging(ctx, todo)
	return nil
}

// unloggedStaging reports whether any of streams' staging is unlogged.
func (w *writer) unloggedStaging(ctx context.Context, streams []stagedStream) (bool, error) {
	var tables []string
	for _, s := range streams {
		for _, t := range slices.Concat(s.Stagings, s.Patches) {
			tables = append(tables, w.ident(t))
		}
	}
	if len(tables) == 0 {
		return false, nil
	}
	var unlogged bool
	err := w.pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM unnest($1::text[]) t(name) JOIN pg_class c ON c.oid = to_regclass(t.name)
		WHERE c.relpersistence = 'u')`, tables).Scan(&unlogged)
	if err != nil {
		return false, fmt.Errorf("postgres: read the staging's persistence: %w", err)
	}
	return unlogged, nil
}

// applyTx applies streams in one transaction. With unlogged staging, it
// applies nothing unless the staging holds what was staged.
func (w *writer) applyTx(ctx context.Context, desc staged, streams []stagedStream, prep map[string]prepared, lock string, unlogged bool, collapsed map[string]uint64) error {
	skipped := map[string]uint64{}
	err := pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		var fence uint64
		err := tx.QueryRow(ctx, "SELECT generation FROM "+w.ident("_openetl_fence")+" WHERE pipeline = $1 "+lock, w.cfg.Pipeline).Scan(&fence)
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("postgres: the fence row is missing; Recover must run first")
		}
		if err != nil {
			return fmt.Errorf("postgres: lock fence: %w", err)
		}
		if fence > w.cfg.Generation {
			return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, w.cfg.Generation)
		}
		if unlogged {
			ok, err := w.intact(ctx, tx, desc.Generation)
			if err != nil {
				return fmt.Errorf("postgres: read the staging's alive table: %w", err)
			}
			if !ok {
				return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf(
					"postgres: the unlogged staging of epoch %d-g%d was emptied after it was staged, as a crash or a failover empties it; the next attempt reads the epoch again",
					desc.Epoch, desc.Generation))
			}
		}
		for _, s := range streams {
			var done bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+w.ident("_openetl_loads")+" WHERE pipeline = $1 AND stream = $2 AND generation = $3 AND epoch = $4)",
				w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch).Scan(&done); err != nil {
				return fmt.Errorf("postgres: read marker for %s: %w", s.Name, err)
			}
			if done {
				continue
			}
			n, err := w.applyStream(ctx, tx, s, prep[s.Name])
			if err != nil {
				return fmt.Errorf("postgres: apply %s: %w", s.Name, err)
			}
			// Dedupe skips append and overwrite rows; a merge's apply counts
			// none of the rows it applies.
			if s.Deduped() && s.Mode != modeMerge && s.Rows > n {
				skipped[s.Name] = s.Rows - n
			}
			if _, err := tx.Exec(ctx, "INSERT INTO "+w.ident("_openetl_loads")+" (pipeline, stream, generation, epoch) VALUES ($1, $2, $3, $4)",
				w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch); err != nil {
				return fmt.Errorf("postgres: write marker for %s: %w", s.Name, err)
			}
		}
		return nil
	})
	if err == nil {
		for name, n := range skipped {
			collapsed[name] += n
		}
	}
	return err
}

// applyStream applies one stream's staging in tx, and returns the rows it
// inserted.
func (w *writer) applyStream(ctx context.Context, tx pgx.Tx, s stagedStream, p prepared) (uint64, error) {
	n, err := w.applyRows(ctx, tx, s, p)
	if err != nil {
		return 0, err
	}
	for _, q := range w.patch(s, p) {
		if _, err := tx.Exec(ctx, q); err != nil {
			return 0, fmt.Errorf("patch: %w", err)
		}
	}
	return n, nil
}

func (w *writer) applyRows(ctx context.Context, tx pgx.Tx, s stagedStream, p prepared) (uint64, error) {
	if s.Mode == modeMerge {
		if len(s.Stagings) == 0 {
			return 0, nil
		}
		for _, q := range w.merge(s, p) {
			if _, err := tx.Exec(ctx, q); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	var n uint64
	if s.Mode == modeOverwrite {
		if err := w.dropAbandonedReads(ctx, tx, func(l sqlstage.ReadLabel, table string) bool { return l.Stream == s.Name && table != s.Read }); err != nil {
			return 0, err
		}
	}
	if len(s.Stagings) > 0 {
		tag, err := tx.Exec(ctx, w.insert(s, p))
		if err != nil {
			return 0, err
		}
		affected := tag.RowsAffected()
		if affected < 0 {
			return 0, fmt.Errorf("postgres: the insert reported %d rows", affected)
		}
		n = uint64(affected)
	}
	if s.Ended {
		return n, w.replace(ctx, tx, s, p)
	}
	return n, nil
}

// insert copies a stream's staging into its target, or its read's table. A
// deduped stream inserts one row per _openetl_id, and with Dedupe only IDs
// the target doesn't have.
func (w *writer) insert(s stagedStream, p prepared) string {
	sel := selectList(p.cols, "s")
	into := "INSERT INTO " + w.ident(s.Into()) + columnList(p.cols)
	id := pgx.Identifier{idColumn}.Sanitize()
	switch {
	case s.Dedupe:
		return into + "SELECT DISTINCT ON (s." + id + ") " + sel + " FROM " + p.rows.rel +
			" s WHERE NOT EXISTS (SELECT 1 FROM " + w.ident(s.Into()) + " t WHERE t." + id + " = s." + id + ")"
	case s.DedupeEpoch:
		return into + "SELECT DISTINCT ON (s." + id + ") " + sel + " FROM " + p.rows.rel + " s"
	default:
		return into + "SELECT " + sel + " FROM " + p.rows.rel + " s"
	}
}

// selectList is cols from rel, with a staged row's list of held columns
// made into the target's object.
func selectList(cols []string, rel string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = rel + "." + pgx.Identifier{c}.Sanitize()
		if c == heldColumn && slices.Contains(cols, idColumn) {
			out[i] = heldObject(rel) + " AS " + pgx.Identifier{c}.Sanitize()
		}
	}
	return strings.Join(out, ", ")
}

// heldObject maps each of rel's held columns to its _openetl_id, the
// version whose value is held; null for none.
func heldObject(rel string) string {
	return "(SELECT jsonb_object_agg(c, " + rel + "." + pgx.Identifier{idColumn}.Sanitize() + "::text) FROM jsonb_array_elements_text(" +
		rel + "." + pgx.Identifier{heldColumn}.Sanitize() + ") c)"
}

// patch fills held cells from the epoch's patches: each column where the
// target still holds the version the patch comes from, matched by key for
// merge and by _openetl_id for append.
func (w *writer) patch(s stagedStream, p prepared) []string {
	cols := p.patch
	if len(cols) == 0 {
		return nil
	}
	q := func(c string) string { return pgx.Identifier{c}.Sanitize() }
	var match []string
	if s.Mode == modeMerge {
		for _, k := range s.Key {
			match = append(match, "t."+q(k)+" = p."+q(k))
		}
	} else {
		match = []string{"t." + q(idColumn) + " = p." + q(idColumn)}
	}
	heldOf := func(c string) string {
		if f, ok := s.Fills[c]; ok {
			return f
		}
		return c
	}
	// The last column that fills each held column, whose UPDATE takes it out
	// of the target's _openetl_held: an UPDATE before it needs it there.
	last := map[string]string{}
	for _, c := range cols {
		if !slices.Contains(metadataColumns, c) && !slices.Contains(s.Key, c) {
			last[heldOf(c)] = c
		}
	}
	var out []string
	for _, c := range cols {
		if slices.Contains(metadataColumns, c) || slices.Contains(s.Key, c) {
			continue
		}
		held := heldOf(c)
		lit := quoteLiteral(held)
		set := q(c) + " = p." + q(c)
		if last[held] == c {
			set += ", " + q(heldColumn) + " = nullif(t." + q(heldColumn) + " - " + lit + ", '{}'::jsonb)"
		}
		if slices.Contains(cols, syncedColumn) {
			set += ", " + q(syncedColumn) + " = p." + q(syncedColumn)
		}
		out = append(out, "UPDATE "+w.ident(s.Table)+" AS t SET "+set+" FROM "+p.patches.rel+" AS p WHERE "+strings.Join(match, " AND ")+
			" AND coalesce(p."+q(heldColumn)+" ? "+lit+", false) AND t."+q(heldColumn)+" ->> "+lit+" = p."+q(idColumn)+"::text")
	}
	return out
}

// merge upserts each key's latest staged version, by _openetl_seq and then
// staging order, unless the target's is newer. A delete keeps the row's
// other columns as they were, with its metadata columns updated, or with
// hard deletes removes the row. A column in unchanged that the latest
// version didn't send takes its value from the key's latest staged version
// that did, unless the target's row is newer than that version, or else
// keeps the target's.
func (w *writer) merge(s stagedStream, p prepared) []string {
	cols, unchanged := p.cols, p.unchanged
	q := func(c string) string { return pgx.Identifier{c}.Sanitize() }
	hasSeq, hasOp := slices.Contains(cols, seqColumn), slices.Contains(cols, opColumn)
	list, key := quoteAll(cols), quoteAll(s.Key)
	order := key
	if hasSeq {
		order += ", " + q(seqColumn) + " DESC NULLS LAST"
	}
	hasHeld := slices.Contains(cols, heldColumn) && slices.Contains(cols, idColumn)
	latest := "WITH s AS (SELECT DISTINCT ON (" + key + ") " + selectList(cols, "x") + " FROM " + p.rows.rel + " x ORDER BY " + order + ", " + p.rows.latestFirst("x") + ") "
	if len(unchanged) > 0 {
		latest = w.latestResolved(s, p, hasSeq)
	}
	sent := func(rel, c string) string {
		return "NOT coalesce(" + rel + "." + q(unchangedColumn) + " ? " + quoteLiteral(c) + ", false)"
	}
	isDelete := func(rel string) string { return rel + "." + q(opColumn) + " = 'delete'" }
	// Unless the target's version is newer; a missing sequence doesn't
	// stop an update.
	newer := func(rel string) string {
		if !hasSeq {
			return "true"
		}
		return "NOT coalesce(t." + q(seqColumn) + " > " + rel + "." + q(seqColumn) + ", false)"
	}
	var sets []string
	for _, c := range cols {
		if slices.Contains(s.Key, c) {
			continue
		}
		v := "EXCLUDED." + q(c)
		switch {
		case c == unchangedColumn:
			// The columns neither ever sent.
			v = "CASE WHEN EXCLUDED." + q(c) + " IS NULL OR t." + q(c) + " IS NULL THEN NULL ELSE (SELECT jsonb_agg(u) FROM jsonb_array_elements_text(EXCLUDED." + q(c) + ") u WHERE t." + q(c) + " ? u) END"
		case c == heldColumn && hasHeld:
			// The row's, and the target's for the columns it didn't send.
			kept := "'{}'::jsonb"
			if slices.Contains(cols, unchangedColumn) {
				kept = "coalesce((SELECT jsonb_object_agg(k, h) FROM jsonb_each(t." + q(c) + ") j(k, h) WHERE coalesce(EXCLUDED." + q(unchangedColumn) + " ? k, false)), '{}'::jsonb)"
			}
			v = "nullif(" + kept + " || coalesce(EXCLUDED." + q(c) + ", '{}'::jsonb), '{}'::jsonb)"
		case slices.Contains(unchanged, c):
			v = "CASE WHEN " + sent("EXCLUDED", c) + " THEN " + v + " ELSE t." + q(c) + " END"
		}
		if hasHeld && slices.Contains(s.Last, c) && !slices.Contains(metadataColumns, c) {
			v = "CASE WHEN coalesce(EXCLUDED." + q(heldColumn) + " ? " + quoteLiteral(c) + ", false) THEN t." + q(c) + " ELSE " + v + " END"
		}
		if hasOp && !s.HardDeletes && (!slices.Contains(metadataColumns, c) || c == unchangedColumn || c == heldColumn) {
			v = "CASE WHEN " + isDelete("EXCLUDED") + " THEN t." + q(c) + " ELSE " + v + " END"
		}
		sets = append(sets, q(c)+" = "+v)
	}
	upsert := latest + "INSERT INTO " + w.ident(s.Table) + " AS t (" + list + ") SELECT " + list + " FROM s"
	if hasOp && s.HardDeletes {
		upsert += " WHERE s." + q(opColumn) + " IS DISTINCT FROM 'delete'"
	}
	if len(sets) == 0 {
		upsert += " ON CONFLICT (" + key + ") DO NOTHING"
	} else {
		upsert += " ON CONFLICT (" + key + ") DO UPDATE SET " + strings.Join(sets, ", ") + " WHERE " + newer("EXCLUDED")
	}
	if !hasOp || !s.HardDeletes {
		return []string{upsert}
	}
	on := make([]string, len(s.Key))
	for i, k := range s.Key {
		on[i] = "t." + q(k) + " = s." + q(k)
	}
	del := latest + "DELETE FROM " + w.ident(s.Table) + " t USING s WHERE " + strings.Join(on, " AND ") + " AND " + isDelete("s") + " AND " + newer("s")
	return []string{upsert, del}
}

// latestResolved is merge's CTE of each key's latest staged version, whose
// unchanged columns come from the key's latest earlier version that sent
// them and isn't older than the target's row, if any; its
// _openetl_unchanged then leaves them out.
func (w *writer) latestResolved(s stagedStream, prep prepared, hasSeq bool) string {
	cols, unchanged := prep.cols, prep.unchanged
	q := func(c string) string { return pgx.Identifier{c}.Sanitize() }
	key := quoteAll(s.Key)
	order, before, latestFirst := key, "("+prep.rows.place("e", "ctid")+") < ("+prep.rows.place("l", "_openetl_ctid")+")", prep.rows.latestFirst("e")
	if hasSeq {
		order += ", " + q(seqColumn) + " DESC NULLS LAST"
		// e was staged before l as order ranks them, where a null seq is
		// below every other: a row comparison with a null in it is unknown.
		es, ls := "e."+q(seqColumn), "l."+q(seqColumn)
		before = "(" + es + " < " + ls + " OR (" + es + " IS NULL AND " + ls + " IS NOT NULL) OR (" + es + " IS NOT DISTINCT FROM " + ls + " AND " + before + "))"
		latestFirst = es + " DESC NULLS LAST, " + latestFirst
	}
	match := make([]string, len(s.Key))
	target := make([]string, len(s.Key))
	for i, k := range s.Key {
		match[i] = "e." + q(k) + " = l." + q(k)
		target[i] = "cur." + q(k) + " = l." + q(k)
	}
	if hasSeq {
		// Nor older than the target's row, whose value is newer; a missing
		// seq doesn't stop it, as in merge.
		before += " AND NOT EXISTS (SELECT 1 FROM " + w.ident(s.Table) + " cur WHERE " + strings.Join(target, " AND ") + " AND cur." + q(seqColumn) + " > e." + q(seqColumn) + ")"
	}
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = "l." + q(c)
	}
	hasHeld := slices.Contains(cols, heldColumn) && slices.Contains(cols, idColumn)
	from, remaining, held := "l", "l."+q(unchangedColumn), "coalesce("+heldObject("l")+", '{}'::jsonb)"
	for i, c := range unchanged {
		p, lit := "p"+strconv.Itoa(i), quoteLiteral(c)
		version := ""
		if hasHeld {
			// The version a column comes from may hold it.
			version = ", coalesce(e." + q(heldColumn) + " ? " + lit + ", false) AS held, e." + q(idColumn) + "::text AS id"
			held += " || CASE WHEN " + p + ".found AND " + p + ".held THEN jsonb_build_object(" + lit + ", " + p + ".id) ELSE '{}'::jsonb END"
		}
		from += " LEFT JOIN LATERAL (SELECT true AS found, e." + q(c) + " AS v" + version + " FROM " + prep.rows.rel + " e" +
			" WHERE l." + q(unchangedColumn) + " ? " + lit + " AND " + strings.Join(match, " AND ") + " AND " + before +
			" AND NOT coalesce(e." + q(unchangedColumn) + " ? " + lit + ", false) ORDER BY " + latestFirst + " LIMIT 1) " + p + " ON true"
		sel[slices.Index(cols, c)] = "CASE WHEN " + p + ".found THEN " + p + ".v ELSE l." + q(c) + " END AS " + q(c)
		remaining = "(" + remaining + " - CASE WHEN " + p + ".found THEN " + lit + " ELSE '' END)"
	}
	sel[slices.Index(cols, unchangedColumn)] = "nullif(" + remaining + ", '[]') AS " + q(unchangedColumn)
	if hasHeld {
		sel[slices.Index(cols, heldColumn)] = "nullif(" + held + ", '{}'::jsonb) AS " + q(heldColumn)
	}
	return "WITH l AS (SELECT DISTINCT ON (" + key + ") *, x.ctid AS _openetl_ctid FROM " + prep.rows.rel + " x ORDER BY " + order + ", " + prep.rows.latestFirst("x") + "), " +
		"s AS (SELECT " + strings.Join(sel, ", ") + " FROM " + from + ") "
}

// replace makes the read's rows the target's, and drops the read's table.
// Readers see the old rows until the transaction commits.
func (w *writer) replace(ctx context.Context, tx pgx.Tx, s stagedStream, p prepared) error {
	switch {
	case s.Table == "" && p.hasRead:
		return fmt.Errorf("its read has rows in %s, and no target table", s.Read)
	case s.Table == "":
		// The stream never had a schema, so it has no table to empty.
		return nil
	}
	var exists bool
	if err := tx.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", w.ident(s.Table)).Scan(&exists); err != nil || !exists {
		return err
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+w.ident(s.Table)); err != nil {
		return err
	}
	if !p.hasRead {
		return nil
	}
	if _, err := tx.Exec(ctx, "INSERT INTO "+w.ident(s.Table)+columnList(p.read)+"SELECT "+quoteAll(p.read)+" FROM "+w.ident(s.Read)); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "DROP TABLE "+w.ident(s.Read))
	return err
}

// columnList is an INSERT's list of cols, spaced; none for no columns, whose
// rows take each column's default.
func columnList(cols []string) string {
	if len(cols) == 0 {
		return " "
	}
	return " (" + quoteAll(cols) + ") "
}

func quoteAll(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = pgx.Identifier{n}.Sanitize()
	}
	return strings.Join(q, ", ")
}

func names(cols []column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.name
	}
	return out
}

// prepare makes the tables a stream's apply writes hold the staging's
// columns, with the indexes it needs.
func (w *writer) prepare(ctx context.Context, desc staged, s stagedStream) (prepared, error) {
	var p prepared
	var cols []column
	hasStaging := len(s.Stagings) > 0
	if hasStaging {
		var ok bool
		var err error
		if cols, p.rows, ok, err = w.acrossLanes(ctx, s.Stagings); err != nil {
			return p, err
		}
		if !ok {
			// A newer generation's Recover drops what older ones staged.
			var fence uint64
			err := w.pool.QueryRow(ctx, "SELECT generation FROM "+w.ident("_openetl_fence")+" WHERE pipeline = $1", w.cfg.Pipeline).Scan(&fence)
			if err == nil && fence > w.cfg.Generation {
				return p, fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, w.cfg.Generation)
			}
			return p, fmt.Errorf("postgres: staging for %s in epoch %d-g%d is missing", s.Name, desc.Epoch, desc.Generation)
		}
		p.cols = names(cols)
	}
	if s.Mode == modeOverwrite {
		if hasStaging {
			if err := w.ensureRead(ctx, s, cols); err != nil {
				return p, err
			}
		}
		have, ok, err := w.tableColumns(ctx, s.Read)
		if err != nil {
			return p, err
		}
		p.read, p.hasRead = names(have), ok
		if s.Ended && ok && s.Table != "" {
			if err := w.ensureTable(ctx, s.Name, s.Table, have); err != nil {
				return p, err
			}
		}
	} else if hasStaging {
		if err := w.ensureTable(ctx, s.Name, s.Table, cols); err != nil {
			return p, err
		}
	}
	if len(s.Patches) > 0 {
		var err error
		if p.patch, p.patches, err = w.preparePatch(ctx, desc, s); err != nil {
			return p, err
		}
	}
	if s.Dedupe && hasStaging {
		if err := w.ensureIndex(ctx, s.Into(), sqlstage.IndexName(s.Into()), false, []string{idColumn}); err != nil {
			return p, fmt.Errorf("postgres: index %s on %s: %w", idColumn, s.Into(), err)
		}
	}
	if s.Mode == modeMerge && (hasStaging || p.patch != nil) {
		if err := w.ensureKey(ctx, s.Table, s.Key); err != nil {
			return p, err
		}
		if slices.Contains(p.cols, unchangedColumn) {
			u := "x." + pgx.Identifier{unchangedColumn}.Sanitize()
			rows, err := w.pool.Query(ctx, "SELECT DISTINCT c FROM "+p.rows.rel+" x, jsonb_array_elements_text("+u+") c WHERE "+u+" IS NOT NULL ORDER BY c")
			if err != nil {
				return p, err
			}
			if p.unchanged, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
				return p, err
			}
			p.unchanged = slices.DeleteFunc(p.unchanged, func(c string) bool { return !slices.Contains(p.cols, c) || slices.Contains(s.Key, c) })
		}
	}
	// The apply joins the staging with the target, and the planner knows
	// nothing of a table just copied into.
	if s.Dedupe || s.Mode == modeMerge {
		for _, t := range s.Stagings {
			if _, err := w.pool.Exec(ctx, "ANALYZE "+w.ident(t)); err != nil {
				return p, fmt.Errorf("postgres: analyze the staging of %s: %w", s.Name, err)
			}
		}
	}
	return p, nil
}

// acrossLanes returns the columns of tables, each staging of one stream on
// a lane, at each column's widest type, and the relation that reads them as
// one with those columns, whatever each lane's schema was. A table's own
// relation if there's one. ok is false if one of them doesn't exist; a
// staging of batches without columns has none.
func (w *writer) acrossLanes(ctx context.Context, tables []string) (_ []column, _ lanesRows, ok bool, _ error) {
	each := make([][]column, len(tables))
	var cols []column
	for i, t := range tables {
		var err error
		if each[i], ok, err = w.tableColumns(ctx, t); err != nil || !ok {
			return nil, lanesRows{}, false, err
		}
		for _, c := range each[i] {
			j := slices.IndexFunc(cols, func(x column) bool { return x.name == c.name })
			switch {
			case j < 0:
				cols = append(cols, c)
			case widens(c.typ, cols[j].typ):
			case widens(cols[j].typ, c.typ):
				cols[j] = c
			default:
				return nil, lanesRows{}, false, fmt.Errorf("postgres: column %q is %s on one lane, and %s on another", c.name, cols[j].typ, c.typ)
			}
		}
	}
	if len(tables) == 1 {
		return cols, lanesRows{rel: w.ident(tables[0])}, true, nil
	}
	selects := make([]string, len(tables))
	for i, t := range tables {
		sel := []string{strconv.Itoa(i) + " AS _openetl_lane", "ctid"}
		for _, c := range cols {
			sel = append(sel, laneColumn(c, each[i]))
		}
		selects[i] = "SELECT " + strings.Join(sel, ", ") + " FROM " + w.ident(t)
	}
	return cols, lanesRows{rel: "(" + strings.Join(selects, " UNION ALL ") + ")", lanes: true}, true, nil
}

// laneColumn selects the stream's column c from a lane's staging of the
// columns have: cast if it's narrower there, or what a row without it holds.
func laneColumn(c column, have []column) string {
	q := pgx.Identifier{c.name}.Sanitize()
	i := slices.IndexFunc(have, func(x column) bool { return x.name == c.name })
	switch {
	case i >= 0 && have[i].typ == c.typ:
		return q
	case i >= 0:
		return q + "::" + c.typ + " AS " + q
	case c.name == deletedColumn:
		return "false AS " + q
	}
	return "NULL::" + c.typ + " AS " + q
}

// preparePatch returns the columns of the stream's patches, none if its
// target doesn't exist or has no columns, which no patch matches a row of;
// _openetl_synced_at only if the target has it. It gives an append target
// the _openetl_id index patches match on.
func (w *writer) preparePatch(ctx context.Context, desc staged, s stagedStream) ([]string, lanesRows, error) {
	cols, rows, ok, err := w.acrossLanes(ctx, s.Patches)
	if err != nil {
		return nil, rows, err
	}
	if !ok {
		return nil, rows, fmt.Errorf("postgres: patches for %s in epoch %d-g%d are missing", s.Name, desc.Epoch, desc.Generation)
	}
	target, err := w.columnTypes(ctx, s.Table)
	if err != nil || len(target) == 0 {
		return nil, rows, err
	}
	if s.Mode != modeMerge {
		if err := w.ensureIndex(ctx, s.Table, sqlstage.IndexName(s.Table), false, []string{idColumn}); err != nil {
			return nil, rows, fmt.Errorf("postgres: index %s on %s: %w", idColumn, s.Table, err)
		}
	}
	for _, t := range s.Patches {
		if _, err := w.pool.Exec(ctx, "ANALYZE "+w.ident(t)); err != nil {
			return nil, rows, fmt.Errorf("postgres: analyze the patches of %s: %w", s.Name, err)
		}
	}
	out := names(cols)
	if !slices.ContainsFunc(target, func(c column) bool { return c.name == syncedColumn }) {
		out = slices.DeleteFunc(out, func(c string) bool { return c == syncedColumn })
	}
	return out, rows, nil
}

// ensureRead creates the read's table, labelled, with the target's types
// where it has the column, or adds its columns to hold cols. A column it has
// narrower than the target's takes the target's type: only a person widens a
// target, after ApplySchema refused to and said how, and the read's table
// replaces the target's rows.
func (w *writer) ensureRead(ctx context.Context, s stagedStream, cols []column) error {
	have, ok, err := w.tableColumns(ctx, s.Read)
	if err != nil {
		return err
	}
	var target []column
	if s.Table != "" {
		if target, err = w.columnTypes(ctx, s.Table); err != nil {
			return err
		}
	}
	targetType := func(c column) column {
		if i := slices.IndexFunc(target, func(x column) bool { return x.name == c.name }); i >= 0 && widens(c.typ, target[i].typ) {
			c.typ = target[i].typ
		}
		return c
	}
	if !ok {
		want := make([]column, len(cols))
		for i, c := range cols {
			want[i] = targetType(c)
		}
		label := sqlstage.ReadLabel{Pipeline: w.cfg.Pipeline, Stream: s.Name, Table: s.Table}
		if err := w.createLabelled(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(s.Read)+" ("+columnDefs(want)+")", s.Read, label); err != nil {
			return fmt.Errorf("postgres: create the read table for %s: %w", s.Name, err)
		}
		return nil
	}
	for _, c := range have {
		t := targetType(c)
		if t.typ == c.typ {
			continue
		}
		q := pgx.Identifier{c.name}.Sanitize()
		if err := w.ddl(ctx, "ALTER TABLE "+w.ident(s.Read)+" ALTER COLUMN "+q+" TYPE "+t.typ+" USING "+q+"::"+t.typ); err != nil {
			return fmt.Errorf("postgres: column %q of the read table for %s, as its target has it: %w", c.name, s.Name, err)
		}
	}
	return w.ensureTable(ctx, s.Name, s.Read, cols)
}

// ensureIndex builds the index without blocking the table's writers, unless
// a valid non-partial index matching cols already exists. A concurrent build
// that was cut off leaves the index invalid, so it drops such an index and
// builds it again.
func (w *writer) ensureIndex(ctx context.Context, table, name string, unique bool, cols []string) error {
	if w.indexed[name] {
		return nil
	}
	var valid bool
	err := w.pool.QueryRow(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)", w.ident(name)).Scan(&valid)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil && !valid {
		if err := w.dropIndex(ctx, name); err != nil {
			return err
		}
	}
	if !unique && len(cols) > 0 {
		var has bool
		err := w.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_index i
			WHERE i.indrelid = to_regclass($1) AND i.indisvalid AND i.indpred IS NULL AND i.indnkeyatts >= cardinality($2::text[])
			AND (SELECT array_agg(a.attname::text ORDER BY k.ord)
				FROM unnest((i.indkey::int2[])[0:cardinality($2::text[]) - 1]) WITH ORDINALITY AS k(attnum, ord)
				JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum AND a.attnum > 0 AND NOT a.attisdropped)
				= $2::text[])`, w.ident(table), cols).Scan(&has)
		if err != nil {
			return err
		}
		if has {
			w.indexed[name] = true
			return nil
		}
	}
	kind := "INDEX"
	if unique {
		kind = "UNIQUE INDEX"
	}
	if _, err := w.pool.Exec(ctx, "CREATE "+kind+" CONCURRENTLY IF NOT EXISTS "+pgx.Identifier{name}.Sanitize()+" ON "+w.ident(table)+" ("+quoteAll(cols)+")"); err != nil {
		// Writes would keep the invalid index up to date until the next
		// build drops it; this build failing on repeats is certainly ours.
		if pe := (*pgconn.PgError)(nil); errors.As(err, &pe) && pe.Code == "23505" {
			_ = w.dropIndex(context.WithoutCancel(ctx), name)
		}
		return err
	}
	w.indexed[name] = true
	return nil
}

func (w *writer) dropIndex(ctx context.Context, name string) error {
	_, err := w.pool.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+w.ident(name))
	return err
}

// ensureKey gives a merge target a unique index on its key, unless it has
// one, for ON CONFLICT.
func (w *writer) ensureKey(ctx context.Context, table string, key []string) error {
	name := sqlstage.KeyIndexName(table)
	if w.indexed[name] {
		return nil
	}
	var has bool
	err := w.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_index i
		WHERE i.indrelid = to_regclass($1) AND i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL
		AND i.indnkeyatts = cardinality($2::text[])
		AND (SELECT array_agg(a.attname::text ORDER BY a.attname) FROM pg_attribute a
			WHERE a.attrelid = i.indrelid AND a.attnum = ANY ((i.indkey::int2[])[0:i.indnkeyatts - 1]))
			= (SELECT array_agg(k ORDER BY k) FROM unnest($2::text[]) k))`, w.ident(table), key).Scan(&has)
	if err != nil {
		return fmt.Errorf("postgres: read the indexes of %s: %w", table, err)
	}
	if has {
		w.indexed[name] = true
		return nil
	}
	err = w.ensureIndex(ctx, table, name, true, key)
	if pe := (*pgconn.PgError)(nil); errors.As(err, &pe) && pe.Code == "23505" {
		return fmt.Errorf("postgres: merging into %s needs one row per primary key (%s), and it has repeats; dedupe it or write to a new table: %w", table, strings.Join(key, ", "), err)
	}
	if err != nil {
		return fmt.Errorf("postgres: unique index on the key of %s: %w", table, err)
	}
	return nil
}

// ApplySchema creates each stream's table and adds its new columns, in one
// statement per table. A column it can't add, or whose type changed, is
// refused, and the rest applied. It reports what each table can't take.
func (w *writer) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	var out destination.SchemaApplied
	if err := w.ensureSchema(ctx); err != nil {
		return out, err
	}
	tables := make([]string, len(streams))
	want := make([][]column, len(streams))
	for i, s := range streams {
		cols, err := columnsOf(s.Schema)
		if err != nil {
			return out, err
		}
		tables[i], want[i] = sqlstage.TargetTable(s.Schema, s.Name), cols
	}
	have, err := w.catalog(ctx, tables)
	if err != nil {
		return out, err
	}
	out.Limits = make(map[string]destination.TableLimits, len(streams))
	for i, s := range streams {
		t := have[tables[i]]
		refused, err := w.alter(ctx, tables[i], want[i], t, w.streams[s.Name].AllowTypeChanges)
		if err != nil {
			return destination.SchemaApplied{}, err
		}
		for _, r := range refused {
			r.Stream = s.Name
			out.Refused = append(out.Refused, r)
		}
		if t == nil {
			t = &tableInfo{attrs: len(want[i])}
		}
		out.Limits[s.Name] = t.limits()
		w.SetTable(s.Name, tables[i])
	}
	return out, nil
}

// ensureTable creates stream's table with cols, or adds its columns to hold
// them. A column already wider is left alone.
func (w *writer) ensureTable(ctx context.Context, stream, table string, cols []column) error {
	have, err := w.catalog(ctx, []string{table})
	if err != nil {
		return err
	}
	refused, err := w.alter(ctx, table, cols, have[table], w.streams[stream].AllowTypeChanges)
	if err != nil {
		return err
	}
	if len(refused) > 0 {
		return fmt.Errorf("postgres: column %q of %s: %s", refused[0].Column, table, refused[0].Reason)
	}
	return nil
}

// Postgres's limit on a table's columns, dropped ones included.
const maxColumns = 1600

// tableInfo is a table as the catalog has it.
type tableInfo struct {
	cols    []column
	notNull map[string]bool
	// Its columns as the limit counts them.
	attrs int
}

func (t *tableInfo) limits() destination.TableLimits {
	l := destination.TableLimits{MaxColumns: maxColumns}
	if t == nil {
		return l
	}
	l.Columns = t.attrs
	for _, c := range t.cols {
		if !slices.Contains(metadataColumns, c.name) && t.notNull[c.name] {
			l.NotNull = append(l.NotNull, c.name)
		}
	}
	return l
}

// catalog reads the tables' columns in one query: their types, and which
// can't take a null. A table that doesn't exist is missing.
func (w *writer) catalog(ctx context.Context, tables []string) (map[string]*tableInfo, error) {
	idents := make([]string, len(tables))
	for i, t := range tables {
		idents[i] = w.ident(t)
	}
	rows, err := w.pool.Query(ctx, `SELECT t.i, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, a.attisdropped
		FROM unnest($1::text[]) WITH ORDINALITY t(name, i) JOIN pg_attribute a ON a.attrelid = to_regclass(t.name)
		WHERE a.attnum > 0 ORDER BY t.i, a.attnum`, idents)
	if err != nil {
		return nil, fmt.Errorf("postgres: read the columns of %s: %w", strings.Join(tables, ", "), err)
	}
	out := map[string]*tableInfo{}
	var i int64
	var c column
	var notNull, dropped bool
	_, err = pgx.ForEachRow(rows, []any{&i, &c.name, &c.typ, &notNull, &dropped}, func() error {
		name := tables[i-1]
		t := out[name]
		if t == nil {
			t = &tableInfo{notNull: map[string]bool{}}
			out[name] = t
		}
		t.attrs++
		if dropped {
			return nil
		}
		t.cols = append(t.cols, c)
		t.notNull[c.name] = notNull
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: read the columns of %s: %w", strings.Join(tables, ", "), err)
	}
	return out, nil
}

// alter creates the table with cols if it doesn't exist, or adds its new
// columns in one ALTER TABLE, and returns the columns it refused, with why.
// It widens a column's type only if the stream allows type changes, since
// that can rewrite the table. have is updated to match.
func (w *writer) alter(ctx context.Context, table string, cols []column, have *tableInfo, allow bool) ([]destination.RefusedColumn, error) {
	if have == nil {
		if err := w.ddl(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(table)+" ("+columnDefs(cols)+")"); err != nil {
			return nil, fmt.Errorf("postgres: create %s: %w", table, err)
		}
		return nil, nil
	}
	var refused []destination.RefusedColumn
	full := func(c column, why string) destination.RefusedColumn {
		return destination.RefusedColumn{Column: c.name, Reason: "it takes no more columns: " + why, ToType: c.typ}
	}
	var clauses []string
	var changed []column
	// Columns to widen, by their index in have.cols.
	widen := map[int]column{}
	attrs := have.attrs
	for _, c := range cols {
		i := slices.IndexFunc(have.cols, func(x column) bool { return x.name == c.name })
		var clause string
		switch {
		case i < 0 && attrs >= maxColumns:
			refused = append(refused, full(c, fmt.Sprintf("the table has the %d columns Postgres takes, dropped ones included", maxColumns)))
			continue
		case i < 0:
			clause = "ADD COLUMN IF NOT EXISTS " + columnDef(c)
			attrs++
		case have.cols[i].typ == c.typ || widens(c.typ, have.cols[i].typ):
			continue
		case !widens(have.cols[i].typ, c.typ):
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: fmt.Sprintf("it can't change a column from %s to %s", have.cols[i].typ, c.typ), FromType: have.cols[i].typ, ToType: c.typ,
			})
			continue
		case allow:
			widen[i] = c
			continue
		default:
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: "it doesn't change a column's type unless the pipeline's typeChanges allows it, as that can rewrite the table",
				FromType: have.cols[i].typ, ToType: c.typ,
				Fix: "ALTER TABLE " + w.ident(table) + " ALTER COLUMN " + pgx.Identifier{c.name}.Sanitize() + " TYPE " + c.typ,
			})
			continue
		}
		clauses, changed = append(clauses, clause), append(changed, c)
	}
	for _, i := range slices.Sorted(maps.Keys(widen)) {
		c := widen[i]
		q := pgx.Identifier{c.name}.Sanitize()
		err := w.ddl(ctx, "ALTER TABLE "+w.ident(table)+" ALTER COLUMN "+q+" TYPE "+c.typ+" USING "+q+"::"+c.typ)
		// Such as a view that depends on the column.
		if pe, ok := errors.AsType[*pgconn.PgError](err); ok && pe.Code == "0A000" {
			refused = append(refused, destination.RefusedColumn{Column: c.name, Reason: "it can't change the column's type: " + pe.Message, FromType: have.cols[i].typ, ToType: c.typ})
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("postgres: change the type of column %q of %s to %s: %w", c.name, table, c.typ, err)
		}
		have.cols[i].typ = c.typ
	}
	if len(clauses) == 0 {
		return refused, nil
	}
	err := w.ddl(ctx, "ALTER TABLE "+w.ident(table)+" "+strings.Join(clauses, ", "))
	if reason, ok := refusal(err); ok && len(clauses) == 1 {
		return append(refused, full(changed[0], reason)), nil
	} else if ok {
		// One of them can't be added; find it, and add the rest.
		for i, clause := range clauses {
			err := w.ddl(ctx, "ALTER TABLE "+w.ident(table)+" "+clause)
			if reason, ok := refusal(err); ok {
				refused = append(refused, full(changed[i], reason))
			} else if err != nil {
				return nil, fmt.Errorf("postgres: add column %q to %s: %w", changed[i].name, table, err)
			} else {
				have.attrs++
				have.cols = append(have.cols, changed[i])
			}
		}
		return refused, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: add columns to %s: %w", table, err)
	}
	have.attrs = attrs
	have.cols = append(have.cols, changed...)
	return refused, nil
}

// refusal is why adding columns failed, if the table can't take them: it
// has too many columns.
func refusal(err error) (string, bool) {
	pe, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pe.Code != "54011" {
		return "", false
	}
	return pe.Message, true
}

// How long a schema change waits for its table's lock before it gives up
// and tries again, so readers don't queue behind it, and how often it tries.
var (
	lockTimeout = 5 * time.Second
	ddlAttempts = 5
)

func (w *writer) ddl(ctx context.Context, stmt string) error {
	backoff := lockTimeout / 5
	var err error
	for range ddlAttempts {
		err = pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = %d", lockTimeout.Milliseconds())); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, stmt)
			return err
		})
		if pe, ok := errors.AsType[*pgconn.PgError](err); !ok || pe.Code != "55P03" {
			return err
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
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("postgres: waited for a lock %d times: %w", ddlAttempts, err))
}

// columnTypes returns a table's columns in order, or none if it doesn't
// exist or has none: tableColumns tells those apart.
func (w *writer) columnTypes(ctx context.Context, table string) ([]column, error) {
	rows, err := w.pool.Query(ctx, `SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute
		WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped ORDER BY attnum`, w.ident(table))
	if err != nil {
		return nil, fmt.Errorf("postgres: read the columns of %s: %w", table, err)
	}
	cols, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (column, error) {
		var c column
		return c, r.Scan(&c.name, &c.typ)
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: read the columns of %s: %w", table, err)
	}
	return cols, nil
}

// tableColumns returns a table's columns in order, and whether it exists.
func (w *writer) tableColumns(ctx context.Context, table string) ([]column, bool, error) {
	cols, err := w.columnTypes(ctx, table)
	if err != nil || len(cols) > 0 {
		return cols, err == nil, err
	}
	ok, err := w.exists(ctx, table)
	return cols, ok, err
}

func (w *writer) Unapplied(ctx context.Context, desc staged, streams []stagedStream) ([]stagedStream, error) {
	var todo []stagedStream
	for _, s := range streams {
		var done bool
		if err := w.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+w.ident("_openetl_loads")+" WHERE pipeline = $1 AND stream = $2 AND generation = $3 AND epoch = $4)",
			w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch).Scan(&done); err != nil {
			return nil, fmt.Errorf("postgres: read marker for %s: %w", s.Name, err)
		}
		if !done {
			todo = append(todo, s)
		}
	}
	return todo, nil
}

// DropStaging is best effort: staging left behind is never applied twice,
// because its marker exists.
func (w *writer) DropStaging(ctx context.Context, streams []stagedStream) {
	for _, s := range streams {
		for _, t := range slices.Concat(s.Stagings, s.Patches) {
			_, _ = w.pool.Exec(ctx, "DROP TABLE IF EXISTS "+w.ident(t))
		}
	}
}

// ensureSchema creates the schema if it's missing. CREATE SCHEMA IF NOT
// EXISTS needs CREATE on the database even when the schema exists, which a
// user with CREATE on only the schema, as on public, lacks.
func (w *writer) ensureSchema(ctx context.Context) error {
	var exists bool
	if err := w.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", w.schema).Scan(&exists); err != nil {
		return classify(fmt.Errorf("postgres: schema %s: %w", w.schema, err))
	}
	if exists {
		return nil
	}
	if _, err := w.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{w.schema}.Sanitize()); err != nil {
		return classify(fmt.Errorf("postgres: create schema %s: %w", w.schema, err))
	}
	return nil
}

// Fence makes the schema and the fence and marker tables, and raises the
// fence to gen.
func (w *writer) Fence(ctx context.Context, gen uint64) error {
	if err := w.ensureSchema(ctx); err != nil {
		return err
	}
	for _, q := range []string{
		"CREATE TABLE IF NOT EXISTS " + w.ident("_openetl_fence") + " (pipeline text PRIMARY KEY, generation bigint NOT NULL)",
		"CREATE TABLE IF NOT EXISTS " + w.ident("_openetl_loads") + ` (pipeline text NOT NULL, stream text NOT NULL,
			generation bigint NOT NULL, epoch bigint NOT NULL, loaded_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (pipeline, stream, generation, epoch))`,
	} {
		if _, err := w.pool.Exec(ctx, q); err != nil {
			return classify(fmt.Errorf("postgres: prepare %s: %w", w.schema, err))
		}
	}
	// Waits for applies that hold the fence row.
	var raised uint64
	err := w.pool.QueryRow(ctx, "INSERT INTO "+w.ident("_openetl_fence")+` AS f (pipeline, generation) VALUES ($1, $2)
		ON CONFLICT (pipeline) DO UPDATE SET generation = EXCLUDED.generation WHERE f.generation <= EXCLUDED.generation
		RETURNING generation`, w.cfg.Pipeline, gen).Scan(&raised)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: recovering as generation %d", destination.ErrFenced, gen)
	}
	if err != nil {
		return fmt.Errorf("postgres: raise fence: %w", err)
	}
	return nil
}

func (w *writer) exists(ctx context.Context, table string) (bool, error) {
	var ok bool
	if err := w.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", w.ident(table)).Scan(&ok); err != nil {
		return false, err
	}
	return ok, nil
}

// Exists is what Recover asks of a staging table it would apply: whether it
// holds what was staged. An unlogged one does only while its generation's
// alive table has its row.
func (w *writer) Exists(ctx context.Context, table string) (bool, error) {
	var persistence string
	var label *string
	err := w.pool.QueryRow(ctx, "SELECT relpersistence::text, obj_description(oid, 'pg_class') FROM pg_class WHERE oid = to_regclass($1)",
		w.ident(table)).Scan(&persistence, &label)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil || persistence != "u" {
		return err == nil, err
	}
	var l sqlstage.StagingLabel
	if label == nil || json.Unmarshal([]byte(*label), &l) != nil {
		return false, nil
	}
	return w.intact(ctx, w.pool, l.Generation)
}

// TrimMarkers deletes the pipeline's markers below an applied epoch. Recovery
// checks only HEAD's, and HEAD never moves back past an applied epoch; a lost
// one moves it back to the previous record, whose markers it then checks.
// Best effort, like dropStaging.
func (w *writer) TrimMarkers(ctx context.Context, applied uint64) {
	_, _ = w.pool.Exec(ctx, "DELETE FROM "+w.ident("_openetl_loads")+" WHERE pipeline = $1 AND epoch < $2", w.cfg.Pipeline, applied)
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// labelled returns the schema's tables whose names start with prefix, with
// their comments.
func (w *writer) labelled(ctx context.Context, q querier, prefix string) ([][2]string, error) {
	rows, err := q.Query(ctx, `SELECT c.relname, d.description FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_description d ON d.objoid = c.oid AND d.classoid = 'pg_class'::regclass AND d.objsubid = 0
		WHERE n.nspname = $1 AND c.relkind = 'r' AND starts_with(c.relname, $2)`, w.schema, prefix)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) ([2]string, error) {
		var t [2]string
		return t, r.Scan(&t[0], &t[1])
	})
}

// DropStale drops what earlier generations of the pipeline staged. None of
// it can be applied once HEAD's epoch is recovered: the fence stops their
// commits. Best effort, like dropStaging.
func (w *writer) DropStale(ctx context.Context, gen uint64) {
	for _, prefix := range []string{"_openetl_stage_", "_openetl_alive_"} {
		labelled, err := w.labelled(ctx, w.pool, prefix)
		if err != nil {
			return
		}
		for _, t := range labelled {
			var l sqlstage.StagingLabel
			if json.Unmarshal([]byte(t[1]), &l) == nil && l.Pipeline == w.cfg.Pipeline && l.Generation < gen {
				_, _ = w.pool.Exec(ctx, "DROP TABLE IF EXISTS "+w.ident(t[0]))
			}
		}
	}
}

// dropAbandonedReads drops the pipeline's read tables that drop says were
// abandoned, in q: an apply's transaction, which holds the fence, so a
// zombie can't drop a read that continues.
func (w *writer) dropAbandonedReads(ctx context.Context, q querier, drop func(l sqlstage.ReadLabel, table string) bool) error {
	labelled, err := w.labelled(ctx, q, "_openetl_read_")
	if err != nil {
		return fmt.Errorf("postgres: find abandoned reads: %w", err)
	}
	for _, t := range labelled {
		var l sqlstage.ReadLabel
		if json.Unmarshal([]byte(t[1]), &l) == nil && l.Pipeline == w.cfg.Pipeline && drop(l, t[0]) {
			if _, err := q.Exec(ctx, "DROP TABLE IF EXISTS "+w.ident(t[0])); err != nil {
				return fmt.Errorf("postgres: drop an abandoned read of %s: %w", l.Stream, err)
			}
		}
	}
	return nil
}

// DropRemovedReads drops the read tables of streams that no longer
// overwrite, once Recover has raised the fence, and while it holds it: a
// newer generation that overwrites such a stream may have raised it since.
// Best effort, like dropStaging.
func (w *writer) DropRemovedReads(ctx context.Context) {
	_ = pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		if err := w.unfenced(ctx, tx, "FOR SHARE"); err != nil {
			return err
		}
		return w.dropAbandonedReads(ctx, tx, func(l sqlstage.ReadLabel, _ string) bool {
			return w.streams[l.Stream].Mode != destination.Overwrite
		})
	})
}

// unfenced fails with destination.ErrFenced if a newer generation has
// raised the fence, which q reads with lock: "" or a lock that holds it.
func (w *writer) unfenced(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, lock string) error {
	var fence uint64
	if err := q.QueryRow(ctx, "SELECT generation FROM "+w.ident("_openetl_fence")+" WHERE pipeline = $1 "+lock, w.cfg.Pipeline).Scan(&fence); err != nil {
		return err
	}
	if fence > w.cfg.Generation {
		return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, w.cfg.Generation)
	}
	return nil
}

func (w *writer) Close(context.Context) error {
	w.pool.Close()
	return nil
}
