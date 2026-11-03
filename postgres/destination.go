// Package postgres has the Postgres source, which reads tables in full or
// their changes through a replication slot, and the Postgres destination:
// append, overwrite and merge writes,
// staged into one table per stream and epoch, and applied behind a fence row
// with a marker per stream in _openetl_loads. Deduped streams insert one row
// per _openetl_id in the epoch, and with DedupeTarget only IDs the target
// doesn't have yet. Overwrite applies a read's epochs to a table of its own,
// which replaces the target's rows when the read ends. Merge upserts each
// key's latest version, guarded by _openetl_seq.
package postgres

import (
	"bytes"
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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

type DestinationConfig struct {
	// A connection URL or a keyword/value connection string.
	DSN sdk.Secret `json:"dsn"`
	// Used when the pipeline names no namespace. Default "public".
	Schema string `json:"schema"`
}

type Destination struct {
	cfg  DestinationConfig
	pool *pgxpool.Config
}

var (
	_ destination.Destination   = (*Destination)(nil)
	_ destination.SchemaApplier = (*writer)(nil)
	_ destination.ReadTracker   = (*writer)(nil)
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
	pool, err := pgxpool.NewWithConfig(ctx, d.pool)
	if err != nil {
		return nil, classify(fmt.Errorf("postgres: connect: %w", err))
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, classify(fmt.Errorf("postgres: connect: %w", err))
	}
	w := &writer{
		pool:    pool,
		cfg:     cfg,
		schema:  cfg.Namespace,
		staging: map[uint64]map[string]*stagingTable{},
		ended:   map[uint64][]string{},
		streams: map[string]destination.Stream{},
		tables:  map[string]string{},
		indexed: map[string]bool{},
	}
	if w.schema == "" {
		w.schema = d.cfg.Schema
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	return w, nil
}

type writer struct {
	pool    *pgxpool.Pool
	cfg     destination.OpenConfig
	schema  string
	streams map[string]destination.Stream
	// Only Write, StartReads, EndRead and PreCommit use these.
	staging map[uint64]map[string]*stagingTable
	starts  map[string]time.Time
	ended   map[uint64][]string // streams whose read ends in each epoch
	// Each stream's target, from its batches and schemas.
	mu     sync.Mutex
	tables map[string]string
	// Only Commit and Recover use it: tables with an _openetl_id index, or
	// one on their key.
	indexed map[string]bool
}

type stagingTable struct {
	name   string
	target string
	cols   []column
	rows   uint64
	// Merge: the key's columns.
	key []string
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
	// Empty when an overwrite read ended in the epoch without rows in it.
	Staging string `json:"staging"`
	// One row per _openetl_id in the epoch, and only IDs the target doesn't
	// have yet.
	Dedupe bool `json:"dedupe,omitempty"`
	// One row per _openetl_id in the epoch.
	DedupeEpoch bool `json:"dedupe_epoch,omitempty"`
	// Staged rows, to count those the apply skips; 0 in descriptors from
	// before it was counted.
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
}

func (s stagedStream) deduped() bool { return s.Dedupe || s.DedupeEpoch }

// into is where the epoch's rows go: the read's table for overwrite.
func (s stagedStream) into() string {
	if s.Mode == modeOverwrite {
		return s.Read
	}
	return s.Table
}

func (w *writer) ident(name string) string { return pgx.Identifier{w.schema, name}.Sanitize() }

const (
	idColumn      = "_openetl_id"
	opColumn      = "_openetl_op"
	seqColumn     = "_openetl_seq"
	deletedColumn = "_openetl_deleted"
	// A changelog's: the columns an update didn't send, as a JSON array,
	// whose values are null. In a merge's target, those it never received.
	unchangedColumn = "_openetl_unchanged"
	// The engine names renamed columns' source names in each schema's
	// metadata: a JSON object from destination name to source name.
	sourceNamesKey = "openetl.source_names"
)

var metadataColumns = []string{idColumn, opColumn, seqColumn, "_openetl_synced_at", deletedColumn}

func indexName(table string) string {
	h := sha256.Sum256([]byte(table))
	return "_openetl_id_" + hex.EncodeToString(h[:12])
}

func keyIndexName(table string) string {
	h := sha256.Sum256([]byte(table))
	return "_openetl_key_" + hex.EncodeToString(h[:12])
}

func stagingName(pipeline, stream string, e destination.Epoch) string {
	h := sha256.Sum256([]byte(pipeline + "\x00" + stream + "\x00" + strconv.FormatUint(e.Generation, 10) + "\x00" + strconv.FormatUint(e.N, 10)))
	return "_openetl_stage_" + hex.EncodeToString(h[:12])
}

func readName(pipeline, stream string, start time.Time) string {
	h := sha256.Sum256([]byte(pipeline + "\x00" + stream + "\x00" + strconv.FormatInt(start.UnixMicro(), 10)))
	return "_openetl_read_" + hex.EncodeToString(h[:12])
}

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
		return fmt.Errorf("postgres: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	if cfg.Dedupe != destination.DedupeNone && len(batch.Schema().FieldIndices(idColumn)) == 0 {
		return fmt.Errorf("postgres: stream %q is deduped at apply, but its batches have no %s", stream, idColumn)
	}
	if _, ok := w.starts[stream]; cfg.Mode == destination.Overwrite && !ok {
		return fmt.Errorf("postgres: stream %q overwrites, but its read has no start", stream)
	}
	cols, err := columnsOf(batch.Schema())
	if err != nil {
		return err
	}
	byStream := w.staging[e.N]
	if byStream == nil {
		byStream = map[string]*stagingTable{}
		w.staging[e.N] = byStream
	}
	st := byStream[stream]
	if st == nil {
		st = &stagingTable{name: stagingName(w.cfg.Pipeline, stream, e), target: targetTable(batch.Schema(), stream)}
		if cfg.Mode == destination.Merge {
			if st.key, err = keyColumns(batch.Schema(), cfg.PrimaryKey); err != nil {
				return fmt.Errorf("postgres: stream %q: %w", stream, err)
			}
		}
		label := stagingLabel{Pipeline: w.cfg.Pipeline, Generation: e.Generation, Epoch: e.N}
		if err := w.createLabelled(ctx, st.name, label, cols); err != nil {
			return fmt.Errorf("postgres: create staging for %s: %w", stream, err)
		}
		st.cols = cols
		byStream[stream] = st
		w.setTable(stream, st.target)
	} else if err := w.widen(ctx, st, cols); err != nil {
		return err
	}

	n := int(batch.NumRows())
	if len(cols) == 0 {
		if _, err := w.pool.Exec(ctx, "INSERT INTO "+w.ident(st.name)+" SELECT FROM generate_series(1, $1)", n); err != nil {
			return err
		}
		st.rows += uint64(n) //nolint:gosec // a batch's rows
		return nil
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
	}
	arrs := batch.Columns()
	row := make([]any, len(cols))
	_, err = w.pool.CopyFrom(ctx, pgx.Identifier{w.schema, st.name}, names, pgx.CopyFromSlice(n, func(i int) ([]any, error) {
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
	}))
	if err != nil {
		return fmt.Errorf("postgres: stage %s: %w", stream, err)
	}
	st.rows += uint64(n) //nolint:gosec // a batch's rows
	return nil
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

// stagingLabel is each staging table's comment, so that Recover can find
// what earlier generations of the pipeline staged.
type stagingLabel struct {
	Pipeline   string `json:"pipeline"`
	Generation uint64 `json:"generation"`
	Epoch      uint64 `json:"epoch"`
}

// readLabel is each read table's comment, so that applies and Recover can
// find reads that were abandoned.
type readLabel struct {
	Pipeline string `json:"pipeline"`
	Stream   string `json:"stream"`
	Table    string `json:"table"`
}

func (w *writer) createLabelled(ctx context.Context, name string, label any, cols []column) error {
	b, err := json.Marshal(label)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(name)+" ("+columnDefs(cols)+")"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "COMMENT ON TABLE "+w.ident(name)+" IS "+quoteLiteral(string(b)))
		return err
	})
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

// widen adds columns that appeared since the staging table was created, and
// widens those whose type widened.
func (w *writer) widen(ctx context.Context, st *stagingTable, cols []column) error {
	for _, c := range cols {
		i := slices.IndexFunc(st.cols, func(x column) bool { return x.name == c.name })
		q := pgx.Identifier{c.name}.Sanitize()
		switch {
		case i < 0:
			if _, err := w.pool.Exec(ctx, "ALTER TABLE "+w.ident(st.name)+" ADD COLUMN IF NOT EXISTS "+columnDef(c)); err != nil {
				return fmt.Errorf("postgres: add column %q to staging: %w", c.name, err)
			}
			st.cols = append(st.cols, c)
		case st.cols[i].typ == c.typ:
		case widens(st.cols[i].typ, c.typ):
			if _, err := w.pool.Exec(ctx, "ALTER TABLE "+w.ident(st.name)+" ALTER COLUMN "+q+" TYPE "+c.typ+" USING "+q+"::"+c.typ); err != nil {
				return fmt.Errorf("postgres: widen column %q in staging: %w", c.name, err)
			}
			st.cols[i] = c
		default:
			return fmt.Errorf("postgres: column %q changed from %s to %s within an epoch", c.name, st.cols[i].typ, c.typ)
		}
	}
	return nil
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

// PreCommit has nothing to flush: every Write committed its rows to staging.
func (w *writer) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	desc := staged{Generation: e.Generation, Epoch: e.N}
	byStream := w.staging[e.N]
	for _, s := range w.cfg.Streams {
		st := byStream[s.Name]
		ended := s.Mode == destination.Overwrite && slices.Contains(w.ended[e.N], s.Name)
		if st == nil && !ended {
			continue
		}
		ss := stagedStream{
			Name: s.Name, Table: w.table(s.Name),
			Dedupe: s.Dedupe == destination.DedupeTarget, DedupeEpoch: s.Dedupe == destination.DedupeEpoch,
		}
		if st != nil {
			ss.Table, ss.Staging, ss.Rows = st.target, st.name, st.rows
		}
		switch s.Mode {
		case destination.Overwrite:
			// A read that ends without rows in the epoch still replaces the
			// target's.
			ss.Mode, ss.Read, ss.Ended = modeOverwrite, readName(w.cfg.Pipeline, s.Name, w.starts[s.Name]), ended
		case destination.Merge:
			ss.Mode, ss.Key, ss.HardDeletes = modeMerge, st.key, s.Deletes == destination.HardDelete
		}
		desc.Streams = append(desc.Streams, ss)
	}
	delete(w.staging, e.N)
	delete(w.ended, e.N)
	return json.Marshal(desc)
}

func (w *writer) Commit(ctx context.Context, _ destination.Epoch, s destination.Staged) (destination.Committed, error) {
	var desc staged
	if err := json.Unmarshal(s, &desc); err != nil {
		return destination.Committed{}, fmt.Errorf("postgres: staged descriptor: %w", err)
	}
	c := destination.Committed{Collapsed: map[string]uint64{}}
	return c, w.apply(ctx, desc, desc.Streams, c.Collapsed)
}

// prepared is what a stream's apply reads before its transaction.
type prepared struct {
	// The staging's columns; none if the epoch staged nothing for it.
	cols []string
	// Overwrite: the read table's columns once it can take the staging's;
	// none if the read has no table.
	read []string
	// Merge: the columns an update in the staging didn't send.
	unchanged []string
}

// apply applies each stream that has no marker yet, then drops its staging.
// It adds the rows each deduped stream skipped to collapsed.
func (w *writer) apply(ctx context.Context, desc staged, streams []stagedStream, collapsed map[string]uint64) error {
	todo, err := w.unapplied(ctx, desc, streams)
	if err != nil || len(todo) == 0 {
		return err
	}
	prep := make(map[string]prepared, len(todo))
	for _, s := range todo {
		if prep[s.Name], err = w.prepare(ctx, desc, s); err != nil {
			return err
		}
	}
	if w.cfg.Scope == destination.ScopeStream {
		for _, s := range todo {
			if err := w.applyTx(ctx, desc, []stagedStream{s}, prep, "FOR SHARE", collapsed); err != nil {
				return err
			}
		}
	} else if err := w.applyTx(ctx, desc, todo, prep, "FOR UPDATE", collapsed); err != nil {
		return err
	}
	w.dropStaging(ctx, todo)
	return nil
}

func (w *writer) applyTx(ctx context.Context, desc staged, streams []stagedStream, prep map[string]prepared, lock string, collapsed map[string]uint64) error {
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
			if s.deduped() && s.Rows > n {
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
	if s.Mode == modeMerge {
		for _, q := range w.merge(s, p.cols, p.unchanged) {
			if _, err := tx.Exec(ctx, q); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	var n uint64
	if s.Mode == modeOverwrite {
		if err := w.dropAbandonedReads(ctx, tx, func(l readLabel, table string) bool { return l.Stream == s.Name && table != s.Read }); err != nil {
			return 0, err
		}
	}
	if s.Staging != "" {
		tag, err := tx.Exec(ctx, w.insert(s, p.cols))
		if err != nil {
			return 0, err
		}
		n = uint64(tag.RowsAffected()) //nolint:gosec // a row count
	}
	if s.Ended {
		return n, w.replace(ctx, tx, s, p.read)
	}
	return n, nil
}

// insert copies a stream's staging into its target, or its read's table. A
// deduped stream inserts one row per _openetl_id, and with Dedupe only IDs
// the target doesn't have.
func (w *writer) insert(s stagedStream, cols []string) string {
	list := quoteAll(cols)
	into := "INSERT INTO " + w.ident(s.into()) + " (" + list + ") "
	id := pgx.Identifier{idColumn}.Sanitize()
	switch {
	case s.Dedupe:
		return into + "SELECT DISTINCT ON (" + id + ") " + list + " FROM " + w.ident(s.Staging) +
			" s WHERE NOT EXISTS (SELECT 1 FROM " + w.ident(s.into()) + " t WHERE t." + id + " = s." + id + ")"
	case s.DedupeEpoch:
		return into + "SELECT DISTINCT ON (" + id + ") " + list + " FROM " + w.ident(s.Staging)
	default:
		return into + "SELECT " + list + " FROM " + w.ident(s.Staging)
	}
}

// merge upserts each key's latest staged version, by _openetl_seq and then
// staging order, unless the target's is newer. A delete keeps the row's
// other columns as they were, with its metadata columns updated, or with
// hard deletes removes the row. A column in unchanged that the latest
// version didn't send takes its value from the key's latest staged version
// that did, or else keeps the target's.
func (w *writer) merge(s stagedStream, cols, unchanged []string) []string {
	q := func(c string) string { return pgx.Identifier{c}.Sanitize() }
	hasSeq, hasOp := slices.Contains(cols, seqColumn), slices.Contains(cols, opColumn)
	list, key := quoteAll(cols), quoteAll(s.Key)
	order := key
	if hasSeq {
		order += ", " + q(seqColumn) + " DESC NULLS LAST"
	}
	latest := "WITH s AS (SELECT DISTINCT ON (" + key + ") " + list + " FROM " + w.ident(s.Staging) + " ORDER BY " + order + ", ctid DESC) "
	if len(unchanged) > 0 {
		latest = w.latestResolved(s, cols, unchanged, hasSeq)
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
		case slices.Contains(unchanged, c):
			v = "CASE WHEN " + sent("EXCLUDED", c) + " THEN " + v + " ELSE t." + q(c) + " END"
		}
		if hasOp && !s.HardDeletes && (!slices.Contains(metadataColumns, c) || c == unchangedColumn) {
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
// them, if any; its _openetl_unchanged then leaves them out.
func (w *writer) latestResolved(s stagedStream, cols, unchanged []string, hasSeq bool) string {
	q := func(c string) string { return pgx.Identifier{c}.Sanitize() }
	key := quoteAll(s.Key)
	order, before, latestFirst := key, "e.ctid < l._openetl_ctid", "e.ctid DESC"
	if hasSeq {
		order += ", " + q(seqColumn) + " DESC NULLS LAST"
		before = "(e." + q(seqColumn) + ", e.ctid) < (l." + q(seqColumn) + ", l._openetl_ctid)"
		latestFirst = "e." + q(seqColumn) + " DESC, " + latestFirst
	}
	match := make([]string, len(s.Key))
	for i, k := range s.Key {
		match[i] = "e." + q(k) + " = l." + q(k)
	}
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = "l." + q(c)
	}
	from, remaining := "l", "l."+q(unchangedColumn)
	for i, c := range unchanged {
		p, lit := "p"+strconv.Itoa(i), quoteLiteral(c)
		from += " LEFT JOIN LATERAL (SELECT true AS found, e." + q(c) + " AS v FROM " + w.ident(s.Staging) + " e" +
			" WHERE l." + q(unchangedColumn) + " ? " + lit + " AND " + strings.Join(match, " AND ") + " AND " + before +
			" AND NOT coalesce(e." + q(unchangedColumn) + " ? " + lit + ", false) ORDER BY " + latestFirst + " LIMIT 1) " + p + " ON true"
		sel[slices.Index(cols, c)] = "CASE WHEN " + p + ".found THEN " + p + ".v ELSE l." + q(c) + " END AS " + q(c)
		remaining = "(" + remaining + " - CASE WHEN " + p + ".found THEN " + lit + " ELSE '' END)"
	}
	sel[slices.Index(cols, unchangedColumn)] = "nullif(" + remaining + ", '[]') AS " + q(unchangedColumn)
	return "WITH l AS (SELECT DISTINCT ON (" + key + ") *, ctid AS _openetl_ctid FROM " + w.ident(s.Staging) + " ORDER BY " + order + ", ctid DESC), " +
		"s AS (SELECT " + strings.Join(sel, ", ") + " FROM " + from + ") "
}

// replace makes the read's rows the target's, and drops the read's table.
// Readers see the old rows until the transaction commits.
func (w *writer) replace(ctx context.Context, tx pgx.Tx, s stagedStream, read []string) error {
	switch {
	case s.Table == "" && len(read) > 0:
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
	if len(read) == 0 {
		return nil
	}
	list := quoteAll(read)
	if _, err := tx.Exec(ctx, "INSERT INTO "+w.ident(s.Table)+" ("+list+") SELECT "+list+" FROM "+w.ident(s.Read)); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "DROP TABLE "+w.ident(s.Read))
	return err
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
	if s.Staging != "" {
		var err error
		if cols, err = w.columnTypes(ctx, s.Staging); err != nil {
			return p, err
		}
		if len(cols) == 0 {
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
		if cols != nil {
			if err := w.ensureRead(ctx, s, cols); err != nil {
				return p, err
			}
		}
		have, err := w.columnTypes(ctx, s.Read)
		if err != nil {
			return p, err
		}
		p.read = names(have)
		if s.Ended && len(have) > 0 && s.Table != "" {
			if err := w.ensureTable(ctx, s.Table, have); err != nil {
				return p, err
			}
		}
	} else if err := w.ensureTable(ctx, s.Table, cols); err != nil {
		return p, err
	}
	if s.Dedupe && cols != nil {
		if err := w.ensureIndex(ctx, s.into(), indexName(s.into()), false, []string{idColumn}); err != nil {
			return p, fmt.Errorf("postgres: index %s on %s: %w", idColumn, s.into(), err)
		}
	}
	if s.Mode == modeMerge {
		if err := w.ensureKey(ctx, s.Table, s.Key); err != nil {
			return p, err
		}
		if slices.Contains(p.cols, unchangedColumn) {
			u := pgx.Identifier{unchangedColumn}.Sanitize()
			rows, err := w.pool.Query(ctx, "SELECT DISTINCT c FROM "+w.ident(s.Staging)+", jsonb_array_elements_text("+u+") c WHERE "+u+" IS NOT NULL ORDER BY c")
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
	if s.Staging != "" && (s.Dedupe || s.Mode == modeMerge) {
		if _, err := w.pool.Exec(ctx, "ANALYZE "+w.ident(s.Staging)); err != nil {
			return p, fmt.Errorf("postgres: analyze the staging of %s: %w", s.Name, err)
		}
	}
	return p, nil
}

// ensureRead creates the read's table, labelled, or adds and widens its
// columns to hold cols.
func (w *writer) ensureRead(ctx context.Context, s stagedStream, cols []column) error {
	have, err := w.columnTypes(ctx, s.Read)
	if err != nil {
		return err
	}
	if len(have) == 0 {
		label := readLabel{Pipeline: w.cfg.Pipeline, Stream: s.Name, Table: s.Table}
		if err := w.createLabelled(ctx, s.Read, label, cols); err != nil {
			return fmt.Errorf("postgres: create the read table for %s: %w", s.Name, err)
		}
	}
	return w.ensureTable(ctx, s.Read, cols)
}

// ensureIndex builds the index without blocking the table's writers. A
// concurrent build that was cut off leaves the index invalid, so it drops
// such an index and builds it again.
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
	name := keyIndexName(table)
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

// ApplySchema creates each stream's table, adds its new columns and widens
// the columns whose type widened.
func (w *writer) ApplySchema(ctx context.Context, streams []destination.StreamSchema) error {
	if _, err := w.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{w.schema}.Sanitize()); err != nil {
		return fmt.Errorf("postgres: create schema %s: %w", w.schema, err)
	}
	for _, s := range streams {
		cols, err := columnsOf(s.Schema)
		if err != nil {
			return err
		}
		table := targetTable(s.Schema, s.Name)
		if err := w.ensureTable(ctx, table, cols); err != nil {
			return err
		}
		w.setTable(s.Name, table)
	}
	return nil
}

// ensureTable creates the table with cols, or adds and widens its columns
// to hold them. A column already wider is left alone.
func (w *writer) ensureTable(ctx context.Context, table string, cols []column) error {
	have, err := w.columnTypes(ctx, table)
	if err != nil {
		return err
	}
	if len(have) == 0 {
		if _, err := w.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(table)+" ("+columnDefs(cols)+")"); err != nil {
			return fmt.Errorf("postgres: create %s: %w", table, err)
		}
		if have, err = w.columnTypes(ctx, table); err != nil {
			return err
		}
	}
	for _, c := range cols {
		i := slices.IndexFunc(have, func(x column) bool { return x.name == c.name })
		q := pgx.Identifier{c.name}.Sanitize()
		var ddl string
		switch {
		case i < 0:
			ddl = "ADD COLUMN IF NOT EXISTS " + columnDef(c)
		case have[i].typ == c.typ || widens(c.typ, have[i].typ):
			continue
		case widens(have[i].typ, c.typ):
			ddl = "ALTER COLUMN " + q + " TYPE " + c.typ + " USING " + q + "::" + c.typ
		default:
			return fmt.Errorf("postgres: column %q of %s is %s, and can't widen to %s", c.name, table, have[i].typ, c.typ)
		}
		if _, err := w.pool.Exec(ctx, "ALTER TABLE "+w.ident(table)+" "+ddl); err != nil {
			return fmt.Errorf("postgres: change column %q of %s: %w", c.name, table, err)
		}
	}
	return nil
}

// columnTypes returns a table's columns in order, or none if it doesn't
// exist.
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

func (w *writer) unapplied(ctx context.Context, desc staged, streams []stagedStream) ([]stagedStream, error) {
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

// dropStaging is best effort: staging left behind is never applied twice,
// because its marker exists.
func (w *writer) dropStaging(ctx context.Context, streams []stagedStream) {
	for _, s := range streams {
		if s.Staging != "" {
			_, _ = w.pool.Exec(ctx, "DROP TABLE IF EXISTS "+w.ident(s.Staging))
		}
	}
}

func (w *writer) Recover(ctx context.Context, gen uint64, e destination.Epoch, s destination.Staged) (destination.Recovery, error) {
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + pgx.Identifier{w.schema}.Sanitize(),
		"CREATE TABLE IF NOT EXISTS " + w.ident("_openetl_fence") + " (pipeline text PRIMARY KEY, generation bigint NOT NULL)",
		"CREATE TABLE IF NOT EXISTS " + w.ident("_openetl_loads") + ` (pipeline text NOT NULL, stream text NOT NULL,
			generation bigint NOT NULL, epoch bigint NOT NULL, loaded_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (pipeline, stream, generation, epoch))`,
	} {
		if _, err := w.pool.Exec(ctx, q); err != nil {
			return 0, fmt.Errorf("postgres: prepare %s: %w", w.schema, err)
		}
	}
	// Waits for applies that hold the fence row.
	var raised uint64
	err := w.pool.QueryRow(ctx, "INSERT INTO "+w.ident("_openetl_fence")+` AS f (pipeline, generation) VALUES ($1, $2)
		ON CONFLICT (pipeline) DO UPDATE SET generation = EXCLUDED.generation WHERE f.generation <= EXCLUDED.generation
		RETURNING generation`, w.cfg.Pipeline, gen).Scan(&raised)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("%w: recovering as generation %d", destination.ErrFenced, gen)
	}
	if err != nil {
		return 0, fmt.Errorf("postgres: raise fence: %w", err)
	}
	if e.IsZero() {
		w.dropRemovedReads(ctx)
		return destination.Applied, nil
	}

	var desc staged
	if err := json.Unmarshal(s, &desc); err != nil {
		return 0, fmt.Errorf("postgres: staged descriptor: %w", err)
	}
	missing, err := w.unapplied(ctx, desc, desc.Streams)
	if err != nil {
		return 0, err
	}
	// An overwrite read that ended without rows in the epoch needs no
	// staging.
	needs, present := 0, 0
	for _, m := range missing {
		if m.Staging == "" {
			continue
		}
		needs++
		var ok bool
		if err := w.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", w.ident(m.Staging)).Scan(&ok); err != nil {
			return 0, err
		}
		if ok {
			present++
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
	case present == 0 && len(missing) == len(desc.Streams):
		r = destination.Lost
	default:
		return 0, fmt.Errorf("postgres: epoch %d-g%d is partly applied and the staging for the rest is gone", desc.Epoch, desc.Generation)
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

// dropStale drops what earlier generations of the pipeline staged. None of
// it can be applied once HEAD's epoch is recovered: the fence stops their
// commits. Best effort, like dropStaging.
func (w *writer) dropStale(ctx context.Context, gen uint64) {
	labelled, err := w.labelled(ctx, w.pool, "_openetl_stage_")
	if err != nil {
		return
	}
	for _, t := range labelled {
		var l stagingLabel
		if json.Unmarshal([]byte(t[1]), &l) == nil && l.Pipeline == w.cfg.Pipeline && l.Generation < gen {
			_, _ = w.pool.Exec(ctx, "DROP TABLE IF EXISTS "+w.ident(t[0]))
		}
	}
}

// dropAbandonedReads drops the pipeline's read tables that drop says were
// abandoned, in q: an apply's transaction, which holds the fence, so a
// zombie can't drop a read that continues.
func (w *writer) dropAbandonedReads(ctx context.Context, q querier, drop func(l readLabel, table string) bool) error {
	labelled, err := w.labelled(ctx, q, "_openetl_read_")
	if err != nil {
		return fmt.Errorf("postgres: find abandoned reads: %w", err)
	}
	for _, t := range labelled {
		var l readLabel
		if json.Unmarshal([]byte(t[1]), &l) == nil && l.Pipeline == w.cfg.Pipeline && drop(l, t[0]) {
			if _, err := q.Exec(ctx, "DROP TABLE IF EXISTS "+w.ident(t[0])); err != nil {
				return fmt.Errorf("postgres: drop an abandoned read of %s: %w", l.Stream, err)
			}
		}
	}
	return nil
}

// dropRemovedReads drops the read tables of streams that no longer
// overwrite, once Recover has raised the fence. Best effort, like
// dropStaging.
func (w *writer) dropRemovedReads(ctx context.Context) {
	_ = w.dropAbandonedReads(ctx, w.pool, func(l readLabel, _ string) bool {
		return w.streams[l.Stream].Mode != destination.Overwrite
	})
}

func (w *writer) Close(context.Context) error {
	w.pool.Close()
	return nil
}
