// Package postgres has the Postgres source, which reads full snapshots of
// tables, and the Postgres destination: append writes, staged into one table
// per stream and epoch, and applied behind a fence row with a marker per
// stream in _openetl_loads. Deduped streams insert one row per _openetl_id,
// and only IDs the target doesn't have yet.
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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
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
		if s.Mode != destination.Append {
			return nil, fmt.Errorf("postgres: stream %q: only append writes are supported", s.Name)
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, d.pool)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	w := &writer{
		pool:    pool,
		cfg:     cfg,
		schema:  cfg.Namespace,
		staging: map[uint64]map[string]*stagingTable{},
		streams: map[string]destination.Stream{},
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
	// Only Write and PreCommit use it.
	staging map[uint64]map[string]*stagingTable
	// Only Commit and Recover use it: targets with an _openetl_id index.
	indexed map[string]bool
}

type stagingTable struct {
	name   string
	target string
	cols   []column
}

// staged is the descriptor PreCommit returns.
type staged struct {
	Generation uint64         `json:"generation"`
	Epoch      uint64         `json:"epoch"`
	Streams    []stagedStream `json:"streams"`
}

type stagedStream struct {
	Name    string `json:"name"`
	Table   string `json:"table"`
	Staging string `json:"staging"`
	Dedupe  bool   `json:"dedupe,omitempty"`
}

func (w *writer) ident(name string) string { return pgx.Identifier{w.schema, name}.Sanitize() }

const idColumn = "_openetl_id"

func indexName(table string) string {
	h := sha256.Sum256([]byte(table))
	return "_openetl_id_" + hex.EncodeToString(h[:12])
}

func stagingName(pipeline, stream string, e destination.Epoch) string {
	h := sha256.Sum256([]byte(pipeline + "\x00" + stream + "\x00" + strconv.FormatUint(e.Generation, 10) + "\x00" + strconv.FormatUint(e.N, 10)))
	return "_openetl_stage_" + hex.EncodeToString(h[:12])
}

func (w *writer) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	cfg, ok := w.streams[stream]
	if !ok {
		return fmt.Errorf("postgres: unknown stream %q", stream)
	}
	if batch.NumRows() == 0 {
		return nil
	}
	if cfg.Dedupe && len(batch.Schema().FieldIndices(idColumn)) == 0 {
		return fmt.Errorf("postgres: stream %q is deduped at apply, but its batches have no %s", stream, idColumn)
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
		if err := w.createStaging(ctx, st.name, e, cols); err != nil {
			return fmt.Errorf("postgres: create staging for %s: %w", stream, err)
		}
		st.cols = cols
		byStream[stream] = st
	} else if err := w.widen(ctx, st, cols); err != nil {
		return err
	}

	n := int(batch.NumRows())
	if len(cols) == 0 {
		_, err := w.pool.Exec(ctx, "INSERT INTO "+w.ident(st.name)+" SELECT FROM generate_series(1, $1)", n)
		return err
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
	return nil
}

// stagingLabel is each staging table's comment, so that Recover can find
// what earlier generations of the pipeline staged.
type stagingLabel struct {
	Pipeline   string `json:"pipeline"`
	Generation uint64 `json:"generation"`
	Epoch      uint64 `json:"epoch"`
}

func (w *writer) createStaging(ctx context.Context, name string, e destination.Epoch, cols []column) error {
	label, err := json.Marshal(stagingLabel{Pipeline: w.cfg.Pipeline, Generation: e.Generation, Epoch: e.N})
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(name)+" ("+columnDefs(cols)+")"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "COMMENT ON TABLE "+w.ident(name)+" IS "+quoteLiteral(string(label)))
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
			if _, err := w.pool.Exec(ctx, "ALTER TABLE "+w.ident(st.name)+" ADD COLUMN IF NOT EXISTS "+q+" "+c.typ); err != nil {
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

func columnDefs(cols []column) string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = pgx.Identifier{c.name}.Sanitize() + " " + c.typ
	}
	return strings.Join(defs, ", ")
}

// PreCommit has nothing to flush: every Write committed its rows to staging.
func (w *writer) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	desc := staged{Generation: e.Generation, Epoch: e.N}
	byStream := w.staging[e.N]
	for _, s := range w.cfg.Streams {
		if st := byStream[s.Name]; st != nil {
			desc.Streams = append(desc.Streams, stagedStream{Name: s.Name, Table: st.target, Staging: st.name, Dedupe: s.Dedupe})
		}
	}
	delete(w.staging, e.N)
	return json.Marshal(desc)
}

func (w *writer) Commit(ctx context.Context, _ destination.Epoch, s destination.Staged) error {
	var desc staged
	if err := json.Unmarshal(s, &desc); err != nil {
		return fmt.Errorf("postgres: staged descriptor: %w", err)
	}
	return w.apply(ctx, desc, desc.Streams)
}

// apply applies each stream that has no marker yet, then drops its staging.
func (w *writer) apply(ctx context.Context, desc staged, streams []stagedStream) error {
	todo, err := w.unapplied(ctx, desc, streams)
	if err != nil || len(todo) == 0 {
		return err
	}
	cols := make(map[string][]string, len(todo))
	for _, s := range todo {
		if cols[s.Name], err = w.ensureTarget(ctx, desc, s); err != nil {
			return err
		}
	}
	if w.cfg.Scope == destination.ScopeStream {
		for _, s := range todo {
			if err := w.applyTx(ctx, desc, []stagedStream{s}, cols, "FOR SHARE"); err != nil {
				return err
			}
		}
	} else if err := w.applyTx(ctx, desc, todo, cols, "FOR UPDATE"); err != nil {
		return err
	}
	w.dropStaging(ctx, todo)
	return nil
}

func (w *writer) applyTx(ctx context.Context, desc staged, streams []stagedStream, cols map[string][]string, lock string) error {
	return pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
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
			if _, err := tx.Exec(ctx, w.insert(s, cols[s.Name])); err != nil {
				return fmt.Errorf("postgres: apply %s: %w", s.Name, err)
			}
			if _, err := tx.Exec(ctx, "INSERT INTO "+w.ident("_openetl_loads")+" (pipeline, stream, generation, epoch) VALUES ($1, $2, $3, $4)",
				w.cfg.Pipeline, s.Name, desc.Generation, desc.Epoch); err != nil {
				return fmt.Errorf("postgres: write marker for %s: %w", s.Name, err)
			}
		}
		return nil
	})
}

// insert copies a stream's staging into its target. A deduped stream inserts
// one row per _openetl_id, and only IDs the target doesn't have.
func (w *writer) insert(s stagedStream, cols []string) string {
	list := quoteAll(cols)
	if !s.Dedupe {
		return "INSERT INTO " + w.ident(s.Table) + " (" + list + ") SELECT " + list + " FROM " + w.ident(s.Staging)
	}
	id := pgx.Identifier{idColumn}.Sanitize()
	return "INSERT INTO " + w.ident(s.Table) + " (" + list + ") SELECT DISTINCT ON (" + id + ") " + list +
		" FROM " + w.ident(s.Staging) + " s WHERE NOT EXISTS (SELECT 1 FROM " + w.ident(s.Table) + " t WHERE t." + id + " = s." + id + ")"
}

func quoteAll(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = pgx.Identifier{n}.Sanitize()
	}
	return strings.Join(q, ", ")
}

// ensureTarget makes the target table hold the staging table's columns,
// and returns them.
func (w *writer) ensureTarget(ctx context.Context, desc staged, s stagedStream) ([]string, error) {
	cols, err := w.columnTypes(ctx, s.Staging)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		// A newer generation's Recover drops what older ones staged.
		var fence uint64
		err := w.pool.QueryRow(ctx, "SELECT generation FROM "+w.ident("_openetl_fence")+" WHERE pipeline = $1", w.cfg.Pipeline).Scan(&fence)
		if err == nil && fence > w.cfg.Generation {
			return nil, fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, w.cfg.Generation)
		}
		return nil, fmt.Errorf("postgres: staging for %s in epoch %d-g%d is missing", s.Name, desc.Epoch, desc.Generation)
	}
	if err := w.ensureTable(ctx, s.Table, cols); err != nil {
		return nil, err
	}
	if s.Dedupe && !w.indexed[s.Table] {
		if _, err := w.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS "+pgx.Identifier{indexName(s.Table)}.Sanitize()+
			" ON "+w.ident(s.Table)+" ("+pgx.Identifier{idColumn}.Sanitize()+")"); err != nil {
			return nil, fmt.Errorf("postgres: index %s on %s: %w", idColumn, s.Table, err)
		}
		w.indexed[s.Table] = true
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
	}
	return names, nil
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
		if err := w.ensureTable(ctx, targetTable(s.Schema, s.Name), cols); err != nil {
			return err
		}
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
			ddl = "ADD COLUMN IF NOT EXISTS " + q + " " + c.typ
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
		_, _ = w.pool.Exec(ctx, "DROP TABLE IF EXISTS "+w.ident(s.Staging))
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
	present := 0
	for _, m := range missing {
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
	case present == len(missing):
		if err := w.apply(ctx, desc, missing); err != nil {
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
	return r, nil
}

// trimMarkers deletes the pipeline's markers below an applied epoch. Recovery
// checks only HEAD's, and HEAD never moves back past an applied epoch; a lost
// one moves it back to the previous record, whose markers it then checks.
// Best effort, like dropStaging.
func (w *writer) trimMarkers(ctx context.Context, applied uint64) {
	_, _ = w.pool.Exec(ctx, "DELETE FROM "+w.ident("_openetl_loads")+" WHERE pipeline = $1 AND epoch < $2", w.cfg.Pipeline, applied)
}

// dropStale drops what earlier generations of the pipeline staged. None of
// it can be applied once HEAD's epoch is recovered: the fence stops their
// commits. Best effort, like dropStaging.
func (w *writer) dropStale(ctx context.Context, gen uint64) {
	rows, err := w.pool.Query(ctx, `SELECT c.relname, d.description FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_description d ON d.objoid = c.oid AND d.classoid = 'pg_class'::regclass AND d.objsubid = 0
		WHERE n.nspname = $1 AND c.relkind = 'r' AND starts_with(c.relname, '_openetl_stage_')`, w.schema)
	if err != nil {
		return
	}
	labelled, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) ([2]string, error) {
		var t [2]string
		return t, r.Scan(&t[0], &t[1])
	})
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

func (w *writer) Close(context.Context) error {
	w.pool.Close()
	return nil
}
