// Package sqlserver has the SQL Server source, which reads tables in full,
// incrementally or their changes through change tracking or change data
// capture, and the SQL Server destination: append, overwrite and merge
// writes, bulk-copied into one staging table per stream, epoch and writer
// lane, and applied as one transaction behind a fence row with a marker per
// stream in _openetl_loads. Deduped streams insert one row per _openetl_id
// in the epoch, and with DedupeTarget only IDs the target doesn't have yet.
// Overwrite applies a read's epochs to a table of its own, which replaces
// the target's rows when the read ends. Merge updates and inserts each key's
// latest version, guarded by _openetl_seq.
package sqlserver

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

type DestinationConfig struct {
	// A sqlserver:// URL, or an ADO or ODBC connection string.
	DSN sdk.Secret `json:"dsn"`
	// Used when the pipeline names no namespace. Default "dbo".
	Schema string `json:"schema"`
}

type Destination struct {
	cfg    DestinationConfig
	db     *sql.DB
	dbName string
}

var (
	_ destination.Destination   = (*Destination)(nil)
	_ destination.SchemaApplier = (*writer)(nil)
	_ destination.ReadTracker   = (*writer)(nil)
	_ destination.Patcher       = (*writer)(nil)
)

func NewDestination(config []byte) (*Destination, error) {
	cfg := DestinationConfig{Schema: "dbo"}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("sqlserver: config: %w", err)
	}
	if cfg.DSN.IsZero() {
		return nil, errors.New("sqlserver: config needs a dsn")
	}
	parsed, err := msdsn.Parse(cfg.DSN.Reveal())
	if err != nil {
		return nil, fmt.Errorf("sqlserver: dsn: %w", err)
	}
	db, err := openDB(cfg.DSN.Reveal())
	if err != nil {
		return nil, fmt.Errorf("sqlserver: dsn: %w", err)
	}
	db.SetMaxOpenConns(2)
	return &Destination{cfg: cfg, db: db, dbName: parsed.Database}, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if s.Mode == destination.Merge && len(s.PrimaryKey) == 0 {
			return nil, fmt.Errorf("sqlserver: stream %q merges, and has no primary key", s.Name)
		}
	}
	db, err := openDB(d.cfg.DSN.Reveal())
	if err != nil {
		return nil, fmt.Errorf("sqlserver: dsn: %w", err)
	}
	// Only lane 0 commits alongside its writes.
	db.SetMaxOpenConns(4)
	if cfg.Lane > 0 {
		db.SetMaxOpenConns(1)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, classify(fmt.Errorf("sqlserver: connect: %w", err))
	}
	w := &writer{
		db:      db,
		cfg:     cfg,
		schema:  cfg.Namespace,
		Epochs:  sqlstage.NewEpochs[*stagingTable](),
		streams: map[string]destination.Stream{},
		indexed: map[string]bool{},
	}
	if w.schema == "" {
		w.schema = d.cfg.Schema
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	w.Dialect = sqlstage.Dialect[*stagingTable]{Name: "sqlserver", Open: cfg, Streams: w.streams,
		StagingName: sqlstage.StagingName, ReadName: sqlstage.ReadName, TargetTable: sqlstage.TargetTable, SQL: w}
	w.Own = sqlstage.Own{DB: db, Name: "sqlserver", Pipeline: cfg.Pipeline, Generation: cfg.Generation, Classify: classify,
		FenceRow: func(lock string) string {
			if lock != "" {
				lock = " WITH (" + lock + ")"
			}
			return "SELECT generation FROM " + w.fenceTable() + lock + " WHERE pipeline = @p1"
		},
		Marked: "SELECT count(*) FROM " + w.loadsTable() + " WHERE pipeline = @p1 AND stream = @p2 AND generation = @p3 AND epoch = @p4",
		Mark:   "INSERT INTO " + w.loadsTable() + " (pipeline, stream, generation, epoch) VALUES (@p1, @p2, @p3, @p4)",
		Trim:   "DELETE FROM " + w.loadsTable() + " WHERE pipeline = @p1 AND epoch < @p2"}
	return w, nil
}

type writer struct {
	db      *sql.DB
	cfg     destination.OpenConfig
	schema  string
	streams map[string]destination.Stream
	sqlstage.Epochs[*stagingTable]
	sqlstage.Own
	// Only Commit and Recover use it: tables with an _openetl_id index, or
	// one on their key.
	indexed map[string]bool
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

func (w *writer) ident(name string) string { return qualified(w.schema, name) }

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
	// A staging table's: each row's place in the order it was staged.
	rowColumn = "_openetl_row"
	// The extended property that labels staging and read tables.
	labelProperty = "openetl"
)

var metadataColumns = []string{idColumn, opColumn, seqColumn, syncedColumn, deletedColumn, unchangedColumn, heldColumn}

// Stage bulk-copies a batch into the stream's staging table for epoch e in
// tables, which it creates, labelled, or widens.
func (w *writer) Stage(ctx context.Context, tables map[uint64]map[string]*stagingTable, name string, e destination.Epoch, stream, target string, key []string, batch arrow.RecordBatch) (*stagingTable, error) {
	cols, err := columnsOf(batch.Schema())
	if err != nil {
		return nil, err
	}
	keyed(cols, key)
	byStream := tables[e.N]
	if byStream == nil {
		byStream = map[string]*stagingTable{}
		tables[e.N] = byStream
	}
	st := byStream[stream]
	label := sqlstage.StagingLabel{Pipeline: w.cfg.Pipeline, Generation: e.Generation, Epoch: e.N}
	if st == nil {
		st = &stagingTable{Table: sqlstage.Table{Name: name, Target: target, Key: key}}
		if err := w.createLabelled(ctx, st.Name, label, cols, true); err != nil {
			return nil, fmt.Errorf("sqlserver: create staging for %s: %w", stream, err)
		}
		st.cols = cols
		byStream[stream] = st
	} else if wider, err := w.addColumns(ctx, st, cols); err != nil {
		return nil, err
	} else if wider != nil {
		st.Earlier = append(st.Earlier, st.Name)
		st.Name = name + "_" + strconv.Itoa(len(st.Earlier)+1)
		if err := w.createLabelled(ctx, st.Name, label, wider, true); err != nil {
			return nil, fmt.Errorf("sqlserver: create staging for %s: %w", stream, err)
		}
		st.cols = wider
	}
	if st.narrower(cols) {
		err = w.copyCast(ctx, st, cols, batch)
	} else {
		err = w.bulk(ctx, w.db, st.Name, cols, batch, st.Rows)
	}
	if err != nil {
		return nil, fmt.Errorf("sqlserver: stage %s: %w", stream, err)
	}
	st.Rows += uint64(batch.NumRows())
	return st, nil
}

type execer interface {
	Conn(ctx context.Context) (*sql.Conn, error)
}

// bulk copies a batch's cols into table, numbering its rows in
// _openetl_row from after first.
func (w *writer) bulk(ctx context.Context, db execer, table string, cols []column, batch arrow.RecordBatch, first uint64) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = conn.Close() }()
	stmt, err := conn.PrepareContext(ctx, mssql.CopyIn(w.ident(table), mssql.BulkOptions{KeepNulls: true}, append(names(cols), rowColumn)...))
	if err != nil {
		return classify(err)
	}
	defer func() { _ = stmt.Close() }()
	arrs := batch.Columns()
	row := make([]any, len(cols)+1)
	for i := range int(batch.NumRows()) {
		for j, c := range cols {
			row[j] = nil
			if arrs[j].IsNull(i) || c.value == nil {
				continue
			}
			v := c.value(arrs[j], i)
			if err, ok := v.(error); ok {
				return fmt.Errorf("column %q: %w", c.name, err)
			}
			row[j] = v
		}
		row[len(cols)] = first + uint64(i) + 1
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return classify(err)
		}
	}
	_, err = stmt.ExecContext(ctx)
	return classify(err)
}

// createLabelled creates a table, unless it exists, with its label in an
// extended property, and with staging an _openetl_row column.
func (w *writer) createLabelled(ctx context.Context, name string, label any, cols []column, staging bool) error {
	b, err := json.Marshal(label)
	if err != nil {
		return err
	}
	defs := columnDefs(cols)
	if staging {
		defs = strings.Join(slices.DeleteFunc([]string{defs, quote(rowColumn) + " bigint NOT NULL"}, func(s string) bool { return s == "" }), ", ")
	}
	_, err = w.db.ExecContext(ctx, "IF OBJECT_ID(@p1, N'U') IS NULL BEGIN CREATE TABLE "+w.ident(name)+" ("+defs+"); "+
		"EXEC sys.sp_addextendedproperty @name = N'"+labelProperty+"', @value = @p2, @level0type = N'SCHEMA', @level0name = @p3, @level1type = N'TABLE', @level1name = @p4 END",
		w.ident(name), string(b), w.schema, name)
	return classify(err)
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
// own types, and converts its columns as widening the staging would have.
func (w *writer) copyCast(ctx context.Context, st *stagingTable, cols []column, batch arrow.RecordBatch) error {
	tmp := st.Name + "_older"
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = quote(c.name)
		if j := slices.IndexFunc(st.cols, func(x column) bool { return x.name == c.name }); st.cols[j].typ != c.typ {
			sel[i] = "CONVERT(" + st.cols[j].typ + ", " + quote(c.name) + ")"
		}
	}
	if _, err := w.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+w.ident(tmp)+"; CREATE TABLE "+w.ident(tmp)+" ("+columnDefs(cols)+", "+quote(rowColumn)+" bigint NOT NULL)"); err != nil {
		return classify(err)
	}
	defer func() { _, _ = w.db.ExecContext(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+w.ident(tmp)) }()
	if err := w.bulk(ctx, w.db, tmp, cols, batch, st.Rows); err != nil {
		return err
	}
	list := quoteAll(append(names(cols), rowColumn))
	_, err := w.db.ExecContext(ctx, "INSERT INTO "+w.ident(st.Name)+" ("+list+") SELECT "+strings.Join(sel, ", ")+", "+quote(rowColumn)+" FROM "+w.ident(tmp))
	return classify(err)
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
			if _, err := w.db.ExecContext(ctx, "ALTER TABLE "+w.ident(st.Name)+" ADD "+columnDef(c)); err != nil {
				return nil, fmt.Errorf("sqlserver: add column %q to staging: %w", c.name, classify(err))
			}
			st.cols = append(st.cols, c)
			wider = append(wider, c)
		case st.cols[i].typ == c.typ, c.value == nil, widens(c.typ, st.cols[i].typ):
		case widens(st.cols[i].typ, c.typ):
			wider[i], changed = c, true
		default:
			return nil, fmt.Errorf("sqlserver: column %q changed from %s to %s within an epoch", c.name, st.cols[i].typ, c.typ)
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
	def := quote(c.name) + " " + c.typ
	if c.name == deletedColumn {
		def += " NOT NULL DEFAULT 0"
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
	// The staging's columns; none if the epoch staged nothing for it.
	cols []string
	// The relation that reads every lane's staging, as acrossLanes makes it.
	rows lanesRows
	// Overwrite: the read table's columns once it can take the staging's;
	// none if the read has no table.
	read []string
	// Merge: the columns an update in the staging didn't send.
	unchanged []string
	// The patches' staging's columns; none if there's nothing to patch.
	patch []string
	// The relation that reads every lane's patches.
	patches lanesRows
	// Merge: the target's key columns.
	key []column
}

// lanesRows is a relation over a stream's staging on every lane.
type lanesRows struct {
	rel string
	// It's more than one lane's, so a row's place is its lane and its
	// _openetl_row.
	lanes bool
}

// place is the columns of rel, an alias of this relation, that order its
// rows as they were staged, as a list of (column, direction).
func (r lanesRows) place(rel string) []string {
	if r.lanes {
		return []string{rel + "._openetl_lane", rel + "." + quote(rowColumn)}
	}
	return []string{rel + "." + quote(rowColumn)}
}

// order spells place as an ORDER BY list, each column with dir.
func (r lanesRows) order(rel, dir string) string {
	p := r.place(rel)
	for i := range p {
		p[i] += dir
	}
	return strings.Join(p, ", ")
}

// Apply applies each stream that has no marker yet, then drops its staging.
// It adds the rows each deduped stream skipped to collapsed.
func (w *writer) Apply(ctx context.Context, desc staged, streams []stagedStream, collapsed map[string]uint64) error {
	todo, err := w.Unapplied(ctx, desc, streams)
	if err != nil {
		return err
	}
	if len(todo) == 0 {
		// Applied, perhaps by a newer generation, which this one must
		// then stop for.
		return w.Unfenced(ctx)
	}
	prep := make(map[string]prepared, len(todo))
	for _, s := range todo {
		if prep[s.Name], err = w.prepare(ctx, desc, s); err != nil {
			return err
		}
	}
	if w.cfg.Scope == destination.ScopeStream {
		for _, s := range todo {
			if err := w.applyTx(ctx, desc, []stagedStream{s}, prep, "HOLDLOCK, ROWLOCK", collapsed); err != nil {
				return err
			}
		}
	} else if err := w.applyTx(ctx, desc, todo, prep, "UPDLOCK, HOLDLOCK, ROWLOCK", collapsed); err != nil {
		return err
	}
	w.DropStaging(ctx, todo)
	return nil
}

func (w *writer) fenceTable() string { return w.ident("_openetl_fence") }
func (w *writer) loadsTable() string { return w.ident("_openetl_loads") }

// applyTx applies streams in one transaction, which holds the fence row:
// shared with lock HOLDLOCK, for applies of different streams at once, and
// alone with UPDLOCK.
func (w *writer) applyTx(ctx context.Context, desc staged, streams []stagedStream, prep map[string]prepared, lock string, collapsed map[string]uint64) error {
	return w.ApplyTx(ctx, nil, lock, desc, streams, func(tx *sql.Tx, s stagedStream) (uint64, error) {
		if s.Mode == modeOverwrite {
			if err := w.dropAbandonedReads(ctx, tx, func(l sqlstage.ReadLabel, table string) bool { return l.Stream == s.Name && table != s.Read }); err != nil {
				return 0, err
			}
		}
		return sqlstage.ApplyStream(ctx, "sqlserver", tx, w, s, prep[s.Name])
	}, collapsed)
}

// InsertSQL copies a stream's staging into its target, or its read's table. A
// deduped stream inserts one row per _openetl_id, the first staged, and with
// Dedupe only IDs the target doesn't have.
func (w *writer) InsertSQL(s stagedStream, p prepared) string {
	list, sel := quoteAll(p.cols), selectCols(p.cols, "s")
	into := "INSERT INTO " + w.ident(s.Into()) + " (" + list + ") "
	id := quote(idColumn)
	first := "(SELECT x.*, ROW_NUMBER() OVER (PARTITION BY x." + id + " ORDER BY " + p.rows.order("x", "") + ") AS _openetl_rn FROM " + p.rows.rel + " AS x) AS s WHERE s._openetl_rn = 1"
	switch {
	case s.Dedupe:
		return into + "SELECT " + sel + " FROM " + first + " AND NOT EXISTS (SELECT 1 FROM " + w.ident(s.Into()) + " AS t WHERE t." + id + " = s." + id + ")"
	case s.DedupeEpoch:
		return into + "SELECT " + sel + " FROM " + first
	default:
		return into + "SELECT " + sel + " FROM " + p.rows.rel + " AS s"
	}
}

// selectCols is cols from rel, with a staged row's list of held columns
// made into the target's object.
func selectCols(cols []string, rel string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = rel + "." + quote(c)
		if c == heldColumn && slices.Contains(cols, idColumn) {
			out[i] = heldObject(rel) + " AS " + quote(c)
		}
	}
	return strings.Join(out, ", ")
}

// idText spells a uniqueidentifier as JSON text holds it.
func idText(expr string) string { return "CONVERT(nvarchar(36), " + expr + ")" }

// heldObject maps each of rel's held columns to its _openetl_id, the
// version whose value is held; null for none.
func heldObject(rel string) string {
	return "(SELECT " + jsonObject("h.k", "h.v") + " FROM (SELECT j.value, " + idText(rel+"."+quote(idColumn)) + " FROM OPENJSON(" + rel + "." + quote(heldColumn) + ") AS j) AS h(k, v))"
}

// jsonObject aggregates rows of key and value text into a JSON object; null
// for none.
func jsonObject(key, value string) string {
	return "N'{' + STRING_AGG(CONVERT(nvarchar(max), N'\"' + STRING_ESCAPE(" + key + ", 'json') + N'\":\"' + STRING_ESCAPE(" + value + ", 'json') + N'\"'), N',') + N'}'"
}

// jsonArray aggregates rows of text into a JSON array; null for none.
func jsonArray(value string) string {
	return "N'[' + STRING_AGG(CONVERT(nvarchar(max), N'\"' + STRING_ESCAPE(" + value + ", 'json') + N'\"'), N',') + N']'"
}

// inArray says the JSON array arr holds the text lit.
func inArray(arr, lit string) string {
	return "EXISTS (SELECT 1 FROM OPENJSON(" + arr + ") AS a WHERE a.value = " + lit + ")"
}

// hasKey says the JSON object obj has the key lit.
func hasKey(obj, lit string) string {
	return "EXISTS (SELECT 1 FROM OPENJSON(" + obj + ") AS o WHERE o.[key] = " + lit + ")"
}

// jsonPath is the path of an object's key.
func jsonPath(key string) string {
	return literal(`$."` + strings.ReplaceAll(strings.ReplaceAll(key, `\`, `\\`), `"`, `\"`) + `"`)
}

// PatchSQL fills held cells from the epoch's patches: each column where the
// target still holds the version the patch comes from, matched by key for
// merge and by _openetl_id for append.
func (w *writer) PatchSQL(s stagedStream, p prepared) []string {
	cols := p.patch
	if len(cols) == 0 {
		return nil
	}
	var match []string
	if s.Mode == modeMerge {
		for _, k := range p.key {
			match = append(match, keyEqual("t", "p", k))
		}
	} else {
		match = []string{"t." + quote(idColumn) + " = p." + quote(idColumn)}
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
		lit, path := literal(held), jsonPath(held)
		set := quote(c) + " = p." + quote(c)
		if last[held] == c {
			set += ", " + quote(heldColumn) + " = NULLIF(JSON_MODIFY(t." + quote(heldColumn) + ", " + path + ", NULL), N'{}')"
		}
		if slices.Contains(cols, syncedColumn) {
			set += ", " + quote(syncedColumn) + " = p." + quote(syncedColumn)
		}
		out = append(out, "UPDATE t SET "+set+" FROM "+w.ident(s.Table)+" AS t JOIN "+p.patches.rel+" AS p ON "+strings.Join(match, " AND ")+
			" WHERE "+inArray("p."+quote(heldColumn), lit)+" AND JSON_VALUE(t."+quote(heldColumn)+", "+path+") = "+idText("p."+quote(idColumn)))
	}
	return out
}

// The temporary table a merge's apply holds each key's latest staged
// version in, for its statements.
const latestTable = "#openetl_latest"

// MergeSQL updates each key's latest staged version, by _openetl_seq and then
// staging order, onto its row unless the target's is newer, and inserts the
// keys the target doesn't have. A delete keeps the row's other columns as
// they were, with its metadata columns updated, or with hard deletes
// removes the row. A column in unchanged that the latest version didn't
// send takes its value from the key's latest staged version that did, unless
// the target's row is newer than that version, or else keeps the target's.
// Merge statements, which race and have had bugs
// with concurrent writers, aren't used: the target is locked by key while
// the apply inserts.
func (w *writer) MergeSQL(s stagedStream, p prepared) []string {
	cols, unchanged := p.cols, p.unchanged
	q := quote
	hasSeq, hasOp := slices.Contains(cols, seqColumn), slices.Contains(cols, opColumn)
	list := quoteAll(cols)
	hasHeld := slices.Contains(cols, heldColumn) && slices.Contains(cols, idColumn)
	latest := w.latest(s, p, hasSeq)
	sent := func(rel, c string) string { return "NOT " + inArray(rel+"."+q(unchangedColumn), literal(c)) }
	isDelete := func(rel string) string { return rel + "." + q(opColumn) + " = N'delete'" }
	notDelete := func(rel string) string {
		return "(" + rel + "." + q(opColumn) + " IS NULL OR " + rel + "." + q(opColumn) + " <> N'delete')"
	}
	// Unless the target's version is newer; a missing sequence doesn't
	// stop an update.
	newer := "1 = 1"
	if hasSeq {
		newer = "(t." + q(seqColumn) + " IS NULL OR s." + q(seqColumn) + " IS NULL OR t." + q(seqColumn) + " <= s." + q(seqColumn) + ")"
	}
	on := make([]string, len(p.key))
	for i, k := range p.key {
		on[i] = keyEqual("t", "s", k)
	}
	var sets []string
	for _, c := range cols {
		if slices.Contains(s.Key, c) {
			continue
		}
		v := "s." + q(c)
		switch {
		case c == unchangedColumn:
			// The columns neither ever sent.
			v = "(SELECT " + jsonArray("u.value") + " FROM OPENJSON(s." + q(c) + ") AS u WHERE " + inArray("t."+q(c), "u.value") + ")"
		case c == heldColumn && hasHeld:
			// The row's, and the target's for the columns it didn't send.
			kept := ""
			if slices.Contains(cols, unchangedColumn) {
				kept = "SELECT k.[key] AS k, k.value AS v FROM OPENJSON(t." + q(c) + ") AS k WHERE " + inArray("s."+q(unchangedColumn), "k.[key]") +
					" AND NOT " + hasKey("s."+q(c), "k.[key]") + " UNION ALL "
			}
			v = "(SELECT " + jsonObject("h.k", "h.v") + " FROM (" + kept + "SELECT n.[key], n.value FROM OPENJSON(s." + q(c) + ") AS n) AS h(k, v))"
		case slices.Contains(unchanged, c):
			v = "CASE WHEN " + sent("s", c) + " THEN " + v + " ELSE t." + q(c) + " END"
		}
		if hasHeld && slices.Contains(s.Last, c) && !slices.Contains(metadataColumns, c) {
			v = "CASE WHEN " + hasKey("s."+q(heldColumn), literal(c)) + " THEN t." + q(c) + " ELSE " + v + " END"
		}
		if hasOp && !s.HardDeletes && (!slices.Contains(metadataColumns, c) || c == unchangedColumn || c == heldColumn) {
			v = "CASE WHEN " + isDelete("s") + " THEN t." + q(c) + " ELSE " + v + " END"
		}
		sets = append(sets, q(c)+" = "+v)
	}
	upsertable := ""
	if hasOp && s.HardDeletes {
		upsertable = " AND " + notDelete("s")
	}
	out := []string{"DROP TABLE IF EXISTS " + latestTable, latest}
	if len(sets) > 0 {
		out = append(out, "UPDATE t SET "+strings.Join(sets, ", ")+" FROM "+w.ident(s.Table)+" AS t JOIN "+latestTable+" AS s ON "+strings.Join(on, " AND ")+
			" WHERE "+newer+upsertable)
	}
	out = append(out, "INSERT INTO "+w.ident(s.Table)+" ("+list+") SELECT "+prefixed(cols, "s")+" FROM "+latestTable+" AS s WHERE NOT EXISTS (SELECT 1 FROM "+
		w.ident(s.Table)+" AS t WITH (UPDLOCK, HOLDLOCK) WHERE "+strings.Join(on, " AND ")+")"+upsertable)
	if hasOp && s.HardDeletes {
		out = append(out, "DELETE t FROM "+w.ident(s.Table)+" AS t JOIN "+latestTable+" AS s ON "+strings.Join(on, " AND ")+" WHERE "+isDelete("s")+" AND "+newer)
	}
	return append(out, "DROP TABLE "+latestTable)
}

func prefixed(cols []string, rel string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = rel + "." + quote(c)
	}
	return strings.Join(out, ", ")
}

// latest fills the merge's temporary table with each key's latest staged
// version. Its held columns are an object, as the target's are. With
// unchanged columns, those the version didn't send come from the key's
// latest earlier version that sent them and isn't older than the target's
// row, if any, and its _openetl_unchanged then leaves them out.
func (w *writer) latest(s stagedStream, p prepared, hasSeq bool) string {
	cols, unchanged := p.cols, p.unchanged
	q := quote
	key := make([]string, len(p.key))
	match := make([]string, len(p.key))
	target := make([]string, len(p.key))
	for i, k := range p.key {
		target[i] = keyEqual("cur", "l", k)
		// Both are staged, in the same collation.
		k.collation = ""
		key[i] = keyPart("x", k)
		match[i] = keyEqual("e", "l", k)
	}
	order := p.rows.order("x", " DESC")
	latestFirst := p.rows.order("e", " DESC")
	if hasSeq {
		order = "x." + q(seqColumn) + " DESC, " + order
		latestFirst = "e." + q(seqColumn) + " DESC, " + latestFirst
	}
	// e was staged before l: (seq, place) below, as order ranks them, where a
	// null seq is below every other.
	before := rowLess(p.rows.place("e"), p.rows.place("l"))
	if hasSeq {
		es, ls := "e."+q(seqColumn), "l."+q(seqColumn)
		before = "(" + es + " < " + ls + " OR (" + es + " IS NULL AND " + ls + " IS NOT NULL) OR ((" + es + " = " + ls + " OR (" + es + " IS NULL AND " + ls + " IS NULL)) AND " + before + "))"
		// Nor older than the target's row, whose value is newer; a missing
		// seq doesn't stop it, as in MergeSQL.
		before += " AND NOT EXISTS (SELECT 1 FROM " + w.ident(s.Table) + " AS cur WHERE " + strings.Join(target, " AND ") + " AND cur." + q(seqColumn) + " > " + es + ")"
	}
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = "l." + q(c)
	}
	hasHeld := slices.Contains(cols, heldColumn) && slices.Contains(cols, idColumn)
	if hasHeld {
		sel[slices.Index(cols, heldColumn)] = heldObject("l") + " AS " + q(heldColumn)
	}
	from := "(SELECT x.*, ROW_NUMBER() OVER (PARTITION BY " + strings.Join(key, ", ") + " ORDER BY " + order + ") AS _openetl_rn FROM " + p.rows.rel + " AS x) AS l"
	if len(unchanged) > 0 {
		var removed, heldFrom []string
		for i, c := range unchanged {
			pi, lit := "p"+strconv.Itoa(i), literal(c)
			version := ""
			if hasHeld {
				// The version a column comes from may hold it.
				version = ", CASE WHEN " + inArray("e."+q(heldColumn), lit) + " THEN 1 ELSE 0 END AS held, " + idText("e."+q(idColumn)) + " AS id"
				heldFrom = append(heldFrom, " UNION ALL SELECT "+lit+", "+pi+".id WHERE "+pi+".found = 1 AND "+pi+".held = 1")
			}
			from += " OUTER APPLY (SELECT TOP (1) 1 AS found, e." + q(c) + " AS v" + version + " FROM " + p.rows.rel + " AS e" +
				" WHERE " + inArray("l."+q(unchangedColumn), lit) + " AND " + strings.Join(match, " AND ") + " AND " + before +
				" AND NOT " + inArray("e."+q(unchangedColumn), lit) + " ORDER BY " + latestFirst + ") AS " + pi
			sel[slices.Index(cols, c)] = "CASE WHEN " + pi + ".found = 1 THEN " + pi + ".v ELSE l." + q(c) + " END AS " + q(c)
			// found is null where no version gives the column: IS NOT NULL keeps
			// the test two-valued, so the column stays unchanged.
			removed = append(removed, "NOT (u.value = "+lit+" AND "+pi+".found IS NOT NULL)")
		}
		sel[slices.Index(cols, unchangedColumn)] = "(SELECT " + jsonArray("u.value") + " FROM OPENJSON(l." + q(unchangedColumn) + ") AS u WHERE " + strings.Join(removed, " AND ") + ") AS " + q(unchangedColumn)
		if hasHeld {
			// The row's own, but for the columns a version before it gives
			// held, and those.
			var given []string
			for i := range unchanged {
				pi := "p" + strconv.Itoa(i)
				given = append(given, "NOT (j.value = "+literal(unchanged[i])+" AND "+pi+".found IS NOT NULL AND "+pi+".held = 1)")
			}
			sel[slices.Index(cols, heldColumn)] = "(SELECT " + jsonObject("h.k", "h.v") + " FROM (SELECT j.value, " + idText("l."+q(idColumn)) +
				" FROM OPENJSON(l." + q(heldColumn) + ") AS j WHERE " + strings.Join(given, " AND ") + strings.Join(heldFrom, "") + ") AS h(k, v)) AS " + q(heldColumn)
		}
	}
	return "SELECT " + strings.Join(sel, ", ") + " INTO " + latestTable + " FROM " + from + " WHERE l._openetl_rn = 1"
}

// rowLess says the row (a...) sorts before (b...), as a row comparison
// does: false where a null decides it.
func rowLess(a, b []string) string {
	expr := a[len(a)-1] + " < " + b[len(b)-1]
	for i := len(a) - 2; i >= 0; i-- {
		expr = a[i] + " < " + b[i] + " OR (" + a[i] + " = " + b[i] + " AND (" + expr + "))"
	}
	return "(" + expr + ")"
}

// Replace makes the read's rows the target's, and drops the read's table.
// Readers see the old rows until the transaction commits.
func (w *writer) Replace(ctx context.Context, tx *sql.Tx, s stagedStream, p prepared) error {
	read := p.read
	switch {
	case s.Table == "" && len(read) > 0:
		return fmt.Errorf("its read has rows in %s, and no target table", s.Read)
	case s.Table == "":
		// The stream never had a schema, so it has no table to empty.
		return nil
	}
	var exists int
	if err := tx.QueryRowContext(ctx, "SELECT CASE WHEN OBJECT_ID(@p1, N'U') IS NULL THEN 0 ELSE 1 END", w.ident(s.Table)).Scan(&exists); err != nil || exists == 0 {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+w.ident(s.Table)); err != nil {
		return err
	}
	if len(read) == 0 {
		return nil
	}
	list := quoteAll(read)
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+w.ident(s.Table)+" ("+list+") SELECT "+list+" FROM "+w.ident(s.Read)); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "DROP TABLE "+w.ident(s.Read))
	return err
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
	if len(s.Stagings) > 0 {
		var err error
		if cols, p.rows, err = w.acrossLanes(ctx, s.Stagings); err != nil {
			return p, err
		}
		if len(cols) == 0 {
			// A newer generation's Recover drops what older ones staged.
			if err := w.Unfenced(ctx); err != nil {
				return p, err
			}
			return p, fmt.Errorf("sqlserver: staging for %s in epoch %d-g%d is missing", s.Name, desc.Epoch, desc.Generation)
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
			if err := w.ensureTable(ctx, s.Name, s.Table, have); err != nil {
				return p, err
			}
		}
	} else if cols != nil {
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
	if s.Dedupe && cols != nil {
		if err := w.ensureIndex(ctx, s.Into(), sqlstage.IndexName(s.Into()), false, []string{idColumn}); err != nil {
			return p, fmt.Errorf("sqlserver: index %s on %s: %w", idColumn, s.Into(), err)
		}
	}
	if s.Mode == modeMerge && (cols != nil || p.patch != nil) {
		var err error
		if p.key, err = w.ensureKey(ctx, s.Table, s.Key); err != nil {
			return p, err
		}
		if slices.Contains(p.cols, unchangedColumn) {
			rows, err := w.db.QueryContext(ctx, "SELECT DISTINCT j.value FROM "+p.rows.rel+" AS x CROSS APPLY OPENJSON(x."+quote(unchangedColumn)+") AS j ORDER BY j.value")
			if err != nil {
				return p, classify(err)
			}
			for rows.Next() {
				var c string
				if err := rows.Scan(&c); err != nil {
					_ = rows.Close()
					return p, classify(err)
				}
				if slices.Contains(p.cols, c) && !slices.Contains(s.Key, c) {
					p.unchanged = append(p.unchanged, c)
				}
			}
			if err := rows.Err(); err != nil {
				return p, classify(err)
			}
		}
	}
	return p, nil
}

// acrossLanes returns the columns of tables, each staging of one stream on
// a lane, at each column's widest type, and the relation that reads them as
// one with those columns, whatever each lane's schema was. A table's own
// relation if there's one. No columns if one of them doesn't exist.
func (w *writer) acrossLanes(ctx context.Context, tables []string) ([]column, lanesRows, error) {
	each := make([][]column, len(tables))
	var cols []column
	for i, t := range tables {
		var err error
		if each[i], err = w.columnTypes(ctx, t); err != nil {
			return nil, lanesRows{}, err
		}
		if len(each[i]) == 0 {
			return nil, lanesRows{}, nil
		}
		each[i] = slices.DeleteFunc(each[i], func(c column) bool { return c.name == rowColumn })
		for _, c := range each[i] {
			j := slices.IndexFunc(cols, func(x column) bool { return x.name == c.name })
			switch {
			case j < 0:
				cols = append(cols, c)
			case widens(c.typ, cols[j].typ):
			case widens(cols[j].typ, c.typ):
				cols[j] = c
			default:
				return nil, lanesRows{}, fmt.Errorf("sqlserver: column %q is %s on one lane, and %s on another", c.name, cols[j].typ, c.typ)
			}
		}
	}
	if len(tables) == 1 {
		return cols, lanesRows{rel: w.ident(tables[0])}, nil
	}
	selects := make([]string, len(tables))
	for i, t := range tables {
		sel := []string{strconv.Itoa(i) + " AS _openetl_lane", quote(rowColumn)}
		for _, c := range cols {
			sel = append(sel, laneColumn(c, each[i]))
		}
		selects[i] = "SELECT " + strings.Join(sel, ", ") + " FROM " + w.ident(t)
	}
	return cols, lanesRows{rel: "(" + strings.Join(selects, " UNION ALL ") + ")", lanes: true}, nil
}

// laneColumn selects the stream's column c from a lane's staging of the
// columns have: converted if it's narrower there, or what a row without it
// holds.
func laneColumn(c column, have []column) string {
	q := quote(c.name)
	i := slices.IndexFunc(have, func(x column) bool { return x.name == c.name })
	switch {
	case i >= 0 && have[i].typ == c.typ:
		return q
	case i >= 0:
		return "CONVERT(" + c.typ + ", " + q + ") AS " + q
	case c.name == deletedColumn:
		return "CONVERT(bit, 0) AS " + q
	}
	return "CONVERT(" + c.typ + ", NULL) AS " + q
}

// preparePatch returns the columns of the stream's patches, none if its
// target doesn't exist, _openetl_synced_at only if the target has it, and
// gives an append target the _openetl_id index patches match on.
func (w *writer) preparePatch(ctx context.Context, desc staged, s stagedStream) ([]string, lanesRows, error) {
	cols, rows, err := w.acrossLanes(ctx, s.Patches)
	if err != nil {
		return nil, rows, err
	}
	if len(cols) == 0 {
		return nil, rows, fmt.Errorf("sqlserver: patches for %s in epoch %d-g%d are missing", s.Name, desc.Epoch, desc.Generation)
	}
	target, err := w.columnTypes(ctx, s.Table)
	if err != nil || len(target) == 0 {
		return nil, rows, err
	}
	if s.Mode != modeMerge {
		if err := w.ensureIndex(ctx, s.Table, sqlstage.IndexName(s.Table), false, []string{idColumn}); err != nil {
			return nil, rows, fmt.Errorf("sqlserver: index %s on %s: %w", idColumn, s.Table, err)
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
	have, err := w.columnTypes(ctx, s.Read)
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
	if len(have) == 0 {
		want := make([]column, len(cols))
		for i, c := range cols {
			want[i] = targetType(c)
		}
		label := sqlstage.ReadLabel{Pipeline: w.cfg.Pipeline, Stream: s.Name, Table: s.Table}
		if err := w.createLabelled(ctx, s.Read, label, want, false); err != nil {
			return fmt.Errorf("sqlserver: create the read table for %s: %w", s.Name, err)
		}
		return nil
	}
	for _, c := range have {
		t := targetType(c)
		if t.typ == c.typ {
			continue
		}
		null := " NULL"
		if c.name == deletedColumn {
			null = " NOT NULL"
		}
		if err := w.ddl(ctx, "ALTER TABLE "+w.ident(s.Read)+" ALTER COLUMN "+quote(c.name)+" "+t.typ+null); err != nil {
			return fmt.Errorf("sqlserver: column %q of the read table for %s, as its target has it: %w", c.name, s.Name, err)
		}
	}
	return w.ensureTable(ctx, s.Name, s.Read, cols)
}

// ensureIndex builds an index on the table unless it has it.
func (w *writer) ensureIndex(ctx context.Context, table, name string, unique bool, cols []string) error {
	if w.indexed[name] {
		return nil
	}
	kind := "INDEX"
	if unique {
		kind = "UNIQUE INDEX"
	}
	_, err := w.db.ExecContext(ctx, "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(@p1) AND name = @p2) CREATE "+kind+" "+quote(name)+" ON "+w.ident(table)+" ("+quoteAll(cols)+")",
		w.ident(table), name)
	if err != nil {
		return err
	}
	w.indexed[name] = true
	return nil
}

// ensureKey returns a merge target's key columns, and gives the target an
// index on them, unless it has one, which the apply's lookups by key use.
// The index is unique unless the key has text or binary, whose index would
// take keys that differ only in trailing spaces or zero bytes as one. A key
// with a column no index can hold, as nvarchar(max), gets none.
func (w *writer) ensureKey(ctx context.Context, table string, key []string) ([]column, error) {
	have, err := w.columnTypes(ctx, table)
	if err != nil {
		return nil, err
	}
	cols := make([]column, len(key))
	unique, indexed := true, true
	for i, k := range key {
		j := slices.IndexFunc(have, func(c column) bool { return c.name == k })
		if j < 0 {
			return nil, fmt.Errorf("sqlserver: %s has no column %q of its key", table, k)
		}
		cols[i] = have[j]
		unique = unique && !isText(cols[i].typ) && !isBinary(cols[i].typ)
		indexed = indexed && indexable(cols[i].typ)
	}
	name := sqlstage.KeyIndexName(table)
	if w.indexed[name] || !indexed {
		return cols, nil
	}
	keys, _ := json.Marshal(key)
	var has int
	err = w.victim(ctx, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT count(*) FROM sys.indexes i WHERE i.object_id = OBJECT_ID(@p1) AND i.type > 0 AND i.has_filter = 0
			AND (SELECT count(*) FROM sys.index_columns ic WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0) = @p3
			AND NOT EXISTS (SELECT 1 FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
				WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0 AND c.name NOT IN (SELECT value FROM OPENJSON(@p2)))`,
			w.ident(table), string(keys), len(key)).Scan(&has)
	})
	if err != nil {
		return nil, classify(fmt.Errorf("sqlserver: read the indexes of %s: %w", table, err))
	}
	if has > 0 {
		w.indexed[name] = true
		return cols, nil
	}
	err = w.ensureIndex(ctx, table, name, unique, key)
	if me, ok := errors.AsType[mssql.Error](err); ok && me.Number == 1505 {
		return nil, fmt.Errorf("sqlserver: merging into %s needs one row per primary key (%s), and it has repeats; dedupe it or write to a new table: %w", table, strings.Join(key, ", "), err)
	}
	if err != nil {
		return nil, classify(fmt.Errorf("sqlserver: index on the key of %s: %w", table, err))
	}
	return cols, nil
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
		if cfg := w.streams[s.Name]; cfg.Mode == destination.Merge {
			// A key the batches lack fails their writes.
			if key, err := sqlstage.KeyColumns(s.Schema, cfg.PrimaryKey); err == nil {
				keyed(cols, key)
			}
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
			t = &tableInfo{}
			t.cols = want[i]
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
		return fmt.Errorf("sqlserver: column %q of %s: %s", refused[0].Column, table, refused[0].Reason)
	}
	return nil
}

// SQL Server's limit on a table's columns, unless they're sparse.
const maxColumns = 1024

// tableInfo is a table as the catalog has it.
type tableInfo struct {
	cols    []column
	notNull map[string]bool
	// The connector's own indexes on each column, which a change of its type
	// drops, for the next apply to build again.
	own map[string][]string
	// An index of the user's on each column, if any, which only a longer
	// length can be had under.
	theirs map[string]string
}

func (t *tableInfo) limits() destination.TableLimits {
	l := destination.TableLimits{MaxColumns: maxColumns}
	if t == nil {
		return l
	}
	l.Columns = len(t.cols)
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
	list, _ := json.Marshal(idents)
	var out map[string]*tableInfo
	err := w.victim(ctx, func(conn *sql.Conn) error {
		rows, err := conn.QueryContext(ctx, `SELECT CONVERT(int, j.[key]), c.name, TYPE_NAME(c.system_type_id), c.max_length, c.precision, c.scale, c.is_nullable, `+ownIndexes+`, coalesce(c.collation_name, ''), `+theirIndex+`
			FROM OPENJSON(@p1) AS j JOIN sys.columns c ON c.object_id = OBJECT_ID(j.value)
			ORDER BY CONVERT(int, j.[key]), c.column_id`, string(list))
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		out = map[string]*tableInfo{}
		for rows.Next() {
			var i int
			var c sourceColumn
			var nullable bool
			var own, theirs string
			if err := rows.Scan(&i, &c.name, &c.typ, &c.maxLength, &c.precision, &c.scale, &nullable, &own, &c.collation, &theirs); err != nil {
				return err
			}
			name := tables[i]
			t := out[name]
			if t == nil {
				t = &tableInfo{notNull: map[string]bool{}, own: map[string][]string{}, theirs: map[string]string{}}
				out[name] = t
			}
			t.cols = append(t.cols, column{name: c.name, typ: c.sqlType(), collation: c.collation})
			t.notNull[c.name] = !nullable
			if own != "" {
				t.own[c.name] = strings.Split(own, ",")
			}
			if theirs != "" {
				t.theirs[c.name] = theirs
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, classify(fmt.Errorf("sqlserver: read the columns of %s: %w", strings.Join(tables, ", "), err))
	}
	return out, nil
}

// victim runs read on a connection of its own at a low deadlock priority, so
// that in a deadlock with another session's DDL, such as another schema's
// drop of a staging table, read is the one rolled back, and runs it again,
// up to 5 times. Reading the catalog, read committed, holds a lock on a row
// of another table's columns while it waits to see that table, which the
// DDL has locked while it waits on the row.
func (w *writer) victim(ctx context.Context, read func(*sql.Conn) error) error {
	wait := 50 * time.Millisecond
	for try := 1; ; try++ {
		err := func() error {
			conn, err := w.db.Conn(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			if _, err := conn.ExecContext(ctx, "SET DEADLOCK_PRIORITY LOW"); err != nil {
				return err
			}
			defer func() { _, _ = conn.ExecContext(context.WithoutCancel(ctx), "SET DEADLOCK_PRIORITY NORMAL") }()
			return read(conn)
		}()
		me, ok := errors.AsType[mssql.Error](err)
		if try == 5 || !ok || me.Number != 1205 && me.Number != 601 {
			return err
		}
		t := time.NewTimer(wait + rand.N(wait))
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
		wait *= 2
	}
}

// alter creates the table with cols if it doesn't exist, or adds its new
// columns in one ALTER TABLE, and returns the columns it refused, with why.
// It widens a column's type only if the stream allows type changes, since
// that can rewrite the table. have is updated to match.
func (w *writer) alter(ctx context.Context, table string, cols []column, have *tableInfo, allow bool) ([]destination.RefusedColumn, error) {
	if have == nil {
		if len(cols) > maxColumns {
			return nil, fmt.Errorf("sqlserver: %s would have %d columns, and SQL Server takes %d", table, len(cols), maxColumns)
		}
		if err := w.ddl(ctx, "IF OBJECT_ID("+literal(w.ident(table))+", N'U') IS NULL CREATE TABLE "+w.ident(table)+" ("+columnDefs(cols)+")"); err != nil {
			return nil, fmt.Errorf("sqlserver: create %s: %w", table, err)
		}
		return nil, nil
	}
	var refused []destination.RefusedColumn
	full := func(c column, why string) destination.RefusedColumn {
		return destination.RefusedColumn{Column: c.name, Reason: "it takes no more columns: " + why, ToType: c.typ}
	}
	var adds []column
	// Columns to widen, by their index in have.cols.
	widen := map[int]column{}
	n := len(have.cols)
	for _, c := range cols {
		i := slices.IndexFunc(have.cols, func(x column) bool { return x.name == c.name })
		switch {
		case i < 0 && n >= maxColumns:
			refused = append(refused, full(c, fmt.Sprintf("the table has the %d columns SQL Server takes", maxColumns)))
		case i < 0:
			adds = append(adds, c)
			n++
		case have.cols[i].typ == c.typ || widens(c.typ, have.cols[i].typ):
		case !widens(have.cols[i].typ, c.typ):
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: fmt.Sprintf("it can't change a column from %s to %s", have.cols[i].typ, c.typ), FromType: have.cols[i].typ, ToType: c.typ,
			})
		case allow:
			widen[i] = c
		case have.theirs[c.name] != "" && !longer(have.cols[i].typ, c.typ):
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: fmt.Sprintf("it can't change the type of a column in your index %s, which SQL Server takes only a longer length under: drop the index, change the type, and create it again", have.theirs[c.name]),
				FromType: have.cols[i].typ, ToType: c.typ,
			})
		default:
			refused = append(refused, destination.RefusedColumn{
				Column: c.name, Reason: "it doesn't change a column's type unless the pipeline's typeChanges allows it, as that can rewrite the table",
				FromType: have.cols[i].typ, ToType: c.typ,
				Fix: alterColumn(w.ident(table), c.name, c.typ, have.notNull[c.name], have.own[c.name]),
			})
		}
	}
	for _, i := range slices.Sorted(maps.Keys(widen)) {
		c := widen[i]
		err := w.ddl(ctx, alterColumn(w.ident(table), c.name, c.typ, have.notNull[c.name], have.own[c.name]))
		// Such as an index or a view that depends on the column.
		if me, ok := errors.AsType[mssql.Error](err); ok && (me.Number == 5074 || me.Number == 4922 || me.Number == 5073) {
			refused = append(refused, destination.RefusedColumn{Column: c.name, Reason: "it can't change the column's type: " + me.Message, FromType: have.cols[i].typ, ToType: c.typ})
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("sqlserver: change the type of column %q of %s to %s: %w", c.name, table, c.typ, err)
		}
		have.cols[i].typ = c.typ
		for _, n := range have.own[c.name] {
			delete(w.indexed, n)
		}
	}
	if len(adds) == 0 {
		return refused, nil
	}
	defs := make([]string, len(adds))
	for i, c := range adds {
		defs[i] = columnDef(c)
	}
	err := w.ddl(ctx, "ALTER TABLE "+w.ident(table)+" ADD "+strings.Join(defs, ", "))
	if reason, ok := refusal(err); ok {
		// One of them can't be added; find it, and add the rest.
		for _, c := range adds {
			err := w.ddl(ctx, "ALTER TABLE "+w.ident(table)+" ADD "+columnDef(c))
			if reason, ok := refusal(err); ok {
				refused = append(refused, full(c, reason))
			} else if err != nil {
				return nil, fmt.Errorf("sqlserver: add column %q to %s: %w", c.name, table, err)
			} else {
				have.cols = append(have.cols, c)
			}
		}
		_ = reason
		return refused, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlserver: add columns to %s: %w", table, err)
	}
	have.cols = append(have.cols, adds...)
	return refused, nil
}

// alterColumn is the statement that changes a column's type, keeping
// whether it takes nulls, after dropping the connector's own indexes on it.
func alterColumn(ident, col, typ string, notNull bool, own []string) string {
	null := " NULL"
	if notNull {
		null = " NOT NULL"
	}
	var drops string
	for _, n := range own {
		drops += "DROP INDEX " + quote(n) + " ON " + ident + "; "
	}
	return drops + "ALTER TABLE " + ident + " ALTER COLUMN " + quote(col) + " " + typ + null + ";"
}

// ownIndexes selects, for a column c of sys.columns, the names of the
// connector's indexes on it, joined by commas.
const ownIndexes = `coalesce((SELECT STRING_AGG(i.name, N',') FROM sys.index_columns ic JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
	WHERE ic.object_id = c.object_id AND ic.column_id = c.column_id AND i.name LIKE N'\_openetl\_%' ESCAPE N'\'), N'')`

// theirIndex selects the name of an index on c that isn't the connector's,
// or an empty string.
const theirIndex = `coalesce((SELECT TOP (1) i.name FROM sys.index_columns ic JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
	WHERE ic.object_id = c.object_id AND ic.column_id = c.column_id AND i.name NOT LIKE N'\_openetl\_%' ESCAPE N'\'), N'')`

// refusal is why adding columns failed, if the table can't take them: too
// many columns, or too wide a row.
func refusal(err error) (string, bool) {
	me, ok := errors.AsType[mssql.Error](err)
	if !ok || me.Number != 1702 && me.Number != 1701 {
		return "", false
	}
	return me.Message, true
}

// How long a schema change waits for its table's lock before it gives up
// and tries again, so readers don't queue behind it, and how often it tries.
var (
	lockTimeout = 5 * time.Second
	ddlAttempts = 5
)

// ddl runs a schema change in a transaction of its own.
func (w *writer) ddl(ctx context.Context, stmt string) error {
	backoff := lockTimeout / 5
	var err error
	for range ddlAttempts {
		err = func() error {
			conn, err := w.db.Conn(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET LOCK_TIMEOUT %d", lockTimeout.Milliseconds())); err != nil {
				return err
			}
			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				_ = tx.Rollback()
				return err
			}
			return tx.Commit()
		}()
		if me, ok := errors.AsType[mssql.Error](err); !ok || me.Number != 1222 {
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
	return transient("sqlserver: waited for a lock %d times: %v", ddlAttempts, err)
}

// columnTypes returns a table's columns in order, or none if it doesn't
// exist.
func (w *writer) columnTypes(ctx context.Context, table string) ([]column, error) {
	have, err := w.catalog(ctx, []string{table})
	if err != nil || have[table] == nil {
		return nil, err
	}
	return have[table].cols, nil
}

// DropStaging is best effort: staging left behind is never applied twice,
// because its marker exists.
func (w *writer) DropStaging(ctx context.Context, streams []stagedStream) {
	for _, s := range streams {
		for _, t := range slices.Concat(s.Stagings, s.Patches) {
			_, _ = w.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+w.ident(t))
		}
	}
}

// ensureSchema creates the schema if it's missing.
func (w *writer) ensureSchema(ctx context.Context) error {
	_, err := w.db.ExecContext(ctx, "IF SCHEMA_ID(@p1) IS NULL EXEC ("+literal("CREATE SCHEMA "+quote(w.schema))+")", w.schema)
	if me, ok := errors.AsType[mssql.Error](err); ok && me.Number == 2714 {
		// Created meanwhile.
		return nil
	}
	if err != nil {
		return classify(fmt.Errorf("sqlserver: create schema %s: %w", w.schema, err))
	}
	return nil
}

// Fence makes the schema and the fence and marker tables, and raises the
// fence to gen.
func (w *writer) Fence(ctx context.Context, gen uint64) error {
	if err := w.ensureSchema(ctx); err != nil {
		return err
	}
	// Names compare by code point: the database's collation may take
	// streams that differ in case as one.
	for _, q := range []string{
		"IF OBJECT_ID(" + literal(w.fenceTable()) + ", N'U') IS NULL CREATE TABLE " + w.fenceTable() + " (pipeline nvarchar(450) COLLATE " + binaryCollation + " NOT NULL PRIMARY KEY, generation bigint NOT NULL)",
		"IF OBJECT_ID(" + literal(w.loadsTable()) + ", N'U') IS NULL CREATE TABLE " + w.loadsTable() + ` (pipeline nvarchar(200) COLLATE ` + binaryCollation + ` NOT NULL, stream nvarchar(200) COLLATE ` + binaryCollation + ` NOT NULL,
			generation bigint NOT NULL, epoch bigint NOT NULL, loaded_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
			PRIMARY KEY (pipeline, stream, generation, epoch))`,
	} {
		_, err := w.db.ExecContext(ctx, q)
		if me, ok := errors.AsType[mssql.Error](err); ok && me.Number == 2714 {
			continue
		}
		if err != nil {
			return classify(fmt.Errorf("sqlserver: prepare %s: %w", w.schema, err))
		}
	}
	return w.raiseFence(ctx, gen)
}

func (w *writer) Exists(ctx context.Context, table string) (bool, error) {
	var ok int
	if err := w.db.QueryRowContext(ctx, "SELECT CASE WHEN OBJECT_ID(@p1, N'U') IS NULL THEN 0 ELSE 1 END", w.ident(table)).Scan(&ok); err != nil {
		return false, classify(err)
	}
	return ok == 1, nil
}

// raiseFence sets the pipeline's fence to gen, unless a newer generation
// has raised it past; it waits for applies that hold the fence row.
func (w *writer) raiseFence(ctx context.Context, gen uint64) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = tx.Rollback() }()
	var fence uint64
	err = tx.QueryRowContext(ctx, "SELECT generation FROM "+w.fenceTable()+" WITH (UPDLOCK, HOLDLOCK) WHERE pipeline = @p1", w.cfg.Pipeline).Scan(&fence)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, "INSERT INTO "+w.fenceTable()+" (pipeline, generation) VALUES (@p1, @p2)", w.cfg.Pipeline, gen)
	case err != nil:
	case fence > gen:
		return fmt.Errorf("%w: recovering as generation %d", destination.ErrFenced, gen)
	default:
		_, err = tx.ExecContext(ctx, "UPDATE "+w.fenceTable()+" SET generation = @p2 WHERE pipeline = @p1", w.cfg.Pipeline, gen)
	}
	if err != nil {
		return classify(fmt.Errorf("sqlserver: raise fence: %w", err))
	}
	return classify(tx.Commit())
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// labelled returns the schema's tables whose names start with prefix, with
// their labels.
func (w *writer) labelled(ctx context.Context, q querier, prefix string) ([][2]string, error) {
	return sqlstage.Pairs(q.QueryContext(ctx, `SELECT t.name, CONVERT(nvarchar(4000), p.value) FROM sys.tables t
		JOIN sys.extended_properties p ON p.class = 1 AND p.major_id = t.object_id AND p.minor_id = 0 AND p.name = @p2
		WHERE t.schema_id = SCHEMA_ID(@p1) AND LEFT(t.name, LEN(@p3)) = @p3`, w.schema, labelProperty, prefix))
}

// DropStale drops what earlier generations of the pipeline staged. None of
// it can be applied once HEAD's epoch is recovered: the fence stops their
// commits. Best effort, like dropStaging.
func (w *writer) DropStale(ctx context.Context, gen uint64) {
	labelled, err := w.labelled(ctx, w.db, "_openetl_stage_")
	if err != nil {
		return
	}
	for _, t := range labelled {
		var l sqlstage.StagingLabel
		if json.Unmarshal([]byte(t[1]), &l) == nil && l.Pipeline == w.cfg.Pipeline && l.Generation < gen {
			_, _ = w.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+w.ident(t[0]))
		}
	}
}

// dropAbandonedReads drops the pipeline's read tables that drop says were
// abandoned, in q: an apply's transaction, which holds the fence, so a
// zombie can't drop a read that continues.
func (w *writer) dropAbandonedReads(ctx context.Context, q querier, drop func(l sqlstage.ReadLabel, table string) bool) error {
	labelled, err := w.labelled(ctx, q, "_openetl_read_")
	if err != nil {
		return fmt.Errorf("sqlserver: find abandoned reads: %w", err)
	}
	for _, t := range labelled {
		var l sqlstage.ReadLabel
		if json.Unmarshal([]byte(t[1]), &l) == nil && l.Pipeline == w.cfg.Pipeline && drop(l, t[0]) {
			if _, err := q.ExecContext(ctx, "DROP TABLE IF EXISTS "+w.ident(t[0])); err != nil {
				return fmt.Errorf("sqlserver: drop an abandoned read of %s: %w", l.Stream, err)
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
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback() }()
	if w.LockFence(ctx, tx, "HOLDLOCK, ROWLOCK") != nil {
		return
	}
	if w.dropAbandonedReads(ctx, tx, func(l sqlstage.ReadLabel, _ string) bool {
		return w.streams[l.Stream].Mode != destination.Overwrite
	}) == nil {
		_ = tx.Commit()
	}
}

func (w *writer) Close(context.Context) error { return w.db.Close() }
