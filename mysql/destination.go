// Package mysql has the MySQL source, which reads tables in full, by a
// cursor column or by their changes in the binary log, and the MySQL
// destination: append, overwrite and merge writes, staged into one table
// per stream, epoch and writer lane, and applied as one behind a fence row
// with a marker per stream in _openetl_loads. Both work against MariaDB.
//
// MySQL commits a transaction before each DDL statement, so the destination
// does every DDL an apply needs before the apply's transaction, which only
// changes rows: the fence check, the stream's rows and its marker commit
// together or not at all.
package mysql

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/go-sql-driver/mysql"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type DestinationConfig struct {
	// A DSN as the Go driver spells it, or a mysql:// URL.
	DSN sdk.Secret `json:"dsn"`
	// Used when the pipeline names no namespace. Default: the DSN's.
	Database string `json:"database"`
	// Nil: as the DSN says.
	TLS *TLSConfig `json:"tls"`
}

type Destination struct {
	cfg  DestinationConfig
	conn *mysql.Config
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
		return nil, fmt.Errorf("mysql: config: %w", err)
	}
	if cfg.DSN.IsZero() {
		return nil, errors.New("mysql: config needs a dsn")
	}
	c, err := parseDSN(cfg.DSN.Reveal())
	if err != nil {
		return nil, err
	}
	if err := applyTLS(c, cfg.TLS); err != nil {
		return nil, err
	}
	cfg.Database = cmp.Or(cfg.Database, c.DBName)
	if cfg.Database == "" {
		return nil, errors.New("mysql: config needs a database, or a dsn that names one")
	}
	return &Destination{cfg: cfg, conn: c}, nil
}

func (d *Destination) Capabilities() destination.Capabilities { return DestinationCapabilities }

func (d *Destination) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if s.Mode == destination.Merge && len(s.PrimaryKey) == 0 {
			return nil, fmt.Errorf("mysql: stream %q merges, and has no primary key", s.Name)
		}
	}
	// Only lane 0 commits alongside its writes, and a fenced drop takes two
	// connections.
	conns := 4
	if cfg.Lane > 0 {
		conns = 1
	}
	db, srv, err := open(ctx, d.conn, conns)
	if err != nil {
		return nil, err
	}
	var packet int
	if err := db.QueryRowContext(ctx, "SELECT @@max_allowed_packet").Scan(&packet); err != nil {
		_ = db.Close()
		return nil, classify(fmt.Errorf("mysql: connect: %w", err))
	}
	w := &writer{
		db:      db,
		srv:     srv,
		packet:  min(packet/4, 4<<20),
		cfg:     cfg,
		schema:  cmp.Or(cfg.Namespace, d.cfg.Database),
		Epochs:  sqlstage.NewEpochs[*stagingTable](),
		streams: map[string]destination.Stream{},
		indexed: map[string]bool{},
	}
	for _, s := range cfg.Streams {
		w.streams[s.Name] = s
	}
	w.Dialect = sqlstage.Dialect[*stagingTable]{Name: "mysql", Open: cfg, Streams: w.streams,
		StagingName: sqlstage.StagingName, ReadName: sqlstage.ReadName, TargetTable: sqlstage.TargetTable, SQL: w}
	w.Own = sqlstage.Own{DB: db, Name: "mysql", Pipeline: cfg.Pipeline, Generation: cfg.Generation, Classify: classify,
		FenceRow: func(lock string) string {
			return "SELECT generation FROM " + w.ident("_openetl_fence") + " WHERE pipeline = ? " + lock
		},
		Marked: "SELECT COUNT(*) FROM " + w.ident("_openetl_loads") + " WHERE pipeline = ? AND stream = ? AND generation = ? AND epoch = ?",
		Mark:   "INSERT INTO " + w.ident("_openetl_loads") + " (pipeline, stream, generation, epoch) VALUES (?, ?, ?, ?)",
		Trim:   "DELETE FROM " + w.ident("_openetl_loads") + " WHERE pipeline = ? AND epoch < ?"}
	return w, nil
}

type writer struct {
	db  *sql.DB
	srv server
	// Bytes of values in one INSERT statement.
	packet  int
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

func (w *writer) ident(name string) string { return quoteIdent(w.schema) + "." + quoteIdent(name) }

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
	// Each staging row's place in it, as it was staged.
	ordColumn = "_openetl_ord"
)

var metadataColumns = []string{idColumn, opColumn, seqColumn, syncedColumn, deletedColumn, unchangedColumn, heldColumn}

// Stage inserts a batch into the stream's staging table for epoch e in
// tables, which it creates, labelled, or widens.
func (w *writer) Stage(ctx context.Context, tables map[uint64]map[string]*stagingTable, name string, e destination.Epoch, stream, target string, key []string, batch arrow.RecordBatch) (*stagingTable, error) {
	cols, err := columnsOf(batch.Schema(), key)
	if err != nil {
		return nil, err
	}
	byStream := tables[e.N]
	if byStream == nil {
		byStream = map[string]*stagingTable{}
		tables[e.N] = byStream
	}
	label := sqlstage.StagingLabel{Pipeline: w.cfg.Pipeline, Generation: e.Generation, Epoch: e.N}
	st := byStream[stream]
	if st == nil {
		st = &stagingTable{Table: sqlstage.Table{Name: name, Target: target, Key: key}}
		if err := w.createLabelled(ctx, st.Name, label, cols, true); err != nil {
			return nil, fmt.Errorf("mysql: create staging for %s: %w", stream, err)
		}
		st.cols = cols
		byStream[stream] = st
	} else if wider, err := w.addColumns(ctx, st, cols); err != nil {
		return nil, err
	} else if wider != nil {
		st.Earlier = append(st.Earlier, st.Name)
		st.Name = name + "_" + strconv.Itoa(len(st.Earlier)+1)
		if err := w.createLabelled(ctx, st.Name, label, wider, true); err != nil {
			return nil, fmt.Errorf("mysql: create staging for %s: %w", stream, err)
		}
		st.cols = wider
	}
	if err := w.insertRows(ctx, st, cols, batch); err != nil {
		return nil, classify(fmt.Errorf("mysql: stage %s: %w", stream, err))
	}
	st.Rows += uint64(batch.NumRows())
	return st, nil
}

// insertRows inserts a batch into st in one transaction, as multi-row
// INSERTs of at most w.packet bytes of values and 65535 values each. A
// narrower batch's values take the staging's wider types as MySQL converts
// them, but for booleans in text, which are spelled true and false.
func (w *writer) insertRows(ctx context.Context, st *stagingTable, cols []column, batch arrow.RecordBatch) error {
	n := int(batch.NumRows())
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	prefix := "INSERT INTO " + w.ident(st.Name) + " (" + quoteAll(names(cols)) + ") VALUES "
	tuple := "()"
	if len(cols) > 0 {
		tuple = "(" + strings.Repeat("?, ", len(cols)-1) + "?)"
	}
	asText := make([]bool, len(cols))
	for j, c := range cols {
		i := slices.IndexFunc(st.cols, func(x column) bool { return x.name == c.name })
		asText[j] = c.typ == "tinyint(1)" && st.cols[i].typ == "longtext"
	}
	maxRows := 65535 / max(len(cols), 1)
	arrs := batch.Columns()
	var sb strings.Builder
	var args []any
	rows, size := 0, 0
	flush := func() error {
		if rows == 0 {
			return nil
		}
		_, err := tx.ExecContext(ctx, sb.String(), args...)
		sb.Reset()
		args, rows, size = args[:0], 0, 0
		return err
	}
	for i := range n {
		if rows == 0 {
			sb.WriteString(prefix)
		} else {
			sb.WriteString(", ")
		}
		sb.WriteString(tuple)
		for j, c := range cols {
			var v any
			if !arrs[j].IsNull(i) && c.value != nil {
				v = c.value(arrs[j], i)
				if err, ok := v.(error); ok {
					return fmt.Errorf("column %q: %w", c.name, err)
				}
				if asText[j] {
					v = strconv.FormatBool(v.(bool))
				}
			}
			args = append(args, v)
			size += valueSize(v)
		}
		rows++
		if rows >= maxRows || size >= w.packet {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	return tx.Commit()
}

// valueSize is about how many bytes a value takes in a statement.
func valueSize(v any) int {
	switch x := v.(type) {
	case string:
		return 2*len(x) + 3
	case []byte:
		return 2*len(x) + 10
	}
	return 24
}

func (w *writer) tableOptions() string {
	return " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=" + w.srv.collation
}

// createLabelled creates a table with its label as its comment, in one
// statement; ordered gives it _openetl_ord.
func (w *writer) createLabelled(ctx context.Context, name string, label any, cols []column, ordered bool) error {
	b, err := json.Marshal(label)
	if err != nil {
		return err
	}
	defs := columnDefs(cols)
	if ordered {
		defs = strings.Join(slices.DeleteFunc([]string{quoteIdent(ordColumn) + " bigint NOT NULL AUTO_INCREMENT PRIMARY KEY", defs}, func(s string) bool { return s == "" }), ", ")
	}
	return w.ddl(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(name)+" ("+defs+")"+w.tableOptions()+" COMMENT="+quoteLiteral(string(b)))
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
			if err := w.ddl(ctx, "ALTER TABLE "+w.ident(st.Name)+" ADD COLUMN "+columnDef(c)); err != nil {
				return nil, fmt.Errorf("mysql: add column %q to staging: %w", c.name, err)
			}
			st.cols = append(st.cols, c)
			wider = append(wider, c)
		case st.cols[i].typ == c.typ, c.value == nil, widens(c.typ, st.cols[i].typ):
		case widens(st.cols[i].typ, c.typ):
			wider[i], changed = c, true
		default:
			return nil, fmt.Errorf("mysql: column %q changed from %s to %s within an epoch", c.name, st.cols[i].typ, c.typ)
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
	def := quoteIdent(c.name) + " " + c.typ
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
	// The table with every lane's staging, as acrossLanes makes it.
	rows string
	// Overwrite: the read table's columns once it can take the staging's;
	// none if the read has no table.
	read []string
	// Merge: the columns an update in the staging didn't send, which the
	// apply resolves, and every name in the staging's _openetl_unchanged.
	unchanged, everUnchanged []string
	// Every name in the staging's _openetl_held.
	held []string
	// The patches' staging's columns; none if there's nothing to patch.
	patch []string
	// The table with every lane's patches.
	patches string
	// Tables acrossLanes made for this apply alone.
	made []string
}

// Apply applies each stream that has no marker yet, then drops its staging.
// It adds the rows each deduped stream skipped to collapsed.
func (w *writer) Apply(ctx context.Context, desc staged, streams []stagedStream, collapsed map[string]uint64) error {
	todo, err := w.Unapplied(ctx, desc, streams)
	if err != nil {
		return err
	}
	if len(todo) == 0 {
		// Applied, perhaps by a newer generation's Recover, which then stops
		// this one.
		return w.Unfenced(ctx)
	}
	prep := make(map[string]prepared, len(todo))
	for _, s := range todo {
		if prep[s.Name], err = w.prepare(ctx, desc, s); err != nil {
			return err
		}
	}
	if slices.ContainsFunc(todo, func(s stagedStream) bool { return s.Mode == modeOverwrite }) {
		// fn must not touch the fence's table, whose metadata lock the
		// transaction holds.
		err := w.Fenced(ctx, "LOCK IN SHARE MODE", func() error {
			return w.dropAbandonedReads(ctx, func(l sqlstage.ReadLabel, table string) bool {
				return slices.ContainsFunc(todo, func(s stagedStream) bool { return s.Mode == modeOverwrite && l.Stream == s.Name && table != s.Read })
			})
		})
		if err != nil {
			return err
		}
	}
	if w.cfg.Scope == destination.ScopeStream {
		for _, s := range todo {
			if err := w.applyTx(ctx, desc, []stagedStream{s}, prep, "LOCK IN SHARE MODE", collapsed); err != nil {
				return err
			}
		}
	} else if err := w.applyTx(ctx, desc, todo, prep, "FOR UPDATE", collapsed); err != nil {
		return err
	}
	for _, s := range todo {
		// The read's rows are the target's now.
		if s.Ended && s.Read != "" {
			_, _ = w.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+w.ident(s.Read))
		}
		for _, t := range prep[s.Name].made {
			_, _ = w.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+w.ident(t))
		}
	}
	w.DropStaging(ctx, todo)
	return nil
}

func (w *writer) applyTx(ctx context.Context, desc staged, streams []stagedStream, prep map[string]prepared, lock string, collapsed map[string]uint64) error {
	return w.ApplyTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, lock, desc, streams, func(tx *sql.Tx, s stagedStream) (uint64, error) {
		return sqlstage.ApplyStream(ctx, "mysql", tx, w, s, prep[s.Name])
	}, collapsed)
}

// latestFirst orders rel's rows from the last staged, by _openetl_seq if
// the staging has it.
func latestFirst(rel string, hasSeq bool) string {
	order := rel + "." + quoteIdent(ordColumn) + " DESC"
	if hasSeq {
		order = rel + "." + quoteIdent(seqColumn) + " DESC, " + order
	}
	return order
}

// jsonString is a SQL literal of name as a JSON string.
func jsonString(name string) string {
	b, _ := json.Marshal(name)
	return quoteLiteral(string(b))
}

// jsonPath is a SQL literal of the JSON path to an object's member name.
func jsonPath(name string) string {
	b, _ := json.Marshal(name)
	return quoteLiteral("$." + string(b))
}

// has is whether the JSON array rel.col has name; false for null.
func has(rel, col, name string) string {
	return "coalesce(JSON_CONTAINS(" + rel + "." + quoteIdent(col) + ", " + jsonString(name) + "), 0)"
}

// jsonOf builds a JSON array or object, as text, from parts that are each an
// element's or a member's text, or null to leave it out; null if all are.
func jsonOf(open, close string, parts []string) string {
	if len(parts) == 0 {
		return "NULL"
	}
	return "NULLIF(CONCAT('" + open + "', CONCAT_WS(',', " + strings.Join(parts, ", ") + "), '" + close + "'), '" + open + close + "')"
}

// member is an object member's text, name to the JSON string value v; null
// if v is.
func member(name, v string) string {
	return "CONCAT(" + jsonString(name) + ", ':', JSON_QUOTE(" + v + "))"
}

// heldObject maps each of rel's held columns to its _openetl_id, the
// version whose value is held; null for none. names are every column a
// staged row holds.
func heldObject(rel string, names []string) string {
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = "IF(" + has(rel, heldColumn, n) + ", " + member(n, rel+"."+quoteIdent(idColumn)) + ", NULL)"
	}
	return jsonOf("{", "}", parts)
}

// selectList is cols from rel, with a staged row's list of held columns
// made into the target's object.
func selectList(cols []string, rel string, held []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = rel + "." + quoteIdent(c)
		if c == heldColumn && slices.Contains(cols, idColumn) {
			out[i] = heldObject(rel, held) + " AS " + quoteIdent(c)
		}
	}
	return strings.Join(out, ", ")
}

// InsertSQL copies a stream's staging into its target, or its read's table. A
// deduped stream inserts one row per _openetl_id, the first staged, and with
// Dedupe only IDs the target doesn't have.
func (w *writer) InsertSQL(s stagedStream, p prepared) string {
	into := "INSERT INTO " + w.ident(s.Into()) + " (" + quoteAll(p.cols) + ") "
	sel := selectList(p.cols, "s", p.held)
	id := quoteIdent(idColumn)
	if !s.Deduped() {
		return into + "SELECT " + sel + " FROM " + p.rows + " s ORDER BY s." + quoteIdent(ordColumn)
	}
	q := into + "SELECT " + sel + " FROM (SELECT x.*, ROW_NUMBER() OVER (PARTITION BY x." + id + " ORDER BY x." + quoteIdent(ordColumn) + ") AS _openetl_rn FROM " +
		p.rows + " x) s WHERE s._openetl_rn = 1"
	if s.Dedupe {
		q += " AND NOT EXISTS (SELECT 1 FROM " + w.ident(s.Into()) + " t WHERE t." + id + " = s." + id + ")"
	}
	return q
}

// PatchSQL fills held cells from the epoch's patches: each column where the
// target still holds the version the patch comes from, matched by key for
// merge and by _openetl_id for append.
func (w *writer) PatchSQL(s stagedStream, p prepared) []string {
	cols := p.patch
	if len(cols) == 0 {
		return nil
	}
	q := quoteIdent
	var match []string
	if s.Mode == modeMerge {
		for _, k := range s.Key {
			match = append(match, "t."+q(k)+" = p."+q(k))
		}
	} else {
		match = []string{"t." + q(idColumn) + " = p." + q(idColumn)}
	}
	var out []string
	for _, c := range cols {
		if slices.Contains(metadataColumns, c) || slices.Contains(s.Key, c) {
			continue
		}
		held := c
		if f, ok := s.Fills[c]; ok {
			held = f
		}
		set := "t." + q(c) + " = p." + q(c) + ", t." + q(heldColumn) + " = NULLIF(JSON_REMOVE(t." + q(heldColumn) + ", " + jsonPath(held) + "), JSON_OBJECT())"
		if slices.Contains(cols, syncedColumn) {
			set += ", t." + q(syncedColumn) + " = p." + q(syncedColumn)
		}
		out = append(out, "UPDATE "+w.ident(s.Table)+" AS t JOIN "+p.patches+" AS p ON "+strings.Join(match, " AND ")+" SET "+set+
			" WHERE "+has("p", heldColumn, held)+" AND CAST(JSON_UNQUOTE(JSON_EXTRACT(t."+q(heldColumn)+", "+jsonPath(held)+")) AS BINARY) = CAST(p."+q(idColumn)+" AS BINARY)")
	}
	return out
}

// MergeSQL upserts each key's latest staged version, by _openetl_seq and then
// staging order, unless the target's is newer. A delete keeps the row's
// other columns as they were, with its metadata columns updated, or with
// hard deletes removes the row. A column in unchanged that the latest
// version didn't send takes its value from the key's latest staged version
// that did, or else keeps the target's.
//
// Each row's values are worked out in the SELECT, against the target's row
// joined as cur, and ON DUPLICATE KEY UPDATE only takes them: its
// assignments would see the values of those before them.
func (w *writer) MergeSQL(s stagedStream, p prepared) []string {
	cols := p.cols
	q := quoteIdent
	hasSeq, hasOp := slices.Contains(cols, seqColumn), slices.Contains(cols, opColumn)
	hasHeld := slices.Contains(cols, heldColumn) && slices.Contains(cols, idColumn)
	latest := w.latest(s, p, hasSeq)
	on := make([]string, len(s.Key))
	for i, k := range s.Key {
		on[i] = "cur." + q(k) + " = s." + q(k)
	}
	found := "cur." + q(s.Key[0]) + " IS NOT NULL"
	isDelete := "s." + q(opColumn) + " = 'delete'"
	// Unless the target's version is newer; a missing sequence doesn't
	// stop an update.
	newer := "TRUE"
	if hasSeq {
		newer = "NOT coalesce(cur." + q(seqColumn) + " > s." + q(seqColumn) + ", 0)"
	}
	exprs := make([]string, len(cols))
	var sets []string
	for i, c := range cols {
		v := "s." + q(c)
		exprs[i] = v
		if slices.Contains(s.Key, c) {
			continue
		}
		sets = append(sets, q(c)+" = VALUES("+q(c)+")")
		switch {
		case c == unchangedColumn:
			// The columns neither ever sent.
			parts := make([]string, len(p.everUnchanged))
			for j, n := range p.everUnchanged {
				parts[j] = "IF(" + has("s", c, n) + " AND " + has("cur", c, n) + ", " + jsonString(n) + ", NULL)"
			}
			v = "CASE WHEN s." + q(c) + " IS NULL OR cur." + q(c) + " IS NULL THEN NULL ELSE " + jsonOf("[", "]", parts) + " END"
		case c == heldColumn && hasHeld:
			// The row's, and the target's for the columns it didn't send.
			var kept []string
			if slices.Contains(cols, unchangedColumn) {
				for _, n := range p.everUnchanged {
					kept = append(kept, "IF("+has("s", unchangedColumn, n)+" AND coalesce(JSON_CONTAINS_PATH(cur."+q(c)+", 'one', "+jsonPath(n)+"), 0), CONCAT("+
						jsonString(n)+", ':', JSON_EXTRACT(cur."+q(c)+", "+jsonPath(n)+")), NULL)")
				}
			}
			v = "NULLIF(JSON_MERGE_PATCH(coalesce(" + jsonOf("{", "}", kept) + ", '{}'), coalesce(s." + q(c) + ", '{}')), JSON_OBJECT())"
		case slices.Contains(p.unchanged, c):
			v = "CASE WHEN " + has("s", unchangedColumn, c) + " THEN cur." + q(c) + " ELSE " + v + " END"
		}
		if hasHeld && slices.Contains(s.Last, c) && !slices.Contains(metadataColumns, c) {
			v = "CASE WHEN coalesce(JSON_CONTAINS_PATH(s." + q(heldColumn) + ", 'one', " + jsonPath(c) + "), 0) THEN cur." + q(c) + " ELSE " + v + " END"
		}
		if hasOp && !s.HardDeletes && (!slices.Contains(metadataColumns, c) || c == unchangedColumn || c == heldColumn) {
			v = "CASE WHEN " + isDelete + " THEN cur." + q(c) + " ELSE " + v + " END"
		}
		exprs[i] = "CASE WHEN " + found + " THEN " + v + " ELSE s." + q(c) + " END"
	}
	if len(sets) == 0 {
		sets = []string{q(s.Key[0]) + " = " + q(s.Key[0])}
	}
	where := "(NOT " + found + " OR " + newer + ")"
	if hasOp && s.HardDeletes {
		where += " AND NOT (s." + q(opColumn) + " <=> 'delete')"
	}
	upsert := "INSERT INTO " + w.ident(s.Table) + " (" + quoteAll(cols) + ") SELECT " + strings.Join(exprs, ", ") + " FROM " + latest +
		" s LEFT JOIN " + w.ident(s.Table) + " cur ON " + strings.Join(on, " AND ") + " WHERE " + where +
		" ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", ")
	if !hasOp || !s.HardDeletes {
		return []string{upsert}
	}
	del := "DELETE cur FROM " + w.ident(s.Table) + " cur JOIN " + latest + " s ON " + strings.Join(on, " AND ") + " WHERE " + isDelete + " AND " + newer
	return []string{upsert, del}
}

// latest is merge's derived table of each key's latest staged version. With
// unchanged columns, each comes from the key's latest earlier version that
// sent it, if any; its _openetl_unchanged then leaves it out.
func (w *writer) latest(s stagedStream, p prepared, hasSeq bool) string {
	cols := p.cols
	q := quoteIdent
	key := make([]string, len(s.Key))
	match := make([]string, len(s.Key))
	for i, k := range s.Key {
		key[i] = "x." + q(k)
		match[i] = "e." + q(k) + " = l." + q(k)
	}
	l := "(SELECT x.*, ROW_NUMBER() OVER (PARTITION BY " + strings.Join(key, ", ") + " ORDER BY " + latestFirst("x", hasSeq) + ") AS _openetl_rn FROM " + p.rows + " x) l"
	if len(p.unchanged) == 0 {
		return "(SELECT " + selectList(cols, "l", p.held) + " FROM " + l + " WHERE l._openetl_rn = 1)"
	}
	before := "e." + q(ordColumn) + " < l." + q(ordColumn)
	if hasSeq {
		before = "(e." + q(seqColumn) + ", e." + q(ordColumn) + ") < (l." + q(seqColumn) + ", l." + q(ordColumn) + ")"
	}
	// sent is where e is a version of l's key before it that sent c.
	sent := func(c string) string {
		return " FROM " + p.rows + " e WHERE " + strings.Join(match, " AND ") + " AND " + before + " AND NOT " + has("e", unchangedColumn, c)
	}
	found := func(c string) string {
		if !slices.Contains(p.unchanged, c) {
			return "FALSE"
		}
		return "(" + has("l", unchangedColumn, c) + " AND EXISTS (SELECT 1" + sent(c) + "))"
	}
	hasHeld := slices.Contains(cols, heldColumn) && slices.Contains(cols, idColumn)
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = "l." + q(c)
		switch {
		case slices.Contains(p.unchanged, c):
			sel[i] = "CASE WHEN " + found(c) + " THEN (SELECT e." + q(c) + sent(c) + " ORDER BY " + latestFirst("e", hasSeq) + " LIMIT 1) ELSE l." + q(c) + " END AS " + q(c)
		case c == unchangedColumn:
			parts := make([]string, len(p.everUnchanged))
			for j, n := range p.everUnchanged {
				parts[j] = "IF(" + has("l", c, n) + " AND NOT " + found(n) + ", " + jsonString(n) + ", NULL)"
			}
			sel[i] = jsonOf("[", "]", parts) + " AS " + q(c)
		case c == heldColumn && hasHeld:
			// The version a column comes from may hold it.
			var parts []string
			for _, n := range slices.Compact(slices.Sorted(slices.Values(slices.Concat(p.held, p.unchanged)))) {
				own := "IF(" + has("l", heldColumn, n) + ", l." + q(idColumn) + ", NULL)"
				v := own
				if slices.Contains(p.unchanged, n) {
					from := "(SELECT IF(" + has("e", heldColumn, n) + ", e." + q(idColumn) + ", NULL)" + sent(n) + " ORDER BY " + latestFirst("e", hasSeq) + " LIMIT 1)"
					v = "coalesce(CASE WHEN " + found(n) + " THEN " + from + " END, " + own + ")"
				}
				parts = append(parts, member(n, v))
			}
			sel[i] = jsonOf("{", "}", parts) + " AS " + q(c)
		}
	}
	return "(SELECT " + strings.Join(sel, ", ") + " FROM " + l + " WHERE l._openetl_rn = 1)"
}

// Replace makes the read's rows the target's. Readers see the old rows
// until the transaction commits; apply drops the read's table after it.
func (w *writer) Replace(ctx context.Context, tx *sql.Tx, s stagedStream, p prepared) error {
	read := p.read
	switch {
	case s.Table == "" && len(read) > 0:
		return fmt.Errorf("its read has rows in %s, and no target table", s.Read)
	case s.Table == "":
		// The stream never had a schema, so it has no table to empty.
		return nil
	}
	var n int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", w.schema, s.Table).Scan(&n); err != nil || n == 0 {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+w.ident(s.Table)); err != nil {
		return err
	}
	if len(read) == 0 {
		return nil
	}
	list := quoteAll(read)
	_, err := tx.ExecContext(ctx, "INSERT INTO "+w.ident(s.Table)+" ("+list+") SELECT "+list+" FROM "+w.ident(s.Read))
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
		if cols, p.rows, err = w.acrossLanes(ctx, desc, s.Stagings, s.Name, &p.made); err != nil {
			return p, err
		}
		if len(cols) == 0 {
			// A newer generation's Recover drops what older ones staged.
			var fence uint64
			err := w.db.QueryRowContext(ctx, "SELECT generation FROM "+w.ident("_openetl_fence")+" WHERE pipeline = ?", w.cfg.Pipeline).Scan(&fence)
			if err == nil && fence > w.cfg.Generation {
				return p, fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, w.cfg.Generation)
			}
			return p, fmt.Errorf("mysql: staging for %s in epoch %d-g%d is missing", s.Name, desc.Epoch, desc.Generation)
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
		if p.patch, p.patches, err = w.preparePatch(ctx, desc, s, &p.made); err != nil {
			return p, err
		}
	}
	if s.Dedupe && cols != nil {
		if err := w.ensureIndex(ctx, s.Into(), sqlstage.IndexName(s.Into()), []string{idColumn}); err != nil {
			return p, fmt.Errorf("mysql: index %s on %s: %w", idColumn, s.Into(), err)
		}
	}
	var err error
	if slices.Contains(p.cols, heldColumn) {
		if p.held, err = w.jsonNames(ctx, p.rows, heldColumn); err != nil {
			return p, err
		}
	}
	if s.Mode == modeMerge && (cols != nil || p.patch != nil) {
		if err := w.ensureKey(ctx, s.Table, s.Key); err != nil {
			return p, err
		}
		if slices.Contains(p.cols, unchangedColumn) {
			if p.everUnchanged, err = w.jsonNames(ctx, p.rows, unchangedColumn); err != nil {
				return p, err
			}
			p.unchanged = slices.DeleteFunc(slices.Clone(p.everUnchanged), func(c string) bool { return !slices.Contains(p.cols, c) || slices.Contains(s.Key, c) })
		}
	}
	return p, nil
}

// jsonNames returns the distinct strings in the JSON arrays of rel's col.
func (w *writer) jsonNames(ctx context.Context, rel, col string) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, "SELECT DISTINCT j.c FROM "+rel+" x, JSON_TABLE(x."+quoteIdent(col)+", '$[*]' COLUMNS (c varchar(255) PATH '$')) j WHERE j.c IS NOT NULL ORDER BY j.c")
	if err != nil {
		return nil, classify(fmt.Errorf("mysql: read %s of the staging: %w", col, err))
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
func (w *writer) acrossLanes(ctx context.Context, desc staged, tables []string, of string, made *[]string) ([]column, string, error) {
	each := make([][]column, len(tables))
	var cols []column
	for i, t := range tables {
		var err error
		if each[i], err = w.columnTypes(ctx, t); err != nil {
			return nil, "", err
		}
		if len(each[i]) == 0 {
			return nil, "", nil
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
				return nil, "", fmt.Errorf("mysql: column %q is %s on one lane, and %s on another", c.name, cols[j].typ, c.typ)
			}
		}
	}
	if len(tables) == 1 {
		return cols, w.ident(tables[0]), nil
	}
	// Named for the applier, so that a fenced one's can't be another's.
	all := sqlstage.StagingName(w.cfg.Pipeline, of+"\x00lanes\x00"+tables[0]+"\x00"+strconv.FormatUint(w.cfg.Generation, 10), destination.Epoch{Generation: desc.Generation, N: desc.Epoch}, 0)
	if _, err := w.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+w.ident(all)); err != nil {
		return nil, "", classify(err)
	}
	label := sqlstage.StagingLabel{Pipeline: w.cfg.Pipeline, Generation: w.cfg.Generation, Epoch: desc.Epoch}
	if err := w.createLabelled(ctx, all, label, cols, true); err != nil {
		return nil, "", fmt.Errorf("mysql: create the staging of every lane: %w", err)
	}
	*made = append(*made, all)
	for i, t := range tables {
		list := quoteAll(names(each[i]))
		if _, err := w.db.ExecContext(ctx, "INSERT INTO "+w.ident(all)+" ("+list+") SELECT "+list+" FROM "+w.ident(t)+" ORDER BY "+quoteIdent(ordColumn)); err != nil {
			return nil, "", classify(fmt.Errorf("mysql: gather the staging of every lane: %w", err))
		}
	}
	return cols, w.ident(all), nil
}

// preparePatch returns the columns of the stream's patches, none if its
// target doesn't exist, _openetl_synced_at only if the target has it, and
// gives an append target the _openetl_id index patches match on.
func (w *writer) preparePatch(ctx context.Context, desc staged, s stagedStream, made *[]string) ([]string, string, error) {
	cols, rows, err := w.acrossLanes(ctx, desc, s.Patches, s.Name+"\x00patch", made)
	if err != nil {
		return nil, rows, err
	}
	if len(cols) == 0 {
		return nil, rows, fmt.Errorf("mysql: patches for %s in epoch %d-g%d are missing", s.Name, desc.Epoch, desc.Generation)
	}
	target, err := w.columnTypes(ctx, s.Table)
	if err != nil || len(target) == 0 {
		return nil, rows, err
	}
	if s.Mode != modeMerge {
		if err := w.ensureIndex(ctx, s.Table, sqlstage.IndexName(s.Table), []string{idColumn}); err != nil {
			return nil, rows, fmt.Errorf("mysql: index %s on %s: %w", idColumn, s.Table, err)
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
			return fmt.Errorf("mysql: create the read table for %s: %w", s.Name, err)
		}
		return nil
	}
	for _, c := range have {
		if t := targetType(c); t.typ != c.typ {
			if err := w.ddl(ctx, "ALTER TABLE "+w.ident(s.Read)+" MODIFY COLUMN "+columnDef(t)); err != nil {
				return fmt.Errorf("mysql: column %q of the read table for %s, as its target has it: %w", c.name, s.Name, err)
			}
		}
	}
	return w.ensureTable(ctx, s.Name, s.Read, cols)
}

// ensureIndex gives table an index on cols, unless it has one by name.
// InnoDB builds it without blocking the table's writers.
func (w *writer) ensureIndex(ctx context.Context, table, name string, cols []string) error {
	if w.indexed[name] {
		return nil
	}
	var n int
	if err := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?",
		w.schema, table, name).Scan(&n); err != nil {
		return classify(err)
	}
	if n == 0 {
		if err := w.ddl(ctx, "ALTER TABLE "+w.ident(table)+" ADD INDEX "+quoteIdent(name)+" ("+quoteAll(cols)+")"); err != nil {
			return err
		}
	}
	w.indexed[name] = true
	return nil
}

// ensureKey gives a merge target a unique index on its key, unless it has
// one on those columns whole, for ON DUPLICATE KEY UPDATE.
func (w *writer) ensureKey(ctx context.Context, table string, key []string) error {
	name := sqlstage.KeyIndexName(table)
	if w.indexed[name] {
		return nil
	}
	rows, err := w.db.QueryContext(ctx, `SELECT INDEX_NAME, COLUMN_NAME, SUB_PART IS NOT NULL FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0 ORDER BY INDEX_NAME, SEQ_IN_INDEX`, w.schema, table)
	if err != nil {
		return classify(fmt.Errorf("mysql: read the indexes of %s: %w", table, err))
	}
	indexes := map[string][]string{}
	partial := map[string]bool{}
	for rows.Next() {
		var index, col string
		var part bool
		if err := rows.Scan(&index, &col, &part); err != nil {
			_ = rows.Close()
			return err
		}
		indexes[index] = append(indexes[index], col)
		partial[index] = partial[index] || part
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	want := slices.Sorted(slices.Values(key))
	for index, cols := range indexes {
		if !partial[index] && slices.Equal(slices.Sorted(slices.Values(cols)), want) {
			w.indexed[name] = true
			return nil
		}
	}
	err = w.ddl(ctx, "ALTER TABLE "+w.ident(table)+" ADD UNIQUE INDEX "+quoteIdent(name)+" ("+quoteAll(key)+")")
	switch n, _ := serverError(err); n {
	case 0:
		w.indexed[name] = true
		return nil
	case 1062:
		return fmt.Errorf("mysql: merging into %s needs one row per primary key (%s), and it has repeats; dedupe it or write to a new table: %w", table, strings.Join(key, ", "), err)
	case 1170, 1071:
		return fmt.Errorf("mysql: merging into %s needs a unique index on its key (%s), and its columns are too long to index whole; make them VARCHAR or VARBINARY: %w", table, strings.Join(key, ", "), err)
	}
	return fmt.Errorf("mysql: unique index on the key of %s: %w", table, err)
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
		var key []string
		if cfg := w.streams[s.Name]; cfg.Mode == destination.Merge {
			key, _ = sqlstage.KeyColumns(s.Schema, cfg.PrimaryKey)
		}
		cols, err := columnsOf(s.Schema, key)
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
			t = &tableInfo{}
			for _, c := range want[i] {
				t.cols = append(t.cols, tableColumn{column: c})
			}
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
		return fmt.Errorf("mysql: column %q of %s: %s", refused[0].Column, table, refused[0].Reason)
	}
	return nil
}

// InnoDB's limit on a table's columns.
const maxColumns = 1017

type tableColumn struct {
	column
	notNull bool
	// What MODIFY COLUMN must say to keep the column's default; empty for
	// none.
	def string
}

// tableInfo is a table as the catalog has it.
type tableInfo struct {
	cols []tableColumn
}

func (t *tableInfo) limits() destination.TableLimits {
	l := destination.TableLimits{MaxColumns: maxColumns}
	if t == nil {
		return l
	}
	l.Columns = len(t.cols)
	for _, c := range t.cols {
		if !slices.Contains(metadataColumns, c.name) && c.notNull {
			l.NotNull = append(l.NotNull, c.name)
		}
	}
	return l
}

// catalog reads the tables' columns in one query: their types, which can't
// take a null, and their defaults. A table that doesn't exist is missing.
func (w *writer) catalog(ctx context.Context, tables []string) (map[string]*tableInfo, error) {
	out := map[string]*tableInfo{}
	if len(tables) == 0 {
		return out, nil
	}
	args := []any{w.schema}
	for _, t := range tables {
		args = append(args, t)
	}
	rows, err := w.db.QueryContext(ctx, `SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE = 'NO', COLUMN_DEFAULT, EXTRA
		FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN (?`+strings.Repeat(", ?", len(tables)-1)+`)
		ORDER BY TABLE_NAME, ORDINAL_POSITION`, args...)
	if err != nil {
		return nil, classify(fmt.Errorf("mysql: read the columns of %s: %w", strings.Join(tables, ", "), err))
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var table, extra string
		var c tableColumn
		var def sql.NullString
		if err := rows.Scan(&table, &c.name, &c.typ, &c.notNull, &def, &extra); err != nil {
			return nil, err
		}
		c.typ = normType(c.typ)
		c.def = w.defaultClause(def, extra)
		t := out[table]
		if t == nil {
			t = &tableInfo{}
			out[table] = t
		}
		t.cols = append(t.cols, c)
	}
	return out, rows.Err()
}

// defaultClause is the DEFAULT clause that keeps a column's default, as the
// catalog spells it: MySQL gives a literal unquoted and marks an expression
// in EXTRA, MariaDB gives SQL.
func (w *writer) defaultClause(def sql.NullString, extra string) string {
	switch {
	case !def.Valid || w.srv.mariadb && def.String == "NULL":
		return ""
	case w.srv.mariadb:
		return " DEFAULT " + def.String
	case strings.Contains(strings.ToUpper(extra), "DEFAULT_GENERATED"):
		return " DEFAULT (" + def.String + ")"
	}
	return " DEFAULT " + quoteLiteral(def.String)
}

// modify is the ALTER TABLE that changes a column's type and keeps its NOT
// NULL and default, which MODIFY COLUMN would otherwise drop.
func (w *writer) modify(table string, have tableColumn, typ string) string {
	stmt := "ALTER TABLE " + w.ident(table) + " MODIFY COLUMN " + quoteIdent(have.name) + " " + typ
	if have.notNull {
		stmt += " NOT NULL"
	}
	return stmt + have.def
}

// alter creates the table with cols if it doesn't exist, or adds its new
// columns in one ALTER TABLE, and returns the columns it refused, with why.
// It widens a column's type only if the stream allows type changes, since
// that can rebuild the table. have is updated to match.
func (w *writer) alter(ctx context.Context, table string, cols []column, have *tableInfo, allow bool) ([]destination.RefusedColumn, error) {
	if have == nil {
		if err := w.ddl(ctx, "CREATE TABLE IF NOT EXISTS "+w.ident(table)+" ("+columnDefs(cols)+")"+w.tableOptions()); err != nil {
			return nil, fmt.Errorf("mysql: create %s: %w", table, err)
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
	n := len(have.cols)
	for _, c := range cols {
		i := slices.IndexFunc(have.cols, func(x tableColumn) bool { return x.name == c.name })
		var clause string
		switch {
		case i < 0 && n >= maxColumns:
			refused = append(refused, full(c, fmt.Sprintf("the table has the %d columns InnoDB takes", maxColumns)))
			continue
		case i < 0:
			clause = "ADD COLUMN " + columnDef(c)
			n++
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
				Column: c.name, Reason: "it doesn't change a column's type unless the pipeline's typeChanges allows it, as that can rebuild the table",
				FromType: have.cols[i].typ, ToType: c.typ, Fix: w.modify(table, have.cols[i], c.typ),
			})
			continue
		}
		clauses, changed = append(clauses, clause), append(changed, c)
	}
	for _, i := range slices.Sorted(maps.Keys(widen)) {
		c := widen[i]
		if err := w.ddl(ctx, w.modify(table, have.cols[i], c.typ)); err != nil {
			return nil, fmt.Errorf("mysql: change the type of column %q of %s to %s: %w", c.name, table, c.typ, err)
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
				return nil, fmt.Errorf("mysql: add column %q to %s: %w", changed[i].name, table, err)
			} else {
				have.cols = append(have.cols, tableColumn{column: changed[i]})
			}
		}
		return refused, nil
	}
	if err != nil {
		return nil, fmt.Errorf("mysql: add columns to %s: %w", table, err)
	}
	for _, c := range changed {
		have.cols = append(have.cols, tableColumn{column: c})
	}
	return refused, nil
}

// refusal is why adding columns failed, if the table can't take them: it
// has too many columns, or its rows would be too big.
func refusal(err error) (string, bool) {
	me, ok := errors.AsType[*mysql.MySQLError](err)
	if !ok || (me.Number != 1117 && me.Number != 1118) {
		return "", false
	}
	return me.Message, true
}

// How long a schema change waits for its table's metadata lock before it
// gives up and tries again, so readers don't queue behind it, and how often
// it tries.
var (
	lockTimeout = 5 * time.Second
	ddlAttempts = 5
)

func (w *writer) ddl(ctx context.Context, stmt string) error {
	backoff := lockTimeout / 5
	var err error
	for range ddlAttempts {
		err = w.ddlOnce(ctx, stmt)
		if n, _ := serverError(err); n != 1205 {
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
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("mysql: waited for a lock %d times: %w", ddlAttempts, err))
}

func (w *writer) ddlOnce(ctx context.Context, stmt string) error {
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", int(lockTimeout.Seconds()))); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, stmt)
	if _, rerr := conn.ExecContext(ctx, "SET SESSION lock_wait_timeout = DEFAULT"); rerr != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	return err
}

// columnTypes returns a table's columns in order, or none if it doesn't
// exist.
func (w *writer) columnTypes(ctx context.Context, table string) ([]column, error) {
	rows, err := w.db.QueryContext(ctx, `SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME <> ? ORDER BY ORDINAL_POSITION`, w.schema, table, ordColumn)
	if err != nil {
		return nil, classify(fmt.Errorf("mysql: read the columns of %s: %w", table, err))
	}
	defer func() { _ = rows.Close() }()
	var cols []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.name, &c.typ); err != nil {
			return nil, err
		}
		c.typ = normType(c.typ)
		cols = append(cols, c)
	}
	return cols, rows.Err()
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

// ensureSchema creates the database if it's missing. CREATE DATABASE IF NOT
// EXISTS needs CREATE even when the database exists.
func (w *writer) ensureSchema(ctx context.Context) error {
	var n int
	if err := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", w.schema).Scan(&n); err != nil {
		return classify(fmt.Errorf("mysql: database %s: %w", w.schema, err))
	}
	if n > 0 {
		return nil
	}
	if _, err := w.db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(w.schema)+" CHARACTER SET utf8mb4 COLLATE "+w.srv.collation); err != nil {
		return classify(fmt.Errorf("mysql: create database %s: %w", w.schema, err))
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
		"CREATE TABLE IF NOT EXISTS " + w.ident("_openetl_fence") + " (pipeline varchar(255) PRIMARY KEY, generation bigint unsigned NOT NULL)" + w.tableOptions(),
		"CREATE TABLE IF NOT EXISTS " + w.ident("_openetl_loads") + ` (pipeline varchar(255) NOT NULL, stream varchar(500) NOT NULL,
			generation bigint unsigned NOT NULL, epoch bigint unsigned NOT NULL, loaded_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			PRIMARY KEY (pipeline, stream, generation, epoch))` + w.tableOptions(),
	} {
		if err := w.ddl(ctx, q); err != nil {
			return fmt.Errorf("mysql: prepare %s: %w", w.schema, err)
		}
	}
	// Waits for applies that hold the fence row.
	if _, err := w.db.ExecContext(ctx, "INSERT INTO "+w.ident("_openetl_fence")+` (pipeline, generation) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE generation = IF(generation <= VALUES(generation), VALUES(generation), generation)`, w.cfg.Pipeline, gen); err != nil {
		return classify(fmt.Errorf("mysql: raise fence: %w", err))
	}
	var raised uint64
	if err := w.db.QueryRowContext(ctx, "SELECT generation FROM "+w.ident("_openetl_fence")+" WHERE pipeline = ?", w.cfg.Pipeline).Scan(&raised); err != nil {
		return classify(fmt.Errorf("mysql: raise fence: %w", err))
	}
	if raised != gen {
		return fmt.Errorf("%w: recovering as generation %d, and the fence is %d", destination.ErrFenced, gen, raised)
	}
	return nil
}

func (w *writer) Exists(ctx context.Context, table string) (bool, error) {
	var n int
	if err := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", w.schema, table).Scan(&n); err != nil {
		return false, classify(err)
	}
	return n > 0, nil
}

// labelled returns the database's tables whose names start with prefix,
// with their comments.
func (w *writer) labelled(ctx context.Context, prefix string) ([][2]string, error) {
	rows, err := w.db.QueryContext(ctx, `SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' AND LEFT(TABLE_NAME, ?) = ?`, w.schema, len(prefix), prefix)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out [][2]string
	for rows.Next() {
		var t [2]string
		if err := rows.Scan(&t[0], &t[1]); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// DropStale drops what earlier generations of the pipeline staged. None of
// it can be applied once HEAD's epoch is recovered: the fence stops their
// commits. Best effort, like dropStaging.
func (w *writer) DropStale(ctx context.Context, gen uint64) {
	labelled, err := w.labelled(ctx, "_openetl_stage_")
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
// abandoned. Callers hold the fence, so a zombie can't drop a read that
// continues.
func (w *writer) dropAbandonedReads(ctx context.Context, drop func(l sqlstage.ReadLabel, table string) bool) error {
	labelled, err := w.labelled(ctx, "_openetl_read_")
	if err != nil {
		return classify(fmt.Errorf("mysql: find abandoned reads: %w", err))
	}
	for _, t := range labelled {
		var l sqlstage.ReadLabel
		if json.Unmarshal([]byte(t[1]), &l) == nil && l.Pipeline == w.cfg.Pipeline && drop(l, t[0]) {
			if err := w.ddl(ctx, "DROP TABLE IF EXISTS "+w.ident(t[0])); err != nil {
				return fmt.Errorf("mysql: drop an abandoned read of %s: %w", l.Stream, err)
			}
		}
	}
	return nil
}

// DropRemovedReads drops the read tables of streams that no longer
// overwrite, once Recover has raised the fence. Best effort, like
// dropStaging.
func (w *writer) DropRemovedReads(ctx context.Context) {
	_ = w.dropAbandonedReads(ctx, func(l sqlstage.ReadLabel, _ string) bool {
		return w.streams[l.Stream].Mode != destination.Overwrite
	})
}

func (w *writer) Close(context.Context) error {
	return w.db.Close()
}
