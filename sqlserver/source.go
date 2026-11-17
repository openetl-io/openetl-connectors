package sqlserver

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl-connectors/keysplit"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type SourceConfig struct {
	// A sqlserver:// URL, or an ADO or ODBC connection string.
	DSN sdk.Secret `json:"dsn"`
	// Default "dbo".
	Schema string `json:"schema"`
	// The tables to offer as streams. Empty: every table in the schema.
	Tables []string `json:"tables"`
	// Rows per query on a table with a primary key; each chunk ends at a
	// resume point. Default 100000.
	ChunkRows int `json:"chunkRows"`
	// Approximate bytes of values per Arrow batch. Default 4 MiB.
	BatchBytes int `json:"batchBytes"`
	// How cdc reads changes: change_tracking (the default) or
	// change_data_capture.
	CDCMethod string `json:"cdcMethod"`
	// Rows per chunk of a cdc table's snapshot. Default 10000.
	SnapshotChunkRows int `json:"snapshotChunkRows"`
	// The most connections a read holds, as many as it has lanes up to this;
	// a cdc read takes one more. Default 4.
	MaxConnections int `json:"maxConnections"`
}

const (
	methodTracking = "change_tracking"
	methodCapture  = "change_data_capture"
)

// Source reads tables: a full refresh reads a table's rows, an incremental
// read the rows from a cursor column's floor on, and cdc reads its changes
// through change tracking or change data capture (cdc.go).
// A table with a primary key is read in key order, or (cursor, key) order, a
// chunk per query, so a resumed read yields exactly the rows after its last
// chunk, within its key range if it's read in splits (source_splits.go). A
// table without one is read in a single query.
type Source struct {
	cfg SourceConfig
	db  *sql.DB

	keysplit.Reads
}

var _ source.Source = (*Source)(nil)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{Schema: "dbo", ChunkRows: 100_000, BatchBytes: 4 << 20, CDCMethod: methodTracking, SnapshotChunkRows: 10_000, MaxConnections: 4}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("sqlserver: config: %w", err)
	}
	switch {
	case cfg.DSN.IsZero():
		return nil, errors.New("sqlserver: config needs a dsn")
	case cfg.ChunkRows <= 0 || cfg.BatchBytes <= 0 || cfg.SnapshotChunkRows <= 0 || cfg.MaxConnections <= 0:
		return nil, errors.New("sqlserver: chunkRows, batchBytes, snapshotChunkRows and maxConnections must be positive")
	case cfg.CDCMethod != methodTracking && cfg.CDCMethod != methodCapture:
		return nil, fmt.Errorf("sqlserver: cdcMethod is %s or %s, not %q", methodTracking, methodCapture, cfg.CDCMethod)
	}
	db, err := openDB(cfg.DSN.Reveal())
	if err != nil {
		return nil, fmt.Errorf("sqlserver: dsn: %w", err)
	}
	db.SetMaxIdleConns(cfg.MaxConnections + 1)
	return &Source{cfg: cfg, db: db}, nil
}

type table struct {
	name string
	cols []sourceColumn
	// Indexes into cols, in key order.
	key []int
	// Change tracking is on for it, and change data capture.
	tracked, captured bool
	view              bool
}

// keyCols are the table's key's columns, in key order.
func (t table) keyCols() []sourceColumn {
	out := make([]sourceColumn, len(t.key))
	for i, k := range t.key {
		out[i] = t.cols[k]
	}
	return out
}

func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	tables, err := s.tables(ctx, s.db)
	if err != nil {
		return source.Catalog{}, err
	}
	var c source.Catalog
	for _, t := range tables {
		b, err := newBatcher(memory.DefaultAllocator, t.cols, false)
		if err != nil {
			return source.Catalog{}, fmt.Errorf("sqlserver: %s: %w", t.name, err)
		}
		b.release()
		cs := source.CatalogStream{Name: t.name, SplitsByKey: len(t.key) > 0, Replay: source.Exact, SyncModes: []source.SyncMode{source.FullRefresh}}
		if slices.ContainsFunc(t.cols, func(c sourceColumn) bool { return cursorTypes[c.typ] }) {
			cs.SyncModes = append(cs.SyncModes, source.Incremental)
		}
		// A rowversion orders a row's versions: every write raises it.
		if i := slices.IndexFunc(t.cols, func(c sourceColumn) bool { return c.typ == "timestamp" }); i >= 0 {
			cs.CursorField = t.cols[i].name
		}
		cdc := len(t.key) > 0 && (s.cfg.CDCMethod == methodTracking && t.tracked || s.cfg.CDCMethod == methodCapture && t.captured)
		if cdc {
			// A row changed while its snapshot chunk is read, which change
			// data capture's job harvests seconds later, arrives in the
			// snapshot and again as its change; a chunk read again to an end
			// has the rows changed since too. Change tracking keeps each key's
			// latest change alone, so a read again brings each key changed
			// since with its row as it is then. The declaration is the
			// stream's, whichever mode reads it.
			cs.SyncModes, cs.Seq, cs.Replay = append(cs.SyncModes, source.CDC), seqType, source.Superset
		}
		cs.Schema = nullability(b.declared, t, cdc)
		for _, i := range t.key {
			cs.PrimaryKey = append(cs.PrimaryKey, t.cols[i].name)
		}
		c.Streams = append(c.Streams, cs)
	}
	return c, nil
}

// nullability is decl with the columns that never hold a null marked so:
// those NOT NULL in the table, except where a CDC read carries nulls, as a
// delete does in every column but its key. A view's are all nullable: what
// SQL Server notes of them goes stale as the tables under them change.
func nullability(decl *arrow.Schema, t table, cdc bool) *arrow.Schema {
	fields := slices.Clone(decl.Fields())
	for i, c := range t.cols {
		fields[i].Nullable = !c.notNull || c.computed || t.view || cdc && !slices.Contains(t.key, i)
	}
	return arrow.NewSchema(fields, nil)
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// tables lists the configured tables, in name order.
func (s *Source) tables(ctx context.Context, q queryer) ([]table, error) {
	rows, err := q.QueryContext(ctx, `SELECT o.name, c.name,
			CASE WHEN ty.name IS NULL OR ty.is_user_defined = 1 AND ty.is_assembly_type = 0 THEN TYPE_NAME(c.system_type_id) ELSE ty.name END,
			c.max_length, c.precision, c.scale, c.is_nullable, c.is_computed, coalesce(c.collation_name, ''), coalesce(ic.key_ordinal, 0),
			CASE WHEN ct.object_id IS NULL THEN 0 ELSE 1 END, coalesce(t.is_tracked_by_cdc, 0), CASE WHEN o.type = 'V' THEN 1 ELSE 0 END
		FROM sys.objects o
		JOIN sys.schemas s ON s.schema_id = o.schema_id
		JOIN sys.columns c ON c.object_id = o.object_id
		-- A view isn't in sys.tables.
		LEFT JOIN sys.tables t ON t.object_id = o.object_id
		-- An alias type the user has no permission on isn't listed.
		LEFT JOIN sys.types ty ON ty.user_type_id = c.user_type_id
		LEFT JOIN sys.indexes i ON i.object_id = o.object_id AND i.is_primary_key = 1
		LEFT JOIN sys.index_columns ic ON ic.object_id = o.object_id AND ic.index_id = i.index_id AND ic.column_id = c.column_id AND ic.key_ordinal > 0
		LEFT JOIN sys.change_tracking_tables ct ON ct.object_id = o.object_id
		WHERE s.name = @p1 AND o.type IN ('U', 'V') AND o.is_ms_shipped = 0
		ORDER BY o.name, c.column_id`, s.cfg.Schema)
	if err != nil {
		return nil, classify(fmt.Errorf("sqlserver: list tables: %w", err))
	}
	defer func() { _ = rows.Close() }()
	var out []table
	var keyPos []int
	finish := func() {
		if len(out) == 0 {
			return
		}
		t := &out[len(out)-1]
		for pos := 1; ; pos++ {
			i := slices.Index(keyPos, pos)
			if i < 0 {
				break
			}
			t.key = append(t.key, i)
		}
	}
	for rows.Next() {
		var name string
		var c sourceColumn
		var nullable, tracked, captured, view bool
		var pos int
		if err := rows.Scan(&name, &c.name, &c.typ, &c.maxLength, &c.precision, &c.scale, &nullable, &c.computed, &c.collation, &pos, &tracked, &captured, &view); err != nil {
			return nil, fmt.Errorf("sqlserver: list tables: %w", err)
		}
		c.notNull = !nullable
		if len(s.cfg.Tables) > 0 && !slices.Contains(s.cfg.Tables, name) {
			continue
		}
		if len(out) == 0 || out[len(out)-1].name != name {
			finish()
			out = append(out, table{name: name, tracked: tracked, captured: captured, view: view})
			keyPos = keyPos[:0]
		}
		t := &out[len(out)-1]
		t.cols = append(t.cols, c)
		keyPos = append(keyPos, pos)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(fmt.Errorf("sqlserver: list tables: %w", err))
	}
	finish()
	for _, want := range s.cfg.Tables {
		if !slices.ContainsFunc(out, func(t table) bool { return t.name == want }) {
			return nil, configInvalid("sqlserver: no table or view %q in schema %q", want, s.cfg.Schema)
		}
	}
	return out, nil
}

type sourceState struct {
	// The primary key of the last row read, spelled as text; in an
	// incremental read, its cursor value first.
	After []string `json:"after,omitempty"`
	// Set once a table without a primary key has been read.
	Done bool `json:"done,omitempty"`
	// An incremental read's cursor floor, inclusive but for a rowversion's,
	// and the highest value
	// it reads, set when it starts, as text. A completed read's Until is the
	// next read's Since.
	Since *string `json:"since,omitempty"`
	Until *string `json:"until,omitempty"`
	// A split's key range.
	keyRange
}

// The column types an incremental read can order by.
var cursorTypes = map[string]bool{
	"tinyint": true, "smallint": true, "int": true, "bigint": true, "decimal": true, "numeric": true, "money": true, "smallmoney": true,
	"date": true, "datetime": true, "smalldatetime": true, "datetime2": true, "datetimeoffset": true,
	"char": true, "varchar": true, "nchar": true, "nvarchar": true, "timestamp": true, "rowversion": true,
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	limit := min(max(req.Lanes, 1), s.cfg.MaxConnections)
	tables, err := s.tables(ctx, s.db)
	if err != nil {
		return err
	}
	var cdc, rest []source.ConfiguredStream
	for _, cs := range req.Streams {
		switch {
		case !slices.ContainsFunc(tables, func(t table) bool { return t.name == cs.Name }):
			return fmt.Errorf("sqlserver: unknown stream %q", cs.Name)
		case cs.SyncMode == source.CDC:
			cdc = append(cdc, cs)
		default:
			rest = append(rest, cs)
		}
	}
	readRest := func(ctx context.Context, lanes int) error {
		return source.ReadSplits(ctx, source.ReadRequest{Streams: rest, Lanes: lanes}, out, func(ctx context.Context, cs source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
			return s.readSplit(ctx, tables, cs, sp, e)
		})
	}
	readCDC := func(ctx context.Context) error { return s.readCDC(ctx, req, cdc, tables, out) }
	if len(cdc) == 0 {
		return readRest(ctx, limit)
	}
	if len(rest) == 0 {
		return readCDC(ctx)
	}
	return keysplit.Alongside(ctx, func(ctx context.Context) error { return readRest(ctx, limit) }, readCDC)
}

// readSplit reads one split of a table read in full or incrementally.
func (s *Source) readSplit(ctx context.Context, tables []table, cs source.ConfiguredStream, sp source.Split, out source.SplitEmitter) error {
	var st, until sourceState
	if err := unmarshalState(cs.Name, sp.State, &st); err != nil {
		return err
	}
	if sp.Until == nil && sp.ID == source.WholeStream {
		sp.Until = cs.Until
	}
	if err := unmarshalState(cs.Name, sp.Until, &until); err != nil {
		return err
	}
	r := &tableRead{s: s, t: tables[slices.IndexFunc(tables, func(t table) bool { return t.name == cs.Name })], id: sp.ID, out: out, until: sp.Until, stop: until}
	if cs.SyncMode == source.Incremental {
		return r.incremental(ctx, cs.Cursor, st)
	}
	return r.full(ctx, st)
}

func unmarshalState(stream string, b []byte, v any) error {
	if b == nil {
		return nil
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("sqlserver: %s state: %w", stream, err)
	}
	return nil
}

// tableRead is one split's read of a table in full or incrementally.
type tableRead struct {
	s   *Source
	t   table
	id  string
	out source.SplitEmitter
	// Where the read ends, as the request gave it, and parsed; nil to read
	// to the end.
	until []byte
	stop  sourceState
}

// args collects a query's parameters.
type args []any

func (a *args) add(v any) string {
	*a = append(*a, v)
	return param(len(*a))
}

// afterKey says a row's key, (refs), is after the position after, in key
// order: (k1 > a1) OR (k1 = a1 AND k2 > a2)..., with k1 >= a1 first, for
// seeks.
func afterKey(cols []sourceColumn, refs, after []string, a *args) string {
	return compareKey(cols, refs, after, a, ">", false)
}

// upToKey says a row's key is at or before the position.
func upToKey(cols []sourceColumn, refs, upTo []string, a *args) string {
	return compareKey(cols, refs, upTo, a, "<", true)
}

func compareKey(cols []sourceColumn, refs, at []string, a *args, op string, orEqual bool) string {
	vals := make([]string, len(at))
	for i, v := range at {
		vals[i] = cols[i].fromText(a.add(v))
	}
	var ors []string
	for i := range at {
		var ands []string
		for j := range i {
			ands = append(ands, refs[j]+" = "+vals[j])
		}
		ands = append(ands, refs[i]+" "+op+" "+vals[i])
		ors = append(ors, "("+strings.Join(ands, " AND ")+")")
	}
	if orEqual {
		eq := make([]string, len(at))
		for i := range at {
			eq[i] = refs[i] + " = " + vals[i]
		}
		ors = append(ors, "("+strings.Join(eq, " AND ")+")")
	}
	return "(" + refs[0] + " " + op + "= " + vals[0] + " AND (" + strings.Join(ors, " OR ") + "))"
}

// list is the table's columns as a query selects them, and refs each
// column's reference, quoted.
func selectList(cols []sourceColumn, alias string) string {
	exprs := make([]string, len(cols))
	for i, c := range cols {
		exprs[i] = c.selectExpr(alias)
	}
	return strings.Join(exprs, ", ")
}

func refs(cols []sourceColumn, alias string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = quote(c.name)
		if alias != "" {
			out[i] = alias + "." + out[i]
		}
	}
	return out
}

func (r *tableRead) full(ctx context.Context, st sourceState) error {
	b, err := newBatcher(memory.DefaultAllocator, r.t.cols, false)
	if err != nil {
		return fmt.Errorf("sqlserver: %s: %w", r.t.name, err)
	}
	defer b.release()
	ident := qualified(r.s.cfg.Schema, r.t.name)
	if len(r.t.key) == 0 {
		if !st.Done {
			if _, _, err := r.s.query(ctx, r.s.db, b, r.t.name, "SELECT "+selectList(r.t.cols, "")+" FROM "+ident, nil, 0, r.out); err != nil {
				return err
			}
			if err := r.resume(sourceState{Done: true}); err != nil {
				return err
			}
		}
		return r.complete(nil)
	}
	keys := r.t.keyCols()
	keyRefs := refs(keys, "")
	keyText := make([]string, len(keys))
	for i, k := range keys {
		keyText[i] = k.textOf(keyRefs[i])
	}
	head := "SELECT TOP (" + strconv.Itoa(r.s.cfg.ChunkRows) + ") " + selectList(r.t.cols, "") + ", " + strings.Join(keyText, ", ") + " FROM " + ident
	tail := " ORDER BY " + strings.Join(keyRefs, ", ")
	if st.After != nil && len(st.After) != len(keys) {
		return fmt.Errorf("sqlserver: %s state has %d key columns, the table has %d", r.t.name, len(st.After), len(keys))
	}
	st = sourceState{After: st.After, keyRange: st.keyRange}
	reading := r.s.Start(r.t.name, r.id, true)
	defer r.s.End(r.t.name, r.id)
	for {
		var a args
		where := st.where(keys[0], keyRefs[0], &a)
		if st.After != nil {
			where = append(where, afterKey(keys, keyRefs, st.After, &a))
		}
		if r.until != nil {
			if r.stop.After == nil {
				return r.endAt()
			}
			where = append(where, upToKey(keys, keyRefs, r.stop.After, &a))
		}
		q := head
		if len(where) > 0 {
			q += " WHERE " + strings.Join(where, " AND ")
		}
		n, last, err := r.s.query(ctx, r.s.db, b, r.t.name, q+tail, a, len(keys), r.out)
		if err != nil {
			return err
		}
		if n > 0 {
			st.After = last
		}
		ended := n < r.s.cfg.ChunkRows
		if r.until != nil && ended {
			return r.endAt()
		}
		added, err := r.s.resplit(ctx, r.t, reading, &st, ended, st.position(), false)
		if err != nil {
			return err
		}
		if err := r.resume(st, added...); err != nil {
			return err
		}
		if ended {
			return r.complete(nil)
		}
	}
}

// endAt ends a read that stopped at its until: a resume point and a
// completion at it.
func (r *tableRead) endAt() error {
	if err := r.out.ResumePoint(r.until); err != nil {
		return err
	}
	return r.out.Complete(r.until)
}

func (r *tableRead) resume(st sourceState, added ...source.Split) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return r.out.ResumePoint(b, added...)
}

func (r *tableRead) complete(next *sourceState) error {
	if next == nil {
		return r.out.Complete(nil)
	}
	b, err := json.Marshal(next)
	if err != nil {
		return err
	}
	return r.out.Complete(b)
}

// incremental reads the rows whose cursor is from the state's floor up to
// the cursor's highest value when the read started, in (cursor, key) order.
// It completes with that value as the next read's floor, so the rows that
// hold it are read again then. Rows with a NULL cursor aren't read. A
// rowversion cursor stops below the oldest active transaction's, whose rows
// may commit later with values below the highest read.
func (r *tableRead) incremental(ctx context.Context, cursor string, st sourceState) error {
	ci, err := cursorColumn(r.t, cursor)
	if err != nil {
		return err
	}
	b, err := newBatcher(memory.DefaultAllocator, r.t.cols, false)
	if err != nil {
		return fmt.Errorf("sqlserver: %s: %w", r.t.name, err)
	}
	defer b.release()
	ident := qualified(r.s.cfg.Schema, r.t.name)
	cc := r.t.cols[ci]
	cref := quote(cc.name)
	if r.until != nil && r.stop.Until != nil {
		st.Until = r.stop.Until
	}
	if st.Until == nil {
		until, err := r.s.cursorTop(ctx, r.s.db, r.t, cc)
		if err != nil {
			return err
		}
		if until == nil {
			return r.complete(&sourceState{Since: st.Since})
		}
		st = sourceState{Since: st.Since, Until: until}
	}
	bounds := func(a *args) string {
		w := cref + " <= " + cc.fromText(a.add(*st.Until))
		if st.Since != nil {
			// A rowversion is unique, and every value up to Since was read.
			op := " >= "
			if cc.typ == "timestamp" {
				op = " > "
			}
			w += " AND " + cref + op + cc.fromText(a.add(*st.Since))
		}
		return w
	}
	if len(r.t.key) == 0 {
		if !st.Done {
			var a args
			if _, _, err := r.s.query(ctx, r.s.db, b, r.t.name, "SELECT "+selectList(r.t.cols, "")+" FROM "+ident+" WHERE "+bounds(&a), a, 0, r.out); err != nil {
				return err
			}
			st.Done = true
			if err := r.resume(st); err != nil {
				return err
			}
		}
		if r.until != nil {
			return r.endAt()
		}
		return r.complete(&sourceState{Since: st.Until})
	}
	order := []sourceColumn{cc}
	for _, k := range r.t.key {
		if k != ci {
			order = append(order, r.t.cols[k])
		}
	}
	orderRefs := refs(order, "")
	text := make([]string, len(order))
	for i, c := range order {
		text[i] = c.textOf(orderRefs[i])
	}
	head := "SELECT TOP (" + strconv.Itoa(r.s.cfg.ChunkRows) + ") " + selectList(r.t.cols, "") + ", " + strings.Join(text, ", ") + " FROM " + ident + " WHERE "
	tail := " ORDER BY " + strings.Join(orderRefs, ", ")
	if st.After != nil && len(st.After) != len(order) {
		return fmt.Errorf("sqlserver: %s state has %d cursor and key columns, the read has %d", r.t.name, len(st.After), len(order))
	}
	first := r.t.cols[r.t.key[0]]
	reading := r.s.Start(r.t.name, r.id, true)
	defer r.s.End(r.t.name, r.id)
	for {
		var a args
		where := []string{bounds(&a)}
		where = append(where, st.where(first, quote(first.name), &a)...)
		if st.After != nil {
			where = append(where, afterKey(order, orderRefs, st.After, &a))
		}
		if r.until != nil {
			if r.stop.After == nil {
				return r.endAt()
			}
			where = append(where, upToKey(order, orderRefs, r.stop.After, &a))
		}
		n, last, err := r.s.query(ctx, r.s.db, b, r.t.name, head+strings.Join(where, " AND ")+tail, a, len(order), r.out)
		if err != nil {
			return err
		}
		if n > 0 {
			st.After = last
		}
		ended := n < r.s.cfg.ChunkRows
		if r.until != nil && ended {
			return r.endAt()
		}
		// The position follows the key only where the cursor is its first
		// column.
		var pos *string
		if r.t.key[0] == ci {
			pos = st.position()
		}
		added, err := r.s.resplit(ctx, r.t, reading, &st, ended, pos, true)
		if err != nil {
			return err
		}
		if err := r.resume(st, added...); err != nil {
			return err
		}
		if ended {
			return r.complete(&sourceState{Since: st.Until})
		}
	}
}

// cursorTop is the highest value an incremental read of the cursor column c
// reads, as text; nil if it has none.
func (s *Source) cursorTop(ctx context.Context, q queryer, t table, c sourceColumn) (*string, error) {
	top := "max(" + quote(c.name) + ")"
	if c.typ == "timestamp" || c.typ == "rowversion" {
		// Rows below MIN_ACTIVE_ROWVERSION() are committed; those at or above
		// it may still be written by transactions that commit later.
		active := "CONVERT(binary(8), CONVERT(bigint, MIN_ACTIVE_ROWVERSION()) - 1)"
		top = "(SELECT max(v) FROM (VALUES (" + top + "), (" + active + ")) AS x(v) WHERE v <= " + active + ")"
	}
	var until *string
	if err := q.QueryRowContext(ctx, "SELECT "+c.textOf(top)+" FROM "+qualified(s.cfg.Schema, t.name)).Scan(&until); err != nil {
		return nil, classify(fmt.Errorf("sqlserver: read %s: %w", t.name, err))
	}
	return until, nil
}

// cursorColumn is the index of a table's column that an incremental read can
// order by.
func cursorColumn(t table, cursor string) (int, error) {
	ci := slices.IndexFunc(t.cols, func(c sourceColumn) bool { return c.name == cursor })
	switch {
	case cursor == "":
		return 0, configInvalid("sqlserver: %s is read incrementally, which needs a cursor column", t.name)
	case ci < 0:
		return 0, configInvalid("sqlserver: %s has no column %q for a cursor", t.name, cursor)
	case !cursorTypes[t.cols[ci].typ] || t.cols[ci].maxLength < 0:
		return 0, configInvalid("sqlserver: %s.%s can't be a cursor: that takes an integer, decimal, money, date and time, rowversion or character column, but not (max)", t.name, cursor)
	}
	return ci, nil
}

// query runs q and emits its rows. The last keyCols result columns are the
// key as text; query returns the last row's. Every row is emitted before it
// returns.
func (s *Source) query(ctx context.Context, db queryer, b *batcher, stream, q string, a args, keyCols int, out source.SplitEmitter) (int, []string, error) {
	rows, err := db.QueryContext(ctx, q, a...)
	if err != nil {
		return 0, nil, classify(fmt.Errorf("sqlserver: read %s: %w", stream, err))
	}
	defer func() { _ = rows.Close() }()
	n := len(b.cols)
	vals := make([]any, n+keyCols)
	ptrs := make([]any, len(vals))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	count := 0
	var last []string
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return count, last, classify(fmt.Errorf("sqlserver: read %s: %w", stream, err))
		}
		if err := b.addRow(vals[:n]); err != nil {
			return count, last, err
		}
		count++
		if b.bytes >= s.cfg.BatchBytes {
			if err := b.flush(out); err != nil {
				return count, last, err
			}
		}
		if keyCols > 0 {
			last = last[:0]
			for _, v := range vals[n:] {
				t, _ := text(v)
				last = append(last, t)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return count, last, classify(fmt.Errorf("sqlserver: read %s: %w", stream, err))
	}
	last = slices.Clone(last)
	return count, last, b.flush(out)
}

// batcher builds one table's Arrow batches, with a changelog's control
// columns if it's a cdc stream's.
type batcher struct {
	mem memory.Allocator
	// The batches', and the catalog's.
	schema, declared *arrow.Schema
	cols             []*colBuilder
	changes          bool
	op               *array.StringBuilder
	seq              *array.Decimal128Builder
	unch             *array.ListBuilder
	// The op and seq addRow gives a changelog's rows.
	rowOp  string
	rowSeq decimal128.Num
	rows   int
	bytes  int
}

func newBatcher(mem memory.Allocator, cols []sourceColumn, changes bool) (*batcher, error) {
	b := &batcher{mem: mem, changes: changes}
	fields := make([]arrow.Field, len(cols))
	declared := make([]arrow.Field, len(cols))
	for i, c := range cols {
		cb, err := newColBuilder(mem, c)
		if err != nil {
			b.release()
			return nil, err
		}
		b.cols = append(b.cols, cb)
		fields[i] = arrow.Field{Name: c.name, Type: cb.b.Type(), Nullable: true}
		declared[i] = fields[i]
		if cb.decl != nil {
			declared[i].Type = cb.decl
		}
	}
	b.declared = arrow.NewSchema(declared, nil)
	if changes {
		fields = append(fields, source.OpField(), source.SeqField(seqType), source.UnchangedField())
		b.op = array.NewStringBuilder(mem)
		b.seq = array.NewDecimal128Builder(mem, seqType)
		b.unch = array.NewListBuilder(mem, arrow.BinaryTypes.String)
	}
	b.schema = arrow.NewSchema(fields, nil)
	return b, nil
}

func (b *batcher) addRow(vals []any) error {
	return b.addChange(vals, b.rowOp, b.rowSeq, nil)
}

// addChange adds a changelog's row: its op, seq and the columns it didn't
// send.
func (b *batcher) addChange(vals []any, op string, seq decimal128.Num, unchanged []string) error {
	if err := b.add(vals); err != nil {
		return err
	}
	if b.changes {
		b.op.Append(op)
		b.seq.Append(seq)
		if len(unchanged) == 0 {
			b.unch.AppendNull()
		} else {
			b.unch.Append(true)
			for _, u := range unchanged {
				b.unch.ValueBuilder().(*array.StringBuilder).Append(u)
			}
		}
	}
	b.rows++
	return nil
}

func (b *batcher) add(vals []any) error {
	for i, c := range b.cols {
		v := vals[i]
		if v == nil {
			c.b.AppendNull()
			continue
		}
		if err := c.add(v); err != nil {
			return fmt.Errorf("sqlserver: column %q: %w", b.schema.Field(i).Name, err)
		}
		switch x := v.(type) {
		case string:
			b.bytes += len(x)
		case []byte:
			b.bytes += len(x)
		default:
			b.bytes += 8
		}
	}
	return nil
}

// flush sends the rows built so far.
func (b *batcher) flush(out source.SplitEmitter) error {
	if b.rows == 0 {
		return nil
	}
	arrs := make([]arrow.Array, 0, b.schema.NumFields())
	for _, c := range b.cols {
		arrs = append(arrs, c.b.NewArray())
	}
	if b.changes {
		arrs = append(arrs, b.op.NewArray(), b.seq.NewArray(), b.unch.NewArray())
	}
	rec := array.NewRecordBatch(b.schema, arrs, int64(b.rows))
	for _, a := range arrs {
		a.Release()
	}
	b.rows, b.bytes = 0, 0
	if err := out.Records(rec); err != nil {
		rec.Release()
		return err
	}
	return nil
}

func (b *batcher) release() {
	for _, c := range b.cols {
		c.b.Release()
	}
	if b.changes {
		b.op.Release()
		b.seq.Release()
		b.unch.Release()
	}
}

// streamEmitter is a whole stream's emitter as a SplitEmitter, for the
// batcher.
func streamEmitter(out source.Emitter, stream string) source.SplitEmitter {
	return source.WholeSplit(out, stream, source.WholeStream)
}
