package mysql

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-sql-driver/mysql"
	"golang.org/x/text/encoding"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type SourceConfig struct {
	DSN sdk.Secret `json:"dsn"`
	// Default: the DSN's.
	Database string `json:"database"`
	// Empty: every table in the database.
	Tables []string `json:"tables"`
	// Rows per query on a table with a primary key; each chunk ends at a
	// resume point. Default 100000.
	ChunkRows int `json:"chunkRows"`
	// Approximate bytes of values per Arrow batch. Default 4 MiB.
	BatchBytes int `json:"batchBytes"`
	// Rows per chunk of a cdc table's snapshot, held in memory until the
	// binlog passes it. Default 10000.
	SnapshotChunkRows int `json:"snapshotChunkRows"`
	// The most connections a read holds to read tables; a cdc read's binlog
	// connection is one more. Default 4.
	MaxConnections int `json:"maxConnections"`
	// The server ID the binlog connection takes, unique among the server's
	// replicas. 0: one derived from the pipeline's name.
	ServerID uint32 `json:"serverId"`
	// Nil: as the DSN says.
	TLS *TLSConfig `json:"tls"`
}

// Source reads a database's tables: a full refresh reads a table's rows, an
// incremental read the rows from a cursor column's floor on, and cdc its
// changes from the binlog (cdc.go). A table with a primary key is read in
// key order, or (cursor, key) order, a chunk per query, so a resumed read
// yields exactly the rows after its last chunk, within its key range if it's
// read in splits (source_splits.go). A table without one, or whose key has a
// column that doesn't order as it compares, is read in a single query.
type Source struct {
	cfg  SourceConfig
	conn *mysql.Config

	mu sync.Mutex
	// The keyed splits being read, which Resplit splits.
	reading map[readingKey]*readingSplit
}

var _ source.Source = (*Source)(nil)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{ChunkRows: 100_000, BatchBytes: 4 << 20, SnapshotChunkRows: 10_000, MaxConnections: 4}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("mysql: config: %w", err)
	}
	switch {
	case cfg.DSN.IsZero():
		return nil, errors.New("mysql: config needs a dsn")
	case cfg.ChunkRows <= 0 || cfg.BatchBytes <= 0 || cfg.SnapshotChunkRows <= 0 || cfg.MaxConnections <= 0:
		return nil, errors.New("mysql: chunkRows, batchBytes, snapshotChunkRows and maxConnections must be positive")
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
		return nil, errors.New("mysql: config needs a database, in the dsn or on its own")
	}
	return &Source{cfg: cfg, conn: c}, nil
}

type table struct {
	name string
	cols []sourceColumn
	// Indexes into cols, in key order.
	key []int
}

type sourceColumn struct {
	name string
	// information_schema's DATA_TYPE and COLUMN_TYPE, in lower case.
	dataType, columnType string
	kind                 kind
	notNull              bool
	// The text's character set, and its decoder to UTF-8 for the binlog's
	// values: nil for UTF-8.
	charset string
	decoder *encoding.Decoder
	// A BINARY(n)'s n.
	octets int
	// A DECIMAL's, or a BIT's bits.
	precision, scale int
	// MariaDB's JSON: text a json_valid constraint checks.
	json bool
}

func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	db, srv, err := open(ctx, s.conn, 1)
	if err != nil {
		return source.Catalog{}, err
	}
	defer func() { _ = db.Close() }()
	tables, err := s.tables(ctx, db, srv)
	if err != nil {
		return source.Catalog{}, err
	}
	var c source.Catalog
	for _, t := range tables {
		cs := source.CatalogStream{Name: t.name, SplitsByKey: splittable(t), Replay: source.Exact, SyncModes: []source.SyncMode{source.FullRefresh}}
		if slices.ContainsFunc(t.cols, func(c sourceColumn) bool { return cursorKinds[c.kind] }) {
			cs.SyncModes = append(cs.SyncModes, source.Incremental)
		}
		if chunked(t) {
			cs.SyncModes, cs.Seq = append(cs.SyncModes, source.CDC), seqType
		}
		if cs.Schema, err = declaredSchema(t); err != nil {
			return source.Catalog{}, fmt.Errorf("mysql: %s: %w", t.name, err)
		}
		for _, i := range t.key {
			cs.PrimaryKey = append(cs.PrimaryKey, t.cols[i].name)
		}
		c.Streams = append(c.Streams, cs)
	}
	return c, nil
}

// declaredSchema is the table's columns' logical types. Dates and date-times
// are nullable too where NOT NULL, since a zero date is read as null.
func declaredSchema(t table) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(t.cols))
	for i, c := range t.cols {
		sent, declared, err := arrowTypes(c)
		if err != nil {
			return nil, err
		}
		zero := c.kind == kDate || c.kind == kDatetime || c.kind == kTimestamp
		if declared == nil {
			declared = sent
		}
		fields[i] = arrow.Field{Name: c.name, Type: declared, Nullable: !c.notNull || zero}
	}
	return arrow.NewSchema(fields, nil), nil
}

// tables lists the configured tables, in name order. Virtual generated
// columns, which the binlog doesn't carry, are left out.
func (s *Source) tables(ctx context.Context, db *sql.DB, srv server) ([]table, error) {
	out, err := s.listTables(ctx, db, srv, "")
	if err != nil {
		return nil, err
	}
	for _, want := range s.cfg.Tables {
		if !slices.ContainsFunc(out, func(t table) bool { return t.name == want }) {
			return nil, configInvalid("mysql: no table %q in database %q", want, s.cfg.Database)
		}
	}
	return out, nil
}

// listTables is tables, or the one named only if it's offered, without
// requiring the configured tables to exist.
func (s *Source) listTables(ctx context.Context, db *sql.DB, srv server, only string) ([]table, error) {
	rows, err := db.QueryContext(ctx, `SELECT c.TABLE_NAME, c.COLUMN_NAME, LOWER(c.DATA_TYPE), LOWER(c.COLUMN_TYPE), c.IS_NULLABLE = 'NO',
			coalesce(c.CHARACTER_SET_NAME, ''), coalesce(c.CHARACTER_OCTET_LENGTH, 0), coalesce(c.NUMERIC_PRECISION, 0), coalesce(c.NUMERIC_SCALE, 0),
			coalesce(k.ORDINAL_POSITION, 0)
		FROM information_schema.COLUMNS c
		JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_TYPE = 'BASE TABLE'
		LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON k.TABLE_SCHEMA = c.TABLE_SCHEMA AND k.TABLE_NAME = c.TABLE_NAME
			AND k.COLUMN_NAME = c.COLUMN_NAME AND k.CONSTRAINT_NAME = 'PRIMARY'
		WHERE c.TABLE_SCHEMA = ? AND c.EXTRA NOT LIKE '%VIRTUAL%' AND (? = '' OR c.TABLE_NAME = ?)
		ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION`, s.cfg.Database, only, only)
	if err != nil {
		return nil, classify(fmt.Errorf("mysql: list tables: %w", err))
	}
	defer func() { _ = rows.Close() }()
	var out []table
	var keyPos [][]int
	for rows.Next() {
		var name string
		var c sourceColumn
		var octets, precision, scale int64
		var pos int
		if err := rows.Scan(&name, &c.name, &c.dataType, &c.columnType, &c.notNull, &c.charset, &octets, &precision, &scale, &pos); err != nil {
			return nil, classify(fmt.Errorf("mysql: list tables: %w", err))
		}
		if len(s.cfg.Tables) > 0 && !slices.Contains(s.cfg.Tables, name) {
			continue
		}
		c.octets, c.precision, c.scale = int(min(octets, 1<<31)), int(min(precision, 255)), int(min(scale, 255))
		if len(out) == 0 || out[len(out)-1].name != name {
			out = append(out, table{name: name})
			keyPos = append(keyPos, nil)
		}
		t := &out[len(out)-1]
		if pos > 0 {
			keyPos[len(keyPos)-1] = append(keyPos[len(keyPos)-1], pos<<16|len(t.cols))
		}
		t.cols = append(t.cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(fmt.Errorf("mysql: list tables: %w", err))
	}
	if srv.mariadb {
		if err := s.markJSON(ctx, db, out); err != nil {
			return nil, err
		}
	}
	for i := range out {
		t := &out[i]
		slices.Sort(keyPos[i])
		for _, p := range keyPos[i] {
			t.key = append(t.key, p&0xffff)
		}
		for j := range t.cols {
			c := &t.cols[j]
			c.kind = kindOf(*c)
			if c.decoder, err = charsetDecoder(c.charset); err != nil {
				return nil, fmt.Errorf("mysql: %s.%s has text in %w, which the source can't read", t.name, c.name, err)
			}
		}
	}
	return out, nil
}

// markJSON marks MariaDB's JSON columns, which are text a json_valid
// constraint checks.
func (s *Source) markJSON(ctx context.Context, db *sql.DB, tables []table) error {
	rows, err := db.QueryContext(ctx, `SELECT TABLE_NAME, CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = ? AND CHECK_CLAUSE LIKE 'json\_valid(%'`, s.cfg.Database)
	if err != nil {
		return classify(fmt.Errorf("mysql: list tables: %w", err))
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, clause string
		if err := rows.Scan(&name, &clause); err != nil {
			return classify(err)
		}
		col := strings.TrimSuffix(strings.TrimPrefix(clause, "json_valid(`"), "`)")
		for i := range tables {
			for j := range tables[i].cols {
				if c := &tables[i].cols[j]; tables[i].name == name && c.name == col && c.dataType == "longtext" {
					c.json = true
				}
			}
		}
	}
	return classify(rows.Err())
}

func findTable(ts []table, name string) (table, bool) {
	i := slices.IndexFunc(ts, func(t table) bool { return t.name == name })
	if i < 0 {
		return table{}, false
	}
	return ts[i], true
}

// orderable kinds compare with a parameter as they order, so a read can
// resume after a key or cursor of them.
var orderable = map[kind]bool{
	kInt16: true, kInt32: true, kInt64: true, kUint64: true, kDecimal: true, kString: true, kBinary: true,
	kDate: true, kDatetime: true, kTimestamp: true, kTime: true, kYear: true, kUUID: true,
}

// The column kinds an incremental read can order by.
var cursorKinds = map[kind]bool{
	kInt16: true, kInt32: true, kInt64: true, kUint64: true, kDecimal: true, kString: true,
	kDate: true, kDatetime: true, kTimestamp: true, kYear: true,
}

// chunked says the table's key orders as it compares, so it's read a chunk
// at a time.
func chunked(t table) bool {
	return len(t.key) > 0 && !slices.ContainsFunc(t.key, func(k int) bool { return !orderable[t.cols[k].kind] })
}

type sourceState struct {
	// The primary key of the last row read, spelled as keyText spells it; in
	// an incremental read, its cursor value first.
	After []string `json:"after,omitempty"`
	// Set once a table read in one query has been read.
	Done bool `json:"done,omitempty"`
	// An incremental read's cursor floor, inclusive, and the highest value
	// it reads, set when it starts. A completed read's Until is the next
	// read's Since.
	Since *string `json:"since,omitempty"`
	Until *string `json:"until,omitempty"`
	// A split's key range.
	keyRange
}

// keyText spells a key's or cursor's value for a state: binary as hex.
func keyText(c sourceColumn, v any) string {
	if b, ok := v.([]byte); ok && c.kind == kBinary {
		return hex.EncodeToString(b)
	}
	t, _ := text(v)
	return string(t)
}

// param is a placeholder for a value keyText spelled, and the value, typed
// so that it compares with the column as the column orders.
func param(c sourceColumn, s string) (string, any, error) {
	switch c.kind {
	case kInt16, kInt32, kInt64, kYear:
		v, err := strconv.ParseInt(s, 10, 64)
		return "?", v, err
	case kUint64:
		v, err := strconv.ParseUint(s, 10, 64)
		return "?", v, err
	case kDecimal:
		return fmt.Sprintf("CAST(? AS DECIMAL(%d, %d))", c.precision, c.scale), s, nil
	case kBinary:
		v, err := hex.DecodeString(s)
		return "?", v, err
	}
	return "?", s, nil
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	limit := min(max(req.Lanes, 1), s.cfg.MaxConnections)
	db, srv, err := open(ctx, s.conn, limit)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	conns := &connPool{db: db, held: make(chan struct{}, limit)}
	tables, err := s.tables(ctx, db, srv)
	if err != nil {
		return err
	}
	var cdc, rest []source.ConfiguredStream
	for _, cs := range req.Streams {
		switch _, ok := findTable(tables, cs.Name); {
		case !ok:
			return fmt.Errorf("mysql: unknown stream %q", cs.Name)
		case cs.SyncMode == source.CDC:
			cdc = append(cdc, cs)
		default:
			rest = append(rest, cs)
		}
	}
	readRest := func(ctx context.Context, lanes int) error {
		return source.ReadSplits(ctx, source.ReadRequest{Streams: rest, Lanes: lanes}, out, func(ctx context.Context, cs source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
			until := sp.Until
			if sp.ID == source.WholeStream && len(cs.Splits) == 0 {
				until = cs.Until
			}
			if req.Continuous {
				until = nil
			}
			return s.readSplit(ctx, conns, tables, cs, sp, until, e)
		})
	}
	if len(cdc) == 0 {
		return readRest(ctx, limit)
	}
	// With one connection the binlog waits for the tables, since it never
	// ends.
	if limit == 1 {
		if err := readRest(ctx, 1); err != nil {
			return err
		}
		return s.readCDC(ctx, conns, srv, req, cdc, tables, out)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := readRest(ctx, limit-1); err != nil {
			cancel(err)
		}
	})
	if err := s.readCDC(ctx, conns, srv, req, cdc, tables, out); err != nil {
		cancel(err)
	}
	wg.Wait()
	return context.Cause(ctx)
}

// readSplit reads one split of a table read in full or incrementally, on a
// connection of its own; up to the resume point whose state is until, if
// it isn't nil.
func (s *Source) readSplit(ctx context.Context, conns *connPool, tables []table, cs source.ConfiguredStream, sp source.Split, until []byte, out source.SplitEmitter) error {
	var st sourceState
	if sp.State != nil {
		if err := json.Unmarshal(sp.State, &st); err != nil {
			return fmt.Errorf("mysql: %s state: %w", cs.Name, err)
		}
	}
	conn, err := conns.get(ctx)
	if err != nil {
		return err
	}
	defer conns.put(conn)
	t, _ := findTable(tables, cs.Name)
	b, err := newBatcher(memory.DefaultAllocator, t.cols, false)
	if err != nil {
		return fmt.Errorf("mysql: %s: %w", t.name, err)
	}
	defer b.release()
	r := &tableReader{s: s, conn: conn, t: t, b: b, out: out}
	if until != nil {
		return r.readUntil(ctx, cs, st, until)
	}
	if cs.SyncMode == source.Incremental {
		return r.readIncremental(ctx, sp.ID, cs.Cursor, st)
	}
	return r.readTable(ctx, sp.ID, st)
}

// readUntil reads what a read from st sent up to its resume point whose
// state is until: the rows past st's position up to and including until's,
// within until's key range and cursor bounds. Then it sends a resume point
// with until, and completes with it.
func (r *tableReader) readUntil(ctx context.Context, cs source.ConfiguredStream, st sourceState, until []byte) error {
	var u sourceState
	if err := json.Unmarshal(until, &u); err != nil {
		return fmt.Errorf("mysql: %s until: %w", cs.Name, err)
	}
	order := r.t.key
	bounds := func(w *where) error { return nil }
	if cs.SyncMode == source.Incremental {
		ci, err := cursorColumn(r.t, cs.Cursor)
		if err != nil {
			return err
		}
		cc := r.t.cols[ci]
		bounds = func(w *where) error {
			if u.Until != nil {
				if err := w.add(cc, "<=", *u.Until); err != nil {
					return err
				}
			}
			if u.Since != nil {
				return w.add(cc, ">=", *u.Since)
			}
			return nil
		}
		order = []int{ci}
		for _, k := range r.t.key {
			if k != ci {
				order = append(order, k)
			}
		}
	}
	switch {
	case !chunked(r.t):
		if !st.Done && u.Done {
			var w where
			if err := bounds(&w); err != nil {
				return err
			}
			if _, _, err := r.query(ctx, "SELECT "+r.list()+" FROM "+r.ident()+w.sql(), w.args, nil); err != nil {
				return err
			}
		}
	case u.After != nil:
		cols := r.cols(order)
		from := st.After
		for {
			var w where
			if err := bounds(&w); err != nil {
				return err
			}
			if err := u.where(r.t.cols[r.t.key[0]], &w); err != nil {
				return err
			}
			if from != nil {
				if err := w.after(cols, from); err != nil {
					return err
				}
			}
			if err := w.compare(cols, "<=", u.After); err != nil {
				return err
			}
			q := "SELECT " + r.list() + " FROM " + r.ident() + w.sql() + " ORDER BY " + r.names(order) + " LIMIT " + strconv.Itoa(r.s.cfg.ChunkRows)
			n, last, err := r.query(ctx, q, w.args, order)
			if err != nil {
				return err
			}
			if n < r.s.cfg.ChunkRows {
				break
			}
			from = last
		}
	}
	if err := r.out.ResumePoint(until); err != nil {
		return err
	}
	return r.out.Complete(until)
}

type tableReader struct {
	s    *Source
	conn *sql.Conn
	t    table
	b    *batcher
	out  source.SplitEmitter
}

func (r *tableReader) ident() string {
	return quoteIdent(r.s.cfg.Database) + "." + quoteIdent(r.t.name)
}

func (r *tableReader) list() string {
	names := make([]string, len(r.t.cols))
	for i, c := range r.t.cols {
		names[i] = c.name
	}
	return quoteAll(names)
}

// where builds a query's conditions, typing its parameters.
type where struct {
	conds []string
	args  []any
}

func (w *where) add(c sourceColumn, op, v string) error {
	ph, arg, err := param(c, v)
	if err != nil {
		return fmt.Errorf("mysql: a state's %s value %q: %w", c.name, v, err)
	}
	w.conds = append(w.conds, quoteIdent(c.name)+" "+op+" "+ph)
	w.args = append(w.args, arg)
	return nil
}

// after says a row is past a position in the order of cols.
func (w *where) after(cols []sourceColumn, vals []string) error { return w.compare(cols, ">", vals) }

// compare compares a row with a position in the order of cols.
func (w *where) compare(cols []sourceColumn, op string, vals []string) error {
	names := make([]string, len(cols))
	phs := make([]string, len(cols))
	for i, c := range cols {
		ph, arg, err := param(c, vals[i])
		if err != nil {
			return fmt.Errorf("mysql: a state's %s value %q: %w", c.name, vals[i], err)
		}
		names[i], phs[i] = quoteIdent(c.name), ph
		w.args = append(w.args, arg)
	}
	w.conds = append(w.conds, "("+strings.Join(names, ", ")+") "+op+" ("+strings.Join(phs, ", ")+")")
	return nil
}

func (w *where) sql() string {
	if len(w.conds) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(w.conds, " AND ")
}

func (r *tableReader) readTable(ctx context.Context, id string, st sourceState) error {
	if !chunked(r.t) {
		if !st.Done {
			if _, _, err := r.query(ctx, "SELECT "+r.list()+" FROM "+r.ident(), nil, nil); err != nil {
				return err
			}
			if err := resume(r.out, sourceState{Done: true}); err != nil {
				return err
			}
		}
		return r.out.Complete(nil)
	}
	key := make([]int, len(r.t.key))
	copy(key, r.t.key)
	keyCols := r.cols(key)
	if st.After != nil && len(st.After) != len(key) {
		return fmt.Errorf("mysql: %s state has %d key columns, the table has %d", r.t.name, len(st.After), len(key))
	}
	st = sourceState{After: st.After, keyRange: st.keyRange}
	reading := r.s.startSplit(r.t.name, id)
	defer r.s.endSplit(r.t.name, id)
	for {
		var w where
		if err := st.where(keyCols[0], &w); err != nil {
			return err
		}
		if st.After != nil {
			if err := w.after(keyCols, st.After); err != nil {
				return err
			}
		}
		q := "SELECT " + r.list() + " FROM " + r.ident() + w.sql() + " ORDER BY " + r.names(key) + " LIMIT " + strconv.Itoa(r.s.cfg.ChunkRows)
		n, last, err := r.query(ctx, q, w.args, key)
		if err != nil {
			return err
		}
		if n > 0 {
			st.After = last
		}
		ended := n < r.s.cfg.ChunkRows
		added, err := r.s.resplit(ctx, r.conn, r.t, reading, &st, ended, st.position(), false)
		if err != nil {
			return err
		}
		if err := resume(r.out, st, added...); err != nil {
			return err
		}
		if ended {
			return r.out.Complete(nil)
		}
	}
}

func (r *tableReader) cols(idx []int) []sourceColumn {
	out := make([]sourceColumn, len(idx))
	for i, k := range idx {
		out[i] = r.t.cols[k]
	}
	return out
}

func (r *tableReader) names(idx []int) string {
	names := make([]string, len(idx))
	for i, k := range idx {
		names[i] = r.t.cols[k].name
	}
	return quoteAll(names)
}

// readIncremental reads the rows whose cursor is from the state's floor up
// to the cursor's highest value when the read started, in (cursor, key)
// order. It completes with that value as the next read's floor, so the rows
// that hold it are read again then. Rows with a NULL cursor aren't read.
func (r *tableReader) readIncremental(ctx context.Context, id, cursor string, st sourceState) error {
	ci, err := cursorColumn(r.t, cursor)
	if err != nil {
		return err
	}
	cc := r.t.cols[ci]
	if st.Until == nil {
		until, err := r.s.maxCursor(ctx, r.conn, r.t, ci)
		if err != nil {
			return err
		}
		if until == nil {
			return complete(r.out, sourceState{Since: st.Since})
		}
		st = sourceState{Since: st.Since, Until: until}
	}
	bounds := func(w *where) error {
		if err := w.add(cc, "<=", *st.Until); err != nil {
			return err
		}
		if st.Since != nil {
			return w.add(cc, ">=", *st.Since)
		}
		return nil
	}
	order := []int{ci}
	for _, k := range r.t.key {
		if k != ci {
			order = append(order, k)
		}
	}
	if !chunked(r.t) {
		if !st.Done {
			var w where
			if err := bounds(&w); err != nil {
				return err
			}
			if _, _, err := r.query(ctx, "SELECT "+r.list()+" FROM "+r.ident()+w.sql(), w.args, nil); err != nil {
				return err
			}
			st.Done = true
			if err := resume(r.out, st); err != nil {
				return err
			}
		}
		return complete(r.out, sourceState{Since: st.Until})
	}
	if st.After != nil && len(st.After) != len(order) {
		return fmt.Errorf("mysql: %s state has %d cursor and key columns, the read has %d", r.t.name, len(st.After), len(order))
	}
	orderCols := r.cols(order)
	reading := r.s.startSplit(r.t.name, id)
	defer r.s.endSplit(r.t.name, id)
	for {
		var w where
		if err := bounds(&w); err != nil {
			return err
		}
		if err := st.where(r.t.cols[r.t.key[0]], &w); err != nil {
			return err
		}
		if st.After != nil {
			if err := w.after(orderCols, st.After); err != nil {
				return err
			}
		}
		q := "SELECT " + r.list() + " FROM " + r.ident() + w.sql() + " ORDER BY " + r.names(order) + " LIMIT " + strconv.Itoa(r.s.cfg.ChunkRows)
		n, last, err := r.query(ctx, q, w.args, order)
		if err != nil {
			return err
		}
		if n > 0 {
			st.After = last
		}
		ended := n < r.s.cfg.ChunkRows
		// The position follows the key only where the cursor is its first
		// column.
		var pos *string
		if r.t.key[0] == ci {
			pos = st.position()
		}
		added, err := r.s.resplit(ctx, r.conn, r.t, reading, &st, ended, pos, true)
		if err != nil {
			return err
		}
		if err := resume(r.out, st, added...); err != nil {
			return err
		}
		if ended {
			return complete(r.out, sourceState{Since: st.Until})
		}
	}
}

// maxCursor is the cursor column's highest value, spelled as keyText spells
// it; nil if the table has none.
func (s *Source) maxCursor(ctx context.Context, conn *sql.Conn, t table, ci int) (*string, error) {
	c := t.cols[ci]
	var v any
	err := queryRow(ctx, conn, "SELECT MAX("+quoteIdent(c.name)+") FROM "+quoteIdent(s.cfg.Database)+"."+quoteIdent(t.name), nil, &v)
	if err != nil || v == nil {
		return nil, err
	}
	m := keyText(c, v)
	return &m, nil
}

// queryRow reads one row through a prepared statement, whose values the
// binary protocol carries as the table's queries read them.
func queryRow(ctx context.Context, conn *sql.Conn, q string, args []any, dest ...any) error {
	stmt, err := conn.PrepareContext(ctx, q)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = stmt.Close() }()
	return classify(stmt.QueryRowContext(ctx, args...).Scan(dest...))
}

// cursorColumn is the index of a table's column that an incremental read can
// order by.
func cursorColumn(t table, cursor string) (int, error) {
	ci := slices.IndexFunc(t.cols, func(c sourceColumn) bool { return c.name == cursor })
	switch {
	case cursor == "":
		return 0, configInvalid("mysql: %s is read incrementally, which needs a cursor column", t.name)
	case ci < 0:
		return 0, configInvalid("mysql: %s has no column %q for a cursor", t.name, cursor)
	case !cursorKinds[t.cols[ci].kind]:
		return 0, configInvalid("mysql: %s.%s can't be a cursor: that takes an integer, decimal, date, datetime, timestamp, year, char or varchar column", t.name, cursor)
	}
	return ci, nil
}

func complete(out source.SplitEmitter, next sourceState) error {
	b, err := json.Marshal(next)
	if err != nil {
		return err
	}
	return out.Complete(b)
}

func resume(out source.SplitEmitter, st sourceState, added ...source.Split) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return out.ResumePoint(b, added...)
}

// query runs q as a prepared statement and emits its rows. It returns how
// many it read, and the last one's values of the columns at order, as
// keyText spells them. Every row is emitted before it returns.
func (r *tableReader) query(ctx context.Context, q string, args []any, order []int) (int, []string, error) {
	n := 0
	var last []string
	err := scan(ctx, r.conn, q, args, len(r.t.cols), func(vals []any) error {
		if err := r.b.add(vals); err != nil {
			return fmt.Errorf("mysql: read %s: %w", r.t.name, err)
		}
		n++
		if r.b.bytes >= r.s.cfg.BatchBytes {
			if err := r.b.flush(r.out.Records); err != nil {
				return err
			}
		}
		if order != nil {
			last = last[:0]
			for _, k := range order {
				last = append(last, keyText(r.t.cols[k], vals[k]))
			}
		}
		return nil
	})
	if err == nil {
		err = r.b.flush(r.out.Records)
	}
	return n, slices.Clone(last), err
}

// scan runs q as a prepared statement and calls f with each row's values,
// which f may keep.
func scan(ctx context.Context, conn interface {
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}, q string, args []any, ncols int, f func([]any) error) error {
	stmt, err := conn.PrepareContext(ctx, q)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = stmt.Close() }()
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return classify(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		vals := make([]any, ncols)
		ptrs := make([]any, ncols)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return classify(err)
		}
		if err := f(vals); err != nil {
			return err
		}
	}
	return classify(rows.Err())
}

// batcher builds one table's Arrow batches; a changelog's carry an op and a
// seq column after the table's.
type batcher struct {
	rb    *array.RecordBuilder
	adds  []adder
	names []string
	cdc   bool
	rows  int
	bytes int
}

func newBatcher(mem memory.Allocator, cols []sourceColumn, cdc bool) (*batcher, error) {
	fields := make([]arrow.Field, 0, len(cols)+2)
	for _, c := range cols {
		sent, _, err := arrowTypes(c)
		if err != nil {
			return nil, err
		}
		fields = append(fields, arrow.Field{Name: c.name, Type: sent, Nullable: true})
	}
	if cdc {
		fields = append(fields, source.OpField(), source.SeqField(seqType))
	}
	b := &batcher{rb: array.NewRecordBuilder(mem, arrow.NewSchema(fields, nil)), cdc: cdc}
	for i, c := range cols {
		b.adds = append(b.adds, newAdder(c, b.rb.Field(i)))
		b.names = append(b.names, c.name)
	}
	return b, nil
}

// add appends a row; a nil value is null. A row that fails leaves the
// batcher unusable.
func (b *batcher) add(vals []any) error {
	for i, add := range b.adds {
		v := vals[i]
		if v == nil {
			b.rb.Field(i).AppendNull()
			continue
		}
		if err := add(v); err != nil {
			return fmt.Errorf("column %q: %w", b.names[i], err)
		}
		if t, ok := v.([]byte); ok {
			b.bytes += len(t)
		} else {
			b.bytes += 8
		}
	}
	b.rows++
	return nil
}

func (b *batcher) flush(emit func(arrow.RecordBatch) error) error {
	if b.rows == 0 {
		return nil
	}
	rec := b.rb.NewRecordBatch()
	b.rows, b.bytes = 0, 0
	if err := emit(rec); err != nil {
		rec.Release()
		return err
	}
	return nil
}

func (b *batcher) release() { b.rb.Release() }
