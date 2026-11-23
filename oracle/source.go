package oracle

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	go_ora "github.com/sijms/go-ora/v2"

	"github.com/openetl-io/openetl-connectors/keysplit"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type SourceConfig struct {
	// oracle://user:password@host:port/service, with go-ora's options.
	DSN sdk.Secret `json:"dsn"`
	// The schema whose tables are offered, as Oracle stores its name.
	// Default: the user's current schema.
	Schema string `json:"schema"`
	// Empty: every table in the schema.
	Tables []string `json:"tables"`
	// Rows per query on a table with a key; each chunk ends at a resume
	// point. Default 100000.
	ChunkRows int `json:"chunkRows"`
	// Approximate bytes of values per Arrow batch. Default 4 MiB.
	BatchBytes int `json:"batchBytes"`
	// Rows per chunk of a cdc table's snapshot. Default 10000.
	SnapshotChunkRows int `json:"snapshotChunkRows"`
	// The most connections a read holds, as many as it has lanes up to
	// this; a cdc read's LogMiner session holds one of them. Default 4.
	MaxConnections int `json:"maxConnections"`
}

// Source reads a schema's tables: a full refresh reads a table's rows, an
// incremental read the rows from a cursor column's floor on, and cdc its
// changes from the redo log through LogMiner (cdc.go). A table with a key
// whose columns order as they compare is read in key order, or (cursor, key)
// order, a chunk per query, so a resumed read yields exactly the rows after
// its last chunk, within its key range if it's read in splits
// (source_splits.go). A table without one is read in a single query.
type Source struct {
	cfg SourceConfig
	// The dsn's host:port, empty if it names none; its user and service.
	addr, user, service string

	keysplit.Reads
}

var _ source.Source = (*Source)(nil)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{ChunkRows: 100_000, BatchBytes: 4 << 20, SnapshotChunkRows: 10_000, MaxConnections: 4}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("oracle: config: %w", err)
	}
	switch {
	case cfg.DSN.IsZero():
		return nil, errors.New("oracle: config needs a dsn")
	case cfg.ChunkRows <= 0 || cfg.BatchBytes <= 0 || cfg.SnapshotChunkRows <= 0 || cfg.MaxConnections <= 0:
		return nil, errors.New("oracle: chunkRows, batchBytes, snapshotChunkRows and maxConnections must be positive")
	}
	if err := parseDSN(cfg.DSN.Reveal()); err != nil {
		return nil, err
	}
	s := &Source{cfg: cfg}
	if u, err := url.Parse(cfg.DSN.Reveal()); err == nil {
		s.user, s.service = u.User.Username(), strings.Trim(u.Path, "/")
		if h := u.Hostname(); h != "" {
			s.addr = net.JoinHostPort(h, cmpOr(u.Port(), "1521"))
		}
	}
	return s, nil
}

// redacted is msg without dsn's password: a URL that doesn't parse is
// quoted whole in the error.
func redacted(msg, dsn string) string {
	_, rest, _ := strings.Cut(dsn, "://")
	if at := strings.LastIndex(rest, "@"); at > 0 {
		if _, pw, ok := strings.Cut(rest[:at], ":"); ok && pw != "" {
			msg = strings.ReplaceAll(msg, pw, "[redacted]")
		}
	}
	return msg
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// owner is the schema the source reads: the configured one, or the
// session's current schema.
func (s *Source) owner(ctx context.Context, db *sql.DB) (string, error) {
	if s.cfg.Schema != "" {
		return s.cfg.Schema, nil
	}
	return currentSchema(ctx, db)
}

func currentSchema(ctx context.Context, q queryer) (string, error) {
	var o string
	if err := q.QueryRowContext(ctx, `SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual`).Scan(&o); err != nil {
		return "", classify(fmt.Errorf("oracle: read the current schema: %w", err))
	}
	return o, nil
}

// tables are the schema's tables the config offers, and the schema.
func (s *Source) tables(ctx context.Context, db *sql.DB) ([]table, string, error) {
	schema, err := s.owner(ctx, db)
	if err != nil {
		return nil, "", err
	}
	tables, err := describe(ctx, db, schema, s.cfg.Tables)
	return tables, schema, err
}

func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	db, err := open(ctx, s.cfg.DSN.Reveal(), 1)
	if err != nil {
		return source.Catalog{}, err
	}
	defer func() { _ = db.Close() }()
	tables, _, err := s.tables(ctx, db)
	if err != nil {
		return source.Catalog{}, err
	}
	var c source.Catalog
	for _, t := range tables {
		cs := source.CatalogStream{
			Name: t.name, SplitsByKey: splittable(t), Replay: source.Exact,
			SyncModes: []source.SyncMode{source.FullRefresh}, Schema: declaredSchema(t),
		}
		if slices.ContainsFunc(t.cols, orderable) {
			cs.SyncModes = append(cs.SyncModes, source.Incremental)
		}
		if chunked(t) && t.noCDC == "" {
			cs.SyncModes, cs.Seq = append(cs.SyncModes, source.CDC), seqType
		}
		for _, i := range t.key {
			cs.PrimaryKey = append(cs.PrimaryKey, t.cols[i].name)
		}
		c.Streams = append(c.Streams, cs)
	}
	return c, nil
}

func findTable(ts []table, name string) (table, bool) {
	i := slices.IndexFunc(ts, func(t table) bool { return t.name == name })
	if i < 0 {
		return table{}, false
	}
	return ts[i], true
}

// binding is how a key's or cursor's value, as keyText spells it, is bound
// to compare with its column as the column orders, with NLS_COMP and
// NLS_SORT BINARY: text through a conversion to the column's type, so that
// the column's index serves the comparison. Zero: the column can't order a
// read, as floats, intervals, LOBs, LONG, ROWID, JSON and BOOLEAN can't.
type binding int

const (
	bindNumber binding = iota + 1
	bindString
	// NCHAR and NVARCHAR2, bound in the national character set.
	bindNString
	bindRaw
	bindDate
	bindTimestamp
	bindZoned
	// TIMESTAMP WITH LOCAL TIME ZONE.
	bindLocal
)

const (
	dateFormat      = `'SYYYY-MM-DD HH24:MI:SS'`
	timestampFormat = `'SYYYY-MM-DD HH24:MI:SS.FF9'`
	zonedFormat     = `'SYYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'`
)

func bindOf(c column) binding {
	switch c.kind {
	case kInt16, kInt32, kInt64, kDecimal:
		return bindNumber
	case kTimestamp:
		if c.dataType == "DATE" {
			return bindDate
		}
		return bindTimestamp
	case kTimestampNS:
		return bindTimestamp
	case kZoned, kZonedNS:
		if strings.Contains(c.dataType, "LOCAL") {
			return bindLocal
		}
		return bindZoned
	}
	switch c.dataType {
	case "VARCHAR2", "CHAR":
		return bindString
	case "NVARCHAR2", "NCHAR":
		return bindNString
	case "RAW":
		return bindRaw
	}
	return 0
}

// orderable columns compare with a bound value as they order, so a read can
// resume after a key or a cursor of them.
func orderable(c column) bool { return bindOf(c) != 0 }

// chunked says the table's key orders as it compares, so it's read a chunk
// at a time.
func chunked(t table) bool {
	return len(t.key) > 0 && !slices.ContainsFunc(t.key, func(k int) bool { return !orderable(t.cols[k]) })
}

type sourceState struct {
	// The key of the last row read, spelled as keyText spells it; in an
	// incremental read, its cursor value first.
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

// keyText spells a key's or a cursor's value, as go-ora reads it, for a
// state: NUMBER as its digits; text as it is, a CHAR's padding included;
// RAW as hex; DATE as SYYYY-MM-DD HH24:MI:SS and TIMESTAMP as SYYYY-MM-DD
// HH24:MI:SS.FF9, its wall clock, a year before 1 AD negative as Oracle
// numbers it (-1 is 1 BC); TIMESTAMP WITH (LOCAL) TIME ZONE as the
// instant in UTC, SYYYY-MM-DD HH24:MI:SS.FF9 +00:00.
func keyText(c column, v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return hex.EncodeToString(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case time.Time:
		switch bindOf(c) {
		case bindDate:
			return oracleTime(x, false)
		case bindZoned, bindLocal:
			return oracleTime(x.UTC(), true) + " +00:00"
		}
		return oracleTime(x, true)
	}
	return fmt.Sprint(v)
}

// oracleTime spells t's fields, its year as Oracle's, as SYYYY-MM-DD
// HH24:MI:SS, with .FF9 if frac.
func oracleTime(t time.Time, frac bool) string {
	y, sign := t.Year(), ""
	if y < 0 {
		y, sign = -y, "-"
	}
	s := fmt.Sprintf("%s%04d-%02d-%02d %02d:%02d:%02d", sign, y, t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	if frac {
		s += fmt.Sprintf(".%09d", t.Nanosecond())
	}
	return s
}

// where builds a query's conditions on the table aliased t, and its binds,
// which go-ora takes by position: each placeholder once, numbered in the
// order they appear.
type where struct {
	conds []string
	args  []any
}

// param binds v, as keyText spelled it for c, and is its placeholder as an
// expression of c's type.
func (w *where) param(c column, v string) (string, error) {
	ph := ":" + strconv.Itoa(len(w.args)+1)
	var arg any = v
	switch bindOf(c) {
	case bindNumber:
		ph = "TO_NUMBER(" + ph + ")"
	case bindDate:
		ph = "TO_DATE(" + ph + ", " + dateFormat + ")"
	case bindTimestamp:
		ph = "TO_TIMESTAMP(" + ph + ", " + timestampFormat + ")"
	case bindZoned:
		ph = "TO_TIMESTAMP_TZ(" + ph + ", " + zonedFormat + ")"
	case bindLocal:
		ph = "CAST(TO_TIMESTAMP_TZ(" + ph + ", " + zonedFormat + ") AS TIMESTAMP WITH LOCAL TIME ZONE)"
	case bindRaw:
		b, err := hex.DecodeString(v)
		if err != nil {
			return "", fmt.Errorf("oracle: a state's %s value %q: %w", c.name, v, err)
		}
		arg = b
	case bindNString:
		arg = go_ora.NVarChar(v)
	case bindString:
	default:
		return "", fmt.Errorf("oracle: %s, of type %s, can't order a read", c.name, c.dataType)
	}
	w.args = append(w.args, arg)
	return ph, nil
}

func colRef(c column) string { return "t." + quoteIdent(c.name) }

// add is the condition that c compares with v by op.
func (w *where) add(c column, op, v string) error {
	ph, err := w.param(c, v)
	if err != nil {
		return err
	}
	w.conds = append(w.conds, colRef(c)+" "+op+" "+ph)
	return nil
}

// after says a row is past a position in the order of cols.
func (w *where) after(cols []column, vals []string) error { return w.compare(cols, ">", vals) }

// compare compares a row with a position in the order of cols, by op, ">"
// or "<=". Oracle has no row-value comparison, so (a, b) > (x, y) is
// a >= x AND (a > x OR (a = x AND b > y)): its first condition bounds the
// first column, which an index on the order ranges over.
func (w *where) compare(cols []column, op string, vals []string) error {
	if len(vals) != len(cols) {
		return fmt.Errorf("oracle: a state has %d values of the read's order, which has %d columns", len(vals), len(cols))
	}
	strict, lead := ">", ">="
	if op == "<=" {
		strict, lead = "<", "<="
	}
	var b strings.Builder
	n := len(cols)
	if n > 1 {
		ph, err := w.param(cols[0], vals[0])
		if err != nil {
			return err
		}
		b.WriteString(colRef(cols[0]) + " " + lead + " " + ph + " AND ")
	}
	for i, c := range cols {
		ph, err := w.param(c, vals[i])
		if err != nil {
			return err
		}
		if i == n-1 {
			b.WriteString(colRef(c) + " " + op + " " + ph)
			break
		}
		eq, err := w.param(c, vals[i])
		if err != nil {
			return err
		}
		b.WriteString("(" + colRef(c) + " " + strict + " " + ph + " OR (" + colRef(c) + " = " + eq + " AND ")
	}
	b.WriteString(strings.Repeat("))", n-1))
	w.conds = append(w.conds, b.String())
	return nil
}

func (w *where) sql() string {
	if len(w.conds) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(w.conds, " AND ")
}

// selectFrom is the query of t's columns, aliased t, as of SCN asOf when it
// isn't 0, and its bind.
func selectFrom(schema string, t table, asOf uint64) (string, []any) {
	cols := make([]string, len(t.cols))
	for i, c := range t.cols {
		cols[i] = c.selectExpr()
	}
	q := "SELECT " + strings.Join(cols, ", ") + " FROM " + quoteIdent(schema) + "." + quoteIdent(t.name)
	if asOf == 0 {
		return q + " t", nil
	}
	return q + " AS OF SCN :1 t", []any{asOf}
}

// readChunk reads up to n rows of t, in key order, after the key after
// (nil: from the first row), as of SCN asOf when it isn't 0 (`FROM "S"."T"
// AS OF SCN :scn t`), and calls f with each row's values as go-ora scans
// them (len(t.cols)). It returns the last row's key as keyText spells it,
// nil if there were no rows.
func readChunk(ctx context.Context, conn *sql.Conn, schema string, t table, asOf uint64, after []string, n int, f func(vals []any) error) ([]string, error) {
	return readOrdered(ctx, conn, schema, t, t.key, asOf, nil, after, n, f)
}

// readOrdered reads up to n rows of t that meet conds, in the order of the
// columns at order, after the position after in that order if it isn't
// nil, as of SCN asOf when it isn't 0. It calls f with each row's values,
// and returns the last row's values at order as keyText spells them, nil
// if there were none.
func readOrdered(ctx context.Context, conn *sql.Conn, schema string, t table, order []int, asOf uint64, conds func(*where) error, after []string, n int, f func([]any) error) ([]string, error) {
	q, args := selectFrom(schema, t, asOf)
	w := where{args: args}
	if conds != nil {
		if err := conds(&w); err != nil {
			return nil, err
		}
	}
	cols := make([]column, len(order))
	names := make([]string, len(order))
	for i, k := range order {
		cols[i], names[i] = t.cols[k], colRef(t.cols[k])
	}
	if after != nil {
		if err := w.after(cols, after); err != nil {
			return nil, err
		}
	}
	q += w.sql() + " ORDER BY " + strings.Join(names, ", ") + " FETCH FIRST " + strconv.Itoa(n) + " ROWS ONLY"
	var last []string
	err := scan(ctx, conn, q, w.args, len(t.cols), func(vals []any) error {
		last = last[:0]
		for _, k := range order {
			last = append(last, keyText(t.cols[k], vals[k]))
		}
		return f(vals)
	})
	if err != nil {
		return nil, err
	}
	return slices.Clone(last), nil
}

// scan runs q and calls f with each row's values, which f may keep.
func scan(ctx context.Context, q queryer, query string, args []any, ncols int, f func([]any) error) error {
	rows, err := q.QueryContext(ctx, query, args...)
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

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	limit := min(max(req.Lanes, 1), s.cfg.MaxConnections)
	db, err := open(ctx, s.cfg.DSN.Reveal(), limit)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	conns := &connPool{DB: db, Held: make(chan struct{}, limit), Failed: func(err error) error {
		return classify(fmt.Errorf("oracle: connect: %w", err))
	}}
	tables, schema, err := s.tables(ctx, db)
	if err != nil {
		return err
	}
	var cdc, rest []source.ConfiguredStream
	for _, cs := range req.Streams {
		t, ok := findTable(tables, cs.Name)
		switch {
		case !ok:
			return configInvalid("oracle: there's no table %s.%s the user can read", schema, cs.Name)
		case cs.SyncMode == source.CDC:
			cdc = append(cdc, cs)
		default:
			if err := undeclared(t, cs.Columns); err != nil {
				return err
			}
			rest = append(rest, cs)
		}
	}
	readRest := func(ctx context.Context, lanes int) error {
		return source.ReadSplits(ctx, source.ReadRequest{Streams: rest, Lanes: lanes}, out, func(ctx context.Context, cs source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
			if req.Continuous {
				sp.Until, sp.CompleteAt = nil, nil
			}
			return s.readSplit(ctx, conns, schema, tables, cs, sp, e)
		})
	}
	var readCDC func(context.Context) error
	if len(cdc) > 0 {
		readCDC = func(ctx context.Context) error { return s.readCDC(ctx, conns, req, cdc, tables, out) }
	}
	return keysplit.ReadWithCDC(ctx, limit, readRest, readCDC)
}

// undeclared fails a read of a table with a column whose values are sent as
// text and that the run didn't declare: the engine has no type to parse
// them as. The next attempt's Discover declares it. No columns: the request
// doesn't say.
func undeclared(t table, columns []string) error {
	if len(columns) == 0 {
		return nil
	}
	for _, c := range t.cols {
		if _, declared := arrowTypes(c); declared != nil && !slices.Contains(columns, c.name) {
			return transient("oracle: %s has a new column %s; the read starts again to declare it", t.name, c.name)
		}
	}
	return nil
}

// readSplit reads one split of a table read in full or incrementally, on a
// connection of its own; up to the split's Until, or its CompleteAt, if it
// has one.
func (s *Source) readSplit(ctx context.Context, conns *connPool, schema string, tables []table, cs source.ConfiguredStream, sp source.Split, out source.SplitEmitter) error {
	var st sourceState
	if sp.State != nil {
		if err := json.Unmarshal(sp.State, &st); err != nil {
			return fmt.Errorf("oracle: %s state: %w", cs.Name, err)
		}
	}
	conn, err := conns.Get(ctx)
	if err != nil {
		return err
	}
	defer conns.Put(conn)
	t, _ := findTable(tables, cs.Name)
	b := newBatcher(memory.DefaultAllocator, t.cols, false)
	defer b.release()
	r := &tableReader{s: s, conn: conn, schema: schema, t: t, b: b, out: out}
	if sp.Until != nil {
		return r.readUntil(ctx, cs, st, sp.Until)
	}
	if cs.SyncMode == source.Incremental {
		return r.readIncremental(ctx, sp.ID, cs.Cursor, st, sp.CompleteAt)
	}
	return r.readTable(ctx, sp.ID, st)
}

type tableReader struct {
	s      *Source
	conn   *sql.Conn
	schema string
	t      table
	b      *batcher
	out    source.SplitEmitter
}

// incrementalOrder is an incremental read's order: the cursor, then the
// key's other columns.
func incrementalOrder(t table, ci int) []int {
	order := []int{ci}
	for _, k := range t.key {
		if k != ci {
			order = append(order, k)
		}
	}
	return order
}

func (r *tableReader) cols(idx []int) []column {
	out := make([]column, len(idx))
	for i, k := range idx {
		out[i] = r.t.cols[k]
	}
	return out
}

// readUntil reads what a read from st sent up to its resume point whose
// state is until: the rows past st's position up to and including until's,
// within until's key range and cursor bounds. Then it sends a resume point
// with until, and completes with it.
func (r *tableReader) readUntil(ctx context.Context, cs source.ConfiguredStream, st sourceState, until []byte) error {
	var u sourceState
	if err := json.Unmarshal(until, &u); err != nil {
		return fmt.Errorf("oracle: %s until: %w", cs.Name, err)
	}
	order := r.t.key
	bounds := func(w *where) error { return nil }
	if cs.SyncMode == source.Incremental {
		ci, err := cursorColumn(r.t, cs.Cursor)
		if err != nil {
			return err
		}
		bounds = cursorBounds(r.t.cols[ci], u)
		order = incrementalOrder(r.t, ci)
	}
	switch {
	case !chunked(r.t):
		if !st.Done && u.Done {
			if _, _, err := r.query(ctx, nil, bounds, nil); err != nil {
				return err
			}
		}
	case u.After != nil:
		cols := r.cols(order)
		from := st.After
		for {
			conds := func(w *where) error {
				if err := bounds(w); err != nil {
					return err
				}
				if err := u.where(r.t.cols[r.t.key[0]], w); err != nil {
					return err
				}
				if from != nil {
					if err := w.after(cols, from); err != nil {
						return err
					}
				}
				return w.compare(cols, "<=", u.After)
			}
			n, last, err := r.query(ctx, order, conds, nil)
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

// cursorBounds are an incremental read's conditions on its cursor c: up to
// st's Until, and from its Since.
func cursorBounds(c column, st sourceState) func(*where) error {
	return func(w *where) error {
		if st.Until != nil {
			if err := w.add(c, "<=", *st.Until); err != nil {
				return err
			}
		}
		if st.Since != nil {
			return w.add(c, ">=", *st.Since)
		}
		return nil
	}
}

func (r *tableReader) readTable(ctx context.Context, id string, st sourceState) error {
	if !chunked(r.t) {
		if !st.Done {
			if _, _, err := r.query(ctx, nil, nil, nil); err != nil {
				return err
			}
			if err := resume(r.out, sourceState{Done: true}); err != nil {
				return err
			}
		}
		return r.out.Complete(nil)
	}
	if st.After != nil && len(st.After) != len(r.t.key) {
		return fmt.Errorf("oracle: %s state has %d key columns, the table has %d", r.t.name, len(st.After), len(r.t.key))
	}
	st = sourceState{After: st.After, keyRange: st.keyRange}
	reading := r.s.Start(r.t.name, id, true)
	defer r.s.End(r.t.name, id)
	for {
		n, last, err := r.query(ctx, r.t.key, func(w *where) error { return st.where(r.t.cols[r.t.key[0]], w) }, st.After)
		if err != nil {
			return err
		}
		if n > 0 {
			st.After = last
		}
		ended := n < r.s.cfg.ChunkRows
		added, err := r.s.resplit(ctx, r.conn, r.schema, r.t, reading, &st, ended, st.position(), false)
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

// readIncremental reads the rows whose cursor is from the state's floor up
// to the cursor's highest value when the read started, in (cursor, key)
// order. It completes with that value as the next read's floor, so the rows
// that hold it are read again then. Rows with a NULL cursor aren't read.
// completeAt, if set, is a completion this source sent before: the read goes
// up to its floor, which was that read's highest value, and completes with
// it.
func (r *tableReader) readIncremental(ctx context.Context, id, cursor string, st sourceState, completeAt []byte) error {
	ci, err := cursorColumn(r.t, cursor)
	if err != nil {
		return err
	}
	end := func() error { return complete(r.out, sourceState{Since: st.Until}) }
	if completeAt != nil {
		end = func() error { return r.out.Complete(completeAt) }
	}
	if st.Until == nil {
		until, err := r.upTo(ctx, ci, completeAt)
		if err != nil {
			return err
		}
		if until == nil {
			// Nothing to read: the next read keeps the floor.
			st.Until = st.Since
			return end()
		}
		st = sourceState{Since: st.Since, Until: until}
	}
	bounds := cursorBounds(r.t.cols[ci], st)
	if !chunked(r.t) {
		if !st.Done {
			if _, _, err := r.query(ctx, nil, bounds, nil); err != nil {
				return err
			}
			st.Done = true
			if err := resume(r.out, st); err != nil {
				return err
			}
		}
		return end()
	}
	order := incrementalOrder(r.t, ci)
	if st.After != nil && len(st.After) != len(order) {
		return fmt.Errorf("oracle: %s state has %d cursor and key columns, the read has %d", r.t.name, len(st.After), len(order))
	}
	reading := r.s.Start(r.t.name, id, true)
	defer r.s.End(r.t.name, id)
	for {
		conds := func(w *where) error {
			if err := bounds(w); err != nil {
				return err
			}
			return st.where(r.t.cols[r.t.key[0]], w)
		}
		n, last, err := r.query(ctx, order, conds, st.After)
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
		added, err := r.s.resplit(ctx, r.conn, r.schema, r.t, reading, &st, ended, pos, true)
		if err != nil {
			return err
		}
		if err := resume(r.out, st, added...); err != nil {
			return err
		}
		if ended {
			return end()
		}
	}
}

// maxCursor is the cursor column's highest value, spelled as keyText spells
// it; nil if the table has none.
func maxCursor(ctx context.Context, conn *sql.Conn, schema string, t table, ci int) (*string, error) {
	c := t.cols[ci]
	var v any
	err := conn.QueryRowContext(ctx, "SELECT MAX("+colRef(c)+") FROM "+quoteIdent(schema)+"."+quoteIdent(t.name)+" t").Scan(&v)
	if err != nil || v == nil {
		return nil, classify(err)
	}
	m := keyText(c, v)
	return &m, nil
}

// upTo is the highest cursor value an incremental read reads: the floor of
// completeAt, a completion it sent before, if set, or else the column's
// highest value now.
func (r *tableReader) upTo(ctx context.Context, ci int, completeAt []byte) (*string, error) {
	if completeAt == nil {
		return maxCursor(ctx, r.conn, r.schema, r.t, ci)
	}
	var at sourceState
	if err := json.Unmarshal(completeAt, &at); err != nil {
		return nil, fmt.Errorf("oracle: where %s completes: %w", r.t.name, err)
	}
	return at.Since, nil
}

// cursorColumn is the index of a table's column that an incremental read can
// order by.
func cursorColumn(t table, cursor string) (int, error) {
	ci := slices.IndexFunc(t.cols, func(c column) bool { return c.name == cursor })
	switch {
	case cursor == "":
		return 0, configInvalid("oracle: %s is read incrementally, which needs a cursor column", t.name)
	case ci < 0:
		return 0, configInvalid("oracle: %s has no column %q for a cursor", t.name, cursor)
	case !orderable(t.cols[ci]):
		return 0, configInvalid("oracle: %s.%s can't be a cursor: %s", t.name, cursor, cursorTypes)
	}
	return ci, nil
}

// cursorTypes say what a cursor can be.
const cursorTypes = "that takes a NUMBER, VARCHAR2, CHAR, NVARCHAR2, NCHAR, RAW, DATE or TIMESTAMP column, with or without a time zone"

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

// query reads the rows that meet conds and emits them: in one query if
// order is nil, or else up to chunkRows of them in the order of the columns
// at order, after the position after. It returns how many it read, and the
// last one's values at order, as keyText spells them. Every row is emitted
// before it returns.
func (r *tableReader) query(ctx context.Context, order []int, conds func(*where) error, after []string) (int, []string, error) {
	n := 0
	emit := func(vals []any) error {
		if err := r.b.add(vals); err != nil {
			return fmt.Errorf("oracle: read %s: %w", r.t.name, err)
		}
		n++
		if r.b.bytes >= r.s.cfg.BatchBytes {
			return r.b.flush(r.out.Records)
		}
		return nil
	}
	var last []string
	var err error
	if order == nil {
		q, args := selectFrom(r.schema, r.t, 0)
		w := where{args: args}
		if conds != nil {
			err = conds(&w)
		}
		if err == nil {
			err = scan(ctx, r.conn, q+w.sql(), w.args, len(r.t.cols), emit)
		}
	} else {
		last, err = readOrdered(ctx, r.conn, r.schema, r.t, order, 0, conds, after, r.s.cfg.ChunkRows, emit)
	}
	if err == nil {
		err = r.b.flush(r.out.Records)
	}
	return n, last, err
}
