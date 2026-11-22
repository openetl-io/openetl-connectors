package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl-connectors/keysplit"
	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type SourceConfig struct {
	connConfig
	// The tables to offer as streams. Empty: every table and view in the
	// database but materialized views.
	Tables []string `json:"tables"`
	// Rows per query on a table read in sorting-key order; each chunk ends
	// at a resume point. Default 100000.
	ChunkRows int `json:"chunkRows"`
	// Rows per Arrow batch. Default 10000.
	BatchRows int `json:"batchRows"`
	// The most connections a read holds, as many as it has lanes up to
	// this. Default 4.
	MaxConnections int `json:"maxConnections"`
}

// Source reads tables: a full refresh reads a table's rows, an incremental
// read the rows from a cursor column's floor on. ClickHouse keeps no change
// log, so there is no CDC. A MergeTree table is read in order of its sorting
// key, as far as that key is plain columns, a chunk per query, so a resumed
// read yields exactly the rows after its last chunk, within its key range
// if it's read in splits (source_splits.go). Tables whose engine collapses
// rows by key, such as ReplacingMergeTree, are read with FINAL, as merged.
// Other tables are read in a single query.
type Source struct {
	cfg SourceConfig

	keysplit.Reads
}

var _ source.Source = (*Source)(nil)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{connConfig: connConfig{User: "default", Database: "default"}, ChunkRows: 100_000, BatchRows: 10_000, MaxConnections: 4}
	if err := decodeConfig(config, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.check("source"); err != nil {
		return nil, err
	}
	if cfg.ChunkRows <= 0 || cfg.BatchRows <= 0 || cfg.MaxConnections <= 0 {
		return nil, errors.New("clickhouse: chunkRows, batchRows and maxConnections must be positive")
	}
	return &Source{cfg: cfg}, nil
}

type table struct {
	name, engine string
	cols         []sourceColumn
	// Indexes into cols: the sorting key's leading plain columns, which
	// hold no null, in order. Empty: the table is read in one query.
	key []int
	// Every row has its own key: the engine collapses rows by it, read with
	// FINAL, and the sorting key is all plain columns.
	unique bool
	final  bool
}

type sourceColumn struct {
	name, typ string
	// The declared type.
	dt arrow.DataType
	// Read as JSON text, with toJSONString.
	opaque bool
	// Sent as text, which the engine parses into dt (sentAsText).
	text     bool
	nullable bool
}

var jsonType, _ = extensions.NewJSONType(arrow.BinaryTypes.String)

// Engines whose rows FINAL merges by sorting key, and those of them that
// leave one row per key.
var (
	finalEngine  = regexp.MustCompile(`^(Shared|Replicated)?(Replacing|Collapsing|VersionedCollapsing|Summing|Aggregating|Coalescing|Graphite)MergeTree$`)
	uniqueEngine = regexp.MustCompile(`^(Shared|Replicated)?(Replacing|Summing|Aggregating|Coalescing)MergeTree$`)
)

func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	conn, err := s.cfg.connect(ctx, 1, nil)
	if err != nil {
		return source.Catalog{}, err
	}
	defer func() { _ = conn.Close() }()
	tables, err := s.tables(ctx, conn)
	if err != nil {
		return source.Catalog{}, err
	}
	var c source.Catalog
	for _, t := range tables {
		cs := source.CatalogStream{Name: t.name, SplitsByKey: len(t.key) > 0, Replay: source.Exact, SyncModes: []source.SyncMode{source.FullRefresh}, Schema: t.schema()}
		if slices.ContainsFunc(t.cols, func(c sourceColumn) bool { return cursorType(c.typ) }) {
			cs.SyncModes = append(cs.SyncModes, source.Incremental)
		}
		if t.unique {
			for _, i := range t.key {
				cs.PrimaryKey = append(cs.PrimaryKey, t.cols[i].name)
			}
		}
		c.Streams = append(c.Streams, cs)
	}
	return c, nil
}

// schema is the table's declared columns.
func (t table) schema() *arrow.Schema {
	fields := make([]arrow.Field, len(t.cols))
	for i, c := range t.cols {
		fields[i] = arrow.Field{Name: c.name, Type: c.dt, Nullable: c.nullable || c.opaque}
	}
	return arrow.NewSchema(fields, nil)
}

// sent is the table's columns as its batches carry them.
func (t table) sent() *arrow.Schema {
	fields := t.schema().Fields()
	for i, c := range t.cols {
		if c.text {
			fields[i].Type = arrow.BinaryTypes.String
		}
	}
	return arrow.NewSchema(fields, nil)
}

const tablesQuery = `SELECT t.name, t.engine, t.sorting_key, c.name, c.type
	FROM system.tables t JOIN system.columns c ON c.database = t.database AND c.table = t.name
	WHERE t.database = ? AND t.engine != 'MaterializedView' AND NOT t.is_temporary AND c.default_kind NOT IN ('ALIAS', 'EPHEMERAL')
	ORDER BY t.name, c.position`

// tables lists the configured tables, in name order.
func (s *Source) tables(ctx context.Context, conn driver.Conn) ([]table, error) {
	rows, err := conn.Query(ctx, tablesQuery, s.cfg.Database)
	if err != nil {
		return nil, classify(fmt.Errorf("clickhouse: list tables: %w", err))
	}
	out, err := s.readTables(rows)
	if err != nil {
		return nil, err
	}
	for _, want := range s.cfg.Tables {
		if !slices.ContainsFunc(out, func(t table) bool { return t.name == want }) {
			return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("clickhouse: no table %q in database %q that the user can read", want, s.cfg.Database))
		}
	}
	return out, nil
}

// tableRows is what both the native driver's rows and database/sql's have.
type tableRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// readTables reads tablesQuery's rows into the configured tables, and
// closes them.
func (s *Source) readTables(rows tableRows) ([]table, error) {
	defer func() { _ = rows.Close() }()
	var out []table
	sortingKeys := map[string]string{}
	for rows.Next() {
		var name, engine, sortingKey string
		var c sourceColumn
		if err := rows.Scan(&name, &engine, &sortingKey, &c.name, &c.typ); err != nil {
			return nil, fmt.Errorf("clickhouse: list tables: %w", err)
		}
		if len(s.cfg.Tables) > 0 && !slices.Contains(s.cfg.Tables, name) {
			continue
		}
		if len(out) == 0 || out[len(out)-1].name != name {
			out = append(out, table{name: name, engine: engine, final: finalEngine.MatchString(engine)})
			sortingKeys[name] = sortingKey
		}
		ct := parseType(c.typ)
		_, c.nullable = ct.unwrap()
		var err error
		if c.dt, err = arrowType(ct); errors.Is(err, errOpaque) {
			c.opaque, c.dt = true, jsonType
		} else if err != nil {
			return nil, fmt.Errorf("clickhouse: %s.%s: %w", name, c.name, err)
		}
		c.text = sentAsText(ct)
		t := &out[len(out)-1]
		t.cols = append(t.cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(fmt.Errorf("clickhouse: list tables: %w", err))
	}
	for i := range out {
		t := &out[i]
		t.key, t.unique = sortingColumns(t.cols, sortingKeys[t.name])
		t.unique = t.unique && uniqueEngine.MatchString(t.engine)
	}
	return out, nil
}

// sortingColumns returns the leading columns of a sorting key that a read
// can order and resume by: plain columns of scalar types that hold no
// null, nor a NaN, which compares false with every value, so a read can't
// resume past one. all says the whole key is.
func sortingColumns(cols []sourceColumn, sortingKey string) (key []int, all bool) {
	if strings.TrimSpace(sortingKey) == "" {
		return nil, false
	}
	parts := parseType("k(" + sortingKey + ")").args
	for _, p := range parts {
		p = strings.Trim(p, "`")
		i := slices.IndexFunc(cols, func(c sourceColumn) bool { return c.name == p })
		if i < 0 || cols[i].nullable || cols[i].opaque || !scalar(cols[i].dt) || arrow.IsFloating(cols[i].dt.ID()) {
			return key, false
		}
		key = append(key, i)
	}
	return key, true
}

func scalar(dt arrow.DataType) bool {
	switch dt.(type) {
	case arrow.NestedType, arrow.ExtensionType:
		return false
	}
	return true
}

// The types an incremental read can order by: numbers, dates and times,
// and strings.
var cursorTypes = regexp.MustCompile(`^(U?Int\d+|Float\d+|BFloat16|Decimal\d*|Date|Date32|DateTime|DateTime64|String|FixedString)$`)

func cursorType(typ string) bool {
	t, _ := parseType(typ).unwrap()
	return cursorTypes.MatchString(t.name)
}

// sourceState is where a read of a table, or a split of it, is. Values
// are each column's toString, in hex, which a query casts back to its type.
type sourceState struct {
	// The order columns of the last row read: the key's, after the cursor's
	// in an incremental read.
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

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	limit := min(max(req.Lanes, 1), s.cfg.MaxConnections)
	conn, err := s.cfg.connect(ctx, limit, nil)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	tables, err := s.tables(ctx, conn)
	if err != nil {
		return err
	}
	for _, cs := range req.Streams {
		i := slices.IndexFunc(tables, func(t table) bool { return t.name == cs.Name })
		switch {
		case i < 0:
			return fmt.Errorf("clickhouse: unknown stream %q", cs.Name)
		case cs.SyncMode == source.CDC || cs.SyncMode == source.Stream:
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("clickhouse: %s can only be read in full or incrementally: ClickHouse keeps no change log", cs.Name))
		}
		if err := undeclaredText(tables[i], cs.Columns); err != nil {
			return err
		}
	}
	return source.ReadSplits(ctx, source.ReadRequest{Streams: req.Streams, Lanes: limit}, out, func(ctx context.Context, cs source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		var st sourceState
		if sp.State != nil {
			if err := json.Unmarshal(sp.State, &st); err != nil {
				return fmt.Errorf("clickhouse: %s state: %w", cs.Name, err)
			}
		}
		until := sp.Until
		if sp.ID == source.WholeStream && until == nil {
			until = cs.Until
		}
		r := &tableRead{s: s, conn: conn, t: tables[slices.IndexFunc(tables, func(t table) bool { return t.name == cs.Name })], id: sp.ID, out: e}
		if until != nil {
			r.until = &sourceState{}
			if err := json.Unmarshal(until, r.until); err != nil {
				return fmt.Errorf("clickhouse: %s until: %w", cs.Name, err)
			}
			r.untilState = until
		}
		if cs.SyncMode == source.Incremental {
			return r.incremental(ctx, cs.Cursor, st)
		}
		return r.full(ctx, st)
	})
}

// undeclaredText fails a read, retryably, if columns, those the run
// declared, leave out one of t's that can come as text for the engine to
// parse: a DateTime64 of more than 6 digits, and a time of day, whose values
// past a day do. Undeclared, its text would be taken for a string. The next
// attempt discovers and declares it. No columns: the request doesn't say.
func undeclaredText(t table, columns []string) error {
	if len(columns) == 0 {
		return nil
	}
	for _, c := range t.cols {
		if (c.text || canBePast(c.dt)) && !slices.Contains(columns, c.name) {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("clickhouse: %s has a column %q the read didn't declare, which can come as text; the read starts again to declare it", t.name, c.name))
		}
	}
	return nil
}

// tableRead is one split's read.
type tableRead struct {
	s    *Source
	conn driver.Conn
	t    table
	id   string
	out  source.SplitEmitter
	// Where the read stops, as given and parsed; nil: where it ends.
	until      *sourceState
	untilState []byte
}

func (r *tableRead) from() string { return r.s.from(r.t) }

// from is what a read of t selects from: t, with FINAL where its engine
// merges rows by key, so that the read sees them merged.
func (s *Source) from(t table) string {
	q := qualified(s.cfg.Database, t.name)
	if t.final {
		q += " FINAL"
	}
	return q
}

// untilQuery selects the highest value of the cursor c in from, as a state
// keeps it; NULL if it has none. from is a read's, so that the value is of
// the rows the read sees. A NaN is left out: no value is at or below one.
func untilQuery(from string, c sourceColumn) string {
	q := "SELECT if(count() > 0, " + stateText(c, "max("+ident(c.name)+")") + ", NULL) FROM " + from + " WHERE " + ident(c.name) + " IS NOT NULL"
	if t, _ := parseType(c.typ).unwrap(); strings.HasPrefix(t.name, "Float") || t.name == "BFloat16" {
		q += " AND NOT isNaN(" + ident(c.name) + ")"
	}
	return q
}

func (r *tableRead) selectList() string {
	sel := make([]string, len(r.t.cols))
	for i, c := range r.t.cols {
		sel[i] = ident(c.name)
		switch m := micros(parseType(c.typ), ident(c.name), 0); {
		case c.opaque:
			sel[i] = "toJSONString(" + ident(c.name) + ")"
		case c.text:
			sel[i] = "concat(toString(" + ident(c.name) + ", 'UTC'), 'Z')"
		case m != "":
			sel[i] = m
		}
	}
	return strings.Join(sel, ", ")
}

// micros is expr, a value of t, with each DateTime64 in it, alone or in
// arrays, as its microseconds since 1970: the driver's time.Time is wrong
// past 2262, as it counts in nanoseconds. Empty if t has none. depth numbers
// an array's lambda's variable.
func micros(t chType, expr string, depth int) string {
	t, _ = t.unwrap()
	switch t.name {
	case "DateTime64":
		return "toUnixTimestamp64Micro(" + expr + ")"
	case "Array":
		x := "x" + strconv.Itoa(depth)
		if m := micros(t.arg(0), x, depth+1); m != "" {
			return "arrayMap(" + x + " -> " + m + ", " + expr + ")"
		}
	}
	return ""
}

// castBack is a state's value, as a SQL expression of the column's type,
// or for a DateTime, of the same instant in UTC.
func castBack(c sourceColumn, v string) string {
	typ := c.typ
	if utc := utcType(c); utc != "" {
		typ = utc
	}
	return "CAST(unhex(" + literal(v) + ") AS " + typ + ")"
}

// keyText is the expression a state keeps of a column's value.
func keyText(c sourceColumn) string { return stateText(c, ident(c.name)) }

// stateText spells expr, a value of c's type, as a state keeps it: its
// text, in hex. A DateTime's is in UTC, as a local time can come twice.
func stateText(c sourceColumn, expr string) string {
	if utcType(c) != "" {
		expr = "toTimeZone(" + expr + ", 'UTC')"
	}
	return "hex(toString(" + expr + "))"
}

// utcType is a DateTime column's type in UTC; empty for other columns.
func utcType(c sourceColumn) string {
	t, _ := parseType(c.typ).unwrap()
	switch t.name {
	case "DateTime":
		return "DateTime('UTC')"
	case "DateTime64":
		return "DateTime64(" + t.args[0] + ", 'UTC')"
	}
	return ""
}

func (r *tableRead) full(ctx context.Context, st sourceState) error {
	if len(r.t.key) == 0 {
		return r.whole(ctx, st, "", sourceState{Done: true}, nil)
	}
	return r.chunks(ctx, st, r.t.key, nil, nil)
}

// whole reads a table, or an incremental read's rows, in one query where
// it has no key, unless st says it has, and completes with next.
func (r *tableRead) whole(ctx context.Context, st sourceState, where string, done sourceState, next []byte) error {
	if !st.Done {
		q := "SELECT " + r.selectList() + " FROM " + r.from()
		if where != "" {
			q += " WHERE " + where
		}
		if _, _, err := r.query(ctx, q, 0); err != nil {
			return err
		}
		if err := r.resume(done); err != nil {
			return err
		}
	}
	return r.complete(next)
}

func (r *tableRead) resume(st sourceState, added ...source.Split) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return r.out.ResumePoint(b, added...)
}

// complete completes the split with next, or at its until, after a resume
// point there.
func (r *tableRead) complete(next []byte) error {
	if r.until != nil {
		if err := r.out.ResumePoint(r.untilState); err != nil {
			return err
		}
		return r.out.Complete(r.untilState)
	}
	return r.out.Complete(next)
}

// chunks reads in order of the columns order, a chunk per query, from st,
// which conds bound. It completes with next.
func (r *tableRead) chunks(ctx context.Context, st sourceState, order []int, conds []string, next []byte) error {
	if st.After != nil && len(st.After) != len(order) {
		return fmt.Errorf("clickhouse: %s state has %d order columns, the read has %d", r.t.name, len(st.After), len(order))
	}
	cols := make([]string, len(order))
	texts := make([]string, len(order))
	for i, k := range order {
		cols[i], texts[i] = ident(r.t.cols[k].name), keyText(r.t.cols[k])
	}
	after := func(vals []string) string {
		exprs := make([]string, len(vals))
		for i, v := range vals {
			exprs[i] = castBack(r.t.cols[order[i]], v)
		}
		return "(" + strings.Join(cols, ", ") + ") > (" + strings.Join(exprs, ", ") + ")"
	}
	head := "SELECT " + r.selectList() + ", " + strings.Join(texts, ", ") + " FROM " + r.from()
	// Rows that share the last row's order columns come in its chunk.
	tail := " ORDER BY " + strings.Join(cols, ", ") + " LIMIT " + strconv.Itoa(r.s.cfg.ChunkRows) + " WITH TIES"
	if r.until != nil && r.until.After != nil {
		if len(r.until.After) != len(order) {
			return fmt.Errorf("clickhouse: %s until has %d order columns, the read has %d", r.t.name, len(r.until.After), len(order))
		}
		conds = append(slices.Clip(conds), "NOT "+after(r.until.After))
	}
	if r.until != nil && r.until.After == nil {
		// It stops before the first row.
		return r.complete(next)
	}
	// A table without a key is read by its cursor alone, in one range.
	keyed := len(r.t.key) > 0
	// The position follows the key only where it's the order's first column.
	positioned := keyed && order[0] == r.t.key[0]
	reading := r.s.Start(r.t.name, r.id, true)
	defer r.s.End(r.t.name, r.id)
	for {
		where := slices.Clone(conds)
		if keyed {
			where = append(where, st.where(r.t.cols[r.t.key[0]])...)
		}
		if st.After != nil {
			where = append(where, after(st.After))
		}
		q := head
		if len(where) > 0 {
			q += " WHERE " + strings.Join(where, " AND ")
		}
		n, last, err := r.query(ctx, q+tail, len(order))
		if err != nil {
			return err
		}
		if n > 0 {
			st.After = last
		}
		ended := n < r.s.cfg.ChunkRows
		if r.until != nil {
			if ended || slices.Equal(st.After, r.until.After) {
				return r.complete(next)
			}
			if err := r.resume(st); err != nil {
				return err
			}
			continue
		}
		var pos *string
		if positioned {
			pos = st.position()
		}
		var added []source.Split
		if keyed {
			if added, err = r.s.resplit(ctx, r.conn, r.t, reading, &st, ended, pos, !positioned); err != nil {
				return err
			}
		}
		if err := r.resume(st, added...); err != nil {
			return err
		}
		if ended {
			return r.complete(next)
		}
	}
}

// incremental reads the rows whose cursor is from the state's floor up to
// the cursor's highest value when the read started, in (cursor, key)
// order. It completes with that value as the next read's floor, so the rows
// that hold it are read again then. Rows with a null cursor aren't read.
func (r *tableRead) incremental(ctx context.Context, cursor string, st sourceState) error {
	ci, err := cursorColumn(r.t, cursor)
	if err != nil {
		return err
	}
	c := r.t.cols[ci]
	if st.Until == nil {
		var until *string
		if err := r.conn.QueryRow(ctx, untilQuery(r.from(), c)).Scan(&until); err != nil {
			return classify(fmt.Errorf("clickhouse: read %s: %w", r.t.name, err))
		}
		if until == nil {
			next, err := json.Marshal(sourceState{Since: st.Since})
			if err != nil {
				return err
			}
			return r.complete(next)
		}
		st = sourceState{Since: st.Since, Until: until, keyRange: st.keyRange}
	}
	bounds := []string{ident(c.name) + " <= " + castBack(c, *st.Until)}
	if st.Since != nil {
		bounds = append(bounds, ident(c.name)+" >= "+castBack(c, *st.Since))
	}
	next, err := json.Marshal(sourceState{Since: st.Until})
	if err != nil {
		return err
	}
	order := []int{ci}
	for _, k := range r.t.key {
		if k != ci {
			order = append(order, k)
		}
	}
	return r.chunks(ctx, st, order, bounds, next)
}

// cursorColumn is the index of a table's column that an incremental read can
// order by.
func cursorColumn(t table, cursor string) (int, error) {
	ci := slices.IndexFunc(t.cols, func(c sourceColumn) bool { return c.name == cursor })
	switch {
	case cursor == "":
		return 0, configInvalid("clickhouse: %s is read incrementally, which needs a cursor column", t.name)
	case ci < 0:
		return 0, configInvalid("clickhouse: %s has no column %q for a cursor", t.name, cursor)
	case !cursorType(t.cols[ci].typ):
		return 0, configInvalid("clickhouse: %s.%s can't be a cursor: that takes a number, date, time or string column", t.name, cursor)
	}
	return ci, nil
}

func configInvalid(format string, args ...any) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf(format, args...))
}

// query runs q and emits its rows. The last keyCols result columns are
// order columns' text; query returns the last row's. Every row is emitted
// before it returns.
func (r *tableRead) query(ctx context.Context, q string, keyCols int) (int, []string, error) {
	b := newBatcher(memory.DefaultAllocator, r.t)
	defer b.release()
	rows, err := r.conn.Query(ctx, q)
	if err != nil {
		return 0, nil, classify(fmt.Errorf("clickhouse: read %s: %w", r.t.name, err))
	}
	defer func() { _ = rows.Close() }()
	types := rows.ColumnTypes()
	dest := make([]any, len(types))
	for i, ct := range types {
		dest[i] = reflect.New(ct.ScanType()).Interface()
	}
	n := 0
	var last []string
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return n, nil, fmt.Errorf("clickhouse: read %s: %w", r.t.name, err)
		}
		if err := b.add(dest[:len(r.t.cols)], r.out); err != nil {
			return n, nil, fmt.Errorf("clickhouse: read %s: %w", r.t.name, err)
		}
		n++
		if b.rows >= r.s.cfg.BatchRows {
			if err := b.flush(r.out); err != nil {
				return n, nil, err
			}
		}
		if keyCols > 0 {
			last = last[:0]
			for _, v := range dest[len(r.t.cols):] {
				s, ok := scannedString(v)
				if !ok {
					return n, nil, fmt.Errorf("clickhouse: read %s: a row's order column is NULL", r.t.name)
				}
				last = append(last, s)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return n, nil, classify(fmt.Errorf("clickhouse: read %s: %w", r.t.name, err))
	}
	return n, slices.Clone(last), b.flush(r.out)
}

// scannedString is the String the driver scanned into v: a *string, or a
// **string for a Nullable one, as a Nullable cursor's text is; false for a
// null.
func scannedString(v any) (string, bool) {
	s := deref(reflect.ValueOf(v))
	if !s.IsValid() || s.Kind() != reflect.String {
		return "", false
	}
	return s.String(), true
}

// batcher builds one table's Arrow batches.
type batcher struct {
	mem    memory.Allocator
	schema *arrow.Schema
	cols   []array.Builder
	// Columns whose values can be past what their type holds.
	checked []int
	rows    int
}

func newBatcher(mem memory.Allocator, t table) *batcher {
	b := &batcher{mem: mem, schema: t.sent()}
	for i, f := range b.schema.Fields() {
		b.cols = append(b.cols, array.NewBuilder(mem, f.Type))
		if canBePast(f.Type) {
			b.checked = append(b.checked, i)
		}
	}
	return b
}

// canBePast says whether a type holds less than ClickHouse's: a time of
// day's 24 hours.
func canBePast(dt arrow.DataType) bool {
	_, ok := dt.(*arrow.Time64Type)
	return ok
}

// add appends a row. A row with a value past its column's type is emitted
// in a batch of its own, with that column's value as text, which the
// engine parses as the declared type and quarantines.
func (b *batcher) add(vals []any, out source.SplitEmitter) error {
	var past map[int]string
	for _, i := range b.checked {
		v := reflect.ValueOf(vals[i])
		if err := fits(b.cols[i].Type(), v); errors.Is(err, errPast) {
			if past == nil {
				past = map[int]string{}
			}
			past[i] = fmt.Sprint(deref(v).Interface())
		} else if err != nil {
			return err
		}
	}
	if past != nil {
		if err := b.flush(out); err != nil {
			return err
		}
	}
	for i, c := range b.cols {
		v := vals[i]
		if _, ok := past[i]; ok {
			v = nil
		}
		if err := appendValue(c, v); err != nil {
			return fmt.Errorf("column %q: %w", b.schema.Field(i).Name, err)
		}
	}
	b.rows++
	if past != nil {
		return b.emit(past, out)
	}
	return nil
}

func (b *batcher) flush(out source.SplitEmitter) error { return b.emit(nil, out) }

// emit sends the rows built so far, with the columns in text replaced by
// strings (one-row batches only).
func (b *batcher) emit(text map[int]string, out source.SplitEmitter) error {
	if b.rows == 0 {
		return nil
	}
	schema := b.schema
	arrs := make([]arrow.Array, len(b.cols))
	for i, c := range b.cols {
		arrs[i] = c.NewArray()
	}
	if text != nil {
		fields := slices.Clone(schema.Fields())
		for i, s := range text {
			sb := array.NewStringBuilder(b.mem)
			sb.Append(s)
			arrs[i].Release()
			arrs[i] = sb.NewArray()
			sb.Release()
			fields[i].Type = arrow.BinaryTypes.String
		}
		schema = arrow.NewSchema(fields, nil)
	}
	rec := array.NewRecordBatch(schema, arrs, int64(b.rows))
	for _, a := range arrs {
		a.Release()
	}
	b.rows = 0
	if err := out.Records(rec); err != nil {
		rec.Release()
		return err
	}
	return nil
}

func (b *batcher) release() {
	for _, c := range b.cols {
		c.Release()
	}
}
