package postgres

import (
	"bytes"
	"context"
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type SourceConfig struct {
	// A connection URL or a keyword/value connection string.
	DSN sdk.Secret `json:"dsn"`
	// Default "public".
	Schema string `json:"schema"`
	// The tables to offer as streams. Empty: every table in the schema.
	Tables []string `json:"tables"`
	// Rows per query on a table with a primary key; each chunk ends at a
	// resume point. Default 100000.
	ChunkRows int `json:"chunkRows"`
	// Approximate bytes of values per Arrow batch. Default 4 MiB.
	BatchBytes int `json:"batchBytes"`
	// The publication that tables read with cdc are in. Created, or given
	// the tables it lacks, if the user may. Default "openetl".
	Publication string `json:"publication"`
	// The replication slot tables read with cdc are read through. Default:
	// one named after the pipeline, openetl_<pipeline>.
	Slot string `json:"slot"`
	// Rows per chunk of a cdc table's snapshot, which is held in memory
	// until the log catches up with it, a chunk per connection reading it.
	// Default 10000.
	SnapshotChunkRows int `json:"snapshotChunkRows"`
	// The most connections a read holds to read tables, as many as it has
	// lanes up to this; a cdc read's replication connection is one more.
	// Default 4.
	MaxConnections int `json:"maxConnections"`
}

// Source reads tables: a full refresh reads a table's rows, an incremental
// read the rows from a cursor column's floor on, and cdc reads its changes
// through a logical replication slot (cdc.go). A table with a primary key is
// read in key order, or (cursor, key) order, a chunk per query, so a resumed
// read yields exactly the rows after its last chunk, within its key range if
// it's read in splits (source_splits.go). A table without one is read in a
// single query.
type Source struct {
	cfg  SourceConfig
	conn *pgx.ConnConfig

	mu sync.Mutex
	// The cdc read running, which Committed acknowledges on.
	reader *cdcReader
	// The keyed splits being read, which Resplit splits.
	reading map[readingKey]*readingSplit
}

var _ source.Source = (*Source)(nil)

func NewSource(config []byte) (*Source, error) {
	cfg := SourceConfig{Schema: "public", ChunkRows: 100_000, BatchBytes: 4 << 20, Publication: "openetl", SnapshotChunkRows: 10_000, MaxConnections: 4}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("postgres: config: %w", err)
	}
	switch {
	case cfg.DSN.IsZero():
		return nil, errors.New("postgres: config needs a dsn")
	case cfg.ChunkRows <= 0 || cfg.BatchBytes <= 0 || cfg.SnapshotChunkRows <= 0 || cfg.MaxConnections <= 0:
		return nil, errors.New("postgres: chunkRows, batchBytes, snapshotChunkRows and maxConnections must be positive")
	case cfg.Publication == "":
		return nil, errors.New("postgres: publication can't be empty")
	case len(cfg.Slot) > 63 || strings.Trim(cfg.Slot, "abcdefghijklmnopqrstuvwxyz0123456789_") != "":
		return nil, errors.New("postgres: slot names are up to 63 lowercase letters, digits and underscores")
	}
	cc, err := pgx.ParseConfig(cfg.DSN.Reveal())
	if err != nil {
		return nil, fmt.Errorf("postgres: dsn: %w", err)
	}
	// Text values, and the keys that resume a read, are spelled in these.
	for k, v := range map[string]string{"DateStyle": "ISO, YMD", "TimeZone": "UTC", "IntervalStyle": "iso_8601", "extra_float_digits": "3", "bytea_output": "hex"} {
		cc.RuntimeParams[k] = v
	}
	return &Source{cfg: cfg, conn: cc}, nil
}

type table struct {
	name string
	cols []sourceColumn
	// Indexes into cols, in key order.
	key []int
	// pg_class.relreplident: what the log says of an update's or delete's
	// old row.
	replident byte
}

type sourceColumn struct {
	name   string
	oid    uint32
	typmod int32
}

func (s *Source) Discover(ctx context.Context) (source.Catalog, error) {
	conn, err := pgx.ConnectConfig(ctx, s.conn)
	if err != nil {
		return source.Catalog{}, classify(fmt.Errorf("postgres: connect: %w", err))
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	tables, err := s.tables(ctx, conn)
	if err != nil {
		return source.Catalog{}, err
	}
	var c source.Catalog
	for _, t := range tables {
		b, err := newBatcher(memory.DefaultAllocator, t.cols)
		if err != nil {
			return source.Catalog{}, fmt.Errorf("postgres: %s: %w", t.name, err)
		}
		b.release()
		cs := source.CatalogStream{Name: t.name, SplitsByKey: len(t.key) > 0, Replay: source.Exact, Schema: b.declared, SyncModes: []source.SyncMode{source.FullRefresh}}
		if slices.ContainsFunc(t.cols, func(c sourceColumn) bool { return cursorTypes[c.oid] }) {
			cs.SyncModes = append(cs.SyncModes, source.Incremental)
		}
		// The log names an update's or delete's row by its primary key, or
		// with replica identity full by all of it.
		if len(t.key) > 0 && (t.replident == 'd' || t.replident == 'f') {
			cs.SyncModes, cs.Seq = append(cs.SyncModes, source.CDC), seqType
		}
		for _, i := range t.key {
			cs.PrimaryKey = append(cs.PrimaryKey, t.cols[i].name)
		}
		c.Streams = append(c.Streams, cs)
	}
	return c, nil
}

// tables lists the configured tables, in name order.
func (s *Source) tables(ctx context.Context, conn *pgx.Conn) ([]table, error) {
	rows, err := conn.Query(ctx, `SELECT c.relname, c.relreplident::text, a.attname, a.atttypid, a.atttypmod,
			coalesce((SELECT k.ord FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) WHERE k.attnum = a.attnum), 0)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary
		WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND NOT c.relispartition
			AND (coalesce(cardinality($2::text[]), 0) = 0 OR c.relname = ANY($2))
		ORDER BY c.relname, a.attnum`, s.cfg.Schema, s.cfg.Tables)
	if err != nil {
		return nil, fmt.Errorf("postgres: list tables: %w", err)
	}
	var out []table
	var keyPos []int
	finish := func() {
		if len(out) == 0 {
			return
		}
		t := &out[len(out)-1]
		for pos := 1; ; pos++ {
			i := indexOf(keyPos, pos)
			if i < 0 {
				break
			}
			t.key = append(t.key, i)
		}
	}
	for rows.Next() {
		var name, replident string
		var c sourceColumn
		var pos int64
		if err := rows.Scan(&name, &replident, &c.name, &c.oid, &c.typmod, &pos); err != nil {
			return nil, fmt.Errorf("postgres: list tables: %w", err)
		}
		if len(out) == 0 || out[len(out)-1].name != name {
			finish()
			out = append(out, table{name: name, replident: replident[0]})
			keyPos = keyPos[:0]
		}
		t := &out[len(out)-1]
		t.cols = append(t.cols, c)
		keyPos = append(keyPos, int(pos))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list tables: %w", err)
	}
	finish()
	for _, want := range s.cfg.Tables {
		if !hasTable(out, want) {
			return nil, fmt.Errorf("postgres: no table %q in schema %q", want, s.cfg.Schema)
		}
	}
	return out, nil
}

func indexOf(xs []int, x int) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return -1
}

func hasTable(ts []table, name string) bool {
	for _, t := range ts {
		if t.name == name {
			return true
		}
	}
	return false
}

type sourceState struct {
	// The primary key of the last row read, spelled as text; in an
	// incremental read, its cursor value first.
	After []string `json:"after,omitempty"`
	// Set once a table without a primary key has been read.
	Done bool `json:"done,omitempty"`
	// An incremental read's cursor floor, inclusive, and the highest value
	// it reads, set when it starts, as text. A completed read's Until is the
	// next read's Since.
	Since *string `json:"since,omitempty"`
	Until *string `json:"until,omitempty"`
	// A split's key range.
	keyRange
}

// The column types an incremental read can order by.
var cursorTypes = map[uint32]bool{
	pgtype.Int2OID: true, pgtype.Int4OID: true, pgtype.Int8OID: true, pgtype.NumericOID: true, pgtype.DateOID: true,
	pgtype.TimestampOID: true, pgtype.TimestamptzOID: true, pgtype.TextOID: true, pgtype.VarcharOID: true,
}

func (s *Source) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	limit := min(max(req.Lanes, 1), s.cfg.MaxConnections)
	conns := newConnPool(s.conn, limit)
	defer conns.closeIdle(ctx)
	conn, err := conns.get(ctx)
	if err != nil {
		return err
	}
	tables, err := s.tables(ctx, conn)
	conns.put(ctx, conn)
	if err != nil {
		return err
	}
	var cdc, rest []source.ConfiguredStream
	for _, cs := range req.Streams {
		switch {
		case !hasTable(tables, cs.Name):
			return fmt.Errorf("postgres: unknown stream %q", cs.Name)
		case cs.SyncMode == source.CDC:
			cdc = append(cdc, cs)
		default:
			rest = append(rest, cs)
		}
	}
	readRest := func(ctx context.Context, lanes int) error {
		return source.ReadSplits(ctx, source.ReadRequest{Streams: rest, Lanes: lanes}, out, func(ctx context.Context, cs source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
			return s.readSplit(ctx, conns, tables, cs, sp, e)
		})
	}
	if len(cdc) == 0 {
		return readRest(ctx, limit)
	}
	// With one connection the log waits for the tables, since it never ends.
	if limit == 1 {
		if err := readRest(ctx, 1); err != nil {
			return err
		}
		return s.readCDC(ctx, conns, req, cdc, tables, out)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := readRest(ctx, limit-1); err != nil {
			cancel(err)
		}
	})
	if err := s.readCDC(ctx, conns, req, cdc, tables, out); err != nil {
		cancel(err)
	}
	wg.Wait()
	return context.Cause(ctx)
}

// readSplit reads one split of a table read in full or incrementally, on a
// connection of its own.
func (s *Source) readSplit(ctx context.Context, conns *connPool, tables []table, cs source.ConfiguredStream, sp source.Split, out source.SplitEmitter) error {
	var st sourceState
	if sp.State != nil {
		if err := json.Unmarshal(sp.State, &st); err != nil {
			return fmt.Errorf("postgres: %s state: %w", cs.Name, err)
		}
	}
	conn, err := conns.get(ctx)
	if err != nil {
		return err
	}
	defer conns.put(ctx, conn)
	t := tables[slices.IndexFunc(tables, func(t table) bool { return t.name == cs.Name })]
	if cs.SyncMode == source.Incremental {
		return s.readIncremental(ctx, conn, t, sp.ID, cs.Cursor, st, out)
	}
	return s.readTable(ctx, conn, t, sp.ID, st, out)
}

func (s *Source) readTable(ctx context.Context, conn *pgx.Conn, t table, id string, st sourceState, out source.SplitEmitter) error {
	b, err := newBatcher(memory.DefaultAllocator, t.cols)
	if err != nil {
		return fmt.Errorf("postgres: %s: %w", t.name, err)
	}
	defer b.release()
	ident := pgx.Identifier{s.cfg.Schema, t.name}.Sanitize()
	cols := make([]string, len(t.cols))
	for i, c := range t.cols {
		cols[i] = pgx.Identifier{c.name}.Sanitize()
	}
	list := strings.Join(cols, ", ")

	if len(t.key) == 0 {
		if !st.Done {
			if _, _, err := s.query(ctx, conn, b, t.name, "SELECT "+list+" FROM "+ident, nil, 0, out); err != nil {
				return err
			}
			if err := resume(out, sourceState{Done: true}); err != nil {
				return err
			}
		}
		return out.Complete(nil)
	}

	key := make([]string, len(t.key))
	keyText := make([]string, len(t.key))
	for i, k := range t.key {
		key[i] = cols[k]
		keyText[i] = cols[k] + "::text AS _openetl_key_" + strconv.Itoa(i)
	}
	head := "SELECT " + list + ", " + strings.Join(keyText, ", ") + " FROM " + ident
	tail := " ORDER BY " + strings.Join(key, ", ") + " LIMIT " + strconv.Itoa(s.cfg.ChunkRows)
	if st.After != nil && len(st.After) != len(t.key) {
		return fmt.Errorf("postgres: %s state has %d key columns, the table has %d", t.name, len(st.After), len(t.key))
	}
	st = sourceState{After: st.After, keyRange: st.keyRange}
	reading := s.startSplit(t.name, id)
	defer s.endSplit(t.name, id)
	for {
		var args []string
		arg := func(v string) string {
			args = append(args, v)
			return "$" + strconv.Itoa(len(args))
		}
		where := st.where(key[0], arg)
		if st.After != nil {
			where = append(where, afterKey(key, st.After, arg))
		}
		sql := head
		if len(where) > 0 {
			sql += " WHERE " + strings.Join(where, " AND ")
		}
		n, last, err := s.query(ctx, conn, b, t.name, sql+tail, args, len(t.key), out)
		if err != nil {
			return err
		}
		if n > 0 {
			st.After = last
		}
		ended := n < s.cfg.ChunkRows
		added, err := s.resplit(ctx, conn, t, reading, &st, ended, st.position(), false)
		if err != nil {
			return err
		}
		if err := resume(out, st, added...); err != nil {
			return err
		}
		if ended {
			return out.Complete(nil)
		}
	}
}

// afterKey says a row is after a position in the order of key.
func afterKey(key, after []string, arg func(string) string) string {
	params := make([]string, len(after))
	for i, v := range after {
		params[i] = arg(v)
	}
	return "(" + strings.Join(key, ", ") + ") > (" + strings.Join(params, ", ") + ")"
}

// readIncremental reads the rows whose cursor is from the state's floor up
// to the cursor's highest value when the read started, in (cursor, key)
// order. It completes with that value as the next read's floor, so the rows
// that hold it are read again then. Rows with a NULL cursor aren't read.
func (s *Source) readIncremental(ctx context.Context, conn *pgx.Conn, t table, id, cursor string, st sourceState, out source.SplitEmitter) error {
	ci, err := cursorColumn(t, cursor)
	if err != nil {
		return err
	}
	b, err := newBatcher(memory.DefaultAllocator, t.cols)
	if err != nil {
		return fmt.Errorf("postgres: %s: %w", t.name, err)
	}
	defer b.release()
	ident := pgx.Identifier{s.cfg.Schema, t.name}.Sanitize()
	cols := make([]string, len(t.cols))
	for i, c := range t.cols {
		cols[i] = pgx.Identifier{c.name}.Sanitize()
	}
	if st.Until == nil {
		var until *string
		if err := conn.QueryRow(ctx, "SELECT max("+cols[ci]+")::text FROM "+ident).Scan(&until); err != nil {
			return classify(fmt.Errorf("postgres: read %s: %w", t.name, err))
		}
		if until == nil {
			return complete(out, sourceState{Since: st.Since})
		}
		st = sourceState{Since: st.Since, Until: until}
	}
	var args []string
	arg := func(v string) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	bounds := func() string {
		args = args[:0]
		w := cols[ci] + " <= " + arg(*st.Until)
		if st.Since != nil {
			w += " AND " + cols[ci] + " >= " + arg(*st.Since)
		}
		return w
	}
	list := strings.Join(cols, ", ")

	if len(t.key) == 0 {
		if !st.Done {
			if _, _, err := s.query(ctx, conn, b, t.name, "SELECT "+list+" FROM "+ident+" WHERE "+bounds(), args, 0, out); err != nil {
				return err
			}
			st.Done = true
			if err := resume(out, st); err != nil {
				return err
			}
		}
		return complete(out, sourceState{Since: st.Until})
	}

	order := []string{cols[ci]}
	for _, k := range t.key {
		if k != ci {
			order = append(order, cols[k])
		}
	}
	text := make([]string, len(order))
	for i, c := range order {
		text[i] = c + "::text AS _openetl_key_" + strconv.Itoa(i)
	}
	head := "SELECT " + list + ", " + strings.Join(text, ", ") + " FROM " + ident + " WHERE "
	tail := " ORDER BY " + strings.Join(order, ", ") + " LIMIT " + strconv.Itoa(s.cfg.ChunkRows)
	if st.After != nil && len(st.After) != len(order) {
		return fmt.Errorf("postgres: %s state has %d cursor and key columns, the read has %d", t.name, len(st.After), len(order))
	}
	first := pgx.Identifier{t.cols[t.key[0]].name}.Sanitize()
	reading := s.startSplit(t.name, id)
	defer s.endSplit(t.name, id)
	for {
		where := []string{bounds()}
		where = append(where, st.where(first, arg)...)
		if st.After != nil {
			where = append(where, afterKey(order, st.After, arg))
		}
		n, last, err := s.query(ctx, conn, b, t.name, head+strings.Join(where, " AND ")+tail, args, len(order), out)
		if err != nil {
			return err
		}
		if n > 0 {
			st.After = last
		}
		ended := n < s.cfg.ChunkRows
		// The position follows the key only where the cursor is its first
		// column.
		var pos *string
		if t.key[0] == ci {
			pos = st.position()
		}
		added, err := s.resplit(ctx, conn, t, reading, &st, ended, pos, true)
		if err != nil {
			return err
		}
		if err := resume(out, st, added...); err != nil {
			return err
		}
		if ended {
			return complete(out, sourceState{Since: st.Until})
		}
	}
}

// cursorColumn is the index of a table's column that an incremental read can
// order by.
func cursorColumn(t table, cursor string) (int, error) {
	ci := slices.IndexFunc(t.cols, func(c sourceColumn) bool { return c.name == cursor })
	switch {
	case cursor == "":
		return 0, configInvalid("postgres: %s is read incrementally, which needs a cursor column", t.name)
	case ci < 0:
		return 0, configInvalid("postgres: %s has no column %q for a cursor", t.name, cursor)
	case !cursorTypes[t.cols[ci].oid]:
		return 0, configInvalid("postgres: %s.%s can't be a cursor: that takes an integer, numeric, date, timestamp, timestamptz, text or varchar column", t.name, cursor)
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

// query runs sql and emits its rows. The last keyCols result columns are the
// key as text; query returns the last row's. Every row is emitted before it
// returns.
func (s *Source) query(ctx context.Context, conn *pgx.Conn, b *batcher, stream, sql string, args []string, keyCols int, out source.SplitEmitter) (int, []string, error) {
	params := make([][]byte, len(args))
	for i, a := range args {
		params[i] = []byte(a)
	}
	formats := make([]int16, 0, len(b.cols)+keyCols)
	for _, c := range b.cols {
		formats = append(formats, c.format)
	}
	for range keyCols {
		formats = append(formats, pgx.TextFormatCode)
	}
	rr := conn.PgConn().ExecParams(ctx, sql, params, nil, nil, formats)
	n := 0
	var last []string
	var err error
	for rr.NextRow() {
		vals := rr.Values()
		if text := b.past(vals[:len(b.cols)]); text != nil {
			err = b.alone(vals[:len(b.cols)], text, out)
		} else {
			err = b.add(vals[:len(b.cols)])
		}
		if err != nil {
			break
		}
		n++
		if b.bytes >= s.cfg.BatchBytes {
			if err = b.flush(out); err != nil {
				break
			}
		}
		if keyCols > 0 {
			last = last[:0]
			for _, v := range vals[len(b.cols):] {
				last = append(last, string(v))
			}
		}
	}
	if _, cerr := rr.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("postgres: read %s: %w", stream, cerr)
	}
	if err == nil {
		err = b.flush(out)
	}
	return n, last, err
}

func resume(out source.SplitEmitter, st sourceState, added ...source.Split) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return out.ResumePoint(b, added...)
}

// batcher builds one table's Arrow batches.
type batcher struct {
	mem    memory.Allocator
	schema *arrow.Schema
	// schema with each column's logical type.
	declared *arrow.Schema
	cols     []*colBuilder
	// Columns whose values can be past what their builder's type holds.
	checked []int
	rows    int
	bytes   int
}

func newBatcher(mem memory.Allocator, cols []sourceColumn) (*batcher, error) {
	b := &batcher{mem: mem}
	fields := make([]arrow.Field, len(cols))
	declared := make([]arrow.Field, len(cols))
	for i, c := range cols {
		cb, err := newColBuilder(mem, c)
		if err != nil {
			b.release()
			return nil, err
		}
		b.cols = append(b.cols, cb)
		if cb.past != nil {
			b.checked = append(b.checked, i)
		}
		fields[i] = arrow.Field{Name: c.name, Type: cb.b.Type(), Nullable: true}
		declared[i] = fields[i]
		if cb.declared != nil {
			declared[i].Type = cb.declared
		}
	}
	b.schema = arrow.NewSchema(fields, nil)
	b.declared = arrow.NewSchema(declared, nil)
	return b, nil
}

func (b *batcher) add(vals [][]byte) error {
	for i, c := range b.cols {
		v := vals[i]
		if v == nil {
			c.b.AppendNull()
			continue
		}
		if err := c.add(v); err != nil {
			return fmt.Errorf("postgres: column %q: %w", b.schema.Field(i).Name, err)
		}
		b.bytes += len(v)
	}
	b.rows++
	return nil
}

// past returns the row's values that their columns' types can't hold, as
// text by column, or nil.
func (b *batcher) past(vals [][]byte) map[int]string {
	var text map[int]string
	for _, i := range b.checked {
		if s, ok := b.cols[i].past(vals[i]); ok {
			if text == nil {
				text = map[int]string{}
			}
			text[i] = s
		}
	}
	return text
}

// alone emits a row in a batch of its own, with text in place of the values
// its columns' types can't hold. The engine parses the text as the declared
// type and quarantines the row.
func (b *batcher) alone(vals [][]byte, text map[int]string, out source.SplitEmitter) error {
	if err := b.flush(out); err != nil {
		return err
	}
	vals = slices.Clone(vals)
	for i := range text {
		vals[i] = nil
	}
	if err := b.add(vals); err != nil {
		return err
	}
	return b.emit(text, out)
}

func (b *batcher) flush(out source.SplitEmitter) error {
	return b.emit(nil, out)
}

// emit sends the rows built so far, with the columns in text replaced by
// strings (one-row batches only).
func (b *batcher) emit(text map[int]string, out source.SplitEmitter) error {
	if b.rows == 0 {
		return nil
	}
	schema := b.schema
	arrs := make([]arrow.Array, len(b.cols))
	for i, c := range b.cols {
		arrs[i] = c.b.NewArray()
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
}
