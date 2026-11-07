package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	_ source.Splitter   = (*Source)(nil)
	_ source.Resplitter = (*Source)(nil)
)

// The most key ranges a table's read in.
const maxKeyRanges = 32

// keyRange is a range of a key's first column, spelled as text: above Lo, up
// to and including Hi. Nil is no bound.
type keyRange struct {
	Lo *string `json:"lo,omitempty"`
	Hi *string `json:"hi,omitempty"`
}

// where are the conditions on col, the key's first column, of being in the
// range.
func (k keyRange) where(col string, arg func(string) string) []string {
	var w []string
	if k.Lo != nil {
		w = append(w, col+" > "+arg(*k.Lo))
	}
	if k.Hi != nil {
		w = append(w, col+" <= "+arg(*k.Hi))
	}
	return w
}

// Splits divides a keyed table read in full or incrementally into key ranges,
// from its statistics. Small tables, and those without statistics, are read
// whole. An incremental read's ranges share the highest cursor value it reads
// and its floor, where the last read's splits all ended.
func (s *Source) Splits(ctx context.Context, cs source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	if cs.SyncMode == source.CDC {
		// A state from a read in splits starts the table again, as one from a
		// full refresh does.
		if whole, err := source.Whole(ended); err == nil {
			return whole, nil
		}
		return []source.Split{{ID: source.WholeStream}}, nil
	}
	conn, err := pgx.ConnectConfig(ctx, s.conn)
	if err != nil {
		return nil, classify(fmt.Errorf("postgres: connect: %w", err))
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	tables, err := s.tables(ctx, conn)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(tables, func(t table) bool { return t.name == cs.Name })
	if i < 0 {
		return nil, fmt.Errorf("postgres: unknown stream %q", cs.Name)
	}
	t := tables[i]
	var st sourceState
	ci := -1
	if cs.SyncMode == source.Incremental {
		if ci, err = cursorColumn(t, cs.Cursor); err != nil {
			return nil, err
		}
		if st.Since, err = floorOf(cs.Name, ended); err != nil {
			return nil, err
		}
	}
	ranges, err := s.keyRanges(ctx, conn, t, s.cfg.ChunkRows)
	if err != nil {
		return nil, err
	}
	if len(ranges) > 0 && ci >= 0 {
		col := pgx.Identifier{t.cols[ci].name}.Sanitize()
		err := conn.QueryRow(ctx, "SELECT max("+col+")::text FROM "+pgx.Identifier{s.cfg.Schema, t.name}.Sanitize()).Scan(&st.Until)
		if err != nil {
			return nil, classify(fmt.Errorf("postgres: read %s: %w", t.name, err))
		}
		if st.Until == nil {
			ranges = nil
		}
	}
	if len(ranges) == 0 {
		if whole, err := source.Whole(ended); err == nil {
			return whole, nil
		}
		state, err := json.Marshal(st)
		if err != nil {
			return nil, err
		}
		return []source.Split{{ID: source.WholeStream, State: state}}, nil
	}
	splits := make([]source.Split, len(ranges))
	for i, r := range ranges {
		st.keyRange = r
		state, err := json.Marshal(st)
		if err != nil {
			return nil, err
		}
		splits[i] = source.Split{ID: strconv.Itoa(i), State: state}
	}
	return splits, nil
}

// floorOf is the floor an incremental read starts from, where its last read's
// splits ended: one floor, which each of them carries.
func floorOf(stream string, ended []source.Split) (*string, error) {
	for _, sp := range ended {
		if sp.State == nil {
			continue
		}
		var st sourceState
		if err := json.Unmarshal(sp.State, &st); err != nil {
			return nil, fmt.Errorf("postgres: %s state: %w", stream, err)
		}
		if st.Since != nil {
			return st.Since, nil
		}
	}
	return nil, nil
}

// keyRanges divides a table into contiguous ranges of its key's first column
// of about rows rows each, none if it's smaller or has no statistics. The
// first has no lower bound and the last no upper, so together they cover
// every key.
func (s *Source) keyRanges(ctx context.Context, conn *pgx.Conn, t table, rows int) ([]keyRange, error) {
	if len(t.key) == 0 {
		return nil, nil
	}
	stats, err := s.keyStats(ctx, conn, t)
	if err != nil {
		return nil, err
	}
	return cutRanges(stats.bounds, stats.rows, rows), nil
}

// cutRanges cuts at bounds, a histogram's in order, evenly: each of its
// buckets holds about as many rows.
func cutRanges(bounds []string, tuples float64, rows int) []keyRange {
	bounds = slices.Compact(slices.Clone(bounds))
	n := min(int(math.Min(math.Ceil(tuples/float64(rows)), maxKeyRanges)), len(bounds)-1)
	if n < 2 {
		return nil
	}
	out := make([]keyRange, n)
	for i := 1; i < n; i++ {
		b := &bounds[i*(len(bounds)-1)/n]
		out[i-1].Hi, out[i].Lo = b, b
	}
	return out
}

// keyStats is what the server's statistics say of a table's key's first
// column: all but rows are empty without statistics.
type keyStats struct {
	// pg_class.reltuples.
	rows float64
	// pg_stats.histogram_bounds, as text, in the column's order.
	bounds []string
	// The column's type and collation in SQL, the latter empty if it has none.
	typ, collation string
}

func (s *Source) keyStats(ctx context.Context, conn *pgx.Conn, t table) (keyStats, error) {
	var st keyStats
	// A table's inherited statistics, where it has them, cover what a read of
	// it reads.
	err := conn.QueryRow(ctx, `SELECT c.reltuples::float8, coalesce(st.histogram_bounds::text::text[], '{}'), format_type(a.atttypid, a.atttypmod),
			coalesce(quote_ident(cn.nspname) || '.' || quote_ident(co.collname), '')
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = $3
		JOIN pg_stats st ON st.schemaname = n.nspname AND st.tablename = c.relname AND st.attname = a.attname
		LEFT JOIN pg_collation co ON co.oid = a.attcollation
		LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace
		WHERE n.nspname = $1 AND c.relname = $2
		ORDER BY st.inherited DESC LIMIT 1`, s.cfg.Schema, t.name, t.cols[t.key[0]].name).Scan(&st.rows, &st.bounds, &st.typ, &st.collation)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, nil
	}
	return st, statsFailed(t, err)
}

// statsFailed passes on what stops the read: statistics the server won't give
// only leave the table unsplit.
func statsFailed(t table, err error) error {
	if _, ok := errors.AsType[*pgconn.PgError](err); ok {
		slog.Warn("reading a table's statistics failed, so it isn't split", "table", t.name, "err", err)
		return nil
	}
	return err
}

// boundsInside are the histogram's bounds of a table's key's first column
// strictly within the range, in order.
func (s *Source) boundsInside(ctx context.Context, conn *pgx.Conn, t table, r keyRange) ([]string, error) {
	st, err := s.keyStats(ctx, conn, t)
	if err != nil || len(st.bounds) == 0 {
		return nil, err
	}
	v := "(v::" + st.typ + ")"
	if st.collation != "" {
		v += " COLLATE " + st.collation
	}
	args := []any{st.bounds}
	q := "SELECT v FROM unnest($1::text[]) WITH ORDINALITY AS u(v, ord) WHERE true"
	if r.Lo != nil {
		args = append(args, *r.Lo)
		q += " AND " + v + " > ($" + strconv.Itoa(len(args)) + "::text)::" + st.typ
	}
	if r.Hi != nil {
		args = append(args, *r.Hi)
		q += " AND " + v + " < ($" + strconv.Itoa(len(args)) + "::text)::" + st.typ
	}
	rows, err := conn.Query(ctx, q+" ORDER BY ord", args...)
	if err != nil {
		return nil, statsFailed(t, err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	return out, statsFailed(t, err)
}

type readingKey struct{ stream, split string }

// readingSplit is a keyed split being read.
type readingSplit struct {
	// Resplit asked, and the split's next resume point answers.
	asked atomic.Bool
	// It can't be split: Resplit refuses.
	over atomic.Bool
}

func (s *Source) startSplit(stream, id string) *readingSplit {
	r := &readingSplit{}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reading == nil {
		s.reading = map[readingKey]*readingSplit{}
	}
	s.reading[readingKey{stream, id}] = r
	return r
}

func (s *Source) endSplit(stream, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.reading, readingKey{stream, id})
}

// Resplit asks a keyed split being read to split its remainder at its next
// resume point, which works out where. It refuses a split it knows it can't
// split, or that's already been asked.
func (s *Source) Resplit(_ context.Context, stream, split string) (bool, error) {
	s.mu.Lock()
	r := s.reading[readingKey{stream, split}]
	s.mu.Unlock()
	if r == nil || r.over.Load() {
		return false, nil
	}
	return r.asked.CompareAndSwap(false, true), nil
}

// position is where a key-ordered read has reached in the key's first column,
// nil before its first row.
func (st sourceState) position() *string {
	if len(st.After) == 0 {
		return nil
	}
	return &st.After[0]
}

// resplit runs at each resume point of a keyed split, after the chunk that
// reached st. If it was asked, it divides the remainder at a midpoint worked
// out from there, so what any chunk read before it stays below: st keeps the
// lower part, and the split to add takes the upper. pos is st's position, if
// it says how far the key has been read; keep has the new split keep st's
// position, where rows of any key can follow it. ended: the chunk was its
// last.
func (s *Source) resplit(ctx context.Context, conn *pgx.Conn, t table, r *readingSplit, st *sourceState, ended bool, pos *string, keep bool) ([]source.Split, error) {
	if ended {
		r.over.Store(true)
		return nil, nil
	}
	if !r.asked.Swap(false) {
		if intKey[t.cols[t.key[0]].oid] {
			low, high, err := intRemainder(st.keyRange, pos, len(t.key) == 1)
			if err != nil {
				return nil, err
			}
			if low != nil && high != nil && low.Cmp(high) >= 0 {
				r.over.Store(true)
			}
		}
		return nil, nil
	}
	mid, err := s.midpoint(ctx, conn, t, st.keyRange, pos)
	if err != nil {
		return nil, err
	}
	if mid == nil {
		r.over.Store(true)
		return nil, nil
	}
	upper := *st
	upper.Lo = mid
	if !keep {
		upper.After = nil
	}
	state, err := json.Marshal(upper)
	if err != nil {
		return nil, err
	}
	st.Hi = mid
	id := sha256.Sum256([]byte(*mid))
	return []source.Split{{ID: hex.EncodeToString(id[:6]), State: state}}, nil
}

// The types of a key's first column whose ranges are divided by arithmetic.
var intKey = map[uint32]bool{pgtype.Int2OID: true, pgtype.Int4OID: true, pgtype.Int8OID: true}

// midpoint is where to divide a range's remainder, nil if nowhere: the upper
// part starts above it, and each part has keys to hold.
func (s *Source) midpoint(ctx context.Context, conn *pgx.Conn, t table, r keyRange, pos *string) (*string, error) {
	first := t.cols[t.key[0]]
	if !intKey[first.oid] {
		if pos != nil {
			r.Lo = pos
		}
		bounds, err := s.boundsInside(ctx, conn, t, r)
		if err != nil || len(bounds) == 0 {
			return nil, err
		}
		return &bounds[len(bounds)/2], nil
	}
	low, high, err := intRemainder(r, pos, len(t.key) == 1)
	if err != nil {
		return nil, err
	}
	if low == nil || high == nil {
		var least, most *string
		col := pgx.Identifier{first.name}.Sanitize()
		q := "SELECT min(" + col + ")::text, max(" + col + ")::text FROM " + pgx.Identifier{s.cfg.Schema, t.name}.Sanitize()
		if err := conn.QueryRow(ctx, q).Scan(&least, &most); err != nil {
			return nil, classify(fmt.Errorf("postgres: read %s: %w", t.name, err))
		}
		if least == nil {
			return nil, nil
		}
		if low == nil {
			if low, err = parseInt(*least); err != nil {
				return nil, err
			}
		}
		if high == nil {
			if high, err = parseInt(*most); err != nil {
				return nil, err
			}
		}
	}
	if low.Cmp(high) >= 0 {
		return nil, nil
	}
	mid := new(big.Int).Sub(high, low)
	mid.Rsh(mid, 1).Add(mid, low)
	text := mid.String()
	return &text, nil
}

// intRemainder is the least and most an integer key's first column can hold
// in what's left of a range, nil where only the table can say. A position in
// a key of one column is that key read; in a longer one, its value has more
// keys to come.
func intRemainder(r keyRange, pos *string, single bool) (low, high *big.Int, err error) {
	switch {
	case pos != nil:
		if low, err = parseInt(*pos); err != nil {
			return nil, nil, err
		}
		if single {
			low.Add(low, big.NewInt(1))
		}
	case r.Lo != nil:
		if low, err = parseInt(*r.Lo); err != nil {
			return nil, nil, err
		}
		low.Add(low, big.NewInt(1))
	}
	if r.Hi != nil {
		if high, err = parseInt(*r.Hi); err != nil {
			return nil, nil, err
		}
	}
	return low, high, nil
}

func parseInt(s string) (*big.Int, error) {
	v, ok := new(big.Int).SetString(strings.TrimSpace(s), 10)
	if !ok {
		return nil, fmt.Errorf("postgres: %q as a key's integer", s)
	}
	return v, nil
}

// connPool is a read's connections to the source: at most limit are held at a
// time, and one given back is reused.
type connPool struct {
	config *pgx.ConnConfig
	limit  int
	held   chan struct{}

	mu   sync.Mutex
	idle []*pgx.Conn
	// Places in held given up, since the server had no more connections.
	lost int
}

func newConnPool(config *pgx.ConnConfig, limit int) *connPool {
	return &connPool{config: config, limit: limit, held: make(chan struct{}, limit)}
}

// get waits until the read can hold another connection. If the server has
// none to give while the read holds others, it reads with those.
func (p *connPool) get(ctx context.Context) (*pgx.Conn, error) {
	for {
		select {
		case p.held <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		c, err := p.open(ctx)
		if err == nil {
			return c, nil
		}
		if !p.giveUp(err) {
			<-p.held
			return nil, classify(fmt.Errorf("postgres: connect: %w", err))
		}
	}
}

// tryGet returns nil if the read holds as many as it may, or can't have more.
func (p *connPool) tryGet(ctx context.Context) *pgx.Conn {
	select {
	case p.held <- struct{}{}:
	default:
		return nil
	}
	c, err := p.open(ctx)
	if err != nil {
		if !p.giveUp(err) {
			<-p.held
		}
		return nil
	}
	return c
}

func (p *connPool) open(ctx context.Context) (*pgx.Conn, error) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		c := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	return pgx.ConnectConfig(ctx, p.config)
}

// giveUp keeps the place of a connection the server refused for having too
// many, if the read holds another, or leaves it if one was given back since.
func (p *connPool) giveUp(err error) bool {
	pe, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pe.Code != "53300" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case len(p.held)-p.lost > 1:
		p.lost++
	case len(p.idle) > 0:
		<-p.held
	default:
		return false
	}
	return true
}

// put gives a connection back; one left in a transaction, or broken, is
// closed.
func (p *connPool) put(ctx context.Context, c *pgx.Conn) {
	if c.IsClosed() || c.PgConn().TxStatus() != 'I' {
		_ = c.Close(context.WithoutCancel(ctx))
	} else {
		p.mu.Lock()
		p.idle = append(p.idle, c)
		p.mu.Unlock()
	}
	<-p.held
}

// closeIdle closes the connections not held.
func (p *connPool) closeIdle(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.idle {
		_ = c.Close(context.WithoutCancel(ctx))
	}
	p.idle = nil
}
