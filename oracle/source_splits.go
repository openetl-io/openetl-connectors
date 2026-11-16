package oracle

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	_ source.Splitter   = (*Source)(nil)
	_ source.Resplitter = (*Source)(nil)
)

// The most key ranges a table's read in.
const maxKeyRanges = 32

// keyRange is a range of a NUMBER key's first column, its bounds integers
// spelled as decimal text: above Lo, up to and including Hi. Nil is no
// bound.
type keyRange struct {
	Lo *string `json:"lo,omitempty"`
	Hi *string `json:"hi,omitempty"`
}

// where adds the conditions on c, the key's first column, of being in the
// range.
func (k keyRange) where(c column, w *where) error {
	if k.Lo != nil {
		if err := w.add(c, ">", *k.Lo); err != nil {
			return err
		}
	}
	if k.Hi != nil {
		return w.add(c, "<=", *k.Hi)
	}
	return nil
}

// splittable says a table is read in key ranges: its key orders as it
// compares, and its first column is a NUMBER, of any scale, which ranges
// with integer bounds divide.
func splittable(t table) bool { return chunked(t) && bindOf(t.cols[t.key[0]]) == bindNumber }

// Splits divides a table read in full or incrementally whose key starts with
// a NUMBER column into ranges of that column, evenly between its least and
// greatest values, as many as the table's rows fill at chunkRows each, by
// its statistics or else a count. Small tables are read whole. An
// incremental read's ranges share the highest cursor value it reads and its
// floor, where the last read's splits all ended.
func (s *Source) Splits(ctx context.Context, cs source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	if cs.SyncMode == source.CDC {
		// A state from a read in splits starts the table again, as one from a
		// full refresh does.
		if whole, err := source.Whole(ended); err == nil {
			return whole, nil
		}
		return []source.Split{{ID: source.WholeStream}}, nil
	}
	db, err := open(ctx, s.cfg.DSN.Reveal(), 1)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	tables, schema, err := s.tables(ctx, db)
	if err != nil {
		return nil, err
	}
	t, ok := findTable(tables, cs.Name)
	if !ok {
		return nil, configInvalid("oracle: there's no table %s.%s the user can read", schema, cs.Name)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = conn.Close() }()
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
	ranges, err := keyRanges(ctx, conn, schema, t, s.cfg.ChunkRows)
	if err != nil {
		return nil, err
	}
	if len(ranges) > 0 && ci >= 0 {
		if st.Until, err = maxCursor(ctx, conn, schema, t, ci); err != nil {
			return nil, err
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
			return nil, fmt.Errorf("oracle: %s state: %w", stream, err)
		}
		if st.Since != nil {
			return st.Since, nil
		}
	}
	return nil, nil
}

// keyRanges divides a table into contiguous ranges of its key's first
// column of about rows rows each, none if it's smaller or can't be split.
// The first has no lower bound and the last no upper, so together they
// cover every key.
func keyRanges(ctx context.Context, conn *sql.Conn, schema string, t table, rows int) ([]keyRange, error) {
	if !splittable(t) {
		return nil, nil
	}
	est, err := estimate(ctx, conn, schema, t)
	if err != nil {
		return nil, err
	}
	n := min((est+int64(rows)-1)/int64(rows), maxKeyRanges)
	if n < 2 {
		return nil, nil
	}
	low, high, err := extent(ctx, conn, schema, t)
	if err != nil || low == nil {
		return nil, err
	}
	return cutRanges(low, high, int(n)), nil
}

// cutRanges cuts [low, high] into n ranges of about as many values, fewer if
// it holds fewer.
func cutRanges(low, high *big.Int, n int) []keyRange {
	span := new(big.Int).Sub(high, low)
	if span.Cmp(big.NewInt(int64(n))) < 0 {
		n = int(span.Int64())
	}
	if n < 2 {
		return nil
	}
	out := make([]keyRange, n)
	for i := 1; i < n; i++ {
		b := new(big.Int).Mul(span, big.NewInt(int64(i)))
		b.Quo(b, big.NewInt(int64(n))).Add(b, low)
		text := b.String()
		out[i-1].Hi, out[i].Lo = &text, &text
	}
	return out
}

// estimate is a table's rows by its statistics, ALL_TABLES.NUM_ROWS, or a
// count where it has none.
func estimate(ctx context.Context, conn *sql.Conn, schema string, t table) (int64, error) {
	var n sql.NullInt64
	err := conn.QueryRowContext(ctx, `SELECT num_rows FROM all_tables WHERE owner = :1 AND table_name = :2`, schema, t.name).Scan(&n)
	if err != nil {
		return 0, classify(err)
	}
	if n.Valid {
		return n.Int64, nil
	}
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteIdent(schema)+"."+quoteIdent(t.name)).Scan(&n)
	return n.Int64, classify(err)
}

// extent is the least and greatest values of a NUMBER key's first column,
// rounded down and up to integers; nil for an empty table.
func extent(ctx context.Context, conn *sql.Conn, schema string, t table) (low, high *big.Int, err error) {
	col := colRef(t.cols[t.key[0]])
	var lo, hi sql.NullString
	err = conn.QueryRowContext(ctx, "SELECT MIN("+col+"), MAX("+col+") FROM "+quoteIdent(schema)+"."+quoteIdent(t.name)+" t").Scan(&lo, &hi)
	if err != nil || !lo.Valid {
		return nil, nil, classify(err)
	}
	if low, _, err = parseNum(lo.String, false); err != nil {
		return nil, nil, err
	}
	high, _, err = parseNum(hi.String, true)
	return low, high, err
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
// reached st. If it was asked, it divides the remainder at its midpoint, so
// what any chunk read before it stays below: st keeps the lower part, and the
// split to add takes the upper. pos is st's position, if it says how far the
// key has been read; keep has the new split keep st's position, where rows of
// any key can follow it. ended: the chunk was its last.
func (s *Source) resplit(ctx context.Context, conn *sql.Conn, schema string, t table, r *readingSplit, st *sourceState, ended bool, pos *string, keep bool) ([]source.Split, error) {
	if ended || !splittable(t) {
		r.over.Store(true)
		return nil, nil
	}
	low, high, err := remainder(st.keyRange, pos, len(t.key) == 1)
	if err != nil {
		return nil, err
	}
	if !r.asked.Swap(false) {
		if low != nil && high != nil && low.Cmp(high) >= 0 {
			r.over.Store(true)
		}
		return nil, nil
	}
	if low == nil || high == nil {
		least, most, err := extent(ctx, conn, schema, t)
		if err != nil || least == nil {
			r.over.Store(least == nil)
			return nil, err
		}
		low, high = cmp.Or(low, least), cmp.Or(high, most)
	}
	if low.Cmp(high) >= 0 {
		r.over.Store(true)
		return nil, nil
	}
	m := new(big.Int).Sub(high, low)
	m.Rsh(m, 1).Add(m, low)
	mid := m.String()
	upper := *st
	upper.Lo = &mid
	if !keep {
		upper.After = nil
	}
	state, err := json.Marshal(upper)
	if err != nil {
		return nil, err
	}
	st.Hi = &mid
	id := sha256.Sum256([]byte(mid))
	return []source.Split{{ID: hex.EncodeToString(id[:6]), State: state}}, nil
}

// remainder is the least and most integer a NUMBER key's first column can
// hold in what's left of a range, nil where only the table can say: an
// upper part split off above the midpoint between them holds no row read.
// A position in a key of one column is that key read; in a longer one, its
// value has more keys to come.
func remainder(r keyRange, pos *string, single bool) (low, high *big.Int, err error) {
	switch {
	case pos != nil:
		var exact bool
		if low, exact, err = parseNum(*pos, true); err != nil {
			return nil, nil, err
		}
		if single && exact {
			low.Add(low, big.NewInt(1))
		}
	case r.Lo != nil:
		if low, _, err = parseNum(*r.Lo, false); err != nil {
			return nil, nil, err
		}
		low.Add(low, big.NewInt(1))
	}
	if r.Hi != nil {
		if high, _, err = parseNum(*r.Hi, false); err != nil {
			return nil, nil, err
		}
	}
	return low, high, nil
}

// parseNum is a NUMBER's text rounded to an integer, down, or up if up;
// exact says it was one.
func parseNum(s string, up bool) (n *big.Int, exact bool, err error) {
	r, ok := new(big.Rat).SetString(strings.TrimSpace(s))
	if !ok {
		return nil, false, fmt.Errorf("oracle: %q as a key's number", s)
	}
	q, m := new(big.Int).QuoRem(r.Num(), r.Denom(), new(big.Int))
	switch {
	case m.Sign() == 0:
		return q, true, nil
	case r.Sign() < 0 && !up:
		q.Sub(q, big.NewInt(1))
	case r.Sign() > 0 && up:
		q.Add(q, big.NewInt(1))
	}
	return q, false, nil
}

// connPool holds at most as many of a read's connections at a time as it
// has room for.
type connPool struct {
	db   *sql.DB
	held chan struct{}
}

func (p *connPool) get(ctx context.Context) (*sql.Conn, error) {
	select {
	case p.held <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c, err := p.db.Conn(ctx)
	if err != nil {
		<-p.held
		return nil, classify(fmt.Errorf("oracle: connect: %w", err))
	}
	return c, nil
}

func (p *connPool) put(c *sql.Conn) {
	_ = c.Close()
	<-p.held
}
