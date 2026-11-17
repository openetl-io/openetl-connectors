package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/openetl-io/openetl-connectors/keysplit"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	_ source.Splitter   = (*Source)(nil)
	_ source.Resplitter = (*Source)(nil)
)

// keyRange is a range of an integer key's first column, spelled as decimal
// text: above Lo, up to and including Hi. Nil is no bound.
type keyRange struct {
	Lo *string `json:"lo,omitempty"`
	Hi *string `json:"hi,omitempty"`
}

// where adds the conditions on c, the key's first column, of being in the
// range.
func (k keyRange) where(c sourceColumn, w *where) error {
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

var intKinds = map[kind]bool{kInt16: true, kInt32: true, kInt64: true, kUint64: true}

// splittable says a table is read in key ranges: its key orders as it
// compares, and its first column is an integer.
func splittable(t table) bool { return chunked(t) && intKinds[t.cols[t.key[0]].kind] }

// Splits divides a table read in full or incrementally whose key starts with
// an integer column into ranges of that column, evenly between its least and
// greatest values, as many as the server's estimate of the table's rows
// fills at chunkRows each. Small tables are read whole. An incremental
// read's ranges share the highest cursor value it reads and its floor, where
// the last read's splits all ended.
func (s *Source) Splits(ctx context.Context, cs source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	if cs.SyncMode == source.CDC {
		// A state from a read in splits starts the table again, as one from a
		// full refresh does.
		return keysplit.Whole(ended, nil), nil
	}
	db, srv, err := open(ctx, s.conn, 1)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	tables, err := s.tables(ctx, db, srv)
	if err != nil {
		return nil, err
	}
	t, ok := findTable(tables, cs.Name)
	if !ok {
		return nil, fmt.Errorf("mysql: unknown stream %q", cs.Name)
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
		if st.Since, err = keysplit.Floor("mysql", cs.Name, ended); err != nil {
			return nil, err
		}
	}
	ranges, err := s.keyRanges(ctx, conn, t, s.cfg.ChunkRows)
	if err != nil {
		return nil, err
	}
	if len(ranges) > 0 && ci >= 0 {
		if st.Until, err = s.maxCursor(ctx, conn, t, ci); err != nil {
			return nil, err
		}
		if st.Until == nil {
			ranges = nil
		}
	}
	return keysplit.Of(ended, len(ranges), func(i int) any {
		if i >= 0 {
			st.keyRange = ranges[i]
		}
		return st
	})
}

// keyRanges divides a table into contiguous ranges of its key's first
// column of about rows rows each by the server's estimate, none if it's
// smaller or can't be split. The first has no lower bound and the last no
// upper, so together they cover every key.
func (s *Source) keyRanges(ctx context.Context, conn *sql.Conn, t table, rows int) ([]keyRange, error) {
	if !splittable(t) {
		return nil, nil
	}
	est, err := s.estimate(ctx, conn, t)
	if err != nil {
		return nil, err
	}
	n := min((est+int64(rows)-1)/int64(rows), keysplit.MaxRanges)
	if n < 2 {
		return nil, nil
	}
	low, high, err := s.extent(ctx, conn, t)
	if err != nil || low == nil {
		return nil, err
	}
	return cutRanges(low, high, int(n)), nil
}

// cutRanges cuts [low, high] into n ranges of about as many values, fewer if
// it holds fewer.
func cutRanges(low, high *big.Int, n int) []keyRange {
	return keysplit.Between(keysplit.CutInts(low, high, n), func(lo, hi *string) keyRange { return keyRange{lo, hi} })
}

// estimate is the optimizer's estimate of a table's rows.
func (s *Source) estimate(ctx context.Context, conn *sql.Conn, t table) (int64, error) {
	rows, err := conn.QueryContext(ctx, "EXPLAIN SELECT * FROM "+quoteIdent(s.cfg.Database)+"."+quoteIdent(t.name))
	if err != nil {
		return 0, classify(err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return 0, classify(err)
	}
	var est int64
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return 0, classify(err)
		}
		for i, c := range cols {
			if strings.EqualFold(c, "rows") && vals[i].Valid {
				est, _ = strconv.ParseInt(vals[i].String, 10, 64)
			}
		}
	}
	return est, classify(rows.Err())
}

// extent is the least and greatest values of an integer key's first column,
// nil for an empty table.
func (s *Source) extent(ctx context.Context, conn *sql.Conn, t table) (low, high *big.Int, err error) {
	col := quoteIdent(t.cols[t.key[0]].name)
	var lo, hi sql.NullString
	err = conn.QueryRowContext(ctx, "SELECT MIN("+col+"), MAX("+col+") FROM "+quoteIdent(s.cfg.Database)+"."+quoteIdent(t.name)).Scan(&lo, &hi)
	if err != nil || !lo.Valid {
		return nil, nil, classify(err)
	}
	if low, err = parseInt(lo.String); err != nil {
		return nil, nil, err
	}
	high, err = parseInt(hi.String)
	return low, high, err
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
func (s *Source) resplit(ctx context.Context, conn *sql.Conn, t table, r *keysplit.Reading, st *sourceState, ended bool, pos *string, keep bool) ([]source.Split, error) {
	if ended || !splittable(t) {
		r.Over.Store(true)
		return nil, nil
	}
	low, high, err := intRemainder(st.keyRange, pos, len(t.key) == 1)
	if err != nil {
		return nil, err
	}
	mid, err := r.IntMid(low, high, func() (*big.Int, *big.Int, error) { return s.extent(ctx, conn, t) })
	if mid == nil || err != nil {
		return nil, err
	}
	upper := *st
	upper.Lo = mid
	if !keep {
		upper.After = nil
	}
	splits, err := keysplit.Upper(*mid, upper)
	if err == nil {
		st.Hi = mid
	}
	return splits, err
}

type connPool = keysplit.Conns

func intRemainder(r keyRange, pos *string, single bool) (low, high *big.Int, err error) {
	return keysplit.IntRemainder("mysql", r.Lo, r.Hi, pos, single)
}

func parseInt(s string) (*big.Int, error) { return keysplit.ParseInt("mysql", s) }
