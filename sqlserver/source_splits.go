package sqlserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/openetl-io/openetl-connectors/keysplit"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	_ source.Splitter   = (*Source)(nil)
	_ source.Resplitter = (*Source)(nil)
)

// keyRange is a range of a key's first column, spelled as text: above Lo, up
// to and including Hi. Nil is no bound.
type keyRange struct {
	Lo *string `json:"lo,omitempty"`
	Hi *string `json:"hi,omitempty"`
}

// where are the conditions on ref, the key's first column c, of being in
// the range.
func (k keyRange) where(c sourceColumn, ref string, a *args) []string {
	var w []string
	if k.Lo != nil {
		w = append(w, ref+" > "+c.fromText(a.add(*k.Lo)))
	}
	if k.Hi != nil {
		w = append(w, ref+" <= "+c.fromText(a.add(*k.Hi)))
	}
	return w
}

// Splits divides a keyed table read in full or incrementally into key ranges,
// from the statistics of its primary key's index. Small tables, and those
// without statistics, are read whole. An incremental read's ranges share the
// highest cursor value it reads and its floor, where the last read's splits
// all ended. A cdc stream is read whole.
func (s *Source) Splits(ctx context.Context, cs source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	if cs.SyncMode == source.CDC {
		// A state from a read in splits starts the table again, as one from a
		// full refresh does.
		return keysplit.Whole(ended, nil), nil
	}
	tables, err := s.tables(ctx, s.db)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(tables, func(t table) bool { return t.name == cs.Name })
	if i < 0 {
		return nil, fmt.Errorf("sqlserver: unknown stream %q", cs.Name)
	}
	t := tables[i]
	var st sourceState
	ci := -1
	if cs.SyncMode == source.Incremental {
		if ci, err = cursorColumn(t, cs.Cursor); err != nil {
			return nil, err
		}
		if st.Since, err = keysplit.Floor("sqlserver", cs.Name, ended); err != nil {
			return nil, err
		}
	}
	ranges, err := s.keyRanges(ctx, t, s.cfg.ChunkRows)
	if err != nil {
		return nil, err
	}
	if len(ranges) > 0 && ci >= 0 {
		if st.Until, err = s.cursorTop(ctx, s.db, t, t.cols[ci]); err != nil {
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

// keyRanges divides a table into contiguous ranges of its key's first column
// of about rows rows each, none if it's smaller or has no statistics. The
// first has no lower bound and the last no upper, so together they cover
// every key.
func (s *Source) keyRanges(ctx context.Context, t table, rows int) ([]keyRange, error) {
	if len(t.key) == 0 {
		return nil, nil
	}
	n, bounds, err := s.keyStats(ctx, t, keyRange{})
	if err != nil {
		return nil, err
	}
	return cutRanges(bounds, n, rows), nil
}

// cutRanges cuts at bounds, a histogram's in order, evenly: each of its
// buckets holds about as many rows.
func cutRanges(bounds []string, tuples float64, rows int) []keyRange {
	return keysplit.Between(keysplit.CutBounds(bounds, tuples, rows), func(lo, hi *string) keyRange { return keyRange{lo, hi} })
}

// keyStats is the table's row count, and the steps of the histogram of its
// primary key's index strictly within r, as text in key order: none without
// statistics.
func (s *Source) keyStats(ctx context.Context, t table, r keyRange) (float64, []string, error) {
	first := t.cols[t.key[0]]
	step := "CONVERT(" + first.sqlType() + ", h.range_high_key)"
	var a args
	a.add(qualified(s.cfg.Schema, t.name))
	where := r.where(first, step, &a)
	if r.Hi != nil {
		where[len(where)-1] = step + " < " + first.fromText(a.add(*r.Hi))
	}
	q := `SELECT (SELECT coalesce(sum(p.rows), 0) FROM sys.partitions p WHERE p.object_id = OBJECT_ID(@p1) AND p.index_id IN (0, 1)), ` +
		first.textOf(step) + ` FROM sys.indexes i CROSS APPLY sys.dm_db_stats_histogram(i.object_id, i.index_id) h
		WHERE i.object_id = OBJECT_ID(@p1) AND i.is_primary_key = 1`
	for _, w := range where {
		q += " AND " + w
	}
	rows, err := s.db.QueryContext(ctx, q+" ORDER BY h.step_number", a...)
	if err != nil {
		return 0, nil, statsFailed(t, err)
	}
	defer func() { _ = rows.Close() }()
	var n float64
	var bounds []string
	for rows.Next() {
		var b string
		var count int64
		if err := rows.Scan(&count, &b); err != nil {
			return 0, nil, statsFailed(t, err)
		}
		n = float64(count)
		bounds = append(bounds, b)
	}
	return n, bounds, statsFailed(t, rows.Err())
}

// statsFailed passes on what stops the read: statistics the server won't give
// only leave the table unsplit.
func statsFailed(t table, err error) error {
	if _, ok := errors.AsType[mssql.Error](err); ok {
		slog.Warn("reading a table's statistics failed, so it isn't split", "table", t.name, "err", err)
		return nil
	}
	return classify(err)
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
func (s *Source) resplit(ctx context.Context, t table, r *keysplit.Reading, st *sourceState, ended bool, pos *string, keep bool) ([]source.Split, error) {
	if ended {
		r.Over.Store(true)
		return nil, nil
	}
	if !r.Asked.Swap(false) {
		if intKey[t.cols[t.key[0]].typ] {
			low, high, err := intRemainder(st.keyRange, pos, len(t.key) == 1)
			if err != nil {
				return nil, err
			}
			if low != nil && high != nil && low.Cmp(high) >= 0 {
				r.Over.Store(true)
			}
		}
		return nil, nil
	}
	mid, err := s.midpoint(ctx, t, st.keyRange, pos)
	if err != nil {
		return nil, err
	}
	if mid == nil {
		r.Over.Store(true)
		return nil, nil
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

// The types of a key's first column whose ranges are divided by arithmetic.
var intKey = map[string]bool{"tinyint": true, "smallint": true, "int": true, "bigint": true}

// midpoint is where to divide a range's remainder, nil if nowhere: the upper
// part starts above it, and each part has keys to hold.
func (s *Source) midpoint(ctx context.Context, t table, r keyRange, pos *string) (*string, error) {
	first := t.cols[t.key[0]]
	if !intKey[first.typ] {
		if pos != nil {
			r.Lo = pos
		}
		_, bounds, err := s.keyStats(ctx, t, r)
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
		var least, most *int64
		col := quote(first.name)
		q := "SELECT CONVERT(bigint, min(" + col + ")), CONVERT(bigint, max(" + col + ")) FROM " + qualified(s.cfg.Schema, t.name)
		if err := s.db.QueryRowContext(ctx, q).Scan(&least, &most); err != nil {
			return nil, classify(fmt.Errorf("sqlserver: read %s: %w", t.name, err))
		}
		if least == nil {
			return nil, nil
		}
		if low == nil {
			low = big.NewInt(*least)
		}
		if high == nil {
			high = big.NewInt(*most)
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

func intRemainder(r keyRange, pos *string, single bool) (low, high *big.Int, err error) {
	return keysplit.IntRemainder("sqlserver", r.Lo, r.Hi, pos, single)
}

func parseInt(s string) (*big.Int, error) { return keysplit.ParseInt("sqlserver", s) }
