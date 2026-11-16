package sqlserver

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
	"sync/atomic"

	mssql "github.com/microsoft/go-mssqldb"

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
		if whole, err := source.Whole(ended); err == nil {
			return whole, nil
		}
		return []source.Split{{ID: source.WholeStream}}, nil
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
		if st.Since, err = floorOf(cs.Name, ended); err != nil {
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
			return nil, fmt.Errorf("sqlserver: %s state: %w", stream, err)
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
func (s *Source) resplit(ctx context.Context, t table, r *readingSplit, st *sourceState, ended bool, pos *string, keep bool) ([]source.Split, error) {
	if ended {
		r.over.Store(true)
		return nil, nil
	}
	if !r.asked.Swap(false) {
		if intKey[t.cols[t.key[0]].typ] {
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
	mid, err := s.midpoint(ctx, t, st.keyRange, pos)
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
		return nil, fmt.Errorf("sqlserver: %q as a key's integer", s)
	}
	return v, nil
}
