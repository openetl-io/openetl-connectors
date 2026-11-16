package clickhouse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"sync/atomic"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	_ source.Splitter   = (*Source)(nil)
	_ source.Resplitter = (*Source)(nil)
)

// The most key ranges a table's read in.
const maxKeyRanges = 32

// keyRange is a range of a key's first column, as its state values are:
// above Lo, up to and including Hi. Nil is no bound.
type keyRange struct {
	Lo *string `json:"lo,omitempty"`
	Hi *string `json:"hi,omitempty"`
}

// where are the conditions on c, the key's first column, of being in the
// range.
func (k keyRange) where(c sourceColumn) []string {
	var w []string
	if k.Lo != nil {
		w = append(w, ident(c.name)+" > "+castBack(c, *k.Lo))
	}
	if k.Hi != nil {
		w = append(w, ident(c.name)+" <= "+castBack(c, *k.Hi))
	}
	return w
}

// Splits divides a table read in sorting-key order into ranges of its
// key's first column, cut at the values its primary index has for its
// granules. Small tables, and those without an index, are read whole. An
// incremental read's ranges share the highest cursor value it reads and its
// floor, where the last read's splits all ended.
func (s *Source) Splits(ctx context.Context, cs source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	conn, err := s.cfg.connect(ctx, 1, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	tables, err := s.tables(ctx, conn)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(tables, func(t table) bool { return t.name == cs.Name })
	if i < 0 {
		return nil, fmt.Errorf("clickhouse: unknown stream %q", cs.Name)
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
	ranges, err := s.keyRanges(ctx, conn, t)
	if err != nil {
		return nil, err
	}
	if len(ranges) > 0 && ci >= 0 {
		c := t.cols[ci]
		q := "SELECT if(count() > 0, " + stateText(c, "max("+ident(c.name)+")") + ", NULL) FROM " + qualified(s.cfg.Database, t.name) + " WHERE " + ident(c.name) + " IS NOT NULL"
		if err := conn.QueryRow(ctx, q).Scan(&st.Until); err != nil {
			return nil, classify(fmt.Errorf("clickhouse: read %s: %w", t.name, err))
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
			return nil, fmt.Errorf("clickhouse: %s state: %w", stream, err)
		}
		if st.Since != nil {
			return st.Since, nil
		}
	}
	return nil, nil
}

// keyRanges divides a table into contiguous ranges of its key's first
// column of about chunkRows rows each, none if it's smaller or has no
// index. The first has no lower bound and the last no upper, so together
// they cover every key.
func (s *Source) keyRanges(ctx context.Context, conn driver.Conn, t table) ([]keyRange, error) {
	if len(t.key) == 0 {
		return nil, nil
	}
	var rows *uint64
	if err := conn.QueryRow(ctx, "SELECT total_rows FROM system.tables WHERE database = ? AND name = ?", s.cfg.Database, t.name).Scan(&rows); err != nil {
		return nil, classify(err)
	}
	if rows == nil {
		return nil, nil
	}
	bounds, err := s.indexBounds(ctx, conn, t, keyRange{})
	if err != nil {
		return nil, err
	}
	return cutRanges(bounds, float64(*rows), s.cfg.ChunkRows), nil
}

// cutRanges cuts at bounds, in order, evenly: each range has about as many
// of them.
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

// indexBounds are the distinct values the primary index has for the key's
// first column strictly within r, in order, as state values. An index the
// server won't give leaves the table unsplit.
func (s *Source) indexBounds(ctx context.Context, conn driver.Conn, t table, r keyRange) ([]string, error) {
	c := t.cols[t.key[0]]
	q := "SELECT " + stateText(c, "k") + " FROM (SELECT DISTINCT " + ident(c.name) + " AS k FROM mergeTreeIndex(" + literal(s.cfg.Database) + ", " + literal(t.name) + ")"
	var where []string
	if r.Lo != nil {
		where = append(where, "k > "+castBack(c, *r.Lo))
	}
	if r.Hi != nil {
		where = append(where, "k < "+castBack(c, *r.Hi))
	}
	q += ") "
	if len(where) > 0 {
		q += "WHERE " + where[0]
		for _, w := range where[1:] {
			q += " AND " + w
		}
	}
	rows, err := conn.Query(ctx, q+" ORDER BY k")
	if err != nil {
		return nil, indexFailed(t, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, indexFailed(t, rows.Err())
}

func indexFailed(t table, err error) error {
	if _, ok := exception(err); ok {
		slog.Warn("reading a table's primary index failed, so it isn't split", "table", t.name, "err", err)
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

// position is where a key-ordered read has reached in the key's first
// column, nil before its first row.
func (st sourceState) position() *string {
	if len(st.After) == 0 {
		return nil
	}
	return &st.After[0]
}

// resplit runs at each resume point of a keyed split, after the chunk that
// reached st. If it was asked, it divides the remainder at the middle of
// the index's values in it, so what any chunk read before it stays below:
// st keeps the lower part, and the split to add takes the upper. pos is
// st's position, if it says how far the key has been read; keep has the
// new split keep st's position, where rows of any key can follow it.
// ended: the chunk was its last.
func (s *Source) resplit(ctx context.Context, conn driver.Conn, t table, r *readingSplit, st *sourceState, ended bool, pos *string, keep bool) ([]source.Split, error) {
	if ended {
		r.over.Store(true)
		return nil, nil
	}
	if !r.asked.Swap(false) {
		return nil, nil
	}
	rest := st.keyRange
	if pos != nil {
		rest.Lo = pos
	}
	bounds, err := s.indexBounds(ctx, conn, t, rest)
	if err != nil {
		return nil, err
	}
	if len(bounds) == 0 {
		r.over.Store(true)
		return nil, nil
	}
	mid := bounds[len(bounds)/2]
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
