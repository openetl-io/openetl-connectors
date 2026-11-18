package keysplit

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"math/big"
	"slices"
	"strconv"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// MaxRanges bounds the ranges a table is split into.
const MaxRanges = 32

// Whole is a stream read whole: the last read's split, if it read it whole,
// or one with state.
func Whole(ended []source.Split, state []byte) []source.Split {
	if whole, err := source.Whole(ended); err == nil {
		return whole
	}
	return []source.Split{{ID: source.WholeStream, State: state}}
}

// Of are a read's splits: one for each of n ranges, whose state is
// state(i); or, without ranges, the stream whole, with state(-1).
func Of(ended []source.Split, n int, state func(i int) any) ([]source.Split, error) {
	if n == 0 {
		if whole, err := source.Whole(ended); err == nil {
			return whole, nil
		}
		b, err := json.Marshal(state(-1))
		if err != nil {
			return nil, err
		}
		return []source.Split{{ID: source.WholeStream, State: b}}, nil
	}
	splits := make([]source.Split, n)
	for i := range splits {
		b, err := json.Marshal(state(i))
		if err != nil {
			return nil, err
		}
		splits[i] = source.Split{ID: strconv.Itoa(i), State: b}
	}
	return splits, nil
}

// Upper is the split a resplit at mid adds, whose state is upper.
func Upper(mid string, upper any) ([]source.Split, error) {
	b, err := json.Marshal(upper)
	if err != nil {
		return nil, err
	}
	id := sha256.Sum256([]byte(mid))
	return []source.Split{{ID: hex.EncodeToString(id[:6]), State: b}}, nil
}

// Between are the ranges that cuts, in order, divide a key into, by mk;
// none without cuts.
func Between[R any](cuts []string, mk func(lo, hi *string) R) []R {
	if len(cuts) == 0 {
		return nil
	}
	out := make([]R, 0, len(cuts)+1)
	var lo *string
	for i := range cuts {
		out = append(out, mk(lo, &cuts[i]))
		lo = &cuts[i]
	}
	return append(out, mk(lo, nil))
}

// CutInts cuts [low, high] into n ranges of about as many values, fewer if
// it holds fewer: the values between them, none for fewer than two.
func CutInts(low, high *big.Int, n int) []string {
	span := new(big.Int).Sub(high, low)
	if span.Cmp(big.NewInt(int64(n))) < 0 {
		n = int(span.Int64())
	}
	if n < 2 {
		return nil
	}
	cuts := make([]string, n-1)
	for i := 1; i < n; i++ {
		b := new(big.Int).Mul(span, big.NewInt(int64(i)))
		cuts[i-1] = b.Quo(b, big.NewInt(int64(n))).Add(b, low).String()
	}
	return cuts
}

// CutBounds cuts at bounds, in order, evenly, into ranges of about rows of
// tuples each: the bounds between them, none for fewer than two.
func CutBounds(bounds []string, tuples float64, rows int) []string {
	bounds = slices.Compact(slices.Clone(bounds))
	n := min(int(math.Min(math.Ceil(tuples/float64(rows)), MaxRanges)), len(bounds)-1)
	if n < 2 {
		return nil
	}
	cuts := make([]string, n-1)
	for i := 1; i < n; i++ {
		cuts[i-1] = bounds[i*(len(bounds)-1)/n]
	}
	return cuts
}

// IntMid is where to resplit a split of an integer key whose remainder is
// low to high, which extent, the table's, gives where they're nil: their
// midpoint. Without a request to resplit, or with nothing left to divide, it
// is nil, and the split is over if nothing's left.
func (r *Reading) IntMid(low, high *big.Int, extent func() (least, most *big.Int, err error)) (*string, error) {
	if !r.Asked.Swap(false) {
		if low != nil && high != nil && low.Cmp(high) >= 0 {
			r.Over.Store(true)
		}
		return nil, nil
	}
	if low == nil || high == nil {
		least, most, err := extent()
		if err != nil {
			return nil, err
		}
		if least == nil {
			// The table is empty.
			r.Over.Store(true)
			return nil, nil
		}
		low, high = cmp.Or(low, least), cmp.Or(high, most)
	}
	if low.Cmp(high) >= 0 {
		r.Over.Store(true)
		return nil, nil
	}
	m := new(big.Int).Sub(high, low)
	mid := m.Rsh(m, 1).Add(m, low).String()
	return &mid, nil
}
