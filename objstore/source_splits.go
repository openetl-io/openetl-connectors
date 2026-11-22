package objstore

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// hashRange is a split's files: those whose key's 32-bit FNV-1a hash is in
// [Lo, Hi).
type hashRange struct {
	Lo uint64 `json:"lo"`
	Hi uint64 `json:"hi"`
}

var wholeRange = hashRange{0, 1 << 32}

// splitCount is how many splits a stream is read in, before Resplit
// divides them.
const splitCount = 8

func keyHash(key string) uint64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return uint64(h.Sum32())
}

func (r hashRange) has(key string) bool {
	h := keyHash(key)
	return r.Lo <= h && h < r.Hi
}

func (r hashRange) id() string { return fmt.Sprintf("files-%08x-%09x", r.Lo, r.Hi) }

// rangeOf is the files of the split: its state's range, or its ID's.
func rangeOf(id string, st *readState) (hashRange, error) {
	if st != nil && st.Range != nil {
		return *st.Range, nil
	}
	if id == source.WholeStream {
		return wholeRange, nil
	}
	var r hashRange
	if n, err := fmt.Sscanf(id, "files-%x-%x", &r.Lo, &r.Hi); err != nil || n != 2 || r.Lo > r.Hi || r.Hi > wholeRange.Hi {
		return r, fmt.Errorf("objstore: split %q isn't one of this source's", id)
	}
	return r, nil
}

func evenRanges() []hashRange {
	out := make([]hashRange, splitCount)
	for i := range out {
		out[i] = hashRange{wholeRange.Hi * uint64(i) / splitCount, wholeRange.Hi * uint64(i+1) / splitCount}
	}
	return out
}

// Splits reads a stream in splits of its files by their keys' hashes, each
// with a state of its own. A stream read whole before starts each split from
// its state, and an incremental stream read in splits before goes on with
// them, as Resplit left them.
func (s *Source) Splits(_ context.Context, cs source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	incremental := cs.SyncMode == source.Incremental
	var from []byte
	if len(ended) == 1 && ended[0].ID == source.WholeStream {
		from, ended = ended[0].State, nil
	}
	if !incremental {
		from, ended = nil, nil
	}
	if len(ended) == 0 {
		var out []source.Split
		for _, r := range evenRanges() {
			out = append(out, source.Split{ID: r.id(), State: from})
		}
		return out, nil
	}
	out := make([]source.Split, len(ended))
	ranges := make([]hashRange, len(ended))
	for i, sp := range ended {
		var st readState
		if len(sp.State) > 0 {
			if err := json.Unmarshal(sp.State, &st); err != nil {
				return nil, fmt.Errorf("objstore: split %q's state: %w", sp.ID, err)
			}
		}
		r, err := rangeOf(sp.ID, &st)
		if err != nil {
			return nil, err
		}
		ranges[i] = r
		out[i] = source.Split{ID: r.id(), State: sp.State}
	}
	sorted := slices.Clone(ranges)
	slices.SortFunc(sorted, func(a, b hashRange) int { return cmp.Compare(a.Lo, b.Lo) })
	at := uint64(0)
	for _, r := range sorted {
		if r.Lo != at {
			return nil, fmt.Errorf("objstore: stream %q's last read's splits don't cover its files once each", cs.Name)
		}
		at = r.Hi
	}
	if at != wholeRange.Hi {
		return nil, fmt.Errorf("objstore: stream %q's last read's splits don't cover its files once each", cs.Name)
	}
	return out, nil
}

// running is a split being read, which Resplit can halve.
type running struct {
	mu sync.Mutex
	// A read with an until isn't divided.
	until   bool
	pending bool
	// The files the split has yet to start.
	left []string
}

func runningKey(stream, split string) string { return stream + "\x00" + split }

// Resplit halves a split's files yet to be read: at its next resume point,
// the split keeps those on one side of a hash and gives the others to a
// new split, whose state is a copy of its own.
func (s *Source) Resplit(_ context.Context, stream, split string) (bool, error) {
	v, ok := s.running.Load(runningKey(stream, split))
	if !ok {
		return false, nil
	}
	r := v.(*running)
	r.mu.Lock()
	defer r.mu.Unlock()
	// The split may finish a file before its next resume point: two left
	// keep one for the new split.
	if r.until || r.pending || len(r.left) < 2 {
		return false, nil
	}
	r.pending = true
	return true, nil
}

// halve divides r's files yet to be read: keep has the file being read, at,
// if any, and give about half of the others.
func halve(r hashRange, left []string, at string) (keep, give hashRange, ok bool) {
	hashes := make([]uint64, 0, len(left))
	for _, k := range left {
		hashes = append(hashes, keyHash(k))
	}
	slices.Sort(hashes)
	if len(hashes) == 0 {
		return r, r, false
	}
	if at == "" {
		m := hashes[len(hashes)/2]
		if m > r.Lo {
			return hashRange{r.Lo, m}, hashRange{m, r.Hi}, true
		}
		// The range starts at the file's hash, as one a halving gave does:
		// the files at that hash go, so the new split isn't the whole
		// range, with its ID.
		if m+1 < r.Hi {
			return hashRange{m + 1, r.Hi}, hashRange{r.Lo, m + 1}, true
		}
		return r, r, false
	}
	a := keyHash(at)
	var lower, upper []uint64
	for _, h := range hashes {
		switch {
		case h < a:
			lower = append(lower, h)
		case h > a:
			upper = append(upper, h)
		}
	}
	half := max(1, (len(lower)+len(upper))/2)
	switch {
	case len(upper) > 0:
		m := upper[len(upper)-min(half, len(upper))]
		return hashRange{r.Lo, m}, hashRange{m, r.Hi}, true
	case len(lower) > 0:
		m := a
		if half < len(lower) {
			m = lower[half]
		}
		return hashRange{m, r.Hi}, hashRange{r.Lo, m}, true
	}
	return r, r, false
}
