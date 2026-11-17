// Package keysplit is what the sources share of reading a table in splits
// of its key: the splits being read, which Resplit divides, an incremental
// read's floor, and what's left of an integer key's range.
package keysplit

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// Reading is a split being read.
type Reading struct {
	// Resplit asked, and the split's next resume point answers.
	Asked atomic.Bool
	// It can't be split: Resplit refuses.
	Over atomic.Bool
}

type key struct{ stream, split string }

// Reads are the splits a source is reading, which Resplit splits.
type Reads struct {
	mu      sync.Mutex
	reading map[key]*Reading
}

// Start registers a split being read; resplits: Resplit may divide it.
func (s *Reads) Start(stream, id string, resplits bool) *Reading {
	r := &Reading{}
	r.Over.Store(!resplits)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reading == nil {
		s.reading = map[key]*Reading{}
	}
	s.reading[key{stream, id}] = r
	return r
}

func (s *Reads) End(stream, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.reading, key{stream, id})
}

// Resplit asks a split being read to split its remainder at its next resume
// point, which works out where. It refuses a split it knows it can't split,
// or that's already been asked.
func (s *Reads) Resplit(_ context.Context, stream, split string) (bool, error) {
	s.mu.Lock()
	r := s.reading[key{stream, split}]
	s.mu.Unlock()
	if r == nil || r.Over.Load() {
		return false, nil
	}
	return r.Asked.CompareAndSwap(false, true), nil
}

// Floor is the floor an incremental read starts from, where its last read's
// splits ended: one floor, which each of them carries as its state's since.
// db names the source in its errors.
func Floor(db, stream string, ended []source.Split) (*string, error) {
	for _, sp := range ended {
		if sp.State == nil {
			continue
		}
		var st struct {
			Since *string `json:"since"`
		}
		if err := json.Unmarshal(sp.State, &st); err != nil {
			return nil, fmt.Errorf("%s: %s state: %w", db, stream, err)
		}
		if st.Since != nil {
			return st.Since, nil
		}
	}
	return nil, nil
}

// IntRemainder is the least and most an integer key's first column can hold
// in what's left of the range from lo to hi, nil where only the table can
// say. A position in a key of one column is that key read; in a longer one,
// its value has more keys to come.
func IntRemainder(db string, lo, hi, pos *string, single bool) (low, high *big.Int, err error) {
	switch {
	case pos != nil:
		if low, err = ParseInt(db, *pos); err != nil {
			return nil, nil, err
		}
		if single {
			low.Add(low, big.NewInt(1))
		}
	case lo != nil:
		if low, err = ParseInt(db, *lo); err != nil {
			return nil, nil, err
		}
		low.Add(low, big.NewInt(1))
	}
	if hi != nil {
		if high, err = ParseInt(db, *hi); err != nil {
			return nil, nil, err
		}
	}
	return low, high, nil
}

func ParseInt(db, s string) (*big.Int, error) {
	v, ok := new(big.Int).SetString(strings.TrimSpace(s), 10)
	if !ok {
		return nil, fmt.Errorf("%s: %q as a key's integer", db, s)
	}
	return v, nil
}
