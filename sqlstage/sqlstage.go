// Package sqlstage is what the SQL destinations share of an epoch's
// staging: the tables a writer stages each stream's rows and patches in, the
// descriptor its PreCommit returns, and how its Commit merges the lanes'
// descriptors. Applying them, in each database's SQL, is each destination's.
package sqlstage

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// Staged is the descriptor PreCommit returns.
type Staged struct {
	Generation uint64   `json:"generation"`
	Epoch      uint64   `json:"epoch"`
	Streams    []Stream `json:"streams"`
}

const (
	ModeOverwrite = "overwrite"
	ModeMerge     = "merge"
)

// Stream is a stream's part of a Staged.
type Stream struct {
	Name  string `json:"name"`
	Table string `json:"table"`
	// Empty when an overwrite read ended in the epoch without rows in it.
	Staging string `json:"staging"`
	// The lane's later staging tables of the epoch, each taking batches
	// wider than the one before.
	Wider []string `json:"wider,omitempty"`
	// One row per _openetl_id in the epoch, and only IDs the target doesn't
	// have yet.
	Dedupe bool `json:"dedupe,omitempty"`
	// One row per _openetl_id in the epoch.
	DedupeEpoch bool `json:"dedupe_epoch,omitempty"`
	// Staged rows, to count those the apply skips; 0 in descriptors from
	// before it was counted.
	Rows uint64 `json:"rows,omitempty"`
	// Empty for append.
	Mode string `json:"mode,omitempty"`
	// Merge: the key's columns, and whether a delete removes the row.
	Key         []string `json:"key,omitempty"`
	HardDeletes bool     `json:"hard_deletes,omitempty"`
	// Overwrite: the read's table, which takes the epoch's rows and, once
	// the read has ended in it, replaces the target's.
	Read  string `json:"read,omitempty"`
	Ended bool   `json:"ended,omitempty"`
	// Merge: columns whose held cells keep the target's value.
	Last []string `json:"last,omitempty"`
	// The epoch's patches' staging, and the columns in it that fill
	// another held column.
	Patch string            `json:"patch,omitempty"`
	Fills map[string]string `json:"fills,omitempty"`
	// As Wider, for Patch.
	WiderPatches []string `json:"wider_patches,omitempty"`
	// Staging and Patch, of every lane that has them, once MergeLanes has
	// merged the lanes' descriptors.
	Stagings, Patches []string `json:"-"`
}

func (s Stream) Deduped() bool { return s.Dedupe || s.DedupeEpoch }

// Into is where the epoch's rows go: the read's table for overwrite.
func (s Stream) Into() string {
	if s.Mode == ModeOverwrite {
		return s.Read
	}
	return s.Table
}

// Table is a stream's staging of an epoch, of its rows or its patches. A
// destination's staging table embeds it, with the columns as it types them.
type Table struct {
	Name   string
	Target string
	Rows   uint64
	// Merge: the key's columns.
	Key []string
	// Columns whose held cells keep the target's value.
	Last []string
	// Patches: columns that fill another held column, by that column.
	Fills map[string]string
	// Tables of the epoch before Name, each narrower than the next: a batch
	// wider than a table starts the next one, as no table's column is ever
	// changed to a wider type.
	Earlier []string
}

// All are the staging's tables, in order.
func (t *Table) All() []string { return append(slices.Clone(t.Earlier), t.Name) }

func (t *Table) Base() *Table { return t }

// Epochs is a writer's staging of the epochs it hasn't pre-committed, by
// epoch and stream; T is its staging table. Only Write, Patch, StartReads,
// EndRead and PreCommit use it, but for Table, which Commit and Recover use
// too.
type Epochs[T interface{ Base() *Table }] struct {
	Staging map[uint64]map[string]T
	Patches map[uint64]map[string]T
	Starts  map[string]time.Time
	// Streams whose read ends in each epoch.
	Ended map[uint64][]string
	// Each stream's target, from its batches and schemas.
	mu     sync.Mutex
	tables map[string]string
}

func NewEpochs[T interface{ Base() *Table }]() Epochs[T] {
	return Epochs[T]{Staging: map[uint64]map[string]T{}, Patches: map[uint64]map[string]T{}, Ended: map[uint64][]string{}, tables: map[string]string{}}
}

func (p *Epochs[T]) StartReads(_ context.Context, starts map[string]time.Time) error {
	p.Starts = starts
	return nil
}

func (p *Epochs[T]) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	p.Ended[e.N] = append(p.Ended[e.N], stream)
	return nil
}

func (p *Epochs[T]) SetTable(stream, table string) {
	p.mu.Lock()
	p.tables[stream] = table
	p.mu.Unlock()
}

func (p *Epochs[T]) Table(stream string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tables[stream]
}

// Describe is what PreCommit returns of epoch e, whose staging it lets go:
// its streams' tables, and readName's table of each overwrite read.
func (p *Epochs[T]) Describe(cfg destination.OpenConfig, e destination.Epoch, readName func(pipeline, stream string, start time.Time) string) (destination.Staged, error) {
	desc := Staged{Generation: e.Generation, Epoch: e.N}
	byStream, patches := p.Staging[e.N], p.Patches[e.N]
	for _, s := range cfg.Streams {
		var st, pt *Table
		if t, ok := byStream[s.Name]; ok {
			st = t.Base()
		}
		if t, ok := patches[s.Name]; ok {
			pt = t.Base()
		}
		ended := s.Mode == destination.Overwrite && slices.Contains(p.Ended[e.N], s.Name)
		if st == nil && pt == nil && !ended {
			continue
		}
		ss := Stream{
			Name: s.Name, Table: p.Table(s.Name),
			Dedupe: s.Dedupe == destination.DedupeTarget, DedupeEpoch: s.Dedupe == destination.DedupeEpoch,
		}
		key := []string(nil)
		if pt != nil {
			tables := pt.All()
			ss.Table, ss.Patch, ss.WiderPatches, ss.Fills, key = pt.Target, tables[0], tables[1:], pt.Fills, pt.Key
		}
		if st != nil {
			tables := st.All()
			ss.Table, ss.Staging, ss.Wider, ss.Rows, ss.Last, key = st.Target, tables[0], tables[1:], st.Rows, st.Last, st.Key
		}
		switch s.Mode {
		case destination.Overwrite:
			// A read that ends without rows in the epoch still replaces the
			// target's.
			ss.Mode, ss.Read, ss.Ended = ModeOverwrite, readName(cfg.Pipeline, s.Name, p.Starts[s.Name]), ended
		case destination.Merge:
			ss.Mode, ss.Key, ss.HardDeletes = ModeMerge, key, s.Deletes == destination.HardDelete
		}
		desc.Streams = append(desc.Streams, ss)
	}
	delete(p.Staging, e.N)
	delete(p.Patches, e.N)
	delete(p.Ended, e.N)
	return json.Marshal(desc)
}

// MergeLanes merges the epoch's descriptors, one per lane of the attempt
// that staged it, by stream; db names the destination in its errors. A lane
// that didn't know a read ended, or had no rows of a stream, says less: its
// table can come from the schemas.
func (p *Epochs[T]) MergeLanes(db string, committables []destination.Staged) (Staged, error) {
	var out Staged
	at := map[string]int{}
	if len(committables) == 0 {
		return out, errors.New(db + ": no staged descriptor")
	}
	for i, c := range committables {
		var desc Staged
		if err := json.Unmarshal(c, &desc); err != nil {
			return out, fmt.Errorf("%s: staged descriptor: %w", db, err)
		}
		if i == 0 {
			out.Generation, out.Epoch = desc.Generation, desc.Epoch
		} else if desc.Generation != out.Generation || desc.Epoch != out.Epoch {
			return out, fmt.Errorf("%s: staged descriptors of epochs %d-g%d and %d-g%d", db, out.Epoch, out.Generation, desc.Epoch, desc.Generation)
		}
		for _, s := range desc.Streams {
			j, ok := at[s.Name]
			if !ok {
				j = len(out.Streams)
				at[s.Name] = j
				out.Streams = append(out.Streams, Stream{
					Name: s.Name, Dedupe: s.Dedupe, DedupeEpoch: s.DedupeEpoch, Mode: s.Mode, HardDeletes: s.HardDeletes, Read: s.Read,
				})
			}
			m := &out.Streams[j]
			if m.Read != s.Read {
				return out, fmt.Errorf("%s: two reads of %s in epoch %d-g%d", db, s.Name, out.Epoch, out.Generation)
			}
			m.Table, m.Rows, m.Ended = cmp.Or(m.Table, s.Table), m.Rows+s.Rows, m.Ended || s.Ended
			if len(m.Key) == 0 {
				m.Key = s.Key
			}
			for _, c := range s.Last {
				if !slices.Contains(m.Last, c) {
					m.Last = append(m.Last, c)
				}
			}
			if len(s.Fills) > 0 && m.Fills == nil {
				m.Fills = map[string]string{}
			}
			maps.Copy(m.Fills, s.Fills)
			if s.Staging != "" {
				m.Stagings = append(append(m.Stagings, s.Staging), s.Wider...)
			}
			if s.Patch != "" {
				m.Patches = append(append(m.Patches, s.Patch), s.WiderPatches...)
			}
		}
	}
	for i := range out.Streams {
		if s := &out.Streams[i]; s.Table == "" {
			s.Table = p.Table(s.Name)
		}
	}
	return out, nil
}
