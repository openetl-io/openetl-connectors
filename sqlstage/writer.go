package sqlstage

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// Dialect is what the Write, Patch, PreCommit, Commit and Recover of Epochs
// need of a destination: how it names its tables, and its SQL. The
// destination's Open sets it.
type Dialect[T interface{ Base() *Table }] struct {
	// The destination's, for its errors.
	Name    string
	Open    destination.OpenConfig
	Streams map[string]destination.Stream
	// The staging of a stream's rows in an epoch. Its patches' is that of
	// the stream's name and "\x00patch".
	StagingName func(pipeline, stream string, e destination.Epoch, lane int) string
	ReadName    func(pipeline, stream string, start time.Time) string
	// The table a batch goes to.
	TargetTable func(schema *arrow.Schema, stream string) string
	SQL         SQL[T]
}

// SQL is how a destination stages, applies and recovers, in its SQL.
type SQL[T interface{ Base() *Table }] interface {
	// Stage stages a batch in tables, in the staging table name, which it
	// creates or widens; key is a merge's key columns.
	Stage(ctx context.Context, tables map[uint64]map[string]T, name string, e destination.Epoch, stream, target string, key []string, batch arrow.RecordBatch) (T, error)
	// Apply applies desc's streams with their markers; collapsed takes the
	// rows each stream's dedupe dropped.
	Apply(ctx context.Context, desc Staged, streams []Stream, collapsed map[string]uint64) error
	// Fence makes the schema and the fence and marker tables, and raises the
	// fence to gen: destination.ErrFenced if it's higher.
	Fence(ctx context.Context, gen uint64) error
	// Unapplied are those of streams without a marker of desc.
	Unapplied(ctx context.Context, desc Staged, streams []Stream) ([]Stream, error)
	Exists(ctx context.Context, table string) (bool, error)
	// Best effort: what's left is dropped later.
	DropStaging(ctx context.Context, streams []Stream)
	TrimMarkers(ctx context.Context, applied uint64)
	DropStale(ctx context.Context, gen uint64)
	DropRemovedReads(ctx context.Context)
}

const (
	idColumn       = "_openetl_id"
	sourceNamesKey = "openetl.source_names"
)

// RowsKey checks a batch of stream's rows for Write, and returns its merge
// key's columns; ok is false for a batch with nothing to stage.
func RowsKey(db string, streams map[string]destination.Stream, starts map[string]time.Time, stream string, batch arrow.RecordBatch) (key []string, ok bool, err error) {
	cfg, ok := streams[stream]
	if !ok {
		return nil, false, fmt.Errorf("%s: unknown stream %q", db, stream)
	}
	if batch.NumRows() == 0 {
		return nil, false, nil
	}
	if cfg.Dedupe != destination.DedupeNone && len(batch.Schema().FieldIndices(idColumn)) == 0 {
		return nil, false, fmt.Errorf("%s: stream %q is deduped at apply, but its batches have no %s", db, stream, idColumn)
	}
	if _, ok := starts[stream]; cfg.Mode == destination.Overwrite && !ok {
		return nil, false, fmt.Errorf("%s: stream %q overwrites, but its read has no start", db, stream)
	}
	if cfg.Mode == destination.Merge {
		if key, err = KeyColumns(batch.Schema(), cfg.PrimaryKey); err != nil {
			return nil, false, fmt.Errorf("%s: stream %q: %w", db, stream, err)
		}
	}
	return key, true, nil
}

// PatchesKey is RowsKey for Patch.
func PatchesKey(db string, streams map[string]destination.Stream, stream string, batch arrow.RecordBatch) (key []string, ok bool, err error) {
	cfg, ok := streams[stream]
	switch {
	case !ok:
		return nil, false, fmt.Errorf("%s: unknown stream %q", db, stream)
	case cfg.Mode != destination.Merge && cfg.Mode != destination.Append:
		return nil, false, fmt.Errorf("%s: stream %q overwrites, and takes no patches", db, stream)
	case batch.NumRows() == 0:
		return nil, false, nil
	case len(batch.Schema().FieldIndices(idColumn)) == 0 || len(batch.Schema().FieldIndices(destination.HeldColumn)) == 0:
		return nil, false, fmt.Errorf("%s: patches for %q need %s and %s", db, stream, idColumn, destination.HeldColumn)
	}
	if cfg.Mode == destination.Merge {
		var err error
		if key, err = KeyColumns(batch.Schema(), cfg.PrimaryKey); err != nil {
			return nil, false, fmt.Errorf("%s: patches for %q: %w", db, stream, err)
		}
	}
	return key, true, nil
}

// Last adds to last the columns of schema whose held cells keep the
// target's value.
func Last(schema *arrow.Schema, last []string) []string {
	for _, f := range schema.Fields() {
		if v, _ := f.Metadata.GetValue(destination.HeldKey); v == "last" && !slices.Contains(last, f.Name) {
			last = append(last, f.Name)
		}
	}
	return last
}

// Fills adds to fills the columns of a patch's schema that fill another held
// column, by that column.
func Fills(schema *arrow.Schema, fills map[string]string) map[string]string {
	for _, f := range schema.Fields() {
		if held, ok := f.Metadata.GetValue(destination.FillsKey); ok {
			if fills == nil {
				fills = map[string]string{}
			}
			fills[f.Name] = held
		}
	}
	return fills
}

// KeyColumns returns the destination names of a stream's primary key, which
// the catalog gives by source name.
func KeyColumns(schema *arrow.Schema, key []string) ([]string, error) {
	renamed := map[string]string{}
	if v, ok := schema.Metadata().GetValue(sourceNamesKey); ok {
		var names map[string]string
		if err := json.Unmarshal([]byte(v), &names); err != nil {
			return nil, fmt.Errorf("its %s metadata: %w", sourceNamesKey, err)
		}
		for dst, src := range names {
			renamed[src] = dst
		}
	}
	out := make([]string, len(key))
	for i, k := range key {
		out[i] = k
		if dst, ok := renamed[k]; ok {
			out[i] = dst
		}
		if len(schema.FieldIndices(out[i])) == 0 {
			return nil, fmt.Errorf("it merges on %q, which its batches don't have", k)
		}
	}
	return out, nil
}

func (p *Epochs[T]) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	d := &p.Dialect
	key, ok, err := RowsKey(d.Name, d.Streams, p.Starts, stream, batch)
	if !ok {
		return err
	}
	st, err := d.SQL.Stage(ctx, p.Staging, d.StagingName(d.Open.Pipeline, stream, e, d.Open.Lane), e, stream, d.TargetTable(batch.Schema(), stream), key, batch)
	if err != nil {
		return err
	}
	t := st.Base()
	p.SetTable(stream, t.Target)
	t.Last = Last(batch.Schema(), t.Last)
	return nil
}

// Patch stages patches in a table of their own, which Commit applies after
// the epoch's rows, in the same transaction and under the same marker.
func (p *Epochs[T]) Patch(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	d := &p.Dialect
	key, ok, err := PatchesKey(d.Name, d.Streams, stream, batch)
	if !ok {
		return err
	}
	target := p.Table(stream)
	if target == "" {
		target = d.TargetTable(batch.Schema(), stream)
	}
	st, err := d.SQL.Stage(ctx, p.Patches, d.StagingName(d.Open.Pipeline, stream+"\x00patch", e, d.Open.Lane), e, stream, target, key, batch)
	if err != nil {
		return err
	}
	t := st.Base()
	t.Fills = Fills(batch.Schema(), t.Fills)
	return nil
}

// PreCommit has nothing to flush: every Write committed its rows to staging.
func (p *Epochs[T]) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	return p.Describe(p.Dialect.Open, e, p.Dialect.ReadName)
}

func (p *Epochs[T]) Commit(ctx context.Context, _ destination.Epoch, committables []destination.Staged) (destination.Committed, error) {
	desc, err := p.MergeLanes(p.Dialect.Name, committables)
	if err != nil {
		return destination.Committed{}, err
	}
	c := destination.Committed{Collapsed: map[string]uint64{}}
	return c, p.Dialect.SQL.Apply(ctx, desc, desc.Streams, c.Collapsed)
}

func (p *Epochs[T]) Recover(ctx context.Context, gen uint64, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	d := &p.Dialect
	if err := d.SQL.Fence(ctx, gen); err != nil {
		return 0, err
	}
	if e.IsZero() {
		d.SQL.DropRemovedReads(ctx)
		return destination.Applied, nil
	}

	desc, err := p.MergeLanes(d.Name, committables)
	if err != nil {
		return 0, err
	}
	missing, err := d.SQL.Unapplied(ctx, desc, desc.Streams)
	if err != nil {
		return 0, err
	}
	// An overwrite read that ended without rows in the epoch needs no
	// staging.
	needs, present := 0, 0
	for _, m := range missing {
		for _, t := range slices.Concat(m.Stagings, m.Patches) {
			needs++
			ok, err := d.SQL.Exists(ctx, t)
			if err != nil {
				return 0, err
			}
			if ok {
				present++
			}
		}
	}
	var r destination.Recovery
	switch {
	case len(missing) == 0:
		d.SQL.DropStaging(ctx, desc.Streams)
		r = destination.Applied
	case present == needs:
		if err := d.SQL.Apply(ctx, desc, missing, map[string]uint64{}); err != nil {
			return 0, err
		}
		r = destination.Redone
	case len(missing) == len(desc.Streams):
		// Nothing of it is visible, and some lane's staging is gone.
		r = destination.Lost
	default:
		return 0, fmt.Errorf("%s: epoch %d-g%d is partly applied and the staging for the rest is gone", d.Name, desc.Epoch, desc.Generation)
	}
	if r != destination.Lost {
		d.SQL.TrimMarkers(ctx, desc.Epoch)
	}
	d.SQL.DropStale(ctx, gen)
	d.SQL.DropRemovedReads(ctx)
	return r, nil
}
