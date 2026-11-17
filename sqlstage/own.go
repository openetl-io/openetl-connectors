package sqlstage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// Own is a database/sql destination's fence and markers, in its SQL: the
// fence's row of each pipeline, which its applies hold and Recover raises,
// and a marker of each stream each epoch applied. Its Open sets it.
type Own struct {
	DB         *sql.DB
	Name       string
	Pipeline   string
	Generation uint64
	Classify   func(error) error
	// FenceRow selects the pipeline's (the first argument) generation,
	// locked by lock; "" doesn't lock it.
	FenceRow func(lock string) string
	// Marked counts the markers of a pipeline, stream, generation and epoch;
	// Mark inserts one, of those; Trim deletes the pipeline's below an epoch.
	Marked, Mark, Trim string
}

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// LockFence reads the pipeline's fence in tx, locking its row by lock, and
// fails if a newer generation raised it.
func (o *Own) LockFence(ctx context.Context, tx *sql.Tx, lock string) error {
	var fence uint64
	err := tx.QueryRowContext(ctx, o.FenceRow(lock), o.Pipeline).Scan(&fence)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New(o.Name + ": the fence row is missing; Recover must run first")
	}
	if err != nil {
		return o.Classify(fmt.Errorf("%s: lock fence: %w", o.Name, err))
	}
	if fence > o.Generation {
		return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, o.Generation)
	}
	return nil
}

// Fenced runs fn, which may run DDL on other connections, while a
// transaction holds the fence row by lock, so that a newer generation's
// Recover waits for it and a fenced writer can't run it.
func (o *Own) Fenced(ctx context.Context, lock string, fn func() error) error {
	tx, err := o.DB.BeginTx(ctx, nil)
	if err != nil {
		return o.Classify(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := o.LockFence(ctx, tx, lock); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	return tx.Commit()
}

// Unfenced fails with destination.ErrFenced if a newer generation has raised
// the fence, as when it recovered an epoch this one would apply.
func (o *Own) Unfenced(ctx context.Context) error {
	var fence uint64
	err := o.DB.QueryRowContext(ctx, o.FenceRow(""), o.Pipeline).Scan(&fence)
	switch {
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return o.Classify(fmt.Errorf("%s: read fence: %w", o.Name, err))
	case fence > o.Generation:
		return fmt.Errorf("%w: fence is %d, applier is %d", destination.ErrFenced, fence, o.Generation)
	}
	return nil
}

func (o *Own) marked(ctx context.Context, q queryRower, desc Staged, stream string) (bool, error) {
	var n int
	if err := q.QueryRowContext(ctx, o.Marked, o.Pipeline, stream, desc.Generation, desc.Epoch).Scan(&n); err != nil {
		return false, o.Classify(fmt.Errorf("%s: read marker for %s: %w", o.Name, stream, err))
	}
	return n > 0, nil
}

func (o *Own) Unapplied(ctx context.Context, desc Staged, streams []Stream) ([]Stream, error) {
	var todo []Stream
	for _, s := range streams {
		done, err := o.marked(ctx, o.DB, desc, s.Name)
		if err != nil {
			return nil, err
		}
		if !done {
			todo = append(todo, s)
		}
	}
	return todo, nil
}

// ApplyTx applies streams in one transaction, which holds the fence row by
// lock: each that has no marker yet, by apply, and its marker. It adds the
// rows each deduped stream skipped to collapsed.
func (o *Own) ApplyTx(ctx context.Context, opts *sql.TxOptions, lock string, desc Staged, streams []Stream, apply func(*sql.Tx, Stream) (uint64, error), collapsed map[string]uint64) error {
	skipped := map[string]uint64{}
	tx, err := o.DB.BeginTx(ctx, opts)
	if err != nil {
		return o.Classify(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := o.LockFence(ctx, tx, lock); err != nil {
		return err
	}
	for _, s := range streams {
		done, err := o.marked(ctx, tx, desc, s.Name)
		if err != nil {
			return err
		}
		if done {
			continue
		}
		n, err := apply(tx, s)
		if err != nil {
			return o.Classify(fmt.Errorf("%s: apply %s: %w", o.Name, s.Name, err))
		}
		if s.Deduped() && s.Rows > n {
			skipped[s.Name] = s.Rows - n
		}
		if _, err := tx.ExecContext(ctx, o.Mark, o.Pipeline, s.Name, desc.Generation, desc.Epoch); err != nil {
			return o.Classify(fmt.Errorf("%s: write marker for %s: %w", o.Name, s.Name, err))
		}
	}
	if err := tx.Commit(); err != nil {
		return o.Classify(err)
	}
	for name, n := range skipped {
		collapsed[name] += n
	}
	return nil
}

// TrimMarkers deletes the pipeline's markers below an applied epoch. Recovery
// checks only HEAD's, and HEAD never moves back past an applied epoch; a lost
// one moves it back to the previous record, whose markers it then checks.
func (o *Own) TrimMarkers(ctx context.Context, applied uint64) {
	_, _ = o.DB.ExecContext(ctx, o.Trim, o.Pipeline, applied)
}

// Applier is how a database/sql destination applies a stream's staging, in
// its SQL; P is what it prepared of the stream for that.
type Applier[P any] interface {
	MergeSQL(s Stream, p P) []string
	// Its rows affected are the rows it applied.
	InsertSQL(s Stream, p P) string
	PatchSQL(s Stream, p P) []string
	// Replace makes an ended overwrite's read the target.
	Replace(ctx context.Context, tx *sql.Tx, s Stream, p P) error
}

// ApplyStream applies one stream's staging in tx, its rows and then its
// patches, and returns the rows it inserted.
func ApplyStream[P any](ctx context.Context, name string, tx *sql.Tx, a Applier[P], s Stream, p P) (uint64, error) {
	n, err := applyRows(ctx, name, tx, a, s, p)
	if err != nil {
		return 0, err
	}
	for _, q := range a.PatchSQL(s, p) {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return 0, fmt.Errorf("patch: %w", err)
		}
	}
	return n, nil
}

func applyRows[P any](ctx context.Context, name string, tx *sql.Tx, a Applier[P], s Stream, p P) (uint64, error) {
	if s.Mode == ModeMerge {
		if len(s.Stagings) == 0 {
			return 0, nil
		}
		for _, q := range a.MergeSQL(s, p) {
			if _, err := tx.ExecContext(ctx, q); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	var n uint64
	if len(s.Stagings) > 0 {
		res, err := tx.ExecContext(ctx, a.InsertSQL(s, p))
		if err != nil {
			return 0, err
		}
		affected, err := res.RowsAffected()
		if err != nil || affected < 0 {
			return 0, fmt.Errorf("%s: the insert reported %d rows: %w", name, affected, err)
		}
		n = uint64(affected)
	}
	if s.Ended {
		return n, a.Replace(ctx, tx, s, p)
	}
	return n, nil
}
