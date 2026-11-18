package sqlstage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"maps"
	"sync/atomic"
	"testing"
	"time"
)

// fakeDB is a database whose every query returns one row of 0: a fence of
// generation 0, and no marker.
type fakeDB struct{ rolledBack, committed atomic.Int32 }

func (d *fakeDB) Connect(context.Context) (driver.Conn, error) { return fakeConn{d}, nil }
func (d *fakeDB) Driver() driver.Driver                        { return d }
func (d *fakeDB) Open(string) (driver.Conn, error)             { return fakeConn{d}, nil }

type fakeConn struct{ d *fakeDB }

func (c fakeConn) Prepare(string) (driver.Stmt, error) { return fakeStmt{}, nil }
func (c fakeConn) Close() error                        { return nil }
func (c fakeConn) Begin() (driver.Tx, error)           { return fakeTx(c), nil }

type fakeTx struct{ d *fakeDB }

func (t fakeTx) Commit() error   { t.d.committed.Add(1); return nil }
func (t fakeTx) Rollback() error { t.d.rolledBack.Add(1); return nil }

type fakeStmt struct{}

func (fakeStmt) Close() error                               { return nil }
func (fakeStmt) NumInput() int                              { return -1 }
func (fakeStmt) Exec([]driver.Value) (driver.Result, error) { return driver.RowsAffected(1), nil }
func (fakeStmt) Query([]driver.Value) (driver.Rows, error)  { return &fakeRows{}, nil }

type fakeRows struct{ done bool }

func (r *fakeRows) Columns() []string { return []string{"n"} }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done, dest[0] = true, int64(0)
	return nil
}

func fakeOwn(d *fakeDB) *Own {
	return &Own{DB: sql.OpenDB(d), Name: "db", Pipeline: "p", Generation: 1, Classify: func(err error) error { return err }, FenceRow: func(string) string { return "fence" }}
}

func TestApplyTxCountsNoCollapsedRowsOfAMerge(t *testing.T) {
	o := fakeOwn(&fakeDB{})
	streams := []Stream{{Name: "merged", Mode: ModeMerge, Dedupe: true, Rows: 5}, {Name: "appended", DedupeEpoch: true, Rows: 5}}
	collapsed := map[string]uint64{}
	err := o.ApplyTx(t.Context(), nil, "", Staged{Generation: 1, Epoch: 1}, streams, func(_ *sql.Tx, s Stream) (uint64, error) {
		if s.Mode == ModeMerge {
			return 0, nil
		}
		return 3, nil
	}, collapsed)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]uint64{"appended": 2}; !maps.Equal(collapsed, want) {
		t.Errorf("collapsed %v, want %v", collapsed, want)
	}
}

func TestFencedHoldsTheFenceUntilFnReturns(t *testing.T) {
	d := &fakeDB{}
	ctx, cancel := context.WithCancel(t.Context())
	err := fakeOwn(d).Fenced(ctx, "", func() error {
		cancel()
		// database/sql rolls a transaction back soon after its context ends.
		time.Sleep(100 * time.Millisecond)
		if d.rolledBack.Load() > 0 {
			t.Error("the fence's transaction ended while fn ran")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if d.committed.Load() != 1 {
		t.Error("the fence's transaction didn't commit once fn returned")
	}
}
