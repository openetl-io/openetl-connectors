//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// Writers that create the same object at the same moment, each on its own
// connection, all succeed, whichever of them the server lets create it:
// lanes a generation's alive table; pipelines a new schema, with its fence
// and marker tables; and writers one target table, or one read's table,
// each with a column of its own, which the table then has every one of.
func TestConcurrentCreates(t *testing.T) {
	const writers, rounds = 8, 30
	e := newEnv(t)
	ctx := t.Context()
	open := func(t *testing.T, namespace, pipeline string, lane int) *writer {
		t.Helper()
		w, err := e.d.Open(ctx, destination.OpenConfig{Pipeline: pipeline, Generation: 1, Lane: lane, Namespace: namespace, Scope: destination.ScopePipeline})
		if err != nil {
			t.Fatal(err)
		}
		return w.(*writer)
	}
	// together runs f on every writer at once, and fails t with their
	// errors.
	together := func(t *testing.T, ws []*writer, f func(i int, w *writer) error) {
		t.Helper()
		start := make(chan struct{})
		errs := make([]error, len(ws))
		var wg sync.WaitGroup
		for i, w := range ws {
			wg.Go(func() {
				<-start
				errs[i] = f(i, w)
			})
		}
		close(start)
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			t.Fatal(err)
		}
	}
	// Each writer's columns: id, and one of its own.
	columns := make([][]column, writers)
	for i := range columns {
		cols, err := columnsOf(arrow.NewSchema([]arrow.Field{idField, {Name: "c" + strconv.Itoa(i), Type: arrow.BinaryTypes.String, Nullable: true}}, nil))
		if err != nil {
			t.Fatal(err)
		}
		columns[i] = cols
	}

	lanes := make([]*writer, writers)
	for i := range lanes {
		lanes[i] = open(t, e.schema, "p1", i)
		t.Cleanup(func() { _ = lanes[i].Close(context.Background()) })
	}
	if err := lanes[0].ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("alive", func(t *testing.T) {
		for gen := uint64(1); gen <= rounds; gen++ {
			together(t, lanes, func(_ int, w *writer) error { return w.ensureAlive(ctx, gen) })
			if ok, err := lanes[0].intact(ctx, lanes[0].pool, gen); err != nil || !ok {
				t.Fatalf("generation %d's alive table: %v %v", gen, ok, err)
			}
		}
	})

	t.Run("fence", func(t *testing.T) {
		for r := range rounds {
			schema := e.schema + "_f" + strconv.Itoa(r)
			t.Cleanup(func() {
				_, _ = e.conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
			})
			ws := make([]*writer, writers)
			for i := range ws {
				ws[i] = open(t, schema, "p"+strconv.Itoa(i), 0)
			}
			together(t, ws, func(_ int, w *writer) error { return w.Fence(ctx, 1) })
			for _, w := range ws {
				_ = w.Close(ctx)
			}
			var n int
			if err := e.conn.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{schema, "_openetl_fence"}.Sanitize()).Scan(&n); err != nil || n != writers {
				t.Fatalf("round %d: %d fences, %v", r, n, err)
			}
		}
	})

	for _, kind := range []string{"target", "read"} {
		t.Run(kind, func(t *testing.T) {
			for r := range rounds {
				table := kind + "_" + strconv.Itoa(r)
				together(t, lanes, func(i int, w *writer) error {
					if kind == "read" {
						return w.ensureRead(ctx, stagedStream{Name: "s", Read: table}, columns[i])
					}
					return w.ensureTable(ctx, "s", table, columns[i])
				})
				if n := e.count(fmt.Sprintf("SELECT count(*) FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", e.schema, table)); n != writers+1 {
					t.Fatalf("round %d: %s has %d columns, want %d", r, table, n, writers+1)
				}
			}
		})
	}
}
