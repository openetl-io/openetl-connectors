//go:build integration

package redshift

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// fenceWriter is a writer of stream s in a schema of its own, on the
// cluster at OPENETL_TEST_REDSHIFT_DSN, or else on the Postgres at
// OPENETL_TEST_POSTGRES_DSN, which runs the fence's, labels' and drops'
// statements as Redshift does; set explicitly, as the package's CI has no
// Postgres.
func fenceWriter(t *testing.T, schema string, gen uint64, mode destination.WriteMode) *writer {
	t.Helper()
	dsn, redshift := os.Getenv("OPENETL_TEST_REDSHIFT_DSN"), true
	if dsn == "" {
		dsn, redshift = os.Getenv("OPENETL_TEST_POSTGRES_DSN"), false
	}
	if dsn == "" {
		t.Skip("neither OPENETL_TEST_REDSHIFT_DSN nor OPENETL_TEST_POSTGRES_DSN is set")
	}
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pc.MaxConns = 4
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if redshift {
		pc.AfterConnect = session
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), pc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	dia, err := probe(t.Context(), pool)
	if err != nil {
		t.Fatal(err)
	}
	return &writer{
		pool: pool, dialect: dia, schema: schema, Epochs: sqlstage.NewEpochs[*stagingTable](),
		cfg:     destination.OpenConfig{Pipeline: "p", Generation: gen},
		streams: map[string]destination.Stream{"s": {Name: "s", Mode: mode}},
	}
}

// A generation that no longer overwrites a stream drops its read's table
// only while the fence is its own: a newer generation, for which the stream
// overwrites, may have raised the fence since its Recover did, and be
// writing that read.
func TestRemovedReadsBehindANewerFence(t *testing.T) {
	schema := fmt.Sprintf("openetl_rs_fence_%d", time.Now().UnixNano())
	newer := fenceWriter(t, schema, 2, destination.Overwrite)
	t.Cleanup(func() {
		_, _ = newer.pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quoteIdent(schema)+" CASCADE")
	})
	if r, err := newer.Recover(t.Context(), 2, destination.Epoch{}, nil); err != nil || r != destination.Applied {
		t.Fatalf("recover: %v, %v", r, err)
	}
	read := readName("p", "s", time.UnixMicro(1))
	if err := newer.createLabelled(t.Context(), read, readLabel{Pipeline: "p", Stream: "s", Table: "s"}, []column{{name: "id", typ: "bigint"}}); err != nil {
		t.Fatal(err)
	}
	exists := func() bool {
		var n int64
		if err := newer.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2`, schema, read).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}

	// Generation 1 raised the fence before generation 2 did, and appends.
	fenceWriter(t, schema, 1, destination.Append).dropRemovedReads(t.Context())
	if !exists() {
		t.Fatal("a fenced generation dropped a newer one's read")
	}
	fenceWriter(t, schema, 2, destination.Append).dropRemovedReads(t.Context())
	if exists() {
		t.Error("the fence's generation, which appends, kept the read")
	}
}
