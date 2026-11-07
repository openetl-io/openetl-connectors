//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openetl-io/openetl/sdk/go/conformance"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// badPassword is dsn() with a password the server rejects.
func badPassword(t *testing.T) string {
	t.Helper()
	u, err := url.Parse(dsn())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(u.User.Username(), "not-the-password")
	return u.String()
}

const closedPort = "postgres://openetl:openetl@127.0.0.1:1/openetl?connect_timeout=2"

func TestSourceConformance(t *testing.T) {
	e := newSourceEnv(t)
	e.exec(`CREATE TABLE $schema.users (id bigint PRIMARY KEY, name text, joined date)`)
	e.exec(`INSERT INTO $schema.users SELECT i, 'u' || i, date '2020-01-01' + i FROM generate_series(1, 120) i`)
	e.exec(`CREATE TABLE $schema.events (id bigint PRIMARY KEY, at timestamptz NOT NULL, body jsonb)`)
	e.exec(`INSERT INTO $schema.events SELECT i, timestamptz '2026-01-01 00:00Z' + i * interval '1 minute', jsonb_build_object('n', i)
		FROM generate_series(1, 90) i`)
	e.exec(`CREATE TYPE $schema.mood AS ENUM ('sad', 'ok')`)
	e.exec(`CREATE TABLE $schema.kinds (id int PRIMARY KEY, s smallint, f real, d double precision, n numeric(12, 3), u numeric,
		b boolean, by bytea, uu uuid, j json, ts timestamp, tz timestamptz, t time, iv interval, ia int[], ta text[], m $schema.mood)`)
	e.exec(`INSERT INTO $schema.kinds VALUES
		(1, 1, 1.5, 'NaN', 123456789.123, 1.000000001, true, '\x00ff', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a": [1]}',
		 '2026-11-03 10:00:00.123456', 'infinity', '23:59:59.999999', '1 mon 2 days 3 hours', '{1,NULL,3}', '{"x","y"}', 'ok'),
		(2, NULL, '-Infinity', 1e300, -0.001, NULL, false, '', NULL, 'null', '-infinity', '0001-01-01 00:00Z', '00:00', '-1 day', '{}', NULL, NULL),
		(3, -32768, NULL, NULL, NULL, 12345678901234567890.5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'sad')`)
	e.exec(`CREATE TABLE $schema.orders (id bigint PRIMARY KEY, total numeric(10, 2), placed timestamptz)`)
	e.exec(`INSERT INTO $schema.orders SELECT i, i * 1.25, timestamptz '2026-03-01 00:00Z' + i * interval '1 hour' FROM generate_series(1, 70) i`)
	pub := "conf_" + e.schema
	t.Cleanup(func() { _, _ = e.conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pub) })
	cfg := func(dsn string) []byte {
		return fmt.Appendf(nil, `{"dsn": %q, "schema": %q, "chunkRows": 25, "snapshotChunkRows": 20, "publication": %q}`, dsn, e.schema, pub)
	}
	conformance.SourceSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/source-postgres")),
		Config:         cfg(dsn()),
		BadCredentials: cfg(badPassword(t)),
		Unreachable:    cfg(closedPort),
		AddRecords: func(ctx context.Context, stream string) (int, error) {
			if stream != "events" {
				return 0, fmt.Errorf("can't add records to %s", stream)
			}
			_, err := e.conn.Exec(ctx, "INSERT INTO "+pgx.Identifier{e.schema, "events"}.Sanitize()+
				" SELECT i, timestamptz '2026-02-01 00:00Z' + i * interval '1 minute', '{}' FROM generate_series(1001, 1010) i")
			return 10, err
		},
		Streams: []conformance.Stream{
			{Name: "users", Mode: source.FullRefresh},
			{Name: "events", Mode: source.Incremental, Cursor: "at"},
			{Name: "kinds", Mode: source.FullRefresh},
			{Name: "orders", Mode: source.CDC},
		},
	}.Test(t)
}

func TestDestinationConformance(t *testing.T) {
	pool, err := pgxpool.New(t.Context(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	cfg := func(dsn string) []byte { return fmt.Appendf(nil, `{"dsn": %q}`, dsn) }
	conformance.DestinationSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/destination-postgres")),
		Config:         cfg(dsn()),
		BadCredentials: cfg(badPassword(t)),
		Unreachable:    cfg(closedPort),
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			// Not from a cached plan, whose columns' types a widening changes.
			rows, err := pool.Query(ctx, "SELECT * FROM "+pgx.Identifier{namespace, stream}.Sanitize(), pgx.QueryExecModeExec)
			var out []map[string]any
			if err == nil {
				out, err = pgx.CollectRows(rows, pgx.RowToMap)
			}
			if pe, ok := errors.AsType[*pgconn.PgError](err); ok && (pe.Code == "42P01" || pe.Code == "3F000") {
				return nil, nil
			}
			return out, err
		},
		Drop: func(ctx context.Context, namespace string) error {
			_, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{namespace}.Sanitize()+" CASCADE")
			return err
		},
		NotNull: func(ctx context.Context, namespace, stream, column string) error {
			_, err := pool.Exec(ctx, "ALTER TABLE "+pgx.Identifier{namespace, stream}.Sanitize()+" ALTER COLUMN "+pgx.Identifier{column}.Sanitize()+" SET NOT NULL")
			return err
		},
	}.Test(t)
}
