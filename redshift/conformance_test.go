//go:build integration

package redshift

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openetl-io/openetl/sdk/go/conformance"
)

// The suite needs a cluster or workgroup and a bucket in its region:
// OPENETL_TEST_REDSHIFT_DSN, OPENETL_TEST_REDSHIFT_BUCKET, and
// OPENETL_TEST_REDSHIFT_IAM_ROLE (or "default"), with
// OPENETL_TEST_REDSHIFT_REGION if the AWS default region isn't the bucket's.
// The AWS default credentials upload the files.
func TestDestinationConformance(t *testing.T) {
	dsn, bucket := os.Getenv("OPENETL_TEST_REDSHIFT_DSN"), os.Getenv("OPENETL_TEST_REDSHIFT_BUCKET")
	if dsn == "" || bucket == "" {
		t.Skip("OPENETL_TEST_REDSHIFT_DSN and OPENETL_TEST_REDSHIFT_BUCKET aren't set")
	}
	cfg := func(dsn string) []byte {
		b, err := json.Marshal(map[string]string{
			"dsn": dsn, "bucket": bucket, "prefix": "openetl-conformance",
			"iamRole": os.Getenv("OPENETL_TEST_REDSHIFT_IAM_ROLE"), "region": os.Getenv("OPENETL_TEST_REDSHIFT_REGION"),
		})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(u.User.Username(), "not-the-password")
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pc.AfterConnect = session
	pool, err := pgxpool.NewWithConfig(t.Context(), pc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	conformance.DestinationSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/destination-redshift")),
		Config:         cfg(dsn),
		BadCredentials: cfg(u.String()),
		Unreachable:    cfg("postgres://openetl:openetl@127.0.0.1:1/dev?connect_timeout=2"),
		LossyTypes: map[string]string{
			"timestamp_ns": "Redshift's TIMESTAMP keeps microseconds",
		},
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			// VARBYTE's text is hex, which the suite would read as base64.
			binary := map[string]bool{}
			rows, err := pool.Query(ctx, `SELECT a.attname::varchar FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
				JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0
				AND (format_type(a.atttypid, a.atttypmod) LIKE 'varbyte%' OR format_type(a.atttypid, a.atttypmod) LIKE 'binary varying%')`, namespace, stream)
			if err != nil {
				return nil, err
			}
			names, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				return nil, err
			}
			for _, n := range names {
				binary[n] = true
			}
			rows, err = pool.Query(ctx, "SELECT * FROM "+quoteIdent(namespace)+"."+quoteIdent(stream))
			var out []map[string]any
			if err == nil {
				out, err = pgx.CollectRows(rows, pgx.RowToMap)
			}
			if pe, ok := errors.AsType[*pgconn.PgError](err); ok && (pe.Code == "42P01" || pe.Code == "3F000") {
				return nil, nil
			}
			for _, r := range out {
				for c := range binary {
					if s, ok := r[c].(string); ok {
						if r[c], err = hex.DecodeString(s); err != nil {
							return nil, err
						}
					}
				}
			}
			return out, err
		},
		Drop: func(ctx context.Context, namespace string) error {
			_, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+quoteIdent(namespace)+" CASCADE")
			return err
		},
		// Redshift can't make a column NOT NULL once it exists, so the suite
		// skips holds/not-null.
	}.Test(t)
}
