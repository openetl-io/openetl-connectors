//go:build integration

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/conformance"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestSourceConformance(t *testing.T) {
	launch := conformance.Exec(conformance.Build(t, "../cmd/source-mysql"))
	eachServer(t, func(t *testing.T, srv testServer) {
		db, _ := testDB(t, srv.dsn)
		database := newDatabase(t, db)
		q := func(stmt string) string { return strings.ReplaceAll(stmt, "$db", quoteIdent(database)) }
		for _, stmt := range []string{
			`CREATE TABLE $db.users (id bigint PRIMARY KEY, name varchar(50), joined date)`,
			`INSERT INTO $db.users WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 120)
				SELECT i, CONCAT('u', i), DATE_ADD('2020-01-01', INTERVAL i DAY) FROM n`,
			`CREATE TABLE $db.events (id bigint PRIMARY KEY, at datetime(6) NOT NULL, body json)`,
			`INSERT INTO $db.events WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 90)
				SELECT i, DATE_ADD('2026-01-01', INTERVAL i MINUTE), JSON_OBJECT('n', i) FROM n`,
			`CREATE TABLE $db.kinds (id int PRIMARY KEY, b tinyint(1), ti tinyint, tu tinyint unsigned, s smallint, su smallint unsigned,
				m mediumint, i int, iu int unsigned, bi bigint, bu bigint unsigned, f float, d double, n decimal(12, 3), w decimal(65, 30),
				c char(3), v varchar(20), l1 varchar(20) CHARACTER SET latin1, t text, e enum('sad', 'ok'), st set('a', 'b', 'c'),
				bn binary(4), vb varbinary(8), bl blob, bt bit(10), b1 bit(1), y year, dt date, dtm datetime(6), ts timestamp(3) NULL,
				tm time(6), j json, g geometry)`,
			`INSERT INTO $db.kinds VALUES
				(1, 1, -128, 255, -32768, 65535, -8388608, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615,
				 1.5, 1e300, 123456789.123, 12345678901234567890.123456789, 'ab', 'héllo', 'café', 'long text', 'ok', 'a,c',
				 X'01020000', X'00ff', X'deadbeef', b'1010101010', b'1', 2155, '2026-11-03', '2026-11-03 10:00:00.123456',
				 '2026-11-03 10:00:00.123', '23:59:59.999999', '{"a": [1, 2.5, "x"]}', ST_GeomFromText('POINT(1 2)')),
				(2, 0, 127, 0, 32767, 0, 8388607, 2147483647, 0, 9223372036854775807, 0, -3.25, -1e-300, -0.001, -1.5, '', '', '', '', 'sad', '',
				 X'', X'', X'', b'0', b'0', 1901, '1000-01-01', '1000-01-01 00:00:00', '1970-01-01 00:00:01', '-12:00:00', 'null', NULL),
				(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
				 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`,
			`CREATE TABLE $db.orders (id bigint PRIMARY KEY, total decimal(10, 2), placed timestamp(6) NULL)`,
			`INSERT INTO $db.orders WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 70)
				SELECT i, i * 1.25, DATE_ADD('2026-03-01', INTERVAL i HOUR) FROM n`,
		} {
			mustExec(t, db, q(stmt))
		}
		cfg := func(dsn string) []byte {
			return fmt.Appendf(nil, `{"dsn": %q, "database": %q, "chunkRows": 25, "snapshotChunkRows": 20}`, dsn, database)
		}
		conformance.SourceSuite{
			Launch:         launch,
			Config:         cfg(srv.dsn),
			BadCredentials: cfg(srv.badPassword(t)),
			Unreachable:    cfg(closedPort),
			StopsAtUntil:   true,
			AddRecords: func(ctx context.Context, stream string) (int, error) {
				switch stream {
				case "events":
					_, err := db.ExecContext(ctx, q(`INSERT INTO $db.events WITH RECURSIVE n(i) AS (SELECT 1001 UNION ALL SELECT i + 1 FROM n WHERE i < 1010)
						SELECT i, DATE_ADD('2026-02-01', INTERVAL i MINUTE), '{}' FROM n`))
					return 10, err
				case "orders":
					// Each call adds new orders, as a continuous read is restarted.
					var from int64
					if err := db.QueryRowContext(ctx, q("SELECT coalesce(max(id), 0) + 1 FROM $db.orders")).Scan(&from); err != nil {
						return 0, err
					}
					_, err := db.ExecContext(ctx, q(`INSERT INTO $db.orders WITH RECURSIVE n(i) AS (SELECT ? UNION ALL SELECT i + 1 FROM n WHERE i < ? + 4)
						SELECT i, i * 1.25, DATE_ADD('2026-04-01', INTERVAL i HOUR) FROM n`), from, from)
					return 5, err
				}
				return 0, fmt.Errorf("can't add records to %s", stream)
			},
			Streams: []conformance.Stream{
				{Name: "users", Mode: source.FullRefresh},
				{Name: "events", Mode: source.Incremental, Cursor: "at"},
				{Name: "kinds", Mode: source.FullRefresh},
				{Name: "orders", Mode: source.CDC},
			},
		}.Test(t)
	})
}

func TestDestinationConformance(t *testing.T) {
	launch := conformance.Exec(conformance.Build(t, "../cmd/destination-mysql"))
	eachServer(t, func(t *testing.T, srv testServer) {
		db, s := testDB(t, srv.dsn)
		cfg := func(dsn string) []byte { return fmt.Appendf(nil, `{"dsn": %q}`, dsn) }
		conformance.DestinationSuite{
			Launch:         launch,
			Config:         cfg(srv.dsn),
			BadCredentials: cfg(srv.badPassword(t)),
			Unreachable:    cfg(closedPort),
			Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
				return readTable(ctx, db, s, namespace, tableName(stream))
			},
			Drop: func(ctx context.Context, namespace string) error {
				_, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteIdent(namespace))
				return err
			},
			NotNull: func(ctx context.Context, namespace, stream, column string) error {
				var typ string
				if err := db.QueryRowContext(ctx, "SELECT COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
					namespace, tableName(stream), column).Scan(&typ); err != nil {
					return err
				}
				_, err := db.ExecContext(ctx, "ALTER TABLE "+quoteIdent(namespace)+"."+quoteIdent(tableName(stream))+" MODIFY COLUMN "+quoteIdent(column)+" "+typ+" NOT NULL")
				return err
			},
			LossyTypes: map[string]string{
				"float32": "NaN and infinities are stored as null; MySQL's FLOAT has neither",
				"float64": "NaN and infinities are stored as null; MySQL's DOUBLE has neither",
			},
		}.Test(t)
	})
}
