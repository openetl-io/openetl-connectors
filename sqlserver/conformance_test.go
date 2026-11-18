//go:build integration

package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	mssql "github.com/microsoft/go-mssqldb"

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

const closedPort = "sqlserver://openetl:openetl@127.0.0.1:1?database=openetl&dial+timeout=2"

func TestSourceConformance(t *testing.T) {
	for _, method := range []string{methodTracking, methodCapture} {
		t.Run(method, func(t *testing.T) {
			e := newSourceEnv(t)
			e.exec(`CREATE TABLE $schema.users (id bigint PRIMARY KEY, name nvarchar(20), joined date)`)
			e.exec(`INSERT INTO $schema.users SELECT n, CONCAT('u', n), DATEADD(day, n, '2020-01-01') FROM (SELECT TOP (120) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x`)
			e.exec(`CREATE TABLE $schema.events (id bigint PRIMARY KEY, at datetime2 NOT NULL, body nvarchar(max))`)
			e.exec(`INSERT INTO $schema.events SELECT n, DATEADD(minute, n, '2026-01-01'), CONCAT('{"n": ', n, '}') FROM (SELECT TOP (90) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x`)
			e.exec(`CREATE TABLE $schema.kinds (id int PRIMARY KEY, s smallint, f real, d float, n decimal(12, 3), m money, b bit, bin varbinary(max),
				uu uniqueidentifier, ts datetime2, tz datetimeoffset, t time, x xml, h hierarchyid, rv rowversion)`)
			e.exec(`INSERT INTO $schema.kinds (id, s, f, d, n, m, b, bin, uu, ts, tz, t, x, h) VALUES
				(1, 1, 1.5, 1e300, 123456789.123, 12.5, 1, 0x00ff, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2026-11-03 10:00:00.1234567', '9999-12-31 23:59:59 +00:00',
				 '23:59:59.999999', '<a/>', hierarchyid::Parse('/1/')),
				(2, NULL, -3.4e38, -2.2e-308, -0.001, -1, 0, 0x, NULL, '0001-01-01', '2026-01-01 00:00 +05:30', '00:00', NULL, NULL),
				(3, -32768, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)
			e.exec(`CREATE TABLE $schema.orders (id bigint PRIMARY KEY, total decimal(10, 2), placed datetimeoffset)`)
			e.exec(`INSERT INTO $schema.orders SELECT n, n * 1.25, DATEADD(hour, n, CONVERT(datetimeoffset, '2026-03-01 00:00 +00:00')) FROM (SELECT TOP (70) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x`)
			if method == methodTracking {
				e.exec("ALTER TABLE $schema.orders ENABLE CHANGE_TRACKING")
			} else {
				e.capture("orders")
				e.harvested()
			}
			cfg := func(dsn string) []byte {
				return fmt.Appendf(nil, `{"dsn": %q, "schema": %q, "chunkRows": 25, "snapshotChunkRows": 20, "cdcMethod": %q}`, dsn, e.schema, method)
			}
			conformance.SourceSuite{
				Launch:         conformance.Exec(conformance.Build(t, "../cmd/source-sqlserver")),
				Config:         cfg(dsn()),
				BadCredentials: cfg(badPassword(t)),
				Unreachable:    cfg(closedPort),
				StopsAtUntil:   true,
				AddRecords: func(ctx context.Context, stream string) (int, error) {
					switch stream {
					case "events":
						_, err := e.db.ExecContext(ctx, "INSERT INTO "+qualified(e.schema, "events")+
							" SELECT n, DATEADD(minute, n, '2026-02-01'), N'{}' FROM (SELECT TOP (10) 1000 + ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x")
						return 10, err
					case "orders":
						// Each call adds new orders, as a continuous read is
						// restarted.
						_, err := e.db.ExecContext(ctx, "DECLARE @from bigint = (SELECT coalesce(max(id), 0) FROM "+qualified(e.schema, "orders")+"); INSERT INTO "+qualified(e.schema, "orders")+
							" SELECT @from + n, n * 1.25, DATEADD(hour, n, CONVERT(datetimeoffset, '2026-04-01 00:00 +00:00')) FROM (SELECT TOP (5) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x")
						if err == nil && method == methodCapture {
							// A read that isn't continuous ends at what the
							// capture job had harvested when it started.
							err = waitHarvested(ctx, e.db)
						}
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
}

func TestDestinationConformance(t *testing.T) {
	db := admin(t)
	cfg := func(dsn string) []byte { return fmt.Appendf(nil, `{"dsn": %q}`, dsn) }
	conformance.DestinationSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/destination-sqlserver")),
		Config:         cfg(dsn()),
		BadCredentials: cfg(badPassword(t)),
		Unreachable:    cfg(closedPort),
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			return readTable(ctx, db, namespace, stream)
		},
		Drop: func(ctx context.Context, namespace string) error {
			dropSchema(db, namespace)
			return nil
		},
		NotNull: func(ctx context.Context, namespace, stream, column string) error {
			var c sourceColumn
			err := db.QueryRowContext(ctx, "SELECT TYPE_NAME(system_type_id), max_length, precision, scale FROM sys.columns WHERE object_id = OBJECT_ID(@p1) AND name = @p2",
				qualified(namespace, stream), column).Scan(&c.typ, &c.maxLength, &c.precision, &c.scale)
			if err == nil {
				_, err = db.ExecContext(ctx, "ALTER TABLE "+qualified(namespace, stream)+" ALTER COLUMN "+quote(column)+" "+c.sqlType()+" NOT NULL")
			}
			return err
		},
		LossyTypes: map[string]string{
			"float32": "NaN and infinities land as NULL: real holds neither",
			"float64": "NaN and infinities land as NULL: float holds neither",
		},
	}.Test(t)
}

// readTable reads a table as its readers see it, each cell as the
// conformance kit takes its type; none if it doesn't exist.
func readTable(ctx context.Context, db *sql.DB, namespace, table string) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, "SELECT * FROM "+qualified(namespace, table))
	if me, ok := errors.AsType[mssql.Error](err); ok && me.Number == 208 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, c := range cols {
			v := vals[i]
			switch b := v.(type) {
			case []byte:
				switch c.DatabaseTypeName() {
				case "DECIMAL", "MONEY", "SMALLMONEY":
					v = string(b)
				case "UNIQUEIDENTIFIER":
					var u mssql.UniqueIdentifier
					if err := u.Scan(b); err != nil {
						return nil, err
					}
					v = u.String()
				}
			case time.Time:
				if c.DatabaseTypeName() == "TIME" {
					v = b.Sub(time.Date(b.Year(), b.Month(), b.Day(), 0, 0, 0, 0, b.Location()))
				}
			}
			row[c.Name()] = v
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
