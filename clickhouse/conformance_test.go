//go:build integration

package clickhouse

import (
	"context"
	"fmt"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/conformance"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestSourceConformance(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.users (id Int64, name String, joined Date) ENGINE = MergeTree ORDER BY id")
	e.exec("INSERT INTO $db.users SELECT number + 1, concat('u', toString(number + 1)), toDate('2020-01-01') + number FROM numbers(120)")
	e.exec("CREATE TABLE $db.events (id UInt64, at DateTime64(3, 'UTC'), body String) ENGINE = ReplacingMergeTree ORDER BY id")
	e.exec("INSERT INTO $db.events SELECT number + 1, toDateTime64('2026-01-01 00:00:00', 3, 'UTC') + toIntervalMinute(number), toJSONString(map('n', number)) FROM numbers(90)")
	e.exec("CREATE TABLE $db.logs (at DateTime, line String) ENGINE = MergeTree ORDER BY tuple()")
	e.exec("INSERT INTO $db.logs SELECT toDateTime('2026-03-01 00:00:00', 'UTC') + number * 60, concat('line ', toString(number)) FROM numbers(70)")
	e.exec(kindsTable)
	e.exec(kindsRows)
	cfg := func(addr, password string) []byte {
		return fmt.Appendf(nil, `{"addr": %q, "user": "openetl", "password": %q, "database": %q, "chunkRows": 25, "batchRows": 10}`, addr, password, db)
	}
	conformance.SourceSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/source-clickhouse")),
		Config:         cfg(testAddr(), testPassword),
		BadCredentials: cfg(testAddr(), "not-the-password"),
		Unreachable:    cfg("127.0.0.1:1", testPassword),
		AddRecords: func(ctx context.Context, stream string) (int, error) {
			switch stream {
			case "events":
				return 10, e.conn.Exec(ctx, "INSERT INTO "+qualified(db, "events")+
					" SELECT number + 1001, toDateTime64('2026-02-01 00:00:00', 3, 'UTC') + toIntervalMinute(number), '{}' FROM numbers(10)")
			case "logs":
				return 5, e.conn.Exec(ctx, "INSERT INTO "+qualified(db, "logs")+
					" SELECT toDateTime('2026-04-01 00:00:00', 'UTC') + number * 60, 'later' FROM numbers(5)")
			}
			return 0, fmt.Errorf("can't add records to %s", stream)
		},
		StopsAtUntil: true,
		Streams: []conformance.Stream{
			{Name: "users", Mode: source.FullRefresh},
			{Name: "events", Mode: source.Incremental, Cursor: "at"},
			{Name: "logs", Mode: source.Incremental, Cursor: "at"},
			{Name: "kinds", Mode: source.FullRefresh},
		},
	}.Test(t)
}

func TestDestinationConformance(t *testing.T) {
	destinationSuite(t, testConfig(testAddr(), testPassword, "openetl"))
}

// TestDestinationConformanceGranted runs the suite as a user with only the
// grants the connector's page lists, on every database, as the suite writes
// to databases of its own, and with CREATE DATABASE.
func TestDestinationConformanceGranted(t *testing.T) {
	user := testUser(t, "", "", "SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER ADD COLUMN, ALTER MODIFY COLUMN, ALTER MOVE PARTITION, ALTER DELETE, ALTER UPDATE, CREATE DATABASE ON *.*")
	destinationSuite(t, userConfig(user, "openetl"))
}

func destinationSuite(t *testing.T, config []byte) {
	conn := testConn(t)
	conformance.DestinationSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/destination-clickhouse")),
		Config:         config,
		BadCredentials: testConfig(testAddr(), "not-the-password", "openetl"),
		Unreachable:    testConfig("127.0.0.1:1", testPassword, "openetl"),
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			return readTable(ctx, conn, namespace, stream)
		},
		Drop: func(ctx context.Context, namespace string) error {
			return conn.Exec(ctx, "DROP DATABASE IF EXISTS "+ident(namespace)+" SYNC")
		},
		NotNull: func(ctx context.Context, namespace, stream, column string) error {
			return conn.Exec(ctx, "ALTER TABLE "+qualified(namespace, stream)+" MODIFY COLUMN "+ident(column)+" String DEFAULT ''")
		},
		LossyTypes: map[string]string{
			"timestamp":    "clamped to 1900 to 2262-04-11",
			"timestamp_tz": "clamped to 1900 to 2262-04-11",
			"timestamp_ns": "clamped to 1900 to 2262-04-11",
			"date":         "clamped to Date32's range, 1900 to 2299",
			"interval":     "a null lands as zero, as a Tuple isn't Nullable",
			"list":         "a null lands as an empty Array, as an Array isn't Nullable",
		},
	}.Test(t)
}
