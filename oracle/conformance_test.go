//go:build integration

package oracle

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/conformance"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestSourceConformance(t *testing.T) {
	launch := conformance.Exec(conformance.Build(t, "../cmd/source-oracle"))
	db := testDB(t)
	_, ver, compat, err := server(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	hasBool, hasJSON := major(compat) >= 23, major(ver) >= 21
	for _, name := range []string{"CONF_USERS", "CONF_EVENTS", "CONF_KINDS", "CONF_ORDERS"} {
		dropTable(t, db, name)
		t.Cleanup(func() { dropTable(t, db, name) })
	}
	kindsCols := []string{
		"id NUMBER(10) PRIMARY KEY", "n5 NUMBER(5)", "n10 NUMBER(10)", "n19 NUMBER(19)", "nd NUMBER(12, 3)", "n NUMBER", "n38 NUMBER(38)",
		"f FLOAT", "bf BINARY_FLOAT", "bd BINARY_DOUBLE", "vc VARCHAR2(20)", "nvc NVARCHAR2(20)", "ch CHAR(3)", "nch NCHAR(2)",
		"cl CLOB", "ncl NCLOB", "bl BLOB", "rw RAW(8)", "d DATE", "ts TIMESTAMP", "ts3 TIMESTAMP(3)", "ts9 TIMESTAMP(9)",
		"tstz TIMESTAMP WITH TIME ZONE", "tsltz TIMESTAMP WITH LOCAL TIME ZONE", "iym INTERVAL YEAR TO MONTH", "ids INTERVAL DAY TO SECOND",
		"rid ROWID", "x XMLTYPE",
	}
	kindsNames := []string{
		"id", "n5", "n10", "n19", "nd", "n", "n38", "f", "bf", "bd", "vc", "nvc", "ch", "nch",
		"cl", "ncl", "bl", "rw", "d", "ts", "ts3", "ts9", "tstz", "tsltz", "iym", "ids", "rid", "x",
	}
	row1 := []string{
		"1", "-32768", "2147483647", "-9223372036854775808", "123456789.123", "1.5", "99999999999999999999999999999999999999",
		"3.25", "1.5", "1e300d", "'héllo'", "'日本'", "'ab'", "'ñ'", "'long text'", "'ncl'", "HEXTORAW('deadbeef')", "HEXTORAW('00ff')",
		"DATE '2026-11-03'", "TIMESTAMP '2026-11-03 10:00:00.123456'", "TIMESTAMP '2026-11-03 10:00:00.123'", "TIMESTAMP '2026-11-03 10:00:00.123456789'",
		"TIMESTAMP '2026-11-03 10:00:00.5 +05:30'", "TIMESTAMP '2026-11-03 10:00:00 UTC'", "INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '3 04:05:06.789' DAY TO SECOND",
		"'AAAR3HAATAAAAPrAAA'", "XMLType('<a>1</a>')",
	}
	row2 := []string{
		"2", "0", "-1", "0", "-0.001", "-1e-130", "-1", "-1e-10", "BINARY_FLOAT_NAN", "-BINARY_DOUBLE_INFINITY", "' '", "' '", "'x'", "'y'",
		"EMPTY_CLOB()", "EMPTY_CLOB()", "EMPTY_BLOB()", "HEXTORAW('00')", "TO_DATE('-4712-01-01', 'SYYYY-MM-DD')", "TIMESTAMP '0001-01-01 00:00:00'",
		"TIMESTAMP '9999-12-31 23:59:59.999'", "TIMESTAMP '1970-01-01 00:00:00.000000001'", "TIMESTAMP '2026-07-01 12:00:00 Europe/London'",
		"TIMESTAMP '1969-12-31 23:59:59 -08:00'", "INTERVAL '-3-4' YEAR TO MONTH", "INTERVAL '-0 00:00:00.000001' DAY TO SECOND",
		"NULL", "XMLType('<b/>')",
	}
	if hasBool {
		kindsCols = append(kindsCols, "b BOOLEAN")
		kindsNames = append(kindsNames, "b")
		row1 = append(row1, "TRUE")
		row2 = append(row2, "FALSE")
	}
	if hasJSON {
		kindsCols = append(kindsCols, "j JSON")
		kindsNames = append(kindsNames, "j")
		row1 = append(row1, `JSON('{"a": [1, 2.5, "x"]}')`)
		row2 = append(row2, `JSON('null')`)
	}
	exec(t, db,
		`CREATE TABLE conf_users (id NUMBER(19) PRIMARY KEY, name VARCHAR2(50), joined DATE)`,
		`INSERT INTO conf_users SELECT level, 'u' || level, DATE '2020-01-01' + level FROM dual CONNECT BY level <= 120`,
		`CREATE TABLE conf_events (id NUMBER(19) PRIMARY KEY, at TIMESTAMP(6) NOT NULL, body VARCHAR2(100))`,
		`INSERT INTO conf_events SELECT level, TIMESTAMP '2026-01-01 00:00:00' + NUMTODSINTERVAL(level, 'MINUTE'), '{"n": ' || level || '}' FROM dual CONNECT BY level <= 90`,
		"CREATE TABLE conf_kinds ("+strings.Join(kindsCols, ", ")+")",
		"INSERT INTO conf_kinds ("+strings.Join(kindsNames, ", ")+") VALUES ("+strings.Join(row1, ", ")+")",
		"INSERT INTO conf_kinds ("+strings.Join(kindsNames, ", ")+") VALUES ("+strings.Join(row2, ", ")+")",
		`INSERT INTO conf_kinds (id) VALUES (3)`,
		`CREATE TABLE conf_orders (id NUMBER(19) PRIMARY KEY, total NUMBER(10, 2), placed TIMESTAMP(6))`,
		`ALTER TABLE conf_orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`,
		`INSERT INTO conf_orders SELECT level, level * 1.25, TIMESTAMP '2026-03-01 00:00:00' + NUMTODSINTERVAL(level, 'HOUR') FROM dual CONNECT BY level <= 70`,
	)
	cfg := func(dsn string) []byte {
		return fmt.Appendf(nil, `{"dsn": %q, "tables": ["CONF_USERS", "CONF_EVENTS", "CONF_KINDS", "CONF_ORDERS"], "chunkRows": 25, "snapshotChunkRows": 20}`, dsn)
	}
	conformance.SourceSuite{
		Launch:         launch,
		Config:         cfg(testDSN()),
		BadCredentials: cfg(badPasswordDSN()),
		Unreachable:    cfg(closedDSN),
		StopsAtUntil:   true,
		AddRecords: func(ctx context.Context, stream string) (int, error) {
			switch stream {
			case "CONF_EVENTS":
				_, err := db.ExecContext(ctx, `INSERT INTO conf_events SELECT 1000 + level, TIMESTAMP '2026-02-01 00:00:00' + NUMTODSINTERVAL(level, 'MINUTE'), '{}' FROM dual CONNECT BY level <= 10`)
				return 10, err
			case "CONF_ORDERS":
				// Each call adds new orders, as a continuous read is restarted.
				_, err := db.ExecContext(ctx, `INSERT INTO conf_orders SELECT m + level, (m + level) * 1.25, TIMESTAMP '2026-04-01 00:00:00' + NUMTODSINTERVAL(level, 'HOUR')
				  FROM (SELECT nvl(max(id), 0) m FROM conf_orders) CONNECT BY level <= 5`)
				return 5, err
			}
			return 0, fmt.Errorf("can't add records to %s", stream)
		},
		Streams: []conformance.Stream{
			{Name: "CONF_USERS", Mode: source.FullRefresh},
			{Name: "CONF_EVENTS", Mode: source.Incremental, Cursor: "AT"},
			{Name: "CONF_KINDS", Mode: source.FullRefresh},
			{Name: "CONF_ORDERS", Mode: source.CDC},
		},
	}.Test(t)
}

func TestDestinationConformance(t *testing.T) {
	launch := conformance.Exec(conformance.Build(t, "../cmd/destination-oracle"))
	admin := adminDB(t)
	// A user with the privileges the README gives for other schemas, as each
	// check writes to a namespace, a schema, of its own.
	dsn := destUser(t, "OPENETL_D_CONF", anyGrants...)
	cfg := func(dsn string) []byte { return fmt.Appendf(nil, `{"dsn": %q}`, dsn) }
	conformance.DestinationSuite{
		Launch: launch,
		Config: cfg(dsn),
		// A user that doesn't exist, which no failed sign-in can lock.
		BadCredentials: cfg(dsnAs("OPENETL_D_NOBODY", "not-the-password")),
		Unreachable:    cfg(closedDSN),
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			return readTable(ctx, admin, namespace, stream)
		},
		Drop: func(ctx context.Context, namespace string) error {
			_, err := admin.ExecContext(ctx, "DROP USER "+quoteIdent(namespace)+" CASCADE")
			if oraCode(err) == 1918 {
				return nil
			}
			return err
		},
		NotNull: func(ctx context.Context, namespace, stream, column string) error {
			_, err := admin.ExecContext(ctx, "ALTER TABLE "+quoteIdent(namespace)+"."+quoteIdent(stream)+" MODIFY ("+quoteIdent(column)+" NOT NULL)")
			return err
		},
		LossyTypes: map[string]string{
			"string": "Oracle stores an empty string as null",
			"binary": "Oracle stores an empty binary value as null",
		},
	}.Test(t)
}
