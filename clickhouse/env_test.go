//go:build integration

package clickhouse

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func testAddr() string { return cmp.Or(os.Getenv("OPENETL_TEST_CLICKHOUSE_ADDR"), "127.0.0.1:9000") }

// testPassword is the user openetl's, as .github/actions/services sets it.
const testPassword = "clickhouse-test-password"

func testConfig(addr, password, database string) []byte {
	return fmt.Appendf(nil, `{"addr": %q, "user": "openetl", "password": %q, "database": %q}`, addr, password, database)
}

func testConn(t *testing.T) driver.Conn {
	t.Helper()
	conn, err := ch.Open(&ch.Options{
		Addr:     []string{testAddr()},
		Auth:     ch.Auth{Database: "openetl", Username: "openetl", Password: testPassword},
		Settings: ch.Settings{"async_insert": 0, "mutations_sync": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Ping(t.Context()); err != nil {
		t.Fatalf("clickhouse at %s: %v", testAddr(), err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// readTable reads a table as a reader should: with FINAL where its engine
// replaces rows. None if it doesn't exist.
func readTable(ctx context.Context, conn driver.Conn, db, table string) ([]map[string]any, error) {
	var engine string
	err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", db, table).Scan(&engine)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	final := ""
	if engine == "ReplacingMergeTree" {
		final = " FINAL"
	}
	rows, err := conn.Query(ctx, "SELECT * FROM "+qualified(db, table)+final)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	types := rows.ColumnTypes()
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(types))
		for i, ct := range types {
			vals[i] = reflect.New(ct.ScanType()).Interface()
		}
		if err := rows.Scan(vals...); err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, ct := range types {
			v := reflect.ValueOf(vals[i]).Elem()
			for v.Kind() == reflect.Pointer {
				if v.IsNil() {
					break
				}
				v = v.Elem()
			}
			if v.Kind() == reflect.Pointer && v.IsNil() {
				row[ct.Name()] = nil
				continue
			}
			row[ct.Name()] = v.Interface()
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// newDatabase creates a database of the test's own, dropped when it ends.
func newDatabase(t *testing.T) string {
	t.Helper()
	conn := testConn(t)
	db := fmt.Sprintf("test_%d", time.Now().UnixNano())
	if err := conn.Exec(t.Context(), "CREATE DATABASE "+ident(db)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+ident(db)+" SYNC") })
	return db
}

type sourceEnv struct {
	t    *testing.T
	db   string
	conn driver.Conn
}

// exec runs a statement, with $db as the test's database.
func (e sourceEnv) exec(q string) {
	e.t.Helper()
	if err := e.conn.Exec(e.t.Context(), strings.ReplaceAll(q, "$db", ident(e.db))); err != nil {
		e.t.Fatalf("%s: %v", q, err)
	}
}

// A table of every type the source maps, with nulls and extremes.
const kindsTable = `CREATE TABLE $db.kinds (
	id Int32, i8 Int8, u64 UInt64, i128 Int128, u256 Nullable(UInt256), i256 Int256, f32 Float32, f64 Nullable(Float64), bf BFloat16,
	d Decimal(18, 4), d76 Nullable(Decimal(76, 10)), b Bool, s Nullable(String), fs FixedString(4), lc LowCardinality(Nullable(String)),
	e Enum8('sad' = 1, 'ok' = 2), u Nullable(UUID), ip4 IPv4, ip6 IPv6, dt DateTime('Asia/Tokyo'), dt3 Nullable(DateTime64(3)), dt9 DateTime64(9, 'UTC'),
	d16 Date, d32 Nullable(Date32), t Nullable(Time), t6 Time64(6), a Array(Nullable(Int64)), aa Array(Array(String)), m Map(String, Int32),
	tup Tuple(n Int32, s Nullable(String)), tu Tuple(Int8, String), j JSON, v Variant(Int64, String), pt Point,
	u8 UInt8, u16 Nullable(UInt16), u32 UInt32, au8 Array(UInt8), at Array(Time), t9 Time64(9), adt9 Array(DateTime64(9))
) ENGINE = MergeTree ORDER BY id`

const kindsRows = `INSERT INTO $db.kinds VALUES
	(1, -128, 18446744073709551615, -170141183460469231731687303715884105728, 115792089237316195423570985008687907853269984665640564039457584007913129639935,
	 -57896044618658097711785492504343953926634992332820282019728792003956564819968, 1.5, nan, 1.5,
	 12345678901234.5678, 123456789012345678901234567890123456789012345678901234567890.1234567891, true, 'naïve', 'ab\0\0', 'x',
	 'ok', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '1.2.3.4', '::1', '2026-11-03 10:00:00', '1900-01-01 00:00:00.001', '2262-04-11 23:47:16.854775807',
	 '2149-06-06', '1900-01-01', '-999:59:59', '23:59:59.999999', [1, NULL, 3], [['a'], []], map('x', 1, 'y', 2),
	 (1, NULL), (-1, 'z'), '{"a": [1, {"b": null}]}', 42, (1.5, -2),
	 255, 65535, 4294967295, [0, 255], ['-01:00:00'], '23:59:59.123456789', ['2262-04-11 23:47:16.854775807']),
	(2, 0, 0, 170141183460469231731687303715884105727, NULL, 57896044618658097711785492504343953926634992332820282019728792003956564819967,
	 -inf, NULL, 0, -0.0001, NULL, false, NULL, '', NULL, 'sad', NULL, '0.0.0.0', '::', '1970-01-01 09:00:00', NULL, '1900-01-01 00:00:00',
	 '1970-01-01', NULL, NULL, '00:00:00', [], [], map(), (0, 's'), (0, ''), '{}', 'text', (0, 0),
	 0, NULL, 0, [], [], '00:00:00', []),
	(3, 1, 1, 1, 1, 1, 0, 1e308, 0, 0, 1, true, '', 'abcd', '', 'ok', '00000000-0000-0000-0000-000000000000', '255.255.255.255', 'ffff::1',
	 '2106-02-07 15:28:15', '2299-12-31 23:59:59.999', '1970-01-01 00:00:00', '2149-06-06', '2299-12-31', '12:00:00', '00:00:00.000001',
	 [NULL], [['']], map('', 0), (2147483647, ''), (127, 'x'), '{"n": 1}', NULL, (0, 0),
	 1, 1, 1, [1], ['12:00:00'], '00:00:00.000000001', ['1970-01-01 00:00:00.000000001'])`
