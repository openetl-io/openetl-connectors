//go:build integration

package mysql

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// testServer is one server the integration tests run against.
type testServer struct {
	name string
	dsn  string
}

func servers() []testServer {
	return []testServer{
		{"mysql", cmp.Or(os.Getenv("OPENETL_TEST_MYSQL_DSN"), "openetl:openetl@tcp(127.0.0.1:3306)/openetl")},
		{"mariadb", cmp.Or(os.Getenv("OPENETL_TEST_MARIADB_DSN"), "openetl:openetl@tcp(127.0.0.1:3307)/openetl")},
	}
}

// eachServer runs f as a subtest per server.
func eachServer(t *testing.T, f func(t *testing.T, srv testServer)) {
	for _, srv := range servers() {
		t.Run(srv.name, func(t *testing.T) { f(t, srv) })
	}
}

// badPassword is the server's DSN with a password it rejects.
func (s testServer) badPassword(t *testing.T) string {
	c, err := mysql.ParseDSN(s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	c.Passwd = "not-the-password"
	return c.FormatDSN()
}

const closedPort = "openetl:openetl@tcp(127.0.0.1:1)/openetl?timeout=2s"

// testDB connects to the server as the connector does.
func testDB(t *testing.T, dsn string) (*sql.DB, server) {
	t.Helper()
	c, err := parseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db, srv, err := open(t.Context(), c, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, srv
}

// newDatabase creates a database that the test drops when it ends.
func newDatabase(t *testing.T, db *sql.DB) string {
	t.Helper()
	name := fmt.Sprintf("t_%x", rand.Uint64())
	if _, err := db.ExecContext(t.Context(), "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name) })
	return name
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// readTable returns a table's rows as readers see them: booleans as bool,
// JSON decoded with its numbers' digits kept, text as string, DATETIME as
// time.Time in UTC, and the rest as the binary protocol gives them, which
// keeps FLOAT's digits. None if the table doesn't exist.
func readTable(ctx context.Context, db *sql.DB, srv server, database, table string) ([]map[string]any, error) {
	types := map[string]string{}
	rows, err := db.QueryContext(ctx, "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", database, table)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		types[name] = normType(typ)
	}
	_ = rows.Close()
	if len(types) == 0 {
		return nil, nil
	}
	if srv.mariadb {
		// MariaDB's JSON is text that a constraint checks.
		rows, err := db.QueryContext(ctx, "SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ?", database, table)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var clause string
			if err := rows.Scan(&clause); err != nil {
				return nil, err
			}
			if col, ok := strings.CutPrefix(clause, "json_valid(`"); ok {
				types[strings.TrimSuffix(col, "`)")] = "json"
			}
		}
		_ = rows.Close()
	}
	stmt, err := db.PrepareContext(ctx, "SELECT * FROM "+quoteIdent(database)+"."+quoteIdent(table))
	if err != nil {
		if n, _ := serverError(err); n == 1146 {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	rows, err = stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
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
			v, err := readValue(types[c], vals[i])
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", c, err)
			}
			row[c] = v
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func readValue(typ string, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	b, isBytes := v.([]byte)
	switch {
	case typ == "tinyint(1)":
		switch x := v.(type) {
		case int64:
			return x != 0, nil
		case []byte:
			return string(x) != "0", nil
		}
	case typ == "json":
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		var out any
		if err := d.Decode(&out); err != nil {
			return nil, err
		}
		return out, nil
	case strings.HasPrefix(typ, "datetime") || strings.HasPrefix(typ, "timestamp"):
		t, err := time.ParseInLocation("2006-01-02 15:04:05.999999", string(b), time.UTC)
		if err != nil {
			return string(b), nil
		}
		return t, nil
	case binary(typ):
		return b, nil
	case isBytes:
		return string(b), nil
	}
	return v, nil
}
