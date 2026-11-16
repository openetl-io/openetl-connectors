package sqlserver

import (
	"database/sql"
	"fmt"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"
)

// openDB parses a DSN, a sqlserver:// URL or an ADO or ODBC connection
// string, into a pool that connects lazily.
func openDB(dsn string) (*sql.DB, error) {
	cfg, err := msdsn.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.AppName == "" {
		cfg.AppName = "openetl"
	}
	return sql.OpenDB(mssql.NewConnectorConfig(cfg)), nil
}

// quote quotes an identifier, as QUOTENAME does.
func quote(name string) string { return "[" + strings.ReplaceAll(name, "]", "]]") + "]" }

// qualified is schema.name, quoted.
func qualified(schema, name string) string { return quote(schema) + "." + quote(name) }

// literal is s as a Unicode string literal, N'...'.
func literal(s string) string { return "N'" + strings.ReplaceAll(s, "'", "''") + "'" }

func quoteAll(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = quote(n)
	}
	return strings.Join(q, ", ")
}

// param is the n-th (from 1) statement parameter's name.
func param(n int) string { return fmt.Sprintf("@p%d", n) }
