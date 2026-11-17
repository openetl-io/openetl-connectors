//go:build integration

package oracle

import (
	"cmp"
	"context"
	"database/sql"
	"net/url"
	"os"
	"testing"
	"time"
)

// testDSN is the Oracle database the integration tests run against: by
// default the container the README's setup describes, with ARCHIVELOG and
// minimal supplemental logging, and a PDB user openetl.
func testDSN() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_ORACLE_DSN"), "oracle://openetl:openetl@127.0.0.1:1521/FREEPDB1")
}

// badPasswordDSN is testDSN as a user that doesn't exist, whose sign-ins
// fail as a wrong password's do, without counting towards locking a real
// user.
func badPasswordDSN() string {
	u, err := url.Parse(testDSN())
	if err != nil {
		panic(err)
	}
	u.User = url.UserPassword("openetl_nobody", "not-the-password")
	return u.String()
}

// adminDSN signs in as SYSDBA to the test database's PDB, to create the
// users the tests sign in as.
func adminDSN() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_ORACLE_ADMIN_DSN"), "oracle://sys:openetl@127.0.0.1:1521/FREEPDB1?DBA PRIVILEGE=SYSDBA")
}

func adminDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := open(t.Context(), adminDSN(), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// closedDSN names a port nothing listens on.
const closedDSN = "oracle://openetl:openetl@127.0.0.1:1/FREEPDB1?CONNECTION TIMEOUT=2"

// testDB connects to the test database as the connector does.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := open(ctx, testDSN(), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// exec runs each statement, failing the test on an error.
func exec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// dropTable drops a table if it's there.
func dropTable(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	exec(t, db, `BEGIN EXECUTE IMMEDIATE 'DROP TABLE `+quoteIdent(name)+` PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`)
}
