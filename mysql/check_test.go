//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// expectItems is connectortest.ExpectStatuses, but cdc.gtid-mode may warn.
func expectItems(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	if _, ok := want["cdc.gtid-mode"]; !ok && got["cdc.gtid-mode"] == "warning setting_mismatch" {
		got = maps.Clone(got)
		delete(got, "cdc.gtid-mode")
	}
	connectortest.ExpectStatuses(t, got, want)
}

// userDSN creates a user with grants, dropped when t ends, and gives the
// server's DSN signing in as it to database.
func userDSN(t *testing.T, admin *sql.DB, srv testServer, database, prefix string, grants ...string) string {
	t.Helper()
	user := prefix + "_" + strconv.FormatInt(time.Now().UnixNano()%1e10, 36)
	mustExec(t, admin, "CREATE USER "+quoteLiteral(user)+" IDENTIFIED BY 'pw'")
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER "+quoteLiteral(user)) })
	for _, g := range grants {
		mustExec(t, admin, "GRANT "+strings.ReplaceAll(g, "$db", quoteIdent(database))+" TO "+quoteLiteral(user))
	}
	c, err := parseDSN(srv.dsn)
	if err != nil {
		t.Fatal(err)
	}
	c.User, c.Passwd, c.DBName = user, "pw", database
	return c.FormatDSN()
}

func monitorPrivilege(srv testServer) string {
	if srv.name == "mariadb" {
		return "BINLOG MONITOR"
	}
	return "REPLICATION CLIENT"
}

// TestSourceCheckItems checks and preflights item by item: what passes, and
// what a stream the source can't read as the pipeline asks fails.
func TestSourceCheckItems(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY, at datetime, j json)")
		e.exec("CREATE TABLE $db.nokey (v int)")
		e.exec("CREATE TABLE $db.fkey (f double PRIMARY KEY)")
		e.exec("CREATE VIEW $db.v AS SELECT id FROM $db.items")
		s := e.source("")
		expectItems(t, connectortest.Items(t, s.Checks()), nil)
		if err := s.Check(t.Context()); err != nil {
			t.Fatal(err)
		}

		var expire int64
		if err := e.db.QueryRowContext(t.Context(), "SELECT @@GLOBAL.binlog_expire_logs_seconds").Scan(&expire); err != nil {
			t.Fatal(err)
		}
		streams := []source.ConfiguredStream{
			{Name: "items", SyncMode: source.CDC},
			{Name: "items", SyncMode: source.Incremental, Cursor: "at"},
			{Name: "nokey", SyncMode: source.CDC},
			{Name: "fkey", SyncMode: source.CDC},
			{Name: "items", SyncMode: source.Incremental, Cursor: "j"},
			{Name: "items", SyncMode: source.Incremental, Cursor: "nope"},
			{Name: "missing", SyncMode: source.FullRefresh},
			{Name: "v", SyncMode: source.FullRefresh},
		}
		retention := "passed"
		if expire > 0 {
			retention = "failed binlog_retention_short"
		}
		for interval, want := range map[time.Duration]string{time.Hour: "passed", time.Duration(expire+86400) * time.Second: retention} {
			got := connectortest.Items(t, s.Preflight(source.PreflightRequest{Pipeline: "p", Streams: streams, Interval: interval}))
			expectItems(t, got, map[string]string{
				"stream.nokey.cdc":    "failed primary_key_missing",
				"stream.fkey.cdc":     "failed primary_key_unsupported",
				"stream.items.cursor": "passed", "stream.items.cursor-2": "failed cursor_unsupported", "stream.items.cursor-3": "failed not_found",
				"stream.missing.read": "failed not_found",
				"stream.v.read":       "failed not_found",
				"cdc.retention":       want,
			})
		}

		// tables in the config: a table it names that isn't there, and a
		// stream it leaves out.
		s = e.source(`, "tables": ["items", "gone"]`)
		expectItems(t, connectortest.Items(t, s.Checks()), map[string]string{"tables.found": "failed not_found", "tables.read": "skipped"})
		s = e.source(`, "tables": ["items"]`)
		expectItems(t, connectortest.Items(t, s.Preflight(source.PreflightRequest{Streams: streams[2:3]})), map[string]string{
			"stream.nokey.read": "failed not_found", "stream.nokey.cdc": "skipped",
		})

		// A wrong password is turned away at db.connect, and nothing after
		// it runs.
		bad, err := NewSource(fmt.Appendf(nil, `{"dsn": %q, "database": %q}`, srv.badPassword(t), e.database))
		if err != nil {
			t.Fatal(err)
		}
		got := connectortest.Items(t, bad.Checks())
		if got["db.connect"] != "failed credentials_rejected" || got["tables.read"] != "skipped" || got["cdc.log-bin"] != "skipped" {
			t.Errorf("with a wrong password: %v", got)
		}

		// allowPublicKeyRetrieval=true isn't the server's to set, and a
		// variable the server doesn't have is the dsn's mistake.
		sep := "?"
		if strings.Contains(srv.dsn, "?") {
			sep = "&"
		}
		retrieve, err := NewSource(fmt.Appendf(nil, `{"dsn": %q, "database": %q}`, srv.dsn+sep+"allowPublicKeyRetrieval=true", e.database))
		if err != nil {
			t.Fatal(err)
		}
		expectItems(t, connectortest.Items(t, retrieve.Checks()), nil)
		unknown, err := NewSource(fmt.Appendf(nil, `{"dsn": %q, "database": %q}`, srv.dsn+sep+"no_such_variable=1", e.database))
		if err != nil {
			t.Fatal(err)
		}
		if got := connectortest.Items(t, unknown.Checks()); got["db.connect"] != "failed config_invalid" || got["tables.read"] != "skipped" {
			t.Errorf("with a variable the server doesn't have: %v", got)
		}
	})
}

// TestSourceChecksWithLeastPrivileges checks as users with the grants the
// README lists, and with one fewer: the check warns of what only cdc
// needs, and a preflight of a cdc read fails.
func TestSourceChecksWithLeastPrivileges(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY)")
		e.exec("CREATE TABLE $db.other (id int PRIMARY KEY)")
		monitor := monitorPrivilege(srv)
		req := source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: "items", SyncMode: source.CDC}}}
		for _, c := range []struct {
			name           string
			grants         []string
			checks, flight map[string]string
		}{
			{"all", []string{"SELECT ON $db.*", "REPLICATION SLAVE, " + monitor + " ON *.*"}, nil, nil},
			{"no replication slave", []string{"SELECT ON $db.*", monitor + " ON *.*"},
				map[string]string{"cdc.replication": "warning privilege_missing"}, map[string]string{"cdc.replication": "failed privilege_missing"}},
			{"no " + monitor, []string{"SELECT ON $db.*", "REPLICATION SLAVE ON *.*"},
				map[string]string{"cdc.binlog-monitor": "warning privilege_missing"}, map[string]string{"cdc.binlog-monitor": "failed privilege_missing"}},
			{"select only", []string{"SELECT ON $db.*"},
				map[string]string{"cdc.replication": "warning privilege_missing", "cdc.binlog-monitor": "warning privilege_missing"},
				map[string]string{"cdc.replication": "failed privilege_missing", "cdc.binlog-monitor": "failed privilege_missing"}},
			{"no select on a table", []string{"SELECT ON $db.other", "INSERT ON $db.items", "REPLICATION SLAVE, " + monitor + " ON *.*"},
				map[string]string{"tables.read": "failed privilege_missing"}, map[string]string{"stream.items.read": "failed privilege_missing", "stream.items.cdc": "skipped"}},
		} {
			t.Run(c.name, func(t *testing.T) {
				dsn := userDSN(t, e.db, srv, e.database, "c", c.grants...)
				s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q}`, dsn))
				if err != nil {
					t.Fatal(err)
				}
				expectItems(t, connectortest.Items(t, s.Checks()), c.checks)
				expectItems(t, connectortest.Items(t, s.Preflight(req)), c.flight)
			})
		}

		// REPLICATION SLAVE through the user's default role, which is
		// active when it signs in: the check sees it, and a cdc read uses it.
		t.Run("through a role", func(t *testing.T) {
			role := "r_" + strconv.FormatInt(time.Now().UnixNano()%1e10, 36)
			e.exec("CREATE ROLE " + quoteIdent(role))
			t.Cleanup(func() { _, _ = e.db.ExecContext(context.Background(), "DROP ROLE "+quoteIdent(role)) })
			e.exec("GRANT REPLICATION SLAVE ON *.* TO " + quoteIdent(role))
			dsn := userDSN(t, e.db, srv, e.database, "c", "SELECT ON $db.*", monitor+" ON *.*", quoteIdent(role))
			c, err := parseDSN(dsn)
			if err != nil {
				t.Fatal(err)
			}
			to := " TO "
			if srv.name == "mariadb" {
				to = " FOR "
			}
			e.exec("SET DEFAULT ROLE " + quoteIdent(role) + to + quoteLiteral(c.User))
			s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q}`, dsn))
			if err != nil {
				t.Fatal(err)
			}
			expectItems(t, connectortest.Items(t, s.Checks()), nil)
			expectItems(t, connectortest.Items(t, s.Preflight(req)), nil)
			first := read(t, s, cdc("role", nil, "items"))
			e.exec("INSERT INTO $db.items VALUES (1)")
			if n := len(read(t, s, cdc("role", map[string]string{"items": first.state("items")}, "items")).rows("items")); n != 1 {
				t.Fatalf("%d changes", n)
			}
		})
	})
}

// TestCDCSettingsAreItems turns off a binlog setting cdc needs: the check
// warns, a preflight of a cdc read fails, and one without cdc passes.
func TestCDCSettingsAreItems(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		e := newSourceEnv(t, srv)
		e.exec("CREATE TABLE $db.items (id int PRIMARY KEY)")
		e.exec("SET GLOBAL binlog_row_image = 'MINIMAL'")
		t.Cleanup(func() { _, _ = e.db.ExecContext(context.Background(), "SET GLOBAL binlog_row_image = 'FULL'") })
		s := e.source("")
		expectItems(t, connectortest.Items(t, s.Checks()), map[string]string{"cdc.binlog-row-image": "warning setting_mismatch"})
		expectItems(t, connectortest.Items(t, s.Preflight(source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "items", SyncMode: source.CDC}}})),
			map[string]string{"cdc.binlog-row-image": "failed setting_mismatch"})
		expectItems(t, connectortest.Items(t, s.Preflight(source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "items"}}})), nil)
	})
}

// TestDestinationCheckItems checks and preflights as users with the grants
// the README lists, and with one fewer.
func TestDestinationCheckItems(t *testing.T) {
	eachServer(t, func(t *testing.T, srv testServer) {
		admin, _ := testDB(t, srv.dsn)
		database := newDatabase(t, admin)
		mustExec(t, admin, "CREATE TABLE "+quoteIdent(database)+".m (id bigint PRIMARY KEY, v text)")
		streams := []destination.Stream{{Name: "a"}, {Name: "o", Mode: destination.Overwrite}, {Name: "m", Mode: destination.Merge, PrimaryKey: []string{"id"}},
			{Name: "nokey", Mode: destination.Merge}}
		all := "CREATE, ALTER, DROP, SELECT, INSERT, UPDATE, DELETE ON $db.*"
		for _, c := range []struct {
			name           string
			grants         []string
			checks, flight map[string]string
		}{
			{"all", []string{all}, nil, nil},
			{"no delete", []string{"CREATE, ALTER, DROP, SELECT, INSERT, UPDATE ON $db.*"},
				map[string]string{"database.create": "failed privilege_missing"},
				map[string]string{"database.create": "failed privilege_missing", "stream.a.write": "skipped", "stream.o.write": "skipped", "stream.m.write": "skipped", "stream.nokey.write": "skipped"}},
			{"no database", []string{"SELECT ON " + quoteIdent(database+"_x") + ".*"},
				map[string]string{"database.create": "failed privilege_missing"}, nil},
		} {
			t.Run(c.name, func(t *testing.T) {
				config := map[string]string{"dsn": userDSN(t, admin, srv, database, "w", c.grants...)}
				if c.name == "no database" {
					// A database the DSN doesn't name, which the first write
					// creates.
					config = map[string]string{"dsn": userDSN(t, admin, srv, "", "w", c.grants...), "database": database + "_x"}
				}
				b, _ := json.Marshal(config)
				d, err := NewDestination(b)
				if err != nil {
					t.Fatal(err)
				}
				expectItems(t, connectortest.Items(t, d.Checks()), c.checks)
				if c.name == "no database" {
					return
				}
				flight := map[string]string{"stream.nokey.write": "failed config_invalid"}
				for k, v := range c.flight {
					flight[k] = v
				}
				expectItems(t, connectortest.Items(t, d.Preflight(destination.PreflightRequest{Pipeline: "p", Streams: streams})), flight)
			})
		}

		// An admin that may create the database passes for one that
		// doesn't exist yet.
		d, err := NewDestination(fmt.Appendf(nil, `{"dsn": %q, "database": %q}`, srv.dsn, database+"_new"))
		if err != nil {
			t.Fatal(err)
		}
		expectItems(t, connectortest.Items(t, d.Checks()), nil)
		expectItems(t, connectortest.Items(t, d.Preflight(destination.PreflightRequest{Namespace: database, Streams: streams[:3]})), nil)
	})
}

// expectTLSItems checks db.tls in each mode against a server whose
// certificate ca signs and names no host.
func expectTLSItems(t *testing.T, dsn, ca string) {
	t.Helper()
	for _, c := range []struct {
		tls  map[string]string
		want string
	}{
		{map[string]string{"mode": "preferred"}, "passed"},
		{map[string]string{"mode": "verify-ca", "ca": ca}, "passed"},
		{map[string]string{"ca": ca}, "failed tls_failed"},
		{map[string]string{"mode": "verify-ca", "ca": otherCA(t)}, "failed tls_failed"},
	} {
		b, _ := json.Marshal(map[string]any{"dsn": dsn, "tls": c.tls})
		s, err := NewSource(b)
		if err != nil {
			t.Fatal(err)
		}
		got := connectortest.Items(t, s.Checks())
		if got["db.tls"] != c.want || c.want == "passed" && got["db.connect"] != "passed" {
			t.Errorf("tls %s: db.tls %s, db.connect %s", c.tls["mode"], got["db.tls"], got["db.connect"])
		}
	}
}
