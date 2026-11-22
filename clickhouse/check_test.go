//go:build integration

package clickhouse

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// testUser creates a user with settings and grants, dropped when t ends,
// and gives its name; $db in a grant is database.
func testUser(t *testing.T, database, settings string, grants ...string) string {
	t.Helper()
	conn := testConn(t)
	user := fmt.Sprintf("u_%d", time.Now().UnixNano())
	q := "CREATE USER " + ident(user) + " IDENTIFIED BY 'pw'"
	if settings != "" {
		q += " SETTINGS " + settings
	}
	if err := conn.Exec(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP USER IF EXISTS "+ident(user)) })
	for _, g := range grants {
		if err := conn.Exec(t.Context(), "GRANT "+strings.ReplaceAll(g, "$db", ident(database))+" TO "+ident(user)); err != nil {
			t.Fatal(err)
		}
	}
	return user
}

func userConfig(user, database string) []byte {
	return fmt.Appendf(nil, `{"addr": %q, "user": %q, "password": "pw", "database": %q}`, testAddr(), user, database)
}

func TestSourceCheckItems(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.items (id Int64, at DateTime, j JSON) ENGINE = MergeTree ORDER BY id")
	e.exec("CREATE TABLE $db.nokey (v Int64) ENGINE = MergeTree ORDER BY tuple()")
	e.exec("CREATE VIEW $db.v AS SELECT id FROM $db.items")
	s, err := NewSource(testConfig(testAddr(), "openetl", db))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectStatuses(t, connectortest.Items(t, s.Checks()), nil)
	if err := s.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectStatuses(t, connectortest.Items(t, s.Preflight(source.PreflightRequest{Streams: []source.ConfiguredStream{
		{Name: "items", SyncMode: source.FullRefresh},
		{Name: "items", SyncMode: source.Incremental, Cursor: "at"},
		{Name: "items", SyncMode: source.Incremental, Cursor: "j"},
		{Name: "items", SyncMode: source.Incremental, Cursor: "nope"},
		{Name: "items", SyncMode: source.CDC},
		{Name: "nokey", SyncMode: source.FullRefresh},
		{Name: "v", SyncMode: source.FullRefresh},
		{Name: "missing", SyncMode: source.FullRefresh},
	}})), map[string]string{
		"stream.items.cursor-2": "failed cursor_unsupported",
		"stream.items.cursor-3": "failed not_found",
		"stream.items.mode":     "failed sync_mode_unsupported",
		"stream.missing.read":   "failed not_found",
		"stream.missing.order":  "skipped",
	})

	// A user that can read one table of the three and see another, which it
	// can't read; the third it doesn't see. And a database that isn't there.
	user := testUser(t, db, "", "SELECT ON $db.nokey", "INSERT ON $db.items")
	s, err = NewSource(userConfig(user, db))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectStatuses(t, connectortest.Items(t, s.Checks()), map[string]string{"tables.read": "failed privilege_missing"})
	connectortest.ExpectStatuses(t, connectortest.Items(t, s.Preflight(source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "nokey", SyncMode: source.FullRefresh}, {Name: "items", SyncMode: source.FullRefresh}}})),
		map[string]string{"stream.items.read": "failed privilege_missing", "stream.items.order": "skipped"})
	connectortest.ExpectStatuses(t, connectortest.Items(t, s.Preflight(source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "v", SyncMode: source.FullRefresh}}})),
		map[string]string{"stream.v.read": "failed not_found", "stream.v.order": "skipped"})
	// Two tables it sees and can't read: the fix grants SELECT on each, in a
	// GRANT the server takes.
	user = testUser(t, db, "", "INSERT ON $db.items", "INSERT ON $db.nokey")
	s, err = NewSource(userConfig(user, db))
	if err != nil {
		t.Fatal(err)
	}
	var fix string
	for _, it := range check.Run(t.Context(), s.Checks()).Items {
		if it.Name == "tables.read" && it.Failure != nil {
			fix = it.Rendered().Failure.Fix
		}
	}
	grant, ok := strings.CutPrefix(strings.TrimSuffix(fix, ";"), "Run ")
	if !ok || strings.Count(grant, "SELECT ON") != 2 {
		t.Fatalf("tables.read's fix: %q", fix)
	}
	e.exec(grant)
	connectortest.ExpectStatuses(t, connectortest.Items(t, s.Checks()), nil)
	// A readonly profile's user reads, as the source sets no setting.
	e.exec("INSERT INTO $db.items (id, at) VALUES (1, now()), (2, now())")
	s, err = NewSource(userConfig(testUser(t, db, "readonly = 1", "SELECT ON $db.*"), db))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectStatuses(t, connectortest.Items(t, s.Checks()), nil)
	c := newCollector()
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "items", SyncMode: source.FullRefresh}}, Lanes: 1}, c); err != nil {
		t.Fatal(err)
	}
	if got := c.ids("items"); !slices.Equal(got, []int64{1, 2}) {
		t.Errorf("a readonly user read %v", got)
	}
	s, err = NewSource(testConfig(testAddr(), "openetl", db+"_none"))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectStatuses(t, connectortest.Items(t, s.Checks()), map[string]string{"database.found": "failed not_found", "tables.read": "skipped"})
}

// A readonly profile's user checks, discovers and reads under a deadline,
// as the platform gives a check one, too: the driver would send it as
// max_execution_time, which such a user can't set.
func TestSourceReadonlyDeadline(t *testing.T) {
	db := newDatabase(t)
	e := sourceEnv{t: t, db: db, conn: testConn(t)}
	e.exec("CREATE TABLE $db.items (id Int64) ENGINE = MergeTree ORDER BY id")
	e.exec("INSERT INTO $db.items VALUES (1), (2)")
	s, err := NewSource(userConfig(testUser(t, db, "readonly = 1", "SELECT ON $db.*"), db))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err := check.Run(ctx, s.Checks()).Err(); err != nil {
		t.Errorf("checks: %v", err)
	}
	if _, err := s.Discover(ctx); err != nil {
		t.Errorf("discover: %v", err)
	}
	cs := source.ConfiguredStream{Name: "items", SyncMode: source.FullRefresh}
	if _, err := s.Splits(ctx, cs, nil); err != nil {
		t.Errorf("splits: %v", err)
	}
	c := newCollector()
	if err := s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{cs}, Lanes: 1}, c); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := c.ids("items"); !slices.Equal(got, []int64{1, 2}) {
		t.Errorf("read %v", got)
	}
}

func TestDestinationCheckItems(t *testing.T) {
	db := newDatabase(t)
	d, err := NewDestination(testConfig(testAddr(), "openetl", db))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectStatuses(t, connectortest.Items(t, d.Checks()), nil)
	if err := d.Check(t.Context()); err != nil {
		t.Fatal(err)
	}

	// A user that can only read: every grant writes need is an item.
	ro := testUser(t, db, "", "SELECT ON $db.*")
	d, err = NewDestination(userConfig(ro, db))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectStatuses(t, connectortest.Items(t, d.Checks()), map[string]string{
		"grant.tables": "failed privilege_missing", "grant.rows": "failed privilege_missing", "grant.columns": "failed privilege_missing",
		"grant.partitions": "failed privilege_missing", "grant.update": "failed privilege_missing", "grant.system": "failed privilege_missing",
	})
	err = d.Check(t.Context())
	if err == nil || !strings.Contains(err.Error(), ident(ro)+" has no CREATE TABLE, DROP TABLE on "+ident(db)+".*") {
		t.Errorf("a read-only user's check: %v", err)
	}

	// A readonly profile's user, with every grant, can write nothing, nor set
	// what writes set.
	locked := testUser(t, db, "readonly = 1", "ALL ON $db.*", "SELECT ON system.parts", "SELECT ON system.mutations")
	d, err = NewDestination(userConfig(locked, db))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectStatuses(t, connectortest.Items(t, d.Checks()), map[string]string{
		"grant.tables": "failed user_readonly", "grant.rows": "failed user_readonly", "grant.columns": "failed user_readonly",
		"grant.partitions": "failed user_readonly", "grant.update": "failed user_readonly", "settings.write": "failed setting_locked",
	})
	// One without CREATE DATABASE can't create a missing database.
	d, err = NewDestination(userConfig(ro, db+"_none"))
	if err != nil {
		t.Fatal(err)
	}
	if got := connectortest.Items(t, d.Checks()); got["database.create"] != "failed privilege_missing" {
		t.Errorf("database.create of a missing database without CREATE DATABASE: %s", got["database.create"])
	}

	// A profile that pins a setting the destination sets.
	for _, pin := range []string{"mutations_sync = 0 CONST", "max_partitions_per_insert_block = 100 CONST"} {
		d, err = NewDestination(userConfig(testUser(t, db, pin, readmeGrants...), db))
		if err != nil {
			t.Fatal(err)
		}
		connectortest.ExpectStatuses(t, connectortest.Items(t, d.Checks()), map[string]string{"settings.write": "failed setting_locked"})
	}

	// Preflight: a merge without a key, an existing table whose key can't
	// take the stream's type, a seq that can't version, and a hard delete
	// that needs one.
	conn := testConn(t)
	if err := conn.Exec(t.Context(), "CREATE TABLE "+qualified(db, "keyed")+" (id Int32, v Int64) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	d, err = NewDestination(testConfig(testAddr(), "openetl", db))
	if err != nil {
		t.Fatal(err)
	}
	withSeq := func(seq arrow.DataType) *arrow.Schema {
		return arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: opColumn, Type: arrow.BinaryTypes.String}, {Name: seqColumn, Type: seq, Nullable: true}}, nil)
	}
	keyed := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	connectortest.ExpectStatuses(t, connectortest.Items(t, d.Preflight(destination.PreflightRequest{
		Namespace: db,
		Streams: []destination.Stream{
			{Name: "nokey", Mode: destination.Merge},
			{Name: "keyed", Mode: destination.Append},
			{Name: "fresh", Mode: destination.Merge, PrimaryKey: []string{"id"}},
			{Name: "strseq", Mode: destination.Merge, PrimaryKey: []string{"id"}},
			{Name: "harddel", Mode: destination.Merge, PrimaryKey: []string{"id"}, Deletes: destination.HardDelete},
		},
		Schemas: map[string]*arrow.Schema{
			"keyed": keyed, "fresh": withSeq(arrow.PrimitiveTypes.Int64),
			"strseq": withSeq(arrow.BinaryTypes.String), "harddel": withSeq(arrow.BinaryTypes.String),
		},
	})), map[string]string{
		"stream.nokey.write":   "failed config_invalid",
		"stream.keyed.write":   "warning key_type_unchangeable",
		"stream.strseq.write":  "warning seq_unversioned",
		"stream.harddel.write": "failed seq_unversioned",
	})

	// A namespace with a dot can't take patches.
	dotted := db + ".x"
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+ident(dotted)) })
	connectortest.ExpectStatuses(t, connectortest.Items(t, d.Preflight(destination.PreflightRequest{
		Namespace: dotted, Streams: []destination.Stream{{Name: "a", Mode: destination.Append}},
		Schemas: map[string]*arrow.Schema{"a": keyed},
	})), map[string]string{"stream.a.write": "warning database_name_dotted"})
}

func TestCheckErrors(t *testing.T) {
	db := newDatabase(t)
	d, err := NewDestination(testConfig(testAddr(), "wrong", db))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectStatuses(t, connectortest.Items(t, d.Checks()), map[string]string{
		"db.connect": "failed credentials_rejected", "server.version": "skipped", "database.create": "skipped",
		"grant.tables": "skipped", "grant.rows": "skipped", "grant.columns": "skipped", "grant.partitions": "skipped",
		"grant.update": "skipped", "grant.system": "skipped", "settings.write": "skipped",
	})
	s, err := NewSource(testConfig("127.0.0.1:1", "openetl", db))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectStatuses(t, connectortest.Items(t, s.Checks()), map[string]string{
		"net.connect": "failed connection_refused", "db.connect": "skipped", "server.version": "skipped", "database.found": "skipped", "tables.read": "skipped",
	})
}

// readmeGrants are the grants the connector's page lists for a destination.
var readmeGrants = []string{
	"SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER ADD COLUMN, ALTER MODIFY COLUMN, ALTER MOVE PARTITION, ALTER DELETE, ALTER UPDATE ON $db.*",
	"SELECT ON system.parts, SELECT ON system.mutations",
}

// A user with only the grants the page lists passes every check item, and
// merges, hard-deletes, dedupes, overwrites and patches.
func TestDestinationGranted(t *testing.T) {
	for name, f := range map[string]func(*testing.T, kit){
		"merge": testDestinationMerge, "dedupe": testDestinationDedupe, "overwrite": testDestinationOverwrite, "patch-merge": testDestinationPatchMerge,
	} {
		t.Run(name, func(t *testing.T) {
			db := newDatabase(t)
			d, err := NewDestination(userConfig(testUser(t, db, "", readmeGrants...), db))
			if err != nil {
				t.Fatal(err)
			}
			connectortest.ExpectStatuses(t, connectortest.Items(t, d.Checks()), nil)
			f(t, kit{t: t, db: db, d: d})
		})
	}
}
