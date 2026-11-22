//go:build integration

package sqlserver

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// statuses runs checks, and gives each item's status and, unless it passed,
// its message, by name.
func statuses(t *testing.T, checks []check.Check) map[string]string {
	t.Helper()
	if err := check.Validate(checks); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, it := range check.Run(t.Context(), checks).Items {
		it = it.Rendered()
		out[it.Name] = it.Status.String()
		if it.Status != check.Passed {
			out[it.Name] += ": " + it.Message
			if f := it.Failure; f != nil && f.Fix != "" {
				out[it.Name] += " fix: " + f.Fix
			}
		}
	}
	return out
}

func wantStatus(t *testing.T, got map[string]string, name, prefix string) {
	t.Helper()
	if !strings.HasPrefix(got[name], prefix) {
		t.Errorf("%s = %q, want %s", name, got[name], prefix)
	}
}

func TestSourceChecks(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.a (id int PRIMARY KEY)")
	e.exec("CREATE TABLE $schema.b (id int PRIMARY KEY)")
	e.exec("CREATE VIEW $schema.vw AS SELECT id FROM $schema.a")
	got := statuses(t, e.source("").Checks())
	for _, name := range []string{"net.dns", "net.connect", "db.connect", "server.version", "schema.exists"} {
		wantStatus(t, got, name, "passed")
	}
	wantStatus(t, got, "schema.read", "passed")
	wantStatus(t, got, "database.change-tracking", "passed")

	e.exec("DENY SELECT ON $schema.b TO openetl")
	got = statuses(t, e.source("").Checks())
	wantStatus(t, got, "schema.read", "failed: openetl has no SELECT on b.")
	// Only the configured tables count.
	got = statuses(t, e.source(`, "tables": ["a"]`).Checks())
	wantStatus(t, got, "table.a.read", "passed")
	got = statuses(t, e.source(`, "tables": ["a", "b", "missing", "vw"]`).Checks())
	wantStatus(t, got, "table.b.read", "failed")
	wantStatus(t, got, "table.missing.read", "failed")
	wantStatus(t, got, "table.vw.read", "passed")
	e.exec("DENY SELECT ON $schema.vw TO openetl")
	got = statuses(t, e.source(`, "tables": ["vw"]`).Checks())
	wantStatus(t, got, "table.vw.read", "failed")

	got = statuses(t, e.source(`, "schema": "nope"`).Checks())
	wantStatus(t, got, "schema.exists", "failed: The schema nope doesn't exist.")
	wantStatus(t, got, "schema.read", "skipped")

	got = statuses(t, e.source(`, "cdcMethod": "change_data_capture"`).Checks())
	wantStatus(t, got, "database.change-data-capture", "passed")
	wantStatus(t, got, "cdc.read", "passed")
	// The login has no VIEW SERVER STATE to see SQL Server Agent.
	wantStatus(t, got, "cdc.agent", "skipped")

	bad, err := NewSource(fmt.Appendf(nil, `{"dsn": %q}`, badPassword(t)))
	if err != nil {
		t.Fatal(err)
	}
	got = statuses(t, bad.Checks())
	wantStatus(t, got, "db.connect", "failed: The upstream rejected the credentials.")
	wantStatus(t, got, "schema.exists", "skipped")
	// Without encrypt, only the sign-in is; with it, the server's
	// self-signed certificate is trusted only when the dsn says so.
	wantStatus(t, statuses(t, e.source("").Checks()), "db.tls", "warning: The connection may not be encrypted after sign-in")
	for suffix, want := range map[string]string{
		"&encrypt=true&trustServerCertificate=true":  "passed",
		"&encrypt=true&trustServerCertificate=false": "failed: The TLS handshake with localhost:1433 failed.",
	} {
		s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q}`, dsn()+suffix))
		if err != nil {
			t.Fatal(err)
		}
		got := statuses(t, s.Checks())
		if want == "passed" {
			wantStatus(t, got, "db.tls", want)
		} else {
			wantStatus(t, got, "db.connect", want)
		}
	}
	closed, err := NewSource(fmt.Appendf(nil, `{"dsn": %q}`, closedPort))
	if err != nil {
		t.Fatal(err)
	}
	got = statuses(t, closed.Checks())
	wantStatus(t, got, "net.connect", "failed")
	wantStatus(t, got, "db.connect", "skipped")
}

func TestSourcePreflight(t *testing.T) {
	e := newSourceEnv(t)
	e.exec("CREATE TABLE $schema.tracked (id int PRIMARY KEY, at datetime2, v varbinary(10), m nvarchar(max))")
	e.exec("ALTER TABLE $schema.tracked ENABLE CHANGE_TRACKING")
	e.exec("CREATE TABLE $schema.untracked (id int PRIMARY KEY)")
	e.exec("CREATE TABLE $schema.loose (id int)")
	e.exec("CREATE TABLE $schema.captured (id int PRIMARY KEY)")
	e.capture("captured")
	// Change data capture keeps only the characters of the database's code
	// page in a varchar of another.
	e.exec("CREATE TABLE $schema.utf8 (id int PRIMARY KEY, v varchar(10) COLLATE Latin1_General_100_CI_AS_SC_UTF8, n nvarchar(10) COLLATE Cyrillic_General_CI_AS)")
	e.capture("utf8")
	e.exec("CREATE VIEW $schema.vw AS SELECT id, at FROM $schema.tracked")
	e.exec("CREATE VIEW $schema.denied AS SELECT id FROM $schema.untracked")
	e.exec("DENY SELECT ON $schema.denied TO openetl")
	req := source.PreflightRequest{Interval: time.Hour, Streams: []source.ConfiguredStream{
		{Name: "vw", SyncMode: source.Incremental, Cursor: "at"},
		{Name: "denied", SyncMode: source.FullRefresh},
		{Name: "tracked", SyncMode: source.CDC},
		{Name: "untracked", SyncMode: source.CDC},
		{Name: "loose", SyncMode: source.CDC},
		{Name: "tracked", SyncMode: source.Incremental, Cursor: "at"},
		{Name: "tracked", SyncMode: source.Incremental, Cursor: "v"},
		{Name: "tracked", SyncMode: source.Incremental, Cursor: "m"},
		{Name: "gone", SyncMode: source.FullRefresh},
	}}
	got := statuses(t, e.source("").Preflight(req))
	wantStatus(t, got, "stream.tracked.read", "passed")
	wantStatus(t, got, "stream.tracked.change-tracking", "passed")
	wantStatus(t, got, "stream.untracked.change-tracking", "failed: Change tracking is off for")
	wantStatus(t, got, "stream.loose.change-tracking", "failed: ["+e.schema+"].[loose] has no primary key")
	wantStatus(t, got, "stream.tracked.cursor", "passed")
	wantStatus(t, got, "stream.tracked.cursor-2", "failed: ["+e.schema+"].[tracked] has no column v")
	wantStatus(t, got, "stream.tracked.cursor-3", "failed: ["+e.schema+"].[tracked] has no column m")
	wantStatus(t, got, "stream.gone.read", "failed")
	wantStatus(t, got, "stream.vw.read", "passed")
	wantStatus(t, got, "stream.vw.cursor", "passed")
	wantStatus(t, got, "stream.denied.read", "failed")
	wantStatus(t, got, "cdc.retention", "passed")
	req.Interval = 72 * time.Hour
	got = statuses(t, e.source("").Preflight(req))
	wantStatus(t, got, "cdc.retention", "warning: The database keeps changes for 48h0m0s")

	req = source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "captured", SyncMode: source.CDC}, {Name: "tracked", SyncMode: source.CDC}, {Name: "utf8", SyncMode: source.CDC}}}
	got = statuses(t, e.source(`, "cdcMethod": "change_data_capture"`).Preflight(req))
	wantStatus(t, got, "stream.captured.change-data-capture", "passed")
	wantStatus(t, got, "stream.utf8.change-data-capture", "warning: v of ["+e.schema+"].[utf8] is a char, varchar or text column")
	wantStatus(t, got, "stream.tracked.change-data-capture", "failed: ["+e.schema+"].[tracked] has no capture instance")
}

func TestDestinationChecks(t *testing.T) {
	e := newEnv(t)
	got := statuses(t, e.d.Checks())
	for _, name := range []string{"net.dns", "net.connect", "db.connect", "server.version", "database.compatibility", "database.create-table", "schema.write"} {
		wantStatus(t, got, name, "passed")
	}
	db := admin(t)
	if _, err := db.ExecContext(t.Context(), "CREATE SCHEMA "+quote(e.schema)+" AUTHORIZATION dbo"); err != nil {
		t.Fatal(err)
	}
	got = statuses(t, e.d.Checks())
	wantStatus(t, got, "schema.write", "failed: openetl has no ALTER, SELECT, INSERT, UPDATE, DELETE on SCHEMA::"+e.schema)
}

func TestDestinationPreflight(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	w := e.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Append})
	if _, err := w.(destination.SchemaApplier).ApplySchema(ctx, []destination.StreamSchema{{Name: "users", Schema: arrow.NewSchema([]arrow.Field{idField, {Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)}}); err != nil {
		t.Fatal(err)
	}
	wider := arrow.NewSchema([]arrow.Field{idField, {Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	req := destination.PreflightRequest{Streams: []destination.Stream{{Name: "users", Mode: destination.Append}, {Name: "fresh", Mode: destination.Append}},
		Schemas: map[string]*arrow.Schema{"users": wider, "fresh": wider}}
	got := statuses(t, e.d.Preflight(req))
	wantStatus(t, got, "namespace.write", "passed")
	wantStatus(t, got, "stream.fresh.write", "passed")
	wantStatus(t, got, "stream.users.write", "warning: n of users is int, the stream's is bigint")
	if !strings.Contains(got["stream.users.write"], "ALTER TABLE ["+e.schema+"].[users] ALTER COLUMN [n] bigint NULL;") {
		t.Errorf("no fix: %s", got["stream.users.write"])
	}
	req.Streams[0].AllowTypeChanges = true
	got = statuses(t, e.d.Preflight(req))
	wantStatus(t, got, "stream.users.write", "passed")
}
