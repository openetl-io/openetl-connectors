//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl-connectors/sqlstage"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A user with little access gets an item for each thing it lacks, each
// with its reason, and the source's and destination's lists say what a
// pipeline needs of each stream.
func TestChecksItemByItem(t *testing.T) {
	e := newSourceEnv(t)
	role := "check_" + strings.ToLower(rand.Text())[:10]
	e.exec("CREATE TABLE $schema.a (id int PRIMARY KEY, at timestamptz, v jsonb)")
	e.exec("CREATE TABLE $schema.b (id int PRIMARY KEY)")
	e.exec("CREATE TABLE $schema.nokey (id int)")
	e.exec(fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'pw'", role))
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = e.conn.Exec(ctx, "DROP OWNED BY "+role)
		_, _ = e.conn.Exec(ctx, "DROP ROLE "+role)
	})
	e.exec("GRANT USAGE ON SCHEMA $schema TO " + role)
	e.exec("GRANT SELECT ON $schema.a, $schema.nokey TO " + role)
	u, err := url.Parse(dsn())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, "pw")
	limited := u.String()

	s, err := NewSource(fmt.Appendf(nil, `{"dsn": %q, "schema": %q, "publication": %q}`, limited, e.schema, e.schema))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectItems(t, "the source's check", check.Run(t.Context(), s.Checks()), map[string]string{
		"net.dns": "passed", "net.connect": "passed", "db.tls": "warning plaintext_connection", "db.connect": "passed",
		"schema.usage": "passed", "tables.read": "failed privilege_missing",
		"cdc.wal-level": "passed", "cdc.replication": "warning privilege_missing",
	})
	pf := check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{
		{Name: "a", SyncMode: source.CDC}, {Name: "a", SyncMode: source.Incremental, Cursor: "v"},
		{Name: "b"}, {Name: "gone"}, {Name: "nokey", SyncMode: source.CDC},
	}}))
	connectortest.ExpectItems(t, "the source's preflight", pf, map[string]string{
		"db.connect": "passed", "stream.a.read": "passed", "stream.a.cdc": "passed",
		"stream.a.read-2": "passed", "stream.a.cursor": "failed cursor_unsupported",
		"stream.b.read": "failed privilege_missing", "stream.gone.read": "failed not_found",
		"stream.nokey.read": "passed", "stream.nokey.cdc": "failed primary_key_missing",
		"cdc.wal-level": "passed", "cdc.replication": "failed privilege_missing", "cdc.wal-keep": "warning setting_mismatch",
		"cdc.publication": "failed publication_missing", "cdc.slot": "passed",
	})

	// The owner, with every right: the publication and slot are made on the
	// first read.
	pf = check.Run(t.Context(), e.source(fmt.Sprintf(`, "publication": %q`, e.schema)).Preflight(source.PreflightRequest{Pipeline: "p",
		Streams: []source.ConfiguredStream{{Name: "a", SyncMode: source.CDC}}}))
	connectortest.ExpectItems(t, "the owner's preflight", pf, map[string]string{"cdc.replication": "passed", "cdc.publication": "passed", "cdc.slot": "passed"})

	bad := *u
	bad.User = url.UserPassword(role, "not-pw")
	s, err = NewSource(fmt.Appendf(nil, `{"dsn": %q, "schema": %q}`, bad.String(), e.schema))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectItems(t, "bad credentials", check.Run(t.Context(), s.Checks()), map[string]string{
		"db.connect": "failed credentials_rejected", "schema.usage": "skipped", "cdc.replication": "skipped",
	})

	d, err := NewDestination(fmt.Appendf(nil, `{"dsn": %q, "schema": %q}`, limited, e.schema))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectItems(t, "the destination's check", check.Run(t.Context(), d.Checks()), map[string]string{
		"db.connect": "passed", "schema.create": "failed privilege_missing",
	})
	e.exec("GRANT CREATE ON SCHEMA $schema TO " + role)
	// CREATE without USAGE: the writer couldn't reach the tables it creates.
	e.exec("REVOKE USAGE ON SCHEMA $schema FROM " + role)
	r := check.Run(t.Context(), d.Checks())
	connectortest.ExpectItems(t, "CREATE without USAGE", r, map[string]string{"schema.create": "failed privilege_missing"})
	if it := r.Items[len(r.Items)-1].Rendered(); !strings.Contains(it.Message, "USAGE") {
		t.Errorf("CREATE without USAGE: %s", it.Message)
	}
	e.exec("GRANT USAGE ON SCHEMA $schema TO " + role)
	e.exec("GRANT INSERT ON $schema.b TO " + role)
	// wider writes to b, with a column b lacks, which only b's owner can add.
	md := arrow.NewMetadata([]string{sqlstage.TableKey}, []string{"b"})
	wider := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}, {Name: "extra", Type: arrow.BinaryTypes.String}}, &md)
	dpf := check.Run(t.Context(), d.Preflight(destination.PreflightRequest{Streams: []destination.Stream{
		{Name: "b", Mode: destination.Append}, {Name: "b", Mode: destination.Merge, PrimaryKey: []string{"id"}}, {Name: "new"}, {Name: "wider"},
		{Name: "b", Mode: destination.Append, Dedupe: destination.DedupeTarget},
	}, Schemas: map[string]*arrow.Schema{"wider": wider}}))
	connectortest.ExpectItems(t, "the destination's preflight", dpf, map[string]string{
		"schema.create": "passed", "stream.b.write": "warning table_not_owned",
		"stream.b.write-2": "failed privilege_missing", "stream.new.write": "passed", "stream.wider.write": "failed table_not_owned",
		"stream.b.write-3": "failed privilege_missing",
	})
	// A merge and a deduped append read the target's rows.
	if it := dpf.Items[3].Rendered(); !strings.Contains(it.Message, "SELECT, UPDATE, DELETE") {
		t.Errorf("the merge's failure: %s", it.Message)
	}
	if it := dpf.Items[6].Rendered(); !strings.Contains(it.Message, "SELECT") {
		t.Errorf("the deduped append's failure: %s", it.Message)
	}

	// A schema that doesn't exist yet: it's created, with CREATE on the
	// database.
	missing := "m_" + strings.ToLower(rand.Text())[:10]
	dpf = check.Run(t.Context(), d.Preflight(destination.PreflightRequest{Namespace: missing}))
	connectortest.ExpectItems(t, "a schema to create", dpf, map[string]string{"schema.create": "failed privilege_missing"})
	var db string
	if err := e.conn.QueryRow(t.Context(), "SELECT current_database()").Scan(&db); err != nil {
		t.Fatal(err)
	}
	e.exec("GRANT CREATE ON DATABASE " + pgx.Identifier{db}.Sanitize() + " TO " + role)
	t.Cleanup(func() {
		_, _ = e.conn.Exec(context.Background(), "REVOKE CREATE ON DATABASE "+pgx.Identifier{db}.Sanitize()+" FROM "+role)
	})
	dpf = check.Run(t.Context(), d.Preflight(destination.PreflightRequest{Namespace: missing}))
	connectortest.ExpectItems(t, "a schema to create", dpf, map[string]string{"schema.create": "passed"})
}
