//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl-connectors/sqlstage"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
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

// A schema name PostgreSQL reserves fails the preflight, and the writer's
// first statement as invalid config, which isn't retried. A keyword, or a
// name with capitals, spaces and quotes, takes writes like any other.
func TestDestinationSchemaNames(t *testing.T) {
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	streams := []destination.Stream{{Name: "full", Mode: destination.Append}}
	open := func(t *testing.T, schema string) (*Destination, destination.Writer) {
		t.Helper()
		cfg, _ := json.Marshal(map[string]string{"dsn": dsn(), "schema": schema})
		d, err := NewDestination(cfg)
		if err != nil {
			t.Fatal(err)
		}
		w, err := d.Open(ctx, destination.OpenConfig{Pipeline: "p1", Generation: 1, Scope: destination.ScopePipeline, Streams: streams})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Close(context.Background()) })
		return d, w
	}

	reserved := "pg_" + strings.ToLower(rand.Text())[:10]
	d, w := open(t, reserved)
	connectortest.ExpectItems(t, "a reserved schema", check.Run(ctx, d.Preflight(destination.PreflightRequest{Streams: streams})), map[string]string{
		"schema.create": "failed schema_name_reserved", "stream.full.write": "skipped",
	})
	if _, err := w.Recover(ctx, 1, destination.Epoch{}, []destination.Staged{nil}); codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID {
		t.Errorf("recover into %s: %v (%v)", reserved, err, codeOf(err))
	}

	names := []string{`Full "Name" ` + strings.ToLower(rand.Text())[:10]}
	var taken bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'full')").Scan(&taken); err != nil {
		t.Fatal(err)
	}
	if !taken {
		names = append(names, "full")
	}
	for _, schema := range names {
		t.Cleanup(func() {
			_, _ = conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		})
		d, w := open(t, schema)
		connectortest.ExpectStatuses(t, connectortest.Statuses(check.Run(ctx, d.Preflight(destination.PreflightRequest{Streams: streams}))), nil)
		recov(t, w, 1, destination.Epoch{}, nil)
		e := destination.Epoch{Generation: 1, N: 1}
		if err := w.Write(ctx, e, "full", batch(t, []arrow.Field{idField, nameField}, `[{"id": 1, "name": "a"}]`)); err != nil {
			t.Fatalf("%s: %v", schema, err)
		}
		if _, err := w.Commit(ctx, e, []destination.Staged{precommit(t, w, e)}); err != nil {
			t.Fatalf("%s: %v", schema, err)
		}
		var n int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{schema, "full"}.Sanitize()).Scan(&n); err != nil || n != 1 {
			t.Errorf("%s: %d rows, %v", schema, n, err)
		}
	}
}

// An append deduped against a table the user doesn't own fails the preflight
// without an index led by _openetl_id, which only the owner can create, and
// writes with one, whatever its name.
func TestDestinationPreflightDedupeIndex(t *testing.T) {
	e := newSourceEnv(t)
	role := "dedupe_" + strings.ToLower(rand.Text())[:10]
	e.exec(fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'pw'", role))
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = e.conn.Exec(ctx, "DROP OWNED BY "+role)
		_, _ = e.conn.Exec(ctx, "DROP ROLE "+role)
	})
	e.exec("GRANT USAGE, CREATE ON SCHEMA $schema TO " + role)

	e.exec("CREATE TABLE $schema.unowned_no_idx (id bigint, _openetl_id uuid)")
	e.exec("GRANT SELECT, INSERT ON $schema.unowned_no_idx TO " + role)

	e.exec("CREATE TABLE $schema.unowned_nonunique_idx (id bigint, _openetl_id uuid)")
	e.exec("CREATE INDEX unowned_nonunique_idx_id ON $schema.unowned_nonunique_idx (_openetl_id)")
	e.exec("GRANT SELECT, INSERT ON $schema.unowned_nonunique_idx TO " + role)

	e.exec("CREATE TABLE $schema.owned_no_idx (id bigint, _openetl_id uuid)")
	e.exec("ALTER TABLE $schema.owned_no_idx OWNER TO " + role)

	e.exec("CREATE TABLE $schema.unowned_overwrite (id bigint, _openetl_id uuid)")
	e.exec("GRANT INSERT, DELETE ON $schema.unowned_overwrite TO " + role)

	e.exec("CREATE TABLE $schema.unowned_narrow (id bigint, _openetl_id uuid)")
	e.exec("GRANT INSERT, DELETE ON $schema.unowned_narrow TO " + role)

	e.exec("CREATE TABLE $schema.owned_overwrite (id bigint, _openetl_id uuid)")
	e.exec("ALTER TABLE $schema.owned_overwrite OWNER TO " + role)
	e.exec("REVOKE TRUNCATE ON $schema.owned_overwrite FROM " + role)

	e.exec("CREATE TABLE $schema.unowned_nonleading_idx (id bigint, _openetl_id uuid)")
	e.exec("CREATE INDEX unowned_nonleading_idx_id ON $schema.unowned_nonleading_idx (id, _openetl_id)")
	e.exec("GRANT SELECT, INSERT ON $schema.unowned_nonleading_idx TO " + role)

	u, err := url.Parse(dsn())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, "pw")
	d, err := NewDestination(fmt.Appendf(nil, `{"dsn": %q, "schema": %q}`, u.String(), e.schema))
	if err != nil {
		t.Fatal(err)
	}

	dpf := check.Run(t.Context(), d.Preflight(destination.PreflightRequest{
		Streams: []destination.Stream{
			{Name: "unowned_no_idx", Mode: destination.Append, Dedupe: destination.DedupeTarget},
			{Name: "unowned_nonunique_idx", Mode: destination.Append, Dedupe: destination.DedupeTarget},
			{Name: "owned_no_idx", Mode: destination.Append, Dedupe: destination.DedupeTarget},
			{Name: "unowned_overwrite", Mode: destination.Overwrite, Dedupe: destination.DedupeTarget},
			{Name: "unowned_nonleading_idx", Mode: destination.Append, Dedupe: destination.DedupeTarget},
		},
	}))
	connectortest.ExpectItems(t, "dedupe target preflight", dpf, map[string]string{
		"schema.create":                       "passed",
		"stream.unowned-no-idx.write":         "failed table_not_owned",
		"stream.unowned-nonunique-idx.write":  "warning table_not_owned",
		"stream.owned-no-idx.write":           "passed",
		"stream.unowned-overwrite.write":      "warning table_not_owned",
		"stream.unowned-nonleading-idx.write": "failed table_not_owned",
	})

	// An overwrite of a table the user doesn't own copies its rows in; with
	// overwriteTruncate, one of a table it owns but may not truncate
	// deletes them if it copies, and the item says so; a table it doesn't
	// own fails or warns as it does without.
	ow := slices.IndexFunc(dpf.Items, func(it check.Item) bool { return it.Name == "stream.unowned-overwrite.write" })
	if ow < 0 || !strings.Contains(dpf.Items[ow].Rendered().Message, "copies the read's rows into it") {
		t.Errorf("the unowned overwrite's item doesn't say it copies: %+v", dpf.Items)
	}
	truncating, err := NewDestination(fmt.Appendf(nil, `{"dsn": %q, "schema": %q, "overwriteTruncate": true}`, u.String(), e.schema))
	if err != nil {
		t.Fatal(err)
	}
	connectortest.ExpectItems(t, "truncating overwrite preflight", check.Run(t.Context(), truncating.Preflight(destination.PreflightRequest{
		Streams: []destination.Stream{
			{Name: "owned_overwrite", Mode: destination.Overwrite},
			{Name: "owned_no_idx", Mode: destination.Overwrite},
			{Name: "unowned_overwrite", Mode: destination.Overwrite},
			{Name: "unowned_narrow", Mode: destination.Overwrite},
		},
		Schemas: map[string]*arrow.Schema{"unowned_narrow": arrow.NewSchema([]arrow.Field{idField, rowID, nameField}, nil)},
	})), map[string]string{
		"schema.create":                  "passed",
		"stream.owned-overwrite.write":   "warning privilege_missing",
		"stream.owned-no-idx.write":      "passed",
		"stream.unowned-overwrite.write": "warning table_not_owned",
		"stream.unowned-narrow.write":    "failed table_not_owned",
	})

	i := slices.IndexFunc(dpf.Items, func(it check.Item) bool { return it.Name == "stream.unowned-no-idx.write" })
	if i < 0 {
		t.Fatal("missing stream.unowned-no-idx.write item")
	}
	r := dpf.Items[i].Rendered()
	if r.Failure == nil {
		t.Fatal("expected Failure on stream.unowned-no-idx.write")
	}
	if !strings.Contains(r.Message, "_openetl_id") {
		t.Errorf("message %q does not mention _openetl_id", r.Message)
	}
	if !strings.Contains(r.Failure.Fix, "CREATE INDEX") || !strings.Contains(r.Failure.Fix, "_openetl_id") {
		t.Errorf("fix %q does not include CREATE INDEX on _openetl_id", r.Failure.Fix)
	}
	if strings.ContainsAny(r.Message+r.Failure.Fix, "{}") {
		t.Errorf("unfilled template placeholder in message=%q fix=%q", r.Message, r.Failure.Fix)
	}

	// Executing the suggested CREATE INDEX fix as the owner satisfies Preflight.
	_, ddl, ok := strings.Cut(r.Failure.Fix, " or ")
	if !ok {
		t.Fatalf("unexpected fix format: %q", r.Failure.Fix)
	}
	if _, err := e.conn.Exec(t.Context(), ddl); err != nil {
		t.Fatalf("owner executing suggested fix %q: %v", ddl, err)
	}
	dpfAfter := check.Run(t.Context(), d.Preflight(destination.PreflightRequest{
		Streams: []destination.Stream{{Name: "unowned_no_idx", Mode: destination.Append, Dedupe: destination.DedupeTarget}},
	}))
	connectortest.ExpectItems(t, "after CREATE INDEX fix", dpfAfter, map[string]string{
		"schema.create":               "passed",
		"stream.unowned-no-idx.write": "warning table_not_owned",
	})

	// Non-owner writer can also apply target-deduped append to an unowned table
	// that already has a valid non-unique _openetl_id index (even under another name).
	ctx := t.Context()
	w, err := d.Open(ctx, destination.OpenConfig{
		Pipeline: "p_dedupe", Generation: 1, Scope: destination.ScopeStream,
		Streams: []destination.Stream{{Name: "unowned_nonunique_idx", Mode: destination.Append, Dedupe: destination.DedupeTarget}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	recov(t, w, 1, destination.Epoch{}, nil)
	e1 := destination.Epoch{Generation: 1, N: 1}
	rows := `[{"id": 1, "_openetl_id": ` + uid(1) + `}, {"id": 1, "_openetl_id": ` + uid(1) + `}, {"id": 2, "_openetl_id": ` + uid(2) + `}]`
	if err := w.Write(ctx, e1, "unowned_nonunique_idx", batch(t, []arrow.Field{idField, rowID}, rows)); err != nil {
		t.Fatal(err)
	}
	c, err := w.Commit(ctx, e1, []destination.Staged{precommit(t, w, e1)})
	if err != nil {
		t.Fatalf("commit on unowned indexed table: %v", err)
	}
	if c.Collapsed["unowned_nonunique_idx"] != 1 {
		t.Errorf("collapsed = %v, want 1", c.Collapsed)
	}
	var count int
	if err := e.conn.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{e.schema, "unowned_nonunique_idx"}.Sanitize()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("unowned_nonunique_idx has %d rows, want 2", count)
	}
}
