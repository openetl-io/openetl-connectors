//go:build integration

package postgres

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// relid is a table's OID, which a swap changes and a copy keeps; 0 if it
// doesn't exist.
func (e *env) relid(table string) uint32 {
	e.t.Helper()
	var oid *uint32
	if err := e.conn.QueryRow(e.t.Context(), "SELECT to_regclass($1)::oid", pgx.Identifier{e.schema, table}.Sanitize()).Scan(&oid); err != nil {
		e.t.Fatal(err)
	}
	if oid == nil {
		return 0
	}
	return *oid
}

// filenode is a table's relfilenode, which TRUNCATE changes and DELETE
// keeps.
func (e *env) filenode(table string) uint32 {
	e.t.Helper()
	var n uint32
	if err := e.conn.QueryRow(e.t.Context(), "SELECT relfilenode FROM pg_class WHERE oid = to_regclass($1)", pgx.Identifier{e.schema, table}.Sanitize()).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// catalog is what a person sees of a table in the catalog, apart from its
// OID, read without the swap's own queries.
func (e *env) catalog(table string) map[string]string {
	e.t.Helper()
	var b []byte
	err := e.conn.QueryRow(e.t.Context(), `SELECT jsonb_build_object(
		'kind', c.relkind::text || c.relpersistence::text,
		'owner', pg_get_userbyid(c.relowner),
		'privileges', (SELECT string_agg(x::text, ' ' ORDER BY x::text) FROM unnest(coalesce(c.relacl, acldefault('r', c.relowner))) x),
		'columns', (SELECT string_agg(concat_ws(' ', quote_ident(a.attname), format_type(a.atttypid, a.atttypmod), 'collate ' || co.collname,
				CASE WHEN a.attnotnull THEN 'not null' END, 'default ' || pg_get_expr(d.adbin, d.adrelid), 'storage ' || a.attstorage::text,
				'comment ' || col_description(c.oid, a.attnum), 'privileges ' || (SELECT string_agg(x::text, ' ' ORDER BY x::text) FROM unnest(a.attacl) x)), ', ' ORDER BY a.attnum)
			FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
				LEFT JOIN pg_collation co ON co.oid = a.attcollation AND a.attcollation <> 100
			WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped),
		'constraints', (SELECT string_agg(conname || ' ' || pg_get_constraintdef(oid), ', ' ORDER BY conname) FROM pg_constraint WHERE conrelid = c.oid),
		'indexes', (SELECT string_agg(indexdef, ', ' ORDER BY indexname) FROM pg_indexes WHERE schemaname = $2 AND tablename = c.relname),
		'comment', obj_description(c.oid, 'pg_class'),
		'options', c.reloptions::text,
		'toast', (SELECT reloptions::text FROM pg_class WHERE oid = c.reltoastrelid))
		FROM pg_class c WHERE c.oid = to_regclass($1)`, pgx.Identifier{e.schema, table}.Sanitize(), e.schema).Scan(&b)
	if err != nil {
		e.t.Fatal(err)
	}
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		e.t.Fatal(err)
	}
	return m
}

func (e *env) sameCatalog(table string, before map[string]string) {
	e.t.Helper()
	after := e.catalog(table)
	for _, k := range []string{"kind", "owner", "privileges", "columns", "constraints", "indexes", "comment", "options", "toast"} {
		if before[k] != after[k] {
			e.t.Errorf("%s's %s changed:\n before %s\n after  %s", table, k, before[k], after[k])
		}
	}
}

// leftovers counts the read tables, and the relations under a swap's names
// of its own, that are left.
func (e *env) leftovers() int {
	e.t.Helper()
	return e.count(`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = '` + e.schema + `' AND (starts_with(c.relname, '_openetl_read_') OR starts_with(c.relname, '_openetl_swap_'))`)
}

// role creates a role that the test drops, with what it owns.
func (e *env) role() string {
	e.t.Helper()
	role := "r_" + strings.ToLower(rand.Text())[:10]
	if _, err := e.conn.Exec(e.t.Context(), "CREATE ROLE "+role); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() {
		ctx := context.Background()
		_, _ = e.conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{e.schema}.Sanitize()+" CASCADE")
		_, _ = e.conn.Exec(ctx, "DROP OWNED BY "+role)
		_, _ = e.conn.Exec(ctx, "DROP ROLE IF EXISTS "+role)
	})
	return role
}

// destination opens the env's writers with config's settings too.
func (e *env) destination(config map[string]any) {
	e.t.Helper()
	config["dsn"], config["schema"] = dsn(), e.schema
	b, _ := json.Marshal(config)
	d, err := NewDestination(b)
	if err != nil {
		e.t.Fatal(err)
	}
	e.d = d
}

// reasons records why overwrites copied their rows in.
func reasons(t *testing.T) func() []string {
	var mu sync.Mutex
	var b bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&lockedWriter{mu: &mu, w: &b}, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		var out []string
		for line := range strings.SplitSeq(strings.TrimSpace(b.String()), "\n") {
			var r struct{ Msg, Reason string }
			if json.Unmarshal([]byte(line), &r) == nil && r.Reason != "" {
				out = append(out, r.Msg+": "+r.Reason)
			}
		}
		return out
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// atStep makes the swap run f at step, until the test ends.
func atStep(t *testing.T, step string, f func(ctx context.Context, tx pgx.Tx) error) {
	prev := swapStep
	swapStep = func(ctx context.Context, tx pgx.Tx, s string) error {
		if s == step {
			return f(ctx, tx)
		}
		return nil
	}
	t.Cleanup(func() { swapStep = prev })
}

// asRole makes the env's destination sign in as role, which it lets sign in.
func (e *env) asRole(role string) {
	e.t.Helper()
	e.exec("ALTER ROLE " + role + " LOGIN PASSWORD 'pw'")
	u, err := url.Parse(dsn())
	if err != nil {
		e.t.Fatal(err)
	}
	u.User = url.UserPassword(role, "pw")
	cfg, _ := json.Marshal(map[string]string{"dsn": u.String(), "schema": e.schema})
	if e.d, err = NewDestination(cfg); err != nil {
		e.t.Fatal(err)
	}
}

// holdLock reads table in a transaction that stays open, holding its lock,
// until the test ends.
func (e *env) holdLock(table string) {
	e.t.Helper()
	reader, err := pgx.Connect(e.t.Context(), dsn())
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = reader.Close(context.Background()) })
	for _, q := range []string{"BEGIN", "SELECT FROM " + pgx.Identifier{e.schema, table}.Sanitize()} {
		if _, err := reader.Exec(e.t.Context(), q); err != nil {
			e.t.Fatal(err)
		}
	}
}

// shortLocks makes lockTimeout short until the test ends.
func shortLocks(t *testing.T) {
	timeout := lockTimeout
	lockTimeout = 200 * time.Millisecond
	t.Cleanup(func() { lockTimeout = timeout })
}

// overwriteTwo overwrites users and orders in one epoch that ends both
// reads, after before; each row copied into orders checks that its backend
// doesn't hold users' exclusive lock. It reports which swapped.
func (e *env) overwriteTwo(before func()) (usersSwapped, ordersSwapped bool) {
	e.t.Helper()
	t := e.t
	e.exec("CREATE SCHEMA $schema")
	e.exec("CREATE TABLE $schema.users (id bigint, name text, n bigint)")
	e.exec(`CREATE FUNCTION $schema.users_unlocked() RETURNS boolean LANGUAGE sql AS $$
		SELECT NOT EXISTS (SELECT FROM pg_locks WHERE pid = pg_backend_pid() AND granted AND mode = 'AccessExclusiveLock'
			AND relation = to_regclass('` + pgx.Identifier{e.schema, "users"}.Sanitize() + `'))$$`)
	e.exec("CREATE TABLE $schema.orders (id bigint, name text, n bigint CHECK ($schema.users_unlocked()))")
	before()
	usersOID, ordersOID := e.relid("users"), e.relid("orders")
	w := e.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Overwrite}, destination.Stream{Name: "orders", Mode: destination.Overwrite})
	recov(t, w, 1, destination.Epoch{}, nil)
	rt := startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000), "orders": time.UnixMicro(1000)})
	e1 := destination.Epoch{Generation: 1, N: 1}
	for _, stream := range []string{"users", "orders"} {
		applySchema(t, w, stream, userFields)
		write(t, w, e1, stream, batch(t, userFields, firstRows))
		endRead(t, rt, e1, stream)
	}
	commit(t, w, e1)
	if got := e.text("SELECT string_agg(id::text, ',' ORDER BY id) FROM $schema.users") + "/" + e.text("SELECT string_agg(id::text, ',' ORDER BY id) FROM $schema.orders"); got != "1,2/1,2" {
		t.Errorf("users/orders: %s", got)
	}
	return e.relid("users") != usersOID, e.relid("orders") != ordersOID
}

var userFields = []arrow.Field{idField, nameField, nField}

// overwrite reads rows into stream users in two epochs, the second of which
// ends the read, and commits them, as generation 1 with scope. It returns
// the second epoch's descriptor.
func (e *env) overwrite(scope destination.Scope, first, second string) destination.Staged {
	e.t.Helper()
	t := e.t
	w := e.openStreams(1, scope, destination.Stream{Name: "users", Mode: destination.Overwrite})
	recov(t, w, 1, destination.Epoch{}, nil)
	rt := startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000)})
	applySchema(t, w, "users", userFields)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, w, e1, "users", batch(t, userFields, first))
	commit(t, w, e1)
	if second != "" {
		write(t, w, e2, "users", batch(t, userFields, second))
	}
	endRead(t, rt, e2, "users")
	st, _ := commit(t, w, e2)
	return st
}

const (
	firstRows  = `[{"id": 1, "name": "a", "n": 1}, {"id": 2, "name": "b", "n": 2}]`
	secondRows = `[{"id": 3, "name": "c", "n": 3}]`
)

// A target the swap can make again on the read's table, which the user owns
// through a role, gets the read's table in its place, with the same name:
// its owner, privileges on it and its columns, comments, columns' defaults,
// collations and NOT NULL, constraints, indexes and storage parameters are
// as they were. Only its OID changes, and nothing is left behind.
func TestOverwriteSwapsTheReadsTableIn(t *testing.T) {
	for _, scope := range []destination.Scope{destination.ScopePipeline, destination.ScopeStream} {
		t.Run(fmt.Sprint(scope), func(t *testing.T) {
			env := newEnv(t)
			why := reasons(t)
			owner, reader := env.role(), env.role()
			env.exec("CREATE SCHEMA $schema")
			env.exec("GRANT USAGE, CREATE ON SCHEMA $schema TO " + owner)
			env.exec(`CREATE TABLE $schema.users (
				id bigint CONSTRAINT users_key PRIMARY KEY,
				name text COLLATE "C" NOT NULL DEFAULT 'none',
				n bigint CONSTRAINT positive CHECK (n > 0),
				CONSTRAINT one_name UNIQUE (name) DEFERRABLE INITIALLY DEFERRED,
				CONSTRAINT n_once EXCLUDE USING btree (n WITH =))`)
			env.exec("CREATE INDEX users_n ON $schema.users (n DESC) WHERE n > 1")
			env.exec("CREATE INDEX users_lower ON $schema.users (lower(name)) WITH (fillfactor = 70)")
			env.exec("ALTER TABLE $schema.users SET (fillfactor = 90, autovacuum_vacuum_scale_factor = 0.05, toast.autovacuum_enabled = false)")
			env.exec("COMMENT ON TABLE $schema.users IS 'the users'")
			env.exec("COMMENT ON COLUMN $schema.users.name IS 'their name'")
			env.exec("ALTER TABLE $schema.users OWNER TO " + owner)
			env.exec("GRANT SELECT ON $schema.users TO " + reader)
			env.exec("GRANT UPDATE (name) ON $schema.users TO " + reader + " WITH GRANT OPTION")
			env.exec("REVOKE TRUNCATE ON $schema.users FROM " + owner)
			env.exec("INSERT INTO $schema.users VALUES (9, 'old', 9)")
			before, oid := env.catalog("users"), env.relid("users")

			env.overwrite(scope, firstRows, secondRows)
			if got := env.text(users); got != "1:a,2:b,3:c" {
				t.Errorf("after the read ended: %s", got)
			}
			if env.relid("users") == oid {
				t.Errorf("the rows were copied in: %v", why())
			}
			env.sameCatalog("users", before)
			if n := env.leftovers(); n != 0 {
				t.Errorf("%d read or swap relations left", n)
			}
			if n := env.count("SELECT count(*) FROM pg_stats WHERE schemaname = '" + env.schema + "' AND tablename = 'users'"); n == 0 {
				t.Error("the table has no statistics")
			}

		})
	}
}

// A first sync's target, which ApplySchema creates, with the columns the
// destination makes NOT NULL itself, and a read that ends without rows,
// which a table of the target's shape replaces.
func TestOverwriteSwapsANewTargetAndAnEmptyRead(t *testing.T) {
	env := newEnv(t)
	why := reasons(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Overwrite})
	recov(t, w, 1, destination.Epoch{}, nil)
	rt := startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000)})
	fields := append(slices.Clone(userFields), deletedField)
	applySchema(t, w, "users", fields)
	before, oid := env.catalog("users"), env.relid("users")
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e1, "users", batch(t, fields, `[{"id": 1, "name": "a", "n": 1, "_openetl_deleted": false}, {"id": 2, "name": "b", "n": 2, "_openetl_deleted": false}]`))
	endRead(t, rt, e1, "users")
	commit(t, w, e1)
	if got := env.text(users); got != "1:a,2:b" {
		t.Errorf("after the first read: %s", got)
	}
	if env.relid("users") == oid {
		t.Errorf("the first read was copied in: %v", why())
	}
	env.sameCatalog("users", before)

	env.exec("CREATE INDEX users_name ON $schema.users (name)")
	env.exec("COMMENT ON TABLE $schema.users IS 'kept'")
	before, oid = env.catalog("users"), env.relid("users")
	rt = startReads(t, w, map[string]time.Time{"users": time.UnixMicro(2000)})
	e2 := destination.Epoch{Generation: 1, N: 2}
	endRead(t, rt, e2, "users")
	commit(t, w, e2)
	if n := env.count("SELECT count(*) FROM $schema.users"); n != 0 {
		t.Errorf("after an empty read, users has %d rows", n)
	}
	if env.relid("users") == oid {
		t.Errorf("the empty read deleted the rows: %v", why())
	}
	env.sameCatalog("users", before)
	if n := env.leftovers(); n != 0 {
		t.Errorf("%d read or swap relations left", n)
	}

	// The table that took the target's place is no read's: a stream that
	// stops overwriting keeps it.
	swapped := env.relid("users")
	w2 := env.openStreams(2, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Append})
	recov(t, w2, 2, destination.Epoch{}, nil)
	if env.relid("users") != swapped {
		t.Error("the swapped table is gone once the stream appends")
	}
}

// Lanes that each staged part of a read's epochs swap its table in once,
// from lane 0.
func TestOverwriteSwapsAcrossLanes(t *testing.T) {
	env := newEnv(t)
	why := reasons(t)
	stream := destination.Stream{Name: "users", Mode: destination.Overwrite}
	ws := env.openLanes(1, 2, destination.Epoch{}, nil, stream)
	for _, w := range ws {
		startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000)})
	}
	applySchema(t, ws[0], "users", userFields)
	oid := env.relid("users")
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, ws[0], e1, "users", batch(t, userFields, `[{"id": 1, "name": "a", "n": 1}]`))
	write(t, ws[1], e1, "users", batch(t, userFields, `[{"id": 2, "name": "b", "n": 2}]`))
	if _, err := ws[0].Commit(t.Context(), e1, precommitAll(t, e1, ws...)); err != nil {
		t.Fatal(err)
	}
	write(t, ws[1], e2, "users", batch(t, userFields, secondRows))
	endRead(t, ws[0].(destination.ReadTracker), e2, "users")
	if _, err := ws[0].Commit(t.Context(), e2, precommitAll(t, e2, ws...)); err != nil {
		t.Fatal(err)
	}
	if got := env.text(users); got != "1:a,2:b,3:c" || env.relid("users") == oid {
		t.Errorf("after the read ended: %s, swapped %v; logged %q", got, env.relid("users") != oid, why())
	}
	if n := env.leftovers(); n != 0 {
		t.Errorf("%d read or swap relations left", n)
	}
}

// Whatever a swap can't make again, or would leave without the table it
// hangs off, makes the overwrite copy the rows in, as it always has: the
// table keeps its OID, and what hangs off it works on the new rows. The
// reason is logged.
func TestOverwriteCopiesInWhereASwapIsntExact(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  []string
		reason string
		// A query of what hangs off the table, and its result once the read
		// has ended.
		check, want string
		rows        string
	}{
		{name: "view", setup: []string{"CREATE VIEW $schema.v AS SELECT id FROM $schema.users"}, reason: "depends on it",
			check: "SELECT string_agg(id::text, ',' ORDER BY id) FROM $schema.v", want: "1,2,3"},
		{name: "materialized view", setup: []string{"CREATE MATERIALIZED VIEW $schema.mv AS SELECT id FROM $schema.users"}, reason: "depends on it",
			check: "SELECT string_agg(id::text, ',') FROM $schema.mv", want: "9"},
		{name: "function of its rows", setup: []string{"CREATE FUNCTION $schema.f(u $schema.users) RETURNS bigint LANGUAGE sql AS 'SELECT u.id'"}, reason: "depends on it",
			check: "SELECT string_agg($schema.f(u)::text, ',' ORDER BY id) FROM $schema.users u", want: "1,2,3"},
		{name: "trigger", setup: []string{
			"CREATE FUNCTION $schema.up() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN NEW.name := upper(NEW.name); RETURN NEW; END'",
			"CREATE TRIGGER up BEFORE INSERT ON $schema.users FOR EACH ROW EXECUTE FUNCTION $schema.up()"}, reason: "triggers", rows: "1:A,2:B,3:C"},
		{name: "rule", setup: []string{"CREATE TABLE $schema.log (id bigint)", "CREATE RULE logged AS ON INSERT TO $schema.users DO ALSO INSERT INTO $schema.log VALUES (NEW.id)"},
			reason: "rules", check: "SELECT string_agg(id::text, ',' ORDER BY id) FROM $schema.log", want: "1,2,3"},
		{name: "row security", setup: []string{"ALTER TABLE $schema.users ENABLE ROW LEVEL SECURITY", "CREATE POLICY mine ON $schema.users USING (true)"}, reason: "row security"},
		{name: "foreign key to it", setup: []string{"ALTER TABLE $schema.users ADD PRIMARY KEY (id)", "CREATE TABLE $schema.refs (id bigint REFERENCES $schema.users)"},
			reason: "foreign key", check: "SELECT count(*)::text FROM pg_constraint WHERE contype = 'f' AND confrelid = '$schema.users'::regclass", want: "1"},
		{name: "foreign key from it", setup: []string{"CREATE TABLE $schema.names (name text PRIMARY KEY)", "INSERT INTO $schema.names VALUES ('a'), ('b'), ('c'), ('old')",
			"ALTER TABLE $schema.users ADD FOREIGN KEY (name) REFERENCES $schema.names"}, reason: "foreign key"},
		{name: "identity column", setup: []string{"ALTER TABLE $schema.users ALTER COLUMN id SET NOT NULL", "ALTER TABLE $schema.users ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY"}, reason: "identity"},
		{name: "owned sequence", setup: []string{"CREATE SEQUENCE $schema.ids OWNED BY $schema.users.id"}, reason: "depends on it",
			check: "SELECT count(*)::text FROM pg_class WHERE oid = to_regclass('$schema.ids')", want: "1"},
		{name: "generated column", setup: []string{"ALTER TABLE $schema.users ADD COLUMN twice bigint GENERATED ALWAYS AS (id * 2) STORED"}, reason: "generated",
			check: "SELECT string_agg(twice::text, ',' ORDER BY id) FROM $schema.users", want: "2,4,6"},
		{name: "column of its own", setup: []string{"ALTER TABLE $schema.users ADD COLUMN note text DEFAULT 'kept'"}, reason: "columns",
			check: "SELECT string_agg(note, ',' ORDER BY id) FROM $schema.users", want: "kept,kept,kept"},
		{name: "inherited from", setup: []string{"CREATE TABLE $schema.more (extra int) INHERITS ($schema.users)"}, reason: "inherit"},
		{name: "extended statistics", setup: []string{"CREATE STATISTICS $schema.st ON id, name FROM $schema.users"}, reason: "extended statistics"},
		{name: "unlogged", setup: []string{"ALTER TABLE $schema.users SET UNLOGGED"}, reason: "unlogged"},
		{name: "replica identity", setup: []string{"ALTER TABLE $schema.users REPLICA IDENTITY FULL"}, reason: "replica identity"},
		{name: "clustered", setup: []string{"CREATE INDEX users_id ON $schema.users (id)", "ALTER TABLE $schema.users CLUSTER ON users_id"}, reason: "clustered"},
		{name: "index comment", setup: []string{"CREATE INDEX users_id ON $schema.users (id)", "COMMENT ON INDEX $schema.users_id IS 'x'"}, reason: "comment"},
		{name: "row type comment", setup: []string{"COMMENT ON TYPE $schema.users IS 'x'"}, reason: "row type"},
		{name: "index statistics target", setup: []string{"CREATE INDEX users_lower ON $schema.users (lower(name))", "ALTER INDEX $schema.users_lower ALTER COLUMN 1 SET STATISTICS 500"},
			reason: "statistics target"},
		{name: "constraint not validated", setup: []string{"ALTER TABLE $schema.users ADD CONSTRAINT small CHECK (id < 100) NOT VALID"}, reason: "validated"},
		{name: "statistics target", setup: []string{"ALTER TABLE $schema.users ALTER COLUMN name SET STATISTICS 500"}, reason: "statistics target"},
		{name: "column options", setup: []string{"ALTER TABLE $schema.users ALTER COLUMN name SET (n_distinct = 10)"}, reason: "options"},
		{name: "column storage", setup: []string{"ALTER TABLE $schema.users ALTER COLUMN name SET STORAGE EXTERNAL"}, reason: "storage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnv(t)
			why := reasons(t)
			env.exec("CREATE SCHEMA $schema")
			env.exec("CREATE TABLE $schema.users (id bigint, name text, n bigint)")
			env.exec("INSERT INTO $schema.users VALUES (9, 'old', 9)")
			for _, q := range tc.setup {
				env.exec(q)
			}
			oid := env.relid("users")
			env.overwrite(destination.ScopePipeline, firstRows, secondRows)
			want := tc.rows
			if want == "" {
				want = "1:a,2:b,3:c"
			}
			if got := env.text(users); got != want {
				t.Errorf("after the read ended: %s, want %s", got, want)
			}
			if env.relid("users") != oid {
				t.Fatal("the read's table took the target's place")
			}
			if tc.check != "" {
				if got := env.text(tc.check); got != tc.want {
					t.Errorf("%s: %s, want %s", tc.check, got, tc.want)
				}
			}
			if got := why(); len(got) != 1 || !strings.Contains(got[0], tc.reason) {
				t.Errorf("logged %q, want one reason with %q", got, tc.reason)
			}
			if n := env.leftovers(); n != 0 {
				t.Errorf("%d read or swap relations left", n)
			}
		})
	}
}

// A publication of the table, and a privilege on it that another role than
// its owner granted, keep the table too.
func TestOverwriteCopiesInForPublicationsAndGrantsOfOthers(t *testing.T) {
	t.Run("publication", func(t *testing.T) {
		env := newEnv(t)
		why := reasons(t)
		env.exec("CREATE SCHEMA $schema")
		env.exec("CREATE TABLE $schema.users (id bigint PRIMARY KEY, name text, n bigint)")
		pub := "pub_" + env.schema
		env.exec("CREATE PUBLICATION " + pub + " FOR TABLE $schema.users")
		t.Cleanup(func() { _, _ = env.conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+pub) })
		oid := env.relid("users")
		env.overwrite(destination.ScopePipeline, firstRows, secondRows)
		if env.relid("users") != oid || env.count("SELECT count(*) FROM pg_publication_tables WHERE pubname = '"+pub+"'") != 1 {
			t.Error("the publication lost its table")
		}
		if got := why(); len(got) != 1 || !strings.Contains(got[0], "publication") {
			t.Errorf("logged %q", got)
		}
	})
	t.Run("grant of another role", func(t *testing.T) {
		env := newEnv(t)
		why := reasons(t)
		granter, grantee := env.role(), env.role()
		env.exec("CREATE SCHEMA $schema")
		env.exec("GRANT USAGE ON SCHEMA $schema TO " + granter)
		env.exec("CREATE TABLE $schema.users (id bigint, name text, n bigint)")
		env.exec("GRANT SELECT ON $schema.users TO " + granter + " WITH GRANT OPTION")
		env.exec("SET ROLE " + granter)
		env.exec("GRANT SELECT ON $schema.users TO " + grantee)
		env.exec("RESET ROLE")
		oid, before := env.relid("users"), env.catalog("users")
		env.overwrite(destination.ScopePipeline, firstRows, secondRows)
		if env.relid("users") != oid {
			t.Error("the read's table took the target's place")
		}
		env.sameCatalog("users", before)
		if got := why(); len(got) != 1 || !strings.Contains(got[0], "granted") {
			t.Errorf("logged %q", got)
		}
	})
}

// A user that doesn't own the target, and may only write it, copies the
// rows in.
func TestOverwriteOfATableTheUserDoesntOwn(t *testing.T) {
	env := newEnv(t)
	why := reasons(t)
	role := env.role()
	env.exec("CREATE SCHEMA $schema")
	env.exec("GRANT USAGE, CREATE ON SCHEMA $schema TO " + role)
	env.exec("CREATE TABLE $schema.users (id bigint, name text, n bigint)")
	env.exec("GRANT SELECT, INSERT, DELETE ON $schema.users TO " + role)
	env.asRole(role)
	oid := env.relid("users")
	env.overwrite(destination.ScopePipeline, firstRows, secondRows)
	if got := env.text(users); got != "1:a,2:b,3:c" || env.relid("users") != oid {
		t.Errorf("after the read ended: %s, swapped %v", got, env.relid("users") != oid)
	}
	if got := why(); len(got) != 1 || !strings.Contains(got[0], "doesn't own") {
		t.Errorf("logged %q", got)
	}
}

// A failure or a lost connection at any step of a swap leaves the target
// with its old rows and the read's table with the epochs applied before;
// the epoch, recovered, swaps once.
func TestASwapThatFailsLeavesTheTarget(t *testing.T) {
	for _, step := range []string{"prepared", "locked", "swapped"} {
		for _, lost := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/lost=%v", step, lost), func(t *testing.T) {
				env := newEnv(t)
				ctx := t.Context()
				env.exec("CREATE SCHEMA $schema")
				env.exec("CREATE TABLE $schema.users (id bigint PRIMARY KEY, name text, n bigint)")
				env.exec("COMMENT ON TABLE $schema.users IS 'the users'")
				env.exec("INSERT INTO $schema.users VALUES (9, 'old', 9)")
				before, oid := env.catalog("users"), env.relid("users")
				stream := destination.Stream{Name: "users", Mode: destination.Overwrite}
				w1 := env.openStreams(1, destination.ScopePipeline, stream)
				recov(t, w1, 1, destination.Epoch{}, nil)
				rt := startReads(t, w1, map[string]time.Time{"users": time.UnixMicro(1000)})
				applySchema(t, w1, "users", userFields)
				e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
				write(t, w1, e1, "users", batch(t, userFields, firstRows))
				commit(t, w1, e1)
				write(t, w1, e2, "users", batch(t, userFields, secondRows))
				endRead(t, rt, e2, "users")
				st2 := precommit(t, w1, e2)

				stop := errors.New("stopped")
				atStep(t, step, func(ctx context.Context, tx pgx.Tx) error {
					if lost {
						_, err := tx.Exec(ctx, "SELECT pg_terminate_backend(pg_backend_pid())")
						return err
					}
					return stop
				})
				if _, err := w1.Commit(ctx, e2, []destination.Staged{st2}); err == nil || !lost && !errors.Is(err, stop) {
					t.Fatalf("Commit: %v", err)
				}
				swapStep = func(context.Context, pgx.Tx, string) error { return nil }
				read := "SELECT string_agg(id::text, ',' ORDER BY id) FROM $schema." + sqlstage.ReadName("p1", "users", time.UnixMicro(1000))
				if got := env.text(users); got != "9:old" || env.relid("users") != oid || env.text(read) != "1,2" || env.markers() != 1 {
					t.Fatalf("after the failed swap: %s, %s in the read, %d markers, swapped %v", got, env.text(read), env.markers(), env.relid("users") != oid)
				}
				env.sameCatalog("users", before)

				w2 := env.openStreams(2, destination.ScopePipeline, stream)
				applySchema(t, w2, "users", userFields)
				if r := recov(t, w2, 2, e2, st2); r != destination.Redone {
					t.Fatalf("Recover = %v, want redone", r)
				}
				if got := env.text(users); got != "1:a,2:b,3:c" || env.relid("users") == oid {
					t.Errorf("after the redo: %s, swapped %v", got, env.relid("users") != oid)
				}
				env.sameCatalog("users", before)
				if n := env.leftovers(); n != 0 {
					t.Errorf("%d read or swap relations left", n)
				}
				swapped := env.relid("users")
				w3 := env.openStreams(3, destination.ScopePipeline, stream)
				if r := recov(t, w3, 3, e2, st2); r != destination.Applied {
					t.Errorf("Recover again = %v, want applied", r)
				}
				if _, err := w2.Commit(ctx, e2, []destination.Staged{st2}); !errors.Is(err, destination.ErrFenced) {
					t.Errorf("Commit of the applied epoch by a fenced writer: %v", err)
				}
				if got := env.text(users); got != "1:a,2:b,3:c" || env.relid("users") != swapped {
					t.Errorf("after recovering an applied epoch: %s", got)
				}
			})
		}
	}
}

// A writer that a newer generation has fenced can't swap.
func TestAFencedWriterDoesntSwap(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	env.exec("CREATE SCHEMA $schema")
	env.exec("CREATE TABLE $schema.users (id bigint, name text, n bigint)")
	env.exec("INSERT INTO $schema.users VALUES (9, 'old', 9)")
	oid := env.relid("users")
	stream := destination.Stream{Name: "users", Mode: destination.Overwrite}
	w1 := env.openStreams(1, destination.ScopePipeline, stream)
	recov(t, w1, 1, destination.Epoch{}, nil)
	rt := startReads(t, w1, map[string]time.Time{"users": time.UnixMicro(1000)})
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w1, e1, "users", batch(t, userFields, firstRows))
	endRead(t, rt, e1, "users")
	st := precommit(t, w1, e1)
	w2 := env.openStreams(2, destination.ScopePipeline, stream)
	recov(t, w2, 2, destination.Epoch{}, nil)
	if _, err := w1.Commit(ctx, e1, []destination.Staged{st}); !errors.Is(err, destination.ErrFenced) {
		t.Fatalf("Commit of a fenced writer: %v", err)
	}
	if got := env.text(users); got != "9:old" || env.relid("users") != oid {
		t.Errorf("the fenced writer replaced the rows: %s", got)
	}
}

// A reader that holds the target's lock past lockTimeout makes the
// overwrite copy the rows in, so the reader keeps its rows and isn't
// waited for; one that starts while the swap holds the lock waits for it,
// then reads the new rows, as does a statement prepared before. A
// transaction whose snapshot is older than the swap, and that reads the
// table first after it, sees the rows the read had when the snapshot was
// taken.
func TestReadersOfASwap(t *testing.T) {
	shortLocks(t)
	connect := func(t *testing.T) *pgx.Conn {
		c, err := pgx.Connect(t.Context(), dsn())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return c
	}
	setup := func(t *testing.T) *env {
		env := newEnv(t)
		env.exec("CREATE SCHEMA $schema")
		env.exec("CREATE TABLE $schema.users (id bigint, name text, n bigint)")
		env.exec("INSERT INTO $schema.users VALUES (9, 'old', 9)")
		return env
	}
	rows := func(t *testing.T, env *env, c *pgx.Conn) string {
		var s string
		if err := c.QueryRow(t.Context(), strings.ReplaceAll(users, "$schema", `"`+env.schema+`"`)).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	t.Run("holding the lock", func(t *testing.T) {
		env := setup(t)
		why := reasons(t)
		reader := connect(t)
		if _, err := reader.Exec(t.Context(), "BEGIN ISOLATION LEVEL REPEATABLE READ"); err != nil {
			t.Fatal(err)
		}
		if got := rows(t, env, reader); got != "9:old" {
			t.Fatal(got)
		}
		oid := env.relid("users")
		env.overwrite(destination.ScopePipeline, firstRows, secondRows)
		if env.relid("users") != oid {
			t.Fatal("the swap didn't wait for the reader")
		}
		if got := why(); len(got) != 1 || !strings.Contains(got[0], "held a lock") {
			t.Errorf("logged %q", got)
		}
		if got := rows(t, env, reader); got != "9:old" {
			t.Errorf("the reader's transaction sees %s", got)
		}
		if _, err := reader.Exec(t.Context(), "COMMIT"); err != nil {
			t.Fatal(err)
		}
		if got := rows(t, env, reader); got != "1:a,2:b,3:c" {
			t.Errorf("after the reader's transaction: %s", got)
		}
	})

	t.Run("waiting for it", func(t *testing.T) {
		env := setup(t)
		reader, prepared, old := connect(t), connect(t), connect(t)
		if _, err := prepared.Prepare(t.Context(), "users", strings.ReplaceAll(users, "$schema", `"`+env.schema+`"`)); err != nil {
			t.Fatal(err)
		}
		got := make(chan string, 1)
		atStep(t, "locked", func(ctx context.Context, _ pgx.Tx) error {
			go func() {
				var s string
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				err := reader.QueryRow(ctx, strings.ReplaceAll(users, "$schema", `"`+env.schema+`"`)).Scan(&s)
				if err != nil {
					s = err.Error()
				}
				got <- s
			}()
			for range 100 {
				var waiting bool
				if err := env.conn.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock')", reader.PgConn().PID()).Scan(&waiting); err != nil {
					return err
				}
				if waiting {
					return nil
				}
				time.Sleep(20 * time.Millisecond)
			}
			return errors.New("the reader never waited for the lock")
		})
		w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Overwrite})
		recov(t, w, 1, destination.Epoch{}, nil)
		rt := startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000)})
		e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
		write(t, w, e1, "users", batch(t, userFields, firstRows))
		commit(t, w, e1)
		// A snapshot taken between the read's epochs.
		if _, err := old.Exec(t.Context(), "BEGIN ISOLATION LEVEL REPEATABLE READ"); err != nil {
			t.Fatal(err)
		}
		if _, err := old.Exec(t.Context(), "SELECT 1"); err != nil {
			t.Fatal(err)
		}
		oid := env.relid("users")
		write(t, w, e2, "users", batch(t, userFields, secondRows))
		endRead(t, rt, e2, "users")
		commit(t, w, e2)
		if env.relid("users") == oid {
			t.Fatal("the rows were copied in")
		}
		select {
		case s := <-got:
			if s != "1:a,2:b,3:c" {
				t.Errorf("the reader that waited got %s", s)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("no reader waited for the swap's lock")
		}
		var s string
		if err := prepared.QueryRow(t.Context(), "users").Scan(&s); err != nil || s != "1:a,2:b,3:c" {
			t.Errorf("the prepared statement: %s, %v", s, err)
		}
		if got := rows(t, env, old); got != "1:a,2:b" {
			t.Errorf("the older snapshot sees %s", got)
		}
	})
}

// A read deduped against itself has an _openetl_id index that the target
// doesn't, which goes; a target that has one, from an earlier mode, keeps
// it, under its name.
func TestASwapKeepsTheTargetsIndexesOnly(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%v", indexed), func(t *testing.T) {
			env := newEnv(t)
			why := reasons(t)
			fields := []arrow.Field{idField, rowID}
			env.exec("CREATE SCHEMA $schema")
			env.exec("CREATE TABLE $schema.events (id bigint, _openetl_id uuid)")
			if indexed {
				env.exec("CREATE INDEX " + sqlstage.IndexName("events") + " ON $schema.events (_openetl_id)")
			}
			before, oid := env.catalog("events"), env.relid("events")
			w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "events", Mode: destination.Overwrite, Dedupe: destination.DedupeTarget})
			recov(t, w, 1, destination.Epoch{}, nil)
			rt := startReads(t, w, map[string]time.Time{"events": time.UnixMicro(1000)})
			e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
			write(t, w, e1, "events", batch(t, fields, `[{"id": 1, "_openetl_id": `+uid(1)+`}]`))
			commit(t, w, e1)
			write(t, w, e2, "events", batch(t, fields, `[{"id": 1, "_openetl_id": `+uid(1)+`}, {"id": 2, "_openetl_id": `+uid(2)+`}]`))
			endRead(t, rt, e2, "events")
			if _, c := commit(t, w, e2); !maps.Equal(c.Collapsed, map[string]uint64{"events": 1}) {
				t.Errorf("collapsed %v", c.Collapsed)
			}
			if got := env.text("SELECT string_agg(id::text, ',' ORDER BY id) FROM $schema.events"); got != "1,2" {
				t.Errorf("events: %s", got)
			}
			if env.relid("events") == oid {
				t.Errorf("the rows were copied in: %v", why())
			}
			env.sameCatalog("events", before)
		})
	}
}

// A read's table narrower than its target, which a person widened after
// the read's last rows, is copied in: the swap changes no column's type.
// One that took the target's wider type swaps.
func TestASwapChangesNoColumnsType(t *testing.T) {
	n32 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	for _, widened := range []bool{false, true} {
		t.Run(fmt.Sprintf("widened before the read's last rows=%v", widened), func(t *testing.T) {
			env := newEnv(t)
			why := reasons(t)
			w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Overwrite})
			recov(t, w, 1, destination.Epoch{}, nil)
			rt := startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000)})
			applySchema(t, w, "users", []arrow.Field{idField, n32})
			e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
			write(t, w, e1, "users", batch(t, []arrow.Field{idField, n32}, `[{"id": 1, "n": 1}]`))
			if widened {
				env.exec("ALTER TABLE $schema.users ALTER COLUMN n TYPE bigint")
			}
			commit(t, w, e1)
			if !widened {
				env.exec("ALTER TABLE $schema.users ALTER COLUMN n TYPE bigint")
			}
			oid := env.relid("users")
			endRead(t, rt, e2, "users")
			commit(t, w, e2)
			if typ := env.text("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = '$schema.users'::regclass AND attname = 'n'"); typ != "bigint" {
				t.Errorf("n is %s", typ)
			}
			if got := env.text("SELECT string_agg(id || ':' || n, ',') FROM $schema.users"); got != "1:1" {
				t.Errorf("users: %s", got)
			}
			if swapped := env.relid("users") != oid; swapped != widened {
				t.Errorf("swapped %v; logged %q", swapped, why())
			}
		})
	}
}

// A stream that never had a schema has no table to replace.
func TestAnOverwriteWithoutATable(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Overwrite})
	recov(t, w, 1, destination.Epoch{}, nil)
	rt := startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000)})
	e1 := destination.Epoch{Generation: 1, N: 1}
	endRead(t, rt, e1, "users")
	commit(t, w, e1)
	if env.relid("users") != 0 || env.leftovers() != 0 {
		t.Error("an overwrite without rows or a schema created a table")
	}
}

// overwriteSwap false keeps the table, as overwriteTruncate does, which
// truncates it; a truncate that can't have its lock in time, or that a
// foreign key refuses, deletes the rows instead.
func TestOverwriteSettings(t *testing.T) {
	shortLocks(t)
	for _, tc := range []struct {
		name   string
		config map[string]any
		setup  []string
		lock   bool
		// Whether the table is truncated, and why it isn't.
		truncated bool
		reason    string
	}{
		{name: "no swap", config: map[string]any{"overwriteSwap": false}},
		{name: "truncate", config: map[string]any{"overwriteSwap": false, "overwriteTruncate": true}, truncated: true},
		{name: "truncate where the swap isn't exact", config: map[string]any{"overwriteTruncate": true}, setup: []string{"CREATE VIEW $schema.v AS SELECT id FROM $schema.users"}, truncated: true},
		{name: "truncate under a foreign key", config: map[string]any{"overwriteSwap": false, "overwriteTruncate": true},
			setup: []string{"ALTER TABLE $schema.users ADD PRIMARY KEY (id)", "CREATE TABLE $schema.refs (id bigint REFERENCES $schema.users)"}, reason: "foreign key"},
		{name: "truncate behind a reader", config: map[string]any{"overwriteSwap": false, "overwriteTruncate": true}, lock: true, reason: "held a lock"},
		{name: "truncate under a publication of deletes only", config: map[string]any{"overwriteSwap": false, "overwriteTruncate": true},
			setup: []string{"ALTER TABLE $schema.users ADD PRIMARY KEY (id)", "CREATE PUBLICATION pub_$name FOR TABLE $schema.users WITH (publish = 'insert, update, delete')"}, reason: "not a truncate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newEnv(t)
			why := reasons(t)
			env.destination(tc.config)
			env.exec("CREATE SCHEMA $schema")
			env.exec("CREATE TABLE $schema.users (id bigint, name text, n bigint)")
			env.exec("INSERT INTO $schema.users VALUES (9, 'old', 9)")
			for _, q := range tc.setup {
				env.exec(strings.ReplaceAll(q, "$name", env.schema))
			}
			t.Cleanup(func() { _, _ = env.conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS pub_"+env.schema) })
			if tc.lock {
				env.holdLock("users")
			}
			oid, node := env.relid("users"), env.filenode("users")
			env.overwrite(destination.ScopePipeline, firstRows, secondRows)
			if got := env.text(users); got != "1:a,2:b,3:c" {
				t.Errorf("users: %s", got)
			}
			if env.relid("users") != oid {
				t.Error("the read's table took the target's place")
			}
			if truncated := env.filenode("users") != node; truncated != tc.truncated {
				t.Errorf("truncated %v; logged %q", truncated, why())
			}
			if tc.reason != "" {
				if got := why(); !strings.Contains(strings.Join(got, "\n"), tc.reason) {
					t.Errorf("logged %q, want %q", got, tc.reason)
				}
			}
		})
	}
}

// The rows an epoch takes into a read's table have their hint bits set
// once it's applied, as do the last epoch's once its table has swapped in:
// a scan of them later sets none, which, with data checksums, would log
// each page whole. The read's table fills its pages as the target's
// fillfactor has it.
func TestAppliedRowsHaveTheirHintBits(t *testing.T) {
	env := newEnv(t)
	env.exec("CREATE SCHEMA $schema")
	env.exec("CREATE EXTENSION pageinspect SCHEMA $schema")
	// The live rows whose inserting transaction no hint bit says committed.
	unhinted := func(table string) int {
		return env.count(`SELECT count(*) FROM generate_series(0, pg_relation_size(to_regclass($$` + pgx.Identifier{env.schema, table}.Sanitize() + `$$)) / current_setting('block_size')::int - 1) b,
			$schema.heap_page_items($schema.get_raw_page($$` + pgx.Identifier{env.schema, table}.Sanitize() + `$$, b::int))
			WHERE lp_flags = 1 AND t_infomask & 256 = 0`)
	}
	rows := func(from, n int) string {
		var b strings.Builder
		b.WriteString("[")
		for i := from; i < from+n; i++ {
			if i > from {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"id": %d, "name": "user %d", "n": %d}`, i, i, i)
		}
		return b.String() + "]"
	}
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Overwrite})
	recov(t, w, 1, destination.Epoch{}, nil)
	rt := startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000)})
	applySchema(t, w, "users", userFields)
	env.exec("ALTER TABLE $schema.users SET (fillfactor = 50)")
	oid := env.relid("users")
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, w, e1, "users", batch(t, userFields, rows(1, 2000)))
	commit(t, w, e1)
	if n := unhinted(sqlstage.ReadName("p1", "users", time.UnixMicro(1000))); n != 0 {
		t.Errorf("%d of the read's rows have no hint bit after their epoch", n)
	}
	write(t, w, e2, "users", batch(t, userFields, rows(2001, 2000)))
	endRead(t, rt, e2, "users")
	commit(t, w, e2)
	if env.relid("users") == oid {
		t.Fatal("the rows were copied in")
	}
	if n, all := unhinted("users"), env.count("SELECT count(*) FROM $schema.users"); n != 0 || all != 4000 {
		t.Errorf("%d of the swapped table's %d rows have no hint bit", n, all)
	}
	if free := env.count(`SELECT upper - lower FROM $schema.page_header($schema.get_raw_page($$` + pgx.Identifier{env.schema, "users"}.Sanitize() + `$$, 0))`); free < 3500 {
		t.Errorf("the first page, at fillfactor 50, has %d bytes free", free)
	}
}

// Where one of an epoch's swaps can't have its lock, every lock is let go
// before that stream's rows are copied, so the copy never runs while
// another swap holds its target; the others then swap.
func TestASwapThatCantLockCopiesWithoutTheOthersLocks(t *testing.T) {
	shortLocks(t)
	env := newEnv(t)
	why := reasons(t)
	if users, orders := env.overwriteTwo(func() { env.holdLock("orders") }); !users || orders {
		t.Errorf("users swapped %v, orders %v; logged %q", users, orders, why())
	}
	if got := why(); len(got) != 1 || !strings.Contains(got[0], "held a lock") {
		t.Errorf("logged %q, want orders' lock", got)
	}
}

// A column whose type the stream changed from the target's keeps the
// stream's type in the read's table, without the target's collation.
func TestAReadsColumnOfAnotherTypeTakesNoCollation(t *testing.T) {
	env := newEnv(t)
	env.exec("CREATE SCHEMA $schema")
	env.exec(`CREATE TABLE $schema.users (id bigint, code varchar(20) COLLATE "C")`)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Overwrite})
	recov(t, w, 1, destination.Epoch{}, nil)
	startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000)})
	code := arrow.Field{Name: "code", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e1, "users", batch(t, []arrow.Field{idField, code}, `[{"id": 1, "code": 7}]`))
	commit(t, w, e1)
	read := pgx.Identifier{env.schema, sqlstage.ReadName("p1", "users", time.UnixMicro(1000))}.Sanitize()
	if got := env.text(`SELECT format_type(atttypid, atttypmod) || ' ' || attcollation FROM pg_attribute WHERE attrelid = '` + read + `'::regclass AND attname = 'code'`); got != "bigint 0" {
		t.Errorf("the read's code is %s", got)
	}
}

// What hangs off the read's table, as off the target, makes the overwrite
// copy; so does a logical replication slot that decodes every table.
func TestOverwriteCopiesForTheReadsTableAndLogicalSlots(t *testing.T) {
	t.Run("trigger on the read's table", func(t *testing.T) {
		env := newEnv(t)
		why := reasons(t)
		w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Overwrite})
		recov(t, w, 1, destination.Epoch{}, nil)
		rt := startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000)})
		applySchema(t, w, "users", userFields)
		oid := env.relid("users")
		e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
		write(t, w, e1, "users", batch(t, userFields, firstRows))
		commit(t, w, e1)
		env.exec("CREATE FUNCTION $schema.f() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'")
		env.exec("CREATE TRIGGER t BEFORE INSERT ON $schema." + sqlstage.ReadName("p1", "users", time.UnixMicro(1000)) + " FOR EACH ROW EXECUTE FUNCTION $schema.f()")
		write(t, w, e2, "users", batch(t, userFields, secondRows))
		endRead(t, rt, e2, "users")
		commit(t, w, e2)
		if got := env.text(users); got != "1:a,2:b,3:c" || env.relid("users") != oid {
			t.Errorf("users: %s, swapped %v", got, env.relid("users") != oid)
		}
		if got := why(); len(got) != 1 || !strings.Contains(got[0], "the read's table: it has triggers") {
			t.Errorf("logged %q", got)
		}
	})
	t.Run("logical slot", func(t *testing.T) {
		env := newEnv(t)
		why := reasons(t)
		slot := "pgdest_" + env.schema
		env.exec("SELECT pg_create_logical_replication_slot('" + slot + "', 'test_decoding', true)")
		env.exec("CREATE SCHEMA $schema")
		env.exec("CREATE TABLE $schema.users (id bigint, name text, n bigint)")
		oid := env.relid("users")
		env.overwrite(destination.ScopePipeline, firstRows, secondRows)
		if env.relid("users") != oid {
			t.Error("the read's table took the target's place")
		}
		if got := why(); len(got) != 1 || !strings.Contains(got[0], "logical replication slot") {
			t.Errorf("logged %q", got)
		}
	})
}

// A target whose owner gave up its own SELECT, which the read's table would
// take on swapping, still has the rows copied in where its swap can't have
// its lock: the read's table takes the target's privileges only in the
// swap.
func TestACopyAfterAFailedSwapReadsTheReadsTable(t *testing.T) {
	shortLocks(t)
	env := newEnv(t)
	why := reasons(t)
	role := env.role()
	env.exec("CREATE SCHEMA $schema")
	env.exec("GRANT USAGE, CREATE ON SCHEMA $schema TO " + role)
	env.exec("CREATE TABLE $schema.users (id bigint, name text, n bigint)")
	env.exec("ALTER TABLE $schema.users OWNER TO " + role)
	env.exec("REVOKE SELECT ON $schema.users FROM " + role)
	env.asRole(role)
	env.holdLock("users")
	oid := env.relid("users")
	env.overwrite(destination.ScopePipeline, firstRows, secondRows)
	if got := env.text(users); got != "1:a,2:b,3:c" || env.relid("users") != oid {
		t.Errorf("users: %s, swapped %v", got, env.relid("users") != oid)
	}
	if got := why(); len(got) != 1 || !strings.Contains(got[0], "held a lock") {
		t.Errorf("logged %q", got)
	}
}

// A swap that fails once its target is dropped and renamed undoes the
// epoch's other swaps too, letting go of their locks, before its rows are
// copied; the others then swap.
func TestALateSwapFailureUndoesTheOthers(t *testing.T) {
	env := newEnv(t)
	why := reasons(t)
	// The second swap, orders', fails as the server would.
	calls := 0
	atStep(t, "swapped", func(ctx context.Context, tx pgx.Tx) error {
		if calls++; calls == 2 {
			_, err := tx.Exec(ctx, "SELECT 1 / 0")
			return err
		}
		return nil
	})
	if users, orders := env.overwriteTwo(func() {}); !users || orders {
		t.Errorf("users swapped %v, orders %v; logged %q", users, orders, why())
	}
	if got := why(); len(got) != 1 || !strings.Contains(got[0], "division by zero") {
		t.Errorf("logged %q", got)
	}
	if n := env.leftovers(); n != 0 {
		t.Errorf("%d read or swap relations left", n)
	}
}
