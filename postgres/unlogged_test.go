//go:build integration

package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl-connectors/sqlstage"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// crash empties every unlogged table of the schema, as the server's
// recovery from a crash, or a standby's promotion, leaves them.
func (e *env) crash() {
	e.t.Helper()
	ctx := e.t.Context()
	rows, err := e.conn.Query(ctx, `SELECT c.oid::regclass::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind = 'r' AND c.relpersistence = 'u'`, e.schema)
	if err != nil {
		e.t.Fatal(err)
	}
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		e.t.Fatal(err)
	}
	if len(tables) == 0 {
		e.t.Fatal("no unlogged table to empty")
	}
	for _, t := range tables {
		if _, err := e.conn.Exec(ctx, "TRUNCATE "+t); err != nil {
			e.t.Fatal(err)
		}
	}
}

// persistence is a table's relpersistence and its storage parameters; ""
// if it doesn't exist.
func (e *env) persistence(table string) (string, string) {
	e.t.Helper()
	var p, opts string
	err := e.conn.QueryRow(e.t.Context(), "SELECT relpersistence::text, coalesce(array_to_string(reloptions, ','), '') FROM pg_class WHERE oid = to_regclass($1)",
		pgx.Identifier{e.schema, table}.Sanitize()).Scan(&p, &opts)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ""
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return p, opts
}

func (e *env) markers() int {
	e.t.Helper()
	return e.count("SELECT count(*) FROM $schema._openetl_loads")
}

// Staging is unlogged with pipeline scope, after its generation's alive
// table, and logged with stream scope; autovacuum takes neither. Targets
// and read tables are logged.
func TestStagingIsUnloggedWithPipelineScope(t *testing.T) {
	for _, tc := range []struct {
		scope destination.Scope
		want  string
	}{{destination.ScopePipeline, "u"}, {destination.ScopeStream, "p"}} {
		env := newEnv(t)
		w := env.openStreams(1, tc.scope, destination.Stream{Name: "users", Mode: destination.Append}, destination.Stream{Name: "names", Mode: destination.Overwrite})
		recov(t, w, 1, destination.Epoch{}, nil)
		startReads(t, w, map[string]time.Time{"names": time.UnixMicro(1000)})
		e := destination.Epoch{Generation: 1, N: 1}
		write(t, w, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`))
		write(t, w, e, "names", batch(t, []arrow.Field{idField}, `[{"id": 2}]`))
		for _, stream := range []string{"users", "names"} {
			if p, opts := env.persistence(sqlstage.StagingName("p1", stream, e, 0)); p != tc.want || opts != "autovacuum_enabled=false" {
				t.Errorf("scope %v: the staging of %s has persistence %q and options %q, want %q and autovacuum off", tc.scope, stream, p, opts, tc.want)
			}
		}
		if p, _ := env.persistence(aliveName("p1", 1)); (p == "u") != (tc.want == "u") {
			t.Errorf("scope %v: the alive table's persistence is %q", tc.scope, p)
		}
		commit(t, w, e)
		if p, _ := env.persistence("users"); p != "p" {
			t.Errorf("scope %v: the target's persistence is %q", tc.scope, p)
		}
		if p, _ := env.persistence(sqlstage.ReadName("p1", "names", time.UnixMicro(1000))); p != "p" {
			t.Errorf("scope %v: the read table's persistence is %q", tc.scope, p)
		}
		if n := env.count("SELECT count(*) FROM $schema.users"); n != 1 {
			t.Errorf("scope %v: users has %d rows, want 1", tc.scope, n)
		}
	}
}

// A crash after an epoch is recorded and before it's applied empties its
// staging. A Commit once the server is back applies nothing, Recover finds
// the epoch lost, the targets keep what they had, and the epoch's rows, read
// again, land once.
func TestACrashBeforeTheApplyLosesTheEpoch(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	id := []arrow.Field{idField}
	const rows = "SELECT (SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM $schema.users) || '/' || (SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM $schema.orders)"
	w1 := env.open(1, destination.ScopePipeline, "users", "orders")
	recov(t, w1, 1, destination.Epoch{}, nil)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, w1, e1, "users", batch(t, id, `[{"id": 1}, {"id": 2}]`))
	write(t, w1, e1, "orders", batch(t, id, `[{"id": 10}]`))
	commit(t, w1, e1)
	write(t, w1, e2, "users", batch(t, id, `[{"id": 3}]`))
	write(t, w1, e2, "orders", batch(t, id, `[{"id": 11}]`))
	st2 := precommit(t, w1, e2)
	env.crash()

	if _, err := w1.Commit(ctx, e2, []destination.Staged{st2}); err == nil || codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Fatalf("Commit of emptied staging: %v, want a transient error", err)
	}
	if got := env.text(rows); got != "1,2/10" || env.markers() != 1 {
		t.Fatalf("after a Commit of emptied staging: %s, %d markers", got, env.markers())
	}
	w2 := env.open(2, destination.ScopePipeline, "users", "orders")
	if r := recov(t, w2, 2, e2, st2); r != destination.Lost {
		t.Fatalf("Recover = %v, want lost", r)
	}
	if got := env.text(rows); got != "1,2/10" || env.markers() != 1 {
		t.Fatalf("after the epoch was lost: %s, %d markers", got, env.markers())
	}
	// The runner restores the epoch before, and reads the lost one again.
	again := destination.Epoch{Generation: 2, N: 2}
	write(t, w2, again, "users", batch(t, id, `[{"id": 3}]`))
	write(t, w2, again, "orders", batch(t, id, `[{"id": 11}]`))
	st, _ := commit(t, w2, again)
	w3 := env.open(3, destination.ScopePipeline, "users", "orders")
	if r := recov(t, w3, 3, again, st); r != destination.Applied {
		t.Fatalf("Recover of the epoch read again = %v, want applied", r)
	}
	if got := env.text(rows); got != "1,2,3/10,11" {
		t.Errorf("after the epoch was read again: %s", got)
	}
}

// A crash while an epoch is staged empties what its lanes staged before it,
// and a lane's next COPY, on a new connection, lands in the emptied table:
// nothing writes the alive table's row again, though a lane that hadn't
// staged yet makes sure of it. The epoch's Commit fails, Recover finds it
// lost, and its rows, read again, land once.
func TestACrashWhileStagingFailsTheCommit(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	id := []arrow.Field{idField}
	stream := destination.Stream{Name: "users", Mode: destination.Append}
	ws := env.openLanes(1, 2, destination.Epoch{}, nil, stream)
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, ws[0], e1, "users", batch(t, id, `[{"id": 1}]`))
	env.crash()
	write(t, ws[0], e1, "users", batch(t, id, `[{"id": 2}]`))
	write(t, ws[1], e1, "users", batch(t, id, `[{"id": 3}]`))
	if n := env.count("SELECT count(*) FROM $schema." + aliveName("p1", 1)); n != 0 {
		t.Fatalf("the alive table has %d rows after the crash", n)
	}
	st := precommitAll(t, e1, ws...)
	if _, err := ws[0].Commit(ctx, e1, st); err == nil || codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Fatalf("Commit of an epoch the crash emptied part of: %v, want a transient error", err)
	}
	// The apply created the target before it took the fence.
	if n := env.count("SELECT count(*) FROM $schema.users"); n != 0 || env.markers() != 0 {
		t.Fatalf("after the failed Commit: %d rows, %d markers", n, env.markers())
	}
	w2 := env.openStreams(2, destination.ScopePipeline, stream)
	r, err := w2.Recover(ctx, 2, e1, st)
	if err != nil || r != destination.Lost {
		t.Fatalf("Recover = %v, %v, want lost", r, err)
	}
	again := destination.Epoch{Generation: 2, N: 1}
	write(t, w2, again, "users", batch(t, id, `[{"id": 1}, {"id": 2}, {"id": 3}]`))
	commit(t, w2, again)
	if got := env.text("SELECT string_agg(id::text, ',' ORDER BY id) FROM $schema.users"); got != "1,2,3" {
		t.Errorf("after the epoch was read again: %s", got)
	}
}

// A crash in the middle of an overwrite read loses only the epoch it hadn't
// applied: the read's table, logged, keeps the epochs before it, and the
// target keeps its old rows until the read, continued, ends once.
func TestACrashMidReadKeepsTheAppliedEpochs(t *testing.T) {
	env := newEnv(t)
	env.exec("CREATE SCHEMA $schema")
	env.exec("CREATE TABLE $schema.users (id bigint, name text)")
	env.exec("INSERT INTO $schema.users VALUES (9, 'old')")
	stream := destination.Stream{Name: "users", Mode: destination.Overwrite}
	fields := []arrow.Field{idField, nameField}
	start := map[string]time.Time{"users": time.UnixMicro(1000)}
	read := "SELECT string_agg(id::text, ',' ORDER BY id) FROM $schema." + sqlstage.ReadName("p1", "users", time.UnixMicro(1000))
	w1 := env.openStreams(1, destination.ScopePipeline, stream)
	recov(t, w1, 1, destination.Epoch{}, nil)
	rt := startReads(t, w1, start)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, w1, e1, "users", batch(t, fields, `[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]`))
	commit(t, w1, e1)
	write(t, w1, e2, "users", batch(t, fields, `[{"id": 3, "name": "c"}]`))
	endRead(t, rt, e2, "users")
	st2 := precommit(t, w1, e2)
	env.crash()

	w2 := env.openStreams(2, destination.ScopePipeline, stream)
	applySchema(t, w2, "users", fields)
	if r := recov(t, w2, 2, e2, st2); r != destination.Lost {
		t.Fatalf("Recover = %v, want lost", r)
	}
	if got := env.text(users); got != "9:old" {
		t.Errorf("the target after the lost epoch: %s", got)
	}
	if got := env.text(read); got != "1,2" {
		t.Errorf("the read's table after the lost epoch: %s", got)
	}
	rt2 := startReads(t, w2, start)
	again := destination.Epoch{Generation: 2, N: 2}
	write(t, w2, again, "users", batch(t, fields, `[{"id": 3, "name": "c"}]`))
	endRead(t, rt2, again, "users")
	commit(t, w2, again)
	if got := env.text(users); got != "1:a,2:b,3:c" {
		t.Errorf("after the read ended: %s", got)
	}
}

// With stream scope, unlogged staging from an attempt with pipeline scope is
// applied in one transaction: a stream that fails leaves the others
// unapplied, so a crash can't leave the epoch partly applied.
func TestUnloggedStagingAppliesWhole(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	id := []arrow.Field{idField}
	env.exec("CREATE SCHEMA $schema")
	env.exec("CREATE TABLE $schema.orders (id bigint CONSTRAINT big CHECK (id > 100))")
	w1 := env.open(1, destination.ScopePipeline, "users", "orders")
	recov(t, w1, 1, destination.Epoch{}, nil)
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w1, e1, "users", batch(t, id, `[{"id": 1}]`))
	write(t, w1, e1, "orders", batch(t, id, `[{"id": 2}]`))
	st := precommit(t, w1, e1)

	w2 := env.open(2, destination.ScopeStream, "users", "orders")
	if _, err := w2.Recover(ctx, 2, e1, []destination.Staged{st}); err == nil {
		t.Fatal("Recover applied a row its target refuses")
	}
	// The apply created users before it took the fence.
	if n := env.count("SELECT count(*) FROM $schema.users"); n != 0 || env.markers() != 0 {
		t.Fatalf("users was applied without orders: %d rows, %d markers", n, env.markers())
	}
	env.exec("ALTER TABLE $schema.orders DROP CONSTRAINT big")
	w3 := env.open(3, destination.ScopeStream, "users", "orders")
	if r := recov(t, w3, 3, e1, st); r != destination.Redone {
		t.Fatalf("Recover = %v, want redone", r)
	}
	if n := env.count("SELECT count(*) FROM $schema.users") + env.count("SELECT count(*) FROM $schema.orders"); n != 2 || env.markers() != 2 {
		t.Errorf("%d rows and %d markers, want 2 of each", n, env.markers())
	}
}
