//go:build integration

package postgres

import (
	"context"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

var (
	opField        = arrow.Field{Name: opColumn, Type: arrow.BinaryTypes.String}
	seqField       = arrow.Field{Name: seqColumn, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	deletedField   = arrow.Field{Name: deletedColumn, Type: arrow.FixedWidthTypes.Boolean}
	unchangedField = arrow.Field{Name: unchangedColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
	docField       = arrow.Field{Name: "doc", Type: arrow.BinaryTypes.String, Nullable: true}
)

func (e *env) exec(q string) {
	e.t.Helper()
	if _, err := e.conn.Exec(e.t.Context(), strings.ReplaceAll(q, "$schema", `"`+e.schema+`"`)); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) text(q string) string {
	e.t.Helper()
	var s *string
	if err := e.conn.QueryRow(e.t.Context(), strings.ReplaceAll(q, "$schema", `"`+e.schema+`"`)).Scan(&s); err != nil {
		e.t.Fatal(err)
	}
	if s == nil {
		return ""
	}
	return *s
}

func applySchema(t *testing.T, w destination.Writer, stream string, fields []arrow.Field) {
	t.Helper()
	if _, err := w.(destination.SchemaApplier).ApplySchema(t.Context(), []destination.StreamSchema{{Name: stream, Schema: arrow.NewSchema(fields, nil)}}); err != nil {
		t.Fatal(err)
	}
}

func write(t *testing.T, w destination.Writer, e destination.Epoch, stream string, b arrow.RecordBatch) {
	t.Helper()
	if err := w.Write(t.Context(), e, stream, b); err != nil {
		t.Fatal(err)
	}
}

func commit(t *testing.T, w destination.Writer, e destination.Epoch) (destination.Staged, destination.Committed) {
	t.Helper()
	st := precommit(t, w, e)
	c, err := w.Commit(t.Context(), e, st)
	if err != nil {
		t.Fatal(err)
	}
	return st, c
}

func startReads(t *testing.T, w destination.Writer, starts map[string]time.Time) destination.ReadTracker {
	t.Helper()
	rt := w.(destination.ReadTracker)
	if err := rt.StartReads(t.Context(), starts); err != nil {
		t.Fatal(err)
	}
	return rt
}

func endRead(t *testing.T, rt destination.ReadTracker, e destination.Epoch, stream string) {
	t.Helper()
	if err := rt.EndRead(t.Context(), e, stream); err != nil {
		t.Fatal(err)
	}
}

const users = "SELECT string_agg(id || ':' || coalesce(name, '-'), ',' ORDER BY id) FROM $schema.users"

// A column an update didn't send takes its value from the key's latest
// earlier version in the epoch that sent it, or else keeps the target's. The
// target's _openetl_unchanged lists the columns it never received.
func TestMergeKeepsUnchangedColumns(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, nameField, docField, opField, seqField, deletedField, unchangedField}
	const rows = `SELECT string_agg(id || ':' || name || ':' || coalesce(doc, '-') || ':' || coalesce(_openetl_unchanged::text, '-') || ':' || _openetl_deleted, ',' ORDER BY id) FROM $schema.users`
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e1, "users", batch(t, fields, `[
		{"id": 1, "name": "a", "doc": "big1", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
		{"id": 2, "name": "b", "_openetl_op": "update", "_openetl_seq": 1, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]},
		{"id": 1, "name": "a2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]},
		{"id": 3, "name": "c", "doc": null, "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
		{"id": 3, "name": "c2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]}]`))
	commit(t, w, e1)
	if got := env.text(rows); got != `1:a2:big1:-:false,2:b:-:["doc"]:false,3:c2:-:-:false` {
		t.Fatalf("after epoch 1: %s", got)
	}

	e2 := destination.Epoch{Generation: 1, N: 2}
	write(t, w, e2, "users", batch(t, fields, `[
		{"id": 1, "name": "a3", "_openetl_op": "update", "_openetl_seq": 3, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]},
		{"id": 2, "name": "b2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]},
		{"id": 3, "name": "c3", "doc": "big3", "_openetl_op": "update", "_openetl_seq": 3, "_openetl_deleted": false},
		{"id": 3, "name": "c4", "_openetl_op": "update", "_openetl_seq": 4, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]}]`))
	commit(t, w, e2)
	if got := env.text(rows); got != `1:a3:big1:-:false,2:b2:-:["doc"]:false,3:c4:big3:-:false` {
		t.Fatalf("after epoch 2: %s", got)
	}

	e3 := destination.Epoch{Generation: 1, N: 3}
	write(t, w, e3, "users", batch(t, fields, `[
		{"id": 1, "_openetl_op": "delete", "_openetl_seq": 4, "_openetl_deleted": true},
		{"id": 2, "name": "b3", "doc": "big2", "_openetl_op": "update", "_openetl_seq": 3, "_openetl_deleted": false}]`))
	commit(t, w, e3)
	if got := env.text(rows); got != `1:a3:big1:-:true,2:b3:big2:-:false,3:c4:big3:-:false` {
		t.Errorf("after epoch 3: %s", got)
	}
	if typ := env.text(`SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = '$schema.users'::regclass AND attname = '_openetl_unchanged'`); typ != "jsonb" {
		t.Errorf("_openetl_unchanged is %s", typ)
	}
}

// Each key keeps its latest version: within an epoch by _openetl_seq, across
// epochs unless the stored one is newer. A delete keeps the row's columns,
// marked deleted, and guards it against older versions.
func TestMerge(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, nameField, opField, seqField, deletedField}
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e1, "users", batch(t, fields, `[
		{"id": 1, "name": "a", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
		{"id": 2, "name": "b", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
		{"id": 1, "name": "a2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false},
		{"id": 3, "name": "c", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false}]`))
	if _, c := commit(t, w, e1); len(c.Collapsed) != 0 {
		t.Errorf("merge counted %v as collapsed", c.Collapsed)
	}
	if got := env.text(users); got != "1:a2,2:b,3:c" {
		t.Fatalf("after epoch 1: %s", got)
	}

	e2 := destination.Epoch{Generation: 1, N: 2}
	write(t, w, e2, "users", batch(t, fields, `[
		{"id": 2, "name": "stale", "_openetl_op": "update", "_openetl_seq": 0, "_openetl_deleted": false},
		{"id": 1, "name": "a3", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false},
		{"id": 3, "_openetl_op": "delete", "_openetl_seq": 2, "_openetl_deleted": true},
		{"id": 4, "_openetl_op": "delete", "_openetl_seq": 1, "_openetl_deleted": true}]`))
	commit(t, w, e2)
	e3 := destination.Epoch{Generation: 1, N: 3}
	write(t, w, e3, "users", batch(t, fields, `[{"id": 3, "name": "c-old", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false}]`))
	commit(t, w, e3)
	// A tie on _openetl_seq updates; an older version doesn't.
	if got := env.text(users); got != "1:a3,2:b,3:c,4:-" {
		t.Errorf("after epochs 2 and 3: %s", got)
	}
	if got := env.text("SELECT string_agg(id || ':' || _openetl_op || ':' || _openetl_seq, ',' ORDER BY id) FROM $schema.users WHERE _openetl_deleted"); got != "3:delete:2,4:delete:1" {
		t.Errorf("deleted rows: %s", got)
	}
	if n := env.count("SELECT count(*) FROM pg_indexes WHERE schemaname = '" + env.schema + "' AND tablename = 'users' AND indexdef LIKE 'CREATE UNIQUE INDEX%(id)'"); n != 1 {
		t.Errorf("users has %d unique indexes on id, want 1", n)
	}
}

func TestMergeHardDeletes(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopeStream,
		destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}, Deletes: destination.HardDelete})
	recov(t, w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, nameField, opField, seqField}
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e1, "users", batch(t, fields, `[
		{"id": 1, "name": "a", "_openetl_op": "insert", "_openetl_seq": 1},
		{"id": 2, "name": "b", "_openetl_op": "insert", "_openetl_seq": 1},
		{"id": 3, "name": "c", "_openetl_op": "insert", "_openetl_seq": 5},
		{"id": 5, "name": "e", "_openetl_op": "insert", "_openetl_seq": 1}]`))
	commit(t, w, e1)
	e2 := destination.Epoch{Generation: 1, N: 2}
	write(t, w, e2, "users", batch(t, fields, `[
		{"id": 1, "_openetl_op": "delete", "_openetl_seq": 2},
		{"id": 2, "name": "b2", "_openetl_op": "update", "_openetl_seq": 2},
		{"id": 2, "_openetl_op": "delete", "_openetl_seq": 3},
		{"id": 3, "_openetl_op": "delete", "_openetl_seq": 4},
		{"id": 4, "_openetl_op": "delete", "_openetl_seq": 1},
		{"id": 5, "_openetl_op": "delete", "_openetl_seq": 2},
		{"id": 5, "name": "e2", "_openetl_op": "insert", "_openetl_seq": 3}]`))
	commit(t, w, e2)
	// 3's stored version is newer than its delete; 5 came back after its own.
	if got := env.text(users); got != "3:c,5:e2" {
		t.Errorf("users: %s", got)
	}
	if n := env.count("SELECT count(*) FROM information_schema.columns WHERE table_schema = '" + env.schema + "' AND column_name = '" + deletedColumn + "'"); n != 0 {
		t.Errorf("hard deletes added %s", deletedColumn)
	}
}

// The catalog names a key by its source names; the engine renames columns.
func TestMergeOnARenamedKey(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"User ID"}})
	recov(t, w, 1, destination.Epoch{}, nil)
	md := arrow.NewMetadata([]string{sourceNamesKey}, []string{`{"user_id": "User ID"}`})
	schema := arrow.NewSchema([]arrow.Field{{Name: "user_id", Type: arrow.PrimitiveTypes.Int64}, nameField, seqField}, &md)
	b, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[
		{"user_id": 1, "name": "a", "_openetl_seq": 1}, {"user_id": 1, "name": "b", "_openetl_seq": 2}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	e := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e, "users", b)
	commit(t, w, e)
	if got := env.text("SELECT string_agg(user_id || ':' || name, ',') FROM $schema.users"); got != "1:b" {
		t.Errorf("users: %s", got)
	}

	w2 := env.openStreams(2, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w2, 2, destination.Epoch{}, nil)
	if err := w2.Write(ctx, destination.Epoch{Generation: 2, N: 1}, "users", b); err == nil || !strings.Contains(err.Error(), `merges on "id"`) {
		t.Errorf("Write without the key's column: %v", err)
	}
}

// A table with a unique index on the key keeps it; one with repeats can't
// be merged into.
func TestMergeIntoAnExistingTable(t *testing.T) {
	env := newEnv(t)
	env.exec("CREATE SCHEMA $schema")
	env.exec("CREATE TABLE $schema.users (id bigint PRIMARY KEY, name text)")
	env.exec("CREATE TABLE $schema.logs (id bigint, name text)")
	env.exec("INSERT INTO $schema.logs VALUES (1, 'a'), (1, 'b')")
	w := env.openStreams(1, destination.ScopeStream,
		destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}},
		destination.Stream{Name: "logs", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e, "users", batch(t, []arrow.Field{idField, nameField}, `[{"id": 1, "name": "a"}]`))
	commit(t, w, e)
	if n := env.count("SELECT count(*) FROM pg_indexes WHERE schemaname = '" + env.schema + "' AND tablename = 'users'"); n != 1 {
		t.Errorf("users has %d indexes, want only its primary key", n)
	}
	e2 := destination.Epoch{Generation: 1, N: 2}
	write(t, w, e2, "logs", batch(t, []arrow.Field{idField, nameField}, `[{"id": 1, "name": "c"}]`))
	if _, err := w.Commit(t.Context(), e2, precommit(t, w, e2)); err == nil || !strings.Contains(err.Error(), "needs one row per primary key") {
		t.Errorf("merging into a table with repeated keys: %v", err)
	}
	if n := env.count("SELECT count(*) FROM pg_indexes WHERE schemaname = '" + env.schema + "' AND tablename = 'logs'"); n != 0 {
		t.Errorf("the failed build left %d indexes on logs", n)
	}
}

// The target keeps its rows until the read ends, then has exactly the
// read's, in the same table, so views and indexes on it stay.
func TestOverwrite(t *testing.T) {
	env := newEnv(t)
	env.exec("CREATE SCHEMA $schema")
	env.exec("CREATE TABLE $schema.users (id bigint, name text)")
	env.exec("INSERT INTO $schema.users VALUES (9, 'old')")
	env.exec("CREATE VIEW $schema.names AS SELECT name FROM $schema.users")
	env.exec("CREATE INDEX users_name ON $schema.users (name)")
	stream := destination.Stream{Name: "users", Mode: destination.Overwrite}
	w := env.openStreams(1, destination.ScopePipeline, stream)
	recov(t, w, 1, destination.Epoch{}, nil)
	rt := startReads(t, w, map[string]time.Time{"users": time.UnixMicro(1000)})
	fields := []arrow.Field{idField, nameField}
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, w, e1, "users", batch(t, fields, `[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]`))
	commit(t, w, e1)
	if got := env.text(users); got != "9:old" {
		t.Errorf("before the read ended: %s", got)
	}
	read := readName("p1", "users", time.UnixMicro(1000))
	if n := env.count("SELECT count(*) FROM $schema." + read); n != 2 {
		t.Errorf("the read's table has %d rows, want 2", n)
	}
	write(t, w, e2, "users", batch(t, fields, `[{"id": 3, "name": "c"}]`))
	endRead(t, rt, e2, "users")
	st2, _ := commit(t, w, e2)
	if _, err := w.Commit(t.Context(), e2, st2); err != nil {
		t.Fatal(err)
	}
	if got := env.text(users); got != "1:a,2:b,3:c" {
		t.Errorf("after the read ended: %s", got)
	}
	if got := env.text("SELECT string_agg(name, ',' ORDER BY name) FROM $schema.names"); got != "a,b,c" {
		t.Errorf("the view: %s", got)
	}
	if n := env.count("SELECT count(*) FROM pg_indexes WHERE schemaname = '" + env.schema + "' AND indexname = 'users_name'"); n != 1 {
		t.Error("the table's index is gone")
	}
	if n := env.count("SELECT count(*) FROM pg_tables WHERE schemaname = '" + env.schema + "' AND tablename LIKE '\\_openetl\\_read\\_%'"); n != 0 {
		t.Errorf("%d read tables left", n)
	}

	// The next read has no rows, in the next attempt: the table empties.
	w2 := env.openStreams(2, destination.ScopePipeline, stream)
	applySchema(t, w2, "users", fields)
	if r := recov(t, w2, 2, e2, st2); r != destination.Applied {
		t.Fatalf("Recover = %v, want applied", r)
	}
	rt2 := startReads(t, w2, map[string]time.Time{"users": time.UnixMicro(2000)})
	e3 := destination.Epoch{Generation: 2, N: 3}
	endRead(t, rt2, e3, "users")
	commit(t, w2, e3)
	if n := env.count("SELECT count(*) FROM $schema.users"); n != 0 {
		t.Errorf("after an empty read, users has %d rows", n)
	}
}

// A new generation redoes the epoch that ends a read, rows or not, and
// replaces the table once.
func TestOverwriteRedo(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Overwrite}
	fields := []arrow.Field{idField, nameField}
	w1 := env.openStreams(1, destination.ScopeStream, stream)
	recov(t, w1, 1, destination.Epoch{}, nil)
	rt := startReads(t, w1, map[string]time.Time{"users": time.UnixMicro(1000)})
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w1, e1, "users", batch(t, fields, `[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]`))
	endRead(t, rt, e1, "users")
	st1 := precommit(t, w1, e1)
	// The attempt dies before Commit.

	// As the runner does, it applies the restored schemas before Recover,
	// which is how a writer knows the table of a stream it writes nothing to.
	w2 := env.openStreams(2, destination.ScopeStream, stream)
	applySchema(t, w2, "users", fields)
	if r := recov(t, w2, 2, e1, st1); r != destination.Redone {
		t.Fatalf("Recover = %v, want redone", r)
	}
	if r := recov(t, w2, 2, e1, st1); r != destination.Applied {
		t.Fatalf("second Recover = %v, want applied", r)
	}
	if got := env.text(users); got != "1:a,2:b" {
		t.Fatalf("after the redo: %s", got)
	}

	rt2 := startReads(t, w2, map[string]time.Time{"users": time.UnixMicro(2000)})
	e2 := destination.Epoch{Generation: 2, N: 2}
	endRead(t, rt2, e2, "users")
	st2 := precommit(t, w2, e2)
	w3 := env.openStreams(3, destination.ScopeStream, stream)
	applySchema(t, w3, "users", fields)
	if r := recov(t, w3, 3, e2, st2); r != destination.Redone {
		t.Fatalf("Recover of an empty read's end = %v, want redone", r)
	}
	if n := env.count("SELECT count(*) FROM $schema.users"); n != 0 {
		t.Errorf("after redoing an empty read's end, users has %d rows", n)
	}
}

// A read that starts again drops what the abandoned one applied, and a
// stream that stops overwriting loses its read's table.
func TestOverwriteDropsAbandonedReads(t *testing.T) {
	env := newEnv(t)
	fields := []arrow.Field{idField}
	overwrite := destination.Stream{Name: "users", Mode: destination.Overwrite}
	w1 := env.openStreams(1, destination.ScopePipeline, overwrite)
	recov(t, w1, 1, destination.Epoch{}, nil)
	startReads(t, w1, map[string]time.Time{"users": time.UnixMicro(1000)})
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w1, e1, "users", batch(t, fields, `[{"id": 1}]`))
	st1, _ := commit(t, w1, e1)
	first, second := readName("p1", "users", time.UnixMicro(1000)), readName("p1", "users", time.UnixMicro(2000))
	exists := func(table string) bool {
		return env.count("SELECT count(*) FROM pg_tables WHERE schemaname = '"+env.schema+"' AND tablename = '"+table+"'") == 1
	}

	w2 := env.openStreams(2, destination.ScopePipeline, overwrite)
	recov(t, w2, 2, e1, st1)
	if !exists(first) {
		t.Fatal("Recover dropped the read's table")
	}
	startReads(t, w2, map[string]time.Time{"users": time.UnixMicro(2000)})
	e2 := destination.Epoch{Generation: 2, N: 2}
	write(t, w2, e2, "users", batch(t, fields, `[{"id": 2}]`))
	st2, _ := commit(t, w2, e2)
	if exists(first) || !exists(second) {
		t.Errorf("after a new read: the abandoned one's table exists: %v; the new one's: %v", exists(first), exists(second))
	}

	w3 := env.openStreams(3, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Append})
	recov(t, w3, 3, e2, st2)
	if exists(second) {
		t.Error("the read's table is left after the stream stopped overwriting")
	}
}

// A read dedupes against its own rows, not the table's from the last read.
func TestOverwriteDedupesAgainstTheRead(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "events", Mode: destination.Overwrite, Dedupe: destination.DedupeTarget})
	recov(t, w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, rowID}
	rt := startReads(t, w, map[string]time.Time{"events": time.UnixMicro(1000)})
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e1, "events", batch(t, fields, `[{"id": 1, "_openetl_id": `+uid(1)+`}, {"id": 2, "_openetl_id": `+uid(2)+`}]`))
	endRead(t, rt, e1, "events")
	commit(t, w, e1)

	rt = startReads(t, w, map[string]time.Time{"events": time.UnixMicro(2000)})
	e2, e3 := destination.Epoch{Generation: 1, N: 2}, destination.Epoch{Generation: 1, N: 3}
	write(t, w, e2, "events", batch(t, fields, `[{"id": 1, "_openetl_id": `+uid(1)+`}, {"id": 3, "_openetl_id": `+uid(3)+`}]`))
	if _, c := commit(t, w, e2); len(c.Collapsed) != 0 {
		t.Errorf("epoch 2 collapsed %v; the last read's rows aren't this one's", c.Collapsed)
	}
	write(t, w, e3, "events", batch(t, fields, `[{"id": 3, "_openetl_id": `+uid(3)+`}, {"id": 4, "_openetl_id": `+uid(4)+`}]`))
	endRead(t, rt, e3, "events")
	if _, c := commit(t, w, e3); !maps.Equal(c.Collapsed, map[string]uint64{"events": 1}) {
		t.Errorf("epoch 3 collapsed %v, want the repeat of 3", c.Collapsed)
	}
	if got := env.text("SELECT string_agg(id::text, ',' ORDER BY id) FROM $schema.events"); got != "1,3,4" {
		t.Errorf("events: %s", got)
	}
}

func TestOverwriteNeedsReads(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Overwrite})
	recov(t, w, 1, destination.Epoch{}, nil)
	err := w.Write(context.Background(), destination.Epoch{Generation: 1, N: 1}, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`))
	if err == nil || !strings.Contains(err.Error(), "no start") {
		t.Errorf("Write before StartReads: %v", err)
	}
}
