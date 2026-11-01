//go:build integration

package postgres

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

// Runs against the compose Postgres (make deps-up) or
// OPENETL_TEST_POSTGRES_DSN.
func dsn() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_POSTGRES_DSN"), "postgres://openetl:openetl@localhost:5432/openetl")
}

type env struct {
	t      *testing.T
	d      *Destination
	conn   *pgx.Conn
	schema string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	schema := "t_" + strings.ToLower(rand.Text())[:10]
	cfg, _ := json.Marshal(map[string]string{"dsn": dsn(), "schema": schema})
	d, err := NewDestination(cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(t.Context(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		_ = conn.Close(ctx)
	})
	return &env{t: t, d: d, conn: conn, schema: schema}
}

func (e *env) open(gen uint64, scope destination.Scope, streams ...string) destination.Writer {
	e.t.Helper()
	var cfg []destination.Stream
	for _, s := range streams {
		cfg = append(cfg, destination.Stream{Name: s, Mode: destination.Append})
	}
	return e.openStreams(gen, scope, cfg...)
}

func (e *env) openStreams(gen uint64, scope destination.Scope, streams ...destination.Stream) destination.Writer {
	e.t.Helper()
	cfg := destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: scope, Streams: streams}
	w, err := e.d.Open(e.t.Context(), cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

func (e *env) count(q string) int {
	e.t.Helper()
	var n int
	if err := e.conn.QueryRow(e.t.Context(), strings.ReplaceAll(q, "$schema", pgx.Identifier{e.schema}.Sanitize())).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func batch(t *testing.T, fields []arrow.Field, rows string) arrow.RecordBatch {
	t.Helper()
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema(fields, nil), strings.NewReader(rows))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rec.Release)
	return rec
}

var (
	idField   = arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	nameField = arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}
	tagsField = arrow.Field{Name: "tags", Type: arrow.BinaryTypes.String, Nullable: true,
		Metadata: arrow.NewMetadata([]string{extensionNameKey}, []string{jsonExtension})}
)

func recov(t *testing.T, w destination.Writer, gen uint64, e destination.Epoch, st destination.Staged) destination.Recovery {
	t.Helper()
	r, err := w.Recover(t.Context(), gen, e, st)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func precommit(t *testing.T, w destination.Writer, e destination.Epoch) destination.Staged {
	t.Helper()
	st, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestCommitAppliesOnce(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.open(1, destination.ScopePipeline, "users")
	if r := recov(t, w, 1, destination.Epoch{}, nil); r != destination.Applied {
		t.Fatalf("Recover = %v", r)
	}
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}, {"id": 2}]`)); err != nil {
		t.Fatal(err)
	}
	// A column appears mid-epoch.
	if err := w.Write(ctx, e, "users", batch(t, []arrow.Field{idField, nameField, tagsField}, `[{"id": 3, "name": "c", "tags": "[\"x\"]"}]`)); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w, e)
	if n := env.count("SELECT count(*) FROM pg_tables WHERE schemaname = '" + env.schema + "' AND tablename = 'users'"); n != 0 {
		t.Fatal("PreCommit made the target visible")
	}
	for range 2 {
		if err := w.Commit(ctx, e, st); err != nil {
			t.Fatal(err)
		}
	}
	if n := env.count("SELECT count(*) FROM $schema.users"); n != 3 {
		t.Errorf("users has %d rows, want 3", n)
	}
	if n := env.count("SELECT count(*) FROM $schema.users WHERE tags @> '[\"x\"]'::jsonb"); n != 1 {
		t.Errorf("tags isn't jsonb with the row's value: %d matches", n)
	}
	if n := env.count("SELECT count(*) FROM $schema._openetl_loads WHERE pipeline = 'p1' AND stream = 'users' AND generation = 1 AND epoch = 1"); n != 1 {
		t.Errorf("%d markers, want 1", n)
	}
	if n := env.count("SELECT count(*) FROM pg_tables WHERE schemaname = '" + env.schema + "' AND tablename LIKE '\\_openetl\\_stage\\_%'"); n != 0 {
		t.Errorf("%d staging tables left", n)
	}
}

func TestRecoverRedoesAStagedEpoch(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w1 := env.open(1, destination.ScopePipeline, "users", "orders")
	recov(t, w1, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w1.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
		t.Fatal(err)
	}
	if err := w1.Write(ctx, e, "orders", batch(t, []arrow.Field{idField}, `[{"id": 10}, {"id": 11}]`)); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w1, e)
	// The attempt dies here, after its checkpoint record but before Commit.

	w2 := env.open(2, destination.ScopePipeline, "users", "orders")
	if r := recov(t, w2, 2, e, st); r != destination.Redone {
		t.Fatalf("Recover = %v, want redone", r)
	}
	w3 := env.open(3, destination.ScopePipeline, "users", "orders")
	if r := recov(t, w3, 3, e, st); r != destination.Applied {
		t.Fatalf("second Recover = %v, want applied", r)
	}
	if n := env.count("SELECT count(*) FROM $schema.users") + env.count("SELECT count(*) FROM $schema.orders"); n != 3 {
		t.Errorf("%d rows, want 3", n)
	}
}

func TestRecoverReportsLost(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w1 := env.open(1, destination.ScopePipeline, "users")
	recov(t, w1, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w1.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w1, e)
	if _, err := env.conn.Exec(ctx, "DROP TABLE "+pgx.Identifier{env.schema, stagingName("p1", "users", e)}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	w2 := env.open(2, destination.ScopePipeline, "users")
	if r := recov(t, w2, 2, e, st); r != destination.Lost {
		t.Fatalf("Recover = %v, want lost", r)
	}
}

// Recovering an applied epoch deletes the pipeline's markers below it; a lost
// one keeps the previous epoch's, which the next recovery checks.
func TestRecoverTrimsMarkers(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	commit := func(w destination.Writer, e destination.Epoch) destination.Staged {
		if err := w.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
			t.Fatal(err)
		}
		st := precommit(t, w, e)
		if err := w.Commit(ctx, e, st); err != nil {
			t.Fatal(err)
		}
		return st
	}
	markers := func(pipeline string) string {
		var s string
		if err := env.conn.QueryRow(ctx, "SELECT coalesce(string_agg(generation || '-' || epoch, ' ' ORDER BY epoch), '') FROM "+
			pgx.Identifier{env.schema, "_openetl_loads"}.Sanitize()+" WHERE pipeline = $1", pipeline).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	other, err := env.d.Open(ctx, destination.OpenConfig{Pipeline: "p2", Generation: 1, Scope: destination.ScopePipeline,
		Streams: []destination.Stream{{Name: "users", Mode: destination.Append}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close(context.Background()) })
	recov(t, other, 1, destination.Epoch{}, nil)
	commit(other, destination.Epoch{Generation: 1, N: 1})

	w1 := env.open(1, destination.ScopePipeline, "users")
	recov(t, w1, 1, destination.Epoch{}, nil)
	var head destination.Staged
	for n := uint64(1); n <= 3; n++ {
		head = commit(w1, destination.Epoch{Generation: 1, N: n})
	}
	w2 := env.open(2, destination.ScopePipeline, "users")
	if r := recov(t, w2, 2, destination.Epoch{Generation: 1, N: 3}, head); r != destination.Applied {
		t.Fatalf("Recover = %v, want applied", r)
	}
	if got := markers("p1"); got != "1-3" {
		t.Errorf("markers after recovering 3: %q, want 1-3", got)
	}

	lost := destination.Epoch{Generation: 2, N: 4}
	if err := w2.Write(ctx, lost, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w2, lost)
	if _, err := env.conn.Exec(ctx, "DROP TABLE "+pgx.Identifier{env.schema, stagingName("p1", "users", lost)}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	w3 := env.open(3, destination.ScopePipeline, "users")
	if r := recov(t, w3, 3, lost, st); r != destination.Lost {
		t.Fatalf("Recover = %v, want lost", r)
	}
	if r := recov(t, w3, 3, destination.Epoch{Generation: 1, N: 3}, head); r != destination.Applied {
		t.Fatalf("recovering the previous epoch after a lost one: %v, want applied", r)
	}
	if got := markers("p2"); got != "1-1" {
		t.Errorf("the other pipeline's markers: %q, want 1-1", got)
	}
}

func TestStaleWriterIsFenced(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w1 := env.open(1, destination.ScopeStream, "users")
	recov(t, w1, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w1.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w1, e)

	w2 := env.open(2, destination.ScopeStream, "users")
	if r := recov(t, w2, 2, destination.Epoch{}, nil); r != destination.Applied {
		t.Fatalf("Recover = %v", r)
	}
	if err := w1.Commit(ctx, e, st); !errors.Is(err, destination.ErrFenced) {
		t.Fatalf("stale Commit: %v, want fenced", err)
	}
	if _, err := w1.Recover(ctx, 1, destination.Epoch{}, nil); !errors.Is(err, destination.ErrFenced) {
		t.Fatalf("stale Recover: %v, want fenced", err)
	}
	if n := env.count("SELECT count(*) FROM $schema._openetl_loads"); n != 0 {
		t.Errorf("a fenced commit wrote %d markers", n)
	}
}

// Recovering an epoch drops what earlier generations of the pipeline
// staged, and nothing else.
func TestRecoverDropsStaleStaging(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	rows := batch(t, []arrow.Field{idField}, `[{"id": 1}]`)
	const pipeline = `p'1\`
	open := func(pipeline string, gen uint64) destination.Writer {
		w, err := env.d.Open(ctx, destination.OpenConfig{Pipeline: pipeline, Generation: gen, Scope: destination.ScopePipeline,
			Streams: []destination.Stream{{Name: "users", Mode: destination.Append}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Close(context.Background()) })
		return w
	}
	stage := func(w destination.Writer, pipeline string, gen, n uint64) string {
		e := destination.Epoch{Generation: gen, N: n}
		if err := w.Write(ctx, e, "users", rows); err != nil {
			t.Fatal(err)
		}
		return pgx.Identifier{env.schema, stagingName(pipeline, "users", e)}.Sanitize()
	}
	exists := func(table string) bool {
		var ok bool
		if err := env.conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	w1, w2 := open(pipeline, 1), open("p2", 1)
	recov(t, w1, 1, destination.Epoch{}, nil)
	recov(t, w2, 1, destination.Epoch{}, nil)
	head := destination.Epoch{Generation: 1, N: 1}
	if err := w1.Write(ctx, head, "users", rows); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w1, head)
	if err := w1.Commit(ctx, head, st); err != nil {
		t.Fatal(err)
	}
	stale, other := stage(w1, pipeline, 1, 2), stage(w2, "p2", 1, 1)
	staleSt := precommit(t, w1, destination.Epoch{Generation: 1, N: 2})
	next := open(pipeline, 2)
	current := stage(next, pipeline, 2, 2)

	recov(t, next, 2, destination.Epoch{}, nil)
	if !exists(stale) {
		t.Fatal("recovering no epoch dropped staging")
	}
	if r := recov(t, next, 2, head, st); r != destination.Applied {
		t.Fatalf("Recover = %v, want applied", r)
	}
	if exists(stale) {
		t.Error("the earlier generation's staging is left")
	}
	if !exists(other) || !exists(current) {
		t.Error("Recover dropped another pipeline's staging, or its own generation's")
	}
	if err := w1.Commit(ctx, destination.Epoch{Generation: 1, N: 2}, staleSt); !errors.Is(err, destination.ErrFenced) {
		t.Errorf("committing dropped staging: %v, want fenced", err)
	}
}

func TestBatchWithoutColumns(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.open(1, destination.ScopePipeline, "empty")
	recov(t, w, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	rec := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 3)
	defer rec.Release()
	if err := w.Write(ctx, e, "empty", rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(ctx, e, "empty", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(ctx, e, precommit(t, w, e)); err != nil {
		t.Fatal(err)
	}
	if n := env.count("SELECT count(*) FROM $schema.empty"); n != 4 {
		t.Errorf("%d rows, want 4", n)
	}
}

func TestOnlyAppend(t *testing.T) {
	env := newEnv(t)
	_, err := env.d.Open(t.Context(), destination.OpenConfig{Pipeline: "p1", Generation: 1, Scope: destination.ScopePipeline,
		Streams: []destination.Stream{{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}}}})
	if err == nil {
		t.Fatal("Open accepted a merge stream")
	}
}

func TestColumnsWiden(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.open(1, destination.ScopePipeline, "nums")
	recov(t, w, 1, destination.Epoch{}, nil)
	ints := []arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}
	floats := []arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	if err := w.Write(ctx, e1, "nums", batch(t, ints, `[{"x": 1}]`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(ctx, e1, precommit(t, w, e1)); err != nil {
		t.Fatal(err)
	}
	// Widened within epoch 2, and across epochs.
	if err := w.Write(ctx, e2, "nums", batch(t, ints, `[{"x": 2}]`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(ctx, e2, "nums", batch(t, floats, `[{"x": 2.5}]`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(ctx, e2, precommit(t, w, e2)); err != nil {
		t.Fatal(err)
	}
	typ := func() string {
		var typ string
		err := env.conn.QueryRow(ctx, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = $1::regclass AND attname = 'x'",
			env.schema+".nums").Scan(&typ)
		if err != nil {
			t.Fatal(err)
		}
		return typ
	}
	if got := typ(); got != "double precision" {
		t.Errorf("x is %s", got)
	}
	if n := env.count("SELECT count(*) FROM $schema.nums WHERE x IN (1, 2, 2.5)"); n != 3 {
		var got string
		_ = env.conn.QueryRow(ctx, "SELECT string_agg(coalesce(x::text, 'null'), ',') FROM "+env.schema+".nums").Scan(&got)
		t.Errorf("%d rows kept their values: %s", n, got)
	}

	sa := w.(destination.SchemaApplier)
	table := func(name string) *arrow.Metadata {
		md := arrow.NewMetadata([]string{tableKey}, []string{name})
		return &md
	}
	text := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.BinaryTypes.String, Nullable: true}, nameField}, table("nums"))
	for range 2 {
		if err := sa.ApplySchema(ctx, []destination.StreamSchema{{Name: "nums", Schema: text}}); err != nil {
			t.Fatal(err)
		}
	}
	narrower := arrow.NewSchema(ints, table("nums"))
	if err := sa.ApplySchema(ctx, []destination.StreamSchema{{Name: "nums", Schema: narrower}}); err != nil {
		t.Fatal(err)
	}
	if got := typ(); got != "text" {
		t.Errorf("x is %s after ApplySchema", got)
	}
	if n := env.count("SELECT count(*) FROM $schema.nums WHERE name IS NULL AND x = '2.5'"); n != 1 {
		t.Errorf("name isn't a new column, or x lost its value")
	}
	flags := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.FixedWidthTypes.Boolean, Nullable: true}}, table("other"))
	if err := sa.ApplySchema(ctx, []destination.StreamSchema{{Name: "other", Schema: flags}}); err != nil {
		t.Fatal(err)
	}
	ints2 := arrow.NewSchema(ints, table("other"))
	if err := sa.ApplySchema(ctx, []destination.StreamSchema{{Name: "other", Schema: ints2}}); err == nil {
		t.Error("boolean became bigint")
	}
}

var rowID = arrow.Field{Name: idColumn, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16},
	Metadata: arrow.NewMetadata([]string{extensionNameKey}, []string{uuidExtension})}

// uid is a JSON _openetl_id whose bytes are all n.
func uid(n byte) string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = n
	}
	return `"` + base64.StdEncoding.EncodeToString(b) + `"`
}

func TestDedupeAtApply(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.openStreams(1, destination.ScopePipeline,
		destination.Stream{Name: "events", Mode: destination.Append, Dedupe: true},
		destination.Stream{Name: "logs", Mode: destination.Append})
	recov(t, w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, rowID}
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	// Repeats within an epoch, and of an earlier epoch's rows.
	rows1 := `[{"id": 1, "_openetl_id": ` + uid(1) + `}, {"id": 2, "_openetl_id": ` + uid(2) + `}, {"id": 2, "_openetl_id": ` + uid(2) + `}]`
	rows2 := `[{"id": 1, "_openetl_id": ` + uid(1) + `}, {"id": 3, "_openetl_id": ` + uid(3) + `}]`
	for _, stream := range []string{"events", "logs"} {
		if err := w.Write(ctx, e1, stream, batch(t, fields, rows1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Commit(ctx, e1, precommit(t, w, e1)); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{"events", "logs"} {
		if err := w.Write(ctx, e2, stream, batch(t, fields, rows2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Commit(ctx, e2, precommit(t, w, e2)); err != nil {
		t.Fatal(err)
	}
	if n := env.count("SELECT count(*) FROM $schema.events"); n != 3 {
		t.Errorf("events has %d rows, want 3", n)
	}
	if n := env.count("SELECT count(*) FROM $schema.logs"); n != 5 {
		t.Errorf("logs, which isn't deduped, has %d rows, want 5", n)
	}
	if n := env.count("SELECT count(*) FROM pg_indexes WHERE schemaname = '" + env.schema + "' AND tablename = 'events' AND indexdef LIKE '%(_openetl_id)'"); n != 1 {
		t.Errorf("events has %d indexes on _openetl_id, want 1", n)
	}
	if n := env.count("SELECT count(*) FROM pg_indexes WHERE schemaname = '" + env.schema + "' AND tablename = 'logs'"); n != 0 {
		t.Errorf("logs has %d indexes, want none", n)
	}
}

func TestRedoDedupesAsStaged(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w1 := env.openStreams(1, destination.ScopeStream, destination.Stream{Name: "events", Mode: destination.Append, Dedupe: true})
	recov(t, w1, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	rows := `[{"id": 1, "_openetl_id": ` + uid(1) + `}, {"id": 1, "_openetl_id": ` + uid(1) + `}]`
	if err := w1.Write(ctx, e, "events", batch(t, []arrow.Field{idField, rowID}, rows)); err != nil {
		t.Fatal(err)
	}
	st := precommit(t, w1, e)

	// The next attempt doesn't dedupe, but redoes the epoch as it was staged.
	w2 := env.open(2, destination.ScopeStream, "events")
	if r := recov(t, w2, 2, e, st); r != destination.Redone {
		t.Fatalf("Recover = %v, want redone", r)
	}
	if n := env.count("SELECT count(*) FROM $schema.events"); n != 1 {
		t.Errorf("events has %d rows, want 1", n)
	}
}

func TestDedupeNeedsRowIDs(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "events", Mode: destination.Append, Dedupe: true})
	recov(t, w, 1, destination.Epoch{}, nil)
	err := w.Write(t.Context(), destination.Epoch{Generation: 1, N: 1}, "events", batch(t, []arrow.Field{idField}, `[{"id": 1}]`))
	if err == nil || !strings.Contains(err.Error(), "no _openetl_id") {
		t.Fatalf("Write = %v, want it to need _openetl_id", err)
	}
}
