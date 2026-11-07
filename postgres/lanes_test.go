//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// openLanes opens n lanes of an attempt, lane 0 recovered.
func (e *env) openLanes(gen uint64, n int, epoch destination.Epoch, staged []destination.Staged, streams ...destination.Stream) []destination.Writer {
	e.t.Helper()
	out := make([]destination.Writer, n)
	for lane := range out {
		cfg := destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: destination.ScopePipeline, Streams: streams, Lane: lane}
		w, err := e.d.Open(e.t.Context(), cfg)
		if err != nil {
			e.t.Fatal(err)
		}
		e.t.Cleanup(func() { _ = w.Close(context.Background()) })
		out[lane] = w
		if lane == 0 {
			if _, err := w.Recover(e.t.Context(), gen, epoch, staged); err != nil {
				e.t.Fatal(err)
			}
		}
	}
	return out
}

func precommitAll(t *testing.T, e destination.Epoch, ws ...destination.Writer) []destination.Staged {
	t.Helper()
	out := make([]destination.Staged, len(ws))
	for i, w := range ws {
		out[i] = precommit(t, w, e)
	}
	return out
}

func TestOnlyLaneZeroHasRoomToCommit(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Append}
	ws := env.openLanes(1, 3, destination.Epoch{}, nil, stream)
	if got := ws[0].(*writer).pool.Config().MaxConns; got != 4 {
		t.Errorf("lane 0 has %d connections, want 4", got)
	}
	if got := ws[2].(*writer).pool.Config().MaxConns; got != 1 {
		t.Errorf("lane 2 has %d connections, want 1", got)
	}
}

// One row per _openetl_id across the epoch, not per lane, and every lane's
// rows count towards those it skipped.
func TestDedupeAcrossLanes(t *testing.T) {
	fields := []arrow.Field{idField, rowID, nameField}
	r := func(id int, n byte, name string) string {
		return fmt.Sprintf(`{"id": %d, "_openetl_id": %s, "name": %q}`, id, uid(n), name)
	}
	for _, c := range []struct {
		name  string
		mode  destination.Dedupe
		later string
	}{
		{"target", destination.DedupeTarget, "1:a,2:b,3:c,4:d"},
		{"epoch", destination.DedupeEpoch, "1:a,1:a,2:b,3:c,4:d"},
	} {
		t.Run(c.name, func(t *testing.T) {
			env := newEnv(t)
			stream := destination.Stream{Name: "users", Mode: destination.Append, Dedupe: c.mode}
			ws := env.openLanes(1, 3, destination.Epoch{}, nil, stream)
			e1 := destination.Epoch{Generation: 1, N: 1}
			write(t, ws[0], e1, "users", batch(t, fields, "["+r(1, 1, "a")+","+r(2, 2, "b")+"]"))
			write(t, ws[1], e1, "users", batch(t, fields, "["+r(1, 1, "a")+","+r(3, 3, "c")+"]"))
			write(t, ws[2], e1, "users", batch(t, fields, "["+r(3, 3, "c")+"]"))
			got, err := ws[0].Commit(t.Context(), e1, precommitAll(t, e1, ws...))
			if err != nil {
				t.Fatal(err)
			}
			if got.Collapsed["users"] != 2 {
				t.Errorf("collapsed %d, want 2", got.Collapsed["users"])
			}
			if rows := env.text(users); rows != "1:a,2:b,3:c" {
				t.Errorf("after the epoch: %s", rows)
			}
			e2 := destination.Epoch{Generation: 1, N: 2}
			write(t, ws[1], e2, "users", batch(t, fields, "["+r(1, 1, "a")+"]"))
			write(t, ws[2], e2, "users", batch(t, fields, "["+r(4, 4, "d")+"]"))
			if _, err := ws[0].Commit(t.Context(), e2, precommitAll(t, e2, ws...)); err != nil {
				t.Fatal(err)
			}
			if rows := env.text(users); rows != c.later {
				t.Errorf("after a later epoch: %s, want %s", rows, c.later)
			}
		})
	}
}

// Lanes' staging tables differ: one has a column another lacks, a narrower
// type, or another order, and the apply reads them as one.
func TestStagingDiffersAcrossLanes(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Append}
	ws := env.openLanes(1, 3, destination.Epoch{}, nil, stream)
	n32 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	n64 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, ws[0], e1, "users", batch(t, []arrow.Field{idField, n32}, `[{"id": 1, "n": 1}]`))
	write(t, ws[1], e1, "users", batch(t, []arrow.Field{nameField, n64, idField}, `[{"id": 2, "n": 5000000000, "name": "b"}]`))
	write(t, ws[2], e1, "users", batch(t, []arrow.Field{idField, deletedField}, `[{"id": 3, "_openetl_deleted": true}]`))
	st := precommitAll(t, e1, ws...)
	if _, err := ws[0].Commit(t.Context(), e1, st); err != nil {
		t.Fatal(err)
	}
	const rows = `SELECT string_agg(id || ':' || coalesce(name, '-') || ':' || coalesce(n::text, '-') || ':' || _openetl_deleted, ',' ORDER BY id) FROM $schema.users`
	if got := env.text(rows); got != "1:-:1:false,2:b:5000000000:false,3:-:-:true" {
		t.Errorf("rows: %s", got)
	}
	if got := env.text(`SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = '$schema.users'::regclass AND attname = 'n'`); got != "bigint" {
		t.Errorf("n is %s, want bigint", got)
	}
	if n := env.count("SELECT count(*) FROM pg_tables WHERE schemaname = '" + env.schema + "' AND tablename LIKE '\\_openetl\\_stage\\_%'"); n != 0 {
		t.Errorf("%d staging tables left", n)
	}
}

// A batch older than the lane's staging has fewer columns and narrower types,
// and takes the staging's, as a batch that widened it would have left it,
// whatever the types.
func TestOlderBatchAfterANewerOne(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Append}
	ws := env.openLanes(1, 2, destination.Epoch{}, nil, stream)
	x := func(dt arrow.DataType) arrow.Field { return arrow.Field{Name: "x", Type: dt, Nullable: true} }
	e1 := destination.Epoch{Generation: 1, N: 1}
	for _, b := range []arrow.RecordBatch{
		batch(t, []arrow.Field{idField, nameField, x(arrow.BinaryTypes.String)}, `[{"id": 1, "name": "a", "x": "text"}]`),
		batch(t, []arrow.Field{idField, x(arrow.PrimitiveTypes.Int32)}, `[{"id": 2, "x": 7}]`),
		batch(t, []arrow.Field{idField, x(arrow.FixedWidthTypes.Boolean)}, `[{"id": 3, "x": true}]`),
		batch(t, []arrow.Field{idField}, `[{"id": 4}]`),
	} {
		write(t, ws[0], e1, "users", b)
	}
	floats := []arrow.Field{idField, {Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}
	write(t, ws[1], e1, "users", batch(t, floats, `[{"id": 5, "y": 1.5}]`))
	write(t, ws[1], e1, "users", batch(t, []arrow.Field{idField, {Name: "y", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, `[{"id": 6, "y": 2}]`))
	st := precommitAll(t, e1, ws...)
	if _, err := ws[0].Commit(t.Context(), e1, st); err != nil {
		t.Fatal(err)
	}
	const rows = `SELECT string_agg(id || ':' || coalesce(name, '-') || ':' || coalesce(x, '-') || ':' || coalesce(y::text, '-'), ',' ORDER BY id) FROM $schema.users`
	if got := env.text(rows); got != "1:a:text:-,2:-:7:-,3:-:true:-,4:-:-:-,5:-:-:1.5,6:-:-:2" {
		t.Errorf("rows: %s", got)
	}
	if n := env.count("SELECT count(*) FROM pg_tables WHERE schemaname = '" + env.schema + "' AND tablename LIKE '%\\_older'"); n != 0 {
		t.Errorf("%d tables left from casting", n)
	}
}

// A merge reads every lane's staging as one: each key's versions, on its lane,
// resolve unchanged columns and holds as they do on one.
func TestMergeAcrossLanes(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}, Deletes: destination.HardDelete}
	ws := env.openLanes(1, 3, destination.Epoch{}, nil, stream)
	wide := []arrow.Field{idField, nameField, docField, opField, seqField, unchangedField}
	plain := []arrow.Field{idField, nameField, opField, seqField}
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, ws[0], e1, "users", batch(t, wide, `[
		{"id": 1, "name": "a", "doc": "big1", "_openetl_op": "insert", "_openetl_seq": 1},
		{"id": 1, "name": "a2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_unchanged": ["doc"]}]`))
	write(t, ws[1], e1, "users", batch(t, plain, `[
		{"id": 2, "name": "b", "_openetl_op": "insert", "_openetl_seq": 3},
		{"id": 2, "name": "b2", "_openetl_op": "update", "_openetl_seq": 4},
		{"id": 3, "name": "c", "_openetl_op": "insert", "_openetl_seq": 3}]`))
	write(t, ws[2], e1, "users", batch(t, plain, `[{"id": 3, "name": "c", "_openetl_op": "delete", "_openetl_seq": 4}]`))
	st := precommitAll(t, e1, ws...)
	if _, err := ws[0].Commit(t.Context(), e1, st); err != nil {
		t.Fatal(err)
	}
	const rows = `SELECT string_agg(id || ':' || name || ':' || coalesce(doc, '-'), ',' ORDER BY id) FROM $schema.users`
	if got := env.text(rows); got != "1:a2:big1,2:b2:-" {
		t.Errorf("rows: %s", got)
	}
	e2 := destination.Epoch{Generation: 1, N: 2}
	write(t, ws[2], e2, "users", batch(t, wide, `[{"id": 1, "name": "a3", "_openetl_op": "update", "_openetl_seq": 5, "_openetl_unchanged": ["doc"]}]`))
	write(t, ws[0], e2, "users", batch(t, plain, `[{"id": 2, "name": "old", "_openetl_op": "update", "_openetl_seq": 1}]`))
	st = precommitAll(t, e2, ws...)
	if _, err := ws[0].Commit(t.Context(), e2, st); err != nil {
		t.Fatal(err)
	}
	if got := env.text(rows); got != "1:a3:big1,2:b2:-" {
		t.Errorf("after the next epoch: %s", got)
	}
}

// Patches stage per lane, and a lane's are read with the others' after every
// lane's rows.
func TestPatchesAcrossLanes(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}}
	ws := env.openLanes(1, 2, destination.Epoch{}, nil, stream)
	fields := []arrow.Field{idField, rowID, opField, seqField, syncedField, priceField, heldField}
	pfields := []arrow.Field{idField, rowID, seqField, syncedField, heldField, priceField}
	p := func(id int, n byte, price string) string {
		return fmt.Sprintf(`{"id": %d, "_openetl_id": %s, "_openetl_seq": 1, "_openetl_synced_at": "2026-01-02T03:04:05Z", "_openetl_held": ["price"], "price": %q}`, id, uid(n), price)
	}
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, ws[0], e1, "users", batch(t, fields, "["+row(1, 1, "null", `["price"]`)+"]"))
	patch(t, ws[0], e1, "users", batch(t, pfields, "["+p(2, 2, "two")+"]"))
	write(t, ws[1], e1, "users", batch(t, fields, "["+row(2, 2, "null", `["price"]`)+"]"))
	patch(t, ws[1], e1, "users", batch(t, pfields, "["+p(1, 1, "one")+"]"))
	st := precommitAll(t, e1, ws...)
	if _, err := ws[0].Commit(t.Context(), e1, st); err != nil {
		t.Fatal(err)
	}
	if got := env.text(prices); got != "1:one:-,2:two:-" {
		t.Errorf("rows: %s", got)
	}
}

// A read's table takes every lane's columns, widening as lanes' schemas move
// on across its epochs. A read that ended on a lane that never saw a batch
// names no table, and lane 0 knows it from the schemas.
func TestOverwriteAcrossLanes(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Overwrite}
	ws := env.openLanes(1, 3, destination.Epoch{}, nil, stream)
	start := map[string]time.Time{"users": time.UnixMicro(1000)}
	for _, w := range ws {
		startReads(t, w, start)
	}
	n32 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	n64 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	applySchema(t, ws[0], "users", []arrow.Field{idField, nameField, n64})
	e1, e2, e3 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}, destination.Epoch{Generation: 1, N: 3}
	write(t, ws[0], e1, "users", batch(t, []arrow.Field{idField, n32}, `[{"id": 1, "n": 1}]`))
	st := precommitAll(t, e1, ws...)
	if _, err := ws[0].Commit(t.Context(), e1, st); err != nil {
		t.Fatal(err)
	}
	write(t, ws[1], e2, "users", batch(t, []arrow.Field{idField, nameField, n64}, `[{"id": 2, "name": "b", "n": 5000000000}]`))
	st = precommitAll(t, e2, ws...)
	if _, err := ws[0].Commit(t.Context(), e2, st); err != nil {
		t.Fatal(err)
	}
	if n := env.count("SELECT count(*) FROM $schema.users"); n != 0 {
		t.Errorf("%d rows before the read ended", n)
	}
	endRead(t, ws[2].(destination.ReadTracker), e3, "users")
	st = precommitAll(t, e3, ws...)
	var d staged
	if err := json.Unmarshal(st[2], &d); err != nil || len(d.Streams) != 1 || d.Streams[0].Table != "" || !d.Streams[0].Ended {
		t.Fatalf("lane 2 knows of the read: %s (%v)", st[2], err)
	}
	if _, err := ws[0].Commit(t.Context(), e3, st); err != nil {
		t.Fatal(err)
	}
	const rows = `SELECT string_agg(id || ':' || coalesce(name, '-') || ':' || n, ',' ORDER BY id) FROM $schema.users`
	if got := env.text(rows); got != "1:-:1,2:b:5000000000" {
		t.Errorf("rows: %s", got)
	}
}

// An epoch is recovered from every lane's committable at any lane count, and
// with some lane's staging gone it's lost as a whole, if nothing of it is
// applied.
func TestRecoverAcrossLanes(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	streams := []destination.Stream{{Name: "users", Mode: destination.Append}, {Name: "orders", Mode: destination.Append}}
	ws := env.openLanes(1, 3, destination.Epoch{}, nil, streams...)
	id := []arrow.Field{idField}
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, ws[0], e1, "users", batch(t, id, `[{"id": 1}]`))
	write(t, ws[1], e1, "users", batch(t, id, `[{"id": 2}]`))
	write(t, ws[2], e1, "orders", batch(t, id, `[{"id": 3}]`))
	st := precommitAll(t, e1, ws...)

	w2 := env.openStreams(2, destination.ScopePipeline, streams...)
	r, err := w2.Recover(ctx, 2, e1, st)
	if err != nil || r != destination.Redone {
		t.Fatalf("Recover = %v, %v, want redone", r, err)
	}
	if n := env.count("SELECT count(*) FROM $schema.users") + env.count("SELECT count(*) FROM $schema.orders"); n != 3 {
		t.Errorf("%d rows, want 3", n)
	}
	if r, err := w2.Recover(ctx, 2, e1, st); err != nil || r != destination.Applied {
		t.Errorf("recovering it again = %v, %v, want applied", r, err)
	}

	e2 := destination.Epoch{Generation: 2, N: 2}
	lanes := env.openLanes(2, 2, e1, st, streams...)
	write(t, lanes[0], e2, "users", batch(t, id, `[{"id": 4}]`))
	write(t, lanes[1], e2, "users", batch(t, id, `[{"id": 5}]`))
	st2 := precommitAll(t, e2, lanes...)
	if _, err := env.conn.Exec(ctx, "DROP TABLE "+pgx.Identifier{env.schema, stagingName("p1", "users", e2, 1)}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	w3 := env.openStreams(3, destination.ScopePipeline, streams...)
	if r, err := w3.Recover(ctx, 3, e2, st2); err != nil || r != destination.Lost {
		t.Fatalf("a lane's staging gone: Recover = %v, %v, want lost", r, err)
	}
	if n := env.count("SELECT count(*) FROM $schema.users"); n != 2 {
		t.Errorf("%d users after a lost epoch, want 2", n)
	}
}

// With stream scope, an epoch that's partly applied can't be lost.
func TestRecoverOfAPartlyAppliedEpochNeedsItsStaging(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	streams := []destination.Stream{{Name: "users", Mode: destination.Append}, {Name: "orders", Mode: destination.Append}}
	cfg := func(gen uint64, lane int) destination.OpenConfig {
		return destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: destination.ScopeStream, Streams: streams, Lane: lane}
	}
	w0, err := env.d.Open(ctx, cfg(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w0.Close(context.Background()) }()
	w1, err := env.d.Open(ctx, cfg(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w1.Close(context.Background()) }()
	recov(t, w0, 1, destination.Epoch{}, nil)
	id := []arrow.Field{idField}
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w0, e1, "users", batch(t, id, `[{"id": 1}]`))
	write(t, w1, e1, "orders", batch(t, id, `[{"id": 2}]`))
	st := precommitAll(t, e1, w0, w1)
	if _, err := env.conn.Exec(ctx, "INSERT INTO "+pgx.Identifier{env.schema, "_openetl_loads"}.Sanitize()+" (pipeline, stream, generation, epoch) VALUES ('p1', 'users', 1, 1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := env.conn.Exec(ctx, "DROP TABLE "+pgx.Identifier{env.schema, stagingName("p1", "orders", e1, 1)}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	w2 := env.openStreams(2, destination.ScopeStream, streams...)
	if _, err := w2.Recover(ctx, 2, e1, st); err == nil || !strings.Contains(err.Error(), "partly applied") {
		t.Fatalf("Recover: %v, want partly applied", err)
	}
}

// A descriptor from before lanes reads as one lane's.
func TestDescriptorsFromBeforeLanes(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.open(1, destination.ScopePipeline, "users")
	recov(t, w, 1, destination.Epoch{}, nil)
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e1, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}, {"id": 2}]`))
	name := stagingName("p1", "users", e1, 0)
	precommit(t, w, e1)
	old, err := json.Marshal(map[string]any{"generation": 1, "epoch": 1, "streams": []map[string]any{{"name": "users", "table": "users", "staging": name}}})
	if err != nil {
		t.Fatal(err)
	}
	w2 := env.open(2, destination.ScopePipeline, "users")
	r, err := w2.Recover(ctx, 2, e1, []destination.Staged{old})
	if err != nil || r != destination.Redone {
		t.Fatalf("Recover = %v, %v, want redone", r, err)
	}
	if n := env.count("SELECT count(*) FROM $schema.users"); n != 2 {
		t.Errorf("%d rows, want 2", n)
	}
}

func TestMergeLanesRefusesOtherEpochs(t *testing.T) {
	w := &writer{tables: map[string]string{"users": "users"}}
	a, _ := json.Marshal(staged{Generation: 1, Epoch: 1})
	b, _ := json.Marshal(staged{Generation: 1, Epoch: 2})
	if _, err := w.mergeLanes([]destination.Staged{a, b}); err == nil {
		t.Error("merged two epochs' descriptors")
	}
	if _, err := w.mergeLanes(nil); err == nil {
		t.Error("merged no descriptors")
	}
	c, _ := json.Marshal(staged{Generation: 1, Epoch: 1, Streams: []stagedStream{{Name: "users", Staging: "s1", Rows: 2, Ended: true, Read: "r", Mode: modeOverwrite, Fills: map[string]string{"a": "b"}, Last: []string{"x"}}}})
	d, _ := json.Marshal(staged{Generation: 1, Epoch: 1, Streams: []stagedStream{{Name: "users", Staging: "s2", Rows: 3, Read: "r", Mode: modeOverwrite, Patch: "p2", Fills: map[string]string{"c": "d"}, Last: []string{"x", "y"}}}})
	got, err := w.mergeLanes([]destination.Staged{a, d, c})
	if err != nil {
		t.Fatal(err)
	}
	s := got.Streams[0]
	if s.Table != "users" || s.Rows != 5 || !s.Ended || !slices.Equal(s.stagings, []string{"s2", "s1"}) || !slices.Equal(s.patches, []string{"p2"}) ||
		len(s.Fills) != 2 || !slices.Equal(s.Last, []string{"x", "y"}) {
		t.Errorf("merged %+v", s)
	}
	e, _ := json.Marshal(staged{Generation: 1, Epoch: 1, Streams: []stagedStream{{Name: "users", Read: "other"}}})
	if _, err := w.mergeLanes([]destination.Staged{c, e}); err == nil {
		t.Error("merged two reads")
	}
}
