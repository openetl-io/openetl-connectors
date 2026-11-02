//go:build integration

package iceberg

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

var (
	opField        = arrow.Field{Name: opColumn, Type: arrow.BinaryTypes.String}
	seqField       = arrow.Field{Name: seqColumn, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	deletedField   = arrow.Field{Name: deletedColumn, Type: arrow.FixedWidthTypes.Boolean}
	unchangedField = arrow.Field{Name: unchangedColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
	docField       = arrow.Field{Name: "doc", Type: arrow.BinaryTypes.String, Nullable: true}
)

func merges(deletes destination.DeleteMode, names ...string) []destination.Stream {
	var out []destination.Stream
	for _, n := range names {
		out = append(out, destination.Stream{Name: n, Mode: destination.Merge, PrimaryKey: []string{"id"}, Deletes: deletes})
	}
	return out
}

func writeRows(t *testing.T, w destination.Writer, e destination.Epoch, stream string, fields []arrow.Field, rows string) {
	t.Helper()
	if err := w.Write(t.Context(), e, stream, batch(t, fields, rows)); err != nil {
		t.Fatal(err)
	}
}

func commitEpoch(t *testing.T, w destination.Writer, e destination.Epoch) destination.Committed {
	t.Helper()
	c, err := w.Commit(t.Context(), e, precommit(t, w, e))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// rows returns the table's rows sorted, each as its columns' values joined
// by ':', with '-' for null.
func (e *env) rows(name string, cols ...string) string {
	e.t.Helper()
	_, it, err := e.table(name).Scan(table.WithSelectedFields(cols...)).ToArrowRecords(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	var out []string
	for b, err := range it {
		if err != nil {
			e.t.Fatal(err)
		}
		for r := range int(b.NumRows()) {
			vals := make([]string, len(cols))
			for i, c := range cols {
				col := b.Column(b.Schema().FieldIndices(c)[0])
				vals[i] = "-"
				if !col.IsNull(r) {
					vals[i] = col.ValueStr(r)
				}
			}
			out = append(out, strings.Join(vals, ":"))
		}
		b.Release()
	}
	slices.Sort(out)
	return strings.Join(out, ",")
}

// Each key keeps its latest version: within an epoch by _openetl_seq, across
// epochs unless the stored one is newer. A delete keeps the row's columns,
// marked deleted, and guards it against older versions.
func TestMerge(t *testing.T) {
	for _, scope := range []destination.Scope{destination.ScopePipeline, destination.ScopeStream} {
		env := newEnv(t)
		w := env.open(1, scope, merges(destination.SoftDelete, "users")...)
		recov(t, w, 1, destination.Epoch{}, nil)
		fields := []arrow.Field{idField, nameField, opField, seqField, deletedField}
		e1 := destination.Epoch{Generation: 1, N: 1}
		writeRows(t, w, e1, "users", fields, `[
			{"id": 1, "name": "a", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
			{"id": 2, "name": "b", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
			{"id": 1, "name": "a2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false},
			{"id": 3, "name": "c", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false}]`)
		if c := commitEpoch(t, w, e1); len(c.Collapsed) != 0 {
			t.Errorf("merge counted %v as collapsed", c.Collapsed)
		}
		if got := env.rows("users", "id", "name"); got != "1:a2,2:b,3:c" {
			t.Fatalf("after epoch 1: %s", got)
		}

		e2 := destination.Epoch{Generation: 1, N: 2}
		writeRows(t, w, e2, "users", fields, `[
			{"id": 2, "name": "stale", "_openetl_op": "update", "_openetl_seq": 0, "_openetl_deleted": false},
			{"id": 1, "name": "a3", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false},
			{"id": 3, "_openetl_op": "delete", "_openetl_seq": 2, "_openetl_deleted": true},
			{"id": 4, "_openetl_op": "delete", "_openetl_seq": 1, "_openetl_deleted": true}]`)
		commitEpoch(t, w, e2)
		e3 := destination.Epoch{Generation: 1, N: 3}
		writeRows(t, w, e3, "users", fields, `[{"id": 3, "name": "c-old", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false}]`)
		commitEpoch(t, w, e3)
		// A tie on _openetl_seq updates; an older version doesn't.
		if got := env.rows("users", "id", "name", opColumn, seqColumn, deletedColumn); got != "1:a3:update:2:false,2:b:insert:1:false,3:c:delete:2:true,4:-:delete:1:true" {
			t.Errorf("after epochs 2 and 3: %s", got)
		}
		if got := env.table("users").CurrentSnapshot().Summary.Properties[epochKey]; got != "2" {
			t.Errorf("the last snapshot is epoch %s's, want 2's: epoch 3 changed nothing", got)
		}
		// The deleted row is older than anything in the epoch, and keeps its
		// columns.
		e4 := destination.Epoch{Generation: 1, N: 4}
		writeRows(t, w, e4, "users", fields, `[{"id": 2, "_openetl_op": "delete", "_openetl_seq": 5, "_openetl_deleted": true}]`)
		commitEpoch(t, w, e4)
		if got := env.rows("users", "id", "name", opColumn, seqColumn, deletedColumn); got != "1:a3:update:2:false,2:b:delete:5:true,3:c:delete:2:true,4:-:delete:1:true" {
			t.Errorf("after epoch 4: %s", got)
		}
	}
}

func TestMergeHardDeletes(t *testing.T) {
	env := newEnv(t)
	w := env.open(1, destination.ScopeStream, merges(destination.HardDelete, "users")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, nameField, opField, seqField}
	e1 := destination.Epoch{Generation: 1, N: 1}
	writeRows(t, w, e1, "users", fields, `[
		{"id": 1, "name": "a", "_openetl_op": "insert", "_openetl_seq": 1},
		{"id": 2, "name": "b", "_openetl_op": "insert", "_openetl_seq": 1},
		{"id": 3, "name": "c", "_openetl_op": "insert", "_openetl_seq": 5},
		{"id": 5, "name": "e", "_openetl_op": "insert", "_openetl_seq": 1}]`)
	commitEpoch(t, w, e1)
	e2 := destination.Epoch{Generation: 1, N: 2}
	writeRows(t, w, e2, "users", fields, `[
		{"id": 1, "_openetl_op": "delete", "_openetl_seq": 2},
		{"id": 2, "name": "b2", "_openetl_op": "update", "_openetl_seq": 2},
		{"id": 2, "_openetl_op": "delete", "_openetl_seq": 3},
		{"id": 3, "_openetl_op": "delete", "_openetl_seq": 4},
		{"id": 4, "_openetl_op": "delete", "_openetl_seq": 1},
		{"id": 5, "_openetl_op": "delete", "_openetl_seq": 2},
		{"id": 5, "name": "e2", "_openetl_op": "insert", "_openetl_seq": 3}]`)
	commitEpoch(t, w, e2)
	// 3's stored version is newer than its delete; 5 came back after its own.
	if got := env.rows("users", "id", "name"); got != "3:c,5:e2" {
		t.Errorf("users: %s", got)
	}
}

// A column an update didn't send takes its value from the key's latest
// earlier version in the epoch that sent it, or else keeps the table's. The
// table's _openetl_unchanged lists the columns it never received.
func TestMergeKeepsUnchangedColumns(t *testing.T) {
	env := newEnv(t)
	w := env.open(1, destination.ScopePipeline, merges(destination.SoftDelete, "users")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, nameField, docField, opField, seqField, deletedField, unchangedField}
	cols := []string{"id", "name", "doc", unchangedColumn, deletedColumn}
	e1 := destination.Epoch{Generation: 1, N: 1}
	writeRows(t, w, e1, "users", fields, `[
		{"id": 1, "name": "a", "doc": "big1", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
		{"id": 2, "name": "b", "_openetl_op": "update", "_openetl_seq": 1, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]},
		{"id": 1, "name": "a2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]},
		{"id": 3, "name": "c", "doc": null, "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
		{"id": 3, "name": "c2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]}]`)
	commitEpoch(t, w, e1)
	if got := env.rows("users", cols...); got != `1:a2:big1:-:false,2:b:-:["doc"]:false,3:c2:-:-:false` {
		t.Fatalf("after epoch 1: %s", got)
	}

	e2 := destination.Epoch{Generation: 1, N: 2}
	writeRows(t, w, e2, "users", fields, `[
		{"id": 1, "name": "a3", "_openetl_op": "update", "_openetl_seq": 3, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]},
		{"id": 2, "name": "b2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]},
		{"id": 3, "name": "c3", "doc": "big3", "_openetl_op": "update", "_openetl_seq": 3, "_openetl_deleted": false},
		{"id": 3, "name": "c4", "_openetl_op": "update", "_openetl_seq": 4, "_openetl_deleted": false, "_openetl_unchanged": ["doc"]}]`)
	commitEpoch(t, w, e2)
	if got := env.rows("users", cols...); got != `1:a3:big1:-:false,2:b2:-:["doc"]:false,3:c4:big3:-:false` {
		t.Fatalf("after epoch 2: %s", got)
	}

	e3 := destination.Epoch{Generation: 1, N: 3}
	writeRows(t, w, e3, "users", fields, `[
		{"id": 1, "_openetl_op": "delete", "_openetl_seq": 4, "_openetl_deleted": true},
		{"id": 2, "name": "b3", "doc": "big2", "_openetl_op": "update", "_openetl_seq": 3, "_openetl_deleted": false}]`)
	commitEpoch(t, w, e3)
	if got := env.rows("users", cols...); got != `1:a3:big1:-:true,2:b3:big2:-:false,3:c4:big3:-:false` {
		t.Errorf("after epoch 3: %s", got)
	}
}

// The catalog names a key by its source names; the engine renames columns.
func TestMergeOnARenamedKey(t *testing.T) {
	env := newEnv(t)
	w := env.open(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"User ID"}})
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
	if err := w.Write(t.Context(), e, "users", b); err != nil {
		t.Fatal(err)
	}
	commitEpoch(t, w, e)
	if got := env.rows("users", "user_id", "name"); got != "1:b" {
		t.Errorf("users: %s", got)
	}

	w2 := env.open(2, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"amount"}})
	recov(t, w2, 2, destination.Epoch{}, nil)
	f := arrow.Field{Name: "amount", Type: arrow.PrimitiveTypes.Float64}
	if err := w2.Write(t.Context(), destination.Epoch{Generation: 2, N: 2}, "users", batch(t, []arrow.Field{f}, `[{"amount": 1.5}]`)); err == nil || !strings.Contains(err.Error(), "floating-point") {
		t.Errorf("a merge on a float key: %v", err)
	}
}

// A later attempt redoes a staged merge from its files, and the files go once
// it's applied; without them, it's lost.
func TestMergeRecover(t *testing.T) {
	env := newEnv(t)
	fields := []arrow.Field{idField, nameField, seqField}
	w := env.open(1, destination.ScopePipeline, merges(destination.SoftDelete, "users")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	e1 := destination.Epoch{Generation: 1, N: 1}
	writeRows(t, w, e1, "users", fields, `[{"id": 1, "name": "a", "_openetl_seq": 1}, {"id": 2, "name": "b", "_openetl_seq": 1}]`)
	commitEpoch(t, w, e1)
	e2 := destination.Epoch{Generation: 1, N: 2}
	writeRows(t, w, e2, "users", fields, `[{"id": 1, "name": "a2", "_openetl_seq": 2}, {"id": 3, "name": "c", "_openetl_seq": 2}]`)
	st := precommit(t, w, e2)

	w2 := env.open(2, destination.ScopePipeline, merges(destination.SoftDelete, "users")...)
	if r := recov(t, w2, 2, e2, st); r != destination.Redone {
		t.Fatalf("recover: %v, want redone", r)
	}
	if got := env.rows("users", "id", "name"); got != "1:a2,2:b,3:c" {
		t.Errorf("after the redo: %s", got)
	}
	var desc staged
	if err := json.Unmarshal(st, &desc); err != nil {
		t.Fatal(err)
	}
	if env.anyExist("users", desc.Streams[0].Files) {
		t.Errorf("the redo left the merge's staged files")
	}
	w3 := env.open(3, destination.ScopePipeline, merges(destination.SoftDelete, "users")...)
	if r := recov(t, w3, 3, e2, st); r != destination.Applied {
		t.Errorf("recover again: %v, want applied", r)
	}

	e3 := destination.Epoch{Generation: 3, N: 3}
	writeRows(t, w3, e3, "users", fields, `[{"id": 2, "name": "b2", "_openetl_seq": 3}]`)
	st3 := precommit(t, w3, e3)
	if err := json.Unmarshal(st3, &desc); err != nil {
		t.Fatal(err)
	}
	fs, err := env.table("users").FS(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range desc.Streams[0].Files {
		if err := fs.Remove(f); err != nil {
			t.Fatal(err)
		}
	}
	w4 := env.open(4, destination.ScopePipeline, merges(destination.SoftDelete, "users")...)
	if r := recov(t, w4, 4, e3, st3); r != destination.Lost {
		t.Errorf("recover without the staged files: %v, want lost", r)
	}
	if got := env.rows("users", "id", "name"); got != "1:a2,2:b,3:c" {
		t.Errorf("after the lost epoch: %s", got)
	}
}

func (e *env) anyExist(name string, paths []string) bool {
	e.t.Helper()
	fs, err := e.table(name).FS(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	for _, p := range paths {
		if f, err := fs.Open(p); err == nil {
			_ = f.Close()
			return true
		}
	}
	return false
}

// Another writer commits a newer version between the merge's read and its
// commit: the commit conflicts, and the merge reads the table again.
func TestMergeReadsAgainAfterAConflict(t *testing.T) {
	env := newEnv(t)
	fields := []arrow.Field{idField, nameField, seqField}
	w := env.open(1, destination.ScopePipeline, merges(destination.SoftDelete, "users")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	e1 := destination.Epoch{Generation: 1, N: 1}
	writeRows(t, w, e1, "users", fields, `[{"id": 1, "name": "a", "_openetl_seq": 1}]`)
	commitEpoch(t, w, e1)

	other, err := env.d.Open(t.Context(), destination.OpenConfig{Pipeline: "p2", Generation: 1, Scope: destination.ScopePipeline, Streams: merges(destination.SoftDelete, "users")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close(t.Context()) })
	recov(t, other, 1, destination.Epoch{}, nil)
	oe := destination.Epoch{Generation: 1, N: 1}
	writeRows(t, other, oe, "users", fields, `[{"id": 1, "name": "other", "_openetl_seq": 9}]`)
	ost := precommit(t, other, oe)

	var once sync.Once
	testHookBeforeCommit = func(cur *writer) {
		if cur.cfg.Pipeline == "p1" {
			once.Do(func() { commit(t, other, oe, ost) })
		}
	}
	t.Cleanup(func() { testHookBeforeCommit = nil })
	e2 := destination.Epoch{Generation: 1, N: 2}
	writeRows(t, w, e2, "users", fields, `[{"id": 1, "name": "a2", "_openetl_seq": 2}, {"id": 2, "name": "b", "_openetl_seq": 2}]`)
	commitEpoch(t, w, e2)
	if got := env.rows("users", "id", "name"); got != "1:other,2:b" {
		t.Errorf("users: %s", got)
	}
}
