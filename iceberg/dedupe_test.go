//go:build integration

package iceberg

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

func idRow(id int, n byte) string { return fmt.Sprintf(`{"id": %d, "_openetl_id": %s}`, id, uid(n)) }

// DedupeEpoch keeps one row per _openetl_id in the epoch; DedupeTarget also
// leaves out the IDs the table has. Each counts what it left out.
func TestDedupeAppend(t *testing.T) {
	fields := []arrow.Field{idField, rowID}
	for _, kind := range []destination.Dedupe{destination.DedupeEpoch, destination.DedupeTarget} {
		env := newEnv(t)
		w := env.open(1, destination.ScopeStream, destination.Stream{Name: "items", Mode: destination.Append, Dedupe: kind})
		recov(t, w, 1, destination.Epoch{}, nil)
		e1 := destination.Epoch{Generation: 1, N: 1}
		writeRows(t, w, e1, "items", fields, list(idRow(1, 1), idRow(2, 2)))
		writeRows(t, w, e1, "items", fields, list(idRow(1, 1), idRow(3, 3)))
		writeRows(t, w, e1, "items", fields, list(idRow(2, 2)))
		if c := commitEpoch(t, w, e1); c.Collapsed["items"] != 2 {
			t.Errorf("%v: epoch 1 collapsed %v, want 2", kind, c.Collapsed)
		}
		env.wantIDs("items", 1, 2, 3)

		// A replay of rows the table has, with one new one.
		e2 := destination.Epoch{Generation: 1, N: 2}
		writeRows(t, w, e2, "items", fields, list(idRow(2, 2), idRow(3, 3), idRow(4, 4)))
		c := commitEpoch(t, w, e2)
		if kind == destination.DedupeTarget {
			env.wantIDs("items", 1, 2, 3, 4)
			if c.Collapsed["items"] != 2 {
				t.Errorf("target: epoch 2 collapsed %v, want 2", c.Collapsed)
			}
		} else {
			env.wantIDs("items", 1, 2, 2, 3, 3, 4)
		}
	}
}

// A replay of an epoch the table has whole leaves every row out, and still
// commits, once; more IDs than a scan's filter takes are found in full.
func TestDedupeAReplay(t *testing.T) {
	fields := []arrow.Field{idField, rowID}
	env := newEnv(t)
	w := env.open(1, destination.ScopeStream, destination.Stream{Name: "items", Mode: destination.Append, Dedupe: destination.DedupeTarget})
	recov(t, w, 1, destination.Epoch{}, nil)
	var rows []string
	for i := range maxInValues + 1 {
		rows = append(rows, fmt.Sprintf(`{"id": %d, "_openetl_id": %s}`, i, bigUID(i)))
	}
	e1 := destination.Epoch{Generation: 1, N: 1}
	writeRows(t, w, e1, "items", fields, list(rows...))
	commitEpoch(t, w, e1)

	e2 := destination.Epoch{Generation: 1, N: 2}
	writeRows(t, w, e2, "items", fields, list(rows...))
	st := precommit(t, w, e2)
	c, err := w.Commit(t.Context(), e2, []destination.Staged{st})
	if err != nil {
		t.Fatal(err)
	}
	if c.Collapsed["items"] != uint64(len(rows)) {
		t.Errorf("collapsed %v, want %d", c.Collapsed, len(rows))
	}
	commit(t, w, e2, st)
	w2 := env.open(2, destination.ScopeStream, destination.Stream{Name: "items", Mode: destination.Append, Dedupe: destination.DedupeTarget})
	if r := recov(t, w2, 2, e2, st); r != destination.Applied {
		t.Errorf("recover: %v", r)
	}
	if n := len(env.ids("items")); n != len(rows) {
		t.Errorf("%d rows, want %d", n, len(rows))
	}
	e3 := destination.Epoch{Generation: 2, N: 3}
	writeRows(t, w2, e3, "items", fields, list(rows[0], fmt.Sprintf(`{"id": -1, "_openetl_id": %s}`, bigUID(-1))))
	commitEpoch(t, w2, e3)
	if n := len(env.ids("items")); n != len(rows)+1 {
		t.Errorf("after epoch 3: %d rows, want %d", n, len(rows)+1)
	}
}

// bigUID is a JSON _openetl_id made from i.
func bigUID(i int) string {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[8:], uint64(i))
	return `"` + base64.StdEncoding.EncodeToString(b) + `"`
}

// An overwrite's read keeps one row per _openetl_id across its epochs, by
// either kind.
func TestDedupeOverwrite(t *testing.T) {
	fields := []arrow.Field{idField, rowID}
	for _, kind := range []destination.Dedupe{destination.DedupeEpoch, destination.DedupeTarget} {
		env := newEnv(t)
		ctx := t.Context()
		w := env.open(1, destination.ScopeStream, destination.Stream{Name: "items", Mode: destination.Overwrite, Dedupe: kind})
		recov(t, w, 1, destination.Epoch{}, nil)
		rt := w.(destination.ReadTracker)
		if err := rt.StartReads(ctx, map[string]time.Time{"items": time.UnixMicro(1000)}); err != nil {
			t.Fatal(err)
		}
		e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
		writeRows(t, w, e1, "items", fields, list(idRow(1, 1), idRow(2, 2), idRow(1, 1)))
		commitEpoch(t, w, e1)
		writeRows(t, w, e2, "items", fields, list(idRow(2, 2), idRow(3, 3)))
		if err := rt.EndRead(ctx, e2, "items"); err != nil {
			t.Fatal(err)
		}
		c := commitEpoch(t, w, e2)
		env.wantIDs("items", 1, 2, 3)
		if c.Collapsed["items"] != 2 {
			t.Errorf("%v: collapsed %v, want 2", kind, c.Collapsed)
		}
	}
}

// A table partitioned by a transform that doesn't keep order, by a string
// longer than its statistics keep, and with a null partition, gets each file
// in its partition; a merge into one partitioned by its key works, and one
// partitioned by another column fails.
func TestPartitionedTables(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	cat, err := env.d.catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(ctx, []string{env.ns}, nil); err != nil {
		t.Fatal(err)
	}
	sc := ice.NewSchema(0,
		ice.NestedField{ID: 1, Name: "id", Type: ice.PrimitiveTypes.Int64},
		ice.NestedField{ID: 2, Name: "name", Type: ice.PrimitiveTypes.String})
	create := func(name string, fields ...ice.PartitionField) {
		spec := ice.NewPartitionSpec(fields...)
		if _, err := cat.CreateTable(ctx, table.Identifier{env.ns, name}, sc, catalog.WithPartitionSpec(&spec)); err != nil {
			t.Fatal(err)
		}
	}
	bucket := ice.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_bucket", Transform: ice.BucketTransform{NumBuckets: 4}}
	create("parts", bucket, ice.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "name", Transform: ice.IdentityTransform{}})
	create("keyed", bucket)
	create("unkeyed", ice.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "name", Transform: ice.IdentityTransform{}})

	long := strings.Repeat("x", 40)
	w := env.open(1, destination.ScopeStream, appends("parts")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	e1 := destination.Epoch{Generation: 1, N: 1}
	writeRows(t, w, e1, "parts", []arrow.Field{idField, nameField}, `[
		{"id": 1, "name": "a"}, {"id": 2, "name": "a"}, {"id": 3, "name": "`+long+`"},
		{"id": 4, "name": null}, {"id": 5, "name": "b"}, {"id": 6, "name": "`+long+`"}]`)
	commitEpoch(t, w, e1)
	env.wantIDs("parts", 1, 2, 3, 4, 5, 6)
	// Each file's partition is its rows': a scan that prunes by it finds
	// each row.
	tbl := env.table("parts")
	for id := range int64(6) {
		_, it, err := tbl.Scan(table.WithRowFilter(ice.EqualTo(ice.Reference("id"), id+1))).ToArrowRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for b, err := range it {
			if err != nil {
				t.Fatal(err)
			}
			n += int(b.NumRows())
			b.Release()
		}
		if n != 1 {
			t.Errorf("id %d: %d rows", id+1, n)
		}
	}
	for name, want := range map[string]int{long: 2, "a": 2} {
		_, it, err := tbl.Scan(table.WithRowFilter(ice.EqualTo(ice.Reference("name"), name))).ToArrowRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for b, err := range it {
			if err != nil {
				t.Fatal(err)
			}
			n += int(b.NumRows())
			b.Release()
		}
		if n != want {
			t.Errorf("name %q: %d rows, want %d", name, n, want)
		}
	}
	_, it, err := tbl.Scan(table.WithRowFilter(ice.IsNull(ice.Reference("name")))).ToArrowRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for b, err := range it {
		if err != nil {
			t.Fatal(err)
		}
		if b.NumRows() != 1 {
			t.Errorf("null name: %d rows", b.NumRows())
		}
		b.Release()
	}

	fields := []arrow.Field{idField, nameField, opField, seqField, deletedField}
	m := env.open(2, destination.ScopeStream, merges(destination.SoftDelete, "keyed")...)
	recov(t, m, 2, destination.Epoch{}, nil)
	for n, rows := range []string{
		`[{"id": 1, "name": "a", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
		  {"id": 2, "name": "b", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false}]`,
		`[{"id": 1, "name": "a2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false}]`,
	} {
		e := destination.Epoch{Generation: 2, N: uint64(n + 1)}
		writeRows(t, m, e, "keyed", fields, rows)
		commitEpoch(t, m, e)
	}
	if got := env.rows("keyed", "id", "name"); got != "1:a2,2:b" {
		t.Errorf("merge into a table partitioned by its key: %s", got)
	}

	u := env.open(3, destination.ScopeStream, merges(destination.SoftDelete, "unkeyed")...)
	recov(t, u, 3, destination.Epoch{}, nil)
	err = u.Write(ctx, destination.Epoch{Generation: 3, N: 1}, "unkeyed", batch(t, fields,
		`[{"id": 1, "name": "a", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false}]`))
	if err == nil || !strings.Contains(err.Error(), "partition column") {
		t.Errorf("merge into a table partitioned off its key: %v", err)
	}
}

// A table partitioned after an epoch's files were written gets them by
// partition, and a format 3 table's files get row IDs.
func TestTableChangesUnderAWriter(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.open(1, destination.ScopeStream, appends("late")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, w, e1, "late", 1)
	commitEpoch(t, w, e1)
	writeRows(t, w, e2, "late", []arrow.Field{idField, nameField}, `[{"id": 2, "name": "a"}, {"id": 3, "name": "b"}, {"id": 4, "name": "a"}]`)
	st := precommit(t, w, e2)
	tx := env.table("late").NewTransaction()
	if err := tx.UpdateSpec(true).AddIdentity("name").Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	commit(t, w, e2, st)
	env.wantIDs("late", 1, 2, 3, 4)
	tasks, err := env.table("late").Scan(table.WithRowFilter(ice.EqualTo(ice.Reference("name"), "a"))).PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].File.Count() != 2 {
		t.Errorf("name a: %d files, want one of 2 rows", len(tasks))
	}

	cat, err := env.d.catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sc := ice.NewSchema(0, ice.NestedField{ID: 1, Name: "id", Type: ice.PrimitiveTypes.Int64}, ice.NestedField{ID: 2, Name: "name", Type: ice.PrimitiveTypes.String})
	if _, err := cat.CreateTable(ctx, table.Identifier{env.ns, "v3"}, sc, catalog.WithProperties(ice.Properties{table.PropertyFormatVersion: "3"})); err != nil {
		t.Fatal(err)
	}
	v := env.open(2, destination.ScopeStream, appends("v3")...)
	recov(t, v, 2, destination.Epoch{}, nil)
	for n := range uint64(2) {
		e := destination.Epoch{Generation: 2, N: n + 1}
		write(t, v, e, "v3", int64(2*n+1), int64(2*n+2))
		commitEpoch(t, v, e)
	}
	env.wantIDs("v3", 1, 2, 3, 4)
	if next := env.table("v3").Metadata().NextRowID(); next != 4 {
		t.Errorf("next row ID %d, want 4", next)
	}
	o := env.open(3, destination.ScopeStream, overwrites("v3")...)
	recov(t, o, 3, destination.Epoch{}, nil)
	if err := o.(destination.ReadTracker).StartReads(ctx, map[string]time.Time{"v3": time.UnixMicro(1000)}); err != nil {
		t.Fatal(err)
	}
	e3 := destination.Epoch{Generation: 3, N: 3}
	write(t, o, e3, "v3", 7, 8, 9)
	if err := o.(destination.ReadTracker).EndRead(ctx, e3, "v3"); err != nil {
		t.Fatal(err)
	}
	commitEpoch(t, o, e3)
	env.wantIDs("v3", 7, 8, 9)
	if next := env.table("v3").Metadata().NextRowID(); next != 7 {
		t.Errorf("after the overwrite: next row ID %d, want 7", next)
	}
}

// A partitioned table's held cells are patched in their partitions; once
// it holds files of an earlier partitioning, which its deletes can't reach,
// a patch fails.
func TestPatchAPartitionedTable(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Append, PrimaryKey: []string{"id"}}
	w := env.writer(1, stream)
	fields := []arrow.Field{idField, rowID, opField, seqField, priceField, heldField}
	pfields := []arrow.Field{idField, rowID, seqField, heldField, priceField}
	partition := func(tf ice.Transform, name string) {
		tx := env.table("users").NewTransaction()
		if err := tx.UpdateSpec(true).AddField("id", tf, name).Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.(destination.SchemaApplier).ApplySchema(t.Context(), []destination.StreamSchema{{Name: "users", Schema: arrow.NewSchema(fields, nil)}}); err != nil {
		t.Fatal(err)
	}
	partition(ice.BucketTransform{NumBuckets: 2}, "id_bucket")
	writeRows(t, w, ep(1), "users", fields, list(row(1, 1, "null", `["price"]`), row(2, 2, `"x"`, "null"), row(3, 3, "null", `["price"]`)))
	commitEpoch(t, w, ep(1))
	writeRows(t, w, ep(2), "users", fields, list(row(4, 4, `"y"`, "null")))
	patch(t, w, ep(2), "users", batch(t, pfields, list(
		`{"id": 1, "_openetl_id": `+uid(1)+`, "_openetl_seq": 1, "_openetl_held": ["price"], "price": "1"}`,
		`{"id": 3, "_openetl_id": `+uid(3)+`, "_openetl_seq": 3, "_openetl_held": ["price"], "price": "3"}`,
	)))
	commitEpoch(t, w, ep(2))
	if got := env.held("users", "id", "price"); got != "1:1:-,2:x:-,3:3:-,4:y:-" {
		t.Errorf("after the patches: %s", got)
	}

	partition(ice.TruncateTransform{Width: 10}, "id_trunc")
	patch(t, w, ep(3), "users", batch(t, pfields, list(
		`{"id": 2, "_openetl_id": `+uid(2)+`, "_openetl_seq": 2, "_openetl_held": ["price"], "price": "2"}`,
	)))
	if _, err := w.Commit(t.Context(), ep(3), []destination.Staged{precommit(t, w, ep(3))}); err == nil || !strings.Contains(err.Error(), "earlier partitioning") {
		t.Errorf("a patch of a table with files of an earlier partitioning: %v", err)
	}
}

// A table partitioned by _openetl_synced_at, which a patch sets, has a
// patched row's delete in the partition the row was in, so the row isn't
// left there too.
func TestPatchATablePartitionedBySyncedAt(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Append, PrimaryKey: []string{"id"}}
	w := env.writer(1, stream)
	fields := []arrow.Field{idField, rowID, opField, seqField, syncedField, priceField, heldField}
	if _, err := w.(destination.SchemaApplier).ApplySchema(t.Context(), []destination.StreamSchema{{Name: "users", Schema: arrow.NewSchema(fields, nil)}}); err != nil {
		t.Fatal(err)
	}
	tx := env.table("users").NewTransaction()
	if err := tx.UpdateSpec(true).AddField(syncedColumn, ice.DayTransform{}, "synced_day").Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	synced := func(r string) string {
		return strings.Replace(r, `{"id"`, `{"_openetl_synced_at": "2026-01-01T00:00:00Z", "id"`, 1)
	}
	writeRows(t, w, ep(1), "users", fields, list(synced(row(1, 1, "null", `["price"]`)), synced(row(2, 2, `"x"`, "null"))))
	commitEpoch(t, w, ep(1))
	// p's _openetl_synced_at is the next day's.
	patch(t, w, ep(2), "users", batch(t, []arrow.Field{idField, rowID, seqField, syncedField, heldField, priceField}, list(p(1, 1, `"1"`))))
	commitEpoch(t, w, ep(2))
	if got := env.held("users", "id", "price"); got != "1:1:-,2:x:-" {
		t.Errorf("after the patch: %s", got)
	}
}
