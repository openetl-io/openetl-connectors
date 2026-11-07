//go:build integration

package iceberg

import (
	"encoding/base64"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

var (
	rowID = arrow.Field{Name: idColumn, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16},
		Metadata: arrow.NewMetadata([]string{extensionNameKey}, []string{uuidExtension})}
	heldField   = arrow.Field{Name: heldColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
	priceField  = arrow.Field{Name: "price", Type: arrow.BinaryTypes.String, Nullable: true}
	syncedField = arrow.Field{Name: syncedColumn, Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true}
)

// uid is a JSON _openetl_id whose bytes are all n.
func uid(n byte) string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = n
	}
	return `"` + base64.StdEncoding.EncodeToString(b) + `"`
}

func patch(t *testing.T, w destination.Writer, e destination.Epoch, stream string, b arrow.RecordBatch) {
	t.Helper()
	if err := w.(destination.Patcher).Patch(t.Context(), e, stream, b); err != nil {
		t.Fatal(err)
	}
}

func row(id int, n byte, price, held string) string {
	return fmt.Sprintf(`{"id": %d, "_openetl_id": %s, "_openetl_op": "update", "_openetl_seq": %d, "price": %s, "_openetl_held": %s}`, id, uid(n), n, price, held)
}

func p(id int, n byte, price string) string {
	return fmt.Sprintf(`{"id": %d, "_openetl_id": %s, "_openetl_seq": %d, "_openetl_synced_at": "2026-01-02T03:04:05Z", "_openetl_held": ["price"], "price": %s}`, id, uid(n), n, price)
}

func list(rows ...string) string { return "[" + strings.Join(rows, ",") + "]" }

// held returns the table's rows sorted, each as cols' values and its held
// columns, each with the first byte of the version it's held for, joined
// by ':', with '-' for null.
func (e *env) held(name string, cols ...string) string {
	e.t.Helper()
	_, it, err := e.table(name).Scan(table.WithSelectedFields(append(slices.Clone(cols), heldColumn)...)).ToArrowRecords(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	var out []string
	for b, err := range it {
		if err != nil {
			e.t.Fatal(err)
		}
		for r := range int(b.NumRows()) {
			var vals []string
			for _, c := range cols {
				col := b.Column(b.Schema().FieldIndices(c)[0])
				v := "-"
				if !col.IsNull(r) {
					v = col.ValueStr(r)
				}
				vals = append(vals, v)
			}
			h := heldAt(b.Column(b.Schema().FieldIndices(heldColumn)[0]), r)
			hs := "-"
			if len(h) > 0 {
				var parts []string
				for _, k := range slices.Sorted(maps.Keys(h)) {
					parts = append(parts, fmt.Sprintf("%s@%d", k, h[k][0]))
				}
				hs = strings.Join(parts, "+")
			}
			out = append(out, strings.Join(append(vals, hs), ":"))
		}
		b.Release()
	}
	slices.Sort(out)
	return strings.Join(out, ",")
}

func (e *env) writer(gen uint64, streams ...destination.Stream) destination.Writer {
	e.t.Helper()
	w := e.open(gen, destination.ScopePipeline, streams...)
	recov(e.t, w, gen, destination.Epoch{}, nil)
	return w
}

var users = destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}}

func ep(n uint64) destination.Epoch { return destination.Epoch{Generation: 1, N: n} }

// A held cell's row names the version whose value is held, and a patch
// fills it only for that version: in any order, once, and never over a
// newer value. A soft delete keeps the held cell.
func TestMergeHoldsCellsAndPatchesThem(t *testing.T) {
	env := newEnv(t)
	w := env.writer(1, users)
	fields := []arrow.Field{idField, rowID, opField, seqField, syncedField, priceField, heldField}
	pfields := []arrow.Field{idField, rowID, seqField, syncedField, heldField, priceField}

	writeRows(t, w, ep(1), "users", fields, list(
		row(1, 1, `"1"`, "null"),
		row(2, 2, "null", `["price"]`),
		row(4, 4, "null", `["price"]`),
		row(5, 10, "null", `["price"]`),
		row(6, 12, "null", `["price"]`),
	))
	commitEpoch(t, w, ep(1))
	if got, want := env.held("users", "id", "price"), "1:1:-,2:-:price@2,4:-:price@4,5:-:price@10,6:-:price@12"; got != want {
		t.Fatalf("after epoch 1:\n got %s\nwant %s", got, want)
	}

	// Key 4 gets a newer value, key 5 a newer held version, and key 6 is
	// deleted, which keeps its columns.
	writeRows(t, w, ep(2), "users", append(fields, deletedField), list(
		row(4, 5, `"live"`, "null"),
		row(5, 11, "null", `["price"]`),
		`{"id": 6, "_openetl_id": `+uid(13)+`, "_openetl_op": "delete", "_openetl_seq": 13, "_openetl_held": null, "_openetl_deleted": true}`,
	))
	commitEpoch(t, w, ep(2))

	patches := list(
		p(2, 9, `"other version"`),
		p(2, 2, `"2.5"`),
		p(3, 3, `"no such key"`),
		p(4, 4, `"stale"`),
		p(5, 10, `"older version"`),
		p(5, 11, `"5"`),
		p(6, 13, `"the delete's"`),
		p(6, 12, `"6"`),
	)
	patch(t, w, ep(3), "users", batch(t, pfields, patches))
	commitEpoch(t, w, ep(3))
	const done = "1:1:-,2:2.5:-,4:live:-,5:5:-,6:6:-"
	if got := env.held("users", "id", "price"); got != done {
		t.Fatalf("after the patches:\n got %s\nwant %s", got, done)
	}
	if got := env.rows("users", "id", deletedColumn); got != "1:-,2:-,4:-,5:-,6:true" {
		t.Errorf("deleted: %s", got)
	}
	if got := env.rows("users", "id", syncedColumn); !strings.Contains(got, "2:2026-01-02T03:04:05Z") || strings.Contains(got, "4:2026") {
		t.Errorf("a patch's synced_at: %s", got)
	}

	// Again, as a repeated replay would: nothing changes.
	patch(t, w, ep(4), "users", batch(t, pfields, strings.ReplaceAll(patches, `"2.5"`, `"again"`)))
	commitEpoch(t, w, ep(4))
	if got := env.held("users", "id", "price"); got != done {
		t.Errorf("after the patches again: %s", got)
	}
}

// An update that doesn't send a held column keeps the version it's held
// for, from the table or from an earlier version in the epoch.
func TestMergeKeepsAnUnchangedColumnsHold(t *testing.T) {
	env := newEnv(t)
	w := env.writer(1, users)
	fields := []arrow.Field{idField, rowID, nameField, docField, opField, seqField, unchangedField, heldField}
	r := func(id int, n byte, name, doc, unchanged, held string) string {
		return fmt.Sprintf(`{"id": %d, "_openetl_id": %s, "name": %q, "doc": %s, "_openetl_op": "update", "_openetl_seq": %d, "_openetl_unchanged": %s, "_openetl_held": %s}`,
			id, uid(n), name, doc, n, unchanged, held)
	}
	writeRows(t, w, ep(1), "users", fields, list(r(1, 1, "a", "null", "null", `["doc"]`)))
	commitEpoch(t, w, ep(1))
	writeRows(t, w, ep(2), "users", fields, list(
		r(1, 2, "a2", "null", `["doc"]`, "null"),
		r(2, 3, "b", "null", "null", `["doc"]`),
		r(2, 4, "b2", "null", `["doc"]`, "null"),
	))
	commitEpoch(t, w, ep(2))
	if got, want := env.held("users", "id", "name", "doc"), "1:a2:-:doc@1,2:b2:-:doc@3"; got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
	pfields := []arrow.Field{idField, rowID, seqField, heldField, docField}
	pd := func(id int, n byte, doc string) string {
		return fmt.Sprintf(`{"id": %d, "_openetl_id": %s, "_openetl_seq": %d, "_openetl_held": ["doc"], "doc": %q}`, id, uid(n), n, doc)
	}
	patch(t, w, ep(3), "users", batch(t, pfields, list(pd(1, 1, "d1"), pd(2, 3, "d2"))))
	commitEpoch(t, w, ep(3))
	if got := env.held("users", "id", "name", "doc"); got != "1:a2:d1:-,2:b2:d2:-" {
		t.Errorf("after the patches: %s", got)
	}
}

// held: last keeps the table's value for a key it has, and takes the
// batch's for one it doesn't.
func TestMergeHeldLast(t *testing.T) {
	env := newEnv(t)
	w := env.writer(1, users)
	last := priceField
	last.Metadata = arrow.NewMetadata([]string{destination.HeldKey}, []string{"last"})
	fields := []arrow.Field{idField, rowID, opField, seqField, last, heldField}
	writeRows(t, w, ep(1), "users", fields, list(row(1, 1, `"1"`, "null")))
	commitEpoch(t, w, ep(1))
	writeRows(t, w, ep(2), "users", fields, list(row(1, 2, "null", `["price"]`), row(2, 3, `"0"`, `["price"]`)))
	commitEpoch(t, w, ep(2))
	if got, want := env.held("users", "id", "price"), "1:1:price@2,2:0:price@3"; got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
	pfields := []arrow.Field{idField, rowID, seqField, heldField, priceField}
	patch(t, w, ep(3), "users", batch(t, pfields, `[{"id": 1, "_openetl_id": `+uid(2)+`, "_openetl_seq": 2, "_openetl_held": ["price"], "price": "2"}]`))
	commitEpoch(t, w, ep(3))
	if got := env.held("users", "id", "price"); got != "1:2:-,2:0:price@3" {
		t.Errorf("after the patch: %s", got)
	}
}

// A keyed append is patched by _openetl_id, its rows rewritten in the
// epoch's snapshot, and a recovery redoes its patches with its rows.
func TestAppendHoldsCellsAndRecoversPatches(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Append, PrimaryKey: []string{"id"}}
	w := env.writer(1, stream)
	fields := []arrow.Field{idField, rowID, opField, seqField, priceField, heldField}
	e1, e2 := ep(1), ep(2)
	writeRows(t, w, e1, "users", fields, list(row(1, 1, "null", `["price"]`), row(1, 2, `"x"`, "null"), row(3, 4, "null", `["price"]`)))
	commitEpoch(t, w, e1)
	if got := env.held("users", "id", "price"); got != "1:-:price@1,1:x:-,3:-:price@4" {
		t.Fatalf("after epoch 1: %s", got)
	}

	pfields := []arrow.Field{idField, rowID, seqField, heldField, priceField}
	writeRows(t, w, e2, "users", fields, list(row(2, 3, `"y"`, "null")))
	patch(t, w, e2, "users", batch(t, pfields, list(
		`{"id": 1, "_openetl_id": `+uid(1)+`, "_openetl_seq": 1, "_openetl_held": ["price"], "price": "1"}`,
		`{"id": 1, "_openetl_id": `+uid(2)+`, "_openetl_seq": 2, "_openetl_held": ["price"], "price": "not held"}`,
		`{"id": 3, "_openetl_id": `+uid(9)+`, "_openetl_seq": 9, "_openetl_held": ["price"], "price": "other version"}`,
	)))
	st := precommit(t, w, e2)

	// The writer dies before its commit; the next generation redoes it.
	w2 := env.open(2, destination.ScopePipeline, stream)
	if r := recov(t, w2, 2, e2, st); r != destination.Redone {
		t.Fatalf("recovery %v", r)
	}
	if got := env.held("users", "id", "price"); got != "1:1:-,1:x:-,2:y:-,3:-:price@4" {
		t.Errorf("after recovery: %s", got)
	}
}

// A patch's column can fill another held column: col__v2 fills col, which
// stops being held and keeps its value.
func TestPatchFillsAnotherColumn(t *testing.T) {
	env := newEnv(t)
	w := env.writer(1, users)
	v2 := arrow.Field{Name: "price__v2", Type: arrow.PrimitiveTypes.Float64, Nullable: true}
	writeRows(t, w, ep(1), "users", []arrow.Field{idField, rowID, opField, seqField, priceField, v2, heldField}, list(row(1, 1, "null", `["price"]`)))
	commitEpoch(t, w, ep(1))
	fills := v2
	fills.Metadata = arrow.NewMetadata([]string{destination.FillsKey}, []string{"price"})
	patch(t, w, ep(2), "users", batch(t, []arrow.Field{idField, rowID, seqField, heldField, fills},
		`[{"id": 1, "_openetl_id": `+uid(1)+`, "_openetl_seq": 1, "_openetl_held": ["price"], "price__v2": 2.5}]`))
	commitEpoch(t, w, ep(2))
	if got := env.held("users", "price", "price__v2"); got != "-:2.5:-" {
		t.Errorf("got %s", got)
	}
}

// A column's type never changes: ApplySchema refuses a type change, with
// the statement that promotes it where Iceberg can, applies the rest, and
// says what each table can't take. Once the user promotes the column, it
// takes the change.
func TestApplySchemaRefusesTypeChanges(t *testing.T) {
	env := newEnv(t)
	w := env.writer(1, users)
	sa := w.(destination.SchemaApplier)
	small := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	wide := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	str := arrow.Field{Name: "n", Type: arrow.BinaryTypes.String, Nullable: true}
	extra := arrow.Field{Name: "extra", Type: arrow.BinaryTypes.String, Nullable: true}
	apply := func(fields ...arrow.Field) destination.SchemaApplied {
		t.Helper()
		a, err := sa.ApplySchema(t.Context(), []destination.StreamSchema{{Name: "users", Schema: arrow.NewSchema(fields, nil)}})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	if a := apply(idField, small); len(a.Refused) != 0 {
		t.Fatalf("refused %+v", a.Refused)
	}
	writeRows(t, w, ep(1), "users", []arrow.Field{idField, small}, `[{"id": 1, "n": 1}]`)
	commitEpoch(t, w, ep(1))

	// The epoch's rows come wider before ApplySchema refuses them.
	writeRows(t, w, ep(2), "users", []arrow.Field{idField, wide, extra}, `[{"id": 2, "n": 5000000000, "extra": "x"}]`)
	a := apply(idField, wide, extra)
	want := destination.RefusedColumn{
		Stream: "users", Column: "n", Reason: "it doesn't change a column's type unless the pipeline's typeChanges allows it", FromType: "int", ToType: "long",
		Fix: "ALTER TABLE `" + env.ns + "`.`users` ALTER COLUMN `n` TYPE BIGINT",
	}
	if len(a.Refused) != 1 || a.Refused[0] != want {
		t.Fatalf("refused %+v", a.Refused)
	}
	sc := env.table("users").Schema()
	if f, _ := sc.FindFieldByName("n"); f.Type.String() != "int" {
		t.Errorf("n is %s", f.Type)
	}
	if _, ok := sc.FindFieldByName("extra"); !ok {
		t.Errorf("extra wasn't added")
	}
	if a := apply(idField, str); len(a.Refused) != 1 || a.Refused[0].Fix != "" || !strings.Contains(a.Refused[0].Reason, "can't change int to string") {
		t.Errorf("refused %+v", a.Refused)
	}

	// The user promotes it and makes extra required, after a write left n
	// out: the epoch is read again.
	writeRows(t, w, ep(3), "users", []arrow.Field{idField, wide}, `[{"id": 3, "n": 5000000000}]`)
	tbl := env.table("users")
	tx := tbl.NewTransaction()
	us := tx.UpdateSchema(true, true).
		UpdateColumn([]string{"n"}, table.ColumnUpdate{FieldType: ice.Optional[ice.Type]{Valid: true, Val: ice.PrimitiveTypes.Int64}}).
		UpdateColumn([]string{"extra"}, table.ColumnUpdate{Required: ice.Optional[bool]{Valid: true, Val: true}})
	if err := us.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := sa.ApplySchema(t.Context(), []destination.StreamSchema{{Name: "users", Schema: arrow.NewSchema([]arrow.Field{idField, wide}, nil)}}); err == nil || !strings.Contains(err.Error(), "read the epoch again") {
		t.Fatalf("ApplySchema after n was left out: %v", err)
	}
	a = apply(idField, wide, extra)
	if len(a.Refused) != 0 {
		t.Errorf("refused %+v after the user's change", a.Refused)
	}
	if l := a.Limits["users"]; !slices.Equal(l.NotNull, []string{"extra"}) || len(l.Fixed) != 0 || l.MaxColumns != 0 {
		t.Errorf("limits %+v", l)
	}
}
