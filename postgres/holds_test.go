//go:build integration

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

var (
	heldField   = arrow.Field{Name: heldColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
	priceField  = arrow.Field{Name: "price", Type: arrow.BinaryTypes.String, Nullable: true}
	syncedField = arrow.Field{Name: syncedColumn, Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true}
)

// uidText is uid(n) as the target's _openetl_held names it.
func uidText(n byte) string {
	h := strings.Repeat(fmt.Sprintf("%02x", n), 16)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
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

const prices = `SELECT string_agg(id || ':' || coalesce(price, '-') || ':' || coalesce(_openetl_held::text, '-'), ',' ORDER BY id) FROM $schema.users`

// A held cell's target row names the version whose value is held, and a
// patch fills it only for that version: in any order, once, and never over
// a newer value.
func TestMergeHoldsCellsAndPatchesThem(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, rowID, opField, seqField, syncedField, priceField, heldField}
	pfields := []arrow.Field{idField, rowID, seqField, syncedField, heldField, priceField}
	e := func(n uint64) destination.Epoch { return destination.Epoch{Generation: 1, N: n} }

	write(t, w, e(1), "users", batch(t, fields, "["+strings.Join([]string{
		row(1, 1, `"1"`, "null"),
		row(2, 2, "null", `["price"]`),
		row(4, 4, "null", `["price"]`),
		row(5, 10, "null", `["price"]`),
		row(6, 12, "null", `["price"]`),
	}, ",")+"]"))
	commit(t, w, e(1))
	want := fmt.Sprintf(`1:1:-,2:-:{"price": "%s"},4:-:{"price": "%s"},5:-:{"price": "%s"},6:-:{"price": "%s"}`, uidText(2), uidText(4), uidText(10), uidText(12))
	if got := env.text(prices); got != want {
		t.Fatalf("after epoch 1:\n got %s\nwant %s", got, want)
	}

	// Key 4 gets a newer value, key 5 a newer held version, and key 6 is
	// deleted, which keeps its columns.
	write(t, w, e(2), "users", batch(t, fields, "["+strings.Join([]string{
		row(4, 5, `"live"`, "null"),
		row(5, 11, "null", `["price"]`),
		`{"id": 6, "_openetl_id": ` + uid(13) + `, "_openetl_op": "delete", "_openetl_seq": 13, "_openetl_held": null}`,
	}, ",")+"]"))
	commit(t, w, e(2))

	p := func(id int, n byte, price string) string {
		return fmt.Sprintf(`{"id": %d, "_openetl_id": %s, "_openetl_seq": %d, "_openetl_synced_at": "2026-01-02T03:04:05Z", "_openetl_held": ["price"], "price": %s}`, id, uid(n), n, price)
	}
	patches := "[" + strings.Join([]string{
		p(2, 9, `"other version"`),
		p(2, 2, `"2.5"`),
		p(3, 3, `"no such key"`),
		p(4, 4, `"stale"`),
		p(5, 10, `"older version"`),
		p(5, 11, `"5"`),
		p(6, 12, `"6"`),
	}, ",") + "]"
	patch(t, w, e(3), "users", batch(t, pfields, patches))
	commit(t, w, e(3))
	const done = `1:1:-,2:2.5:-,4:live:-,5:5:-,6:6:-`
	if got := env.text(prices); got != done {
		t.Fatalf("after the patches:\n got %s\nwant %s", got, done)
	}
	if got := env.text("SELECT _openetl_synced_at::text FROM $schema.users WHERE id = 2"); got != "2026-01-02 03:04:05+00" {
		t.Errorf("a patch's synced_at: %s", got)
	}
	if n := env.count("SELECT count(*) FROM $schema.users WHERE id = 3"); n != 0 {
		t.Errorf("a patch inserted key 3")
	}

	// Again, as a repeated replay would: nothing changes.
	patch(t, w, e(4), "users", batch(t, pfields, strings.ReplaceAll(patches, `"2.5"`, `"again"`)))
	commit(t, w, e(4))
	if got := env.text(prices); got != done {
		t.Errorf("after the patches again: %s", got)
	}
}

// An update that doesn't send a held column keeps the version it's held
// for, from the target or from an earlier version in the epoch.
func TestMergeKeepsAnUnchangedColumnsHold(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, rowID, nameField, docField, opField, seqField, unchangedField, heldField}
	r := func(id int, n byte, name, doc, unchanged, held string) string {
		return fmt.Sprintf(`{"id": %d, "_openetl_id": %s, "name": %q, "doc": %s, "_openetl_op": "update", "_openetl_seq": %d, "_openetl_unchanged": %s, "_openetl_held": %s}`,
			id, uid(n), name, doc, n, unchanged, held)
	}
	e := func(n uint64) destination.Epoch { return destination.Epoch{Generation: 1, N: n} }
	write(t, w, e(1), "users", batch(t, fields, "["+r(1, 1, "a", "null", "null", `["doc"]`)+"]"))
	commit(t, w, e(1))
	write(t, w, e(2), "users", batch(t, fields, "["+strings.Join([]string{
		r(1, 2, "a2", "null", `["doc"]`, "null"),
		r(2, 3, "b", "null", "null", `["doc"]`),
		r(2, 4, "b2", "null", `["doc"]`, "null"),
	}, ",")+"]"))
	commit(t, w, e(2))
	const held = `SELECT string_agg(id || ':' || name || ':' || coalesce(doc, '-') || ':' || coalesce(_openetl_held::text, '-'), ',' ORDER BY id) FROM $schema.users`
	want := fmt.Sprintf(`1:a2:-:{"doc": "%s"},2:b2:-:{"doc": "%s"}`, uidText(1), uidText(3))
	if got := env.text(held); got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}

	pfields := []arrow.Field{idField, rowID, seqField, heldField, docField}
	p := func(id int, n byte, doc string) string {
		return fmt.Sprintf(`{"id": %d, "_openetl_id": %s, "_openetl_seq": %d, "_openetl_held": ["doc"], "doc": %q}`, id, uid(n), n, doc)
	}
	patch(t, w, e(3), "users", batch(t, pfields, "["+p(1, 1, "d1")+","+p(2, 3, "d2")+"]"))
	commit(t, w, e(3))
	if got := env.text(held); got != "1:a2:d1:-,2:b2:d2:-" {
		t.Errorf("after the patches: %s", got)
	}
}

// held: last keeps the target's value for a key it has, and takes the
// batch's for one it doesn't.
func TestMergeHeldLast(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w, 1, destination.Epoch{}, nil)
	last := priceField
	last.Metadata = arrow.NewMetadata([]string{destination.HeldKey}, []string{"last"})
	fields := []arrow.Field{idField, rowID, opField, seqField, last, heldField}
	e := func(n uint64) destination.Epoch { return destination.Epoch{Generation: 1, N: n} }
	write(t, w, e(1), "users", batch(t, fields, "["+row(1, 1, `"1"`, "null")+"]"))
	commit(t, w, e(1))
	write(t, w, e(2), "users", batch(t, fields, "["+row(1, 2, "null", `["price"]`)+","+row(2, 3, `"0"`, `["price"]`)+"]"))
	commit(t, w, e(2))
	want := fmt.Sprintf(`1:1:{"price": "%s"},2:0:{"price": "%s"}`, uidText(2), uidText(3))
	if got := env.text(prices); got != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
	pfields := []arrow.Field{idField, rowID, seqField, heldField, priceField}
	patch(t, w, e(3), "users", batch(t, pfields, `[{"id": 1, "_openetl_id": `+uid(2)+`, "_openetl_seq": 2, "_openetl_held": ["price"], "price": "2"}]`))
	commit(t, w, e(3))
	if got := env.text(prices); got != fmt.Sprintf(`1:2:-,2:0:{"price": "%s"}`, uidText(3)) {
		t.Errorf("after the patch: %s", got)
	}
}

// A keyed append is patched by _openetl_id, in the epoch's transaction, and
// a recovery redoes its patches with its rows.
func TestAppendHoldsCellsAndRecoversPatches(t *testing.T) {
	env := newEnv(t)
	stream := destination.Stream{Name: "users", Mode: destination.Append, PrimaryKey: []string{"id"}}
	w := env.openStreams(1, destination.ScopePipeline, stream)
	recov(t, w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, rowID, opField, seqField, priceField, heldField}
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, w, e1, "users", batch(t, fields, "["+row(1, 1, "null", `["price"]`)+","+row(1, 2, `"x"`, "null")+"]"))
	commit(t, w, e1)
	if n := env.count(`SELECT count(*) FROM $schema.users WHERE _openetl_held = '{"price": "` + uidText(1) + `"}'`); n != 1 {
		t.Fatalf("%d rows hold version 1", n)
	}

	pfields := []arrow.Field{idField, rowID, seqField, heldField, priceField}
	write(t, w, e2, "users", batch(t, fields, "["+row(2, 3, `"y"`, "null")+"]"))
	patch(t, w, e2, "users", batch(t, pfields, `[{"id": 1, "_openetl_id": `+uid(1)+`, "_openetl_seq": 1, "_openetl_held": ["price"], "price": "1"}]`))
	st := precommit(t, w, e2)

	// The writer dies before its commit; the next generation redoes it.
	w2 := env.openStreams(2, destination.ScopePipeline, stream)
	if r := recov(t, w2, 2, e2, st); r != destination.Redone {
		t.Fatalf("recovery %v", r)
	}
	const rows = `SELECT string_agg(id || ':' || coalesce(price, '-') || ':' || coalesce(_openetl_held::text, '-'), ',' ORDER BY id, price) FROM $schema.users`
	if got := env.text(rows); got != "1:1:-,1:x:-,2:y:-" {
		t.Errorf("after recovery: %s", got)
	}
	if n := env.count("SELECT count(*) FROM pg_tables WHERE schemaname = '" + env.schema + "' AND tablename LIKE '_openetl_stage_%'"); n != 0 {
		t.Errorf("%d staging tables left", n)
	}
}

// A column's type never changes, so ApplySchema refuses a widening and
// applies the rest, and says up front what each table can't take. No
// column is fixed: one the user has altered takes the change.
func TestApplySchemaRefusesWhatItCantChange(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.open(1, destination.ScopePipeline, "users")
	recov(t, w, 1, destination.Epoch{}, nil)
	small := arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	applySchema(t, w, "users", []arrow.Field{idField, small, nameField, priceField})
	env.exec("ALTER TABLE $schema.users ALTER COLUMN name SET NOT NULL")

	sa := w.(destination.SchemaApplier)
	wide := arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	z := arrow.Field{Name: "z", Type: arrow.BinaryTypes.String, Nullable: true}
	wider := arrow.Field{Name: "price", Type: arrow.BinaryTypes.String, Nullable: true}
	applied, err := sa.ApplySchema(ctx, []destination.StreamSchema{{Name: "users", Schema: arrow.NewSchema([]arrow.Field{idField, wide, nameField, wider, z}, nil)}})
	if err != nil {
		t.Fatal(err)
	}
	want := destination.RefusedColumn{
		Stream: "users", Column: "x", Reason: "it doesn't change a column's type, which can rewrite the table", FromType: "integer", ToType: "bigint",
		Fix: `ALTER TABLE "` + env.schema + `"."users" ALTER COLUMN "x" TYPE bigint`,
	}
	if len(applied.Refused) != 1 || applied.Refused[0] != want {
		t.Fatalf("refused %+v", applied.Refused)
	}
	l := applied.Limits["users"]
	if len(l.Fixed) != 0 || strings.Join(l.NotNull, ",") != "name" || l.MaxColumns != 1600 {
		t.Errorf("limits %+v", l)
	}
	if l.Columns != 5 {
		t.Errorf("%d columns, want 5", l.Columns)
	}
	typ := func(c string) string {
		return env.text("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = '" + env.schema + ".users'::regclass AND attname = '" + c + "'")
	}
	if typ("x") != "integer" || typ("z") != "text" {
		t.Errorf("x is %s and z is %q", typ("x"), typ("z"))
	}

	// Once the user has run the fix, the column takes the change.
	env.exec(want.Fix)
	applied, err = sa.ApplySchema(ctx, []destination.StreamSchema{{Name: "users", Schema: arrow.NewSchema([]arrow.Field{idField, wide}, nil)}})
	if err != nil || len(applied.Refused) != 0 || typ("x") != "bigint" {
		t.Errorf("after altering x: %+v, %v, x is %s", applied, err, typ("x"))
	}
}

// A schema change waits for its table's lock only a while, so readers don't
// queue behind it, and tries again.
func TestSchemaChangesWaitForLocksBriefly(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	timeout := lockTimeout
	lockTimeout = 200 * time.Millisecond
	t.Cleanup(func() { lockTimeout = timeout })
	w := env.open(1, destination.ScopePipeline, "users")
	recov(t, w, 1, destination.Epoch{}, nil)
	small := arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	applySchema(t, w, "users", []arrow.Field{idField, small})

	tx, err := env.conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "LOCK TABLE "+w.(*writer).ident("users")+" IN ACCESS SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(700 * time.Millisecond)
		_ = tx.Rollback(context.Background())
	}()
	start := time.Now()
	applySchema(t, w, "users", []arrow.Field{idField, small, nameField})
	if d := time.Since(start); d < 600*time.Millisecond {
		t.Errorf("the change didn't wait for the lock: %v", d)
	}
	if n := env.count("SELECT count(*) FROM pg_attribute WHERE attrelid = '" + env.schema + ".users'::regclass AND attname = 'name'"); n != 1 {
		t.Error("name wasn't added")
	}
}

// Columns added since the catalog read still count toward Postgres's limit:
// the column past it is refused, and the rest added.
func TestAlterFindsTheColumnItCantAdd(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.open(1, destination.ScopePipeline, "users")
	recov(t, w, 1, destination.Epoch{}, nil)
	applySchema(t, w, "users", []arrow.Field{idField})
	wr := w.(*writer)
	have, err := wr.catalog(ctx, []string{"users"})
	if err != nil {
		t.Fatal(err)
	}
	adds := make([]string, maxColumns-2)
	for i := range adds {
		adds[i] = fmt.Sprintf("ADD COLUMN c%d int", i)
	}
	env.exec("ALTER TABLE $schema.users " + strings.Join(adds, ", "))
	cols, err := columnsOf(arrow.NewSchema([]arrow.Field{idField, nameField, priceField}, nil))
	if err != nil {
		t.Fatal(err)
	}
	refused, err := wr.alter(ctx, "users", cols, have["users"])
	if err != nil {
		t.Fatal(err)
	}
	if len(refused) != 1 || refused[0].Column != "price" || !strings.HasPrefix(refused[0].Reason, "it takes no more columns: ") || refused[0].ToType != "text" || refused[0].Fix != "" {
		t.Errorf("refused %+v", refused)
	}
	if n := env.count("SELECT count(*) FROM pg_attribute WHERE attrelid = '" + env.schema + ".users'::regclass AND attname = 'name'"); n != 1 {
		t.Error("name wasn't added")
	}
}

// A patch's column can fill another held column, as col__v2 fills col's.
func TestPatchFillsAnotherColumn(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w, 1, destination.Epoch{}, nil)
	v2 := arrow.Field{Name: "price__v2", Type: arrow.PrimitiveTypes.Float64, Nullable: true}
	e := func(n uint64) destination.Epoch { return destination.Epoch{Generation: 1, N: n} }
	write(t, w, e(1), "users", batch(t, []arrow.Field{idField, rowID, opField, seqField, priceField, v2, heldField}, "["+row(1, 1, "null", `["price"]`)+"]"))
	commit(t, w, e(1))
	fills := v2
	fills.Metadata = arrow.NewMetadata([]string{destination.FillsKey}, []string{"price"})
	patch(t, w, e(2), "users", batch(t, []arrow.Field{idField, rowID, seqField, heldField, fills},
		`[{"id": 1, "_openetl_id": `+uid(1)+`, "_openetl_seq": 1, "_openetl_held": ["price"], "price__v2": 2.5}]`))
	commit(t, w, e(2))
	if got := env.text("SELECT coalesce(price, '-') || ':' || price__v2 || ':' || coalesce(_openetl_held::text, '-') FROM $schema.users"); got != "-:2.5:-" {
		t.Errorf("got %s", got)
	}
}
