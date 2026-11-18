//go:build integration

package sqlserver

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// env is a destination writing a fresh schema, which the connectors' login
// creates and owns.
type env struct {
	t      *testing.T
	d      *Destination
	db     *sql.DB
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
	t.Cleanup(func() { _ = d.db.Close() })
	db := admin(t)
	t.Cleanup(func() { dropSchema(db, schema) })
	return &env{t: t, d: d, db: db, schema: schema}
}

func (e *env) openStreams(gen uint64, scope destination.Scope, streams ...destination.Stream) destination.Writer {
	e.t.Helper()
	w, err := e.d.Open(e.t.Context(), destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: scope, Streams: streams})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

func (e *env) rows(q string) []string {
	e.t.Helper()
	rows, err := e.db.QueryContext(e.t.Context(), strings.ReplaceAll(q, "$schema", quote(e.schema)))
	if err != nil {
		e.t.Fatalf("%s: %v", q, err)
	}
	defer func() { _ = rows.Close() }()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			e.t.Fatal(err)
		}
		cells := make([]string, len(cols))
		for i, v := range vals {
			cells[i] = "null"
			if v.Valid {
				cells[i] = v.String
			}
		}
		out = append(out, strings.Join(cells, ","))
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return out
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
	// The control columns as the engine names them for destinations.
	opField      = arrow.Field{Name: opColumn, Type: arrow.BinaryTypes.String, Nullable: true}
	seqField     = arrow.Field{Name: seqColumn, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	unchField    = arrow.Field{Name: unchangedColumn, Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true}
	deletedField = arrow.Field{Name: deletedColumn, Type: arrow.FixedWidthTypes.Boolean, Nullable: true}
)

func recov(t *testing.T, w destination.Writer, gen uint64, e destination.Epoch, st ...destination.Staged) destination.Recovery {
	t.Helper()
	r, err := w.Recover(t.Context(), gen, e, st)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// commit writes batches of one stream in epoch n, and commits them.
func commit(t *testing.T, w destination.Writer, n uint64, stream string, batches ...arrow.RecordBatch) {
	t.Helper()
	e := destination.Epoch{Generation: 1, N: n}
	for _, b := range batches {
		if err := w.Write(t.Context(), e, stream, b); err != nil {
			t.Fatal(err)
		}
	}
	st, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), e, []destination.Staged{st}); err != nil {
		t.Fatal(err)
	}
}

func TestCommitAppliesOnce(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Append})
	if r := recov(t, w, 1, destination.Epoch{}); r != destination.Applied {
		t.Fatalf("Recover = %v", r)
	}
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}, {"id": 2}]`)); err != nil {
		t.Fatal(err)
	}
	// A column appears mid-epoch, and another widens.
	if err := w.Write(ctx, e, "users", batch(t, []arrow.Field{idField, nameField}, `[{"id": 3, "name": "c"}]`)); err != nil {
		t.Fatal(err)
	}
	st, err := w.PreCommit(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if got := env.rows("SELECT count(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('" + env.schema + "') AND name = 'users'"); got[0] != "0" {
		t.Fatal("PreCommit made the target visible")
	}
	for range 2 {
		if _, err := w.Commit(ctx, e, []destination.Staged{st}); err != nil {
			t.Fatal(err)
		}
	}
	if got := env.rows("SELECT id, name FROM $schema.users ORDER BY id"); !slices.Equal(got, []string{"1,null", "2,null", "3,c"}) {
		t.Errorf("users = %v", got)
	}
	if got := env.rows("SELECT count(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('" + env.schema + "') AND name LIKE '\\_openetl\\_stage\\_%' ESCAPE '\\'"); got[0] != "0" {
		t.Errorf("%s staging tables left", got[0])
	}
}

// A merge applies each key's latest version by seq, keeps the columns an
// update didn't send, and soft-deletes.
func TestMergeUnchangedAndDeletes(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w, 1, destination.Epoch{})
	fields := []arrow.Field{idField, nameField, {Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, opField, seqField, unchField, deletedField}
	commit(t, w, 1, "users", batch(t, fields, `[
		{"id": 1, "name": "a", "n": 1, "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
		{"id": 2, "name": "b", "n": 2, "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
		{"id": 3, "name": "c", "n": 3, "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false}]`))
	commit(t, w, 2, "users", batch(t, fields, `[
		{"id": 1, "name": null, "n": 10, "_openetl_op": "update", "_openetl_seq": 3, "_openetl_unchanged": ["name"], "_openetl_deleted": false},
		{"id": 1, "name": "older", "n": 9, "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false},
		{"id": 2, "_openetl_op": "delete", "_openetl_seq": 2, "_openetl_deleted": true},
		{"id": 3, "name": "stale", "n": 0, "_openetl_op": "update", "_openetl_seq": 0, "_openetl_deleted": false},
		{"id": 4, "name": "d", "n": 4, "_openetl_op": "insert", "_openetl_seq": 5, "_openetl_deleted": false}]`))
	got := env.rows("SELECT id, name, n, _openetl_deleted, _openetl_seq FROM $schema.users ORDER BY id")
	want := []string{"1,older,10,false,3", "2,b,2,true,2", "3,c,3,false,1", "4,d,4,false,5"}
	if !slices.Equal(got, want) {
		t.Errorf("users = %v, want %v", got, want)
	}
}

// A column the latest version didn't send comes from the version staged
// before it that did, where the versions have no seq too.
func TestMergeUnchangedWithoutSeq(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w, 1, destination.Epoch{})
	fields := []arrow.Field{idField, nameField, {Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, opField, seqField, unchField, deletedField}
	commit(t, w, 1, "users", batch(t, fields, `[
		{"id": 1, "name": "a", "n": 1, "_openetl_op": "insert", "_openetl_deleted": false},
		{"id": 1, "name": null, "n": 2, "_openetl_op": "update", "_openetl_unchanged": ["name"], "_openetl_deleted": false},
		{"id": 2, "name": "b", "n": 1, "_openetl_op": "insert", "_openetl_deleted": false},
		{"id": 2, "name": null, "n": 2, "_openetl_op": "update", "_openetl_seq": 4, "_openetl_unchanged": ["name"], "_openetl_deleted": false}]`))
	got := env.rows("SELECT id, name, n FROM $schema.users ORDER BY id")
	if want := []string{"1,a,2", "2,b,2"}; !slices.Equal(got, want) {
		t.Errorf("users = %v, want %v", got, want)
	}
}

// A column the latest version didn't send keeps the target's value where
// the staged version that sent it is older than the target's row, or where
// no staged version sent it.
func TestMergeUnchangedOlderThanTarget(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "users", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	recov(t, w, 1, destination.Epoch{})
	fields := []arrow.Field{idField, nameField, {Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, opField, seqField, unchField, deletedField}
	commit(t, w, 1, "users", batch(t, fields, `[
		{"id": 1, "name": "current", "n": 10, "_openetl_op": "insert", "_openetl_seq": 10, "_openetl_deleted": false},
		{"id": 2, "name": "current", "n": 10, "_openetl_op": "insert", "_openetl_seq": 10, "_openetl_deleted": false},
		{"id": 3, "name": "current", "n": 10, "_openetl_op": "insert", "_openetl_seq": 10, "_openetl_deleted": false}]`))
	commit(t, w, 2, "users", batch(t, fields, `[
		{"id": 1, "name": "old", "n": 5, "_openetl_op": "update", "_openetl_seq": 5, "_openetl_deleted": false},
		{"id": 1, "name": null, "n": 11, "_openetl_op": "update", "_openetl_seq": 11, "_openetl_unchanged": ["name"], "_openetl_deleted": false},
		{"id": 2, "name": "newer", "n": 12, "_openetl_op": "update", "_openetl_seq": 12, "_openetl_deleted": false},
		{"id": 2, "name": null, "n": 13, "_openetl_op": "update", "_openetl_seq": 13, "_openetl_unchanged": ["name"], "_openetl_deleted": false},
		{"id": 3, "name": null, "n": 11, "_openetl_op": "update", "_openetl_seq": 11, "_openetl_unchanged": ["name"], "_openetl_deleted": false}]`))
	got := env.rows("SELECT id, name, n, _openetl_seq, _openetl_unchanged FROM $schema.users ORDER BY id")
	if want := []string{"1,current,11,11,null", "2,newer,13,13,null", "3,current,11,11,null"}; !slices.Equal(got, want) {
		t.Errorf("users = %v, want %v", got, want)
	}
}

func TestMergeHardDeletesAndStringKeys(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "tags", Mode: destination.Merge, PrimaryKey: []string{"name"}, Deletes: destination.HardDelete})
	recov(t, w, 1, destination.Epoch{})
	fields := []arrow.Field{nameField, {Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, opField, seqField}
	commit(t, w, 1, "tags", batch(t, fields, `[
		{"name": "a", "n": 1, "_openetl_op": "insert", "_openetl_seq": 1},
		{"name": "A", "n": 2, "_openetl_op": "insert", "_openetl_seq": 1},
		{"name": "a ", "n": 3, "_openetl_op": "insert", "_openetl_seq": 1},
		{"name": "b", "n": 5, "_openetl_op": "insert", "_openetl_seq": 1},
		{"name": "b ", "n": 6, "_openetl_op": "insert", "_openetl_seq": 1},
		{"name": "gone", "n": 4, "_openetl_op": "insert", "_openetl_seq": 1}]`))
	commit(t, w, 2, "tags", batch(t, fields, `[
		{"name": "gone", "_openetl_op": "delete", "_openetl_seq": 2},
		{"name": "b ", "_openetl_op": "delete", "_openetl_seq": 2},
		{"name": "A", "n": 20, "_openetl_op": "update", "_openetl_seq": 2}]`))
	// The database's default collation takes a, A and "a " as one, but the
	// source's keys differ in case and trailing spaces, and so do the target's.
	got := env.rows("SELECT CONCAT('[', name, ']'), n FROM $schema.tags ORDER BY n")
	want := []string{"[a],1", "[a ],3", "[b],5", "[A],20"}
	if !slices.Equal(got, want) {
		t.Errorf("tags = %v, want %v", got, want)
	}
	// The key is bounded, and indexed.
	got = env.rows(`SELECT TYPE_NAME(c.system_type_id), c.max_length, i.is_unique FROM sys.columns c
		JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		WHERE c.object_id = OBJECT_ID('$schema.tags') AND c.name = 'name'`)
	if want := []string{"nvarchar,900,false"}; !slices.Equal(got, want) {
		t.Errorf("key column and index %v, want %v", got, want)
	}
	// A key longer than its column holds fails the write, counted in UTF-16.
	for _, k := range []string{strings.Repeat("x", 451), strings.Repeat("\U0001F600", 226)} {
		err := w.Write(t.Context(), destination.Epoch{Generation: 1, N: 3}, "tags", batch(t, fields, `[{"name": "`+k+`", "n": 7}]`))
		if err == nil || !strings.Contains(err.Error(), "longer than the 450 a key column holds") {
			t.Errorf("a key of %d runes: %v", len([]rune(k)), err)
		}
	}
	commit(t, w, 3, "tags", batch(t, fields, `[{"name": "`+strings.Repeat("\U0001F600", 225)+`", "n": 7}]`))
	if got := env.rows("SELECT n FROM $schema.tags WHERE LEN(name) = 450"); !slices.Equal(got, []string{"7"}) {
		t.Errorf("a key of 450 UTF-16 units: %v", got)
	}
}

// A table the user made keeps its collation, and the merge still tells
// keys apart as the source does: one whose primary key takes two of them as
// one refuses the second, rather than merge them.
func TestMergeIntoTablesOfTheirOwnCollation(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline,
		destination.Stream{Name: "cs", Mode: destination.Merge, PrimaryKey: []string{"name"}},
		destination.Stream{Name: "ci", Mode: destination.Merge, PrimaryKey: []string{"name"}})
	recov(t, w, 1, destination.Epoch{})
	for _, q := range []string{
		"CREATE TABLE $schema.cs (name nvarchar(450) COLLATE Latin1_General_100_CS_AS NOT NULL PRIMARY KEY, n bigint)",
		"CREATE TABLE $schema.ci (name nvarchar(450) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL PRIMARY KEY, n bigint)",
	} {
		if _, err := env.db.ExecContext(t.Context(), strings.ReplaceAll(q, "$schema", quote(env.schema))); err != nil {
			t.Fatal(err)
		}
	}
	fields := []arrow.Field{nameField, {Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, opField, seqField}
	commit(t, w, 1, "cs", batch(t, fields, `[{"name": "a", "n": 1, "_openetl_seq": 1}, {"name": "A", "n": 2, "_openetl_seq": 1}]`))
	commit(t, w, 2, "cs", batch(t, fields, `[{"name": "a", "n": 10, "_openetl_op": "update", "_openetl_seq": 2}]`))
	if got := env.rows("SELECT name, n FROM $schema.cs ORDER BY n"); !slices.Equal(got, []string{"A,2", "a,10"}) {
		t.Errorf("cs = %v", got)
	}
	commit(t, w, 3, "ci", batch(t, fields, `[{"name": "a", "n": 1, "_openetl_seq": 1}]`))
	e := destination.Epoch{Generation: 1, N: 4}
	if err := w.Write(t.Context(), e, "ci", batch(t, fields, `[{"name": "A", "n": 2, "_openetl_seq": 2}]`)); err != nil {
		t.Fatal(err)
	}
	st, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), e, []destination.Staged{st}); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("commit of A after a into a case-insensitive key: %v", err)
	}
	if got := env.rows("SELECT name, n FROM $schema.ci"); !slices.Equal(got, []string{"a,1"}) {
		t.Errorf("ci = %v", got)
	}
}

// A key column of a table of yours shorter than a merge's 450 characters is
// refused, with the ALTER that lengthens it, which SQL Server makes under a
// primary key, or lengthened where the pipeline allows it; a longer one takes
// the keys as it is. A change of type under your index has no such fix.
func TestMergeIntoKeysOfTheirOwnLength(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	stream := func(name string, allow bool) destination.Stream {
		return destination.Stream{Name: name, Mode: destination.Merge, PrimaryKey: []string{"name"}, AllowTypeChanges: allow}
	}
	w := env.openStreams(1, destination.ScopePipeline, stream("narrow", false), stream("allowed", false), stream("wide", false))
	recov(t, w, 1, destination.Epoch{})
	for _, q := range []string{
		"CREATE TABLE $schema.narrow (name nvarchar(50) NOT NULL PRIMARY KEY, n bigint)",
		"CREATE TABLE $schema.allowed (name nvarchar(50) NOT NULL PRIMARY KEY, n bigint)",
		"CREATE TABLE $schema.wide (name nvarchar(500) NOT NULL PRIMARY KEY, n bigint)",
		"CREATE TABLE $schema.ascii (name varchar(20) NOT NULL CONSTRAINT ascii_pk PRIMARY KEY, n bigint)",
	} {
		if _, err := env.db.ExecContext(ctx, strings.ReplaceAll(q, "$schema", quote(env.schema))); err != nil {
			t.Fatal(err)
		}
	}
	fields := []arrow.Field{nameField, {Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, seqField}
	apply := func(name string, allow bool) []destination.RefusedColumn {
		w := env.openStreams(1, destination.ScopePipeline, stream(name, allow))
		res, err := w.(destination.SchemaApplier).ApplySchema(ctx, []destination.StreamSchema{{Name: name, Schema: arrow.NewSchema(fields, nil)}})
		if err != nil {
			t.Fatal(err)
		}
		return res.Refused
	}
	got := statuses(t, env.d.Preflight(destination.PreflightRequest{
		Streams: []destination.Stream{stream("narrow", false), stream("ascii", true), stream("wide", false)},
		Schemas: map[string]*arrow.Schema{"narrow": arrow.NewSchema(fields, nil), "ascii": arrow.NewSchema(fields, nil), "wide": arrow.NewSchema(fields, nil)},
	}))
	wantStatus(t, got, "stream.narrow.write", "warning: name of narrow is nvarchar(50), the stream's is nvarchar(450)")
	wantStatus(t, got, "stream.ascii.write", "warning: name of ascii is varchar(20), the stream's is nvarchar(450), and SQL Server changes a column in your index ascii_pk only to a longer length")
	wantStatus(t, got, "stream.wide.write", "passed")

	r := apply("narrow", false)
	if len(r) != 1 || r[0].FromType != "nvarchar(50)" || r[0].ToType != "nvarchar(450)" || r[0].Fix == "" {
		t.Fatalf("narrow: refused %+v", r)
	}
	if _, err := env.db.ExecContext(ctx, r[0].Fix); err != nil {
		t.Fatalf("%s: %v", r[0].Fix, err)
	}
	if r := apply("narrow", false); len(r) != 0 {
		t.Errorf("narrow, after the fix: refused %+v", r)
	}
	if r := apply("allowed", true); len(r) != 0 {
		t.Errorf("allowed: refused %+v", r)
	}
	if r := apply("wide", false); len(r) != 0 {
		t.Errorf("wide: refused %+v", r)
	}
	for _, allow := range []bool{false, true} {
		if r := apply("ascii", allow); len(r) != 1 || r[0].Fix != "" {
			t.Errorf("ascii, allowing type changes %v: refused %+v", allow, r)
		}
	}
	if got := env.rows("SELECT OBJECT_NAME(object_id), max_length FROM sys.columns WHERE name = 'name' AND OBJECT_SCHEMA_NAME(object_id) = '" + env.schema + "' ORDER BY 1"); !slices.Equal(got, []string{"allowed,900", "ascii,20", "narrow,900", "wide,1000"}) {
		t.Errorf("key columns %v", got)
	}
	for i, name := range []string{"narrow", "allowed", "wide"} {
		commit(t, w, uint64(i+1), name, batch(t, fields, `[{"name": "a", "n": 1, "_openetl_seq": 1}, {"name": "`+strings.Repeat("x", 300)+`", "n": 2, "_openetl_seq": 1}]`))
		if got := env.rows("SELECT LEN(name), n FROM $schema." + name + " ORDER BY n"); !slices.Equal(got, []string{"1,1", "300,2"}) {
			t.Errorf("%s = %v", name, got)
		}
	}
}

// Two text key columns' index can be longer than the 1,700 bytes SQL Server
// takes for a row's key: a row whose key is longer fails its commit.
func TestMergeCompositeKeyPastIndexLimit(t *testing.T) {
	env := newEnv(t)
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"a", "b"}})
	recov(t, w, 1, destination.Epoch{})
	fields := []arrow.Field{{Name: "a", Type: arrow.BinaryTypes.String}, {Name: "b", Type: arrow.BinaryTypes.String}, seqField}
	row := func(n int) string {
		return fmt.Sprintf(`[{"a": %q, "b": %q, "_openetl_seq": 1}]`, strings.Repeat("a", n), strings.Repeat("b", n))
	}
	commit(t, w, 1, "m", batch(t, fields, row(425)))
	e := destination.Epoch{Generation: 1, N: 2}
	if err := w.Write(t.Context(), e, "m", batch(t, fields, row(450))); err != nil {
		t.Fatal(err)
	}
	st, err := w.PreCommit(t.Context(), e)
	if err == nil {
		_, err = w.Commit(t.Context(), e, []destination.Staged{st})
	}
	if err == nil || !strings.Contains(err.Error(), "1700 bytes") {
		t.Errorf("commit of a 1,800-byte key: %v", err)
	}
	if got := env.rows("SELECT LEN(a) + LEN(b) FROM $schema.m"); !slices.Equal(got, []string{"850"}) {
		t.Errorf("m = %v", got)
	}
}

func TestDedupe(t *testing.T) {
	env := newEnv(t)
	idCol := arrow.Field{Name: idColumn, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true,
		Metadata: arrow.NewMetadata([]string{extensionNameKey}, []string{uuidExtension})}
	w := env.openStreams(1, destination.ScopePipeline,
		destination.Stream{Name: "target", Mode: destination.Append, Dedupe: destination.DedupeTarget},
		destination.Stream{Name: "epoch", Mode: destination.Append, Dedupe: destination.DedupeEpoch})
	recov(t, w, 1, destination.Epoch{})
	fields := []arrow.Field{idField, idCol}
	rows := `[{"id": 1, "_openetl_id": "AAAAAAAAAAAAAAAAAAAAAQ=="}, {"id": 2, "_openetl_id": "AAAAAAAAAAAAAAAAAAAAAQ=="}, {"id": 3, "_openetl_id": "AAAAAAAAAAAAAAAAAAAAAg=="}]`
	for n := uint64(1); n <= 2; n++ {
		e := destination.Epoch{Generation: 1, N: n}
		for _, s := range []string{"target", "epoch"} {
			if err := w.Write(t.Context(), e, s, batch(t, fields, rows)); err != nil {
				t.Fatal(err)
			}
		}
		st, err := w.PreCommit(t.Context(), e)
		if err != nil {
			t.Fatal(err)
		}
		c, err := w.Commit(t.Context(), e, []destination.Staged{st})
		if err != nil {
			t.Fatal(err)
		}
		if want := map[string]uint64{"target": 1 + 2*(n-1), "epoch": 1}; c.Collapsed["target"] != want["target"] || c.Collapsed["epoch"] != want["epoch"] {
			t.Errorf("epoch %d collapsed %v, want %v", n, c.Collapsed, want)
		}
	}
	if got := env.rows("SELECT id FROM $schema.target ORDER BY id"); !slices.Equal(got, []string{"1", "3"}) {
		t.Errorf("target = %v", got)
	}
	if got := env.rows("SELECT id FROM $schema.epoch ORDER BY id"); !slices.Equal(got, []string{"1", "1", "3", "3"}) {
		t.Errorf("epoch = %v", got)
	}
}

// A column whose type a stream widens is refused, with the statement that
// widens it, unless the pipeline allows type changes; then it widens, in
// the key of a merge too.
func TestTypeChanges(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	n32 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	n64 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	k32 := arrow.Field{Name: "k", Type: arrow.PrimitiveTypes.Int32, Nullable: false}
	k64 := arrow.Field{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: false}
	apply := func(allow bool, fields ...arrow.Field) []destination.RefusedColumn {
		w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"k"}, AllowTypeChanges: allow})
		res, err := w.(destination.SchemaApplier).ApplySchema(ctx, []destination.StreamSchema{{Name: "m", Schema: arrow.NewSchema(fields, nil)}})
		if err != nil {
			t.Fatal(err)
		}
		return res.Refused
	}
	if r := apply(false, k32, n32, seqField); len(r) != 0 {
		t.Fatal(r)
	}
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"k"}})
	recov(t, w, 1, destination.Epoch{})
	commit(t, w, 1, "m", batch(t, []arrow.Field{k32, n32, seqField}, `[{"k": 1, "n": 1, "_openetl_seq": 1}]`))

	r := apply(false, k64, n64, seqField)
	if len(r) != 2 || r[0].FromType != "int" || r[0].ToType != "bigint" || r[0].Fix == "" {
		t.Fatalf("refused %+v", r)
	}
	// A person runs the fix the refusal gives, the key's too.
	for _, c := range r {
		if _, err := env.db.ExecContext(ctx, c.Fix); err != nil {
			t.Errorf("%s: %v", c.Fix, err)
		}
	}
	if r := apply(false, k64, n64, seqField); len(r) != 0 {
		t.Fatalf("after the fix, refused %+v", r)
	}
	r = apply(true, k64, arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, seqField)
	if len(r) != 0 {
		t.Fatalf("with type changes allowed, refused %+v", r)
	}
	if got := env.rows("SELECT TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id = OBJECT_ID('[" + env.schema + "].[m]') AND name IN ('k', 'n') ORDER BY name"); !slices.Equal(got, []string{"bigint", "float"}) {
		t.Errorf("types %v", got)
	}
	// A type that doesn't widen is refused, without a fix.
	r = apply(true, k64, arrow.Field{Name: "n", Type: arrow.FixedWidthTypes.Boolean, Nullable: true}, seqField)
	if len(r) != 1 || r[0].Fix != "" || !strings.Contains(r[0].Reason, "from float to bit") {
		t.Errorf("refused %+v", r)
	}
	w = env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"k"}})
	commit(t, w, 2, "m", batch(t, []arrow.Field{k64, n64, seqField}, `[{"k": 5000000000, "n": 2, "_openetl_seq": 2}, {"k": 1, "n": 3, "_openetl_seq": 2}]`))
	if got := env.rows("SELECT k, n FROM $schema.m ORDER BY k"); !slices.Equal(got, []string{"1,3", "5000000000,2"}) {
		t.Errorf("m = %v", got)
	}
}

// What another writer's apply holds the fence for, a newer generation's
// Recover waits on; after it, the old generation's commit is fenced.
func TestFence(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	s := destination.Stream{Name: "users", Mode: destination.Append}
	w1 := env.openStreams(1, destination.ScopePipeline, s)
	recov(t, w1, 1, destination.Epoch{})
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w1.Write(ctx, e, "users", batch(t, []arrow.Field{idField}, `[{"id": 1}]`)); err != nil {
		t.Fatal(err)
	}
	st, err := w1.PreCommit(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	w2 := env.openStreams(2, destination.ScopePipeline, s)
	if r := recov(t, w2, 2, e, st); r != destination.Redone {
		t.Fatalf("Recover = %v", r)
	}
	if _, err := w1.Commit(ctx, e, []destination.Staged{st}); err == nil || !strings.Contains(err.Error(), destination.ErrFenced.Error()) {
		t.Errorf("the old generation's commit: %v", err)
	}
	if got := env.rows("SELECT id FROM $schema.users"); !slices.Equal(got, []string{"1"}) {
		t.Errorf("users = %v", got)
	}
}

func TestDestinationTypes(t *testing.T) {
	env := newEnv(t)
	fields := []arrow.Field{
		idField,
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "f", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: 9}, Nullable: true},
		{Name: "t", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "iv", Type: arrow.FixedWidthTypes.MonthDayNanoInterval, Nullable: true},
		{Name: "l", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	w := env.openStreams(1, destination.ScopePipeline, destination.Stream{Name: "kinds", Mode: destination.Append})
	recov(t, w, 1, destination.Epoch{})
	commit(t, w, 1, "kinds", batch(t, fields, `[
		{"id": 1, "u8": 255, "u64": 18446744073709551615, "f": 1e308, "d": "-99999999999999999999999999999.999999999", "t": 86399999999999,
		 "ts": "9999-12-31T23:59:59.999999Z", "iv": {"months": 1, "days": -2, "nanoseconds": 3000000004}, "l": [1, null], "b": "", "s": ""},
		{"id": 2}]`))
	got := env.rows(`SELECT id, u8, u64, CONVERT(varchar(30), f, 3), d, CONVERT(varchar(20), t), CONVERT(varchar(40), ts, 127), iv, l, CONVERT(varchar(10), b, 1), CONCAT('[', s, ']')
		FROM $schema.kinds ORDER BY id`)
	want := []string{
		"1,255,18446744073709551615,1.0000000000000000e+308,-99999999999999999999999999999.999999999,23:59:59.9999999,9999-12-31T23:59:59.999999Z,P1M-2DT3.000000004S,[1,null],0x,[]",
		"2,null,null,null,null,null,null,null,null,null,[]",
	}
	if !slices.Equal(got, want) {
		t.Errorf("kinds =\n%v, want\n%v", got, want)
	}
	types := env.rows("SELECT name + ' ' + TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id = OBJECT_ID('[" + env.schema + "].[kinds]') ORDER BY column_id")
	if fmt.Sprint(types) != "[id bigint u8 tinyint u64 decimal f float d decimal t time ts datetimeoffset iv nvarchar l nvarchar b varbinary s nvarchar]" {
		t.Errorf("types %v", types)
	}
}
