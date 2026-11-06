//go:build integration

package iceberg

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// Runs against the compose Iceberg REST catalog and S3 store (make deps-up),
// or OPENETL_TEST_ICEBERG_URI and OPENETL_TEST_S3_ENDPOINT. The catalog keeps
// its tables in the store's iceberg bucket.
func uri() string { return cmp.Or(os.Getenv("OPENETL_TEST_ICEBERG_URI"), "http://127.0.0.1:8181") }
func s3() string  { return cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333") }

type env struct {
	t  *testing.T
	d  *Destination
	ns string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, s3()+"/iceberg", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create the iceberg bucket (is the S3 store running? make deps-up): %v", err)
	}
	_ = res.Body.Close()
	ns := "t_" + strings.ToLower(rand.Text())[:10]
	cfg, _ := json.Marshal(map[string]string{
		"uri": uri(), "namespace": ns, "s3Endpoint": s3(), "region": "us-east-1",
		"accessKeyId": "openetl", "secretAccessKey": "openetl",
	})
	d, err := NewDestination(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Check(t.Context()); err != nil {
		t.Fatalf("check (is the Iceberg catalog running? make deps-up): %v", err)
	}
	return &env{t: t, d: d, ns: ns}
}

func (e *env) open(gen uint64, scope destination.Scope, streams ...destination.Stream) destination.Writer {
	e.t.Helper()
	cfg := destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: scope, Streams: streams}
	w, err := e.d.Open(e.t.Context(), cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

func appends(names ...string) []destination.Stream {
	var out []destination.Stream
	for _, n := range names {
		out = append(out, destination.Stream{Name: n, Mode: destination.Append})
	}
	return out
}

func (e *env) table(name string) *table.Table {
	e.t.Helper()
	c, err := e.d.catalog(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	tbl, err := c.LoadTable(e.t.Context(), table.Identifier{e.ns, name})
	if err != nil {
		e.t.Fatal(err)
	}
	return tbl
}

// ids returns the table's ids, sorted; none if it doesn't exist.
func (e *env) ids(name string) []int64 {
	e.t.Helper()
	c, err := e.d.catalog(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	tbl, err := c.LoadTable(e.t.Context(), table.Identifier{e.ns, name})
	if errors.Is(err, catalog.ErrNoSuchTable) {
		return nil
	}
	if err != nil {
		e.t.Fatal(err)
	}
	_, it, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	var ids []int64
	for b, err := range it {
		if err != nil {
			e.t.Fatal(err)
		}
		col := b.Column(0)
		for i := range col.Len() {
			switch c := col.(type) {
			case *array.Int64:
				ids = append(ids, c.Value(i))
			case *array.Int32:
				ids = append(ids, int64(c.Value(i)))
			}
		}
		b.Release()
	}
	slices.Sort(ids)
	return ids
}

func (e *env) wantIDs(name string, want ...int64) {
	e.t.Helper()
	if got := e.ids(name); !slices.Equal(got, want) {
		e.t.Errorf("%s: ids %v, want %v", name, got, want)
	}
}

func span(from, to int64) []int64 {
	var out []int64
	for i := from; i <= to; i++ {
		out = append(out, i)
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

func idRows(ids ...int64) string {
	var b strings.Builder
	b.WriteString("[")
	for i, id := range ids {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"id":` + jsonInt(id) + `,"name":"n` + jsonInt(id) + `"}`)
	}
	b.WriteString("]")
	return b.String()
}

func jsonInt(i int64) string { b, _ := json.Marshal(i); return string(b) }

var (
	idField   = arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	nameField = arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}
)

func write(t *testing.T, w destination.Writer, e destination.Epoch, stream string, ids ...int64) {
	t.Helper()
	if err := w.Write(t.Context(), e, stream, batch(t, []arrow.Field{idField, nameField}, idRows(ids...))); err != nil {
		t.Fatal(err)
	}
}

func recov(t *testing.T, w destination.Writer, gen uint64, e destination.Epoch, st destination.Staged) destination.Recovery {
	t.Helper()
	r, err := w.Recover(t.Context(), gen, e, []destination.Staged{st})
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

func commit(t *testing.T, w destination.Writer, e destination.Epoch, st destination.Staged) {
	t.Helper()
	if _, err := w.Commit(t.Context(), e, []destination.Staged{st}); err != nil {
		t.Fatal(err)
	}
}

func TestCommitAppliesOnce(t *testing.T) {
	for _, scope := range []destination.Scope{destination.ScopePipeline, destination.ScopeStream} {
		env := newEnv(t)
		w := env.open(1, scope, appends("users", "orders")...)
		if r := recov(t, w, 1, destination.Epoch{}, nil); r != destination.Applied {
			t.Fatalf("recover: %v", r)
		}
		e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
		write(t, w, e1, "users", 1, 2)
		write(t, w, e1, "orders", 10)
		write(t, w, e1, "users", 3)
		st1 := precommit(t, w, e1)
		write(t, w, e2, "users", 4)
		st2 := precommit(t, w, e2)
		env.wantIDs("users")
		commit(t, w, e1, st1)
		commit(t, w, e1, st1)
		env.wantIDs("users", 1, 2, 3)
		env.wantIDs("orders", 10)
		commit(t, w, e2, st2)
		commit(t, w, e2, st2)
		env.wantIDs("users", 1, 2, 3, 4)

		snap := env.table("users").CurrentSnapshot()
		got := snap.Summary.Properties
		if got[pipelineKey] != "p1" || got[streamKey] != "users" || got[epochKey] != "2" || got[generationKey] != "1" {
			t.Errorf("snapshot summary: %v", got)
		}
	}
}

func TestRecoverRedoesAStagedEpoch(t *testing.T) {
	env := newEnv(t)
	w := env.open(1, destination.ScopePipeline, appends("users", "orders")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e, "users", 1, 2)
	write(t, w, e, "orders", 5)
	st := precommit(t, w, e)

	w2 := env.open(2, destination.ScopePipeline, appends("users", "orders")...)
	if r := recov(t, w2, 2, e, st); r != destination.Redone {
		t.Fatalf("recover: %v, want redone", r)
	}
	env.wantIDs("users", 1, 2)
	env.wantIDs("orders", 5)
	if r := recov(t, w2, 2, e, st); r != destination.Applied {
		t.Fatalf("recover again: %v, want applied", r)
	}
	if _, err := w.Commit(t.Context(), e, []destination.Staged{st}); !errors.Is(err, destination.ErrFenced) {
		t.Fatalf("stale commit: %v, want fenced", err)
	}
	env.wantIDs("users", 1, 2)
	if _, err := w.Recover(t.Context(), 1, e, []destination.Staged{st}); !errors.Is(err, destination.ErrFenced) {
		t.Fatalf("stale recover: %v, want fenced", err)
	}
}

func TestRecoverReportsLost(t *testing.T) {
	env := newEnv(t)
	w := env.open(1, destination.ScopePipeline, appends("users")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e, "users", 1)
	st := precommit(t, w, e)
	var desc staged
	if err := json.Unmarshal(st, &desc); err != nil {
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
	w2 := env.open(2, destination.ScopePipeline, appends("users")...)
	if r := recov(t, w2, 2, e, st); r != destination.Lost {
		t.Fatalf("recover: %v, want lost", r)
	}
	env.wantIDs("users")
}

// A stale attempt commits the epoch at HEAD after checking the table, but
// the next attempt recovers it first: one of them applies it.
func TestStaleCommitAfterRecovery(t *testing.T) {
	env := newEnv(t)
	w := env.open(1, destination.ScopePipeline, appends("users")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e, "users", 1, 2)
	st := precommit(t, w, e)
	// Tables can make iceberg-go retry a conflicting commit on the new head.
	tx := env.table("users").NewTransaction()
	if err := tx.SetProperties(map[string]string{table.CommitNumRetriesKey: "4"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	w2 := env.open(2, destination.ScopePipeline, appends("users")...)
	var once sync.Once
	testHookBeforeCommit = func(cur *writer) {
		if cur.cfg.Generation == 1 {
			once.Do(func() {
				if r := recov(t, w2, 2, e, st); r != destination.Redone {
					t.Errorf("recover: %v, want redone", r)
				}
			})
		}
	}
	t.Cleanup(func() { testHookBeforeCommit = nil })
	if _, err := w.Commit(t.Context(), e, []destination.Staged{st}); !errors.Is(err, destination.ErrFenced) {
		t.Errorf("stale commit: %v, want fenced", err)
	}
	env.wantIDs("users", 1, 2)
}

// New columns are added, and a narrower type is written into a wider
// column.
func TestColumnsAddAndNarrowerTypesFit(t *testing.T) {
	env := newEnv(t)
	w := env.open(1, destination.ScopePipeline, appends("users")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	small := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	extra := arrow.Field{Name: "extra", Type: arrow.BinaryTypes.String, Nullable: true}
	for _, b := range []arrow.RecordBatch{
		batch(t, []arrow.Field{idField, nameField}, `[{"id": 1, "name": "a"}]`),
		batch(t, []arrow.Field{small, nameField, extra}, `[{"id": 2, "name": "b", "extra": "x"}]`),
	} {
		if err := w.Write(t.Context(), e1, "users", b); err != nil {
			t.Fatal(err)
		}
	}
	commit(t, w, e1, precommit(t, w, e1))
	if err := w.Write(t.Context(), e2, "users", batch(t, []arrow.Field{small}, `[{"id": 3}]`)); err != nil {
		t.Fatal(err)
	}
	commit(t, w, e2, precommit(t, w, e2))
	env.wantIDs("users", 1, 2, 3)
	sc := env.table("users").Schema()
	if f, ok := sc.FindFieldByName("id"); !ok || f.Type.String() != "long" {
		t.Errorf("id: %v", f)
	}
	if _, ok := sc.FindFieldByName("extra"); !ok {
		t.Errorf("no extra column: %s", sc)
	}
}

func TestTypes(t *testing.T) {
	env := newEnv(t)
	w := env.open(1, destination.ScopePipeline, appends("typed")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	ext := func(name string) arrow.Metadata {
		return arrow.NewMetadata([]string{extensionNameKey}, []string{name})
	}
	fields := []arrow.Field{
		idField,
		{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "tiny", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "price", Type: &arrow.Decimal128Type{Precision: 12, Scale: 2}, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "local", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "day", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "doc", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: ext("arrow.json")},
		{Name: "uid", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true, Metadata: ext(uuidExtension)},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "blob", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "nothing", Type: arrow.Null, Nullable: true},
	}
	e := destination.Epoch{Generation: 1, N: 1}
	rows := `[{"id": 1, "flag": true, "tiny": 7, "price": "12.50", "at": "2026-11-02T10:00:00Z", "local": "2026-11-02T10:00:00",
		"day": "2026-11-02", "doc": "{\"a\": 1}", "uid": "AAECAwQFBgcICQoLDA0ODw==", "tags": ["x", "y"], "blob": "AQI=", "nothing": null},
		{"id": 2}]`
	if err := w.Write(t.Context(), e, "typed", batch(t, fields, rows)); err != nil {
		t.Fatal(err)
	}
	commit(t, w, e, precommit(t, w, e))
	env.wantIDs("typed", 1, 2)
	got := map[string]string{}
	for _, f := range env.table("typed").Schema().Fields() {
		got[f.Name] = f.Type.String()
	}
	want := map[string]string{
		"id": "long", "flag": "boolean", "tiny": "int", "price": "decimal(12, 2)", "at": "timestamptz", "local": "timestamp",
		"day": "date", "doc": "string", "uid": "uuid", "tags": "list<string>", "blob": "binary", "nothing": "string",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %s, want %s", k, got[k], v)
		}
	}
}

func overwrites(names ...string) []destination.Stream {
	var out []destination.Stream
	for _, n := range names {
		out = append(out, destination.Stream{Name: n, Mode: destination.Overwrite})
	}
	return out
}

func TestOverwriteReplacesWhenTheReadEnds(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	w := env.open(1, destination.ScopeStream, overwrites("items")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	read := func(start time.Time) {
		if err := w.(destination.ReadTracker).StartReads(ctx, map[string]time.Time{"items": start}); err != nil {
			t.Fatal(err)
		}
	}
	end := func(e destination.Epoch) {
		if err := w.(destination.ReadTracker).EndRead(ctx, e, "items"); err != nil {
			t.Fatal(err)
		}
	}
	e := func(n uint64) destination.Epoch { return destination.Epoch{Generation: 1, N: n} }

	read(time.UnixMicro(1000))
	write(t, w, e(1), "items", 1, 2)
	commit(t, w, e(1), precommit(t, w, e(1)))
	write(t, w, e(2), "items", 3)
	end(e(2))
	commit(t, w, e(2), precommit(t, w, e(2)))
	env.wantIDs("items", 1, 2, 3)

	// The next read: the old rows until it ends.
	read(time.UnixMicro(2000))
	write(t, w, e(3), "items", 7)
	st3 := precommit(t, w, e(3))
	commit(t, w, e(3), st3)
	env.wantIDs("items", 1, 2, 3)

	// An attempt that continues the read, after a crash.
	w2 := env.open(2, destination.ScopeStream, overwrites("items")...)
	if r := recov(t, w2, 2, e(3), st3); r != destination.Applied {
		t.Fatalf("recover: %v", r)
	}
	if err := w2.(destination.ReadTracker).StartReads(ctx, map[string]time.Time{"items": time.UnixMicro(2000)}); err != nil {
		t.Fatal(err)
	}
	e4 := destination.Epoch{Generation: 2, N: 4}
	write(t, w2, e4, "items", 8)
	if err := w2.(destination.ReadTracker).EndRead(ctx, e4, "items"); err != nil {
		t.Fatal(err)
	}
	commit(t, w2, e4, precommit(t, w2, e4))
	env.wantIDs("items", 7, 8)
	if op := env.table("items").CurrentSnapshot().Summary.Operation; op != table.OpOverwrite {
		t.Errorf("operation %s, want overwrite", op)
	}

	// A read without rows empties the table.
	if err := w2.(destination.ReadTracker).StartReads(ctx, map[string]time.Time{"items": time.UnixMicro(3000)}); err != nil {
		t.Fatal(err)
	}
	e5 := destination.Epoch{Generation: 2, N: 5}
	if err := w2.(destination.ReadTracker).EndRead(ctx, e5, "items"); err != nil {
		t.Fatal(err)
	}
	commit(t, w2, e5, precommit(t, w2, e5))
	env.wantIDs("items")
}

// Against moto's Glue at OPENETL_TEST_GLUE_ENDPOINT, with the data files in
// the S3 store; skipped without it.
func TestGlue(t *testing.T) {
	endpoint := os.Getenv("OPENETL_TEST_GLUE_ENDPOINT")
	if endpoint == "" {
		t.Skip("set OPENETL_TEST_GLUE_ENDPOINT to a Glue endpoint, such as moto's")
	}
	newEnv(t) // the bucket
	ns := "g_" + strings.ToLower(rand.Text())[:10]
	cfg, _ := json.Marshal(map[string]any{
		"catalog": "glue", "region": "us-east-1", "namespace": ns, "warehouse": "s3://iceberg/glue",
		"accessKeyId": "openetl", "secretAccessKey": "openetl", "s3Endpoint": s3(),
		"properties": map[string]string{"glue.endpoint": endpoint},
	})
	d, err := NewDestination(cfg)
	if err != nil {
		t.Fatal(err)
	}
	env := &env{t: t, d: d, ns: ns}
	w := env.open(1, destination.ScopeStream, appends("users")...)
	recov(t, w, 1, destination.Epoch{}, nil)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, w, e1, "users", 1, 2)
	commit(t, w, e1, precommit(t, w, e1))
	write(t, w, e2, "users", 3)
	st := precommit(t, w, e2)
	w2 := env.open(2, destination.ScopeStream, appends("users")...)
	if r := recov(t, w2, 2, e2, st); r != destination.Redone {
		t.Fatalf("recover: %v, want redone", r)
	}
	if _, err := w.Commit(t.Context(), e2, []destination.Staged{st}); !errors.Is(err, destination.ErrFenced) {
		t.Fatalf("stale commit: %v, want fenced", err)
	}
	env.wantIDs("users", 1, 2, 3)

	// Glue's VersionId is the compare-and-swap: a stale commit that checked
	// the table before recovery applied the epoch fails.
	w3 := env.open(3, destination.ScopeStream, appends("users")...)
	recov(t, w3, 3, destination.Epoch{}, nil)
	e3 := destination.Epoch{Generation: 3, N: 3}
	write(t, w3, e3, "users", 4)
	st3 := precommit(t, w3, e3)
	w4 := env.open(4, destination.ScopeStream, appends("users")...)
	var once sync.Once
	testHookBeforeCommit = func(cur *writer) {
		if cur.cfg.Generation == 3 {
			once.Do(func() { recov(t, w4, 4, e3, st3) })
		}
	}
	t.Cleanup(func() { testHookBeforeCommit = nil })
	if _, err := w3.Commit(t.Context(), e3, []destination.Staged{st3}); !errors.Is(err, destination.ErrFenced) {
		t.Errorf("stale commit after recovery: %v, want fenced", err)
	}
	env.wantIDs("users", 1, 2, 3, 4)

	w5 := env.open(5, destination.ScopeStream, merges(destination.SoftDelete, "accounts")...)
	recov(t, w5, 5, destination.Epoch{}, nil)
	fields := []arrow.Field{idField, nameField, opField, seqField, deletedField}
	e5, e6 := destination.Epoch{Generation: 5, N: 5}, destination.Epoch{Generation: 5, N: 6}
	writeRows(t, w5, e5, "accounts", fields, `[
		{"id": 1, "name": "a", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false},
		{"id": 2, "name": "b", "_openetl_op": "insert", "_openetl_seq": 1, "_openetl_deleted": false}]`)
	commitEpoch(t, w5, e5)
	writeRows(t, w5, e6, "accounts", fields, `[
		{"id": 1, "name": "a2", "_openetl_op": "update", "_openetl_seq": 2, "_openetl_deleted": false},
		{"id": 2, "_openetl_op": "delete", "_openetl_seq": 2, "_openetl_deleted": true}]`)
	commitEpoch(t, w5, e6)
	if got := env.rows("accounts", "id", "name", deletedColumn); got != "1:a2:false,2:b:true" {
		t.Errorf("accounts: %s", got)
	}
}

func TestGlueNeedsStreamScope(t *testing.T) {
	d, err := NewDestination([]byte(`{"catalog": "glue", "region": "us-east-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Open(t.Context(), destination.OpenConfig{Pipeline: "p", Generation: 1, Scope: destination.ScopePipeline}); err == nil {
		t.Fatal("pipeline scope on Glue: no error")
	}
}
