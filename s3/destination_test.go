package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocloud.dev/blob"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

func TestFile(t *testing.T) {
	suite(t, func(t *testing.T, extra map[string]any) *Destination {
		return newDest(t, map[string]any{"url": "file://" + t.TempDir()}, extra)
	})
}

func newDest(t *testing.T, cfg, extra map[string]any) *Destination {
	t.Helper()
	for k, v := range extra {
		cfg[k] = v
	}
	b, _ := json.Marshal(cfg)
	d, err := NewDestination(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	return d
}

type env struct {
	t *testing.T
	d *Destination
	b *blob.Bucket
}

// suite runs the destination's tests against the destinations newDest makes.
func suite(t *testing.T, newDest func(*testing.T, map[string]any) *Destination) {
	for name, test := range map[string]func(*env){
		"CommitCopiesOnce":           testCommitCopiesOnce,
		"RecoverRedoes":              testRecoverRedoes,
		"RecoverReportsLost":         testRecoverReportsLost,
		"StaleCommitAfterRecovery":   testStaleCommitAfterRecovery,
		"RecoverDropsOldGenerations": testRecoverDropsOldGenerations,
		"CloseDropsUncommitted":      testCloseDropsUncommitted,
	} {
		t.Run(name, func(t *testing.T) {
			d := newDest(t, nil)
			b, err := d.bucket(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = b.Close() })
			test(&env{t: t, d: d, b: b})
		})
	}
	t.Run("FilesRollOver", func(t *testing.T) {
		d := newDest(t, map[string]any{"fileSize": 1 << 20})
		b, err := d.bucket(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		testFilesRollOver(&env{t: t, d: d, b: b})
	})
	t.Run("Types", func(t *testing.T) {
		d := newDest(t, nil)
		b, err := d.bucket(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		testTypes(&env{t: t, d: d, b: b})
	})
}

func (e *env) open(gen uint64, streams ...string) destination.Writer {
	e.t.Helper()
	cfg := destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: destination.ScopeStream}
	for _, s := range streams {
		cfg.Streams = append(cfg.Streams, destination.Stream{Name: s, Mode: destination.Append})
	}
	w, err := e.d.Open(e.t.Context(), cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

var (
	idField   = arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}
	nameField = arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}
)

func batch(t *testing.T, fields []arrow.Field, rows string) arrow.RecordBatch {
	t.Helper()
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema(fields, nil), strings.NewReader(rows))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rec.Release)
	return rec
}

func (e *env) write(w destination.Writer, ep destination.Epoch, stream string, ids ...int64) {
	e.t.Helper()
	rows := make([]string, len(ids))
	for i, id := range ids {
		rows[i] = fmt.Sprintf(`{"id": %d, "name": "n%d"}`, id, id)
	}
	if err := w.Write(e.t.Context(), ep, stream, batch(e.t, []arrow.Field{idField, nameField}, "["+strings.Join(rows, ",")+"]")); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) precommit(w destination.Writer, ep destination.Epoch) destination.Staged {
	e.t.Helper()
	st, err := w.PreCommit(e.t.Context(), ep)
	if err != nil {
		e.t.Fatal(err)
	}
	return st
}

func (e *env) commit(w destination.Writer, ep destination.Epoch, st destination.Staged) {
	e.t.Helper()
	if _, err := w.Commit(e.t.Context(), ep, []destination.Staged{st}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) recover(w destination.Writer, gen uint64, ep destination.Epoch, st destination.Staged) destination.Recovery {
	e.t.Helper()
	r, err := w.Recover(e.t.Context(), gen, ep, []destination.Staged{st})
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

// keys lists the keys under prefix.
func (e *env) keys(prefix string) []string {
	e.t.Helper()
	var out []string
	it := e.b.List(&blob.ListOptions{Prefix: prefix})
	for {
		obj, err := it.Next(e.t.Context())
		if err != nil {
			break
		}
		out = append(out, obj.Key)
	}
	return out
}

// read returns the table's files' rows.
func (e *env) read(table string) []arrow.RecordBatch {
	e.t.Helper()
	var out []arrow.RecordBatch
	for _, k := range e.keys(table + "/") {
		data, err := e.b.ReadAll(e.t.Context(), k)
		if err != nil {
			e.t.Fatal(err)
		}
		pf, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			e.t.Fatal(err)
		}
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1 << 16}, memory.DefaultAllocator)
		if err != nil {
			e.t.Fatal(err)
		}
		tbl, err := fr.ReadTable(e.t.Context())
		if err != nil {
			e.t.Fatal(err)
		}
		tr := array.NewTableReader(tbl, -1)
		for tr.Next() {
			b := tr.RecordBatch()
			b.Retain()
			e.t.Cleanup(b.Release)
			out = append(out, b)
		}
		tr.Release()
		tbl.Release()
		_ = pf.Close()
	}
	return out
}

func (e *env) ids(table string) []int64 {
	e.t.Helper()
	var ids []int64
	for _, b := range e.read(table) {
		col := b.Column(b.Schema().FieldIndices("id")[0]).(*array.Int64)
		ids = append(ids, col.Int64Values()...)
	}
	slices.Sort(ids)
	return ids
}

func (e *env) wantIDs(table string, want ...int64) {
	e.t.Helper()
	if got := e.ids(table); !slices.Equal(got, want) {
		e.t.Errorf("%s: ids %v, want %v", table, got, want)
	}
}

func testCommitCopiesOnce(e *env) {
	w := e.open(1, "users", "orders")
	e.recover(w, 1, destination.Epoch{}, nil)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	e.write(w, e1, "users", 1, 2)
	e.write(w, e1, "orders", 10)
	e.write(w, e1, "users", 3)
	st1 := e.precommit(w, e1)
	e.write(w, e2, "users", 4)
	st2 := e.precommit(w, e2)
	e.wantIDs("users")
	e.commit(w, e1, st1)
	e.commit(w, e1, st1)
	e.wantIDs("users", 1, 2, 3)
	e.wantIDs("orders", 10)
	e.commit(w, e2, st2)
	e.wantIDs("users", 1, 2, 3, 4)
	if got := e.keys("users/"); !slices.Equal(got, []string{
		fmt.Sprintf("users/p1-%020d-g%020d-00000.parquet", 1, 1), fmt.Sprintf("users/p1-%020d-g%020d-00000.parquet", 2, 1),
	}) {
		e.t.Errorf("users' files: %v", got)
	}
	if got := e.keys(reserved + "/p1/staging/"); len(got) != 0 {
		e.t.Errorf("staging left after the commits: %v", got)
	}
	if got := e.keys(reserved + "/p1/loads/"); len(got) != 3 {
		e.t.Errorf("markers %v, want one per stream and epoch", got)
	}
}

func testRecoverRedoes(e *env) {
	w := e.open(1, "users")
	e.recover(w, 1, destination.Epoch{}, nil)
	ep := destination.Epoch{Generation: 1, N: 1}
	e.write(w, ep, "users", 1, 2)
	st := e.precommit(w, ep)
	w2 := e.open(2, "users")
	if r := e.recover(w2, 2, ep, st); r != destination.Redone {
		e.t.Fatalf("recover: %v, want redone", r)
	}
	if r := e.recover(w2, 2, ep, st); r != destination.Applied {
		e.t.Fatalf("recover again: %v, want applied", r)
	}
	e.wantIDs("users", 1, 2)
}

func testRecoverReportsLost(e *env) {
	w := e.open(1, "users")
	e.recover(w, 1, destination.Epoch{}, nil)
	ep := destination.Epoch{Generation: 1, N: 1}
	e.write(w, ep, "users", 1)
	st := e.precommit(w, ep)
	for _, k := range e.keys(reserved + "/p1/staging/") {
		if err := e.b.Delete(e.t.Context(), k); err != nil {
			e.t.Fatal(err)
		}
	}
	if r := e.recover(e.open(2, "users"), 2, ep, st); r != destination.Lost {
		e.t.Fatalf("recover: %v, want lost", r)
	}
	e.wantIDs("users")
}

// A stale attempt commits the epoch at HEAD after the next one recovered it:
// it copies the same files to the same names.
func testStaleCommitAfterRecovery(e *env) {
	w := e.open(1, "users")
	e.recover(w, 1, destination.Epoch{}, nil)
	ep := destination.Epoch{Generation: 1, N: 1}
	e.write(w, ep, "users", 1, 2)
	st := e.precommit(w, ep)
	if r := e.recover(e.open(2, "users"), 2, ep, st); r != destination.Redone {
		e.t.Fatalf("recover: %v, want redone", r)
	}
	if _, err := w.Commit(e.t.Context(), ep, []destination.Staged{st}); err != nil {
		e.t.Errorf("stale commit: %v", err)
	}
	e.wantIDs("users", 1, 2)
}

// What earlier generations staged goes once a later one recovers, the epoch
// it recovers included.
func testRecoverDropsOldGenerations(e *env) {
	w := e.open(1, "users")
	e.recover(w, 1, destination.Epoch{}, nil)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	e.write(w, e1, "users", 1)
	st := e.precommit(w, e1)
	e.write(w, e2, "users", 2)
	e.precommit(w, e2)
	w3 := e.open(3, "users")
	e3 := destination.Epoch{Generation: 3, N: 2}
	e.write(w3, e3, "users", 5)
	e.precommit(w3, e3)
	if r := e.recover(e.open(4, "users"), 4, e1, st); r != destination.Redone {
		e.t.Fatalf("recover: %v, want redone", r)
	}
	if got := e.keys(reserved + "/p1/staging/"); len(got) != 0 {
		e.t.Errorf("staging left: %v", got)
	}
	e.wantIDs("users", 1)
}

func testCloseDropsUncommitted(e *env) {
	w := e.open(1, "users")
	e.recover(w, 1, destination.Epoch{}, nil)
	ep := destination.Epoch{Generation: 1, N: 1}
	e.write(w, ep, "users", 1)
	if err := w.Close(e.t.Context()); err != nil {
		e.t.Fatal(err)
	}
	if got := e.keys(reserved + "/p1/staging/"); len(got) != 0 {
		e.t.Errorf("staging left: %v", got)
	}
}

// A file ends once it reaches fileSize, and a new schema starts another.
func testFilesRollOver(e *env) {
	w := e.open(1, "users")
	e.recover(w, 1, destination.Epoch{}, nil)
	ep := destination.Epoch{Generation: 1, N: 1}
	var want []int64
	for i := range 6 {
		var rows []string
		for j := range 10000 {
			id := int64(i*10000 + j)
			rows = append(rows, fmt.Sprintf(`{"id": %d, "name": %q}`, id, rand.Text()+rand.Text()))
			want = append(want, id)
		}
		if err := w.Write(e.t.Context(), ep, "users", batch(e.t, []arrow.Field{idField, nameField}, "["+strings.Join(rows, ",")+"]")); err != nil {
			e.t.Fatal(err)
		}
	}
	wider := arrow.Field{Name: "extra", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	if err := w.Write(e.t.Context(), ep, "users", batch(e.t, []arrow.Field{idField, nameField, wider}, `[{"id": -1, "name": "x", "extra": 7}]`)); err != nil {
		e.t.Fatal(err)
	}
	want = append(want, -1)
	slices.Sort(want)
	e.commit(w, ep, e.precommit(w, ep))
	if n := len(e.keys("users/")); n < 3 {
		e.t.Errorf("%d files, want the rows split by size and by schema", n)
	}
	e.wantIDs("users", want...)
}

func testTypes(e *env) {
	w := e.open(1, "things")
	e.recover(w, 1, destination.Epoch{}, nil)
	fields := []arrow.Field{
		idField,
		{Name: "uid", Type: extensions.NewUUIDType(), Nullable: true},
		{Name: "price", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	}
	ep := destination.Epoch{Generation: 1, N: 1}
	if err := w.Write(e.t.Context(), ep, "things", batch(e.t, fields, `[
		{"id": 1, "uid": "4fbd2c5e-49cb-4b5f-9c6c-0d1e2f3a4b5c", "price": "12.34", "at": "2026-11-03T01:02:03.123456789Z", "tags": ["a", "b"]},
		{"id": 2}]`)); err != nil {
		e.t.Fatal(err)
	}
	e.commit(w, ep, e.precommit(w, ep))
	got := e.read("things")
	if len(got) != 1 || got[0].NumRows() != 2 {
		e.t.Fatalf("read %v", got)
	}
	sc := got[0].Schema()
	for _, f := range fields {
		i := sc.FieldIndices(f.Name)
		// Parquet's lists name their element "element".
		if len(i) == 0 || strings.ReplaceAll(sc.Field(i[0]).Type.String(), "element:", "item:") != f.Type.String() {
			e.t.Errorf("column %s: %v, want %v", f.Name, sc, f.Type)
		}
	}
	at := got[0].Column(sc.FieldIndices("at")[0]).(*array.Timestamp)
	if at.Value(0)%1000 != 789 {
		e.t.Errorf("at lost its nanoseconds: %d", at.Value(0))
	}
}
