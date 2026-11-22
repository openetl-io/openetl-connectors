package objstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/openetl-io/openetl-connectors/objstore/s3store"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/check"
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
	d, err := testDestination(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Run(t.Context(), d.Checks()).Err(); err != nil {
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
		"LanesNameTheirFiles":        testLanesNameTheirFiles,
		"TypesDontChange":            testTypesDontChange,
		"OverwritePublishes":         testOverwritePublishes,
		"SwapRefusesStale":           testSwapRefusesStale,
		"Preflight":                  testDestinationPreflight,
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
	for _, c := range []struct{ format, compression, ext string }{
		{formatCSV, "zstd", ".csv.zst"}, {formatJSONL, "gzip", ".jsonl.gz"}, {formatCSV, "none", ".csv"},
	} {
		t.Run("Text/"+c.format+"/"+c.compression, func(t *testing.T) {
			d := newDest(t, map[string]any{"format": c.format, "compression": c.compression})
			b, err := d.bucket(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = b.Close() })
			testText(&env{t: t, d: d, b: b}, c.ext)
		})
	}
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

// Each lane's files have names of their own, and one commit of every lane's
// committable copies them under one marker per stream.
func testLanesNameTheirFiles(e *env) {
	w0 := e.open(1, "users", "orders")
	cfg := destination.OpenConfig{Pipeline: "p1", Generation: 1, Scope: destination.ScopeStream, Lane: 1,
		Streams: []destination.Stream{{Name: "users", Mode: destination.Append}, {Name: "orders", Mode: destination.Append}}}
	w1, err := e.d.Open(e.t.Context(), cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = w1.Close(context.Background()) })
	e.recover(w0, 1, destination.Epoch{}, nil)
	ep := destination.Epoch{Generation: 1, N: 1}
	e.write(w0, ep, "users", 1)
	e.write(w1, ep, "users", 2)
	e.write(w1, ep, "orders", 10)
	st := []destination.Staged{e.precommit(w0, ep), e.precommit(w1, ep)}
	for range 2 {
		if _, err := w0.Commit(e.t.Context(), ep, st); err != nil {
			e.t.Fatal(err)
		}
	}
	e.wantIDs("users", 1, 2)
	e.wantIDs("orders", 10)
	if got := e.keys("users/"); !slices.Equal(got, []string{
		fmt.Sprintf("users/p1-%020d-g%020d-00000.parquet", 1, 1), fmt.Sprintf("users/p1-%020d-g%020d-l1-00000.parquet", 1, 1),
	}) {
		e.t.Errorf("users' files: %v", got)
	}
	if got := e.keys(reserved + "/p1/loads/"); len(got) != 2 {
		e.t.Errorf("markers %v, want one per stream", got)
	}
	w2 := e.open(2, "users", "orders")
	if r, err := w2.Recover(e.t.Context(), 2, ep, st); err != nil || r != destination.Applied {
		e.t.Errorf("recover on one lane: %v, %v, want applied", r, err)
	}
	if _, err := w2.Commit(e.t.Context(), destination.Epoch{Generation: 1, N: 2}, st); err == nil {
		e.t.Error("committed another epoch's descriptors")
	}
}

// A table takes new columns, and refuses to change a column's type until a
// person removes its schema record, as the refusal says.
func testTypesDontChange(e *env) {
	ctx := e.t.Context()
	w := e.open(1, "items")
	sa, ok := w.(destination.SchemaApplier)
	if !ok {
		e.t.Fatal("not a SchemaApplier")
	}
	apply := func(fields ...arrow.Field) destination.SchemaApplied {
		e.t.Helper()
		a, err := sa.ApplySchema(ctx, []destination.StreamSchema{{Name: "items", Schema: arrow.NewSchema(fields, nil)}})
		if err != nil {
			e.t.Fatal(err)
		}
		return a
	}
	n32 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	n64 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	if a := apply(idField, n32); len(a.Refused) != 0 {
		e.t.Fatalf("the first schema: %+v", a.Refused)
	}
	if a := apply(idField, n32, nameField); len(a.Refused) != 0 {
		e.t.Fatalf("an added column: %+v", a.Refused)
	}
	a := apply(idField, n64, nameField)
	if len(a.Refused) != 1 || a.Refused[0].Column != "n" || a.Refused[0].FromType != "int32" || a.Refused[0].ToType != "int64" || !strings.Contains(a.Refused[0].Fix, schemaKey("items")) {
		e.t.Fatalf("a changed type: %+v", a.Refused)
	}
	if err := e.b.Delete(ctx, schemaKey("items")); err != nil {
		e.t.Fatal(err)
	}
	if a := apply(idField, n64, nameField); len(a.Refused) != 0 {
		e.t.Errorf("once a person removed the record: %+v", a.Refused)
	}
}

// testText writes a text format, compressed as its extension says.
func testText(e *env, ext string) {
	w := e.open(1, "users")
	e.recover(w, 1, destination.Epoch{}, nil)
	ep := destination.Epoch{Generation: 1, N: 1}
	e.write(w, ep, "users", 1, 2)
	e.commit(w, ep, e.precommit(w, ep))
	keys := e.keys("users/")
	if len(keys) != 1 || !strings.HasSuffix(keys[0], ext) {
		e.t.Fatalf("files %v, want one ending %s", keys, ext)
	}
	data, err := e.b.ReadAll(e.t.Context(), keys[0])
	if err != nil {
		e.t.Fatal(err)
	}
	compression, _ := compressionOf(keys[0])
	r, err := decompress(compression, bytes.NewReader(data))
	if err != nil {
		e.t.Fatal(err)
	}
	text, err := io.ReadAll(r)
	if err != nil {
		e.t.Fatal(err)
	}
	want := "id,name\n1,n1\n2,n2\n"
	if strings.Contains(ext, "jsonl") {
		want = `{"id":1,"name":"n1"}` + "\n" + `{"id":2,"name":"n2"}` + "\n"
	}
	if string(text) != want {
		e.t.Errorf("%s: %q, want %q", keys[0], text, want)
	}
}

// manifestIDs are the ids of the files the table's manifest lists.
func (e *env) manifestIDs(table string) []int64 {
	e.t.Helper()
	listed, err := e.b.ReadAll(e.t.Context(), table+"/"+manifestPath)
	if err != nil {
		return nil
	}
	var ids []int64
	for _, loc := range strings.Fields(string(listed)) {
		k, ok := e.d.store.key(loc)
		if !ok {
			e.t.Fatalf("the manifest lists %s, outside the URL", loc)
		}
		data, err := e.b.ReadAll(e.t.Context(), k)
		if err != nil {
			e.t.Fatalf("the manifest lists %s: %v", loc, err)
		}
		pf, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			e.t.Fatal(err)
		}
		fr, _ := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1 << 16}, memory.DefaultAllocator)
		tbl, err := fr.ReadTable(e.t.Context())
		if err != nil {
			e.t.Fatal(err)
		}
		for _, c := range tbl.Column(int(tbl.Schema().FieldIndices("id")[0])).Data().Chunks() {
			ids = append(ids, c.(*array.Int64).Int64Values()...)
		}
		tbl.Release()
		_ = pf.Close()
	}
	slices.Sort(ids)
	return ids
}

// An overwrite's files appear at once, when its read ends, and a stale
// attempt can't move the manifest back to an older read.
func testOverwritePublishes(e *env) {
	open := func(gen uint64) destination.Writer {
		w, err := e.d.Open(e.t.Context(), destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: destination.ScopeStream,
			Streams: []destination.Stream{{Name: "users", Mode: destination.Overwrite}}})
		if err != nil {
			e.t.Fatal(err)
		}
		e.t.Cleanup(func() { _ = w.Close(context.Background()) })
		e.recover(w, gen, destination.Epoch{}, nil)
		return w
	}
	rt := func(w destination.Writer) destination.ReadTracker { return w.(destination.ReadTracker) }
	w := open(1)
	first := time.UnixMicro(1_000_000)
	if err := rt(w).StartReads(e.t.Context(), map[string]time.Time{"users": first}); err != nil {
		e.t.Fatal(err)
	}
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	e.write(w, e1, "users", 1, 2)
	e.commit(w, e1, e.precommit(w, e1))
	if ids := e.manifestIDs("users"); ids != nil {
		e.t.Fatalf("a read that hasn't ended shows %v", ids)
	}
	e.write(w, e2, "users", 3)
	_ = rt(w).EndRead(e.t.Context(), e2, "users")
	st2 := e.precommit(w, e2)
	e.commit(w, e2, st2)
	if ids := e.manifestIDs("users"); !slices.Equal(ids, []int64{1, 2, 3}) {
		e.t.Fatalf("after the first read: %v", ids)
	}

	// The next read, by the next generation, ends without rows.
	w2 := open(2)
	if err := rt(w2).StartReads(e.t.Context(), map[string]time.Time{"users": first.Add(time.Second)}); err != nil {
		e.t.Fatal(err)
	}
	e3 := destination.Epoch{Generation: 2, N: 3}
	_ = rt(w2).EndRead(e.t.Context(), e3, "users")
	e.commit(w2, e3, e.precommit(w2, e3))
	if listed, err := e.b.ReadAll(e.t.Context(), "users/"+manifestPath); err != nil || len(listed) != 0 {
		e.t.Fatalf("an empty read's manifest: %q, %v", listed, err)
	}
	if got := e.keys("users/" + readsDir + "/"); len(got) != 0 {
		e.t.Errorf("the first read's files stay: %v", got)
	}
	// The first generation publishing its read again, as a zombie would.
	stale := stagedStream{Name: "users", Table: "users", Read: strconv.FormatInt(first.UnixMicro(), 10), Ended: true}
	if err := w.(*writer).publish(e.t.Context(), staged{Generation: 1, Epoch: 2}, stale); err != nil {
		e.t.Fatal(err)
	}
	if listed, _ := e.b.ReadAll(e.t.Context(), "users/"+manifestPath); len(listed) != 0 {
		e.t.Fatalf("a stale attempt moved the manifest back: %q", listed)
	}
}

// swap writes only over the version it was given, on every store.
func testSwapRefusesStale(e *env) {
	ctx, st := e.t.Context(), e.d.store
	check := func(key string) func() (*version, error) {
		return func() (*version, error) {
			a, err := e.b.Attributes(ctx, key)
			if gcerrors.Code(err) == gcerrors.NotFound {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			return st.version(a), nil
		}
	}
	key := "swap/obj"
	if err := st.swap(ctx, e.b, key, []byte("1"), nil, nil, check(key)); err != nil {
		e.t.Fatal(err)
	}
	if err := st.swap(ctx, e.b, key, []byte("x"), nil, nil, check(key)); !errors.Is(err, errConflict) {
		e.t.Fatalf("creating what exists: %v", err)
	}
	a, err := e.b.Attributes(ctx, key)
	if err != nil {
		e.t.Fatal(err)
	}
	v1 := st.version(a)
	if err := st.swap(ctx, e.b, key, []byte("2"), nil, v1, check(key)); err != nil {
		e.t.Fatal(err)
	}
	if err := st.swap(ctx, e.b, key, []byte("x"), nil, v1, check(key)); !errors.Is(err, errConflict) {
		e.t.Fatalf("over a stale version: %v", err)
	}
	if got, _ := e.b.ReadAll(ctx, key); string(got) != "2" {
		e.t.Fatalf("%q, want 2", got)
	}
}

// Preflight checks each stream's mode and column types, and, for a
// pipeline that overwrites, conditional writes, leaving nothing behind.
func testDestinationPreflight(e *env) {
	t := e.t
	sc := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	req := destination.PreflightRequest{Pipeline: "p", Streams: []destination.Stream{{Name: "a", Mode: destination.Append}}, Schemas: map[string]*arrow.Schema{"a": sc, "b": sc}}
	statuses := func(req destination.PreflightRequest) map[string]string {
		t.Helper()
		checks := e.d.Preflight(req)
		if err := check.Validate(append(e.d.Checks(), checks...)); err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for _, it := range check.Run(t.Context(), checks).Items {
			out[it.Name] = it.Status.String()
			if it.Failure != nil {
				out[it.Name] += " " + it.Failure.Reason
			}
		}
		return out
	}
	if got := statuses(req); !maps.Equal(got, map[string]string{"stream.a.mode": "passed", "stream.a.types": "passed"}) {
		t.Errorf("append: %v", got)
	}
	if err := e.b.WriteAll(t.Context(), schemaKey("b"), []byte(`{"columns":[{"name":"x","type":"int64"}]}`), nil); err != nil {
		t.Fatal(err)
	}
	req.Streams = append(req.Streams, destination.Stream{Name: "b", Mode: destination.Overwrite}, destination.Stream{Name: "c", Mode: destination.Merge})
	want := map[string]string{
		"stream.a.mode": "passed", "stream.a.types": "passed",
		"stream.b.mode": "passed", "stream.b.types": "failed type_change_refused",
		"stream.c.mode": "failed setting_mismatch", "stream.c.types": "skipped",
		"store.conditional-write": "passed",
	}
	if got := statuses(req); !maps.Equal(got, want) {
		t.Errorf("overwrite and merge: %v", got)
	}
	req.Streams[1].AllowTypeChanges = true
	if got := statuses(req)["stream.b.types"]; got != "passed" {
		t.Errorf("a stream that may change types: %s", got)
	}
	it := e.b.List(&blob.ListOptions{Prefix: reserved + "/check-"})
	if obj, err := it.Next(t.Context()); err == nil {
		t.Errorf("the preflight left %s", obj.Key)
	}
}

// A PreCommit that fails to close one stream's file leaves the epoch's other
// files to Close, which drops them.
func TestFailedPreCommitLeavesItsEpochToClose(t *testing.T) {
	dir := t.TempDir()
	d := newDest(t, map[string]any{"url": "file://" + dir}, nil)
	b, err := d.bucket(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	e := &env{t: t, d: d, b: b}
	w := e.open(1, "a", "b")
	e.recover(w, 1, destination.Epoch{}, nil)
	ep := destination.Epoch{Generation: 1, N: 1}
	e.write(w, ep, "a", 1)
	e.write(w, ep, "b", 2)
	// a's file can't take its name, where a directory is.
	if err := b.WriteAll(t.Context(), reserved+"/p1/staging/1/1/a/part-00000.parquet/x", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PreCommit(t.Context(), ep); err == nil {
		t.Fatal("PreCommit took a file where a directory is")
	}
	if err := w.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	var left []string
	_ = filepath.WalkDir(filepath.Join(dir, reserved, "p1", "staging"), func(p string, de fs.DirEntry, err error) error {
		if err == nil && !de.IsDir() && !strings.Contains(p, "part-00000.parquet/x") {
			left = append(left, p)
		}
		return nil
	})
	if len(left) > 0 {
		t.Errorf("left %v", left)
	}
}

// Close returns within its context's time though the store stops answering
// its deletes, whose files stay for a later Recover or DropStale.
func TestCloseOfAStoreThatHangs(t *testing.T) {
	release := make(chan struct{})
	deletes := make(chan string, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deletes <- r.URL.Path
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	st, err := s3store.New(s3store.Config{Endpoint: srv.URL, Region: "us-east-1", AccessKeyID: sdk.NewSecret("a"), SecretAccessKey: sdk.NewSecret("b")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := st.Open(t.Context(), "b")
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithCancel(context.Background())
	w := &writer{b: b, ctx: wctx, cancel: cancel, parts: map[uint64]map[string]*epochFiles{1: {"a": {files: []stagedFile{{Staged: "x"}, {Staged: "y"}}}}}}
	ctx, stop := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer stop()
	closed := make(chan error, 1)
	go func() { closed <- w.Close(ctx) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close waits on the store")
	}
	if len(deletes) == 0 {
		t.Error("Close deleted nothing")
	}
}

// A file's size is at most 4 GiB, which Commit's copy of it can take: S3
// copies at most 5 GiB at once.
func TestFileSizeLimits(t *testing.T) {
	for size, ok := range map[int64]bool{1<<20 - 1: false, 1 << 20: true, 4 << 30: true, 4<<30 + 1: false} {
		b, _ := json.Marshal(map[string]any{"url": "file://" + t.TempDir(), "fileSize": size})
		if _, err := testDestination(b); (err == nil) != ok {
			t.Errorf("%d: %v", size, err)
		}
	}
}
