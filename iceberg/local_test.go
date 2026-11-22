package iceberg

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// The tests in this file run against a Hadoop catalog in a temporary
// directory, whose tables keep their files on local disk.

func localCatalog(t *testing.T) catalog.Catalog {
	t.Helper()
	cat, err := hadoop.NewCatalog("local", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

// localWriter opens a writer of pipeline p on cat, with stream scope.
func localWriter(t *testing.T, cat catalog.Catalog, gen uint64, streams ...destination.Stream) *writer {
	t.Helper()
	d := &Destination{cfg: DestinationConfig{CatalogConfig: defaultCatalogConfig()}}
	d.conn = &conn{cfg: d.cfg.CatalogConfig}
	w := d.newWriter(t.Context(), cat, destination.OpenConfig{Pipeline: "p", Generation: gen, Scope: destination.ScopeStream, Streams: streams})
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

var localFields = []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}}

// localBatch is rows, a JSON array of objects of localFields.
func localBatch(t *testing.T, rows string) arrow.RecordBatch {
	t.Helper()
	b, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema(localFields, nil), strings.NewReader(rows))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Release)
	return b
}

// localWrite writes rows, as localBatch takes them, to stream in epoch e.
func localWrite(t *testing.T, w *writer, e destination.Epoch, stream, rows string) {
	t.Helper()
	if err := w.Write(t.Context(), e, stream, localBatch(t, rows)); err != nil {
		t.Fatalf("write %s: %v", stream, err)
	}
}

// localDataDir is the table's data directory.
func localDataDir(t *testing.T, cat catalog.Catalog, name string) string {
	t.Helper()
	tbl, err := cat.LoadTable(t.Context(), table.Identifier{"default", name})
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(tbl.Location())
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(u.Path, "data")
}

// localRows is the table's rows, by id: each one's name.
func localRows(t *testing.T, cat catalog.Catalog, name string) map[int64]string {
	t.Helper()
	tbl, err := cat.LoadTable(t.Context(), table.Identifier{"default", name})
	if err != nil {
		t.Fatal(err)
	}
	_, it, err := tbl.Scan().ToArrowRecords(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	out := map[int64]string{}
	for b, err := range it {
		if err != nil {
			t.Fatalf("scan %s: %v", name, err)
		}
		ids, names := b.Column(b.Schema().FieldIndices("id")[0]), b.Column(b.Schema().FieldIndices("name")[0])
		for r := range int(b.NumRows()) {
			out[ids.(*array.Int64).Value(r)] = names.ValueStr(r)
		}
		b.Release()
	}
	return out
}

func TestLocalCommit(t *testing.T) {
	cat := localCatalog(t)
	w := localWriter(t, cat, 1, destination.Stream{Name: "users", Mode: destination.Append})
	e := destination.Epoch{Generation: 1, N: 1}
	localWrite(t, w, e, "users", `[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]`)
	st, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), e, []destination.Staged{st}); err != nil {
		t.Fatal(err)
	}
	if got := localRows(t, cat, "users"); len(got) != 2 || got[1] != "a" || got[2] != "b" {
		t.Errorf("users holds %v", got)
	}
}

// hookCatalog runs hook once, once the at-th LoadTable has read its table.
// The tables it loads commit through cat itself.
type hookCatalog struct {
	catalog.Catalog
	mu        sync.Mutex
	loads, at int
	hook      func()
}

func (c *hookCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, ident)
	c.mu.Lock()
	c.loads++
	run := c.loads == c.at
	c.mu.Unlock()
	if run {
		c.hook()
	}
	return tbl, err
}

// A newer generation can raise the fence, redo the epoch and drop a merge's
// staged files while an older one recovers it. The older one then finds
// those files gone, and is fenced: the epoch isn't lost, and the files it
// staged for an append, which are the table's now, stay.
func TestRecoverIsLostOnlyBehindItsOwnFence(t *testing.T) {
	cat := localCatalog(t)
	streams := []destination.Stream{{Name: "a", Mode: destination.Append}, {Name: "m", Mode: destination.Merge, PrimaryKey: []string{"id"}}}
	e := destination.Epoch{Generation: 1, N: 1}
	w1 := localWriter(t, cat, 1, streams...)
	localWrite(t, w1, e, "a", `[{"id": 1, "name": "a1"}]`)
	localWrite(t, w1, e, "m", `[{"id": 1, "name": "m1"}]`)
	st, err := w1.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	staged := []destination.Staged{st}

	hc := &hookCatalog{Catalog: cat}
	w2 := localWriter(t, hc, 2, streams...)
	if _, err := w2.raiseFence(t.Context(), w2.ns, []string{"a", "m"}, 2); err != nil {
		t.Fatal(err)
	}
	hc.at = hc.loads + 2
	hc.hook = func() {
		w3 := localWriter(t, cat, 3, streams...)
		if r, err := w3.Recover(t.Context(), 3, e, staged); err != nil || r != destination.Redone {
			t.Errorf("the newer generation's recovery is %v, %v; want redone", r, err)
		}
	}
	if r, err := w2.Recover(t.Context(), 2, e, staged); !errors.Is(err, destination.ErrFenced) {
		t.Errorf("the older generation's recovery is %v, %v; want fenced", r, err)
	}
	if got := localRows(t, cat, "a"); len(got) != 1 || got[1] != "a1" {
		t.Errorf("a holds %v; want 1: a1", got)
	}
}

// A PreCommit that fails on one stream's files leaves the epoch's other
// files to Close, which drops them, as it does an epoch's that was never
// pre-committed.
func TestCloseDropsAFailedPreCommitsFiles(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes into a directory it can't write to")
	}
	cat := localCatalog(t)
	w := localWriter(t, cat, 1, destination.Stream{Name: "a", Mode: destination.Append}, destination.Stream{Name: "b", Mode: destination.Append})
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	localWrite(t, w, e1, "a", `[{"id": 1, "name": "a1"}]`)
	localWrite(t, w, e1, "b", `[{"id": 1, "name": "b1"}]`)
	st, err := w.PreCommit(t.Context(), e1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), e1, []destination.Staged{st}); err != nil {
		t.Fatal(err)
	}
	dir := localDataDir(t, cat, "a")
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	_ = w.Write(t.Context(), e2, "a", localBatch(t, `[{"id": 2, "name": "a2"}]`))
	localWrite(t, w, e2, "b", `[{"id": 2, "name": "b2"}]`)
	if _, err := w.PreCommit(t.Context(), e2); err == nil {
		t.Fatal("PreCommit wrote into a directory it can't write to")
	}
	if err := w.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadDir(localDataDir(t, cat, "b")); err != nil || len(got) != 1 {
		t.Errorf("b's data files are %v, %v; want only epoch 1's", got, err)
	}
}

// deniedCatalog refuses to load a table, as a REST catalog does a principal
// without the privilege.
type deniedCatalog struct{ catalog.Catalog }

func (deniedCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return nil, fmt.Errorf("%w: the principal can't read the table", rest.ErrForbidden)
}

// A writer's refusals carry their codes, so a run fails at once rather than
// retrying what can't succeed.
func TestWriterErrorsHaveCodes(t *testing.T) {
	cat := localCatalog(t)
	streams := []destination.Stream{{Name: "users", Mode: destination.Append}}
	e := destination.Epoch{Generation: 1, N: 1}
	w := localWriter(t, cat, 1, streams...)
	localWrite(t, w, e, "users", `[{"id": 1, "name": "a"}]`)
	st, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	code := func(err error) protocolv1.ErrorCode {
		if c, ok := errors.AsType[interface {
			error
			ErrorCode() protocolv1.ErrorCode
		}](err); ok {
			return c.ErrorCode()
		}
		return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
	}
	denied := localWriter(t, deniedCatalog{cat}, 2, streams...)
	if err := denied.Write(t.Context(), destination.Epoch{Generation: 2, N: 2}, "users", localBatch(t, `[{"id": 2, "name": "b"}]`)); code(err) != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Errorf("write: %v, coded %v; want permission denied", err, code(err))
	}
	if _, err := denied.Commit(t.Context(), e, []destination.Staged{st}); code(err) != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Errorf("commit: %v, coded %v; want permission denied", err, code(err))
	}
	if _, err := denied.Recover(t.Context(), 2, e, []destination.Staged{st}); code(err) != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Errorf("recover: %v, coded %v; want permission denied", err, code(err))
	}
}
