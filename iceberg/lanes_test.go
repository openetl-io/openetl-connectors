//go:build integration

package iceberg

import (
	"context"
	"encoding/json"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	ice "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// openLanes opens n lanes of an attempt, lane 0 recovered.
func (e *env) openLanes(gen uint64, n int, epoch destination.Epoch, staged []destination.Staged, streams ...destination.Stream) ([]destination.Writer, destination.Recovery) {
	e.t.Helper()
	out := make([]destination.Writer, n)
	var r destination.Recovery
	for lane := range out {
		cfg := destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: destination.ScopeStream, Streams: streams, Lane: lane}
		w, err := e.d.Open(e.t.Context(), cfg)
		if err != nil {
			e.t.Fatal(err)
		}
		e.t.Cleanup(func() { _ = w.Close(context.Background()) })
		out[lane] = w
		if lane == 0 {
			if r, err = w.Recover(e.t.Context(), gen, epoch, staged); err != nil {
				e.t.Fatal(err)
			}
		}
	}
	return out, r
}

func startLanes(t *testing.T, ws []destination.Writer, stream string, at time.Time) {
	t.Helper()
	for _, w := range ws {
		if err := w.(destination.ReadTracker).StartReads(t.Context(), map[string]time.Time{stream: at}); err != nil {
			t.Fatal(err)
		}
	}
}

func precommitAll(t *testing.T, e destination.Epoch, ws []destination.Writer) []destination.Staged {
	t.Helper()
	out := make([]destination.Staged, len(ws))
	for i, w := range ws {
		out[i] = precommit(t, w, e)
	}
	return out
}

func commitAll(t *testing.T, w destination.Writer, e destination.Epoch, st []destination.Staged) {
	t.Helper()
	if _, err := w.Commit(t.Context(), e, st); err != nil {
		t.Fatal(err)
	}
}

func describe(t *testing.T, st destination.Staged) stagedStream {
	t.Helper()
	var d staged
	if err := json.Unmarshal(st, &d); err != nil {
		t.Fatal(err)
	}
	if len(d.Streams) != 1 {
		t.Fatalf("descriptor of %d streams: %s", len(d.Streams), st)
	}
	return d.Streams[0]
}

// objects lists what the table's location holds under the path.
func (e *env) objects(tbl *table.Table, path string) []string {
	e.t.Helper()
	store, err := tbl.FS(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	var out []string
	_ = store.(iceio.ListableIO).WalkDir(strings.TrimSuffix(tbl.Location(), "/")+"/"+path, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			out = append(out, p)
		}
		return nil
	})
	return out
}

// A long read's descriptors stay small, as its earlier files are in
// manifests of its lanes. The read's end replaces the table's files with every
// lane's, and its manifests go.
func TestOverwriteKeepsEarlierFilesInManifests(t *testing.T) {
	env := newEnv(t)
	ws, _ := env.openLanes(1, 3, destination.Epoch{}, nil, overwrites("items")...)
	startLanes(t, ws, "items", time.UnixMicro(1000))
	const epochs = 16
	var want []int64
	var st []destination.Staged
	for n := uint64(1); n <= epochs; n++ {
		e := destination.Epoch{Generation: 1, N: n}
		for lane, w := range ws {
			id := int64(n)*10 + int64(lane)
			write(t, w, e, "items", id)
			want = append(want, id)
		}
		st = precommitAll(t, e, ws)
		commitAll(t, ws[0], e, st)
	}
	env.wantIDs("items")
	tbl := env.table("items")
	for lane, s := range st {
		d := describe(t, s)
		if len(d.Earlier) != 0 {
			t.Errorf("lane %d's descriptor lists %d earlier files", lane, len(d.Earlier))
		}
		files := 0
		for _, ref := range d.Manifests {
			files += ref.Files
		}
		if files != epochs-1 {
			t.Errorf("lane %d's manifests hold %d files, want %d", lane, files, epochs-1)
		}
		if len(d.Manifests) > 4 {
			t.Errorf("lane %d's descriptor has %d manifests", lane, len(d.Manifests))
		}
		if len(s) > 1024 {
			t.Errorf("lane %d's descriptor has %d bytes", lane, len(s))
		}
	}
	if len(env.objects(tbl, "_openetl")) == 0 {
		t.Error("no manifests under the table's _openetl")
	}

	end := destination.Epoch{Generation: 1, N: epochs + 1}
	write(t, ws[0], end, "items", 1000)
	want = append(want, 1000)
	if err := ws[1].(destination.ReadTracker).EndRead(t.Context(), end, "items"); err != nil {
		t.Fatal(err)
	}
	commitAll(t, ws[0], end, precommitAll(t, end, ws))
	slices.Sort(want)
	env.wantIDs("items", want...)
	if left := env.objects(env.table("items"), "_openetl"); len(left) != 0 {
		t.Errorf("manifests left after the read: %v", left)
	}

	// The lanes that didn't know it ended go on describing it. What they
	// save of it goes with the next read.
	stale := destination.Epoch{Generation: 1, N: epochs + 2}
	st = precommitAll(t, stale, ws)
	if d := describe(t, st[0]); len(d.Manifests) == 0 || d.Ended {
		t.Errorf("lane 0 describes %+v", d)
	}
	commitAll(t, ws[0], stale, st)
	env.wantIDs("items", want...)
	if left := env.objects(env.table("items"), "_openetl"); len(left) == 0 {
		t.Error("no manifests saved after the read ended")
	}
	startLanes(t, ws, "items", time.UnixMicro(2000))
	next := destination.Epoch{Generation: 1, N: epochs + 3}
	write(t, ws[0], next, "items", 1)
	if err := ws[0].(destination.ReadTracker).EndRead(t.Context(), next, "items"); err != nil {
		t.Fatal(err)
	}
	commitAll(t, ws[0], next, precommitAll(t, next, ws))
	env.wantIDs("items", 1)
	if left := env.objects(env.table("items"), "_openetl"); len(left) != 0 {
		t.Errorf("manifests left after the next read: %v", left)
	}
}

// Lane 0 takes over every lane's references when an attempt has fewer lanes,
// and keeps them going in its committables, with or without rows.
func TestOverwriteRecoversAtFewerLanes(t *testing.T) {
	env := newEnv(t)
	start := time.UnixMicro(1000)
	ws, _ := env.openLanes(1, 3, destination.Epoch{}, nil, overwrites("items")...)
	startLanes(t, ws, "items", start)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	for lane, w := range ws {
		write(t, w, e1, "items", int64(lane+1))
	}
	commitAll(t, ws[0], e1, precommitAll(t, e1, ws))
	write(t, ws[1], e2, "items", 4)
	write(t, ws[2], e2, "items", 5)
	st2 := precommitAll(t, e2, ws)
	if d := describe(t, st2[0]); len(d.Manifests) != 1 || len(d.Files) != 0 {
		t.Errorf("lane 0 without rows in the epoch describes %+v", d)
	}

	one, r := env.openLanes(2, 1, e2, st2, overwrites("items")...)
	if r != destination.Applied {
		t.Errorf("recover: %v", r)
	}
	startLanes(t, one, "items", start)
	e3 := destination.Epoch{Generation: 2, N: 3}
	st3 := precommitAll(t, e3, one)
	d := describe(t, st3[0])
	files := 0
	for _, ref := range d.Manifests {
		files += ref.Files
	}
	if files != 5 {
		t.Errorf("lane 0 keeps %d files in its manifests, want 5: %s", files, st3[0])
	}
	commitAll(t, one[0], e3, st3)
	env.wantIDs("items")

	two, r := env.openLanes(3, 2, e3, st3, overwrites("items")...)
	if r != destination.Applied {
		t.Errorf("recover: %v", r)
	}
	startLanes(t, two, "items", start)
	e4 := destination.Epoch{Generation: 3, N: 4}
	write(t, two[0], e4, "items", 6)
	if err := two[1].(destination.ReadTracker).EndRead(t.Context(), e4, "items"); err != nil {
		t.Fatal(err)
	}
	commitAll(t, two[0], e4, precommitAll(t, e4, two))
	env.wantIDs("items", 1, 2, 3, 4, 5, 6)
}

// An attempt that starts a read over drops every lane's files of the one it
// abandoned, and its manifests.
func TestOverwriteDropsAnAbandonedRead(t *testing.T) {
	env := newEnv(t)
	ws, _ := env.openLanes(1, 2, destination.Epoch{}, nil, overwrites("items")...)
	startLanes(t, ws, "items", time.UnixMicro(1000))
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	write(t, ws[0], e1, "items", 1)
	write(t, ws[1], e1, "items", 2)
	st1 := precommitAll(t, e1, ws)
	commitAll(t, ws[0], e1, st1)
	st2 := precommitAll(t, e2, ws)
	commitAll(t, ws[0], e2, st2)
	files := slices.Concat(describe(t, st1[0]).Files, describe(t, st1[1]).Files)

	next, r := env.openLanes(2, 2, e2, st2, overwrites("items")...)
	if r != destination.Applied {
		t.Errorf("recover: %v", r)
	}
	tbl := env.table("items")
	if !next[0].(*writer).allExist(t.Context(), tbl, files) {
		t.Fatal("the read's files aren't there before it's abandoned")
	}
	startLanes(t, next, "items", time.UnixMicro(2000))
	for _, f := range files {
		if next[0].(*writer).allExist(t.Context(), tbl, []string{f}) {
			t.Errorf("%s is left", f)
		}
	}
	if left := env.objects(tbl, "_openetl"); len(left) != 0 {
		t.Errorf("manifests left: %v", left)
	}
}

// oldDescriptor is a descriptor from before manifests: it lists the read's
// earlier files.
func (e *env) oldDescriptor(gen, epoch uint64, start int64, earlier, files []string, ended bool) destination.Staged {
	e.t.Helper()
	b, err := json.Marshal(map[string]any{
		"generation": gen, "epoch": epoch, "namespace": []string{e.ns},
		"streams": []map[string]any{{
			"name": "items", "table": "items", "files": files, "mode": "overwrite", "read": start, "earlier": earlier, "ended": ended,
		}},
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return b
}

// A recorded descriptor's list of earlier files still holds: the read
// continues with them in a manifest, or an epoch it ended in is redone.
func TestOverwriteDescriptorsFromBeforeManifests(t *testing.T) {
	stage := func(env *env) (first, second []string, start time.Time) {
		start = time.UnixMicro(1000)
		w := env.open(1, destination.ScopeStream, overwrites("items")...)
		recov(t, w, 1, destination.Epoch{}, nil)
		startLanes(t, []destination.Writer{w}, "items", start)
		e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
		write(t, w, e1, "items", 1, 2)
		first = describe(t, precommit(t, w, e1)).Files
		write(t, w, e2, "items", 3)
		second = describe(t, precommit(t, w, e2)).Files
		return first, second, start
	}
	t.Run("continues", func(t *testing.T) {
		env := newEnv(t)
		first, second, start := stage(env)
		old := env.oldDescriptor(1, 2, start.UnixMicro(), first, second, false)
		ws, r := env.openLanes(2, 2, destination.Epoch{Generation: 1, N: 2}, []destination.Staged{old}, overwrites("items")...)
		if r != destination.Applied {
			t.Errorf("recover: %v", r)
		}
		startLanes(t, ws, "items", start)
		e3 := destination.Epoch{Generation: 2, N: 3}
		write(t, ws[1], e3, "items", 4)
		if err := ws[1].(destination.ReadTracker).EndRead(t.Context(), e3, "items"); err != nil {
			t.Fatal(err)
		}
		st := precommitAll(t, e3, ws)
		d := describe(t, st[0])
		if len(d.Earlier) != 0 || len(d.Manifests) != 1 || d.Manifests[0].Files != len(first)+len(second) {
			t.Errorf("lane 0 keeps %s", st[0])
		}
		commitAll(t, ws[0], e3, st)
		env.wantIDs("items", 1, 2, 3, 4)
	})
	t.Run("ended", func(t *testing.T) {
		env := newEnv(t)
		first, second, start := stage(env)
		old := env.oldDescriptor(1, 2, start.UnixMicro(), first, second, true)
		_, r := env.openLanes(2, 1, destination.Epoch{Generation: 1, N: 2}, []destination.Staged{old}, overwrites("items")...)
		if r != destination.Redone {
			t.Errorf("recover: %v", r)
		}
		env.wantIDs("items", 1, 2, 3)
	})
}

// A lane that left a column's values out, since the table couldn't take their
// type, fails the epoch if the table can by then: lane 0's ApplySchema would
// have no way to know of them.
func TestLaneChecksTheValuesItLeftOut(t *testing.T) {
	env := newEnv(t)
	streams := appends("items")
	ws, _ := env.openLanes(1, 2, destination.Epoch{}, nil, streams...)
	narrow := arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	wide := arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	if _, err := ws[0].(destination.SchemaApplier).ApplySchema(t.Context(), []destination.StreamSchema{{Name: "items", Schema: arrow.NewSchema([]arrow.Field{idField, narrow}, nil)}}); err != nil {
		t.Fatal(err)
	}
	rows := batch(t, []arrow.Field{idField, wide}, `[{"id": 1, "x": 5000000000}]`)
	e1, e2 := destination.Epoch{Generation: 1, N: 1}, destination.Epoch{Generation: 1, N: 2}
	if err := ws[1].Write(t.Context(), e1, "items", rows); err != nil {
		t.Fatal(err)
	}
	precommit(t, ws[1], e1)

	if err := ws[1].Write(t.Context(), e2, "items", rows); err != nil {
		t.Fatal(err)
	}
	tbl := env.table("items")
	tx := tbl.NewTransaction()
	if err := tx.UpdateSchema(true, false).UpdateColumn([]string{"x"}, table.ColumnUpdate{FieldType: ice.Optional[ice.Type]{Valid: true, Val: ice.PrimitiveTypes.Int64}}).Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := ws[1].PreCommit(t.Context(), e2); err == nil || !strings.Contains(err.Error(), "left out") {
		t.Errorf("pre-commit: %v, want the values left out", err)
	}
}

// With some lane's files gone the epoch is lost as a whole, if nothing of it
// is applied.
func TestRecoverWithALanesFilesGone(t *testing.T) {
	env := newEnv(t)
	ws, _ := env.openLanes(1, 2, destination.Epoch{}, nil, appends("items", "others")...)
	e1 := destination.Epoch{Generation: 1, N: 1}
	write(t, ws[0], e1, "items", 1)
	write(t, ws[1], e1, "items", 2)
	write(t, ws[0], e1, "others", 3)
	st := precommitAll(t, e1, ws)
	var d staged
	if err := json.Unmarshal(st[1], &d); err != nil {
		t.Fatal(err)
	}
	tbl := env.table("items")
	store, err := tbl.FS(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(d.Streams[0].Files[0]); err != nil {
		t.Fatal(err)
	}
	_, r := env.openLanes(2, 3, e1, st, appends("items", "others")...)
	if r != destination.Lost {
		t.Errorf("recover: %v, want lost", r)
	}
	env.wantIDs("items")
	env.wantIDs("others")
}

// Every lane's files reach the table in one snapshot, and a stream's lanes
// keep their own names.
func TestOneSnapshotOverLanes(t *testing.T) {
	env := newEnv(t)
	ws, _ := env.openLanes(1, 3, destination.Epoch{}, nil, appends("items")...)
	e1 := destination.Epoch{Generation: 1, N: 1}
	seen := map[string]bool{}
	for lane, w := range ws {
		write(t, w, e1, "items", int64(lane+1))
	}
	st := precommitAll(t, e1, ws)
	for lane, s := range st {
		for _, f := range describe(t, s).Files {
			if seen[f] {
				t.Errorf("lane %d's file %s is another lane's", lane, f)
			}
			seen[f] = true
		}
	}
	before := len(env.table("items").Metadata().Snapshots())
	commitAll(t, ws[0], e1, st)
	if got := len(env.table("items").Metadata().Snapshots()); got != before+1 {
		t.Errorf("%d snapshots for one epoch", got-before)
	}
	env.wantIDs("items", 1, 2, 3)
	if s := env.table("items").CurrentSnapshot().Summary.Properties[epochKey]; s != "1" {
		t.Errorf("the snapshot's epoch is %q", s)
	}
}
