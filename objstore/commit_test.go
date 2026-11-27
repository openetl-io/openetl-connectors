package objstore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// A commit stopped at any of its writes, as by a crash, leaves the table as
// it was or partly applied, never a row twice, and leaves what the next
// generation's Recover finishes: each row once, every stream's marker, an
// overwrite's manifest listing its whole read, and nothing staged.
func TestCommitStoppedAtAnyWrite(t *testing.T) {
	for _, mode := range []destination.WriteMode{destination.Append, destination.Overwrite} {
		t.Run(modeName(mode), func(t *testing.T) {
			writes := commitStoppedAt(t, mode, 0)
			for k := 1; k <= writes; k++ {
				t.Run(strconv.Itoa(k), func(t *testing.T) { commitStoppedAt(t, mode, k) })
			}
		})
	}
}

// commitStoppedAt commits an epoch of two streams that three lanes wrote,
// after an epoch of one lane, on a store that refuses every request from
// the commit's k-th write on (none for 0), then recovers it on the next
// generation. It returns how many writes the commit made.
func commitStoppedAt(t *testing.T, mode destination.WriteMode, k int) int {
	e, f := fakeEnv(t)
	ctx := t.Context()
	streams := []string{"users", "orders"}
	open := func(gen uint64, lane int) destination.Writer {
		cfg := destination.OpenConfig{Pipeline: "p1", Generation: gen, Scope: destination.ScopeStream, Lane: lane}
		for _, s := range streams {
			cfg.Streams = append(cfg.Streams, destination.Stream{Name: s, Mode: mode})
		}
		w, err := e.d.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Close(context.Background()) })
		return w
	}
	// Overwrite: each generation reads every stream again, from start.
	read := func(w destination.Writer, start int64) {
		if mode != destination.Overwrite {
			return
		}
		starts := map[string]time.Time{}
		for _, s := range streams {
			starts[s] = time.UnixMicro(start)
		}
		if err := w.(destination.ReadTracker).StartReads(ctx, starts); err != nil {
			t.Fatal(err)
		}
	}
	end := func(w destination.Writer, ep destination.Epoch) {
		if mode != destination.Overwrite {
			return
		}
		for _, s := range streams {
			if err := w.(destination.ReadTracker).EndRead(ctx, ep, s); err != nil {
				t.Fatal(err)
			}
		}
	}

	w1 := open(1, 0)
	e.recover(w1, 1, destination.Epoch{}, nil)
	read(w1, 1)
	e1 := destination.Epoch{Generation: 1, N: 1}
	for _, s := range streams {
		e.write(w1, e1, s, 1)
	}
	end(w1, e1)
	st1 := e.precommit(w1, e1)
	e.commit(w1, e1, st1)

	lanes := []destination.Writer{open(2, 0), open(2, 1), open(2, 2)}
	e.recover(lanes[0], 2, e1, st1)
	e2 := destination.Epoch{Generation: 2, N: 2}
	for i, w := range lanes {
		read(w, 2)
		for _, s := range streams {
			e.write(w, e2, s, int64(10+i))
		}
	}
	end(lanes[0], e2)
	var st2 []destination.Staged
	for _, w := range lanes {
		st2 = append(st2, e.precommit(w, e2))
	}

	var writes atomic.Int64
	var stopped atomic.Bool
	f.setHook(func(_ context.Context, op, _ string) int {
		if op == "put" || op == "copy" || op == "delete" {
			if n := writes.Add(1); k > 0 && n >= int64(k) {
				stopped.Store(true)
			}
		}
		if stopped.Load() {
			return http.StatusForbidden
		}
		return 0
	})
	_, err := lanes[0].Commit(ctx, e2, st2)
	f.setHook(nil)
	if k == 0 && err != nil {
		t.Fatal(err)
	}

	before, after := []int64{1}, []int64{10, 11, 12}
	marked := 0
	for _, s := range streams {
		if mode == destination.Overwrite {
			// A reader of the manifest sees one whole read or the other.
			if got := e.manifestIDs(s); !slices.Equal(got, before) && !slices.Equal(got, after) {
				t.Errorf("%s: the stopped commit's manifest lists %v", s, got)
			}
		} else if got := e.ids(s); !slices.Contains(got, 1) || !atMostOnce(got, append(before, after...)) {
			t.Errorf("%s: the stopped commit left %v", s, got)
		}
		if ok, err := e.b.Exists(ctx, lanes[0].(*writer).marker(s, e2.N, e2.Generation)); err != nil {
			t.Fatal(err)
		} else if ok {
			marked++
		}
	}
	first := destination.Redone
	if marked == len(streams) {
		first = destination.Applied
	}
	w3 := open(3, 0)
	for i, want := range []destination.Recovery{first, destination.Applied} {
		if r, err := w3.Recover(ctx, 3, e2, st2); err != nil || r != want {
			t.Fatalf("recover %d of a commit stopped at write %d: %v, %v, want %v", i+1, k, r, err, want)
		}
	}
	for _, s := range streams {
		if mode == destination.Overwrite {
			if got := e.manifestIDs(s); !slices.Equal(got, after) {
				t.Errorf("%s: the manifest lists %v, want %v", s, got, after)
			}
			for _, key := range e.keys(s + "/" + readsDir + "/") {
				if !strings.HasPrefix(key, readDir(s, time.UnixMicro(2))) {
					t.Errorf("%s: an older read's %s stays", s, key)
				}
			}
		} else {
			e.wantIDs(s, append(before, after...)...)
		}
		if ok, err := e.b.Exists(ctx, w3.(*writer).marker(s, e2.N, e2.Generation)); err != nil || !ok {
			t.Errorf("%s: no marker: %v", s, err)
		}
	}
	if got := e.keys(reserved + "/p1/staging/"); len(got) != 0 {
		t.Errorf("staging left: %v", got)
	}
	return int(writes.Load())
}

// A commit copies commitConcurrency files at once across its streams,
// writes a stream's manifest and marker only once all its files are in
// place, and deletes what it staged only once every stream's marker is.
func TestCommitOrder(t *testing.T) {
	e, f := fakeEnv(t)
	w := e.open(1, "a", "b")
	ep := destination.Epoch{Generation: 1, N: 1}
	desc := staged{Generation: 1, Epoch: 1, Streams: []stagedStream{
		{Name: "a", Table: "a"},
		{Name: "b", Table: "b", Read: "5", Ended: true},
	}}
	dirs := []string{"a/", readDir("b", time.UnixMicro(5))}
	// The keys on f a stream's manifest and marker wait for, by their keys.
	waits := map[string][]string{}
	var markers []string
	for i, n := range []int{20, 5} {
		st := &desc.Streams[i]
		for j := range n {
			file := stagedFile{
				Staged: fmt.Sprintf("%s/p1/staging/1/1/%s/part-%05d.parquet", reserved, st.Table, j),
				Target: fmt.Sprintf("%sp1-%05d.parquet", dirs[i], j),
			}
			f.put("p/"+file.Staged, []byte("x"))
			st.Files = append(st.Files, file)
		}
		marker := "p/" + w.(*writer).marker(st.Name, 1, 1)
		markers = append(markers, marker)
		for _, file := range st.Files {
			waits[marker] = append(waits[marker], "p/"+file.Target)
		}
		if st.Ended {
			waits["p/"+st.Table+"/"+manifestPath] = waits[marker]
		}
	}
	var inflight, peak atomic.Int64
	full := make(chan struct{})
	var once sync.Once
	f.setHook(func(ctx context.Context, op, key string) int {
		switch {
		case op == "copy":
			n := inflight.Add(1)
			defer inflight.Add(-1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			// Until as many copies run as may, and a while for more to come,
			// or a longer while if fewer do.
			if n == commitConcurrency {
				time.AfterFunc(200*time.Millisecond, func() { once.Do(func() { close(full) }) })
			}
			select {
			case <-full:
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				once.Do(func() { close(full) })
			}
		case op == "put" && waits[key] != nil:
			for _, k := range waits[key] {
				if !f.has(k) {
					t.Errorf("%s written before %s", key, k)
				}
			}
		case op == "delete" && strings.Contains(key, "/staging/"):
			for _, k := range markers {
				if !f.has(k) {
					t.Errorf("%s deleted before %s", key, k)
				}
			}
		}
		return 0
	})
	body, _ := json.Marshal(desc)
	if _, err := w.Commit(t.Context(), ep, []destination.Staged{body}); err != nil {
		t.Fatal(err)
	}
	if got := peak.Load(); got != commitConcurrency {
		t.Errorf("%d copies at once, want %d", got, commitConcurrency)
	}
	for k, keys := range waits {
		if !f.has(k) {
			t.Errorf("%s missing", k)
		}
		for _, target := range keys {
			if !f.has(target) {
				t.Errorf("%s missing", target)
			}
		}
	}
	if got := e.keys(reserved + "/p1/staging/"); len(got) != 0 {
		t.Errorf("staging left: %v", got)
	}
}

// atMostOnce: got, sorted, has each id at most once, and only want's.
func atMostOnce(got, want []int64) bool {
	return len(slices.Compact(slices.Clone(got))) == len(got) &&
		!slices.ContainsFunc(got, func(id int64) bool { return !slices.Contains(want, id) })
}

// BenchmarkCommit commits an epoch of files staged on fakeS3, whose copies
// take copyLatency and its other requests requestLatency, as S3's copies
// inside a bucket take time by the bytes they copy.
func BenchmarkCommit(b *testing.B) {
	const copyLatency, requestLatency = 50 * time.Millisecond, 5 * time.Millisecond
	for _, shape := range []struct{ streams, files int }{{1, 1}, {1, 10}, {1, 64}, {32, 1}} {
		b.Run(fmt.Sprintf("streams=%d/files=%d", shape.streams, shape.files), func(b *testing.B) {
			f := newFakeS3(b)
			f.setHook(func(ctx context.Context, op, _ string) int {
				d := requestLatency
				if op == "copy" {
					d = copyLatency
				}
				select {
				case <-time.After(d):
				case <-ctx.Done():
				}
				return 0
			})
			d := fakeDestination(b, f, nil)
			bk, err := d.bucket(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = bk.Close() })
			w := &writer{d: d, b: bk, cfg: destination.OpenConfig{Pipeline: "p"}}
			b.StopTimer()
			for i := range b.N {
				ep := destination.Epoch{Generation: 1, N: uint64(i + 1)}
				desc := staged{Generation: ep.Generation, Epoch: ep.N}
				for s := range shape.streams {
					st := stagedStream{Name: fmt.Sprintf("s%d", s), Table: fmt.Sprintf("s%d", s)}
					for j := range shape.files {
						file := stagedFile{
							Staged: fmt.Sprintf("%s/p/staging/1/%d/%s/part-%05d.parquet", reserved, ep.N, st.Table, j),
							Target: fmt.Sprintf("%s/p-%020d-g%020d-%05d.parquet", st.Table, ep.N, ep.Generation, j),
							Rows:   1,
						}
						f.put("p/"+file.Staged, []byte("x"))
						st.Files = append(st.Files, file)
					}
					desc.Streams = append(desc.Streams, st)
				}
				body, _ := json.Marshal(desc)
				b.StartTimer()
				if _, err := w.Commit(b.Context(), ep, []destination.Staged{body}); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
			}
		})
	}
}
