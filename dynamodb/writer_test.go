package dynamodb

import (
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// An append deduped within each epoch drops a repeat of an _openetl_id in
// the epoch, and a later epoch's row of that _openetl_id lands beside it;
// deduped against the target, both are skipped; not deduped, every row
// lands.
func TestAppendDedupe(t *testing.T) {
	for _, c := range []struct {
		dedupe    destination.Dedupe
		items     int
		collapsed uint64
	}{{destination.DedupeEpoch, 2, 1}, {destination.DedupeTarget, 1, 2}, {destination.DedupeNone, 3, 0}} {
		db := newMemDB(map[string][]string{controlTable: {"p", "n"}, "t": {idColumn, rowColumn}})
		w := memWriter(t, db, destination.Stream{Name: "t", Mode: destination.Append, Dedupe: c.dedupe})
		cols, row := []string{"v", idColumn}, []*string{sp("a"), sp("id-1")}
		got := memCommit(t, w, 1, "t", memBatch(cols, row, row)).Collapsed["t"]
		got += memCommit(t, w, 2, "t", memBatch(cols, row)).Collapsed["t"]
		if n := len(db.items("t")); n != c.items || got != c.collapsed {
			t.Errorf("dedupe %d: %d items, %d collapsed; want %d and %d", c.dedupe, n, got, c.items, c.collapsed)
		}
	}
}

// An overwrite without a key, deduped within each epoch, drops a repeat of
// an _openetl_id in the epoch, and keeps a later epoch's row of it.
func TestOverwriteDedupeEpoch(t *testing.T) {
	db := newMemDB(map[string][]string{controlTable: {"p", "n"}, "t": {idColumn, rowColumn}})
	w := memWriter(t, db, destination.Stream{Name: "t", Mode: destination.Overwrite, Dedupe: destination.DedupeEpoch})
	if err := w.StartReads(t.Context(), map[string]time.Time{"t": time.Now()}); err != nil {
		t.Fatal(err)
	}
	cols, row := []string{"v", idColumn}, []*string{sp("a"), sp("id-1")}
	got := memCommit(t, w, 1, "t", memBatch(cols, row, row)).Collapsed["t"]
	if err := w.EndRead(t.Context(), destination.Epoch{Generation: 1, N: 2}, "t"); err != nil {
		t.Fatal(err)
	}
	got += memCommit(t, w, 2, "t", memBatch(cols, row)).Collapsed["t"]
	if n := len(db.items("t")); n != 2 || got != 1 {
		t.Errorf("%d items, %d collapsed; want 2 and 1", n, got)
	}
}
