package connectortest

import (
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// An emitter's caller may reuse what it passed once the call returns.
func TestRecordersCopyWhatTheyAreGiven(t *testing.T) {
	state := []byte("1")
	added := []source.Split{{ID: "a", State: []byte("s"), Until: []byte("u"), CompleteAt: []byte("c")}}
	reuse := func() {
		state[0] = '2'
		added[0].State[0], added[0].Until[0], added[0].CompleteAt[0] = 'x', 'x', 'x'
		added[0].ID = "b"
	}
	want := func(t *testing.T, got []source.Split) {
		t.Helper()
		if len(got) != 1 || got[0].ID != "a" || string(got[0].State) != "s" || string(got[0].Until) != "u" || string(got[0].CompleteAt) != "c" {
			t.Errorf("recorded %+v", got)
		}
	}

	var r Recorder
	if err := r.ResumePoint("s", state); err != nil {
		t.Fatal(err)
	}
	if err := r.Split("s", "a").Complete(state, added...); err != nil {
		t.Fatal(err)
	}
	reuse()
	if got, _ := r.Last("s"); string(got) != "1" {
		t.Errorf("Last is %q, want 1", got)
	}
	want(t, r.Snapshot()[1].Added)

	state, added = []byte("1"), []source.Split{{ID: "a", State: []byte("s"), Until: []byte("u"), CompleteAt: []byte("c")}}
	var c SplitCapture
	if err := c.Split("s", "a").ResumePoint(state, added...); err != nil {
		t.Fatal(err)
	}
	reuse()
	want(t, c.Added())

	var none Recorder
	_ = none.Complete("s", nil)
	if got, done := none.Last("s"); got != nil || !done {
		t.Errorf("a full refresh's completion recorded %q", got)
	}
}

// A batch's column may hold more values than the batch has rows.
func TestSplitCaptureKeepsTheBatchsRows(t *testing.T) {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues([]int64{1, 2}, nil)
	col := b.NewArray()
	defer col.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	c := SplitCapture{Col: "id"}
	if err := c.Records("s", array.NewRecordBatch(schema, []arrow.Array{col}, 1)); err != nil {
		t.Fatal(err)
	}
	if got := c.Values(); !slices.Equal(got, []string{"1"}) {
		t.Errorf("captured %v, want [1]", got)
	}
}
