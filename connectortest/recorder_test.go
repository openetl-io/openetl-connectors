package connectortest

import (
	"testing"

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
