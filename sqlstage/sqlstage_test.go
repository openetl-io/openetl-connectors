package sqlstage

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

func TestMergeLanesRefusesOtherEpochs(t *testing.T) {
	w := NewEpochs[*Table]()
	w.SetTable("users", "users")
	a, _ := json.Marshal(Staged{Generation: 1, Epoch: 1})
	b, _ := json.Marshal(Staged{Generation: 1, Epoch: 2})
	if _, err := w.MergeLanes("db", []destination.Staged{a, b}); err == nil {
		t.Error("merged two epochs' descriptors")
	}
	if _, err := w.MergeLanes("db", nil); err == nil {
		t.Error("merged no descriptors")
	}
	c, _ := json.Marshal(Staged{Generation: 1, Epoch: 1, Streams: []Stream{{Name: "users", Staging: "s1", Rows: 2, Ended: true, Read: "r", Mode: ModeOverwrite, Fills: map[string]string{"a": "b"}, Last: []string{"x"}}}})
	d, _ := json.Marshal(Staged{Generation: 1, Epoch: 1, Streams: []Stream{{Name: "users", Staging: "s2", Rows: 3, Read: "r", Mode: ModeOverwrite, Patch: "p2", Fills: map[string]string{"c": "d"}, Last: []string{"x", "y"}}}})
	got, err := w.MergeLanes("db", []destination.Staged{a, d, c})
	if err != nil {
		t.Fatal(err)
	}
	s := got.Streams[0]
	if s.Table != "users" || s.Rows != 5 || !s.Ended || !slices.Equal(s.Stagings, []string{"s2", "s1"}) || !slices.Equal(s.Patches, []string{"p2"}) ||
		len(s.Fills) != 2 || !slices.Equal(s.Last, []string{"x", "y"}) {
		t.Errorf("merged %+v", s)
	}
	e, _ := json.Marshal(Staged{Generation: 1, Epoch: 1, Streams: []Stream{{Name: "users", Read: "other"}}})
	if _, err := w.MergeLanes("db", []destination.Staged{c, e}); err == nil {
		t.Error("merged two reads")
	}
}
