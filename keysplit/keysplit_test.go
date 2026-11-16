package keysplit

import (
	"testing"

	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestKeysplit(t *testing.T) {
	var r Reads
	a, b := r.Start("s", "a", true), r.Start("s", "b", false)
	if ok, _ := r.Resplit(t.Context(), "s", "a"); !ok || !a.Asked.Load() {
		t.Error("a split that resplits wasn't asked")
	}
	if ok, _ := r.Resplit(t.Context(), "s", "a"); ok {
		t.Error("a split was asked twice")
	}
	if ok, _ := r.Resplit(t.Context(), "s", "b"); ok || b.Asked.Load() {
		t.Error("a split that doesn't resplit was asked")
	}
	r.End("s", "a")
	if ok, _ := r.Resplit(t.Context(), "s", "a"); ok {
		t.Error("a split that ended was asked")
	}

	floor, err := Floor("db", "s", []source.Split{{}, {State: []byte(`{"since": "7"}`)}})
	if err != nil || floor == nil || *floor != "7" {
		t.Errorf("floor %v, %v", floor, err)
	}

	lo, hi, pos := "10", "20", "15"
	for _, c := range []struct {
		pos     *string
		single  bool
		low, hi string
	}{{nil, true, "11", "20"}, {&pos, true, "16", "20"}, {&pos, false, "15", "20"}} {
		low, high, err := IntRemainder("db", &lo, &hi, c.pos, c.single)
		if err != nil || low.String() != c.low || high.String() != c.hi {
			t.Errorf("pos %v single %v: %v to %v, %v", c.pos, c.single, low, high, err)
		}
	}
	if _, err := ParseInt("db", "x"); err == nil {
		t.Error("parsed x")
	}
}
