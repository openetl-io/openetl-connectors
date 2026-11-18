package keysplit

import (
	"errors"
	"math/big"
	"slices"
	"testing"
)

func TestCuts(t *testing.T) {
	if got := CutInts(big.NewInt(0), big.NewInt(100), 4); !slices.Equal(got, []string{"25", "50", "75"}) {
		t.Errorf("CutInts(0, 100, 4) = %v", got)
	}
	if got := CutInts(big.NewInt(5), big.NewInt(6), 4); got != nil {
		t.Errorf("CutInts(5, 6, 4) = %v, want none", got)
	}
	if got := CutBounds([]string{"a", "b", "b", "c", "d", "e"}, 400, 100); !slices.Equal(got, []string{"b", "c", "d"}) {
		t.Errorf("CutBounds = %v", got)
	}
	type r struct{ lo, hi string }
	got := Between([]string{"1", "2"}, func(lo, hi *string) r {
		x := r{"-", "-"}
		if lo != nil {
			x.lo = *lo
		}
		if hi != nil {
			x.hi = *hi
		}
		return x
	})
	if !slices.Equal(got, []r{{"-", "1"}, {"1", "2"}, {"2", "-"}}) {
		t.Errorf("Between = %v", got)
	}
	if Between(nil, func(lo, hi *string) r { return r{} }) != nil {
		t.Error("Between without cuts has ranges")
	}
}

func TestIntMid(t *testing.T) {
	var r Reading
	if mid, _ := r.IntMid(big.NewInt(0), big.NewInt(10), nil); mid != nil || r.Over.Load() {
		t.Fatalf("unasked: %v, over %v", mid, r.Over.Load())
	}
	r.Asked.Store(true)
	extent := func() (*big.Int, *big.Int, error) { return big.NewInt(0), big.NewInt(9), nil }
	if mid, err := r.IntMid(nil, nil, extent); err != nil || mid == nil || *mid != "4" {
		t.Fatalf("asked: %v, %v", mid, err)
	}
	r.Asked.Store(true)
	if mid, _ := r.IntMid(big.NewInt(3), big.NewInt(3), extent); mid != nil || !r.Over.Load() {
		t.Fatalf("nothing left: %v, over %v", mid, r.Over.Load())
	}

	var failed Reading
	failed.Asked.Store(true)
	if _, err := failed.IntMid(nil, nil, func() (*big.Int, *big.Int, error) { return nil, nil, errors.New("lookup failed") }); err == nil || failed.Over.Load() {
		t.Fatalf("a failed lookup: %v, over %v", err, failed.Over.Load())
	}
	var empty Reading
	empty.Asked.Store(true)
	if mid, err := empty.IntMid(nil, nil, func() (*big.Int, *big.Int, error) { return nil, nil, nil }); mid != nil || err != nil || !empty.Over.Load() {
		t.Fatalf("an empty table: %v, %v, over %v", mid, err, empty.Over.Load())
	}
}
