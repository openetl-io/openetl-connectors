package mongodb

import "testing"

// Discovery reads in ranges only where the server bounds them, and else a
// collection whole, as one aggregation would read it anyway.
func TestRangesBounded(t *testing.T) {
	for _, tc := range []struct {
		v                    serverVersion
		indexed, clusteredOK bool
	}{
		{serverVersion{4, 4, 29}, false, false},
		{serverVersion{5, 0, 0}, true, false},
		{serverVersion{5, 3, 2}, true, false},
		{serverVersion{6, 0, 16}, true, false},
		{serverVersion{6, 0, 17}, true, true},
		{serverVersion{6, 0, 26}, true, true},
		{serverVersion{6, 3, 2}, true, false},
		{serverVersion{7, 0, 12}, true, false},
		{serverVersion{7, 0, 13}, true, true},
		{serverVersion{7, 2, 2}, true, false},
		{serverVersion{7, 3, 0}, true, true},
		{serverVersion{8, 0, 0}, true, true},
		{serverVersion{8, 2, 1}, true, true},
	} {
		if got := rangesBounded(tc.v, false); got != tc.indexed {
			t.Errorf("%v: a collection's ranges bounded: %v, want %v", tc.v, got, tc.indexed)
		}
		if got := rangesBounded(tc.v, true); got != tc.clusteredOK {
			t.Errorf("%v: a clustered collection's ranges bounded: %v, want %v", tc.v, got, tc.clusteredOK)
		}
	}
}
