package postgres

import (
	"strings"
	"testing"
)

// Of two patch columns that fill one held column, the first's UPDATE leaves
// it held for the second's to match, and the second's takes it out.
func TestPatchSharedHeldColumn(t *testing.T) {
	w := &writer{schema: "s"}
	s := stagedStream{Table: "t", Mode: modeMerge, Key: []string{"id"}, Fills: map[string]string{"price__v2": "price"}}
	stmts := w.patch(s, prepared{patch: []string{"id", "price__v2", "price", idColumn, heldColumn}, patches: lanesRows{rel: `"s"."p"`}})
	if len(stmts) != 2 {
		t.Fatalf("%d statements: %q", len(stmts), stmts)
	}
	unhold := `"` + heldColumn + `" = nullif(`
	for i, want := range []bool{false, true} {
		if got := strings.Contains(stmts[i], unhold); got != want {
			t.Errorf("statement %d takes the held column out: %v, want %v\n%s", i, got, want, stmts[i])
		}
	}
}
