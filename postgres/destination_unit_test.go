package postgres

import (
	"strings"
	"testing"
)

// Staging is unlogged only when asked, and autovacuum never takes it.
func TestStagingDDL(t *testing.T) {
	cols := []column{{name: "id", typ: "bigint"}}
	for _, unlogged := range []bool{true, false} {
		ddl := stagingDDL(`"s"."t"`, cols, unlogged)
		if got := strings.HasPrefix(ddl, "CREATE UNLOGGED TABLE "); got != unlogged {
			t.Errorf("unlogged %v: %s", unlogged, ddl)
		}
		if !strings.HasSuffix(ddl, " WITH (autovacuum_enabled = false)") {
			t.Errorf("autovacuum isn't off: %s", ddl)
		}
	}
	if ddl := stagingDDL(`"s"."t"`, nil, true); !strings.Contains(ddl, `"s"."t" () WITH`) {
		t.Errorf("a staging of batches without columns: %s", ddl)
	}
}

// Each generation of a pipeline has an alive table of its own, which isn't
// counted among its staging, and whose name Postgres keeps whole.
func TestAliveName(t *testing.T) {
	a, b, c := aliveName("p1", 1), aliveName("p1", 2), aliveName("p2", 1)
	if a == b || a == c {
		t.Errorf("names repeat: %s %s %s", a, b, c)
	}
	if strings.HasPrefix(a, "_openetl_stage_") || len(a) > 63 {
		t.Errorf("name %s", a)
	}
}

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
