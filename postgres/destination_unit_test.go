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

// An overwrite swaps when it's exact unless the config says not to, and
// deletes rather than truncates unless it says to.
func TestOverwriteConfig(t *testing.T) {
	for _, tc := range []struct {
		config         string
		swap, truncate bool
	}{
		{`{"dsn": "postgres://h/db"}`, true, false},
		{`{"dsn": "postgres://h/db", "overwriteSwap": false, "overwriteTruncate": true}`, false, true},
	} {
		d, err := NewDestination([]byte(tc.config))
		if err != nil {
			t.Fatal(err)
		}
		if d.cfg.OverwriteSwap != tc.swap || d.cfg.OverwriteTruncate != tc.truncate {
			t.Errorf("%s: swap %v, truncate %v", tc.config, d.cfg.OverwriteSwap, d.cfg.OverwriteTruncate)
		}
	}
}

// A read's columns follow its target's order, then their own, and take its
// collations.
func TestReadColumnsFollowTheTarget(t *testing.T) {
	cols := []column{{name: "c", typ: "text"}, {name: "new", typ: "text"}, {name: "a", typ: "bigint"}, {name: "other", typ: "text"}}
	target := []column{{name: "a", typ: "bigint"}, {name: "b", typ: "text"}, {name: "c", typ: "text", collation: `pg_catalog."C"`}}
	var got []string
	for _, c := range inOrderOf(cols, target) {
		got = append(got, c.name)
	}
	if strings.Join(got, ",") != "a,c,new,other" {
		t.Errorf("order %v", got)
	}
	if def := columnDef(column{name: "c", typ: "text", collation: `pg_catalog."C"`}); def != `"c" text COLLATE pg_catalog."C"` {
		t.Errorf("columnDef: %s", def)
	}
}

func TestSwapHelpers(t *testing.T) {
	opts := []string{"fillfactor=90", "autovacuum_vacuum_scale_factor=0.05"}
	if got := options(opts, "toast."); got != "toast.fillfactor = '90', toast.autovacuum_vacuum_scale_factor = '0.05'" {
		t.Errorf("options: %s", got)
	}
	if got := strings.Join(optionNames(opts, ""), ","); got != "fillfactor,autovacuum_vacuum_scale_factor" {
		t.Errorf("optionNames: %s", got)
	}
	if got := differs(`{"a": 1, "b": [2], "c": "x"}`, `{"a": 1, "b": [3], "c": "y"}`); got != "b, c" {
		t.Errorf("differs: %s", got)
	}
}
