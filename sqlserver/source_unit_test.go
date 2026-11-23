package sqlserver

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl-connectors/connectortest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A state at a key, or in a key range, goes on only in the order of the
// key whose columns it names, or, naming none, as a state from before they
// were kept, of the table's primary key; one that hasn't reached a key, or
// ended without one, goes on in any order.
func TestSameOrder(t *testing.T) {
	keyed := table{name: "t", cols: []sourceColumn{{name: "a"}, {name: "b"}}, key: []int{1}}
	loose := table{name: "t", cols: keyed.cols}
	lo := "5"
	for _, c := range []struct {
		name string
		t    table
		st   sourceState
		lost bool
	}{
		{"its key", keyed, sourceState{After: []string{"1"}, Key: []string{"b"}}, false},
		{"no key named", keyed, sourceState{After: []string{"1"}}, false},
		{"ended without a key", keyed, sourceState{Done: true}, false},
		{"no key reached", loose, sourceState{Since: &lo}, false},
		{"another key", keyed, sourceState{After: []string{"1"}, Key: []string{"a"}}, true},
		{"no key now", loose, sourceState{After: []string{"1"}, Key: []string{"b"}}, true},
		{"no key now, none named", loose, sourceState{After: []string{"1"}}, true},
		{"a range, no key now", loose, sourceState{Key: []string{"b"}, keyRange: keyRange{Lo: &lo}}, true},
	} {
		err := sameOrder(c.t, c.st)
		if lost := connectortest.CodeOf(err) == protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST; lost != c.lost || !lost && err != nil {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

// A read of a table with a column declared as another type than the run
// declared it fails, retryably, before it reads: its values would be read as
// the declared type. A change to a type declared the same, as nvarchar(20)
// to nvarchar(40), reads on, and so does an extension type declared the
// same.
func TestRetyped(t *testing.T) {
	declared := func(cols ...sourceColumn) *arrow.Schema {
		b, err := newBatcher(memory.DefaultAllocator, cols, false)
		if err != nil {
			t.Fatal(err)
		}
		b.release()
		return b.declared
	}
	id := sourceColumn{name: "id", typ: "int", notNull: true}
	ref := sourceColumn{name: "ref", typ: "uniqueidentifier"}
	was := declared(id, ref, sourceColumn{name: "v", typ: "nvarchar", maxLength: 40})
	bin := table{name: "items", key: []int{0}, cols: []sourceColumn{id, ref, {name: "v", typ: "varbinary", maxLength: 20}}}
	if err := retyped(bin, was); connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT || !strings.Contains(err.Error(), `"v"`) {
		t.Errorf("v changed to varbinary: %v", err)
	}
	cs := source.ConfiguredStream{Name: "items", SyncMode: source.FullRefresh, Columns: []string{"id", "ref", "v"}, Schema: was}
	if err := (&Source{}).readSplit(t.Context(), []table{bin}, cs, source.Split{}, nil); connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Errorf("a full read with v changed: %v", err)
	}
	wider := table{name: "items", key: []int{0}, cols: []sourceColumn{id, ref, {name: "v", typ: "nvarchar", maxLength: 80}}}
	if err := retyped(wider, was); err != nil {
		t.Errorf("v widened to nvarchar(40): %v", err)
	}
	if err := retyped(bin, nil); err != nil {
		t.Errorf("a request that doesn't say: %v", err)
	}
}
