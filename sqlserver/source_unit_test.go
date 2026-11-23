package sqlserver

import (
	"testing"

	"github.com/openetl-io/openetl-connectors/connectortest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
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
