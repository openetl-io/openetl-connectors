package docstore

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ListStrings is row i of a list of strings, nil if it's null.
func ListStrings(a arrow.Array, i int) []string {
	if e, ok := a.(array.ExtensionArray); ok {
		a = e.Storage()
	}
	l := a.(*array.List)
	if l.IsNull(i) {
		return nil
	}
	from, to := l.ValueOffsets(i)
	vals := l.ListValues().(*array.String)
	out := make([]string, 0, to-from)
	for j := from; j < to; j++ {
		out = append(out, vals.Value(int(j)))
	}
	return out
}
