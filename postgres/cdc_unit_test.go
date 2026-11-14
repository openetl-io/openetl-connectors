package postgres

import (
	"errors"
	"slices"
	"testing"

	"github.com/jackc/pglogrepl"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func TestDeclaredOnly(t *testing.T) {
	users := table{name: "users", cols: []sourceColumn{{name: "a"}, {name: "k1"}, {name: "new"}, {name: "k2"}}, key: []int{3, 1}}

	got, err := declaredOnly(users, []string{"a", "k1", "k2"})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range got.cols {
		names = append(names, c.name)
	}
	if !slices.Equal(names, []string{"a", "k1", "k2"}) || !slices.Equal(got.key, []int{2, 1}) {
		t.Errorf("columns %v, key %v", names, got.key)
	}
	if len(users.cols) != 4 || !slices.Equal(users.key, []int{3, 1}) {
		t.Errorf("changed the table it was given: %+v", users)
	}

	got, err = declaredOnly(users, nil)
	if err != nil || len(got.cols) != 4 {
		t.Errorf("a request that doesn't say: %+v, %v", got, err)
	}

	_, err = declaredOnly(users, []string{"a", "k1"})
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Errorf("a key column it didn't declare: %v", err)
	}
}

// An update's value that the log left out, as an unchanged TOAST value, is
// named as unchanged; with the whole old row, as replica identity full
// sends it, it's filled from that.
func TestAnUnsentValueIsFilledFromAWholeOldRow(t *testing.T) {
	rel := &relation{
		stream: &cdcStream{t: table{name: "users", cols: []sourceColumn{{name: "id"}, {name: "name"}, {name: "doc"}}}},
		at:     []int{0, 1, 2},
		conv:   make([]func([]byte) []byte, 3),
		names:  []string{"id", "name", "doc"},
	}
	col := func(kind byte, v string) *pglogrepl.TupleDataColumn {
		return &pglogrepl.TupleDataColumn{DataType: kind, Data: []byte(v)}
	}
	update := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		col(pglogrepl.TupleDataTypeText, "3"), col(pglogrepl.TupleDataTypeText, "renamed"), col(pglogrepl.TupleDataTypeToast, ""),
	}}
	vals, unchanged := rel.values(update, nil)
	if vals[2] != nil || !slices.Equal(unchanged, []string{"doc"}) {
		t.Errorf("without the old row: doc %q, unchanged %v", vals[2], unchanged)
	}
	old := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		col(pglogrepl.TupleDataTypeText, "3"), col(pglogrepl.TupleDataTypeText, "before"), col(pglogrepl.TupleDataTypeText, "big"),
	}}
	vals, unchanged = rel.values(update, old)
	if string(vals[1]) != "renamed" || string(vals[2]) != "big" || len(unchanged) != 0 {
		t.Errorf("with the whole old row: %q, unchanged %v", vals, unchanged)
	}
}
