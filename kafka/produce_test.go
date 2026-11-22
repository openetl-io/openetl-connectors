package kafka

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl-connectors/records"
)

// A row too large for a record fails its epoch with an error that names
// the topic and the limit, rather than the broker's code alone.
func TestARowTooLargeNamesItsTopicAndTheLimit(t *testing.T) {
	l := newLanes(t)
	w := l.open(1, 0)
	md := arrow.NewMetadata([]string{records.TableKey}, []string{l.table})
	sc := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "blob", Type: arrow.BinaryTypes.String}}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StringBuilder).Append(strings.Repeat("x", 1100000))
	batch := b.NewRecordBatch()
	defer batch.Release()
	e := epochOf(1, 1)
	if err := w.Write(t.Context(), e, l.table, batch); err != nil {
		t.Fatal(err)
	}
	_, err := w.PreCommit(t.Context(), e)
	if err == nil || !strings.Contains(err.Error(), l.table) || !strings.Contains(err.Error(), "max.message.bytes") {
		t.Errorf("a row of 1.1 MB pre-committed with %v", err)
	}
}
