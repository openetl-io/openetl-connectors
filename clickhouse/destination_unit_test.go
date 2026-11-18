package clickhouse

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// A list column's values are in its child, which counts towards a flush as
// its offsets do; a batch past flushBytes counts as flushBytes.
func TestBatchBytes(t *testing.T) {
	list := func(values int) arrow.RecordBatch {
		b := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
		defer b.Release()
		b.Append(true)
		vb := b.ValueBuilder().(*array.Int64Builder)
		for i := range values {
			vb.Append(int64(i))
		}
		arr := b.NewArray()
		defer arr.Release()
		return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "l", Type: arr.DataType()}}, nil), []arrow.Array{arr}, 1)
	}
	small := list(1 << 20)
	defer small.Release()
	if got := batchBytes(small); got < 8<<20 || got >= flushBytes {
		t.Errorf("a row of a list of 2^20 int64s counts %d bytes", got)
	}
	big := list(5 << 20)
	defer big.Release()
	if got := batchBytes(big); got != flushBytes {
		t.Errorf("a row of a list of 5 * 2^20 int64s counts %d bytes, not %d", got, flushBytes)
	}
}
