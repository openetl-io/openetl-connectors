package iceberg

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// A value the table's type can't hold exactly fails the write, rather than
// being cut short.
func TestConformDoesntTruncate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		from, to arrow.DataType
		exact    int64
		inexact  int64
	}{
		{"timestamp", &arrow.TimestampType{Unit: arrow.Nanosecond}, &arrow.TimestampType{Unit: arrow.Microsecond}, 1_000_000_000, 1_000_000_001},
		{"time", arrow.FixedWidthTypes.Time64ns, arrow.FixedWidthTypes.Time64us, 1_000, 1_001},
		{"date", arrow.FixedWidthTypes.Date64, arrow.FixedWidthTypes.Date32, 86_400_000, 86_400_001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := arrow.NewSchema([]arrow.Field{{Name: "v", Type: tc.to, Nullable: true}}, nil)
			for _, v := range []int64{tc.exact, tc.inexact} {
				b := array.NewBuilder(memory.DefaultAllocator, tc.from)
				switch b := b.(type) {
				case *array.TimestampBuilder:
					b.Append(arrow.Timestamp(v))
				case *array.Time64Builder:
					b.Append(arrow.Time64(v))
				case *array.Date64Builder:
					b.Append(arrow.Date64(v))
				}
				col := b.NewArray()
				b.Release()
				batch := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "v", Type: tc.from, Nullable: true}}, nil), []arrow.Array{col}, 1)
				col.Release()
				out, err := conform(t.Context(), batch, sc)
				batch.Release()
				if out != nil {
					out.Release()
				}
				if want := v == tc.exact; (err == nil) != want {
					t.Errorf("%d: %v; want an error: %v", v, err, !want)
				}
			}
		})
	}
}
