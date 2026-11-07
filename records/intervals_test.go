package records

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestIntervalsAsStructs(t *testing.T) {
	ivType := arrow.FixedWidthTypes.MonthDayNanoInterval
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "iv", Type: ivType, Nullable: true},
		{Name: "ivs", Type: arrow.ListOf(ivType), Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	iv := b.Field(1).(*array.MonthDayNanoIntervalBuilder)
	iv.Append(arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: 3000})
	iv.AppendNull()
	iv.Append(arrow.MonthDayNanoInterval{Days: -1})
	lb := b.Field(2).(*array.ListBuilder)
	lb.Append(true)
	lb.ValueBuilder().(*array.MonthDayNanoIntervalBuilder).Append(arrow.MonthDayNanoInterval{Months: 14})
	lb.AppendNull()
	lb.Append(true)
	rec := b.NewRecordBatch()
	defer rec.Release()

	// From the second row on, so offsets count.
	sliced := rec.NewSlice(1, 3)
	defer sliced.Release()
	for _, tc := range []struct {
		in   arrow.RecordBatch
		want []string
	}{
		{rec, []string{`{"days":2,"months":1,"nanoseconds":3000}`, `null`, `{"days":-1,"months":0,"nanoseconds":0}`}},
		{sliced, []string{`null`, `{"days":-1,"months":0,"nanoseconds":0}`}},
	} {
		out := IntervalsAsStructs(tc.in)
		if !arrow.TypeEqual(out.Schema().Field(1).Type, IntervalType) || HasIntervals(out.Schema()) {
			t.Fatalf("schema %s", out.Schema())
		}
		for i, w := range tc.want {
			if got := out.Column(1).ValueStr(i); got != w && !(w == "null" && out.Column(1).IsNull(i)) {
				t.Errorf("row %d: %s, want %s", i, got, w)
			}
		}
		out.Release()
	}
	out := IntervalsAsStructs(rec)
	defer out.Release()
	if got := out.Column(2).ValueStr(0); got != `[{"days":0,"months":14,"nanoseconds":0}]` {
		t.Errorf("a list's interval: %s", got)
	}
}
