package records

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// IntervalType is how a format with no interval of nanoseconds, such as
// Parquet or Iceberg, holds a month-day-nanosecond interval: its parts, by
// the names JSON spells them with.
var IntervalType = arrow.StructOf(
	arrow.Field{Name: "months", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	arrow.Field{Name: "days", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	arrow.Field{Name: "nanoseconds", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
)

// HasInterval reports whether dt holds a month-day-nanosecond interval, at
// any depth.
func HasInterval(dt arrow.DataType) bool {
	switch t := dt.(type) {
	case *arrow.MonthDayNanoIntervalType:
		return true
	case arrow.ListLikeType:
		return HasInterval(t.ElemField().Type)
	case *arrow.StructType:
		for _, f := range t.Fields() {
			if HasInterval(f.Type) {
				return true
			}
		}
	}
	return false
}

// WithoutIntervals is dt with each month-day-nanosecond interval in it as
// IntervalType.
func WithoutIntervals(dt arrow.DataType) arrow.DataType {
	if !HasInterval(dt) {
		return dt
	}
	switch t := dt.(type) {
	case *arrow.MonthDayNanoIntervalType:
		return IntervalType
	case *arrow.ListType:
		f := t.ElemField()
		f.Type = WithoutIntervals(f.Type)
		return arrow.ListOfField(f)
	case *arrow.LargeListType:
		f := t.ElemField()
		f.Type = WithoutIntervals(f.Type)
		return arrow.LargeListOfField(f)
	case *arrow.StructType:
		fields := t.Fields()
		for i := range fields {
			fields[i].Type = WithoutIntervals(fields[i].Type)
		}
		return arrow.StructOf(fields...)
	}
	return dt
}

// IntervalsAsStructs is batch with its intervals as IntervalType. It's
// batch, retained, when batch has none.
func IntervalsAsStructs(batch arrow.RecordBatch) arrow.RecordBatch {
	sc := batch.Schema()
	if !HasIntervals(sc) {
		batch.Retain()
		return batch
	}
	fields := sc.Fields()
	cols := make([]arrow.Array, len(fields))
	for i := range fields {
		fields[i].Type = WithoutIntervals(fields[i].Type)
		cols[i] = IntervalArrayAsStruct(batch.Column(i))
		defer cols[i].Release()
	}
	md := sc.Metadata()
	return array.NewRecordBatch(arrow.NewSchema(fields, &md), cols, batch.NumRows())
}

// HasIntervals reports whether any of sc's fields holds an interval.
func HasIntervals(sc *arrow.Schema) bool {
	for _, f := range sc.Fields() {
		if HasInterval(f.Type) {
			return true
		}
	}
	return false
}

// IntervalArrayAsStruct is a with its intervals as IntervalType, retained.
func IntervalArrayAsStruct(a arrow.Array) arrow.Array {
	if !HasInterval(a.DataType()) {
		a.Retain()
		return a
	}
	data := a.Data()
	switch a := a.(type) {
	case *array.MonthDayNanoInterval:
		b := array.NewStructBuilder(memory.DefaultAllocator, IntervalType)
		defer b.Release()
		months, days, ns := b.FieldBuilder(0).(*array.Int32Builder), b.FieldBuilder(1).(*array.Int32Builder), b.FieldBuilder(2).(*array.Int64Builder)
		for i := range a.Len() {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			v := a.Value(i)
			b.Append(true)
			months.Append(v.Months)
			days.Append(v.Days)
			ns.Append(v.Nanoseconds)
		}
		return b.NewArray()
	case array.ListLike:
		values := IntervalArrayAsStruct(a.ListValues())
		defer values.Release()
		out := array.NewData(WithoutIntervals(a.DataType()), data.Len(), data.Buffers(), []arrow.ArrayData{values.Data()}, data.NullN(), data.Offset())
		defer out.Release()
		return array.MakeFromData(out)
	case *array.Struct:
		children := make([]arrow.ArrayData, a.NumField())
		for i := range children {
			c := IntervalArrayAsStruct(a.Field(i))
			defer c.Release()
			children[i] = c.Data()
		}
		out := array.NewData(WithoutIntervals(a.DataType()), data.Len(), data.Buffers(), children, data.NullN(), data.Offset())
		defer out.Release()
		return array.MakeFromData(out)
	}
	a.Retain()
	return a
}
