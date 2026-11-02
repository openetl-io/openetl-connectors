package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

const (
	extensionNameKey = "ARROW:extension:name"
	uuidExtension    = "arrow.uuid"
	// The engine names each stream's table in its schema's metadata.
	tableKey = "openetl.table"
)

// icebergSchema is the Iceberg schema a stream's Arrow schema needs, with
// fresh field IDs. Every column is optional, so later ones can be added.
func icebergSchema(sc *arrow.Schema) (*ice.Schema, error) {
	fields := make([]arrow.Field, sc.NumFields())
	for i, f := range sc.Fields() {
		dt, err := icebergArrowType(f)
		if err != nil {
			return nil, fmt.Errorf("iceberg: column %q: %w", f.Name, err)
		}
		fields[i] = arrow.Field{Name: f.Name, Type: dt, Nullable: true}
	}
	return table.ArrowSchemaToIcebergWithFreshIDs(arrow.NewSchema(fields, nil), false)
}

// icebergArrowType is the Arrow type, of those iceberg-go converts, that holds
// f's values.
func icebergArrowType(f arrow.Field) (arrow.DataType, error) {
	dt := f.Type
	ext := ""
	if et, ok := dt.(arrow.ExtensionType); ok {
		ext, dt = et.ExtensionName(), et.StorageType()
	} else if v, ok := f.Metadata.GetValue(extensionNameKey); ok {
		ext = v
	}
	if ext == uuidExtension {
		if t, ok := dt.(*arrow.FixedSizeBinaryType); ok && t.ByteWidth == 16 {
			return extensions.NewUUIDType(), nil
		}
	}
	switch t := dt.(type) {
	case *arrow.NullType, *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return arrow.BinaryTypes.String, nil
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType:
		return arrow.BinaryTypes.Binary, nil
	case *arrow.BooleanType, *arrow.Int32Type, *arrow.Int64Type, *arrow.Float32Type, *arrow.Float64Type,
		*arrow.Date32Type, *arrow.FixedSizeBinaryType:
		return dt, nil
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Uint8Type, *arrow.Uint16Type:
		return arrow.PrimitiveTypes.Int32, nil
	case *arrow.Uint32Type:
		return arrow.PrimitiveTypes.Int64, nil
	case *arrow.Float16Type:
		return arrow.PrimitiveTypes.Float32, nil
	case *arrow.Decimal128Type:
		return dt, nil
	case *arrow.Date64Type:
		return arrow.FixedWidthTypes.Date32, nil
	case *arrow.Time32Type, *arrow.Time64Type:
		return arrow.FixedWidthTypes.Time64us, nil
	case *arrow.TimestampType:
		if t.TimeZone == "" {
			return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
		}
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case arrow.ListLikeType:
		elem, err := icebergArrowType(t.ElemField())
		if err != nil {
			return nil, err
		}
		return arrow.ListOf(elem), nil
	case *arrow.StructType:
		fields := make([]arrow.Field, t.NumFields())
		for i, c := range t.Fields() {
			ct, err := icebergArrowType(c)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", c.Name, err)
			}
			fields[i] = arrow.Field{Name: c.Name, Type: ct, Nullable: true}
		}
		return arrow.StructOf(fields...), nil
	}
	return nil, fmt.Errorf("no Iceberg type holds %s", f.Type)
}

// conform reshapes batch into the table's Arrow schema: its columns in the
// table's order, cast to the table's types, with nulls for columns the
// batch doesn't have. The batch's columns must all be in the table.
func conform(ctx context.Context, batch arrow.RecordBatch, sc *arrow.Schema) (arrow.RecordBatch, error) {
	n := batch.NumRows()
	for _, f := range batch.Schema().Fields() {
		if !sc.HasField(f.Name) {
			return nil, fmt.Errorf("iceberg: the table has no column %q", f.Name)
		}
	}
	cols := make([]arrow.Array, sc.NumFields())
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()
	for i, f := range sc.Fields() {
		idx := batch.Schema().FieldIndices(f.Name)
		if len(idx) == 0 {
			cols[i] = array.MakeArrayOfNull(memory.DefaultAllocator, f.Type, int(n))
			continue
		}
		c, err := castTo(ctx, batch.Column(idx[0]), f.Type)
		if err != nil {
			return nil, fmt.Errorf("iceberg: column %q: %w", f.Name, err)
		}
		cols[i] = c
	}
	return array.NewRecordBatch(sc, cols, n), nil
}

// castTo returns arr as dt, retained.
func castTo(ctx context.Context, arr arrow.Array, dt arrow.DataType) (arrow.Array, error) {
	if arrow.TypeEqual(arr.DataType(), dt) {
		arr.Retain()
		return arr, nil
	}
	if et, ok := dt.(arrow.ExtensionType); ok {
		storage, err := castTo(ctx, storageOf(arr), et.StorageType())
		if err != nil {
			return nil, err
		}
		defer storage.Release()
		return array.NewExtensionArrayWithStorage(et, storage), nil
	}
	if _, ok := arr.DataType().(*arrow.NullType); ok {
		return array.MakeArrayOfNull(memory.DefaultAllocator, dt, arr.Len()), nil
	}
	return compute.CastArray(ctx, storageOf(arr), compute.SafeCastOptions(dt))
}

func storageOf(arr arrow.Array) arrow.Array {
	if e, ok := arr.(array.ExtensionArray); ok {
		return e.Storage()
	}
	return arr
}

// targetTable is the stream's table, as the engine named it.
func targetTable(sc *arrow.Schema, stream string) string {
	if t, ok := sc.Metadata().GetValue(tableKey); ok {
		return t
	}
	return stream
}
