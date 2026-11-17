// Package docstore is what the MongoDB and DynamoDB connectors share: a
// source's batch of text columns, and a destination's markers and how it
// settles an epoch's staged streams.
package docstore

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// SeqType is a cdc read's openetl.seq.
var SeqType = &arrow.Decimal128Type{Precision: 38, Scale: 0}

// Batch is the rows a read adds since its last flush, every column text,
// and in a cdc read each row's operation and sequence.
type Batch struct {
	Names []string
	Index map[string]int
	Cols  []*array.StringBuilder
	// Nil but in a cdc read.
	Op   *array.StringBuilder
	Seq  *array.Decimal128Builder
	Rows int
	// The rows' text, in bytes.
	Size int
}

// NewBatch is a batch without columns, of a cdc read if changes.
func NewBatch(changes bool) Batch {
	b := Batch{Index: map[string]int{}}
	if changes {
		b.Op = array.NewStringBuilder(memory.DefaultAllocator)
		b.Seq = array.NewDecimal128Builder(memory.DefaultAllocator, SeqType)
	}
	return b
}

// Column is the index of the column name, which it adds, null in the rows
// so far, if the batch hasn't it.
func (b *Batch) Column(name string) int {
	if i, ok := b.Index[name]; ok {
		return i
	}
	c := array.NewStringBuilder(memory.DefaultAllocator)
	c.AppendNulls(b.Rows)
	b.Index[name] = len(b.Names)
	b.Names = append(b.Names, name)
	b.Cols = append(b.Cols, c)
	return len(b.Names) - 1
}

// Flush emits the rows added since the last flush, if any.
func (b *Batch) Flush(out source.SplitEmitter) error {
	if b.Rows == 0 {
		return nil
	}
	fields := make([]arrow.Field, 0, len(b.Cols)+2)
	arrs := make([]arrow.Array, 0, len(b.Cols)+2)
	for i, c := range b.Cols {
		fields = append(fields, arrow.Field{Name: b.Names[i], Type: arrow.BinaryTypes.String, Nullable: true})
		arrs = append(arrs, c.NewArray())
	}
	if b.Op != nil {
		fields = append(fields, source.OpField(), source.SeqField(SeqType))
		arrs = append(arrs, b.Op.NewArray(), b.Seq.NewArray())
	}
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), arrs, int64(b.Rows))
	for _, a := range arrs {
		a.Release()
	}
	b.Rows, b.Size = 0, 0
	if err := out.Records(rec); err != nil {
		rec.Release()
		return err
	}
	return nil
}

func (b *Batch) Release() {
	for _, c := range b.Cols {
		c.Release()
	}
	if b.Op != nil {
		b.Op.Release()
		b.Seq.Release()
	}
}
