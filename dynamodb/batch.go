package dynamodb

import (
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// batcher builds a stream's batches: its attributes as text, and for cdc
// its op and seq. With the run's columns, every batch has each of them,
// and an item with another attribute fails the read, retryably, so the
// next attempt's discovery declares it; without, a batch has a column for
// each attribute its items have. Packed, a batch is the key attributes and
// data. Callers flush it after each page, which DynamoDB keeps to 1 MB.
type batcher struct {
	s        *Source
	stream   string
	keys     []string
	declared bool
	changes  bool
	packed   bool
	names    []string
	index    map[string]int
	cols     []*array.StringBuilder
	op       *array.StringBuilder
	seq      *array.Decimal128Builder
	rows     int
	size     int
	// For an attribute the run doesn't declare, in a cdc read: whether the
	// item no longer has it, so the row leaves it out. Nil: it has.
	gone func(it map[string]types.AttributeValue, attr string) (bool, error)
}

// newBatcher makes a batcher of the run's columns where cs has them, keys
// first.
func (s *Source) newBatcher(cs source.ConfiguredStream, t *table, changes bool) *batcher {
	b := &batcher{s: s, stream: cs.Name, keys: t.keys, changes: changes, index: map[string]int{}, declared: cs.Columns != nil, packed: s.cfg.Packed}
	for _, k := range t.keys {
		b.column(k)
	}
	if b.packed {
		b.column(dataColumn)
	}
	for _, c := range columnsOf(cs) {
		if !b.packed {
			b.column(c)
		}
	}
	if changes {
		b.op = array.NewStringBuilder(memory.DefaultAllocator)
		b.seq = array.NewDecimal128Builder(memory.DefaultAllocator, seqType)
	}
	return b
}

func (b *batcher) column(name string) int {
	if i, ok := b.index[name]; ok {
		return i
	}
	c := array.NewStringBuilder(memory.DefaultAllocator)
	c.AppendNulls(b.rows)
	b.index[name] = len(b.names)
	b.names = append(b.names, name)
	b.cols = append(b.cols, c)
	return len(b.names) - 1
}

// add appends an item's row, with op and seq for cdc. A delete's row has
// only the attributes the run declares, whose values only say what it
// deleted; packed, its data is the item it deleted, or null where whole is
// false.
func (b *batcher) add(it map[string]types.AttributeValue, op string, seq decimal128.Num, whole bool) error {
	if b.packed {
		return b.addPacked(it, op, seq, whole)
	}
	names := slices.Sorted(func(yield func(string) bool) {
		for k := range it {
			if !yield(k) {
				return
			}
		}
	})
	// Each attribute's column, or -1 to leave it out, settled before any is
	// added, so that a failure leaves the batch as it was.
	at := make([]int, len(names))
	for j, k := range names {
		i, ok := b.index[k]
		switch {
		case ok:
		case op == source.OpDelete:
			i = -1
		case b.declared:
			gone := false
			if b.gone != nil {
				var err error
				if gone, err = b.gone(it, k); err != nil {
					return err
				}
			}
			if !gone {
				return b.s.newFieldError(b.stream, k)
			}
			i = -1
		default:
			i = b.column(k)
		}
		at[j] = i
	}
	set := make([]bool, len(b.cols))
	for j, k := range names {
		i := at[j]
		if i < 0 {
			continue
		}
		set[i] = true
		if t, ok := text(it[k]); ok {
			b.cols[i].Append(t)
			b.size += len(t)
		} else {
			b.cols[i].AppendNull()
		}
	}
	for i, done := range set {
		if !done {
			b.cols[i].AppendNull()
		}
	}
	b.end(op, seq)
	return nil
}

func (b *batcher) addPacked(it map[string]types.AttributeValue, op string, seq decimal128.Num, whole bool) error {
	for i, k := range b.keys {
		v, ok := it[k]
		t, isText := text(v)
		if !ok || !isText {
			return fmt.Errorf("dynamodb: an item of %s has no key attribute %q", b.stream, k)
		}
		b.cols[i].Append(t)
		b.size += len(t)
	}
	data := b.cols[len(b.keys)]
	if whole {
		j := appendMap(nil, it, true)
		data.Append(string(j))
		b.size += len(j)
	} else {
		data.AppendNull()
	}
	b.end(op, seq)
	return nil
}

func (b *batcher) end(op string, seq decimal128.Num) {
	if b.changes {
		b.op.Append(op)
		b.seq.Append(seq)
	}
	b.rows++
}

// flush emits the rows added since the last flush, if any.
func (b *batcher) flush(out source.SplitEmitter) error {
	if b.rows == 0 {
		return nil
	}
	fields := make([]arrow.Field, 0, len(b.cols)+2)
	arrs := make([]arrow.Array, 0, len(b.cols)+2)
	for i, c := range b.cols {
		fields = append(fields, arrow.Field{Name: b.names[i], Type: arrow.BinaryTypes.String, Nullable: true})
		arrs = append(arrs, c.NewArray())
	}
	if b.changes {
		fields = append(fields, source.OpField(), source.SeqField(seqType))
		arrs = append(arrs, b.op.NewArray(), b.seq.NewArray())
	}
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), arrs, int64(b.rows))
	for _, a := range arrs {
		a.Release()
	}
	b.rows, b.size = 0, 0
	if err := out.Records(rec); err != nil {
		rec.Release()
		return err
	}
	return nil
}

func (b *batcher) release() {
	for _, c := range b.cols {
		c.Release()
	}
	if b.changes {
		b.op.Release()
		b.seq.Release()
	}
}
