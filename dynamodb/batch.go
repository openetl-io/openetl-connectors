package dynamodb

import (
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/openetl-io/openetl-connectors/docstore"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// batcher builds a stream's batches: its attributes as text, and for cdc
// its op and seq. With the run's columns, every batch has each of them,
// and an item with another attribute fails the read, retryably, so the
// next attempt's discovery declares it; without, a batch has a column for
// each attribute its items have. Packed, a batch is the key attributes and
// data. Callers flush it after each page, which DynamoDB keeps to 1 MB.
type batcher struct {
	docstore.Batch
	s        *Source
	stream   string
	keys     []string
	declared bool
	changes  bool
	packed   bool
	// For an attribute the run doesn't declare, in a cdc read: whether the
	// item no longer has it, so the row leaves it out. Nil: it has.
	gone func(it map[string]types.AttributeValue, attr string) (bool, error)
}

// newBatcher makes a batcher of the run's columns where cs has them, keys
// first.
func (s *Source) newBatcher(cs source.ConfiguredStream, t *table, changes bool) *batcher {
	b := &batcher{s: s, stream: cs.Name, keys: t.keys, changes: changes, declared: cs.Columns != nil, packed: s.cfg.Packed}
	b.Batch = docstore.NewBatch(changes)
	for _, k := range t.keys {
		b.Column(k)
	}
	if b.packed {
		b.Column(dataColumn)
	}
	for _, c := range cs.Columns {
		if !b.packed && (!changes || !slices.Contains(controlNames, c)) {
			b.Column(c)
		}
	}
	return b
}

// controlNames are the names of a cdc batch's control columns, which the
// run's columns can have too.
var controlNames = []string{source.OpField().Name, source.SeqField(seqType).Name}

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
		i, ok := b.Index[k]
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
			i = b.Column(k)
		}
		at[j] = i
	}
	set := make([]bool, len(b.Cols))
	for j, k := range names {
		i := at[j]
		if i < 0 {
			continue
		}
		set[i] = true
		if t, ok := text(it[k]); ok {
			b.Cols[i].Append(t)
			b.Size += len(t)
		} else {
			b.Cols[i].AppendNull()
		}
	}
	for i, done := range set {
		if !done {
			b.Cols[i].AppendNull()
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
		b.Cols[i].Append(t)
		b.Size += len(t)
	}
	data := b.Cols[len(b.keys)]
	if whole {
		j := appendMap(nil, it, true)
		data.Append(string(j))
		b.Size += len(j)
	} else {
		data.AppendNull()
	}
	b.end(op, seq)
	return nil
}

func (b *batcher) end(op string, seq decimal128.Num) {
	if b.changes {
		b.Op.Append(op)
		b.Seq.Append(seq)
	}
	b.Rows++
}
