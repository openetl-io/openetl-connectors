package kafka

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/destination"
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
	if err == nil || !strings.Contains(err.Error(), l.table) || !strings.Contains(err.Error(), "max.message.bytes") || codeOf(err) != "ERROR_CODE_DATA_INVALID" {
		t.Errorf("a row of 1.1 MB pre-committed with %s %v", codeOf(err), err)
	}
}

// A transaction open past its timeout, which the brokers abort, fails its
// epoch as transient, at the commit or at the pre-commit: no newer writer
// owns the lane, as a fence would say.
func TestATransactionTheBrokersTimedOutIsNoFence(t *testing.T) {
	defer func(s time.Duration) { txnSlack = s }(txnSlack)
	txnSlack = 0
	l := newLanes(t)
	w := l.open(1, 0)
	e1, e2 := epochOf(1, 1), epochOf(1, 2)
	st := l.stage(w, e1, 0, 3)
	b := idRows(l.table, 3, 6)
	defer b.Release()
	if err := w.Write(t.Context(), e2, l.table, b); err != nil {
		t.Fatal(err)
	}
	time.Sleep(timeout(time.Second) + time.Second)
	_, err := w.Commit(t.Context(), e1, []destination.Staged{st})
	if errors.Is(err, destination.ErrFenced) || codeOf(err) != "ERROR_CODE_TRANSIENT" {
		t.Errorf("commit: %s %v", codeOf(err), err)
	}
	_, err = w.PreCommit(t.Context(), e2)
	if errors.Is(err, destination.ErrFenced) || codeOf(err) != "ERROR_CODE_TRANSIENT" {
		t.Errorf("pre-commit: %s %v", codeOf(err), err)
	}
	l.want("after the brokers aborted both", 0, 0)
}
