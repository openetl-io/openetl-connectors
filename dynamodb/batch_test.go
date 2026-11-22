package dynamodb

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Of the run's columns, a batcher leaves out only the control columns a cdc
// batch adds itself, not an attribute whose name starts as theirs do.
func TestBatcherControlNames(t *testing.T) {
	cols := []string{"id", "openetl.note", "openetl.op", "openetl.seq"}
	for _, changes := range []bool{false, true} {
		b := (&Source{}).newBatcher(source.ConfiguredStream{Name: "t", Columns: cols}, &table{name: "t", keys: []string{"id"}}, changes)
		it := map[string]types.AttributeValue{"id": num("1"), "openetl.note": str("x")}
		if err := b.add(it, source.OpInsert, decimal128.Num{}, true); err != nil {
			t.Fatalf("changes %v: an attribute named openetl.note: %v", changes, err)
		}
		if _, ok := b.Index["openetl.op"]; ok && changes {
			t.Errorf("the batch has openetl.op as an attribute too: %v", b.Names)
		}
		rec := &connectortest.Recorder{}
		if err := b.Flush(rec.Split("t", source.WholeStream)); err != nil {
			t.Fatal(err)
		}
		b.Release()
		if rows := rec.RowsOf("t"); len(rows) != 1 || rows[0]["openetl.note"] != "x" {
			t.Errorf("changes %v: rows = %v", changes, rows)
		}
	}
}
