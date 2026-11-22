package dynamodb

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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

// A scan with a filter and a projection keeps both.
func TestScanFilterAndProjection(t *testing.T) {
	db := newMemDB(map[string][]string{"t": {"id"}})
	for _, it := range []map[string]types.AttributeValue{{"id": str("1"), "c": str("b"), "v": str("x")}, {"id": str("2"), "v": str("y")}} {
		if _, err := db.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String("t"), Item: it}); err != nil {
			t.Fatal(err)
		}
	}
	var got []map[string]types.AttributeValue
	err := (&Source{}).scanPages(t.Context(), &clients{db: db}, "t", 0, 1, nil, &scanFilter{cursor: "c", floor: str("a")}, []string{"id"}, false,
		func(items []map[string]types.AttributeValue, _ map[string]types.AttributeValue) (bool, error) {
			got = append(got, items...)
			return false, nil
		})
	if err != nil || len(got) != 1 || len(got[0]) != 1 || stringOf(got[0]["id"]) != "1" {
		t.Errorf("items %v, %v; want only id 1's key", got, err)
	}
}

// Packed, discovery scans nothing of a table whose keys are strings or
// binaries, and only the key of one keyed on a number, which it types.
func TestPackedDiscovery(t *testing.T) {
	db := newMemDB(map[string][]string{"t": {"id"}})
	if _, err := db.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String("t"), Item: map[string]types.AttributeValue{"id": num("1.5"), "v": str("x")}}); err != nil {
		t.Fatal(err)
	}
	s := &Source{cfg: SourceConfig{Packed: true}}
	if _, err := s.attributes(t.Context(), &clients{db: db}, &table{name: "t", keys: []string{"id"}, keyTypes: map[string]string{"id": "S"}}); err != nil || len(db.scans) != 0 {
		t.Errorf("a table keyed on a string: %d scans, %v", len(db.scans), err)
	}
	attrs, err := s.attributes(t.Context(), &clients{db: db}, &table{name: "t", keys: []string{"id"}, keyTypes: map[string]string{"id": "N"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(db.scans) != 1 || aws.ToString(db.scans[0].ProjectionExpression) != "#k0" || db.scans[0].ExpressionAttributeNames["#k0"] != "id" {
		t.Errorf("a table keyed on a number: scans %+v", db.scans)
	}
	if got := keyType("N", *attrs["id"]); !arrow.TypeEqual(got, decimalType) {
		t.Errorf("the key's type is %v, want %v", got, decimalType)
	}
}
