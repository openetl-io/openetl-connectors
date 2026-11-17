//go:build integration

package dynamodb

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/openetl-io/openetl-connectors/sourcetest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// The names of a cdc row's op and seq.
var opCol, seqCol = source.OpField().Name, source.SeqField(seqType).Name

// latest is each key's row with the highest seq, as a merge applies them.
func latest(t *testing.T, rows []map[string]string) map[string]map[string]string {
	t.Helper()
	out := map[string]map[string]string{}
	seqs := map[string]*big.Int{}
	for _, r := range rows {
		seq, ok := new(big.Int).SetString(r[seqCol], 10)
		if !ok {
			t.Fatalf("a row's seq %q", r[seqCol])
		}
		if cur := seqs[r["id"]]; cur == nil || seq.Cmp(cur) > 0 {
			seqs[r["id"]], out[r["id"]] = seq, r
		}
	}
	return out
}

func readChanges(t *testing.T, s *Source, cs *source.ConfiguredStream) ([]map[string]string, error) {
	t.Helper()
	rec, err := read(t, s, *cs)
	if err != nil {
		return nil, err
	}
	st, complete := rec.Last(cs.Name)
	if !complete {
		t.Fatal("a bounded cdc read didn't complete")
	}
	cs.State = st
	return rec.RowsOf(cs.Name), nil
}

// A cdc read snapshots the table and then reads its stream: inserts,
// updates and deletes, each key's last change with the highest seq, and a
// read from its state reads only what changed since.
func TestCDC(t *testing.T) {
	cl := testClient(t)
	name := prefix(t) + "cdc"
	createTable(t, cl, name, types.StreamViewTypeNewAndOldImages, key{"id", types.ScalarAttributeTypeN})
	for i := 1; i <= 3; i++ {
		put(t, cl, name, map[string]types.AttributeValue{"id": avI(i), "v": avS(fmt.Sprint("a", i))})
	}
	s := testSource(t, fmt.Sprintf(`, "tables": [%q], "segments": 2`, name))
	cs := &source.ConfiguredStream{Name: name, SyncMode: source.CDC, Columns: []string{"id", "v"}}
	rows, err := readChanges(t, s, cs)
	if err != nil {
		t.Fatal(err)
	}
	got := latest(t, rows)
	for i := 1; i <= 3; i++ {
		if r := got[fmt.Sprint(i)]; r["v"] != fmt.Sprint("a", i) || r[opCol] != source.OpInsert {
			t.Errorf("snapshot's item %d: %v", i, r)
		}
	}
	put(t, cl, name, map[string]types.AttributeValue{"id": avI(4), "v": avS("new")})
	put(t, cl, name, map[string]types.AttributeValue{"id": avI(2), "v": avS("changed")})
	if _, err := cl.DeleteItem(t.Context(), &dynamodb.DeleteItemInput{TableName: aws.String(name), Key: map[string]types.AttributeValue{"id": avI(3)}}); err != nil {
		t.Fatal(err)
	}
	more, err := readChanges(t, s, cs)
	if err != nil {
		t.Fatal(err)
	}
	got = latest(t, append(rows, more...))
	for id, want := range map[string][2]string{"1": {"a1", source.OpInsert}, "2": {"changed", source.OpUpdate}, "3": {"a3", source.OpDelete}, "4": {"new", source.OpInsert}} {
		if r := got[id]; r["v"] != want[0] || r[opCol] != want[1] {
			t.Errorf("item %s: %v, want v %s by %s", id, r, want[0], want[1])
		}
	}
	// Restarted from its state, with nothing new, the read reads nothing.
	again, err := readChanges(t, s, cs)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Errorf("a read from the state read %v", again)
	}
}

// A change with an attribute the run doesn't declare fails the read,
// retryably, while the item has it, so that discovery declares it; once
// the item doesn't, the change is read without it.
func TestCDCNewAttribute(t *testing.T) {
	cl := testClient(t)
	name := prefix(t) + "cdcnew"
	createTable(t, cl, name, types.StreamViewTypeNewAndOldImages, key{"id", types.ScalarAttributeTypeN})
	s := testSource(t, fmt.Sprintf(`, "tables": [%q]`, name))
	cs := &source.ConfiguredStream{Name: name, SyncMode: source.CDC, Columns: []string{"id", "v"}}
	if _, err := readChanges(t, s, cs); err != nil {
		t.Fatal(err)
	}
	put(t, cl, name, map[string]types.AttributeValue{"id": avI(1), "v": avS("a"), "w": avS("new")})
	state := cs.State
	if _, err := readChanges(t, s, cs); sourcetest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT {
		t.Fatalf("a new attribute: %v (%v), want TRANSIENT", err, sourcetest.CodeOf(err))
	}
	if cs := discovered(t, s, name); index(cs.Schema, "w") < 0 {
		t.Errorf("discovery doesn't declare w: %v", cs.Schema)
	}
	put(t, cl, name, map[string]types.AttributeValue{"id": avI(1), "v": avS("b")})
	cs.State = state
	rows, err := readChanges(t, s, cs)
	if err != nil {
		t.Fatal(err)
	}
	if r := latest(t, rows)["1"]; r["v"] != "b" {
		t.Errorf("item 1: %v", r)
	}
}

// A stream of keys alone: an insert's and an update's item is read from
// the table, and a delete's keys.
func TestCDCKeysOnly(t *testing.T) {
	cl := testClient(t)
	name := prefix(t) + "keys"
	createTable(t, cl, name, types.StreamViewTypeKeysOnly, key{"id", types.ScalarAttributeTypeS})
	s := testSource(t, fmt.Sprintf(`, "tables": [%q]`, name))
	cs := &source.ConfiguredStream{Name: name, SyncMode: source.CDC}
	if _, err := readChanges(t, s, cs); err != nil {
		t.Fatal(err)
	}
	put(t, cl, name, map[string]types.AttributeValue{"id": avS("a"), "v": avI(1)}, map[string]types.AttributeValue{"id": avS("a"), "v": avI(2)})
	put(t, cl, name, map[string]types.AttributeValue{"id": avS("b"), "v": avI(1)})
	if _, err := cl.DeleteItem(t.Context(), &dynamodb.DeleteItemInput{TableName: aws.String(name), Key: map[string]types.AttributeValue{"id": avS("b")}}); err != nil {
		t.Fatal(err)
	}
	rows, err := readChanges(t, s, cs)
	if err != nil {
		t.Fatal(err)
	}
	got := latest(t, rows)
	if r := got["a"]; r["v"] != "2" || r[opCol] != source.OpUpdate {
		t.Errorf("item a: %v", r)
	}
	if r := got["b"]; r[opCol] != source.OpDelete {
		t.Errorf("item b: %v", r)
	}
	// b's insert is skipped: the item is gone when the read gets to it.
	for _, r := range rows {
		if r["id"] == "b" && r[opCol] != source.OpDelete {
			t.Errorf("b's insert read after it was deleted: %v", r)
		}
	}
}

// A stream turned off and on is another stream: a read from the old one's
// state fails, its position lost.
func TestCDCStreamReplaced(t *testing.T) {
	cl := testClient(t)
	name := prefix(t) + "replaced"
	createTable(t, cl, name, types.StreamViewTypeNewAndOldImages, key{"id", types.ScalarAttributeTypeN})
	s := testSource(t, fmt.Sprintf(`, "tables": [%q]`, name))
	cs := &source.ConfiguredStream{Name: name, SyncMode: source.CDC}
	if _, err := readChanges(t, s, cs); err != nil {
		t.Fatal(err)
	}
	for _, on := range []bool{false, true} {
		spec := &types.StreamSpecification{StreamEnabled: aws.Bool(on)}
		if on {
			spec.StreamViewType = types.StreamViewTypeNewAndOldImages
		}
		if _, err := cl.UpdateTable(t.Context(), &dynamodb.UpdateTableInput{TableName: aws.String(name), StreamSpecification: spec}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := readChanges(t, s, cs); sourcetest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST {
		t.Errorf("a read of a replaced stream: %v (%v), want POSITION_LOST", err, sourcetest.CodeOf(err))
	}
}
