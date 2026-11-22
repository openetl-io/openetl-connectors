package dynamodb

import (
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// An append deduped within each epoch drops a repeat of an _openetl_id in
// the epoch, and a later epoch's row of that _openetl_id lands beside it;
// deduped against the target, both are skipped; not deduped, every row
// lands.
func TestAppendDedupe(t *testing.T) {
	for _, c := range []struct {
		dedupe    destination.Dedupe
		items     int
		collapsed uint64
	}{{destination.DedupeEpoch, 2, 1}, {destination.DedupeTarget, 1, 2}, {destination.DedupeNone, 3, 0}} {
		db := newMemDB(map[string][]string{controlTable: {"p", "n"}, "t": {idColumn, rowColumn}})
		w := memWriter(t, db, destination.Stream{Name: "t", Mode: destination.Append, Dedupe: c.dedupe})
		cols, row := []string{"v", idColumn}, []*string{sp("a"), sp("id-1")}
		got := memCommit(t, w, 1, "t", memBatch(cols, row, row)).Collapsed["t"]
		got += memCommit(t, w, 2, "t", memBatch(cols, row)).Collapsed["t"]
		if n := len(db.items("t")); n != c.items || got != c.collapsed {
			t.Errorf("dedupe %d: %d items, %d collapsed; want %d and %d", c.dedupe, n, got, c.items, c.collapsed)
		}
	}
}

// An overwrite without a key, deduped within each epoch, drops a repeat of
// an _openetl_id in the epoch, and keeps a later epoch's row of it.
func TestOverwriteDedupeEpoch(t *testing.T) {
	db := newMemDB(map[string][]string{controlTable: {"p", "n"}, "t": {idColumn, rowColumn}})
	w := memWriter(t, db, destination.Stream{Name: "t", Mode: destination.Overwrite, Dedupe: destination.DedupeEpoch})
	if err := w.StartReads(t.Context(), map[string]time.Time{"t": time.Now()}); err != nil {
		t.Fatal(err)
	}
	cols, row := []string{"v", idColumn}, []*string{sp("a"), sp("id-1")}
	got := memCommit(t, w, 1, "t", memBatch(cols, row, row)).Collapsed["t"]
	if err := w.EndRead(t.Context(), destination.Epoch{Generation: 1, N: 2}, "t"); err != nil {
		t.Fatal(err)
	}
	got += memCommit(t, w, 2, "t", memBatch(cols, row)).Collapsed["t"]
	if n := len(db.items("t")); n != 2 || got != 1 {
		t.Errorf("%d items, %d collapsed; want 2 and 1", n, got)
	}
}

// Every refusal of a type change says what makes the table take it, a local
// secondary index's too.
func TestRefusalsHaveFixes(t *testing.T) {
	keyed := func(names ...string) []types.KeySchemaElement {
		var out []types.KeySchemaElement
		for i, n := range names {
			kt := types.KeyTypeHash
			if i > 0 {
				kt = types.KeyTypeRange
			}
			out = append(out, types.KeySchemaElement{AttributeName: aws.String(n), KeyType: kt})
		}
		return out
	}
	td := &types.TableDescription{
		TableName: aws.String("t"), KeySchema: keyed("id", "at"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("at"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("g"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("l"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{{IndexName: aws.String("by_g"), KeySchema: keyed("g")}},
		LocalSecondaryIndexes:  []types.LocalSecondaryIndexDescription{{IndexName: aws.String("by_l"), KeySchema: keyed("id", "l")}},
	}
	var fields []arrow.Field
	for _, n := range []string{"id", "at", "g", "l"} {
		fields = append(fields, arrow.Field{Name: n, Type: arrow.PrimitiveTypes.Int64})
	}
	refused, _ := columnLimits(td, "s", arrow.NewSchema(fields, nil))
	if len(refused) != 4 {
		t.Fatalf("refused %+v, want each of 4 columns", refused)
	}
	for _, r := range refused {
		if r.Fix == "" {
			t.Errorf("%s's refusal has no fix: %+v", r.Column, r)
		}
	}
}

// A patch that read an item before another write of it, as a replaced
// attempt's can, leaves that write as it is: a newer version, or an
// overwrite's sweep of the item.
func TestStaleFillKeepsTheItem(t *testing.T) {
	db := newMemDB(map[string][]string{controlTable: {"p", "n"}, "t": {"id"}})
	w := memWriter(t, db, destination.Stream{Name: "t", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	stale := func(id string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{"id": str(id), "v": str("old"), idColumn: str(id + "-20"),
			heldColumn: &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"c": str(id + "-10")}}}
	}
	newer := map[string]types.AttributeValue{"id": str("1"), "v": str("new"), "c": str("filled"), idColumn: str("1-30")}
	if _, err := db.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String("t"), Item: newer}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"1", "2"} {
		if err := w.fill(t.Context(), "t", stale(id), id+"-10", map[string]types.AttributeValue{"c": str("filled")}); err != nil {
			t.Fatal(err)
		}
	}
	if items := db.items("t"); len(items) != 1 || stringOf(items[0]["v"]) != "new" || stringOf(items[0][idColumn]) != "1-30" {
		t.Errorf("after stale patches, the table is %v; want only id 1's newer version", items)
	}
}

// A merged row that doesn't send a held column keeps the item's value and
// its hold, so that the held version's patch still fills it.
func TestUnchangedKeepsHold(t *testing.T) {
	db := newMemDB(map[string][]string{controlTable: {"p", "n"}, "t": {"id"}})
	w := memWriter(t, db, destination.Stream{Name: "t", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	list := arrow.ListOf(arrow.BinaryTypes.String)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: idColumn, Type: arrow.BinaryTypes.String},
		{Name: seqColumn, Type: arrow.PrimitiveTypes.Int64},
		{Name: heldColumn, Type: list, Nullable: true},
		{Name: unchangedColumn, Type: list, Nullable: true},
	}, nil)
	// row is id 1's version with v, held and unchanged as given; nil is null.
	row := func(v *string, version string, seq int64, held, unchanged []string) arrow.RecordBatch {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		b.Field(0).(*array.StringBuilder).Append("1")
		if v == nil {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append(*v)
		}
		b.Field(2).(*array.StringBuilder).Append(version)
		b.Field(3).(*array.Int64Builder).Append(seq)
		for i, l := range [][]string{held, unchanged} {
			lb := b.Field(4 + i).(*array.ListBuilder)
			if l == nil {
				lb.AppendNull()
				continue
			}
			lb.Append(true)
			for _, s := range l {
				lb.ValueBuilder().(*array.StringBuilder).Append(s)
			}
		}
		return b.NewRecordBatch()
	}
	memCommit(t, w, 1, "t", row(nil, "1-10", 10, []string{"v"}, nil))
	memCommit(t, w, 2, "t", row(nil, "1-20", 20, nil, []string{"v"}))
	if it := db.items("t")[0]; !reflect.DeepEqual(it[heldColumn], &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"v": str("1-10")}}) {
		t.Errorf("after a row that didn't send v, the item's holds are %v", it[heldColumn])
	}
	e := destination.Epoch{Generation: 1, N: 3}
	p := row(sp("filled"), "1-10", 10, []string{"v"}, nil)
	defer p.Release()
	if err := w.Patch(t.Context(), e, "t", p); err != nil {
		t.Fatal(err)
	}
	staged, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), e, []destination.Staged{staged}); err != nil {
		t.Fatal(err)
	}
	if it := db.items("t")[0]; stringOf(it["v"]) != "filled" || it[heldColumn] != nil || stringOf(it[idColumn]) != "1-20" {
		t.Errorf("after the held version's patch: %v", it)
	}
}

// A replaced attempt's put of a merged row, after the next attempt has
// applied the row and filled a cell an earlier version held, leaves the cell
// filled: a put that reads nothing, one that keeps a column, and a soft
// delete, whose read comes before the next attempt's writes.
func TestLatePutKeepsFill(t *testing.T) {
	cfg := destination.Stream{Name: "t", Mode: destination.Merge, PrimaryKey: []string{"id"}}
	version := func(id, op, seq string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{"id": str("1"), idColumn: str(id), opColumn: str(op), seqColumn: &types.AttributeValueMemberN{Value: seq}}
	}
	first := func() map[string]types.AttributeValue {
		it := version("1-10", "insert", "10")
		it[heldColumn] = &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"c": str("1-10")}}
		return it
	}
	for name, tc := range map[string]struct {
		row  func() map[string]types.AttributeValue
		keep []string
	}{
		"put":         {row: first},
		"keep":        {row: func() map[string]types.AttributeValue { return version("1-20", "update", "20") }, keep: []string{"c"}},
		"soft delete": {row: func() map[string]types.AttributeValue { return version("1-30", "delete", "30") }},
	} {
		t.Run(name, func(t *testing.T) {
			db := newMemDB(map[string][]string{controlTable: {"p", "n"}, "t": {"id"}})
			w := memWriter(t, db, cfg)
			if _, err := db.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String("t"), Item: first()}); err != nil {
				t.Fatal(err)
			}
			next := func() {
				if _, err := w.applyRow(t.Context(), cfg, "t", tc.row(), tc.keep, ""); err != nil {
					t.Error(err)
				}
				cur, err := w.get(t.Context(), "t", map[string]types.AttributeValue{"id": str("1")})
				if err != nil {
					t.Error(err)
				}
				if err := w.fill(t.Context(), "t", cur, "1-10", map[string]types.AttributeValue{"c": str("filled")}); err != nil {
					t.Error(err)
				}
			}
			if name == "put" {
				next()
			} else {
				db.afterGet = next
			}
			if _, err := w.applyRow(t.Context(), cfg, "t", tc.row(), tc.keep, ""); err != nil {
				t.Fatal(err)
			}
			it := db.items("t")[0]
			if stringOf(it["c"]) != "filled" || it[heldColumn] != nil || stringOf(it[idColumn]) != stringOf(tc.row()[idColumn]) {
				t.Errorf("after the replaced attempt's put, the item is %v; want %s with c filled and no hold", it, stringOf(tc.row()[idColumn]))
			}
		})
	}
}

// A version put again with other values, as a read that starts over can
// send it, lands over the item it put before, when it holds no value.
func TestSameVersionAgain(t *testing.T) {
	cfg := destination.Stream{Name: "t", Mode: destination.Merge, PrimaryKey: []string{"id"}}
	db := newMemDB(map[string][]string{controlTable: {"p", "n"}, "t": {"id"}})
	w := memWriter(t, db, cfg)
	for _, v := range []string{"a", "b"} {
		row := map[string]types.AttributeValue{"id": str("1"), "v": str(v), idColumn: str("1-10"), opColumn: str("insert"), seqColumn: &types.AttributeValueMemberN{Value: "10"}}
		if _, err := w.applyRow(t.Context(), cfg, "t", row, nil, ""); err != nil {
			t.Fatal(err)
		}
	}
	if it := db.items("t")[0]; stringOf(it["v"]) != "b" {
		t.Errorf("after the version again with v b, the item is %v", it)
	}
}
