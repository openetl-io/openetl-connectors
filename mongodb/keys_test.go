//go:build integration

package mongodb

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// collectionKeys are the keyText of a collection's _ids.
func collectionKeys(t *testing.T, coll *mongo.Collection) []string {
	t.Helper()
	cur, err := coll.Find(t.Context(), bson.D{}, options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for cur.Next(t.Context()) {
		keys = append(keys, keyText(cur.Current.Lookup("_id")))
	}
	if err := cur.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

func rowKeys(rows []map[string]string) []string {
	var keys []string
	for _, r := range rows {
		keys = append(keys, r["_id"])
	}
	return keys
}

// cdcBatch is cdc rows of _id and n as the engine passes them on: _id a
// string, the op and seq in the destination's columns.
func cdcBatch(t *testing.T, rows []map[string]string) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "_id", Type: arrow.BinaryTypes.String},
		{Name: "n", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: idColumn, Type: arrow.BinaryTypes.String},
		{Name: opColumn, Type: arrow.BinaryTypes.String},
		{Name: seqColumn, Type: seqType},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i, r := range rows {
		b.Field(0).(*array.StringBuilder).Append(r["_id"])
		if n, ok := r["n"]; ok {
			b.Field(1).(*array.StringBuilder).Append(n)
		} else {
			b.Field(1).AppendNull()
		}
		b.Field(2).(*array.StringBuilder).Append(fmt.Sprint(i))
		b.Field(3).(*array.StringBuilder).Append(r["openetl.op"])
		b.Field(4).(*array.Decimal128Builder).Append(decimal128.FromBigInt(seqOf(t, r)))
	}
	return b.NewRecordBatch()
}

// TestMixedIDKeys reads _ids whose plain text is another's, by snapshot,
// change stream and incremental and full refresh reads, and writes them
// to destination-mongodb.
func TestMixedIDKeys(t *testing.T) {
	db := testDB(t)
	coll := db.Collection("c")
	ctx := t.Context()
	oid, _ := bson.ObjectIDFromHex("65f0a1b2c3d4e5f601234567")
	dec, _ := bson.ParseDecimal128("0.1")
	ids := []any{int32(5), "5", int64(1 << 40), "1099511627776", 0.1, dec, oid, oid.Hex()}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	doc := func(i int) bson.D {
		return bson.D{{Key: "_id", Value: ids[i]}, {Key: "n", Value: i}, {Key: "at", Value: at.Add(time.Duration(i) * time.Hour)}}
	}
	insert(t, coll, doc(0), doc(1), doc(2), doc(3))
	// MongoDB's _id index takes 5 and Long(5) for one key, so a collection
	// can't hold both.
	if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: int64(5)}}); !mongo.IsDuplicateKeyError(err) {
		t.Fatalf("Long(5) beside 5: %v", err)
	}

	s := newTestSource(t, db, "")
	columns := []string{"_id", "at", "n", "openetl.op", "openetl.seq"}
	rec, done := follow(t, s, "c", nil, columns)
	snapshot := changes(t, rec, done, "c", 0, 4)
	insert(t, coll, doc(4), doc(5), doc(6), doc(7))
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: int32(5)}}, bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: 10}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: "5"}}); err != nil {
		t.Fatal(err)
	}
	stream := changes(t, rec, done, "c", 4, 6)

	var want []string
	for _, id := range ids {
		want = append(want, keyText(raw(t, id)))
	}
	// The snapshot reads in _id order, numbers before strings.
	if got := rowKeys(snapshot); !slices.Equal(got, []string{want[0], want[2], want[3], want[1]}) {
		t.Errorf("the snapshot's keys are %q, want %q", got, want[:4])
	}
	// The change stream's keys are the snapshot's for the same documents.
	if got := rowKeys(stream); !slices.Equal(got, append(slices.Clone(want[4:]), want[0], want[1])) {
		t.Errorf("the change stream's keys are %q", got)
	}
	if stream[4]["openetl.op"] != source.OpUpdate || stream[4]["n"] != "10" || stream[5]["openetl.op"] != source.OpDelete {
		t.Errorf("the update and delete: %v", stream[4:])
	}
	all := slices.Clone(want)
	slices.Sort(all)
	if len(slices.Compact(all)) != len(ids) {
		t.Errorf("%d documents have keys %q", len(ids), want)
	}

	// A full refresh and an incremental read key each document alike.
	left := collectionKeys(t, coll)
	full := readAll(t, s, source.ConfiguredStream{Name: "c", SyncMode: source.FullRefresh, Columns: columns[:3]}).rows("c")
	incr := readAll(t, s, source.ConfiguredStream{Name: "c", SyncMode: source.Incremental, Cursor: "at", Columns: columns[:3]}).rows("c")
	for name, rows := range map[string][]map[string]string{"full refresh": full, "incremental": incr} {
		got := rowKeys(rows)
		slices.Sort(got)
		if !slices.Equal(got, left) || len(left) != len(ids)-1 {
			t.Errorf("%s keys are %q, want %q", name, got, left)
		}
	}

	// Merged into destination-mongodb by key, each change lands on its own
	// document, which keeps its _id's BSON type.
	w := openWriterWith(t, db, `, "extendedJSONKeys": true`, 1,
		destination.Stream{Name: "out", Mode: destination.Merge, PrimaryKey: []string{"_id"}, Deletes: destination.HardDelete, Dedupe: destination.DedupeTarget})
	commit(t, w, 1, "out", cdcBatch(t, rec.rows("c")))
	if got := collectionKeys(t, db.Collection("out")); !slices.Equal(got, left) {
		t.Errorf("destination-mongodb has _ids %q, want %q", got, left)
	}
	var five bson.M
	if err := db.Collection("out").FindOne(ctx, bson.D{{Key: "_id", Value: int32(5)}}).Decode(&five); err != nil || five["n"] != "10" {
		t.Errorf("5's document: %v, %v", five, err)
	}
	// A key the source wouldn't spell, a bare 5, is refused.
	bad := cdcBatch(t, []map[string]string{{"_id": "5", "openetl.op": source.OpInsert, "openetl.seq": "1"}})
	defer bad.Release()
	if err := w.Write(ctx, destination.Epoch{Generation: 1, N: 2}, "out", bad); codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID {
		t.Errorf("a key that isn't Extended JSON: %s: %v", codeOf(err), err)
	}

	// Without extendedJSONKeys, the keys are strings, one for each _id.
	w = openWriterWith(t, db, "", 2, destination.Stream{Name: "text", Mode: destination.Merge, PrimaryKey: []string{"_id"}, Deletes: destination.HardDelete})
	commit(t, w, 1, "text", cdcBatch(t, rec.rows("c")))
	if n, err := db.Collection("text").CountDocuments(ctx, bson.D{}); err != nil || n != int64(len(left)) {
		t.Errorf("with string keys, %d documents: %v", n, err)
	}
}

// TestPacked reads documents whole, whatever their fields, with each sync
// mode.
func TestPacked(t *testing.T) {
	db := testDB(t)
	coll := db.Collection("c")
	ctx := t.Context()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	insert(t, coll,
		bson.D{{Key: "_id", Value: 1}, {Key: "at", Value: at}, {Key: "a", Value: int64(7)}},
		bson.D{{Key: "_id", Value: "x"}, {Key: "at", Value: at.Add(time.Hour)}, {Key: "b", Value: bson.D{{Key: "c", Value: bson.A{1, 2.5}}}}},
	)
	s := newTestSource(t, db, `, "packed": true, "chunkDocuments": 1`)
	cs := discovered(t, s, "c")
	if cs.Schema.NumFields() != 2 || cs.Schema.Field(0).Name != "_id" || !arrow.TypeEqual(cs.Schema.Field(0).Type, arrow.BinaryTypes.String) ||
		cs.Schema.Field(1).Name != dataColumn || !arrow.TypeEqual(cs.Schema.Field(1).Type, jsonType) || len(cs.SyncModes) != 3 {
		t.Fatalf("packed stream: %+v", cs)
	}
	columns := []string{"_id", "data"}
	// data is each document, exactly.
	same := func(r map[string]string, want bson.D) bool {
		var got bson.Raw
		if err := bson.UnmarshalExtJSON([]byte(r[dataColumn]), true, &got); err != nil {
			t.Errorf("data %s: %v", r[dataColumn], err)
			return false
		}
		b, _ := bson.Marshal(want)
		return slices.Equal(got, b) && r["_id"] == keyText(got.Lookup("_id"))
	}
	full := readAll(t, s, source.ConfiguredStream{Name: "c", SyncMode: source.FullRefresh, Columns: columns}).rows("c")
	if len(full) != 2 || !same(full[0], bson.D{{Key: "_id", Value: int32(1)}, {Key: "at", Value: bson.NewDateTimeFromTime(at)}, {Key: "a", Value: int64(7)}}) ||
		!same(full[1], bson.D{{Key: "_id", Value: "x"}, {Key: "at", Value: bson.NewDateTimeFromTime(at.Add(time.Hour))}, {Key: "b", Value: bson.D{{Key: "c", Value: bson.A{int32(1), 2.5}}}}}) {
		t.Errorf("full refresh: %v", full)
	}

	// A field no discovery saw is in data, not a new column, so no read fails
	// on it.
	incr := source.ConfiguredStream{Name: "c", SyncMode: source.Incremental, Cursor: "at", Columns: columns}
	rec := readAll(t, s, incr)
	if n := len(rec.rows("c")); n != 2 {
		t.Errorf("the first incremental read read %d", n)
	}
	incr.State, _ = rec.last("c")
	insert(t, coll, bson.D{{Key: "_id", Value: 2}, {Key: "at", Value: at.Add(2 * time.Hour)}, {Key: "unseen", Value: true}})
	rows := readAll(t, s, incr).rows("c")
	// From the last read's highest at, in _id order.
	if len(rows) != 2 || rows[0]["_id"] != intID(2) || rows[1]["_id"] != `"x"` || !same(rows[0], bson.D{{Key: "_id", Value: int32(2)}, {Key: "at", Value: bson.NewDateTimeFromTime(at.Add(2 * time.Hour))}, {Key: "unseen", Value: true}}) {
		t.Errorf("the next incremental read: %v", rows)
	}

	// cdc: inserts and updates as their documents, a delete as its key with
	// no data where the collection keeps no pre-images, and the document
	// before it where it does.
	if err := db.CreateCollection(ctx, "img", options.CreateCollection().SetChangeStreamPreAndPostImages(bson.D{{Key: "enabled", Value: true}})); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"c", "img"} {
		coll := db.Collection(name)
		if name == "img" {
			insert(t, coll, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}}, bson.D{{Key: "_id", Value: "x"}}, bson.D{{Key: "_id", Value: 2}})
		}
		rec, done := follow(t, s, name, nil, []string{"_id", "data", "openetl.op", "openetl.seq"})
		changes(t, rec, done, name, 0, 3)
		insert(t, coll, bson.D{{Key: "_id", Value: 3}, {Key: "brand new", Value: bson.A{}}})
		if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: "x"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "z", Value: nil}}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: 1}}); err != nil {
			t.Fatal(err)
		}
		rows := changes(t, rec, done, name, 3, 3)
		if rows[0]["openetl.op"] != source.OpInsert || !same(rows[0], bson.D{{Key: "_id", Value: int32(3)}, {Key: "brand new", Value: bson.A{}}}) {
			t.Errorf("%s: the insert: %v", name, rows[0])
		}
		if rows[1]["openetl.op"] != source.OpUpdate || rows[1]["_id"] != `"x"` || !same(rows[1], docOf(t, coll, "x")) {
			t.Errorf("%s: the update: %v", name, rows[1])
		}
		del := rows[2]
		switch {
		case del["openetl.op"] != source.OpDelete || del["_id"] != intID(1):
			t.Errorf("%s: the delete: %v", name, del)
		case name == "c" && del[dataColumn] != "":
			t.Errorf("a delete without a pre-image has data %s", del[dataColumn])
		case name == "img" && !same(del, bson.D{{Key: "_id", Value: int32(1)}, {Key: "a", Value: int32(1)}}):
			t.Errorf("a delete's pre-image: %v", del)
		}
	}
}

// docOf is the document of a collection with _id id.
func docOf(t *testing.T, coll *mongo.Collection, id any) bson.D {
	t.Helper()
	var d bson.D
	if err := coll.FindOne(t.Context(), bson.D{{Key: "_id", Value: id}}).Decode(&d); err != nil {
		t.Fatal(err)
	}
	return d
}
