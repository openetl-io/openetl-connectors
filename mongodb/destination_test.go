//go:build integration

package mongodb

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func openWriter(t *testing.T, db *mongo.Database, gen uint64, streams ...destination.Stream) *writer {
	t.Helper()
	return openWriterWith(t, db, "", gen, streams...)
}

// openWriterWith opens a writer of a destination whose config has extra.
func openWriterWith(t *testing.T, db *mongo.Database, extra string, gen uint64, streams ...destination.Stream) *writer {
	t.Helper()
	d, err := NewDestination(fmt.Appendf(nil, `{"uri": %q, "database": %q%s}`, uri(), db.Name(), extra))
	if err != nil {
		t.Fatal(err)
	}
	w, err := d.Open(t.Context(), destination.OpenConfig{Pipeline: "p", Generation: gen, Scope: destination.ScopePipeline, Streams: streams})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close(t.Context()) })
	if _, err := w.Recover(t.Context(), gen, destination.Epoch{}, nil); err != nil {
		t.Fatal(err)
	}
	return w.(*writer)
}

// row is a merge row: id, v, its op and its seq in microseconds.
type row struct {
	id  int64
	v   string
	op  string
	seq int64
}

func rowsBatch(t *testing.T, rows ...row) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: idColumn, Type: arrow.BinaryTypes.String},
		{Name: opColumn, Type: arrow.BinaryTypes.String},
		{Name: seqColumn, Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, r := range rows {
		b.Field(0).(*array.Int64Builder).Append(r.id)
		b.Field(1).(*array.StringBuilder).Append(r.v)
		b.Field(2).(*array.StringBuilder).Append(fmt.Sprintf("%d-%d", r.id, r.seq))
		b.Field(3).(*array.StringBuilder).Append(r.op)
		b.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(r.seq))
	}
	return b.NewRecordBatch()
}

// commit writes batches of stream as epoch n and commits it.
func commit(t *testing.T, w *writer, n uint64, stream string, batches ...arrow.RecordBatch) {
	t.Helper()
	e := destination.Epoch{Generation: w.cfg.Generation, N: n}
	for _, b := range batches {
		err := w.Write(t.Context(), e, stream, b)
		b.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	staged, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), e, []destination.Staged{staged}); err != nil {
		t.Fatal(err)
	}
}

func docs(t *testing.T, coll *mongo.Collection) map[int64]bson.M {
	t.Helper()
	cur, err := coll.Find(t.Context(), bson.D{})
	if err != nil {
		t.Fatal(err)
	}
	var all []bson.M
	if err := cur.All(t.Context(), &all); err != nil {
		t.Fatal(err)
	}
	out := map[int64]bson.M{}
	for _, d := range all {
		out[d["id"].(int64)] = d
	}
	return out
}

func TestMergeSeqGuard(t *testing.T) {
	for name, deletes := range map[string]destination.DeleteMode{"soft": destination.SoftDelete, "hard": destination.HardDelete} {
		t.Run(name, func(t *testing.T) {
			db := testDB(t)
			w := openWriter(t, db, 1, destination.Stream{Name: "s", Mode: destination.Merge, PrimaryKey: []string{"id"}, Deletes: deletes, Dedupe: destination.DedupeTarget})
			commit(t, w, 1, "s", rowsBatch(t, row{1, "new", "update", 20}, row{2, "b", "insert", 10}))
			// An older version doesn't replace a newer one, nor delete it.
			commit(t, w, 2, "s", rowsBatch(t, row{1, "old", "update", 10}, row{2, "", "delete", 5}))
			got := docs(t, db.Collection("s"))
			if got[1]["v"] != "new" || got[1]["_id"] != int64(1) || got[2]["v"] != "b" || got[2][deletedColumn] != nil {
				t.Errorf("after older versions: %v", got)
			}
			commit(t, w, 3, "s", rowsBatch(t, row{2, "", "delete", 30}))
			got = docs(t, db.Collection("s"))
			switch deletes {
			case destination.HardDelete:
				if _, ok := got[2]; ok || len(got) != 1 {
					t.Errorf("after a hard delete: %v", got)
				}
			default:
				// A soft delete keeps the document's values.
				if d := got[2]; d[deletedColumn] != true || d["v"] != "b" || d[opColumn] != "delete" {
					t.Errorf("after a soft delete: %v", d)
				}
			}
			// A version of a key seen in two lanes' order: the higher seq wins
			// whichever lands last.
			commit(t, w, 4, "s", rowsBatch(t, row{3, "z", "insert", 50}, row{3, "y", "update", 40}))
			if d := docs(t, db.Collection("s"))[3]; d["v"] != "z" {
				t.Errorf("of two versions in an epoch, %v landed", d)
			}
		})
	}
}

func TestAppendKeepsDuplicates(t *testing.T) {
	db := testDB(t)
	w := openWriter(t, db, 1, destination.Stream{Name: "s", Mode: destination.Append})
	commit(t, w, 1, "s", rowsBatch(t, row{1, "a", "insert", 1}, row{1, "a", "insert", 1}))
	n, err := db.Collection("s").CountDocuments(t.Context(), bson.D{})
	if err != nil || n != 2 {
		t.Errorf("append without dedupe has %d documents: %v", n, err)
	}
	// Deduped at apply, a version lands once, whatever the epoch.
	w = openWriter(t, db, 2, destination.Stream{Name: "d", Mode: destination.Append, Dedupe: destination.DedupeTarget})
	commit(t, w, 1, "d", rowsBatch(t, row{1, "a", "insert", 1}, row{1, "a", "insert", 1}))
	commit(t, w, 2, "d", rowsBatch(t, row{1, "a", "insert", 1}))
	if n, err := db.Collection("d").CountDocuments(t.Context(), bson.D{}); err != nil || n != 1 {
		t.Errorf("append deduped at apply has %d documents: %v", n, err)
	}
}

// An overwrite's swap, run again after a crash between the rename and the
// marker, leaves the target as it is.
func TestOverwriteSwapAgain(t *testing.T) {
	db := testDB(t)
	start := time.Now()
	w := openWriter(t, db, 1, destination.Stream{Name: "s", Mode: destination.Overwrite})
	if err := w.StartReads(t.Context(), map[string]time.Time{"s": start}); err != nil {
		t.Fatal(err)
	}
	insert(t, db.Collection("s"), bson.D{{Key: "id", Value: int64(99)}})
	if _, err := db.Collection("s").Indexes().CreateOne(t.Context(), mongo.IndexModel{Keys: bson.D{{Key: "v", Value: 1}}}); err != nil {
		t.Fatal(err)
	}
	e := destination.Epoch{Generation: 1, N: 1}
	b := rowsBatch(t, row{1, "a", "insert", 1})
	if err := w.Write(t.Context(), e, "s", b); err != nil {
		t.Fatal(err)
	}
	b.Release()
	if err := w.EndRead(t.Context(), e, "s"); err != nil {
		t.Fatal(err)
	}
	staged, err := w.PreCommit(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), e, []destination.Staged{staged}); err != nil {
		t.Fatal(err)
	}
	m := marker{Stream: "s", Generation: 1, N: 1, Swap: readName("p", "s", start), Target: "s"}
	if err := w.swap(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	got := docs(t, db.Collection("s"))
	if len(got) != 1 || got[1]["v"] != "a" {
		t.Errorf("after the swap again: %v", got)
	}
	specs, err := db.Collection("s").Indexes().ListSpecifications(t.Context())
	if err != nil || len(specs) != 2 {
		t.Errorf("the target's indexes after the swap: %v, %v", specs, err)
	}
}

func TestDestinationTypes(t *testing.T) {
	db := testDB(t)
	w := openWriter(t, db, 1, destination.Stream{Name: "s", Mode: destination.Append})
	jsonT, _ := extensions.NewJSONType(arrow.BinaryTypes.String)
	uuidT := extensions.NewUUIDType()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "j", Type: jsonT, Nullable: true},
		{Name: "u", Type: uuidT, Nullable: true},
		{Name: "d", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true},
		{Name: "day", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.ExtensionBuilder).Builder.(*array.StringBuilder).Append(`{"o": {"$oid": "65f0a1b2c3d4e5f601234567"}, "n": null}`)
	b.Field(2).(*extensions.UUIDBuilder).Append(uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
	b.Field(3).(*array.Decimal128Builder).Append(decimal128.FromI64(-1250))
	b.Field(4).(*array.Date32Builder).Append(arrow.Date32FromTime(time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)))
	b.Field(5).AppendNull()
	batch := b.NewRecordBatch()
	b.Release()
	commit(t, w, 1, "s", batch)
	var got bson.Raw
	if err := db.Collection("s").FindOne(t.Context(), bson.D{}).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if v := got.Lookup("j", "o"); v.Type != bson.TypeObjectID {
		t.Errorf("JSON's $oid landed as %s", v.Type)
	}
	if v := got.Lookup("j", "n"); v.Type != bson.TypeNull {
		t.Errorf("JSON's null landed as %s", v.Type)
	}
	if sub, data, ok := got.Lookup("u").BinaryOK(); !ok || sub != bson.TypeBinaryUUID || uuidText(data) != "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" {
		t.Errorf("the UUID landed as %s", got.Lookup("u"))
	}
	if d, ok := got.Lookup("d").Decimal128OK(); !ok || d.String() != "-12.50" {
		t.Errorf("the decimal landed as %s", got.Lookup("d"))
	}
	if d, ok := got.Lookup("day").DateTimeOK(); !ok || time.UnixMilli(d).UTC().Format(time.DateOnly) != "2024-02-29" {
		t.Errorf("the date landed as %s", got.Lookup("day"))
	}
	if _, err := got.LookupErr("n"); err == nil {
		t.Errorf("a null landed as a field")
	}
}

func TestApplySchemaValidator(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()
	validator := bson.D{{Key: "$jsonSchema", Value: bson.D{
		{Key: "required", Value: bson.A{"v"}},
		{Key: "properties", Value: bson.D{{Key: "n", Value: bson.D{{Key: "bsonType", Value: "int"}}}}},
	}}}
	for _, c := range []string{"refused", "allowed"} {
		if err := db.RunCommand(ctx, bson.D{{Key: "create", Value: c}, {Key: "validator", Value: validator}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	w := openWriter(t, db, 1,
		destination.Stream{Name: "refused", Mode: destination.Append},
		destination.Stream{Name: "allowed", Mode: destination.Append, AllowTypeChanges: true},
		destination.Stream{Name: "plain", Mode: destination.Append},
	)
	schema := func(table string) *arrow.Schema {
		return arrow.NewSchema([]arrow.Field{
			{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: idColumn, Type: arrow.BinaryTypes.String},
		}, func() *arrow.Metadata { m := arrow.NewMetadata([]string{tableKey}, []string{table}); return &m }())
	}
	applied, err := w.ApplySchema(ctx, []destination.StreamSchema{
		{Name: "refused", Schema: schema("refused")}, {Name: "allowed", Schema: schema("allowed")}, {Name: "plain", Schema: schema("plain")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Refused) != 1 {
		t.Fatalf("refused %+v", applied.Refused)
	}
	r := applied.Refused[0]
	if r.Stream != "refused" || r.Column != "n" || r.FromType != "int" || r.ToType != "long" || !strings.HasPrefix(r.Fix, "db.runCommand(") {
		t.Errorf("refusal %+v", r)
	}
	for _, s := range []string{"refused", "allowed"} {
		if l := applied.Limits[s]; len(l.NotNull) != 1 || l.NotNull[0] != "v" {
			t.Errorf("%s's limits: %+v", s, l)
		}
	}
	if l := applied.Limits["plain"]; len(l.NotNull) != 0 {
		t.Errorf("plain's limits: %+v", l)
	}
	if names, _ := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: "plain"}}); len(names) != 1 {
		t.Errorf("ApplySchema didn't create plain")
	}
	// The refusal's fix, run, makes the collection take the change.
	var cmd bson.D
	if err := bson.UnmarshalExtJSON([]byte(strings.TrimSuffix(strings.TrimPrefix(r.Fix, "db.runCommand("), ")")), false, &cmd); err != nil {
		t.Fatalf("the fix %s: %v", r.Fix, err)
	}
	if err := db.RunCommand(ctx, cmd).Err(); err != nil {
		t.Fatalf("the fix %s: %v", r.Fix, err)
	}
	applied, err = w.ApplySchema(ctx, []destination.StreamSchema{{Name: "refused", Schema: schema("refused")}, {Name: "allowed", Schema: schema("allowed")}})
	if err != nil || len(applied.Refused) != 0 {
		t.Errorf("after the fix, and allowed: %+v, %v", applied.Refused, err)
	}
	// Both take a long now.
	for _, c := range []string{"refused", "allowed"} {
		if _, err := db.Collection(c).InsertOne(ctx, bson.D{{Key: "v", Value: "x"}, {Key: "n", Value: int64(1) << 40}}); err != nil {
			t.Errorf("%s takes no long: %v", c, err)
		}
	}
	// A write the validator rejects fails, and says why.
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema("refused"))
	b.Field(0).AppendNull()
	b.Field(1).(*array.Int64Builder).Append(1)
	b.Field(2).(*array.StringBuilder).Append("x")
	batch := b.NewRecordBatch()
	b.Release()
	e := destination.Epoch{Generation: 1, N: 1}
	if err := w.Write(ctx, e, "refused", batch); err != nil {
		t.Fatal(err)
	}
	batch.Release()
	staged, err := w.PreCommit(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(ctx, e, []destination.Staged{staged}); connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID {
		t.Errorf("a document the validator rejects: %s: %v", connectortest.CodeOf(err), err)
	}
}

// A user's unique index that a row breaks fails the commit, rather than the
// row being taken for one an earlier apply wrote.
func TestUniqueIndex(t *testing.T) {
	for name, mode := range map[string]destination.WriteMode{"append": destination.Append, "merge": destination.Merge} {
		t.Run(name, func(t *testing.T) {
			db := testDB(t)
			if _, err := db.Collection("s").Indexes().CreateOne(t.Context(), mongo.IndexModel{Keys: bson.D{{Key: "v", Value: 1}}, Options: options.Index().SetUnique(true)}); err != nil {
				t.Fatal(err)
			}
			w := openWriter(t, db, 1, destination.Stream{Name: "s", Mode: mode, PrimaryKey: []string{"id"}})
			e := destination.Epoch{Generation: 1, N: 1}
			b := rowsBatch(t, row{1, "same", "insert", 1}, row{2, "same", "insert", 1})
			if err := w.Write(t.Context(), e, "s", b); err != nil {
				t.Fatal(err)
			}
			b.Release()
			staged, err := w.PreCommit(t.Context(), e)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Commit(t.Context(), e, []destination.Staged{staged}); connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID {
				t.Errorf("a row the unique index refuses: %s: %v", connectortest.CodeOf(err), err)
			}
		})
	}
}
