//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/openetl-io/openetl-connectors/connectortest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func discovered(t *testing.T, s *Source, stream string) source.CatalogStream {
	t.Helper()
	cat, err := s.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, cs := range cat.Streams {
		if cs.Name == stream {
			return cs
		}
	}
	t.Fatalf("discovery has no %s: %v", stream, cat.Streams)
	return source.CatalogStream{}
}

func readAll(t *testing.T, s *Source, streams ...source.ConfiguredStream) *connectortest.Recorder {
	t.Helper()
	rec := &connectortest.Recorder{}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: streams}, rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestSourceTypes(t *testing.T) {
	db := testDB(t)
	insert(t, db.Collection("kinds"), kinds()...)
	s := newTestSource(t, db, "")
	cs := discovered(t, s, "kinds")
	json, uuid := jsonType, extensions.NewUUIDType()
	want := map[string]arrow.DataType{
		"_id": arrow.BinaryTypes.String, "double": arrow.PrimitiveTypes.Float64, "string": arrow.BinaryTypes.String,
		"doc": json, "array": json, "binary": arrow.BinaryTypes.Binary, "uuid": uuid, "olduuid": arrow.BinaryTypes.Binary,
		"md5": arrow.BinaryTypes.Binary, "undefined": arrow.BinaryTypes.String, "oid": arrow.BinaryTypes.String,
		"bool": arrow.FixedWidthTypes.Boolean, "date": timestampType, "null": arrow.BinaryTypes.String, "regex": json,
		"dbpointer": json, "js": json, "symbol": arrow.BinaryTypes.String, "jsscope": json, "int32": arrow.PrimitiveTypes.Int64,
		"ts": json, "int64": arrow.PrimitiveTypes.Int64, "decimal": decimalType, "minkey": json, "maxkey": json,
		"varies": arrow.BinaryTypes.String, "num": arrow.PrimitiveTypes.Float64,
	}
	if cs.Schema.NumFields() != len(want) || cs.Schema.Field(0).Name != "_id" || cs.Schema.Field(0).Nullable {
		t.Errorf("schema: %s", cs.Schema)
	}
	for _, f := range cs.Schema.Fields() {
		if w, ok := want[f.Name]; !ok || !arrow.TypeEqual(f.Type, w) {
			t.Errorf("%s is declared %s, want %v", f.Name, f.Type, w)
		}
	}
	if len(cs.PrimaryKey) != 1 || cs.PrimaryKey[0] != "_id" || !cs.SplitsByKey || cs.Seq == nil {
		t.Errorf("stream: %+v", cs)
	}

	rows := readAll(t, s, source.ConfiguredStream{Name: "kinds", SyncMode: source.FullRefresh}).RowsOf("kinds")
	if len(rows) != 3 {
		t.Fatalf("rows: %v", rows)
	}
	for k, w := range map[string]string{
		"_id": intID(1), "double": "1.5", "string": "héllo ✓", "doc": `{"a":[1,"x"]}`, "array": "[1,2,3]", "binary": "AP8=",
		"uuid": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "olduuid": "AAAAAAAAAAAAAAAAAAAAAA==", "oid": "65f0a1b2c3d4e5f601234567",
		"bool": "true", "date": "2026-11-03T10:00:00.123Z", "regex": `{"$regularExpression":{"pattern":"^a.*","options":"i"}}`,
		"dbpointer": `{"$dbPointer":{"$ref":"db.c","$id":{"$oid":"65f0a1b2c3d4e5f601234567"}}}`, "js": `{"$code":"function() {}"}`,
		"symbol": "sym", "jsscope": `{"$code":"x","$scope":{"x":1}}`, "int32": "-7", "ts": `{"$timestamp":{"t":1700000000,"i":3}}`,
		"int64": "9223372036854775807", "decimal": "12345678901234567890.123456789", "minkey": `{"$minKey":1}`, "maxkey": `{"$maxKey":1}`,
		"varies": "1", "num": "1",
	} {
		if rows[0][k] != w {
			t.Errorf("document 1's %s is %q, want %q", k, rows[0][k], w)
		}
	}
	for _, k := range []string{"null", "undefined"} {
		if v, ok := rows[0][k]; ok {
			t.Errorf("document 1's %s is %q, want null", k, v)
		}
	}
	for k, w := range map[string]string{"double": "NaN", "int32": "-2147483648", "date": "0001-01-01T00:00:00.000Z", "decimal": "-Infinity", "varies": "text", "num": "2.5"} {
		if rows[1][k] != w {
			t.Errorf("document 2's %s is %q, want %q", k, rows[1][k], w)
		}
	}
	for k, w := range map[string]string{"double": "-inf", "varies": `{"x":null}`, "num": "1099511627776", "array": "[]"} {
		if rows[2][k] != w {
			t.Errorf("document 3's %s is %q, want %q", k, rows[2][k], w)
		}
	}
}

func TestSourceSampleSize(t *testing.T) {
	db := testDB(t)
	var docs []any
	for i := range 200 {
		docs = append(docs, bson.D{{Key: "_id", Value: i}, {Key: "a", Value: i}})
	}
	insert(t, db.Collection("c"), docs...)
	insert(t, db.Collection("c"), bson.D{{Key: "_id", Value: 1000}, {Key: "rare", Value: true}})
	cs := discovered(t, newTestSource(t, db, `, "sampleSize": 5`), "c")
	if cs.Schema.NumFields() > 3 {
		t.Errorf("a sample of 5 declared %s", cs.Schema)
	}
	cs = discovered(t, newTestSource(t, db, ""), "c")
	if _, ok := cs.Schema.FieldsByName("rare"); !ok {
		t.Errorf("reading every document didn't declare rare: %s", cs.Schema)
	}
}

func TestSplitsMixedIDs(t *testing.T) {
	db := testDB(t)
	coll := db.Collection("mixed")
	var docs []any
	want := map[string]bool{}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 60 {
		ids := []any{
			i, int64(i) + 1000, float64(i) + 0.5, fmt.Sprintf("s%03d", i), bson.NewObjectID(),
			bson.D{{Key: "k", Value: i}}, bson.Binary{Data: []byte{byte(i)}}, base.Add(time.Duration(i) * time.Hour),
		}
		ids = append(ids, keyText(raw(t, i)), bson.NewObjectID().Hex())
		for _, id := range ids {
			docs = append(docs, bson.D{{Key: "_id", Value: id}, {Key: "i", Value: i}})
			want[keyText(raw(t, id))] = true
		}
	}
	for _, id := range []any{true, nil, bson.MinKey{}, bson.MaxKey{}} {
		docs = append(docs, bson.D{{Key: "_id", Value: id}})
		want[keyText(raw(t, id))] = true
	}
	insert(t, coll, docs...)
	if len(want) != len(docs) {
		t.Fatalf("%d documents have %d keys", len(docs), len(want))
	}
	total := len(docs)

	s := newTestSource(t, db, `, "chunkDocuments": 20`)
	cs := source.ConfiguredStream{Name: "mixed", SyncMode: source.FullRefresh}
	splits, err := s.Splits(t.Context(), cs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) < 8 {
		t.Errorf("%d documents in chunks of 20 make %d splits", total, len(splits))
	}
	cs.Splits = splits
	// Read in smaller chunks, the first split read is asked to split at its
	// first resume point.
	s = newTestSource(t, db, `, "chunkDocuments": 5`)
	rec := &connectortest.Recorder{}
	asked := false
	hook := &hookRecorder{Recorder: rec, onResume: func(stream, split string) {
		if !asked {
			asked = true
			if ok, err := s.Resplit(context.Background(), stream, split); !ok || err != nil {
				t.Errorf("Resplit(%s, %s) = %v, %v", stream, split, ok, err)
			}
		}
	}}
	if err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{cs}}, hook); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, r := range rec.RowsOf("mixed") {
		seen[r["_id"]]++
	}
	for id := range want {
		if seen[id] != 1 {
			t.Errorf("_id %s was read %d times", id, seen[id])
		}
	}
	if len(seen) != total {
		t.Errorf("read %d _ids, want %d", len(seen), total)
	}
	added := 0
	for _, e := range rec.Snapshot() {
		added += len(e.Added)
	}
	if added != 1 {
		t.Errorf("the resplit added %d splits, want 1", added)
	}

	// Read whole, in chunks of one and of seven.
	for _, chunk := range []int{1, 7} {
		s := newTestSource(t, db, fmt.Sprintf(`, "chunkDocuments": %d`, chunk))
		rows := readAll(t, s, source.ConfiguredStream{Name: "mixed", SyncMode: source.FullRefresh}).RowsOf("mixed")
		seen := map[string]int{}
		for _, r := range rows {
			seen[r["_id"]]++
		}
		if len(rows) != total || len(seen) != total {
			t.Errorf("in chunks of %d, read %d rows of %d _ids, want %d", chunk, len(rows), len(seen), total)
		}
	}
}

// hookRecorder calls onResume after each split's resume point.
type hookRecorder struct {
	*connectortest.Recorder
	onResume func(stream, split string)
}

func (h *hookRecorder) Split(stream, id string) source.SplitEmitter {
	return &hookSplit{SplitEmitter: h.Recorder.Split(stream, id), h: h, stream: stream, id: id}
}

type hookSplit struct {
	source.SplitEmitter
	h          *hookRecorder
	stream, id string
}

func (s *hookSplit) ResumePoint(state []byte, added ...source.Split) error {
	err := s.SplitEmitter.ResumePoint(state, added...)
	s.h.onResume(s.stream, s.id)
	return err
}

func TestIncrementalCursorTypes(t *testing.T) {
	db := testDB(t)
	coll := db.Collection("ev")
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 30 {
		insert(t, coll, bson.D{{Key: "_id", Value: i}, {Key: "at", Value: day.Add(time.Duration(i) * time.Hour)}})
	}
	// A cursor value of another type than the highest doesn't compare.
	insert(t, coll, bson.D{{Key: "_id", Value: 100}, {Key: "at", Value: "not a date"}}, bson.D{{Key: "_id", Value: 101}})
	s := newTestSource(t, db, `, "chunkDocuments": 7`)
	cs := source.ConfiguredStream{Name: "ev", SyncMode: source.Incremental, Cursor: "at"}
	rec := readAll(t, s, cs)
	if n := len(rec.RowsOf("ev")); n != 30 {
		t.Errorf("the first incremental read read %d, want 30", n)
	}
	next, complete := rec.Last("ev")
	if !complete {
		t.Fatalf("the read didn't complete")
	}
	insert(t, coll, bson.D{{Key: "_id", Value: 200}, {Key: "at", Value: day.AddDate(1, 0, 0)}}, bson.D{{Key: "_id", Value: 201}, {Key: "at", Value: day}})
	cs.State = next
	rows := readAll(t, s, cs).RowsOf("ev")
	// From the last read's highest value: 29's again, and 200's.
	if len(rows) != 2 || rows[0]["_id"] != intID(29) || rows[1]["_id"] != intID(200) {
		t.Errorf("the next read: %v", rows)
	}
	// Without a cursor, incremental can't read.
	err := s.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "ev", SyncMode: source.Incremental}}}, &connectortest.Recorder{})
	if connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID {
		t.Errorf("incremental without a cursor: %s: %v", connectortest.CodeOf(err), err)
	}
}

func TestSourceCheck(t *testing.T) {
	db := testDB(t)
	insert(t, db.Collection("a"), bson.D{{Key: "_id", Value: 1}})
	if err := newTestSource(t, db, `, "collections": ["a"]`).Check(t.Context()); err != nil {
		t.Errorf("Check: %v", err)
	}
	err := newTestSource(t, db, `, "collections": ["missing"]`).Check(t.Context())
	if connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID {
		t.Errorf("Check of a missing collection: %s: %v", connectortest.CodeOf(err), err)
	}
	bad, err := NewSource(sourceConfig(badPassword(t), db, ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := bad.Check(t.Context()); connectortest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID {
		t.Errorf("Check with a bad password: %s: %v", connectortest.CodeOf(err), err)
	}
}

// A range's bounds are values: a string _id that starts with $ isn't a
// field path, which would make every _id fall inside the range.
func TestSampleIDsLiteralBounds(t *testing.T) {
	db := testDB(t)
	coll := db.Collection("c")
	for _, id := range []string{"$a", "$b", "$c", "$d", "$e"} {
		insert(t, coll, bson.D{{Key: "_id", Value: id}, {Key: "c", Value: ""}})
	}
	lo, hi := &value{raw(t, "$a")}, &value{raw(t, "$e")}
	ids, err := sampleIDs(t.Context(), coll, 10, lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, id := range ids {
		got = append(got, id.StringValue())
	}
	if fmt.Sprint(got) != "[$b $c $d]" {
		t.Errorf("sampled %v between $a and $e", got)
	}
}
