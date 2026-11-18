//go:build integration

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/openetl-io/openetl/sdk/go/conformance"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// kinds are documents with every BSON type, and fields whose type varies.
func kinds() []any {
	d := func(s string) bson.Decimal128 { v, _ := bson.ParseDecimal128(s); return v }
	oid, _ := bson.ObjectIDFromHex("65f0a1b2c3d4e5f601234567")
	return []any{
		bson.D{
			{Key: "_id", Value: 1}, {Key: "double", Value: 1.5}, {Key: "string", Value: "héllo ✓"}, {Key: "doc", Value: bson.D{{Key: "a", Value: bson.A{1, "x"}}}},
			{Key: "array", Value: bson.A{1, 2, 3}}, {Key: "binary", Value: bson.Binary{Data: []byte{0, 255}}},
			{Key: "uuid", Value: bson.Binary{Subtype: 4, Data: []byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11}}},
			{Key: "olduuid", Value: bson.Binary{Subtype: 3, Data: make([]byte, 16)}}, {Key: "md5", Value: bson.Binary{Subtype: 5, Data: make([]byte, 16)}},
			{Key: "undefined", Value: bson.Undefined{}}, {Key: "oid", Value: oid}, {Key: "bool", Value: true},
			{Key: "date", Value: bson.NewDateTimeFromTime(time.Date(2026, 11, 3, 10, 0, 0, 123e6, time.UTC))},
			{Key: "null", Value: nil}, {Key: "regex", Value: bson.Regex{Pattern: "^a.*", Options: "i"}},
			{Key: "dbpointer", Value: bson.DBPointer{DB: "db.c", Pointer: oid}}, {Key: "js", Value: bson.JavaScript("function() {}")},
			{Key: "symbol", Value: bson.Symbol("sym")}, {Key: "jsscope", Value: bson.CodeWithScope{Code: "x", Scope: bson.D{{Key: "x", Value: 1}}}},
			{Key: "int32", Value: int32(-7)}, {Key: "ts", Value: bson.Timestamp{T: 1700000000, I: 3}}, {Key: "int64", Value: int64(math.MaxInt64)},
			{Key: "decimal", Value: d("12345678901234567890.123456789")}, {Key: "minkey", Value: bson.MinKey{}}, {Key: "maxkey", Value: bson.MaxKey{}},
			{Key: "varies", Value: 1}, {Key: "num", Value: int32(1)},
		},
		bson.D{
			{Key: "_id", Value: 2}, {Key: "double", Value: math.NaN()}, {Key: "int32", Value: int32(math.MinInt32)},
			{Key: "date", Value: bson.NewDateTimeFromTime(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))},
			{Key: "decimal", Value: d("-Infinity")}, {Key: "varies", Value: "text"}, {Key: "num", Value: 2.5},
		},
		bson.D{
			{Key: "_id", Value: 3}, {Key: "double", Value: math.Inf(-1)}, {Key: "varies", Value: bson.D{{Key: "x", Value: nil}}},
			{Key: "num", Value: int64(1 << 40)}, {Key: "array", Value: bson.A{}},
		},
	}
}

func TestSourceConformance(t *testing.T) {
	t.Run("fields", func(t *testing.T) { sourceConformance(t, "") })
	t.Run("packed", func(t *testing.T) { sourceConformance(t, `, "packed": true`) })
}

// sourceConformance runs the suite with config's extra settings.
func sourceConformance(t *testing.T, extra string) {
	db := testDB(t)
	var users, events, mixed, orders []any
	day := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= 120; i++ {
		users = append(users, bson.D{{Key: "_id", Value: i}, {Key: "name", Value: fmt.Sprint("u", i)}, {Key: "joined", Value: day.AddDate(0, 0, i)}})
	}
	for i := 1; i <= 90; i++ {
		events = append(events, bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "at", Value: day.Add(time.Duration(i) * time.Minute)}, {Key: "body", Value: bson.D{{Key: "n", Value: i}}}})
	}
	// _ids of every type, which BSON orders by type and then value.
	for i := range 20 {
		mixed = append(mixed,
			bson.D{{Key: "_id", Value: i}, {Key: "v", Value: i}},
			bson.D{{Key: "_id", Value: fmt.Sprintf("s%02d", i)}, {Key: "v", Value: i}},
			bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "v", Value: i}},
			bson.D{{Key: "_id", Value: float64(i) + 0.5}, {Key: "v", Value: i}},
			bson.D{{Key: "_id", Value: bson.D{{Key: "k", Value: i}}}, {Key: "v", Value: i}},
		)
	}
	for i := 1; i <= 70; i++ {
		orders = append(orders, bson.D{{Key: "_id", Value: int64(i)}, {Key: "total", Value: float64(i) * 1.25}})
	}
	insert(t, db.Collection("users"), users...)
	insert(t, db.Collection("events"), events...)
	insert(t, db.Collection("mixed"), mixed...)
	insert(t, db.Collection("kinds"), kinds()...)
	insert(t, db.Collection("orders"), orders...)
	cfg := func(uri string) []byte { return sourceConfig(uri, db, `, "chunkDocuments": 25`+extra) }
	conformance.SourceSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/source-mongodb")),
		Config:         cfg(uri()),
		BadCredentials: cfg(badPassword(t)),
		Unreachable:    cfg(closedPort),
		StopsAtUntil:   true,
		AddRecords: func(ctx context.Context, stream string) (int, error) {
			switch stream {
			case "events":
				var docs []any
				for i := range 10 {
					docs = append(docs, bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "at", Value: day.AddDate(1, 0, i)}, {Key: "body", Value: bson.D{{Key: "n", Value: i}}}})
				}
				_, err := db.Collection("events").InsertMany(ctx, docs)
				return 10, err
			case "orders":
				// Each call adds new orders, as a continuous read is restarted.
				var last struct {
					ID int64 `bson:"_id"`
				}
				err := db.Collection("orders").FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})).Decode(&last)
				if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
					return 0, err
				}
				var docs []any
				for i := range 5 {
					docs = append(docs, bson.D{{Key: "_id", Value: last.ID + 1 + int64(i)}, {Key: "total", Value: 2.5}})
				}
				_, err = db.Collection("orders").InsertMany(ctx, docs)
				return 5, err
			}
			return 0, fmt.Errorf("can't add records to %s", stream)
		},
		Streams: []conformance.Stream{
			{Name: "users", Mode: source.FullRefresh},
			{Name: "mixed", Mode: source.FullRefresh},
			{Name: "events", Mode: source.Incremental, Cursor: "at"},
			{Name: "kinds", Mode: source.FullRefresh},
			{Name: "orders", Mode: source.CDC},
		},
	}.Test(t)
}

// readCollection is a collection's documents as the destination suite reads
// them: BSON's types as Go's.
func readCollection(ctx context.Context, coll *mongo.Collection) ([]map[string]any, error) {
	cur, err := coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	var docs []bson.D
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]map[string]any, len(docs))
	for i, d := range docs {
		out[i] = goValue(d).(map[string]any)
	}
	return out, nil
}

func goValue(v any) any {
	switch v := v.(type) {
	case bson.D:
		m := map[string]any{}
		for _, e := range v {
			m[e.Key] = goValue(e.Value)
		}
		return m
	case bson.A:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = goValue(x)
		}
		return out
	case bson.DateTime:
		return v.Time().UTC()
	case bson.Binary:
		return v.Data
	case bson.Decimal128:
		return v.String()
	}
	return v
}

func TestDestinationConformance(t *testing.T) {
	c := testClient(t)
	cfg := func(uri string) []byte { return fmt.Appendf(nil, `{"uri": %q, "database": "openetl"}`, uri) }
	conformance.DestinationSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/destination-mongodb")),
		Config:         cfg(uri()),
		BadCredentials: cfg(badPassword(t)),
		Unreachable:    cfg(closedPort),
		// A field takes a value of any type.
		Untyped: true,
		LossyTypes: map[string]string{
			"timestamp_tz": "BSON dates keep milliseconds",
			"timestamp":    "BSON dates keep milliseconds",
		},
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			return readCollection(ctx, c.Database(namespace).Collection(stream))
		},
		Drop: func(ctx context.Context, namespace string) error { return c.Database(namespace).Drop(ctx) },
		NotNull: func(ctx context.Context, namespace, stream, column string) error {
			return c.Database(namespace).RunCommand(ctx, bson.D{
				{Key: "collMod", Value: stream},
				{Key: "validator", Value: bson.D{{Key: "$jsonSchema", Value: bson.D{{Key: "required", Value: bson.A{column}}}}}},
			}).Err()
		},
	}.Test(t)
}
