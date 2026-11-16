//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// statuses is each item's status and reason, as "status reason".
func statuses(r check.Report) map[string]string {
	out := map[string]string{}
	for _, it := range r.Items {
		s := it.Status.String()
		if it.Failure != nil {
			s += " " + it.Failure.Reason
		}
		out[it.Name] = s
	}
	return out
}

func expectItems(t *testing.T, what string, r check.Report, want map[string]string) {
	t.Helper()
	got := statuses(r)
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s: %s is %q, want %q", what, name, got[name], w)
		}
	}
	if t.Failed() {
		for _, it := range r.Items {
			it = it.Rendered()
			t.Logf("%s %s: %s: %s", it.Status, it.Name, it.Title, it.Message)
		}
	}
}

// Each thing a user lacks is an item of its own, with its reason, and the
// lists say what a pipeline needs of each stream.
func TestSourceChecks(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()
	insert(t, db.Collection("a"), bson.D{{Key: "_id", Value: 1}, {Key: "at", Value: 1}})
	insert(t, db.Collection("b"), bson.D{{Key: "_id", Value: 1}})
	if _, err := db.Collection("b").Indexes().CreateOne(ctx, indexOn("at", false)); err != nil {
		t.Fatal(err)
	}
	if err := db.RunCommand(ctx, bson.D{{Key: "collMod", Value: "b"}, {Key: "changeStreamPreAndPostImages", Value: bson.D{{Key: "enabled", Value: true}}}}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateView(ctx, "v", "a", mongo.Pipeline{}); err != nil {
		t.Fatal(err)
	}
	if err := db.RunCommand(ctx, bson.D{{Key: "create", Value: "ts"}, {Key: "timeseries", Value: bson.D{{Key: "timeField", Value: "t"}}}}).Err(); err != nil {
		t.Fatal(err)
	}

	s := newTestSource(t, db, "")
	expectItems(t, "the owner's check", check.Run(ctx, s.Checks()), map[string]string{
		"net.dns": "passed", "net.connect": "passed", "db.connect": "passed", "db.read": "passed",
		"cdc.replica-set": "passed", "cdc.watch": "passed", "cdc.images": "warning pre_and_post_images_off", "cdc.oplog": "passed",
	})
	pf := check.Run(ctx, s.Preflight(source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{
		{Name: "a", SyncMode: source.CDC}, {Name: "a", SyncMode: source.Incremental, Cursor: "at"},
		{Name: "b", SyncMode: source.Incremental, Cursor: "at"}, {Name: "b", SyncMode: source.Incremental},
		{Name: "gone"}, {Name: "v"}, {Name: "ts"},
	}}))
	expectItems(t, "the owner's preflight", pf, map[string]string{
		"stream.a.read": "passed", "stream.a.read-2": "passed", "stream.a.cursor": "warning cursor_not_indexed",
		"stream.b.read": "passed", "stream.b.cursor": "passed", "stream.b.read-2": "passed", "stream.b.cursor-2": "failed config_invalid",
		"stream.gone.read": "failed not_found", "stream.v.read": "failed collection_unsupported", "stream.ts.read": "failed collection_unsupported",
		"cdc.replica-set": "passed", "cdc.watch": "passed", "cdc.images": "warning pre_and_post_images_off", "cdc.oplog": "passed",
	})

	// read on the database, and nothing on local.
	reader := userURI(t, "reader_"+db.Name(), bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: db.Name()}}})
	s, err := NewSource(sourceConfig(reader, db, `, "collections": ["a", "v", "nope"]`))
	if err != nil {
		t.Fatal(err)
	}
	expectItems(t, "a reader's check", check.Run(ctx, s.Checks()), map[string]string{
		"db.connect": "passed", "collections.found": "failed not_found", "db.read": "passed",
		"cdc.watch": "passed", "cdc.oplog": "warning privilege_missing",
	})
	s, err = NewSource(sourceConfig(reader, db, `, "collections": ["a", "v", "ts"]`))
	if err != nil {
		t.Fatal(err)
	}
	expectItems(t, "a view and a time series named", check.Run(ctx, s.Checks()), map[string]string{"collections.found": "failed config_invalid"})
	// With a long gap between syncs, a fresh server's oplog is too short.
	pf = check.Run(ctx, s.Preflight(source.PreflightRequest{Pipeline: "p", Interval: 1 << 62, Streams: []source.ConfiguredStream{{Name: "b", SyncMode: source.CDC}}}))
	expectItems(t, "a reader's preflight", pf, map[string]string{"cdc.watch": "passed", "cdc.images": "passed", "cdc.oplog": "warning privilege_missing"})
	pf = check.Run(ctx, newTestSource(t, db, "").Preflight(source.PreflightRequest{Pipeline: "p", Interval: 1 << 62, Streams: []source.ConfiguredStream{{Name: "b", SyncMode: source.CDC}}}))
	expectItems(t, "a long interval", pf, map[string]string{"cdc.oplog": "warning oplog_window_short"})

	// find on a alone, which reads a but can't list or watch the rest.
	role := "role_" + db.Name()
	admin := testClient(t).Database("admin")
	if err := admin.RunCommand(ctx, bson.D{{Key: "createRole", Value: role}, {Key: "roles", Value: bson.A{}}, {Key: "privileges", Value: bson.A{
		bson.D{{Key: "resource", Value: bson.D{{Key: "db", Value: db.Name()}, {Key: "collection", Value: ""}}}, {Key: "actions", Value: bson.A{"listCollections"}}},
		bson.D{{Key: "resource", Value: bson.D{{Key: "db", Value: db.Name()}, {Key: "collection", Value: "a"}}}, {Key: "actions", Value: bson.A{"find", "listIndexes"}}},
	}}}).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.RunCommand(context.Background(), bson.D{{Key: "dropRole", Value: role}}).Err() })
	narrow := userURI(t, "narrow_"+db.Name(), bson.A{bson.D{{Key: "role", Value: role}, {Key: "db", Value: "admin"}}})
	s, err = NewSource(sourceConfig(narrow, db, ""))
	if err != nil {
		t.Fatal(err)
	}
	r := check.Run(ctx, s.Checks())
	expectItems(t, "a narrow role's check", r, map[string]string{"db.read": "failed privilege_missing", "cdc.watch": "warning privilege_missing"})
	for _, it := range r.Items {
		if it = it.Rendered(); it.Name == "db.read" && (!strings.Contains(it.Message, db.Name()+".b") || strings.Contains(it.Message, db.Name()+".a")) {
			t.Errorf("db.read: %s", it.Message)
		}
	}
	pf = check.Run(ctx, s.Preflight(source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{
		{Name: "a", SyncMode: source.CDC}, {Name: "b"},
	}}))
	expectItems(t, "a narrow role's preflight", pf, map[string]string{
		"stream.a.read": "passed", "stream.b.read": "failed privilege_missing", "cdc.watch": "failed privilege_missing",
	})

	for config, want := range map[string]map[string]string{
		badPassword(t): {"db.connect": "failed credentials_rejected", "db.read": "skipped", "cdc.oplog": "skipped"},
		closedPort:     {"net.connect": "failed connection_refused", "db.connect": "skipped", "db.read": "skipped"},
	} {
		s, err := NewSource(sourceConfig(config, db, ""))
		if err != nil {
			t.Fatal(err)
		}
		expectItems(t, config, check.Run(ctx, s.Checks()), want)
	}
}

func indexOn(field string, unique bool) mongo.IndexModel {
	return mongo.IndexModel{Keys: bson.D{{Key: field, Value: 1}}, Options: options.Index().SetUnique(unique)}
}

// The destination's check fails a user who can only read, and its
// preflight each collection that would refuse what its stream writes.
func TestDestinationChecks(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()
	reader := userURI(t, "reader_"+db.Name(), bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: db.Name()}}})
	writer := userURI(t, "writer_"+db.Name(), bson.A{bson.D{{Key: "role", Value: "readWrite"}, {Key: "db", Value: db.Name()}}})
	dest := func(uri, extra string) *Destination {
		d, err := NewDestination(fmt.Appendf(nil, `{"uri": %q, "database": %q%s}`, uri, db.Name(), extra))
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	expectItems(t, "a reader's check", check.Run(ctx, dest(reader, "").Checks()), map[string]string{"db.connect": "passed", "db.write": "failed privilege_missing"})
	expectItems(t, "a writer's check", check.Run(ctx, dest(writer, "").Checks()), map[string]string{"db.connect": "passed", "db.write": "passed"})

	create := func(name string, opts ...bson.E) {
		if err := db.RunCommand(ctx, append(bson.D{{Key: "create", Value: name}}, opts...)).Err(); err != nil {
			t.Fatal(err)
		}
	}
	validator := func(js bson.D) bson.E {
		return bson.E{Key: "validator", Value: bson.D{{Key: "$jsonSchema", Value: js}}}
	}
	create("req", validator(bson.D{{Key: "required", Value: bson.A{"must"}}}))
	create("closed", validator(bson.D{{Key: "additionalProperties", Value: false}, {Key: "properties", Value: bson.D{{Key: "_id", Value: bson.D{}}, {Key: "id", Value: bson.D{}}}}}))
	create("typed", validator(bson.D{{Key: "properties", Value: bson.D{{Key: "n", Value: bson.D{{Key: "bsonType", Value: "string"}}}}}}))
	create("uniq")
	for _, f := range []string{"id", "email", "sku"} {
		if _, err := db.Collection("uniq").Indexes().CreateOne(ctx, indexOn(f, true)); err != nil {
			t.Fatal(err)
		}
	}
	insert(t, db.Collection("oids"), bson.D{{Key: "_id", Value: bson.NewObjectID()}})
	insert(t, db.Collection("hex"), bson.D{{Key: "_id", Value: bson.NewObjectID().Hex()}})
	if err := db.CreateView(ctx, "view", "oids", mongo.Pipeline{}); err != nil {
		t.Fatal(err)
	}
	create("ts", bson.E{Key: "timeseries", Value: bson.D{{Key: "timeField", Value: "t"}}})
	create("patterns", validator(bson.D{{Key: "additionalProperties", Value: false}, {Key: "properties", Value: bson.D{{Key: "_id", Value: bson.D{}}, {Key: "id", Value: bson.D{}}}},
		{Key: "patternProperties", Value: bson.D{{Key: "^_openetl_", Value: bson.D{}}}}}))

	schema := func(table string, fields ...arrow.Field) *arrow.Schema {
		md := arrow.NewMetadata([]string{tableKey}, []string{table})
		return arrow.NewSchema(append([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, fields...), &md)
	}
	n := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	email := arrow.Field{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true}
	merge := func(name string) destination.Stream {
		return destination.Stream{Name: name, Mode: destination.Merge, PrimaryKey: []string{"id"}}
	}
	widen := merge("widen")
	widen.AllowTypeChanges = true
	req := destination.PreflightRequest{Streams: []destination.Stream{
		merge("new"), merge("req"), merge("closed"), merge("typed"), widen, merge("uniq"),
		{Name: "uniq-append", Mode: destination.Append, PrimaryKey: []string{"id"}},
		merge("oids"), merge("hex"), merge("view"), merge("ts"), merge("patterns"), {Name: "nokey", Mode: destination.Merge},
	}, Schemas: map[string]*arrow.Schema{
		"new": schema("new"), "req": schema("req"), "closed": schema("closed", n), "typed": schema("typed", n), "widen": schema("typed", n),
		"uniq": schema("uniq", email), "uniq-append": schema("uniq", email), "oids": schema("oids"), "hex": schema("hex"), "view": schema("view"), "ts": schema("ts"),
		"patterns": schema("patterns", arrow.Field{Name: "_openetl_synced_at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true}),
	}}
	pf := check.Run(ctx, dest(uri(), "").Preflight(req))
	expectItems(t, "the preflight", pf, map[string]string{
		"db.write":         "passed",
		"stream.new.write": "passed", "stream.new.indexes": "passed", "stream.new.keys": "passed",
		"stream.req.write":    "failed validator_requires_fields",
		"stream.closed.write": "failed validator_refuses_fields",
		"stream.typed.write":  "warning type_change_refused", "stream.widen.write": "passed",
		"stream.uniq.write": "passed", "stream.uniq.indexes": "failed unique_index_conflict",
		"stream.uniq-append.indexes": "failed unique_index_conflict", // sku_1, on a field it lacks
		"stream.oids.keys":           "warning key_mismatch", "stream.hex.keys": "passed",
		"stream.view.write": "failed collection_unsupported", "stream.ts.write": "failed collection_unsupported",
		"stream.patterns.write": "passed",
		"stream.nokey.write":    "failed config_invalid",
	})
	pf = check.Run(ctx, dest(uri(), `, "extendedJSONKeys": true`).Preflight(req))
	expectItems(t, "the preflight with extendedJSONKeys", pf, map[string]string{"stream.oids.keys": "passed", "stream.hex.keys": "warning key_mismatch"})
	// A writer may not widen a validator.
	pf = check.Run(ctx, dest(writer, "").Preflight(req))
	expectItems(t, "a writer's preflight", pf, map[string]string{"db.write": "passed", "stream.widen.write": "failed privilege_missing", "stream.new.write": "passed"})
	pf = check.Run(ctx, dest(reader, "").Preflight(req))
	expectItems(t, "a reader's preflight", pf, map[string]string{"db.write": "failed privilege_missing", "stream.new.write": "skipped"})
}
