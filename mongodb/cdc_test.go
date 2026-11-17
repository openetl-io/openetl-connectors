//go:build integration

package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/openetl-io/openetl-connectors/sourcetest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// waitFor polls cond until it holds, or fails t after d.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(d); !cond(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("within %s: %s", d, what)
		}
	}
}

// follow starts a continuous cdc read of stream, from state, stopped when t
// ends; done has its error.
func follow(t *testing.T, s *Source, stream string, state []byte, columns []string) (*sourcetest.Recorder, <-chan error) {
	t.Helper()
	ctx := t.Context()
	rec := &sourcetest.Recorder{}
	done := make(chan error, 1)
	go func() {
		done <- s.Read(ctx, source.ReadRequest{Continuous: true, Streams: []source.ConfiguredStream{
			{Name: stream, SyncMode: source.CDC, State: state, Columns: columns},
		}}, rec)
		close(done)
	}()
	// t.Context() is canceled before cleanups run.
	t.Cleanup(func() {
		for range done {
		}
	})
	return rec, done
}

func TestCDCContinuous(t *testing.T) {
	db := testDB(t)
	coll := db.Collection("orders")
	var docs []any
	for i := 1; i <= 70; i++ {
		docs = append(docs, bson.D{{Key: "_id", Value: i}, {Key: "total", Value: 1.5}})
	}
	insert(t, coll, docs...)
	s := newTestSource(t, db, `, "chunkDocuments": 25`)
	rec, done := follow(t, s, "orders", nil, nil)
	waitFor(t, 20*time.Second, "the snapshot's 70 rows", func() bool {
		select {
		case err := <-done:
			t.Fatalf("the read ended: %v", err)
		default:
		}
		return len(rec.RowsOf("orders")) >= 70
	})
	insert(t, coll, bson.D{{Key: "_id", Value: 71}, {Key: "total", Value: 2}})
	waitFor(t, 20*time.Second, "the insert", func() bool { return len(rec.RowsOf("orders")) >= 71 })
	rows := rec.RowsOf("orders")
	if got := rows[70]; got["_id"] != intID(71) || got["openetl.op"] != "insert" {
		t.Errorf("the insert's row: %v", got)
	}
	state, _ := rec.Last("orders")
	var st cdcState
	if err := json.Unmarshal(state, &st); err != nil || st.Snapshot != nil {
		t.Errorf("the state after the snapshot: %s: %v", state, err)
	}
}

// changes waits for n rows past the first skip, and returns them.
func changes(t *testing.T, rec *sourcetest.Recorder, done <-chan error, stream string, skip, n int) []map[string]string {
	t.Helper()
	waitFor(t, 20*time.Second, fmt.Sprintf("%d rows of %s", skip+n, stream), func() bool {
		select {
		case err := <-done:
			t.Fatalf("the read ended: %v", err)
		default:
		}
		return len(rec.RowsOf(stream)) >= skip+n
	})
	return rec.RowsOf(stream)[skip:]
}

func checkRows(t *testing.T, got []map[string]string, want ...map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rows %v, want %v", got, want)
	}
	for i, w := range want {
		g := maps.Clone(got[i])
		delete(g, "openetl.seq")
		if !maps.Equal(g, w) {
			t.Errorf("row %d is %v, want %v", i, g, w)
		}
	}
}

func seqOf(t *testing.T, r map[string]string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(r["openetl.seq"], 10)
	if !ok {
		t.Fatalf("a row's seq: %v", r)
	}
	return n
}

func TestCDCChanges(t *testing.T) {
	db := testDB(t)
	coll := db.Collection("c")
	insert(t, coll, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}}, bson.D{{Key: "_id", Value: 2}, {Key: "a", Value: 2}}, bson.D{{Key: "_id", Value: 3}, {Key: "a", Value: 3}})
	s := newTestSource(t, db, "")
	rec, done := follow(t, s, "c", nil, nil)
	changes(t, rec, done, "c", 0, 3)
	ctx := t.Context()
	insert(t, coll, bson.D{{Key: "_id", Value: 4}, {Key: "a", Value: 4}})
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: 10}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: 2}}, bson.D{{Key: "b", Value: "r"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: 3}}); err != nil {
		t.Fatal(err)
	}
	rows := changes(t, rec, done, "c", 3, 4)
	checkRows(t, rows,
		map[string]string{"_id": intID(4), "a": "4", "openetl.op": "insert"},
		map[string]string{"_id": intID(1), "a": "10", "openetl.op": "update"},
		map[string]string{"_id": intID(2), "b": "r", "openetl.op": "update"},
		map[string]string{"_id": intID(3), "openetl.op": "delete"},
	)
	all := rec.RowsOf("c")
	for i := 1; i < len(all); i++ {
		if seqOf(t, all[i-1]).Cmp(seqOf(t, all[i])) > 0 {
			t.Errorf("row %d's seq is below the one before it", i)
		}
	}
	// A change's seq is above the snapshot's.
	if seqOf(t, all[0]).Cmp(seqOf(t, all[3])) >= 0 {
		t.Errorf("a change's seq isn't above the snapshot's")
	}
}

func TestCDCImages(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()
	if err := db.CreateCollection(ctx, "c", options.CreateCollection().SetChangeStreamPreAndPostImages(bson.D{{Key: "enabled", Value: true}})); err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("c")
	insert(t, coll, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}})
	rec, done := follow(t, newTestSource(t, db, ""), "c", nil, nil)
	changes(t, rec, done, "c", 0, 1)
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: 2}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	// The update's post-image, though the document is gone, and the
	// delete's pre-image.
	checkRows(t, changes(t, rec, done, "c", 1, 2),
		map[string]string{"_id": intID(1), "a": "2", "openetl.op": "update"},
		map[string]string{"_id": intID(1), "a": "2", "openetl.op": "delete"},
	)
}

func TestCDCUpdateOfDeleted(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()
	coll := db.Collection("c")
	insert(t, coll, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}})
	s := newTestSource(t, db, "")
	rec := readAll(t, s, source.ConfiguredStream{Name: "c", SyncMode: source.CDC})
	state, _ := rec.Last("c")
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: 2}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	// Without post-images, an update of a document since deleted is left
	// to its delete.
	rec, done := follow(t, s, "c", state, nil)
	checkRows(t, changes(t, rec, done, "c", 0, 1), map[string]string{"_id": intID(1), "openetl.op": "delete"})
}

func TestCDCNewField(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()
	coll := db.Collection("c")
	insert(t, coll, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}})
	s := newTestSource(t, db, "")
	columns := []string{"_id", "a", "openetl.op", "openetl.seq"}
	rec := &sourcetest.Recorder{}
	if err := s.Read(ctx, source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "c", SyncMode: source.CDC, Columns: columns}}}, rec); err != nil {
		t.Fatal(err)
	}
	state, _ := rec.Last("c")

	// A field that a document had and no document has now is left out.
	insert(t, coll, bson.D{{Key: "_id", Value: 2}, {Key: "gone", Value: 1}})
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: 2}}, bson.D{{Key: "$unset", Value: bson.D{{Key: "gone", Value: ""}}}}); err != nil {
		t.Fatal(err)
	}
	rec, done := follow(t, s, "c", state, columns)
	checkRows(t, changes(t, rec, done, "c", 0, 2),
		map[string]string{"_id": intID(2), "openetl.op": "insert"},
		map[string]string{"_id": intID(2), "openetl.op": "update"},
	)

	// A field a document has fails the read, retryably, to declare it: the
	// one left out above too, once a document brings it back.
	insert(t, coll, bson.D{{Key: "_id", Value: 3}, {Key: "gone", Value: 1}})
	select {
	case err := <-done:
		if sourcetest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT || !strings.Contains(err.Error(), `new field "gone"`) {
			t.Errorf("a new field: %s: %v", sourcetest.CodeOf(err), err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("a new field didn't end the read")
	}
}

func TestCDCEnds(t *testing.T) {
	for _, c := range []struct {
		name, want string
		do         func(ctx context.Context, db *mongo.Database) error
	}{
		{"drop", "was dropped", func(ctx context.Context, db *mongo.Database) error { return db.Collection("c").Drop(ctx) }},
		{"rename", "was renamed to d", func(ctx context.Context, db *mongo.Database) error {
			return db.Client().Database("admin").RunCommand(ctx, bson.D{{Key: "renameCollection", Value: db.Name() + ".c"}, {Key: "to", Value: db.Name() + ".d"}}).Err()
		}},
		{"drop the database", "was dropped", func(ctx context.Context, db *mongo.Database) error { return db.Drop(ctx) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			db := testDB(t)
			insert(t, db.Collection("c"), bson.D{{Key: "_id", Value: 1}})
			rec, done := follow(t, newTestSource(t, db, ""), "c", nil, nil)
			changes(t, rec, done, "c", 0, 1)
			if err := c.do(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if sourcetest.CodeOf(err) != protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST || !strings.Contains(err.Error(), c.want) {
					t.Errorf("%s: %s: %v", c.name, sourcetest.CodeOf(err), err)
				}
			case <-time.After(20 * time.Second):
				t.Fatalf("%s didn't end the read", c.name)
			}
		})
	}
}

// The test server's oplog hasn't dropped an entry, so it resumes from any
// time: values_test.go's TestPositionLost checks the error it gives once it
// has.
func TestRetention(t *testing.T) {
	db := testDB(t)
	insert(t, db.Collection("c"), bson.D{{Key: "_id", Value: 1}})
	s := newTestSource(t, db, "")
	// Before the oplog's oldest entry.
	old, _ := json.Marshal(cdcState{Time: bson.Timestamp{T: 1, I: 1}})
	rec := readAll(t, s, source.ConfiguredStream{Name: "c", SyncMode: source.CDC})
	now, _ := rec.Last("c")
	ps, err := s.Retention(t.Context(), "p", map[string][]byte{"c": now, "old": old, "bad": []byte("{")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 1 || ps[0].Stream != "c" || ps[0].Lag == nil || ps[0].RetentionLeft == nil || *ps[0].RetentionLeft <= 0 {
		t.Errorf("Retention = %+v", ps)
	}
}

// A transaction's changes share a cluster time, which the place in a seq
// orders, and a read restarted between them gives the rest the same seqs.
func TestCDCTransactionSeq(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()
	coll := db.Collection("c")
	insert(t, coll, bson.D{{Key: "_id", Value: 0}})
	s := newTestSource(t, db, `, "chunkDocuments": 1`)
	rec := readAll(t, s, source.ConfiguredStream{Name: "c", SyncMode: source.CDC})
	state, _ := rec.Last("c")
	sess, err := db.Client().StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer sess.EndSession(ctx)
	if _, err := sess.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		_, err := coll.InsertMany(ctx, []any{bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "_id", Value: 2}}, bson.D{{Key: "_id", Value: 3}}})
		return nil, err
	}); err != nil {
		t.Fatal(err)
	}
	first, done := follow(t, s, "c", state, nil)
	rows := changes(t, first, done, "c", 0, 3)
	seqs := []*big.Int{seqOf(t, rows[0]), seqOf(t, rows[1]), seqOf(t, rows[2])}
	if seqs[0].Cmp(seqs[1]) >= 0 || seqs[1].Cmp(seqs[2]) >= 0 {
		t.Errorf("a transaction's seqs %v don't rise", seqs)
	}
	// The resume point after the first change.
	var after []byte
	n := 0
	for _, e := range first.Snapshot() {
		n += len(e.Rows)
		if e.Resume && n == 1 {
			after = e.State
		}
	}
	if after == nil {
		t.Fatal("no resume point after the first change")
	}
	again, done := follow(t, s, "c", after, nil)
	rows = changes(t, again, done, "c", 0, 2)
	if seqOf(t, rows[0]).Cmp(seqs[1]) != 0 || seqOf(t, rows[1]).Cmp(seqs[2]) != 0 || rows[0]["_id"] != intID(2) {
		t.Errorf("restarted, rows %v, want seqs %v", rows, seqs[1:])
	}
}
