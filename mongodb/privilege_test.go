//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// userURI is uri() as a new user with roles, dropped when t ends.
func userURI(t *testing.T, name string, roles bson.A) string {
	t.Helper()
	admin := testClient(t).Database("admin")
	if err := admin.RunCommand(t.Context(), bson.D{{Key: "createUser", Value: name}, {Key: "pwd", Value: "pw"}, {Key: "roles", Value: roles}}).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.RunCommand(context.Background(), bson.D{{Key: "dropUser", Value: name}}).Err() })
	u, err := url.Parse(uri())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(name, "pw")
	return u.String()
}

// The README's roles are enough: read on the database, and on local for
// Retention; readWrite on the destination's database.
func TestLeastPrivilege(t *testing.T) {
	db := testDB(t)
	ctx := t.Context()
	insert(t, db.Collection("c"), bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}})
	reader := userURI(t, "reader_"+db.Name(), bson.A{
		bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: db.Name()}},
		bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: "local"}},
	})
	s, err := NewSource(sourceConfig(reader, db, ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Check(ctx); err != nil {
		t.Errorf("Check: %v", err)
	}
	discovered(t, s, "c")
	if _, err := s.Splits(ctx, source.ConfiguredStream{Name: "c", SyncMode: source.FullRefresh}, nil); err != nil {
		t.Errorf("Splits: %v", err)
	}
	rec := readAll(t, s, source.ConfiguredStream{Name: "c", SyncMode: source.CDC})
	state, _ := rec.last("c")
	if ps, err := s.Retention(ctx, "p", map[string][]byte{"c": state}, nil); err != nil || len(ps) != 1 {
		t.Errorf("Retention: %v, %v", ps, err)
	}

	writerURI := userURI(t, "writer_"+db.Name(), bson.A{bson.D{{Key: "role", Value: "readWrite"}, {Key: "db", Value: db.Name()}}})
	d, err := NewDestination(fmt.Appendf(nil, `{"uri": %q, "database": %q}`, writerURI, db.Name()))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Check(ctx); err != nil {
		t.Errorf("destination Check: %v", err)
	}
	streams := []destination.Stream{
		{Name: "m", Mode: destination.Merge, PrimaryKey: []string{"id"}},
		{Name: "o", Mode: destination.Overwrite},
	}
	wr, err := d.Open(ctx, destination.OpenConfig{Pipeline: "p", Generation: 1, Scope: destination.ScopePipeline, Streams: streams})
	if err != nil {
		t.Fatal(err)
	}
	defer wr.Close(ctx)
	w := wr.(*writer)
	if _, err := w.Recover(ctx, 1, destination.Epoch{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ApplySchema(ctx, []destination.StreamSchema{{Name: "m", Schema: rowsBatch(t).Schema()}}); err != nil {
		t.Errorf("ApplySchema: %v", err)
	}
	if err := w.StartReads(ctx, map[string]time.Time{"o": time.Now()}); err != nil {
		t.Fatal(err)
	}
	e := destination.Epoch{Generation: 1, N: 1}
	for _, s := range []string{"m", "o"} {
		b := rowsBatch(t, row{1, "a", "insert", 1})
		if err := w.Write(ctx, e, s, b); err != nil {
			t.Fatal(err)
		}
		b.Release()
	}
	if err := w.EndRead(ctx, e, "o"); err != nil {
		t.Fatal(err)
	}
	staged, err := w.PreCommit(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(ctx, e, []destination.Staged{staged}); err != nil {
		t.Errorf("Commit as readWrite: %v", err)
	}
	if got := docs(t, db.Collection("o")); len(got) != 1 {
		t.Errorf("the overwrite's collection: %v", got)
	}
}
