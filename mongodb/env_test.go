//go:build integration

package mongodb

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// uri is the test server's, a single-node replica set with a user.
func uri() string {
	if u := os.Getenv("OPENETL_TEST_MONGODB_URI"); u != "" {
		return u
	}
	return "mongodb://openetl:openetl@127.0.0.1:27017/?directConnection=true"
}

// badPassword is uri() with a password the server rejects.
func badPassword(t *testing.T) string {
	t.Helper()
	u, err := url.Parse(uri())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(u.User.Username(), "not-the-password")
	return u.String()
}

const closedPort = "mongodb://openetl:openetl@127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=2000&connectTimeoutMS=2000"

// testClient is a client of the test server, disconnected when t ends.
func testClient(t *testing.T) *mongo.Client {
	t.Helper()
	c, err := mongo.Connect(options.Client().ApplyURI(uri()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := c.Ping(ctx, nil); err != nil {
		t.Fatalf("the test server at %s: %v", uri(), err)
	}
	return c
}

// testDB is a new database, dropped when t ends.
func testDB(t *testing.T) *mongo.Database {
	t.Helper()
	db := testClient(t).Database("t_" + rand.Text()[:10])
	t.Cleanup(func() { _ = db.Drop(context.Background()) })
	return db
}

func insert(t *testing.T, coll *mongo.Collection, docs ...any) {
	t.Helper()
	if _, err := coll.InsertMany(t.Context(), docs); err != nil {
		t.Fatal(err)
	}
}

func sourceConfig(uri string, db *mongo.Database, extra string) []byte {
	return fmt.Appendf(nil, `{"uri": %q, "database": %q%s}`, uri, db.Name(), extra)
}

func newTestSource(t *testing.T, db *mongo.Database, extra string) *Source {
	t.Helper()
	s, err := NewSource(sourceConfig(uri(), db, extra))
	if err != nil {
		t.Fatal(err)
	}
	return s
}
