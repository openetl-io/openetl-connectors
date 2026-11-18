package keysplit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

type fakeDriver struct{}

func (fakeDriver) Connect(context.Context) (driver.Conn, error) { return fakeConn{}, nil }
func (fakeDriver) Driver() driver.Driver                        { return fakeDriver{} }
func (fakeDriver) Open(string) (driver.Conn, error)             { return fakeConn{}, nil }

type fakeConn struct{}

func (fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no statements") }
func (fakeConn) Close() error                        { return nil }
func (fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("no transactions") }

// A pool whose connections are all in use, one of them the caller's, which
// Conns doesn't count, would keep TryGet waiting.
func TestTryGetDoesntWaitForAFullPool(t *testing.T) {
	db := sql.OpenDB(fakeDriver{})
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	held, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	p := &Conns{DB: db, Held: make(chan struct{}, 2)}
	got := make(chan *sql.Conn, 1)
	go func() { got <- p.TryGet(context.Background()) }()
	select {
	case c := <-got:
		if c != nil {
			t.Error("TryGet got a connection past the pool's limit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TryGet waited for the pool")
	}
	if len(p.Held) != 0 {
		t.Errorf("TryGet holds %d", len(p.Held))
	}
	if c := (&Conns{DB: db, Held: make(chan struct{}, 1)}).TryGet(t.Context()); c != nil {
		t.Error("TryGet got a connection past the pool's limit")
	}
	_ = held.Close()
	if c := p.TryGet(t.Context()); c == nil {
		t.Error("TryGet got none from a pool with room")
	} else {
		p.Put(c)
	}
}
