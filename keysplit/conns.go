package keysplit

import (
	"context"
	"database/sql"
)

// Conns holds at most as many of a read's connections at a time as Held has
// room for.
type Conns struct {
	DB   *sql.DB
	Held chan struct{}
	// Failed is what Get returns for a connection it couldn't make; nil
	// returns the error as it is.
	Failed func(error) error
}

func (p *Conns) Get(ctx context.Context) (*sql.Conn, error) {
	select {
	case p.Held <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c, err := p.DB.Conn(ctx)
	if err != nil {
		<-p.Held
		if p.Failed != nil {
			err = p.Failed(err)
		}
		return nil, err
	}
	return c, nil
}

// TryGet returns nil if the read holds as many as it may, or can't have
// more.
func (p *Conns) TryGet(ctx context.Context) *sql.Conn {
	select {
	case p.Held <- struct{}{}:
	default:
		return nil
	}
	c, err := p.DB.Conn(ctx)
	if err != nil {
		<-p.Held
		return nil
	}
	return c
}

func (p *Conns) Put(c *sql.Conn) {
	_ = c.Close()
	<-p.Held
}
