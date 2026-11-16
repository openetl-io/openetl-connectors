package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"time"

	"github.com/openetl-io/openetl/sdk/go/source"
)

var _ source.RetentionReporter = (*Source)(nil)

// Retention says, without reading, how far the states' earliest binlog
// position is behind the binlog's end, and how much longer the server keeps
// the binlog from there: binlog_expire_logs_seconds after the last
// transaction read committed, at the least, and nothing once its file is
// purged. States without a position, or of another server, say nothing.
func (s *Source) Retention(ctx context.Context, _ string, states map[string][]byte, splits map[string][]source.Split) ([]source.Progress, error) {
	var first *cdcState
	take := func(b []byte) {
		var st cdcState
		if json.Unmarshal(b, &st) == nil && st.File != "" && (first == nil || st.binlogPos.less(first.binlogPos)) {
			first = &st
		}
	}
	for _, b := range states {
		take(b)
	}
	for _, ss := range splits {
		for _, sp := range ss {
			take(sp.State)
		}
	}
	if first == nil {
		return nil, nil
	}
	db, srv, err := open(ctx, s.conn, 1)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	server := "server_id:"
	q := "SELECT @@server_id"
	if !srv.mariadb {
		server, q = "", "SELECT @@server_uuid"
	}
	var id string
	if err := db.QueryRowContext(ctx, q).Scan(&id); err != nil {
		return nil, classify(err)
	}
	if server+id != first.Server {
		return nil, nil
	}
	p, err := binlogRetention(ctx, db, first.binlogPos, deref(first.At))
	if err != nil {
		return nil, err
	}
	if p.LagBytes != nil && *p.LagBytes > 0 && first.At != nil {
		p.Lag = new(max(time.Since(*first.At), 0))
	}
	return []source.Progress{p}, nil
}

// binlogRetention is how many bytes of binlog follow pos, and how long the
// server keeps pos's file at the least: binlog_expire_logs_seconds if it's
// the file being written, otherwise that long from the time of the last
// transaction read, at, since the file was written no earlier; unknown
// without at; zero once the file is purged. Lag is left to the caller.
func binlogRetention(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, pos binlogPos, at time.Time) (source.Progress, error) {
	var p source.Progress
	rows, err := q.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return p, classify(err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return p, classify(err)
	}
	found, last := false, true
	var behind uint64
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return p, classify(err)
		}
		f := binlogPos{File: vals[0].String}
		size, _ := strconv.ParseUint(vals[1].String, 10, 64)
		switch {
		case f.File == pos.File:
			found = true
			behind += size - min(size, uint64(pos.Pos))
		case pos.less(f):
			behind += size
			last = false
		}
	}
	if err := rows.Err(); err != nil {
		return p, classify(err)
	}
	if !found {
		p.RetentionLeft = new(time.Duration(0))
		return p, nil
	}
	p.LagBytes = &behind
	var expire int64
	if err := q.QueryRowContext(ctx, "SELECT @@binlog_expire_logs_seconds").Scan(&expire); err != nil {
		return p, classify(err)
	}
	switch {
	case expire == 0:
	case last:
		// The file being written is kept, and once rotated, kept that long.
		p.RetentionLeft = new(time.Duration(expire) * time.Second)
	case !at.IsZero():
		p.RetentionLeft = new(max(time.Until(at.Add(time.Duration(expire)*time.Second)), 0))
	}
	return p, nil
}
