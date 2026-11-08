package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// Retention says how far the replication slot the states read through is
// behind the log, and how much more WAL it can hold before
// max_slot_wal_keep_size invalidates it, without reading. States without a
// slot position, and a slot that's gone or invalidated, say nothing: the next
// read reports the position lost.
func (s *Source) Retention(ctx context.Context, _ string, states map[string][]byte, splits map[string][]source.Split) ([]source.Progress, error) {
	slot := ""
	take := func(b []byte) {
		var st cdcState
		if json.Unmarshal(b, &st) == nil && st.Slot != "" && st.LSN != 0 {
			slot = st.Slot
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
	if slot == "" {
		return nil, nil
	}
	conn, err := pgx.ConnectConfig(ctx, s.conn)
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var behind, safe *int64
	var lost bool
	err = conn.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, safe_wal_size,
		coalesce(wal_status = 'lost', false) FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&behind, &safe, &lost)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && lost {
		return nil, nil
	}
	if err != nil {
		return nil, classify(err)
	}
	var p source.Progress
	if behind != nil && *behind >= 0 {
		p.LagBytes = new(uint64(*behind))
	}
	if safe != nil && *safe >= 0 {
		p.RetentionLeftBytes = new(uint64(*safe))
	}
	if p.LagBytes == nil && p.RetentionLeftBytes == nil {
		return nil, nil
	}
	return []source.Progress{p}, nil
}
