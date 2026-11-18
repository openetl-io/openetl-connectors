package oracle

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openetl-io/openetl/sdk/go/source"
)

var _ source.RetentionReporter = (*Source)(nil)

// Retention says, without reading, how much redo is behind the states'
// earliest position, and how old it is. Oracle keeps archived logs until
// RMAN or the recovery area's space deletes them, which the database doesn't
// schedule: once the redo the position needs is gone, nothing is left;
// otherwise how long is left isn't known. States without a position, or of
// another database, say nothing.
func (s *Source) Retention(ctx context.Context, _ string, states map[string][]byte, splits map[string][]source.Split) ([]source.Progress, error) {
	var positions []cdcState
	take := func(b []byte) {
		var st cdcState
		if json.Unmarshal(b, &st) == nil && st.DB != "" && st.Snapshot == nil {
			positions = append(positions, st)
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
	if len(positions) == 0 {
		return nil, nil
	}
	db, err := open(ctx, s.cfg.DSN.Reveal(), 1)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var id string
	if err := db.QueryRowContext(ctx, `SELECT dbid || ':' || resetlogs_change# FROM v$database`).Scan(&id); err != nil {
		return nil, classify(fmt.Errorf("oracle: read the database's identity: %w", err))
	}
	first := earliest(positions, id)
	if first == nil {
		return nil, nil
	}
	logs, err := redoLogs(ctx, db, first.From)
	if err != nil {
		return nil, err
	}
	var p source.Progress
	if covered(logs, first.From) != nil {
		p.RetentionLeft = new(time.Duration)
		return []source.Progress{p}, nil
	}
	var behind uint64
	if err := db.QueryRowContext(ctx, `SELECT nvl(sum(blocks * block_size), 0) FROM v$archived_log a, v$database d
	  WHERE a.next_change# > :1 AND a.status = 'A' AND a.standby_dest = 'NO' AND a.dest_id = (
	    SELECT min(dest_id) FROM v$archived_log WHERE next_change# > :2 AND status = 'A')
	  AND a.resetlogs_change# = d.resetlogs_change#`, first.From, first.From).Scan(&behind); err != nil {
		return nil, classify(fmt.Errorf("oracle: size the archived redo: %w", err))
	}
	p.LagBytes = &behind
	if first.At != nil && behind > 0 {
		p.Lag = new(max(time.Since(*first.At), 0))
	}
	return []source.Progress{p}, nil
}

// earliest is the position of database db that mining starts from
// earliest; nil if none is of db.
func earliest(positions []cdcState, db string) *cdcState {
	var first *cdcState
	for i, st := range positions {
		if st.DB == db && (first == nil || st.From < first.From) {
			first = &positions[i]
		}
	}
	return first
}
