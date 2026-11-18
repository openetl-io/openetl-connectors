package sqlserver

import (
	"context"
	"encoding/json"
	"math/big"
	"strconv"
	"time"

	"github.com/openetl-io/openetl/sdk/go/source"
)

var _ source.RetentionReporter = (*Source)(nil)

// Retention says, for each cdc stream past its snapshot, how far behind the
// database its position is, and how long the database keeps the changes
// after it. A stream whose changes are no longer kept, or whose position
// isn't known, has none.
func (s *Source) Retention(ctx context.Context, _ string, states map[string][]byte, _ map[string][]source.Split) ([]source.Progress, error) {
	var out []source.Progress
	for name, b := range states {
		var st cdcState
		if json.Unmarshal(b, &st) != nil || st.Method != s.cfg.CDCMethod || st.Snapshot != nil {
			continue
		}
		p, err := s.progressOf(ctx, name, st)
		if err != nil {
			return nil, err
		}
		if p != nil {
			out = append(out, *p)
		}
	}
	return out, nil
}

// progressOf is a cdc stream's progress, nil if its changes aren't all kept
// or the table isn't there.
func (s *Source) progressOf(ctx context.Context, stream string, st cdcState) (*source.Progress, error) {
	ident := qualified(s.cfg.Schema, stream)
	p := &source.Progress{Stream: stream}
	if st.Method == methodTracking {
		var minValid, current *int64
		var now time.Time
		var period, units int
		var cleanup bool
		err := s.db.QueryRowContext(ctx, `SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@p1)), CHANGE_TRACKING_CURRENT_VERSION(), SYSUTCDATETIME(),
			d.retention_period, d.retention_period_units, d.is_auto_cleanup_on
			FROM sys.change_tracking_databases d WHERE d.database_id = DB_ID()`, ident).Scan(&minValid, &current, &now, &period, &units, &cleanup)
		if err != nil {
			return nil, classify(err)
		}
		pos, err := strconv.ParseInt(st.Position, 10, 64)
		if err != nil || minValid == nil || current == nil || pos < *minValid {
			return nil, nil
		}
		lag := time.Duration(0)
		if pos < *current && st.At != nil {
			lag = max(now.Sub(*st.At), 0)
		}
		p.Lag = &lag
		if cleanup && st.At != nil {
			unit := map[int]time.Duration{1: time.Minute, 2: time.Hour, 3: 24 * time.Hour}[units]
			left := max(st.At.Add(time.Duration(period)*unit).Sub(now), 0)
			p.RetentionLeft = &left
		}
		return p, nil
	}
	var instance string
	err := s.db.QueryRowContext(ctx, "SELECT TOP (1) capture_instance FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(@p1) ORDER BY create_date, capture_instance", ident).Scan(&instance)
	if err != nil {
		return nil, nil
	}
	var low, high []byte
	var at *time.Time
	var behind *int64
	pos := lsnBytes(lsnInt(st.Position))
	err = s.db.QueryRowContext(ctx, `SELECT sys.fn_cdc_get_min_lsn(@p1), sys.fn_cdc_get_max_lsn(), sys.fn_cdc_map_lsn_to_time(@p2),
		DATEDIFF_BIG(millisecond, sys.fn_cdc_map_lsn_to_time(@p2), SYSDATETIME())`, instance, pos).Scan(&low, &high, &at, &behind)
	if err != nil {
		return nil, classify(err)
	}
	// Positions are exclusive: the next change to read is after it.
	if new(big.Int).Add(lsnInt(st.Position), big.NewInt(1)).Cmp(new(big.Int).SetBytes(low)) < 0 {
		return nil, nil
	}
	lag := time.Duration(0)
	if lsnInt(st.Position).Cmp(new(big.Int).SetBytes(high)) < 0 && behind != nil {
		lag = time.Duration(max(*behind, 0)) * time.Millisecond
	}
	p.Lag = &lag
	// The cleanup job's retention, where the user may read msdb's record
	// of it.
	var minutes int64
	if at != nil && behind != nil && s.db.QueryRowContext(ctx, "SELECT retention FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'cleanup'").Scan(&minutes) == nil {
		left := max(time.Duration(minutes)*time.Minute-time.Duration(*behind)*time.Millisecond, 0)
		p.RetentionLeft = &left
	}
	return p, nil
}
