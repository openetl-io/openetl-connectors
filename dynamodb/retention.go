package dynamodb

import (
	"context"
	"encoding/json"

	"maps"
	"slices"
	"time"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// Retention says, for each cdc stream, how long ago at most the records
// after its shards' positions were created, and how long until the
// stream's 24 hours reach them, without reading: nothing after a shard's
// position was created before its Since, and a snapshot that runs needs
// every record since its start. Streams read by scans have no position the
// table keeps.
func (s *Source) Retention(_ context.Context, _ string, states map[string][]byte, _ map[string][]source.Split) ([]source.Progress, error) {
	at := now()
	var out []source.Progress
	for _, name := range slices.Sorted(maps.Keys(states)) {
		var st cdcState
		if err := json.Unmarshal(states[name], &st); err != nil || st.Stream == "" {
			// A scan's state, or none.
			continue
		}
		oldest := time.Time{}
		if len(st.Snapshot) > 0 {
			oldest = st.Skip
		}
		for _, sh := range st.Shards {
			if !sh.Done && (oldest.IsZero() || sh.Since.Before(oldest)) {
				oldest = sh.Since
			}
		}
		if oldest.IsZero() {
			continue
		}
		lag := max(0, at.Sub(oldest))
		left := max(0, retention-lag)
		out = append(out, source.Progress{Stream: name, Lag: &lag, RetentionLeft: &left})
	}
	return out, nil
}
