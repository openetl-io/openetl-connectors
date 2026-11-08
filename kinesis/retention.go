package kinesis

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// Retention says, for each stream, how long ago at most the records after
// its shards' positions arrived, and how long until the stream's retention
// period reaches them, without reading: nothing after a position arrived
// before its Since, so the oldest record unread is no older than that.
// Shards read to their end don't count.
func (s *Source) Retention(ctx context.Context, _ string, states map[string][]byte, splits map[string][]source.Split) ([]source.Progress, error) {
	now := time.Now()
	shards := map[string]map[string]shardState{}
	for name, b := range states {
		st, err := parseState(b)
		if err != nil {
			return nil, err
		}
		shards[name] = st
	}
	for name, ss := range splits {
		st, err := splitShards(ss, now)
		if err != nil {
			return nil, err
		}
		shards[name] = st
	}
	var since map[string]time.Time
	for name, st := range shards {
		for _, sh := range st {
			if sh.Done || sh.Since.IsZero() {
				continue
			}
			if since == nil {
				since = map[string]time.Time{}
			}
			if t, ok := since[name]; !ok || sh.Since.Before(t) {
				since[name] = sh.Since
			}
		}
	}
	if since == nil {
		return nil, nil
	}
	cl, err := s.cfg.client(ctx)
	if err != nil {
		return nil, err
	}
	var out []source.Progress
	for _, name := range slices.Sorted(maps.Keys(since)) {
		sum, err := cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
		if err != nil {
			return nil, classify(fmt.Errorf("kinesis: describe stream %s: %w", name, err))
		}
		retention := time.Duration(aws.ToInt32(sum.StreamDescriptionSummary.RetentionPeriodHours)) * time.Hour
		lag := max(0, now.Sub(since[name]))
		left := max(0, retention-lag)
		out = append(out, source.Progress{Stream: name, Lag: &lag, RetentionLeft: &left})
	}
	return out, nil
}
