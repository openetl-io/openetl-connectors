package kafka

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/openetl-io/openetl/sdk/go/source"
)

// Retention says, for each topic, how many records its partitions'
// positions are behind, how old the oldest of them is, and how long until
// the topic's retention.ms reaches it, without reading the rest: it fetches
// the record at each position behind, for its timestamp. Retention by size
// isn't counted, and a topic whose config the credentials can't describe
// gets no retention.
func (s *Source) Retention(ctx context.Context, _ string, states map[string][]byte, splits map[string][]source.Split) ([]source.Progress, error) {
	next := map[string]map[int32]int64{}
	for name, b := range states {
		n, err := parseState(b)
		if err != nil {
			return nil, err
		}
		next[name] = n
	}
	for name, ss := range splits {
		n, err := splitOffsets(ss)
		if err != nil {
			return nil, err
		}
		next[name] = n
	}
	if len(next) == 0 {
		return nil, nil
	}
	cl, err := s.cfg.client()
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	topics := slices.Sorted(maps.Keys(next))
	reads := make(map[string]*topicRead, len(topics))
	for _, t := range topics {
		reads[t] = &topicRead{name: t}
	}
	retentions(ctx, adm, reads, topics)
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, classify(fmt.Errorf("kafka: list end offsets: %w", err))
	}
	lags := map[string]uint64{}
	behind := map[string]map[int32]kgo.Offset{}
	ends.Each(func(o kadm.ListedOffset) {
		n, ok := next[o.Topic][o.Partition]
		if o.Err != nil || !ok || o.Offset <= n {
			return
		}
		lags[o.Topic] += uint64(o.Offset - n)
		if behind[o.Topic] == nil {
			behind[o.Topic] = map[int32]kgo.Offset{}
		}
		behind[o.Topic][o.Partition] = kgo.NewOffset().At(n)
	})
	oldest := s.oldest(ctx, behind)
	now := time.Now()
	out := make([]source.Progress, 0, len(topics))
	for _, t := range topics {
		lag, retention := lags[t], reads[t].retention
		p := source.Progress{Stream: t, LagRecords: &lag}
		age, known := time.Duration(0), lag == 0
		if ts, ok := oldest[t]; ok {
			age, known = max(0, now.Sub(ts)), true
		}
		if known {
			p.Lag = &age
			if retention > 0 {
				left := max(0, retention-age)
				p.RetentionLeft = &left
			}
		}
		out = append(out, p)
	}
	return out, nil
}

// oldest is each topic's oldest timestamp of the records at the offsets, by
// fetching them; a partition that doesn't answer within 10 seconds is left
// out.
func (s *Source) oldest(ctx context.Context, at map[string]map[int32]kgo.Offset) map[string]time.Time {
	out := map[string]time.Time{}
	want := 0
	for _, ps := range at {
		want += len(ps)
	}
	if want == 0 {
		return out
	}
	cl, err := s.cfg.client(kgo.ConsumePartitions(at), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(time.Second))
	if err != nil {
		return out
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	seen := map[string]map[int32]bool{}
	for got := 0; got < want && ctx.Err() == nil; {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			if seen[r.Topic][r.Partition] {
				return
			}
			if seen[r.Topic] == nil {
				seen[r.Topic] = map[int32]bool{}
			}
			seen[r.Topic][r.Partition] = true
			got++
			if t, ok := out[r.Topic]; !ok || r.Timestamp.Before(t) {
				out[r.Topic] = r.Timestamp
			}
		})
	}
	return out
}
