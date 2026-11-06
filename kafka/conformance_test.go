//go:build integration

package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/openetl-io/openetl/sdk/go/conformance"
)

var noBroker = map[string]any{"brokers": []string{"127.0.0.1:1"}}

func TestSourceConformance(t *testing.T) {
	a, b := uniq("conf_a"), uniq("conf_b")
	createTopic(t, a, 3)
	createTopic(t, b, 1)
	for i := range 6 {
		produce(t, a, i*50, 50)
	}
	produce(t, b, 0, 30)
	conformance.SourceSuite{
		Launch:      conformance.Exec(conformance.Build(t, "../cmd/source-kafka")),
		Config:      config(t, map[string]any{"topics": []string{a, b}}),
		Unreachable: config(t, map[string]any{"topics": []string{a, b}, "brokers": noBroker["brokers"]}),
	}.Test(t)
}

func TestDestinationConformance(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	conformance.DestinationSuite{
		Launch:      conformance.Exec(conformance.Build(t, "../cmd/destination-kafka")),
		Config:      config(t, nil),
		Unreachable: config(t, noBroker),
		// Partitions show a transaction once its coordinator has written
		// their markers, just after the commit returns.
		Settle: 10 * time.Second,
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			recs, err := readCommitted(ctx, cl, namespace+"."+stream)
			if err != nil {
				return nil, err
			}
			var out []map[string]any
			for _, r := range recs {
				d := json.NewDecoder(bytes.NewReader(r.Value))
				d.UseNumber()
				var m map[string]any
				if err := d.Decode(&m); err != nil {
					return nil, fmt.Errorf("value %q: %w", r.Value, err)
				}
				out = append(out, m)
			}
			return out, nil
		},
		Drop: func(ctx context.Context, namespace string) error {
			_, err := kadm.NewClient(cl).DeleteTopics(ctx, namespace+".alpha", namespace+".beta")
			return err
		},
	}.Test(t)
}

// readCommitted reads every committed record of topic; none if it doesn't
// exist.
func readCommitted(ctx context.Context, cl *kgo.Client, topic string) ([]*kgo.Record, error) {
	ends, err := kadm.NewClient(cl).ListCommittedOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	starts := map[string]map[int32]kgo.Offset{topic: {}}
	left := map[int32]int64{}
	var lerr error
	ends.Each(func(o kadm.ListedOffset) {
		switch {
		case errors.Is(o.Err, kerr.UnknownTopicOrPartition):
		case o.Err != nil:
			lerr = o.Err
		case o.Offset > 0:
			starts[topic][o.Partition] = kgo.NewOffset().AtStart()
			left[o.Partition] = o.Offset
		}
	})
	if lerr != nil || len(left) == 0 {
		return nil, lerr
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers()), kgo.ConsumePartitions(starts),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())
	if err != nil {
		return nil, err
	}
	defer consumer.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(left) > 0 {
		fs := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil, fmt.Errorf("reading %s: %w; partitions left %v", topic, ctx.Err(), left)
		}
		for _, r := range fs.Records() {
			if r.Offset+1 >= left[r.Partition] {
				delete(left, r.Partition)
			}
			if !r.Attrs.IsControl() {
				out = append(out, r)
			}
		}
	}
	return out, nil
}
