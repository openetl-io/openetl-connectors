//go:build integration

package kinesis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/unsuitable001/openetl/sdk/go/conformance"
)

const closedPort = "http://127.0.0.1:1"

func TestSourceConformance(t *testing.T) {
	a, b := uniq("conf_a"), uniq("conf_b")
	createStream(t, a, 2)
	createStream(t, b, 1)
	for i := range 4 {
		putJSON(t, a, i*75, 75)
	}
	putJSON(t, b, 0, 40)
	conformance.SourceSuite{
		Launch:      conformance.Exec(conformance.Build(t, "../cmd/source-kinesis")),
		Config:      config(t, map[string]any{"streams": []string{a, b}}),
		Unreachable: config(t, map[string]any{"streams": []string{a, b}, "endpoint": closedPort}),
	}.Test(t)
}

func TestDestinationConformance(t *testing.T) {
	cl := client(t)
	conformance.DestinationSuite{
		Launch:      conformance.Exec(conformance.Build(t, "../cmd/destination-kinesis")),
		Config:      config(t, nil),
		Unreachable: config(t, map[string]any{"endpoint": closedPort}),
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			recs, err := readAll(ctx, cl, namespace+"."+stream)
			if err != nil {
				return nil, err
			}
			var out []map[string]any
			for _, r := range recs {
				d := json.NewDecoder(bytes.NewReader(r.Data))
				d.UseNumber()
				var m map[string]any
				if err := d.Decode(&m); err != nil {
					return nil, fmt.Errorf("data %q: %w", r.Data, err)
				}
				out = append(out, m)
			}
			return out, nil
		},
		Drop: func(ctx context.Context, namespace string) error {
			for _, s := range []string{"alpha", "beta"} {
				_, _ = cl.DeleteStream(ctx, &awskinesis.DeleteStreamInput{StreamName: aws.String(namespace + "." + s)})
			}
			return nil
		},
		Settle: 5 * time.Second,
	}.Test(t)
}

// readAll reads every record of stream; none if it doesn't exist.
func readAll(ctx context.Context, cl *awskinesis.Client, stream string) ([]types.Record, error) {
	shards, err := cl.ListShards(ctx, &awskinesis.ListShardsInput{StreamName: aws.String(stream)})
	if _, ok := errors.AsType[*types.ResourceNotFoundException](err); ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []types.Record
	for _, sh := range shards.Shards {
		it, err := cl.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{StreamName: aws.String(stream), ShardId: sh.ShardId,
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon})
		if err != nil {
			return nil, err
		}
		for next := it.ShardIterator; next != nil; {
			res, err := cl.GetRecords(ctx, &awskinesis.GetRecordsInput{ShardIterator: next})
			if err != nil {
				return nil, err
			}
			out = append(out, res.Records...)
			if len(res.Records) == 0 && aws.ToInt64(res.MillisBehindLatest) == 0 {
				break
			}
			next = res.NextShardIterator
		}
	}
	return out, nil
}
