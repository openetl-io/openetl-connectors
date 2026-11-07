//go:build integration

package kinesis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/openetl-io/openetl/sdk/go/conformance"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
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

// Over the protocol, the connector declares splits and reads them; a shard's
// children join with its completion.
func TestSourceSplitsOverTheProtocol(t *testing.T) {
	name := uniq("conf_splits")
	createStream(t, name, 1)
	putJSON(t, name, 0, 50)
	const parent = "shardId-000000000000"
	split(t, name, parent)
	putJSON(t, name, 50, 50)
	launch := conformance.Exec(conformance.Build(t, "../cmd/source-kinesis"))
	p, err := launch(t.Context(), config(t, map[string]any{"streams": []string{name}}))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	spec, err := protocolv1.NewConnectorServiceClient(p.Conn()).Spec(t.Context(), &protocolv1.SpecRequest{Host: conformance.Host})
	if err != nil {
		t.Fatal(err)
	}
	if !spec.GetSource().GetSplits() || spec.GetSource().GetResplit() {
		t.Errorf("capabilities %v", spec.GetSource())
	}
	cl := protocolv1.NewSourceServiceClient(p.Conn())
	stream := &protocolv1.ConfiguredStream{Name: name, SyncMode: protocolv1.SyncMode_SYNC_MODE_STREAM}
	res, err := cl.Splits(t.Context(), &protocolv1.SplitsRequest{Config: p.Config(), Stream: stream})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.GetSplits()) != 1 || res.GetSplits()[0].GetId() != parent {
		t.Fatalf("splits %v", res.GetSplits())
	}
	stream.Splits = res.GetSplits()
	rs, err := cl.Read(t.Context(), &protocolv1.ReadRequest{Config: p.Config(), Pipeline: "p", Lanes: 3, Streams: []*protocolv1.ConfiguredStream{stream}})
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	completed := map[string]bool{}
	var added []*protocolv1.Split
	for {
		m, err := rs.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch msg := m.GetMsg().(type) {
		case *protocolv1.ReadResponse_Batch:
			if msg.Batch.GetSplit() == "" {
				t.Errorf("a batch of %q", msg.Batch.GetSplit())
			}
			rows += len(msg.Batch.GetRows().GetRows())
		case *protocolv1.ReadResponse_Complete:
			completed[msg.Complete.GetSplit()] = true
			added = append(added, msg.Complete.GetAdded()...)
		}
	}
	if rows != 100 || len(completed) != 3 || len(added) != 2 {
		t.Errorf("%d rows, completed %v, added %v", rows, completed, added)
	}
}

func TestDestinationConformance(t *testing.T) {
	cl := client(t)
	conformance.DestinationSuite{
		Launch:      conformance.Exec(conformance.Build(t, "../cmd/destination-kinesis")),
		Config:      config(t, nil),
		Unreachable: config(t, map[string]any{"endpoint": closedPort}),
		// JSON records, which keep no types.
		Untyped: true,
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
