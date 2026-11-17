//go:build integration

package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/openetl-io/openetl/sdk/go/conformance"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

var noBroker = map[string]any{"brokers": []string{"127.0.0.1:1"}}

func TestSourceConformance(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	sourceSuite(t, cl, func(extra map[string]any) []byte { return config(t, extra) }, nil)
}

// Over SASL_SSL, with SCRAM-SHA-512, and with a wrong password for bad
// credentials.
func TestSourceConformanceSASL(t *testing.T) {
	sourceSuite(t, saslAdmin(t), func(extra map[string]any) []byte {
		return saslConfig(t, "scram-sha-512", "scram", scramPassword, extra)
	}, saslConfig(t, "scram-sha-512", "scram", "wrong", nil))
}

// sourceSuite runs the source's suite on two topics that cl makes, with
// configs that cfg makes, and bad, if not nil, for bad credentials.
func sourceSuite(t *testing.T, cl *kgo.Client, cfg func(map[string]any) []byte, bad []byte) {
	a, b := uniq("conf_a"), uniq("conf_b")
	adm := kadm.NewClient(cl)
	for topic, n := range map[string]int32{a: 3, b: 1} {
		if _, err := adm.CreateTopic(t.Context(), n, 1, nil, topic); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 6 {
		if err := produceWith(t.Context(), cl, a, i*50, 50); err != nil {
			t.Fatal(err)
		}
	}
	if err := produceWith(t.Context(), cl, b, 0, 30); err != nil {
		t.Fatal(err)
	}
	topics := []string{a, b}
	if bad != nil {
		var m map[string]any
		if err := json.Unmarshal(bad, &m); err != nil {
			t.Fatal(err)
		}
		m["topics"] = topics
		bad, _ = json.Marshal(m)
	}
	next := map[string]int{a: 300, b: 30}
	conformance.SourceSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/source-kafka")),
		Config:         cfg(map[string]any{"topics": topics}),
		BadCredentials: bad,
		Unreachable:    cfg(map[string]any{"topics": topics, "brokers": noBroker["brokers"]}),
		AddRecords: func(ctx context.Context, topic string) (int, error) {
			from := next[topic]
			next[topic] += 7
			return 7, produceWith(ctx, cl, topic, from, 7)
		},
		StopsAtUntil: true,
	}.Test(t)
}

// produceWith writes n JSON records, ids from..from+n-1, with cl.
func produceWith(ctx context.Context, cl *kgo.Client, topic string, from, n int) error {
	var recs []*kgo.Record
	for i := from; i < from+n; i++ {
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte("k" + strconv.Itoa(i)), Value: []byte(`{"id":` + strconv.Itoa(i) + `,"name":"n"}`)})
	}
	return cl.ProduceSync(ctx, recs...).FirstErr()
}

// Over the protocol, the connector declares splits, reads them, and commits
// their offsets to the group.
func TestSourceSplitsOverTheProtocol(t *testing.T) {
	topic, group := uniq("conf_splits"), uniq("group")
	createTopic(t, topic, 3)
	produce(t, topic, 0, 90)
	launch := conformance.Exec(conformance.Build(t, "../cmd/source-kafka"))
	p, err := launch(t.Context(), config(t, map[string]any{"topics": []string{topic}, "group": group}))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	spec, err := protocolv1.NewConnectorServiceClient(p.Conn()).Spec(t.Context(), &protocolv1.SpecRequest{Host: conformance.Host})
	if err != nil {
		t.Fatal(err)
	}
	if !spec.GetSource().GetSplits() || spec.GetSource().GetResplit() || !spec.GetSource().GetAcknowledges() {
		t.Errorf("capabilities %v", spec.GetSource())
	}
	cl := protocolv1.NewSourceServiceClient(p.Conn())
	stream := &protocolv1.ConfiguredStream{Name: topic, SyncMode: protocolv1.SyncMode_SYNC_MODE_STREAM}
	res, err := cl.Splits(t.Context(), &protocolv1.SplitsRequest{Config: p.Config(), Stream: stream})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.GetSplits()) != 3 {
		t.Fatalf("splits %v", res.GetSplits())
	}
	stream.Splits = res.GetSplits()
	rs, err := cl.Read(t.Context(), &protocolv1.ReadRequest{Config: p.Config(), Pipeline: "p", Lanes: 3, Streams: []*protocolv1.ConfiguredStream{stream}})
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	ended := map[string][]byte{}
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
			ended[msg.Complete.GetSplit()] = msg.Complete.GetNext()
		}
	}
	if rows != 90 || len(ended) != 3 {
		t.Errorf("%d rows, completed %v", rows, ended)
	}
	done := &protocolv1.Splits{}
	var sum int64
	for id, next := range ended {
		done.Splits = append(done.Splits, &protocolv1.Split{Id: id, State: next})
		var st splitState
		if err := json.Unmarshal(next, &st); err != nil {
			t.Fatal(err)
		}
		sum += st.Offset
	}
	if sum != 90 {
		t.Errorf("the splits' offsets add to %d", sum)
	}
	if _, err := cl.Committed(t.Context(), &protocolv1.CommittedRequest{Config: p.Config(), Splits: map[string]*protocolv1.Splits{topic: done}}); err != nil {
		t.Fatal(err)
	}
	offsets := groupOffsets(t, group, topic)
	want := map[int32]int64{}
	for id, next := range ended {
		pn, err := parsePartition(id)
		if err != nil {
			t.Fatal(err)
		}
		var st splitState
		if err := json.Unmarshal(next, &st); err != nil {
			t.Fatal(err)
		}
		want[pn] = st.Offset
	}
	if !maps.Equal(offsets, want) {
		t.Errorf("the group's offsets %v, want %v", offsets, want)
	}
}

func TestDestinationConformance(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	destinationSuite(t, cl, config(t, nil), nil)
}

// Over SASL_SSL, with PLAIN, and with a wrong password for bad credentials.
func TestDestinationConformanceSASL(t *testing.T) {
	destinationSuite(t, saslAdmin(t), saslConfig(t, "plain", "admin", adminPassword, nil), saslConfig(t, "plain", "admin", "wrong", nil))
}

// destinationSuite runs the destination's suite with cfg, and bad, if not
// nil, for bad credentials, reading what it wrote with cl.
func destinationSuite(t *testing.T, cl *kgo.Client, cfg, bad []byte) {
	var m map[string]any
	if err := json.Unmarshal(cfg, &m); err != nil {
		t.Fatal(err)
	}
	m["brokers"] = noBroker["brokers"]
	unreachable, _ := json.Marshal(m)
	conformance.DestinationSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/destination-kafka")),
		Config:         cfg,
		BadCredentials: bad,
		Unreachable:    unreachable,
		// Partitions show a transaction once its coordinator has written
		// their markers, just after the commit returns.
		Settle: 10 * time.Second,
		// JSON records, which keep no types.
		Untyped: true,
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
			_, err := kadm.NewClient(cl).DeleteTopics(ctx, namespace+".alpha", namespace+".beta", namespace+".types")
			return err
		},
	}.Test(t)
}

// readCommitted reads every committed record of topic, with a client made
// as cl is; none if it doesn't exist.
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
	consumer, err := kgo.NewClient(append(cl.Opts(), kgo.ConsumePartitions(starts),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())...)
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
