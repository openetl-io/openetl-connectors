package kinesis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// How long a consumer may take to become active, and how often register
// asks.
const (
	consumerActiveWithin = 5 * time.Minute
	consumerPollEvery    = time.Second
	subscribeEvery       = time.Second
)

// register returns the ARN of the stream's consumer named name, registering
// it if it isn't, once it's active.
func (s *Source) register(ctx context.Context, cl *awskinesis.Client, streamARN, name string) (string, error) {
	failed := func(err error) error {
		return classify(fmt.Errorf("kinesis: consumer %s of %s: %w", name, streamARN, err))
	}
	th := throttle{retry: &s.cfg.Retry}
	deadline := time.Now().Add(consumerActiveWithin)
	// Kinesis refusing to register the consumer for want of room, the last
	// time it was asked.
	var refused error
	for {
		d, err := cl.DescribeStreamConsumer(ctx, &awskinesis.DescribeStreamConsumerInput{StreamARN: aws.String(streamARN), ConsumerName: aws.String(name)})
		switch {
		case throttled(err):
			if err := th.wait(ctx, err); err != nil {
				return "", failed(err)
			}
			continue
		case apiCode(err) == "ResourceNotFoundException":
			_, err = cl.RegisterStreamConsumer(ctx, &awskinesis.RegisterStreamConsumerInput{StreamARN: aws.String(streamARN), ConsumerName: aws.String(name)})
			refused = nil
			switch {
			case apiCode(err) == "LimitExceededException":
				refused = err
			case err != nil && apiCode(err) != "ResourceInUseException" && !throttled(err):
				return "", failed(err)
			}
		case err != nil:
			return "", failed(err)
		case d.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive:
			return aws.ToString(d.ConsumerDescription.ConsumerARN), nil
		}
		th.ok()
		if time.Now().After(deadline) {
			if refused != nil {
				// The limit of consumers a stream takes, or of those being
				// registered at once, held for the whole wait.
				return "", sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED, fmt.Errorf("kinesis: consumer %s of %s: Kinesis refused to register it for %s; the stream may have as many consumers as it takes: %w", name, streamARN, consumerActiveWithin, refused))
			}
			return "", sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("kinesis: consumer %s of %s isn't active after %s", name, streamARN, consumerActiveWithin))
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(consumerPollEvery):
		}
	}
}

// Teardown deregisters the pipeline's efo consumer from every stream the
// source offers, unless consumer.deregisterOnTeardown is off.
func (s *Source) Teardown(ctx context.Context, pipeline string) error {
	c := s.cfg.Consumer
	if c.Type != consumerEFO || c.DeregisterOnTeardown != nil && !*c.DeregisterOnTeardown {
		return nil
	}
	name, err := s.cfg.consumerName(pipeline)
	if err != nil {
		return err
	}
	cl, err := s.cfg.client(ctx)
	if err != nil {
		return err
	}
	streams, err := s.streams(ctx, cl, 0)
	if err != nil {
		return err
	}
	for _, st := range streams {
		sum, err := cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(st)})
		if apiCode(err) == "ResourceNotFoundException" {
			continue
		}
		if err != nil {
			return classify(fmt.Errorf("kinesis: describe stream %s: %w", st, err))
		}
		if err := s.deregister(ctx, cl, aws.ToString(sum.StreamDescriptionSummary.StreamARN), name); err != nil {
			return classify(fmt.Errorf("kinesis: deregister consumer %s of %s: %w", name, st, err))
		}
	}
	return nil
}

// deregister deregisters a stream's consumer, once it's active if it's
// being registered. One being deregistered, or gone, is done.
func (s *Source) deregister(ctx context.Context, cl *awskinesis.Client, streamARN, name string) error {
	deadline := time.Now().Add(consumerActiveWithin)
	for {
		_, err := cl.DeregisterStreamConsumer(ctx, &awskinesis.DeregisterStreamConsumerInput{StreamARN: aws.String(streamARN), ConsumerName: aws.String(name)})
		if err == nil {
			return nil
		}
		switch apiCode(err) {
		case "ResourceNotFoundException":
			return nil
		case "ResourceInUseException":
			d, err := cl.DescribeStreamConsumer(ctx, &awskinesis.DescribeStreamConsumerInput{StreamARN: aws.String(streamARN), ConsumerName: aws.String(name)})
			if apiCode(err) == "ResourceNotFoundException" || err == nil && d.ConsumerDescription.ConsumerStatus == types.ConsumerStatusDeleting {
				return nil
			}
			if err != nil && !throttled(err) {
				return err
			}
		default:
			if !throttled(err) {
				return err
			}
		}
		if time.Now().After(deadline) {
			return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("still being registered after %s", consumerActiveWithin))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(consumerPollEvery):
		}
	}
}

// streams lists the streams the source offers, up to limit; 0: all. Pages
// follow NextToken, or the last name where a page says there are more but
// has no token, as emulators answer.
func (s *Source) streams(ctx context.Context, cl *awskinesis.Client, limit int) ([]string, error) {
	var out []string
	in := &awskinesis.ListStreamsInput{}
	for {
		page, err := cl.ListStreams(ctx, in)
		if err != nil {
			return nil, classify(fmt.Errorf("kinesis: list streams: %w", err))
		}
		for _, name := range page.StreamNames {
			if s.offered(name) {
				out = append(out, name)
				if limit > 0 && len(out) == limit {
					return out, nil
				}
			}
		}
		switch n := len(page.StreamNames); {
		case page.NextToken != nil:
			in = &awskinesis.ListStreamsInput{NextToken: page.NextToken}
		case aws.ToBool(page.HasMoreStreams) && n > 0:
			in = &awskinesis.ListStreamsInput{ExclusiveStartStreamName: aws.String(page.StreamNames[n-1])}
		default:
			return out, nil
		}
	}
}

// eventStream is a SubscribeToShard subscription's events.
type eventStream interface {
	Events() <-chan types.SubscribeToShardEventStream
	Err() error
	Close() error
}

func subscriber(cl *awskinesis.Client) func(context.Context, *awskinesis.SubscribeToShardInput) (eventStream, error) {
	return func(ctx context.Context, in *awskinesis.SubscribeToShardInput) (eventStream, error) {
		out, err := cl.SubscribeToShard(ctx, in)
		if err != nil {
			return nil, err
		}
		return out.GetStream(), nil
	}
}

// subscribe reads the shard through its efo consumer, subscribing again
// each time a subscription ends, as Kinesis ends each after five minutes.
func (sr *shardRead) subscribe(ctx context.Context) (bool, error) {
	cont := ""
	var last time.Time
	for {
		// Kinesis takes one subscription a second per consumer and shard.
		select {
		case <-ctx.Done():
			return false, nil
		case <-time.After(time.Until(last.Add(subscribeEvery))):
		}
		if !last.IsZero() {
			pos := sr.st
			if cont != "" {
				pos.After = cont
			}
			if err := sr.retained(ctx, pos); err != nil {
				return false, err
			}
		}
		last = time.Now()
		at := sr.startingPosition(cont)
		es, err := sr.rd.subscribe(ctx, &awskinesis.SubscribeToShardInput{
			ConsumerARN: aws.String(sr.r.consumer), ShardId: aws.String(sr.shard), StartingPosition: at,
		})
		if err == nil {
			var over bool
			over, cont, err = sr.events(ctx, es, cont)
			if over || err == nil {
				return over, err
			}
		}
		if ctx.Err() != nil {
			return false, nil
		}
		if sr.skewed(err, at.Type) {
			continue
		}
		if !resubscribable(err) {
			return false, sr.failed(sr.startRefused(err, at.Type))
		}
		if errors.Is(err, errSubscriptionEnded) {
			continue
		}
		if err := sr.th.wait(ctx, err); err != nil {
			return false, sr.failed(err)
		}
	}
}

// resubscribable says a subscription that failed with err can be tried
// again: throttled, another subscription of the consumer to the shard that
// hasn't ended yet, or the connection or Kinesis failing.
func resubscribable(err error) bool {
	if throttled(err) {
		return true
	}
	switch apiCode(err) {
	case "ResourceInUseException", "InternalFailure", "InternalFailureException", "ServiceUnavailable":
		return true
	}
	_, isNet := errors.AsType[net.Error](err)
	return isNet || errors.Is(err, errSubscriptionEnded)
}

// errSubscriptionEnded is a subscription that ended without an error, to
// subscribe again from where it got to.
var errSubscriptionEnded = errors.New("the subscription ended")

func (sr *shardRead) startingPosition(cont string) *types.StartingPosition {
	switch {
	case cont != "":
		return &types.StartingPosition{Type: types.ShardIteratorTypeAfterSequenceNumber, SequenceNumber: aws.String(cont)}
	case sr.st.After != "":
		return &types.StartingPosition{Type: types.ShardIteratorTypeAfterSequenceNumber, SequenceNumber: aws.String(sr.st.After)}
	case !sr.st.From.IsZero():
		return &types.StartingPosition{Type: types.ShardIteratorTypeAtTimestamp, Timestamp: aws.Time(sr.st.From)}
	}
	return &types.StartingPosition{Type: types.ShardIteratorTypeTrimHorizon}
}

// events handles a subscription's events, each as one fetch, until the
// subscription or the shard's read ends. It returns where to subscribe
// again from, and errSubscriptionEnded when the subscription ended without
// an error. Each event is emitted before the next is taken, so a reader
// that's behind holds the subscription back.
func (sr *shardRead) events(ctx context.Context, es eventStream, cont string) (bool, string, error) {
	defer func() { _ = es.Close() }()
	for {
		var ev types.SubscribeToShardEventStream
		var ok bool
		select {
		case <-ctx.Done():
			return false, cont, ctx.Err()
		case ev, ok = <-es.Events():
		}
		if !ok {
			if err := es.Err(); err != nil {
				return false, cont, err
			}
			return false, cont, errSubscriptionEnded
		}
		e, isEvent := ev.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent)
		if !isEvent {
			continue
		}
		release, err := sr.rd.lane(ctx, sr.r)
		if err != nil {
			return false, cont, err
		}
		v := e.Value
		over, err := sr.batch(ctx, time.Now(), v.Records, time.Duration(aws.ToInt64(v.MillisBehindLatest))*time.Millisecond, v.ContinuationSequenceNumber == nil)
		release()
		if err != nil || over {
			return over, cont, err
		}
		sr.th.ok()
		cont = aws.ToString(v.ContinuationSequenceNumber)
	}
}
