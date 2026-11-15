package kinesis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/openetl-io/openetl/sdk/go/preflight"
)

// preflightStreams is how many streams a source's pre-flight looks at when
// it's given a pattern, or none, rather than names.
const preflightStreams = 5

// efoConsumersPerStream is how many consumers Kinesis registers on a stream.
const efoConsumersPerStream = 20

// Preflight checks the credentials, the schema registry, and for each
// stream the source would read (or the first few it offers), that it exists
// and that the reads the source makes are allowed, reading no records.
func (s *Source) Preflight(ctx context.Context, r *preflight.Report) {
	cl, err := s.cfg.client(ctx)
	if err != nil {
		r.Fail("connect", "Reach Kinesis and list streams", err, preflight.Field("region"))
		return
	}
	if !connect(ctx, &s.cfg.Connection, cl, r) {
		r.Skip("streams", "Find the streams to read", "Kinesis wasn't reached.")
		return
	}
	names := s.cfg.Streams
	field := "streams"
	if len(names) == 0 || s.pattern != nil {
		if len(names) == 0 {
			field = "streamPattern"
		}
		all, err := s.streams(ctx, cl, 0)
		if err != nil {
			r.Fail("streams", "Find the streams to read", err, preflight.Field(field))
			return
		}
		switch {
		case len(all) == 0:
			r.Warn("streams", "Find the streams to read", "No stream the credentials can list matches streams and streamPattern, so the source offers none.",
				preflight.Field(field), preflight.WithFix("Check streams and streamPattern, and that the streams are in the region."))
			return
		case len(all) > preflightStreams:
			r.Pass("streams", "Find the streams to read", fmt.Sprintf("%d streams match; the checks below look at the first %d.", len(all), preflightStreams), preflight.Field(field))
			all = all[:preflightStreams]
		default:
			r.Pass("streams", "Find the streams to read", fmt.Sprintf("%d streams match.", len(all)), preflight.Field(field))
		}
		names = all
	}
	for _, name := range names {
		s.preflightStream(ctx, cl, r, name)
	}
}

func (s *Source) preflightStream(ctx context.Context, cl *awskinesis.Client, r *preflight.Report, name string) {
	item := func(what string) string { return "stream." + name + "." + what }
	arnOf := "arn:aws:kinesis:REGION:ACCOUNT:stream/" + name
	var sum *types.StreamDescriptionSummary
	if !r.Run(item("describe"), "Describe stream "+name, func() error {
		res, err := cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
		if apiCode(err) == "ResourceNotFoundException" {
			return preflight.Fix(invalid(fmt.Errorf("kinesis: stream %s doesn't exist", name)), "Create the stream, or correct streams. The stream has to be in the region of region.")
		}
		if err != nil {
			return allow(classify(wrap("describe stream "+name, err)), "kinesis:DescribeStreamSummary", arnOf)
		}
		sum = res.StreamDescriptionSummary
		return nil
	}, preflight.Field("streams")) {
		r.Skip(item("read"), "Read stream "+name, "The stream couldn't be described.")
		return
	}
	arnOf = aws.ToString(sum.StreamARN)
	retention := time.Duration(aws.ToInt32(sum.RetentionPeriodHours)) * time.Hour
	described := fmt.Sprintf("%s, %d open shards, %s retention, %s", strings.ToLower(string(sum.StreamStatus)), aws.ToInt32(sum.OpenShardCount), retention, encryption(sum))
	if sum.StreamStatus != types.StreamStatusActive && sum.StreamStatus != types.StreamStatusUpdating {
		r.Warn(item("status"), "Stream "+name+" is active", "The stream is "+described+"; a read waits until it's active.")
	} else {
		r.Pass(item("status"), "Stream "+name+" is active", described+".")
	}
	if s.cfg.StartPosition == startTimestamp && time.Since(*s.cfg.StartTimestamp) > retention {
		r.Warn(item("start"), "Stream "+name+" still holds records from startTimestamp",
			fmt.Sprintf("startTimestamp is older than the stream's %s retention: a first read starts at the oldest record it holds, and what arrived before that is gone.", retention),
			preflight.Field("startTimestamp"))
	}
	var shard string
	if !r.Run(item("shards"), "List stream "+name+"'s shards", func() error {
		res, err := cl.ListShards(ctx, &awskinesis.ListShardsInput{StreamName: aws.String(name), ShardFilter: &types.ShardFilter{Type: types.ShardFilterTypeAtLatest}})
		if err != nil {
			return allow(classify(wrap("list "+name+"'s shards", err)), "kinesis:ListShards", arnOf)
		}
		if len(res.Shards) > 0 {
			shard = aws.ToString(res.Shards[0].ShardId)
		}
		return nil
	}) {
		r.Skip(item("read"), "Read stream "+name, "Its shards couldn't be listed.")
		return
	}
	if s.cfg.Consumer.Type == consumerEFO {
		s.preflightConsumer(ctx, cl, r, name, arnOf)
		return
	}
	if shard == "" {
		r.Skip(item("read"), "Read stream "+name, "The stream has no open shard to read.")
		return
	}
	r.Run(item("read"), "Read stream "+name+" with GetRecords", func() error {
		it, err := cl.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{StreamName: aws.String(name), ShardId: aws.String(shard), ShardIteratorType: types.ShardIteratorTypeLatest})
		if err != nil {
			return allow(classify(wrap("get a shard iterator of "+name, err)), "kinesis:GetShardIterator", arnOf)
		}
		_, err = cl.GetRecords(ctx, &awskinesis.GetRecordsInput{ShardIterator: it.ShardIterator, Limit: aws.Int32(1)})
		if apiCode(err) == "KMSAccessDeniedException" {
			return allow(classify(wrap("read "+name, err)), "kms:Decrypt", "the stream's KMS key")
		}
		return allow(classify(wrap("read "+name, err)), "kinesis:GetRecords", arnOf)
	})
}

// preflightConsumer checks what enhanced fan-out needs without registering a
// consumer: the pipeline's consumer if consumer.name names it, or the
// stream's consumers.
func (s *Source) preflightConsumer(ctx context.Context, cl *awskinesis.Client, r *preflight.Report, name, streamARN string) {
	item := func(what string) string { return "stream." + name + "." + what }
	consumerARN := streamARN + "/consumer/" + "*"
	if c := s.cfg.Consumer.Name; c != "" {
		consumerARN = streamARN + "/consumer/" + c + ":*"
		r.Run(item("consumer"), "Find consumer "+c+" of stream "+name, func() error {
			res, err := cl.DescribeStreamConsumer(ctx, &awskinesis.DescribeStreamConsumerInput{StreamARN: aws.String(streamARN), ConsumerName: aws.String(c)})
			switch {
			case apiCode(err) == "ResourceNotFoundException":
				return nil
			case err != nil:
				return allow(classify(wrap("describe consumer "+c, err)), "kinesis:DescribeStreamConsumer", consumerARN)
			case res.ConsumerDescription.ConsumerStatus == types.ConsumerStatusDeleting:
				return fmt.Errorf("kinesis: consumer %s of %s is being deregistered", c, name)
			}
			return nil
		}, preflight.Field("consumer.name"))
	} else {
		r.Run(item("consumers"), "List stream "+name+"'s consumers", func() error {
			n := 0
			p := awskinesis.NewListStreamConsumersPaginator(cl, &awskinesis.ListStreamConsumersInput{StreamARN: aws.String(streamARN)})
			for p.HasMorePages() {
				page, err := p.NextPage(ctx)
				if err != nil {
					return allow(classify(wrap("list "+name+"'s consumers", err)), "kinesis:ListStreamConsumers", streamARN)
				}
				n += len(page.Consumers)
			}
			if n >= efoConsumersPerStream {
				return preflight.Fix(fmt.Errorf("kinesis: stream %s has %d consumers, Kinesis's limit, so the pipeline's can't be registered", name, n), "Deregister a consumer the stream no longer needs.")
			}
			return nil
		}, preflight.Field("consumer.type"))
	}
	r.Skip(item("subscribe"), "Register a consumer of stream "+name+" and subscribe to its shards",
		"Registering a consumer would change the stream, so the first read does it.",
		preflight.Field("consumer.type"),
		preflight.WithFix(fmt.Sprintf("Allow kinesis:DescribeStreamConsumer and kinesis:RegisterStreamConsumer on %s, kinesis:SubscribeToShard on %s, and kinesis:DeregisterStreamConsumer for teardown.", streamARN, consumerARN)))
}

func encryption(sum *types.StreamDescriptionSummary) string {
	if sum.EncryptionType == types.EncryptionTypeKms {
		return "KMS encryption"
	}
	return "no encryption"
}

// preflightExisting is how many existing streams under the namespace a
// destination's pre-flight compares with its config.
const preflightExisting = 10

// Preflight checks the credentials and the schema registry, compares the
// streams already under the namespace with what the config would create,
// and says what it can't check without writing: creating streams and
// putting records.
func (d *Destination) Preflight(ctx context.Context, r *preflight.Report) {
	cfg := &d.cfg
	cl, err := cfg.client(ctx)
	if err != nil {
		r.Fail("connect", "Reach Kinesis and list streams", err, preflight.Field("region"))
		return
	}
	if !connect(ctx, &cfg.Connection, cl, r) {
		return
	}
	streams := "arn:aws:kinesis:REGION:ACCOUNT:stream/*"
	if cfg.Namespace != "" {
		streams = "arn:aws:kinesis:REGION:ACCOUNT:stream/" + cfg.Namespace + ".*"
		d.preflightExisting(ctx, cl, r)
	}
	if c := cfg.CreateStreams; c != nil && !*c {
		r.Pass("create", "Create missing streams", "createStreams is off: each stream has to exist before the pipeline writes to it.", preflight.Field("createStreams"))
	} else {
		actions := "kinesis:CreateStream and kinesis:DescribeStream"
		if cfg.RetentionHours > minRetentionHours {
			actions += ", kinesis:IncreaseStreamRetentionPeriod"
		}
		if cfg.KMSKeyID != "" {
			actions += ", kinesis:StartStreamEncryption, and kms:GenerateDataKey on the key"
		}
		r.Skip("create", "Create missing streams", "Creating a stream would change the account, so the first write does it.",
			preflight.Field("createStreams"), preflight.WithFix(fmt.Sprintf("Allow %s on %s.", actions, streams)))
	}
	put := "kinesis:DescribeStreamSummary and kinesis:PutRecords"
	if cfg.KMSKeyID != "" {
		put += ", and kms:GenerateDataKey on the streams' key"
	}
	r.Skip("put", "Put records", "Putting a record would write to a stream, so it's not checked.",
		preflight.WithFix(fmt.Sprintf("Allow %s on %s.", put, streams)))
}

// preflightExisting describes the streams under the namespace that exist,
// and warns of those that differ from what the config would create, which
// the destination leaves as they are.
func (d *Destination) preflightExisting(ctx context.Context, cl *awskinesis.Client, r *preflight.Report) {
	prefix := d.cfg.Namespace + "."
	var names []string
	more := false
	p := awskinesis.NewListStreamsPaginator(cl, &awskinesis.ListStreamsInput{})
	for p.HasMorePages() && !more {
		page, err := p.NextPage(ctx)
		if err != nil {
			r.Fail("existing", "List the streams under the namespace", allow(classify(wrap("list streams", err)), "kinesis:ListStreams", "*"), preflight.Field("namespace"))
			return
		}
		for _, n := range page.StreamNames {
			if strings.HasPrefix(n, prefix) {
				if len(names) == preflightExisting {
					more = true
					break
				}
				names = append(names, n)
			}
		}
	}
	switch {
	case len(names) == 0:
		r.Pass("existing", "List the streams under the namespace", "No stream is named "+prefix+"* yet.", preflight.Field("namespace"))
		return
	case more:
		r.Pass("existing", "List the streams under the namespace", fmt.Sprintf("More than %d streams are named %s*; the checks below look at the first %d.", preflightExisting, prefix, preflightExisting), preflight.Field("namespace"))
	default:
		r.Pass("existing", "List the streams under the namespace", fmt.Sprintf("%d streams are named %s*.", len(names), prefix), preflight.Field("namespace"))
	}
	for _, n := range names {
		var sum *types.StreamDescriptionSummary
		if !r.Run("stream."+n+".describe", "Describe stream "+n, func() error {
			res, err := cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(n)})
			if err != nil {
				return allow(classify(wrap("describe stream "+n, err)), "kinesis:DescribeStreamSummary", "arn:aws:kinesis:REGION:ACCOUNT:stream/"+n)
			}
			sum = res.StreamDescriptionSummary
			return nil
		}) {
			continue
		}
		if diff := differs(&d.cfg, sum); diff != "" {
			r.Warn("stream."+n+".settings", "Stream "+n+" has the config's settings",
				"The stream is "+diff+". The destination writes to it as it is: streamMode, shards, retentionHours and kmsKeyId apply only to streams it creates.",
				preflight.WithFix("Change the stream in Kinesis if it should match."))
		}
	}
}
