package kinesis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var syncModes = map[source.SyncMode]string{source.FullRefresh: "full_refresh", source.Incremental: "incremental", source.CDC: "cdc"}

// efoConsumersPerStream is how many consumers Kinesis registers on a stream.
const efoConsumersPerStream = 20

// streamARN is the IAM resource of a stream, for fixes.
func streamARN(name string) string { return "arn:aws:kinesis:REGION:ACCOUNT:stream/" + name }

// CheckReport checks the credentials reach Kinesis, the schema registry if
// there is one, and that streams and streamPattern match a stream.
func (s *Source) CheckReport(ctx context.Context, r *check.Report) {
	cl, err := s.cfg.client(ctx)
	if err != nil {
		r.Fail("connect", "Reach Kinesis and list streams", err, check.Field("region"))
		return
	}
	if !connect(ctx, &s.cfg.Connection, cl, r) {
		return
	}
	field := "streams"
	if len(s.cfg.Streams) == 0 {
		field = "streamPattern"
	}
	names, err := s.streams(ctx, cl, 0)
	switch {
	case err != nil:
		r.Fail("streams", "Find the streams to offer", err, check.Field(field))
	case len(names) == 0:
		r.Warn("streams", "Find the streams to offer", "No stream the credentials can list matches streams and streamPattern, so the source offers none.",
			check.Field(field), check.WithFix("Check streams and streamPattern, and that the streams are in the region."))
	default:
		r.Pass("streams", "Find the streams to offer", fmt.Sprintf("%d streams match.", len(names)), check.Field(field))
	}
}

// Preflight checks, for each stream a pipeline would read, that it's there
// and active, that the reads the source makes are allowed, that its
// retention keeps what the schedule and startTimestamp need, and with
// enhanced fan-out, the pipeline's consumer. It reads no records and
// registers nothing.
func (s *Source) Preflight(ctx context.Context, req source.PreflightRequest, r *check.Report) {
	cl, err := s.cfg.client(ctx)
	if err != nil {
		r.Fail("connect", "Reach Kinesis", err, check.Field("region"))
		return
	}
	for _, cs := range req.Streams {
		if cs.SyncMode != 0 && cs.SyncMode != source.Stream {
			r.Fail("stream."+cs.Name+".mode", "Read stream "+cs.Name+" with its sync mode",
				invalid(fmt.Errorf("kinesis: stream %s: the source reads Kinesis streams with the stream sync mode only, not %s", cs.Name, syncModes[cs.SyncMode])),
				check.WithFix("Read the stream with the stream sync mode."))
			continue
		}
		s.preflightStream(ctx, cl, r, cs.Name, req)
	}
}

func (s *Source) preflightStream(ctx context.Context, cl *awskinesis.Client, r *check.Report, name string, req source.PreflightRequest) {
	item := func(what string) string { return "stream." + name + "." + what }
	arn := streamARN(name)
	var sum *types.StreamDescriptionSummary
	if !r.Run(item("describe"), "Describe stream "+name, func() error {
		res, err := cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
		if apiCode(err) == "ResourceNotFoundException" {
			return check.Fix(invalid(fmt.Errorf("kinesis: stream %s doesn't exist", name)), "Create the stream in the region, or take it out of the pipeline.")
		}
		if err != nil {
			return allow(classify(wrap("describe stream "+name, err)), "kinesis:DescribeStreamSummary", arn)
		}
		sum = res.StreamDescriptionSummary
		return nil
	}, check.Field("streams")) {
		return
	}
	arn = aws.ToString(sum.StreamARN)
	retention := time.Duration(aws.ToInt32(sum.RetentionPeriodHours)) * time.Hour
	described := fmt.Sprintf("%s, %d open shards, %s retention, %s", strings.ToLower(string(sum.StreamStatus)), aws.ToInt32(sum.OpenShardCount), retention, encryption(sum))
	if sum.StreamStatus == types.StreamStatusActive || sum.StreamStatus == types.StreamStatusUpdating {
		r.Pass(item("status"), "Stream "+name+" is active", described+".")
	} else {
		r.Warn(item("status"), "Stream "+name+" is active", "The stream is "+described+"; a read waits until it's active.")
	}
	if s.cfg.StartPosition == startTimestamp && time.Since(*s.cfg.StartTimestamp) > retention {
		r.Warn(item("start"), "Stream "+name+" still holds records from startTimestamp",
			fmt.Sprintf("startTimestamp is older than the stream's %s retention: the first read starts at the oldest record the stream holds, and what arrived before that is gone.", retention),
			check.Field("startTimestamp"))
	}
	if req.Interval > retention {
		r.Warn(item("schedule"), "Stream "+name+" keeps records from one sync to the next",
			fmt.Sprintf("The pipeline can go %s between syncs, longer than the stream's %s retention: records that arrive just after a sync can be gone by the next, and the read stops with its position lost.", req.Interval, retention),
			check.WithFix(fmt.Sprintf("Sync at least every %s, read continuously, or raise the stream's retention.", retention)))
	}
	var shard string
	if !r.Run(item("shards"), "List stream "+name+"'s shards", func() error {
		res, err := cl.ListShards(ctx, &awskinesis.ListShardsInput{StreamName: aws.String(name), ShardFilter: &types.ShardFilter{Type: types.ShardFilterTypeAtLatest}})
		if err != nil {
			return allow(classify(wrap("list "+name+"'s shards", err)), "kinesis:ListShards", arn)
		}
		if len(res.Shards) > 0 {
			shard = aws.ToString(res.Shards[0].ShardId)
		}
		return nil
	}) {
		return
	}
	if shard == "" {
		r.Skip(item("read"), "Read stream "+name, "The stream has no open shard to read.")
	} else {
		r.Run(item("read"), "Read stream "+name, func() error {
			it, err := cl.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{StreamName: aws.String(name), ShardId: aws.String(shard), ShardIteratorType: types.ShardIteratorTypeLatest})
			if err != nil {
				return allow(classify(wrap("get a shard iterator of "+name, err)), "kinesis:GetShardIterator", arn)
			}
			_, err = cl.GetRecords(ctx, &awskinesis.GetRecordsInput{ShardIterator: it.ShardIterator, Limit: aws.Int32(1)})
			if apiCode(err) == "KMSAccessDeniedException" {
				return allow(classify(wrap("read "+name, err)), "kms:Decrypt", "the stream's KMS key")
			}
			return allow(classify(wrap("read "+name, err)), "kinesis:GetRecords", arn)
		})
	}
	if s.cfg.Consumer.Type == consumerEFO {
		s.preflightConsumer(ctx, cl, r, name, arn, req.Pipeline)
	}
}

// preflightConsumer checks the pipeline's enhanced fan-out consumer of a
// stream, without registering it: there already, or with room for it.
func (s *Source) preflightConsumer(ctx context.Context, cl *awskinesis.Client, r *check.Report, stream, arn, pipeline string) {
	item := "stream." + stream + ".consumer"
	name, err := s.cfg.consumerName(pipeline)
	if err != nil {
		r.Fail(item, "Find the pipeline's consumer of stream "+stream, err, check.Field("consumer.name"))
		return
	}
	title := "Find consumer " + name + " of stream " + stream
	consumerARN := arn + "/consumer/" + name + ":*"
	d, err := cl.DescribeStreamConsumer(ctx, &awskinesis.DescribeStreamConsumerInput{StreamARN: aws.String(arn), ConsumerName: aws.String(name)})
	switch {
	case err == nil && d.ConsumerDescription.ConsumerStatus == types.ConsumerStatusDeleting:
		r.Warn(item, title, "The consumer is being deregistered; the first read waits until it's gone, and registers it again.", check.Field("consumer.name"))
	case err == nil:
		r.Pass(item, title, "Registered, "+strings.ToLower(string(d.ConsumerDescription.ConsumerStatus))+".", check.Field("consumer.name"))
	case apiCode(err) != "ResourceNotFoundException":
		r.Fail(item, title, allow(classify(wrap("describe consumer "+name, err)), "kinesis:DescribeStreamConsumer", consumerARN), check.Field("consumer.name"))
		return
	default:
		if !r.Run(item, title, func() error {
			n := 0
			p := awskinesis.NewListStreamConsumersPaginator(cl, &awskinesis.ListStreamConsumersInput{StreamARN: aws.String(arn)})
			for p.HasMorePages() {
				page, err := p.NextPage(ctx)
				if err != nil {
					return allow(classify(wrap("list "+stream+"'s consumers", err)), "kinesis:ListStreamConsumers", arn)
				}
				n += len(page.Consumers)
			}
			if n >= efoConsumersPerStream {
				return check.Fix(invalid(fmt.Errorf("kinesis: stream %s has %d consumers, Kinesis's limit, so the pipeline's can't be registered", stream, n)), "Deregister a consumer the stream no longer needs.")
			}
			return nil
		}, check.Field("consumer.name")) {
			return
		}
	}
	r.Skip("stream."+stream+".subscribe", "Register consumer "+name+" of stream "+stream+" and subscribe to its shards",
		"Registering a consumer would change the stream, so the first read does it if it isn't registered yet.", check.Field("consumer.type"),
		check.WithFix(fmt.Sprintf("Allow kinesis:DescribeStreamConsumer, kinesis:ListStreamConsumers, kinesis:RegisterStreamConsumer and kinesis:DeregisterStreamConsumer on %s, and kinesis:SubscribeToShard on %s.", arn, consumerARN)))
}

func encryption(sum *types.StreamDescriptionSummary) string {
	if sum.EncryptionType == types.EncryptionTypeKms {
		return "KMS encryption"
	}
	return "no encryption"
}

// CheckReport checks the credentials reach Kinesis, and the schema registry
// if there is one.
func (d *Destination) CheckReport(ctx context.Context, r *check.Report) {
	cl, err := d.cfg.client(ctx)
	if err != nil {
		r.Fail("connect", "Reach Kinesis and list streams", err, check.Field("region"))
		return
	}
	connect(ctx, &d.cfg.Connection, cl, r)
}

// Preflight checks, for each stream a pipeline would write, its write mode,
// its partition keys, and its Kinesis stream: there and as the config would
// create it, or missing and to be created. It writes nothing.
func (d *Destination) Preflight(ctx context.Context, req destination.PreflightRequest, r *check.Report) {
	cl, err := d.cfg.client(ctx)
	if err != nil {
		r.Fail("connect", "Reach Kinesis", err, check.Field("region"))
		return
	}
	var targets []string
	for _, s := range req.Streams {
		sc := req.Schemas[s.Name]
		name := target(req.Namespace, d.cfg.Namespace, tableOf(s.Name, sc))
		targets = append(targets, streamARN(name))
		item := func(what string) string { return "stream." + s.Name + "." + what }
		if !DestinationCapabilities.SupportsMode(s.Mode) {
			r.Fail(item("mode"), "Write stream "+s.Name+" with its write mode", invalid(fmt.Errorf("kinesis: stream %s: Kinesis only appends", s.Name)),
				check.WithFix("Write the stream with the append mode."))
		}
		if sc != nil {
			// The engine adds _openetl_id to every batch, so a stream
			// without a primary key still has a partition key.
			r.Run(item("keys"), "Make partition keys for stream "+s.Name, func() error {
				_, err := keyColumns(d.cfg.PartitionKey.Mode, sc, s.PrimaryKey, d.cfg.PartitionKey.Fields, true)
				return err
			}, check.Field("partitionKey"))
		}
		d.preflightTarget(ctx, cl, r, s.Name, name)
	}
	put := "kinesis:DescribeStreamSummary and kinesis:PutRecords"
	if d.cfg.KMSKeyID != "" {
		put += ", and kms:GenerateDataKey on the streams' key"
	}
	if len(targets) > 0 {
		r.Skip("put", "Put records", "Putting a record would write to a stream, so it's not checked.",
			check.WithFix(fmt.Sprintf("Allow %s on %s.", put, strings.Join(targets, ", "))))
	}
}

// preflightTarget checks the Kinesis stream a pipeline's stream goes to.
func (d *Destination) preflightTarget(ctx context.Context, cl *awskinesis.Client, r *check.Report, stream, name string) {
	item := "stream." + stream + ".target"
	title := "Kinesis stream " + name
	cfg := &d.cfg
	res, err := cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
	switch {
	case apiCode(err) == "ResourceNotFoundException" && cfg.CreateStreams != nil && !*cfg.CreateStreams:
		r.Fail(item, title, check.Fix(invalid(fmt.Errorf("kinesis: stream %s doesn't exist, and createStreams is off", name)), "Create the stream, or turn createStreams on."), check.Field("createStreams"))
	case apiCode(err) == "ResourceNotFoundException":
		actions := "kinesis:CreateStream and kinesis:DescribeStream"
		if cfg.RetentionHours > minRetentionHours {
			actions += ", kinesis:IncreaseStreamRetentionPeriod"
		}
		if cfg.KMSKeyID != "" {
			actions += ", kinesis:StartStreamEncryption, and kms:GenerateDataKey on the key"
		}
		r.Skip(item, title, fmt.Sprintf("The stream doesn't exist yet. The first write creates it %s.", creates(cfg)),
			check.Field("createStreams"), check.WithFix(fmt.Sprintf("Allow %s on %s.", actions, streamARN(name))))
	case err != nil:
		r.Fail(item, title, allow(classify(wrap("describe stream "+name, err)), "kinesis:DescribeStreamSummary", streamARN(name)))
	default:
		sum := res.StreamDescriptionSummary
		described := fmt.Sprintf("%s, %d open shards, %d hours' retention, %s", strings.ToLower(string(sum.StreamStatus)), aws.ToInt32(sum.OpenShardCount), aws.ToInt32(sum.RetentionPeriodHours), encryption(sum))
		diff := differs(cfg, sum)
		switch {
		case sum.StreamStatus != types.StreamStatusActive && sum.StreamStatus != types.StreamStatusUpdating:
			r.Warn(item, title, "The stream is "+described+"; a write waits until it's active.")
		case diff != "":
			r.Warn(item, title, "The stream is "+diff+". The destination writes to it as it is: streamMode, shards, retentionHours and kmsKeyId apply only to streams it creates.",
				check.WithFix("Change the stream in Kinesis if it should match."))
		default:
			r.Pass(item, title, "The stream is "+described+".")
		}
	}
}

// creates says how the destination creates a stream.
func creates(cfg *DestinationConfig) string {
	out := "on-demand"
	if cfg.StreamMode == streamProvisioned {
		out = fmt.Sprintf("provisioned with %d shards", cfg.Shards)
	}
	out += fmt.Sprintf(", with %d hours' retention", max(cfg.RetentionHours, minRetentionHours))
	if cfg.KMSKeyID != "" {
		return out + ", encrypted with kmsKeyId"
	}
	return out + ", unencrypted"
}
