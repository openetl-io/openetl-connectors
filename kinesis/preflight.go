package kinesis

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	_ source.CheckLister      = (*Source)(nil)
	_ source.Preflighter      = (*Source)(nil)
	_ destination.CheckLister = (*Destination)(nil)
	_ destination.Preflighter = (*Destination)(nil)
)

var (
	syncModes  = map[source.SyncMode]string{source.FullRefresh: "full_refresh", source.Incremental: "incremental", source.CDC: "cdc", source.Stream: "stream"}
	writeModes = map[destination.WriteMode]string{destination.Append: "append", destination.Overwrite: "overwrite", destination.Merge: "merge"}
)

const (
	// How many enhanced fan-out consumers a stream takes at the least: 20,
	// or 50 for an On-demand Advantage stream, which the API doesn't tell
	// apart.
	consumerLimit = 20
	// How far startTimestamp may be ahead of the runner's clock before the
	// preflight counts it as in the future, rather than as clocks that
	// differ.
	clockSkew = time.Minute
)

var (
	noStreams = check.Reason{
		Name:    "no_streams",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "No stream the credentials list in {region} matches streams and streamPattern, so the source offers none.",
		Fix:     "Check streams and streamPattern, and that the streams are in {region}.",
	}
	streamsMissing = check.Reason{
		Name:    "streams_missing",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The source offers none of {missing}, which streams names.",
		Fix:     "Check that each stream in streams exists in {region}, that the credentials can list it, and that it matches streamPattern if that's set.",
	}
	startInFuture = check.Reason{
		Name:    "start_in_future",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "startTimestamp {start} is in the future. Kinesis starts a read only at a time that has passed, so every first read would fail until then.",
		Fix:     "Set startTimestamp to a time that has passed, or set startPosition to latest.",
	}
	syncModeUnsupported = check.Reason{
		Name:    "sync_mode_unsupported",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The source reads Kinesis streams with the stream sync mode only, not {mode}.",
		Fix:     "Read {stream} with the stream sync mode in the pipeline.",
	}
	streamMissing = check.Reason{
		Name:    check.NotFound.Name,
		Code:    check.NotFound.Code,
		Message: "{stream} doesn't exist in {region}.",
		Fix:     "Create {stream} in {region}, or take it out of the pipeline.",
	}
	streamCreating = check.Reason{
		Name:    "stream_not_active",
		Code:    protocolv1.ErrorCode_ERROR_CODE_TRANSIENT,
		Message: "{stream} is {status}: a read waits until it's active.",
	}
	streamDeleting = check.Reason{
		Name:    "stream_deleting",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{stream} is being deleted.",
		Fix:     "Take {stream} out of the pipeline, or create it again once it's gone.",
	}
	startBeforeRetention = check.Reason{
		Name:    "start_before_retention",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "startTimestamp {start} is older than {stream}'s retention of {retention}. A first read starts at the oldest record the stream holds, and what arrived before that is gone.",
		Fix:     "Set startTimestamp within the last {retention}, or accept that the first read starts at the oldest record.",
	}
	scheduleOutrunsRetention = check.Reason{
		Name:    "schedule_outruns_retention",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The pipeline can go {interval} between syncs, longer than {stream}'s retention of {retention}. Records that arrive just after a sync can be gone by the next, and the read then stops with its position lost.",
		Fix:     "Sync at least every {retention}, read continuously, or raise {stream}'s retention.",
	}
	consumerUnknown = check.Reason{
		Name:    "consumer_unknown",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The preflight doesn't name the pipeline, so it can't tell which consumer of {stream} the pipeline reads with.",
		Fix:     "Set consumer.name, or preflight the pipeline itself.",
	}
	consumerDeleting = check.Reason{
		Name:    "consumer_deleting",
		Code:    protocolv1.ErrorCode_ERROR_CODE_TRANSIENT,
		Message: "{consumer} is being deregistered from {stream}. The first read waits until it's gone, then registers it again.",
	}
	consumersFull = check.Reason{
		Name:    "consumer_limit",
		Code:    protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED,
		Message: "{stream} has {count} enhanced fan-out consumers. Kinesis allows 20 a stream, or 50 for an On-demand Advantage stream, so registering the pipeline's may fail.",
		Fix:     "Deregister a consumer that {stream} no longer needs, or read with consumer.type polling.",
	}
)

// Checks sign in to Kinesis, reach the schema registry if the format needs
// one, and find the streams the source offers.
func (s *Source) Checks() []check.Check {
	k := newConn(&s.cfg.Connection)
	return append(connectionChecks(k, false), s.offeredCheck(k))
}

func (s *Source) offeredCheck(k *conn) check.Check {
	field := "streams"
	if len(s.cfg.Streams) == 0 && s.cfg.StreamPattern != "" {
		field = "streamPattern"
	}
	return check.Check{
		Name: "streams.offered", Kind: check.Resource, Title: "Find the streams to offer",
		Description: "Lists the streams the credentials can list, and keeps those that streams and streamPattern name.",
		Field:       field, Needs: []string{"kinesis.sign-in"}, Timeout: callTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := k.client(ctx)
			if err != nil {
				return err
			}
			p.Param("region", k.ac.Region)
			names, err := s.streams(ctx, cl, 0)
			if err != nil {
				return call{action: "kinesis:ListStreams", resource: "*"}.failure(err)
			}
			if len(names) == 0 {
				return check.Warn(noStreams.Fail())
			}
			p.Param("count", len(names))
			p.Param("streams", list(names))
			var missing []string
			for _, n := range s.cfg.Streams {
				if !slices.Contains(names, n) {
					missing = append(missing, n)
				}
			}
			if len(missing) > 0 {
				p.Param("missing", list(missing))
				return check.Warn(streamsMissing.Fail())
			}
			p.Found("The source offers {count} streams: {streams}.")
			return nil
		},
	}
}

// Preflight checks, for each stream the pipeline reads, its sync mode; that
// the Kinesis stream is there and active, and its shards listed and read;
// that its retention keeps what startTimestamp and the schedule need; and
// with enhanced fan-out, the pipeline's consumer. It reads no records and
// registers nothing.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	k := newConn(&s.cfg.Connection)
	var checks []check.Check
	if s.cfg.StartPosition == startTimestamp {
		checks = append(checks, s.startCheck())
	}
	names := make([]string, len(req.Streams))
	for i, cs := range req.Streams {
		names[i] = cs.Name
	}
	parts := streamParts(names)
	seen := map[string]bool{}
	for _, cs := range req.Streams {
		if seen[cs.Name] {
			continue
		}
		seen[cs.Name] = true
		checks = append(checks, s.streamChecks(k, "stream."+parts[cs.Name], cs, req)...)
	}
	return checks
}

func (s *Source) startCheck() check.Check {
	start := *s.cfg.StartTimestamp
	return check.Check{
		Name: "start-timestamp", Kind: check.Configuration, Title: "Start new reads at {start}",
		Field:  "startTimestamp",
		Params: check.Params{"start": start.UTC().Format(time.RFC3339)},
		Run: func(context.Context, *check.Probe) error {
			if start.After(time.Now().Add(clockSkew)) {
				return startInFuture.Fail()
			}
			return nil
		},
	}
}

// streamFacts is what a stream's checks found, for the checks after them.
type streamFacts struct {
	sum found[*types.StreamDescriptionSummary]
	// An open shard; empty for none.
	shard found[string]
	// The pipeline's consumer is registered or being registered.
	registered found[bool]
}

func (s *Source) streamChecks(k *conn, base string, cs source.ConfiguredStream, req source.PreflightRequest) []check.Check {
	name := cs.Name
	f := &streamFacts{}
	params := func(kv ...string) check.Params {
		p := check.Params{"stream": name}
		for i := 0; i+1 < len(kv); i += 2 {
			p[kv[i]] = kv[i+1]
		}
		return p
	}
	mode := cmp.Or(syncModes[cs.SyncMode], "stream")
	checks := []check.Check{{
		Name: base + ".mode", Kind: check.Configuration, Title: "Read {stream} with the stream sync mode",
		Params: params("mode", mode),
		Run: func(context.Context, *check.Probe) error {
			if mode != "stream" {
				return syncModeUnsupported.Fail()
			}
			return nil
		},
	}, {
		Name: base + ".describe", Kind: check.Resource, Title: "Describe {stream}",
		Description: "Describes the Kinesis stream: its status, open shards, retention and encryption.",
		Field:       "streams", Params: params(), Needs: []string{base + ".mode"}, Timeout: callTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := k.client(ctx)
			if err != nil {
				return err
			}
			p.Param("region", k.ac.Region)
			res, err := cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
			if apiCode(err) == "ResourceNotFoundException" {
				return streamMissing.Fail()
			}
			if err != nil {
				return call{action: "kinesis:DescribeStreamSummary", resource: k.streamARN(name)}.failure(err)
			}
			sum := res.StreamDescriptionSummary
			k.learn(sum.StreamARN)
			f.sum.set(sum)
			describe(p, sum)
			switch {
			case sum.StreamStatus == types.StreamStatusDeleting:
				return streamDeleting.Fail()
			case !isActive(sum):
				return check.Warn(streamCreating.Fail())
			}
			p.Found("{stream} is {status}, with {shards} open shards, {retention} of retention and {encryption}.")
			return nil
		},
	}}
	retention := func() time.Duration {
		return time.Duration(aws.ToInt32(f.sum.get().RetentionPeriodHours)) * time.Hour
	}
	if s.cfg.StartPosition == startTimestamp {
		start := *s.cfg.StartTimestamp
		checks = append(checks, check.Check{
			Name: base + ".start", Kind: check.Configuration, Title: "Hold {stream}'s records from {start}",
			Field:  "startTimestamp",
			Params: params("start", start.UTC().Format(time.RFC3339)),
			Needs:  []string{"start-timestamp", base + ".describe"},
			Run: func(_ context.Context, p *check.Probe) error {
				r := retention()
				p.Param("retention", human(r))
				if time.Since(start) > r {
					return check.Warn(startBeforeRetention.Fail())
				}
				p.Found("{start} is within {stream}'s retention of {retention}.")
				return nil
			},
		})
	}
	if req.Interval > 0 {
		checks = append(checks, check.Check{
			Name: base + ".schedule", Kind: check.Configuration, Title: "Keep {stream}'s records from one sync to the next",
			Params: params("interval", human(req.Interval)),
			Needs:  []string{base + ".describe"},
			Run: func(_ context.Context, p *check.Probe) error {
				r := retention()
				p.Param("retention", human(r))
				if req.Interval > r {
					return check.Warn(scheduleOutrunsRetention.Fail())
				}
				p.Found("{stream} keeps records for {retention}, longer than the {interval} between syncs.")
				return nil
			},
		})
	}
	checks = append(checks, check.Check{
		Name: base + ".shards", Kind: check.Permission, Title: "List {stream}'s shards",
		Params: params(), Needs: []string{base + ".describe"}, Timeout: callTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := k.client(ctx)
			if err != nil {
				return err
			}
			res, err := cl.ListShards(ctx, &awskinesis.ListShardsInput{StreamName: aws.String(name), ShardFilter: &types.ShardFilter{Type: types.ShardFilterTypeAtLatest}})
			if err != nil {
				return call{action: "kinesis:ListShards", resource: k.streamARN(name)}.failure(err)
			}
			if len(res.Shards) > 0 {
				f.shard.set(aws.ToString(res.Shards[0].ShardId))
			}
			return nil
		},
	}, check.Check{
		Name: base + ".read", Kind: check.Permission, Title: "Read {stream}",
		Description: "Reads one open shard from its newest record, which returns nothing: the preflight reads no records.",
		Params:      params(), Needs: []string{base + ".shards"}, Timeout: callTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			shard := f.shard.get()
			if shard == "" {
				return check.Skip("{stream} has no open shard to read.")
			}
			p.Param("shard", shard)
			cl, err := k.client(ctx)
			if err != nil {
				return err
			}
			arn := k.streamARN(name)
			it, err := cl.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{StreamName: aws.String(name), ShardId: aws.String(shard), ShardIteratorType: types.ShardIteratorTypeLatest})
			if err != nil {
				return call{action: "kinesis:GetShardIterator", resource: arn}.failure(err)
			}
			if _, err := cl.GetRecords(ctx, &awskinesis.GetRecordsInput{ShardIterator: it.ShardIterator, Limit: aws.Int32(1)}); err != nil {
				return call{action: "kinesis:GetRecords", resource: arn, kmsAction: "kms:Decrypt"}.failure(err)
			}
			p.Found("Read {stream}'s shard {shard} from its newest record.")
			return nil
		},
	})
	if s.cfg.Consumer.Type == consumerEFO {
		checks = append(checks, s.consumerChecks(k, base, name, req.Pipeline, f)...)
	}
	return checks
}

// consumerChecks check the pipeline's enhanced fan-out consumer of a stream
// without registering it: whether it's registered, and if it isn't, whether
// the stream has room for it.
func (s *Source) consumerChecks(k *conn, base, stream, pipeline string, f *streamFacts) []check.Check {
	consumer, err := s.cfg.consumerName(pipeline)
	known := err == nil
	params := check.Params{"stream": stream}
	title := "Find the pipeline's consumer of {stream}"
	subscribe := "Register the pipeline's consumer of {stream}, and subscribe to its shards"
	if known {
		params["consumer"] = consumer
		title = "Find the consumer {consumer} of {stream}"
		subscribe = "Register {consumer} on {stream}, and subscribe to its shards"
	}
	return []check.Check{{
		Name: base + ".consumer", Kind: check.Resource, Title: title,
		Field: "consumer.name", Params: params, Needs: []string{base + ".describe"}, Timeout: callTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			if !known {
				return check.Warn(consumerUnknown.Fail())
			}
			cl, err := k.client(ctx)
			if err != nil {
				return err
			}
			arn := aws.ToString(f.sum.get().StreamARN)
			d, err := cl.DescribeStreamConsumer(ctx, &awskinesis.DescribeStreamConsumerInput{StreamARN: aws.String(arn), ConsumerName: aws.String(consumer)})
			switch {
			case apiCode(err) == "ResourceNotFoundException":
				p.Found("{consumer} isn't registered yet: the first read registers it.")
				return nil
			case err != nil:
				return call{action: "kinesis:DescribeStreamConsumer", resource: consumerARN(arn, consumer)}.failure(err)
			}
			switch d.ConsumerDescription.ConsumerStatus {
			case types.ConsumerStatusDeleting:
				return check.Warn(consumerDeleting.Fail())
			case types.ConsumerStatusCreating:
				p.Found("{consumer} is being registered.")
			default:
				p.Found("{consumer} is registered and active.")
			}
			f.registered.set(true)
			return nil
		},
	}, {
		Name: base + ".consumer-limit", Kind: check.Quota, Title: "Find room for another consumer of {stream}",
		Description: "Counts the stream's enhanced fan-out consumers, when the pipeline's isn't registered yet.",
		Field:       "consumer.type", Params: params, Needs: []string{base + ".consumer"}, Timeout: callTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			if f.registered.get() {
				return check.Skip("{consumer} is registered already, so it takes no new place.")
			}
			sum := f.sum.get()
			n := int(aws.ToInt32(sum.ConsumerCount))
			if sum.ConsumerCount == nil {
				cl, err := k.client(ctx)
				if err != nil {
					return err
				}
				pages := awskinesis.NewListStreamConsumersPaginator(cl, &awskinesis.ListStreamConsumersInput{StreamARN: sum.StreamARN})
				for pages.HasMorePages() {
					page, err := pages.NextPage(ctx)
					if err != nil {
						return call{action: "kinesis:ListStreamConsumers", resource: aws.ToString(sum.StreamARN)}.failure(err)
					}
					n += len(page.Consumers)
				}
			}
			p.Param("count", n)
			if n >= consumerLimit {
				return check.Warn(consumersFull.Fail())
			}
			p.Found("{stream} has {count} enhanced fan-out consumers, fewer than the 20 Kinesis allows a stream.")
			return nil
		},
	}, {
		Name: base + ".subscribe", Kind: check.Permission, Title: subscribe,
		Field: "consumer.type", Params: params, Needs: []string{base + ".describe"},
		Run: func(_ context.Context, p *check.Probe) error {
			arn := aws.ToString(f.sum.get().StreamARN)
			p.Param("arn", arn)
			p.Param("consumer_arn", consumerARN(arn, cmp.Or(consumer, "*")))
			return check.Skip("Registering a consumer changes the stream, so the preflight doesn't. The first read registers the consumer with kinesis:RegisterStreamConsumer on {arn}, and subscribes with kinesis:SubscribeToShard on {consumer_arn}. Deregistering it at teardown takes kinesis:DeregisterStreamConsumer on {consumer_arn}.")
		},
	}}
}

var (
	appendOnly = check.Reason{
		Name:    "write_mode_unsupported",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "Kinesis streams only take appends, and the pipeline writes {stream} with {mode}.",
		Fix:     "Write {stream} with the append mode in the pipeline.",
	}
	keysUnusable = check.Reason{
		Name:    "partition_key_unusable",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The destination can't make partition keys for {stream}.",
	}
	targetMissing = check.Reason{
		Name:    check.NotFound.Name,
		Code:    check.NotFound.Code,
		Message: "{target} doesn't exist in {region}, and createStreams is off.",
		Fix:     "Create {target} in {region}, or set createStreams to true.",
	}
	targetNotActive = check.Reason{
		Name:    "stream_not_active",
		Code:    protocolv1.ErrorCode_ERROR_CODE_TRANSIENT,
		Message: "{target} is {status}: a write waits until it's active.",
	}
	targetDiffers = check.Reason{
		Name:    "stream_differs",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{target} differs from the config: {differences}. The destination writes to it as it is: streamMode, shards, retentionHours and kmsKeyId apply only to streams it creates.",
		Fix:     "Change {target} in Kinesis if it should match the config, or change the config to match it.",
	}
)

// Checks sign in to Kinesis, and reach the schema registry if the format
// needs one.
func (d *Destination) Checks() []check.Check {
	return connectionChecks(newConn(&d.cfg.Connection), true)
}

// Preflight checks, for each stream a pipeline would write, its write mode,
// its partition keys, and the Kinesis stream it goes to, named as the writer
// names it: there and as the config would create it, or missing and to be
// created. It writes nothing.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	k := newConn(&d.cfg.Connection)
	names := make([]string, len(req.Streams))
	for i, s := range req.Streams {
		names[i] = s.Name
	}
	parts := streamParts(names)
	var checks []check.Check
	var targets []string
	seen := map[string]bool{}
	for _, s := range req.Streams {
		if seen[s.Name] {
			continue
		}
		seen[s.Name] = true
		sc := req.Schemas[s.Name]
		t := target(req.Namespace, d.cfg.Namespace, tableOf(s.Name, sc))
		targets = append(targets, t)
		base := "stream." + parts[s.Name]
		checks = append(checks, check.Check{
			Name: base + ".mode", Kind: check.Configuration, Title: "Write {stream} by appending",
			Params: check.Params{"stream": s.Name, "mode": cmp.Or(writeModes[s.Mode], fmt.Sprint(int(s.Mode)))},
			Run: func(context.Context, *check.Probe) error {
				if !DestinationCapabilities.SupportsMode(s.Mode) {
					return appendOnly.Fail()
				}
				return nil
			},
		})
		if sc != nil && d.cfg.PartitionKey.Mode != partitionRandom {
			checks = append(checks, d.keysCheck(base, s, sc))
		}
		checks = append(checks, d.targetCheck(k, base, s.Name, t))
	}
	if len(targets) == 0 {
		return checks
	}
	ns := cmp.Or(req.Namespace, d.cfg.Namespace)
	return append(checks, check.Check{
		Name: "put", Kind: check.Permission, Title: "Put records into the pipeline's Kinesis streams",
		Run: func(ctx context.Context, p *check.Probe) error {
			if _, err := k.client(ctx); err != nil {
				return err
			}
			arns := k.streamARN(ns + ".*")
			if ns == "" {
				all := make([]string, len(targets))
				for i, t := range targets {
					all[i] = k.streamARN(t)
				}
				arns = list(all)
			}
			p.Param("arns", arns)
			return check.Skip("Putting a record writes to a stream, so the preflight doesn't. Writing takes kinesis:DescribeStreamSummary and kinesis:PutRecords on {arns}, and kms:GenerateDataKey on the key of a stream encrypted with a customer-managed KMS key.")
		},
	})
}

// keysCheck checks that the columns partition keys are made of are the
// stream's. The engine adds _openetl_id to every batch, so a stream without
// a primary key still has partition keys.
func (d *Destination) keysCheck(base string, s destination.Stream, sc *arrow.Schema) check.Check {
	field, fix := "partitionKey.mode", "Give {stream} the primary key columns it names, or set partitionKey.mode to fields or random."
	if d.cfg.PartitionKey.Mode == partitionByFields {
		field, fix = "partitionKey.fields", "Name columns of {stream} in partitionKey.fields, or set partitionKey.mode to key or random."
	}
	return check.Check{
		Name: base + ".keys", Kind: check.Configuration, Title: "Make partition keys for {stream}",
		Field: field, Params: check.Params{"stream": s.Name},
		Run: func(_ context.Context, p *check.Probe) error {
			cols, err := keyColumns(d.cfg.PartitionKey.Mode, sc, s.PrimaryKey, d.cfg.PartitionKey.Fields, true)
			if err != nil {
				f := keysUnusable.Wrap(err)
				f.Fix = fix
				return f
			}
			if len(cols) == 0 {
				p.Found("Partition keys are each row's " + idColumn + ".")
				return nil
			}
			p.Param("columns", strings.Join(cols, ", "))
			p.Found("Partition keys are made of {columns}.")
			return nil
		},
	}
}

// targetCheck checks the Kinesis stream a pipeline's stream goes to.
func (d *Destination) targetCheck(k *conn, base, stream, name string) check.Check {
	cfg := &d.cfg
	c := check.Check{
		Name: base + ".target", Kind: check.Resource, Title: "Find the Kinesis stream {target}",
		Description: "Describes the Kinesis stream the destination writes {stream} to.",
		Params:      check.Params{"stream": stream, "target": name}, Timeout: callTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := k.client(ctx)
			if err != nil {
				return err
			}
			p.Param("region", k.ac.Region)
			res, err := cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
			switch {
			case apiCode(err) == "ResourceNotFoundException" && !cfg.createsStreams():
				return targetMissing.Fail()
			case apiCode(err) == "ResourceNotFoundException":
				p.Param("arn", k.streamARN(name))
				p.Param("creates", creates(cfg))
				p.Param("actions", and(createActions(cfg)))
				if cfg.KMSKeyID != "" {
					return check.Skip("{target} doesn't exist yet: the first write creates it {creates}. Creating a stream changes the account, so the preflight doesn't. It takes {actions} on {arn}, and kms:GenerateDataKey on kmsKeyId's key.")
				}
				return check.Skip("{target} doesn't exist yet: the first write creates it {creates}. Creating a stream changes the account, so the preflight doesn't. It takes {actions} on {arn}.")
			case err != nil:
				return call{action: "kinesis:DescribeStreamSummary", resource: k.streamARN(name)}.failure(err)
			}
			sum := res.StreamDescriptionSummary
			k.learn(sum.StreamARN)
			describe(p, sum)
			if !isActive(sum) {
				return check.Warn(targetNotActive.Fail())
			}
			if diff := differs(cfg, sum); diff != "" {
				p.Param("differences", diff)
				if cfg.setup() {
					if pending, err := pendingSetup(ctx, cl, name); err == nil && pending {
						p.Found("{target} differs from the config: {differences}. The destination created it and hasn't finished setting it up, which its next write does.")
						return nil
					}
				}
				return check.Warn(targetDiffers.Fail())
			}
			p.Found("{target} is {status}, with {shards} open shards, {retention} of retention and {encryption}.")
			return nil
		},
	}
	if !cfg.createsStreams() {
		c.Field = "createStreams"
	}
	return c
}

// creates says how the destination creates a stream.
func creates(cfg *DestinationConfig) string {
	out := "on-demand"
	if cfg.StreamMode == streamProvisioned {
		out = fmt.Sprintf("provisioned with %d shards", cfg.Shards)
	}
	out += ", with " + hours(max(cfg.RetentionHours, minRetentionHours)) + " of retention"
	if cfg.KMSKeyID != "" {
		return out + ", encrypted with kmsKeyId"
	}
	return out + ", unencrypted"
}

// createActions are the IAM actions creating a stream takes.
func createActions(cfg *DestinationConfig) []string {
	out := []string{"kinesis:CreateStream", "kinesis:DescribeStream", "kinesis:DescribeStreamSummary"}
	if cfg.setup() {
		out = append(out, "kinesis:AddTagsToStream", "kinesis:ListTagsForStream", "kinesis:RemoveTagsFromStream")
	}
	if cfg.RetentionHours > minRetentionHours {
		out = append(out, "kinesis:IncreaseStreamRetentionPeriod")
	}
	if cfg.KMSKeyID != "" {
		out = append(out, "kinesis:StartStreamEncryption")
	}
	return out
}
