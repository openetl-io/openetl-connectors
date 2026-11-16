package kinesis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
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

const (
	configInvalid = protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID
	// How long each check that calls AWS may take.
	awsTimeout = 20 * time.Second
	// How long the identity lookup may take, which a network without a
	// route to STS mustn't hold up.
	identityTimeout = 5 * time.Second
)

var (
	noMatch = check.Reason{
		Name: "no_streams", Code: configInvalid,
		Message: "No stream the credentials can list is one the config offers, so the catalog is empty.",
		Fix:     "Check streams and streamPattern, and the region.",
	}
	noSubject = check.Reason{
		Name: "subject_missing", Code: configInvalid,
		Message: "The schema registry has no subject {subject}, so the catalog declares no fields of {stream}'s data.",
		Fix:     "Register {stream}'s value schema under {subject}. Until then, records still decode by the schema IDs they carry, and their fields' types come from their values.",
	}
	shortRetention = check.Reason{
		Name: "retention_below_interval", Code: configInvalid,
		Message: "{stream} keeps records for {retention}, less than the {interval} between the pipeline's syncs, so Kinesis can discard records before a sync reads them.",
		Fix:     "Sync more often, or raise {stream}'s retention period.",
	}
	readOnlyMode = check.Reason{
		Name: "sync_mode_unsupported", Code: configInvalid,
		Message: "{stream} is read with {mode}, and a Kinesis stream is only read with stream.",
		Fix:     "Read {stream} with the stream sync mode.",
	}
	appendOnly = check.Reason{
		Name: "write_mode_unsupported", Code: configInvalid,
		Message: "{stream} is written in {mode}, and a Kinesis stream is only appended to.",
		Fix:     "Write {stream} in append mode.",
	}
	noStream = check.Reason{
		Name: "stream_missing", Code: configInvalid,
		Message: "The Kinesis stream {target} doesn't exist, and createStreams is off.",
		Fix:     "Create {target}, or set createStreams to true.",
	}
	notActive = check.Reason{
		Name: "stream_not_active", Code: protocolv1.ErrorCode_ERROR_CODE_TRANSIENT,
		Message: "The Kinesis stream {name} is {status}, not ACTIVE.",
		Fix:     "Wait for {name} to become active, or recreate it if it's being deleted.",
	}
)

// awsProbe is one list's Kinesis client and, without an endpoint, the
// identity STS gives, made by the first check that needs them.
type awsProbe struct {
	c  *Connection
	cl *awskinesis.Client
	// The identity's ARN, if STS said.
	identity string
	// What listing streams returned.
	listed error
}

func (ap *awsProbe) client(ctx context.Context) (*awskinesis.Client, error) {
	if ap.cl != nil {
		return ap.cl, nil
	}
	cl, err := ap.c.client(ctx)
	if err != nil {
		return nil, check.ConfigInvalid.Wrap(err)
	}
	ap.cl = cl
	if ap.c.Endpoint == "" {
		ap.identity = ap.c.identity(ctx)
	}
	return cl, nil
}

func (ap *awsProbe) principal() string {
	if ap.identity != "" {
		return ap.identity
	}
	return "The AWS identity"
}

// failure is err as a check's failure: a denied action names it, on object.
func (ap *awsProbe) failure(err error, action, object string) error {
	switch apiCode(err) {
	case "AccessDeniedException", "AccessDenied", "KMSAccessDeniedException":
		return check.PrivilegeMissing.Wrap(err, "principal", ap.principal(), "privilege", action, "object", object)
	case "ResourceNotFoundException":
		return check.NotFound.Wrap(err, "resource", "The Kinesis stream "+object)
	}
	return classify(err)
}

func (c *Connection) credentialField() string {
	if c.AccessKeyID.IsZero() {
		return "region"
	}
	return "secretAccessKey"
}

// address is where Kinesis is served, as host:port, and whether it starts
// with TLS; empty if the region is the AWS config's, unknown until it loads.
func (c *Connection) address() (string, bool) {
	if c.Endpoint == "" {
		if c.Region == "" {
			return "", false
		}
		host := "kinesis." + c.Region + ".amazonaws.com"
		if strings.HasPrefix(c.Region, "cn-") {
			host += ".cn"
		}
		return host + ":443", true
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" {
		return "", false
	}
	tls := u.Scheme != "http"
	if u.Port() != "" {
		return u.Host, tls
	}
	if tls {
		return net.JoinHostPort(u.Hostname(), "443"), true
	}
	return net.JoinHostPort(u.Hostname(), "80"), false
}

// connectionChecks reach Kinesis, then sign in: listing streams shows
// Kinesis takes the credentials.
func (c *Connection) connectionChecks(ap *awsProbe) []check.Check {
	var checks []check.Check
	if addr, tls := c.address(); addr != "" {
		if tls {
			checks = netcheck.ReachTLS(addr, nil)
		} else {
			checks = netcheck.Reach(addr)
		}
		for i := range checks {
			checks[i].Field = "endpoint"
			if c.Endpoint == "" {
				checks[i].Field = "region"
				// A runner may reach AWS only through a proxy, which the
				// SDK takes and a dial doesn't.
				checks[i].Advisory = true
			}
		}
	}
	var needs []string
	if len(checks) > 0 {
		needs = []string{checks[len(checks)-1].Name}
	}
	auth := check.Check{
		Name: "kinesis.auth", Kind: check.Authentication, Title: "Sign in to Kinesis", Field: c.credentialField(), Needs: needs, Timeout: awsTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := ap.client(ctx)
			if err != nil {
				return err
			}
			_, ap.listed = cl.ListStreams(ctx, &awskinesis.ListStreamsInput{Limit: aws.Int32(1)})
			switch err := ap.listed; {
			case err == nil:
			case apiCode(err) == "AccessDeniedException":
				// Kinesis knows the identity, whatever it may do.
			default:
				return ap.failure(fmt.Errorf("kinesis: list streams: %w", err), "", "")
			}
			if ap.identity != "" {
				p.Param("identity", ap.identity)
				p.Found("Signed in as {identity}.")
			}
			return nil
		},
	}
	return append(append(checks, auth), c.RegistryChecks(awsTimeout)...)
}

// identity is the credentials' ARN as STS gives it, or empty.
func (c *Connection) identity(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, identityTimeout)
	defer cancel()
	ac, err := c.awsConfig(ctx)
	if err != nil {
		return ""
	}
	out, err := sts.NewFromConfig(ac).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return ""
	}
	return aws.ToString(out.Arn)
}

// Checks are the connection's, then that listing streams offers some, or
// that the streams the config names exist.
func (s *Source) Checks() []check.Check {
	ap := &awsProbe{c: &s.cfg.Connection}
	field := "streamPattern"
	if len(s.cfg.Streams) > 0 {
		field = "streams"
	}
	return append(s.cfg.connectionChecks(ap), check.Check{
		Name: "streams.list", Kind: check.Permission, Title: "Find the streams to offer", Field: field, Needs: []string{"kinesis.auth"}, Timeout: awsTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := ap.client(ctx)
			if err != nil {
				return err
			}
			if ap.listed != nil {
				return ap.failure(ap.listed, "kinesis:ListStreams", "the account's streams")
			}
			listed, err := listStreams(ctx, cl)
			if err != nil {
				return ap.failure(err, "kinesis:ListStreams", "the account's streams")
			}
			names := map[string]bool{}
			for _, n := range listed {
				names[n] = true
			}
			for _, n := range s.cfg.Streams {
				if !names[n] {
					return check.NotFound.Fail("resource", "The Kinesis stream "+n)
				}
			}
			count := 0
			for n := range names {
				if s.offered(n) {
					count++
				}
			}
			if count == 0 {
				return noMatch.Fail()
			}
			p.Param("count", count)
			p.Found("The config offers {count} streams.")
			return nil
		},
	})
}

// Preflight checks, for each stream, its sync mode, that it's active, that
// its shards can be read, that its retention outlasts the schedule, and
// with Avro or Protobuf that its value schema is in the registry.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	ap := &awsProbe{c: &s.cfg.Connection}
	names := make([]string, len(req.Streams))
	for i, st := range req.Streams {
		names[i] = st.Name
	}
	var checks []check.Check
	for i, item := range itemNames("stream", names) {
		st := req.Streams[i]
		params := check.Params{"stream": st.Name}
		var retention time.Duration
		checks = append(checks,
			check.Check{
				Name: item + ".mode", Kind: check.Compatibility, Title: "Read {stream} in its sync mode", Params: params,
				Run: func(context.Context, *check.Probe) error {
					if st.SyncMode != 0 && st.SyncMode != source.Stream {
						return readOnlyMode.Fail("mode", syncModeName(st.SyncMode))
					}
					return nil
				},
			},
			check.Check{
				Name: item + ".describe", Kind: check.Resource, Title: "Find the Kinesis stream {stream}", Params: params, Field: "streams", Timeout: awsTimeout,
				Run: func(ctx context.Context, p *check.Probe) error {
					sum, err := ap.describe(ctx, p, st.Name)
					if err != nil {
						return err
					}
					retention = time.Duration(aws.ToInt32(sum.RetentionPeriodHours)) * time.Hour
					return nil
				},
			},
			check.Check{
				Name: item + ".read", Kind: check.Permission, Title: "Read {stream}'s shards", Params: params, Field: "streams", Needs: []string{item + ".describe"}, Timeout: awsTimeout,
				Description: "Lists the shards and reads one from its latest record, which reads nothing.",
				Run: func(ctx context.Context, p *check.Probe) error {
					return ap.readable(ctx, p, st.Name)
				},
			},
		)
		if req.Interval > 0 {
			checks = append(checks, check.Check{
				Name: item + ".retention", Kind: check.Compatibility, Title: "Keep {stream}'s records between syncs", Params: params, Needs: []string{item + ".describe"}, Advisory: true,
				Run: func(_ context.Context, p *check.Probe) error {
					p.Param("interval", req.Interval.String())
					p.Param("retention", retention.String())
					if retention < req.Interval {
						return shortRetention.Fail()
					}
					p.Found("{stream} keeps records for {retention}, longer than the {interval} between syncs.")
					return nil
				},
			})
		}
		if s.reg != nil && (s.cfg.Format == records.Avro || s.cfg.Format == records.Protobuf) {
			subject := st.Name + "-value"
			checks = append(checks, check.Check{
				Name: item + ".schema", Kind: check.Resource, Title: "Find {stream}'s value schema under {subject}", Field: "schemaRegistryUrl",
				Params: check.Params{"stream": st.Name, "subject": subject}, Advisory: true, Timeout: awsTimeout,
				Run: func(ctx context.Context, p *check.Probe) error {
					ss, err := s.reg.SchemaByVersion(ctx, subject, -1)
					var re *sr.ResponseError
					switch {
					case errors.As(err, &re) && re.StatusCode == http.StatusNotFound:
						return noSubject.Wrap(err)
					case err != nil:
						return err
					}
					p.Param("version", ss.Version)
					p.Found("{subject} is at version {version}.")
					return nil
				},
			})
		}
	}
	return checks
}

// describe is a stream's summary; it fails unless the stream is active, or
// updating, which still reads and writes.
func (ap *awsProbe) describe(ctx context.Context, p *check.Probe, name string) (*types.StreamDescriptionSummary, error) {
	cl, err := ap.client(ctx)
	if err != nil {
		return nil, err
	}
	out, err := cl.DescribeStreamSummary(ctx, &awskinesis.DescribeStreamSummaryInput{StreamName: aws.String(name)})
	if err != nil {
		return nil, ap.failure(err, "kinesis:DescribeStreamSummary", name)
	}
	sum := out.StreamDescriptionSummary
	status := string(sum.StreamStatus)
	p.Param("name", name)
	if sum.StreamStatus != types.StreamStatusActive && sum.StreamStatus != types.StreamStatusUpdating {
		return nil, notActive.Fail("status", status)
	}
	mode := "provisioned"
	if sum.StreamModeDetails != nil && sum.StreamModeDetails.StreamMode == types.StreamModeOnDemand {
		mode = "on-demand"
	}
	p.Param("mode", mode)
	p.Param("shards", strconv.Itoa(int(aws.ToInt32(sum.OpenShardCount))))
	p.Param("retention", (time.Duration(aws.ToInt32(sum.RetentionPeriodHours)) * time.Hour).String())
	p.Param("status", status)
	p.Found("{name} is {status}, {mode}, with {shards} open shards, and keeps records for {retention}.")
	return sum, nil
}

// readable lists a stream's shards and reads the first from its latest
// record.
func (ap *awsProbe) readable(ctx context.Context, p *check.Probe, name string) error {
	cl, err := ap.client(ctx)
	if err != nil {
		return err
	}
	shards, err := cl.ListShards(ctx, &awskinesis.ListShardsInput{StreamName: aws.String(name), MaxResults: aws.Int32(1)})
	if err != nil {
		return ap.failure(err, "kinesis:ListShards", name)
	}
	if len(shards.Shards) == 0 {
		return check.Skip("{stream} has no shards to read")
	}
	shard := shards.Shards[0].ShardId
	it, err := cl.GetShardIterator(ctx, &awskinesis.GetShardIteratorInput{StreamName: aws.String(name), ShardId: shard, ShardIteratorType: types.ShardIteratorTypeLatest})
	if err != nil {
		return ap.failure(err, "kinesis:GetShardIterator", name)
	}
	if _, err := cl.GetRecords(ctx, &awskinesis.GetRecordsInput{ShardIterator: it.ShardIterator, Limit: aws.Int32(1)}); err != nil {
		return ap.failure(err, "kinesis:GetRecords", name)
	}
	p.Found("The credentials may list {stream}'s shards and read them.")
	return nil
}

// Checks are the connection's.
func (d *Destination) Checks() []check.Check {
	return d.cfg.connectionChecks(&awsProbe{c: &d.cfg.Connection})
}

// Preflight checks, for each stream, its write mode, and that its Kinesis
// stream is active, or can be created.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	ap := &awsProbe{c: &d.cfg.Connection}
	w := &writer{d: d, cfg: destination.OpenConfig{Namespace: req.Namespace}}
	names := make([]string, len(req.Streams))
	for i, st := range req.Streams {
		names[i] = st.Name
	}
	var checks []check.Check
	for i, item := range itemNames("stream", names) {
		st := req.Streams[i]
		table := st.Name
		if sc := req.Schemas[st.Name]; sc != nil {
			if t, ok := sc.Metadata().GetValue(records.TableKey); ok {
				table = t
			}
		}
		params := check.Params{"stream": st.Name, "target": w.target(table)}
		checks = append(checks,
			check.Check{
				Name: item + ".mode", Kind: check.Compatibility, Title: "Write {stream} in its write mode", Params: params,
				Run: func(context.Context, *check.Probe) error {
					if !DestinationCapabilities.SupportsMode(st.Mode) {
						return appendOnly.Fail("mode", writeModeName(st.Mode))
					}
					return nil
				},
			},
			check.Check{
				Name: item + ".write", Kind: check.Resource, Title: "Write {stream} to the Kinesis stream {target}", Params: params, Field: "createStreams", Timeout: awsTimeout,
				Description: "Writing a record is the only test of kinesis:PutRecords, and it would add the record to the stream, so this check doesn't: the credentials need it on the stream.",
				Run: func(ctx context.Context, p *check.Probe) error {
					target := w.target(table)
					_, err := ap.describe(ctx, p, target)
					if f := check.Classify(err); f == nil || f.Reason != check.NotFound.Name {
						return err
					}
					if c := d.cfg.CreateStreams; c != nil && !*c {
						return noStream.Fail()
					}
					p.Found("{target} doesn't exist yet: the destination will create it, which takes kinesis:CreateStream.")
					return nil
				},
			},
		)
	}
	return checks
}

// awsConfig is the AWS config the connection's clients are made from.
func (c *Connection) awsConfig(ctx context.Context) (aws.Config, error) {
	return awsconfig.LoadDefaultConfig(ctx, c.loadOptions()...)
}

// itemNames are each name's item name: check.Name's, made unique where two
// names give one.
func itemNames(prefix string, names []string) []string {
	out := make([]string, len(names))
	used := map[string]bool{}
	for i, n := range names {
		base := check.Name(prefix, n)
		out[i] = base
		for j := 2; used[out[i]]; j++ {
			out[i] = fmt.Sprintf("%s-%d", base, j)
		}
		used[out[i]] = true
	}
	return out
}

func syncModeName(m source.SyncMode) string {
	switch m {
	case source.FullRefresh:
		return "full_refresh"
	case source.Incremental:
		return "incremental"
	case source.CDC:
		return "cdc"
	case source.Stream:
		return "stream"
	}
	return strconv.Itoa(int(m))
}

func writeModeName(m destination.WriteMode) string {
	switch m {
	case destination.Append:
		return "append"
	case destination.Overwrite:
		return "overwrite"
	case destination.Merge:
		return "merge"
	}
	return strconv.Itoa(int(m))
}
