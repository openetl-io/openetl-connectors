package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
	transient     = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
	// How long the checks that talk to the cluster may take, each.
	clusterTimeout = 20 * time.Second
)

var (
	brokersDown = check.Reason{
		Name: "brokers_unreachable", Code: transient,
		Message: "None of the {count} bootstrap brokers answered.",
		Fix:     "Check brokers in the config, and the network path from the runner to them.",
	}
	advertised = check.Reason{
		Name: "advertised_unreachable", Code: transient,
		Message: "The cluster's brokers advertise {unreachable}, which the runner can't reach, so it can only bootstrap.",
		Fix:     "Set the brokers' advertised.listeners to addresses the runner can reach, or open the network path to them.",
	}
	noMatch = check.Reason{
		Name: "no_topics", Code: configInvalid,
		Message: "No topic the credentials can describe is one the config offers, so the catalog is empty.",
		Fix:     "Check topics and topicPattern, and that the credentials may DESCRIBE the topics.",
	}
	noSubject = check.Reason{
		Name: "subject_missing", Code: configInvalid,
		Message: "The schema registry has no subject {subject}, so the catalog declares no fields of {topic}'s values.",
		Fix:     "Register {topic}'s value schema under {subject}. Until then, records still decode by the schema IDs they carry, and their fields' types come from their values.",
	}
	shortRetention = check.Reason{
		Name: "retention_below_interval", Code: configInvalid,
		Message: "{topic} keeps records for {retention}, less than the {interval} between the pipeline's syncs, so Kafka can delete records before a sync reads them.",
		Fix:     "Sync more often, or raise {topic}'s retention.ms.",
	}
	readOnlyMode = check.Reason{
		Name: "sync_mode_unsupported", Code: configInvalid,
		Message: "{stream} is read with {mode}, and a topic is only read with stream.",
		Fix:     "Read {stream} with the stream sync mode.",
	}
	appendOnly = check.Reason{
		Name: "write_mode_unsupported", Code: configInvalid,
		Message: "{stream} is written in {mode}, and a topic is only appended to.",
		Fix:     "Write {stream} in append mode.",
	}
	noTopic = check.Reason{
		Name: "topic_missing", Code: configInvalid,
		Message: "The topic {topic} doesn't exist, and createTopics is off.",
		Fix:     "Create {topic}, or set createTopics to true.",
	}
	clearPassword = check.Reason{
		Name: "credentials_unencrypted", Code: configInvalid,
		Message: "PLAIN without TLS sends the password across the network as it is, and the records unencrypted.",
		Fix:     "Set tls to true, on a listener that has it, or use scram-sha-256 or scram-sha-512.",
	}
	notCompacted = check.Reason{
		Name: "marker_topic_not_compacted", Code: configInvalid,
		Message: "{topic}'s cleanup.policy is {policy}, so the destination's markers expire with its retention, and recovery can't find an epoch's marker once it has.",
		Fix:     "Set cleanup.policy=compact on {topic}.",
	}
)

// clusterProbe is one list's client of the cluster, made by the first check
// that needs it and closed once the list has run.
type clusterProbe struct {
	c *Connection

	mu sync.Mutex
	cl *kgo.Client
	// How many bootstrap brokers accepted a connection.
	reached atomic.Int32
}

func (cp *clusterProbe) client(p *check.Probe) (*kgo.Client, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.cl == nil {
		cl, err := cp.c.client(kgo.RetryTimeout(clusterTimeout))
		if err != nil {
			return nil, err
		}
		cp.cl = cl
		p.Cleanup(func(context.Context) { cl.Close() })
	}
	return cp.cl, nil
}

// principal is who the credentials sign in as, for messages.
func (c *Connection) principal() string {
	switch c.SASLMechanism {
	case saslPlain, saslScram256, saslScram512:
		return "User:" + c.SASLUsername
	case saslOAuth:
		return "the OAuth client " + c.OAuthClientID
	case saslMSKIAM:
		return "the AWS identity"
	}
	return "the connector's identity"
}

// credentialField is the config field of the credentials.
func (c *Connection) credentialField() string {
	switch c.SASLMechanism {
	case saslPlain, saslScram256, saslScram512:
		return "saslPassword"
	case saslOAuth:
		return "oauthClientSecret"
	case saslMSKIAM:
		return "saslMechanism"
	}
	return "brokers"
}

// failure is err, from a request to the cluster, as a check's failure: a
// SASL failure is the credentials', a refused operation names it.
func (c *Connection) failure(err error, privilege, object string) error {
	err = classify(err)
	op, isOp := errors.AsType[*net.OpError](err)
	switch {
	case isOp && op.Op == "remote error":
		// An alert the broker sent, such as for a client certificate it
		// doesn't trust.
		return check.TLSFailed.Wrap(err, "address", strings.Join(c.Brokers, ", "))
	case errors.Is(err, kerr.SaslAuthenticationFailed), errors.Is(err, kerr.IllegalSaslState), errors.Is(err, kerr.UnsupportedSaslMechanism):
		return check.CredentialsRejected.Wrap(err)
	case privilege != "" && (errors.Is(err, kerr.TopicAuthorizationFailed) || errors.Is(err, kerr.ClusterAuthorizationFailed) ||
		errors.Is(err, kerr.GroupAuthorizationFailed) || errors.Is(err, kerr.TransactionalIDAuthorizationFailed)):
		return check.PrivilegeMissing.Wrap(err, "principal", c.principal(), "privilege", privilege, "object", object)
	}
	return err
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

// connectionChecks reach each bootstrap broker, agree on TLS with it, sign
// in, reach the brokers the cluster advertises, and reach and sign in to the
// schema registry: what both roles need. One dead bootstrap broker among
// several only warns.
func (c *Connection) connectionChecks(cp *clusterProbe) []check.Check {
	var checks []check.Check
	var last []string
	several := len(c.Brokers) > 1
	tlsField := "tls"
	switch {
	case c.TLSCA != "":
		tlsField = "tlsCa"
	case !c.TLS && c.TLSCert != "":
		tlsField = "tlsCert"
	}
	for i, name := range itemNames("broker", c.Brokers) {
		dial := netcheck.Dial(c.Brokers[i])
		dial.Name, dial.Title, dial.Field, dial.Advisory = name+".connect", "Reach the broker {address}", "brokers", several
		run := dial.Run
		dial.Run = func(ctx context.Context, p *check.Probe) error {
			err := run(ctx, p)
			if err == nil {
				cp.reached.Add(1)
			}
			return err
		}
		checks = append(checks, dial)
		end := dial.Name
		if c.tc != nil {
			t := netcheck.TLS(c.Brokers[i], c.tc)
			t.Name, t.Title, t.Field, t.Advisory, t.Needs = name+".tls", "Agree on TLS with {address}", tlsField, several, []string{dial.Name}
			checks = append(checks, t)
			end = t.Name
		}
		last = append(last, end)
	}
	title, params := "Sign in to the cluster", check.Params{"count": strconv.Itoa(len(c.Brokers))}
	if c.SASLMechanism != "" {
		title = "Sign in as {principal} with {mechanism}"
		params["principal"], params["mechanism"] = c.principal(), strings.ToUpper(c.SASLMechanism)
	}
	checks = append(checks,
		check.Check{
			Name: "kafka.auth", Kind: check.Authentication, Title: title, Field: c.credentialField(), Params: params, Needs: last, Timeout: clusterTimeout,
			Description: "Asks a bootstrap broker for the cluster's metadata, over TLS and SASL as the config sets them.",
			Run: func(ctx context.Context, p *check.Probe) error {
				if several && cp.reached.Load() == 0 {
					return brokersDown.Fail()
				}
				cl, err := cp.client(p)
				if err != nil {
					return err
				}
				m, err := kadm.NewClient(cl).BrokerMetadata(ctx)
				if err != nil {
					return c.failure(fmt.Errorf("kafka: the cluster's metadata: %w", err), "", "")
				}
				p.Param("brokers", len(m.Brokers))
				p.Param("cluster", m.Cluster)
				p.Found("The cluster {cluster} answered. Brokers: {brokers}.")
				if c.SASLMechanism == saslPlain && !c.usesTLS() {
					return check.Warn(clearPassword.Fail())
				}
				return nil
			},
		},
		check.Check{
			Name: "cluster.advertised", Kind: check.Connection, Title: "Reach the brokers the cluster advertises", Field: "brokers", Needs: []string{"kafka.auth"},
			Description: "Clients bootstrap from brokers, then talk to each broker at the address it advertises.",
			Run: func(ctx context.Context, p *check.Probe) error {
				cl, err := cp.client(p)
				if err != nil {
					return err
				}
				bs, err := kadm.NewClient(cl).ListBrokers(ctx)
				if err != nil {
					return c.failure(err, "", "")
				}
				var bad []string
				var mu sync.Mutex
				var wg sync.WaitGroup
				for _, b := range bs {
					addr := net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port)))
					wg.Go(func() {
						if err := c.dial(ctx, addr); err != nil {
							mu.Lock()
							bad = append(bad, addr)
							mu.Unlock()
						}
					})
				}
				wg.Wait()
				p.Param("count", len(bs))
				if len(bad) > 0 {
					slices.Sort(bad)
					return advertised.Fail("unreachable", strings.Join(bad, ", "))
				}
				p.Found("The runner reaches every broker the cluster advertises: {count}.")
				return nil
			},
		},
	)
	return append(checks, c.RegistryChecks(clusterTimeout)...)
}

// dial connects to addr as the clients do, over TLS if the config uses it.
func (c *Connection) dial(ctx context.Context, addr string) error {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	var conn net.Conn
	var err error
	if c.tc != nil {
		conn, err = (&tls.Dialer{Config: c.tc}).DialContext(ctx, "tcp", addr)
	} else {
		conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return err
	}
	return conn.Close()
}

// topicInfo is what a Metadata request says of a topic.
type topicInfo struct {
	partitions int
	// Nil: the broker didn't say.
	ops []kmsg.ACLOperation
	err error
}

func (t topicInfo) may(op kmsg.ACLOperation) bool {
	return t.ops == nil || slices.Contains(t.ops, op)
}

// describeTopics asks for topics' metadata with the operations the
// credentials may do on each, without creating any.
func describeTopics(ctx context.Context, cl *kgo.Client, topics ...string) (map[string]topicInfo, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	req.IncludeTopicAuthorizedOperations = true
	for _, t := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	out := make(map[string]topicInfo, len(resp.Topics))
	for _, t := range resp.Topics {
		if t.Topic == nil {
			continue
		}
		ti := topicInfo{partitions: len(t.Partitions), err: kerr.ErrorForCode(t.ErrorCode)}
		if t.AuthorizedOperations != math.MinInt32 {
			ti.ops = kadm.DecodeACLOperations(t.AuthorizedOperations)
			if ti.ops == nil {
				ti.ops = []kmsg.ACLOperation{}
			}
		}
		out[*t.Topic] = ti
	}
	return out, nil
}

// topicConfig is one of a topic's configs; empty if the credentials may not
// describe them.
func topicConfig(ctx context.Context, cl *kgo.Client, topic, key string) string {
	rcs, err := kadm.NewClient(cl).DescribeTopicConfigs(ctx, topic)
	if err != nil || len(rcs) == 0 || rcs[0].Err != nil {
		return ""
	}
	for _, c := range rcs[0].Configs {
		if c.Key == key && c.Value != nil {
			return *c.Value
		}
	}
	return ""
}

// readable fails unless the topic exists and the credentials may describe
// and read it.
func (c *Connection) readable(ctx context.Context, p *check.Probe, cl *kgo.Client, topic string) error {
	ts, err := describeTopics(ctx, cl, topic)
	if err != nil {
		return c.failure(err, "DESCRIBE", "the topic "+topic)
	}
	t := ts[topic]
	switch {
	case errors.Is(t.err, kerr.TopicAuthorizationFailed):
		return c.failure(t.err, "DESCRIBE", "the topic "+topic)
	case errors.Is(t.err, kerr.UnknownTopicOrPartition):
		return check.NotFound.Wrap(t.err, "resource", "The topic "+topic)
	case t.err != nil:
		return c.failure(t.err, "", "")
	case !t.may(kmsg.ACLOperationRead):
		return check.PrivilegeMissing.Fail("principal", c.principal(), "privilege", "READ", "object", "the topic "+topic)
	}
	p.Param("partitions", t.partitions)
	return nil
}

// Checks are the connection's, then that the topics the config names exist
// and the credentials may read them, or that some topic is offered.
func (s *Source) Checks() []check.Check {
	cp := &clusterProbe{c: &s.cfg.Connection}
	checks := s.cfg.connectionChecks(cp)
	if len(s.cfg.Topics) == 0 {
		return append(checks, check.Check{
			Name: "topics.list", Kind: check.Resource, Title: "Find the topics to offer", Field: "topicPattern", Needs: []string{"kafka.auth"},
			Run: func(ctx context.Context, p *check.Probe) error {
				cl, err := cp.client(p)
				if err != nil {
					return err
				}
				tds, err := kadm.NewClient(cl).ListTopics(ctx)
				if err != nil {
					return s.cfg.failure(err, "DESCRIBE", "the cluster's topics")
				}
				n := 0
				for _, t := range tds.Names() {
					if s.offered(t) {
						n++
					}
				}
				if n == 0 {
					return noMatch.Fail()
				}
				p.Param("count", n)
				p.Found("The config offers {count} topics.")
				return nil
			},
		})
	}
	return append(checks, check.Check{
		Name: "topics.read", Kind: check.Permission, Title: "Read the topics {topics}", Field: "topics",
		Params: check.Params{"topics": strings.Join(s.cfg.Topics, ", ")}, Needs: []string{"kafka.auth"},
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := cp.client(p)
			if err != nil {
				return err
			}
			for _, t := range s.cfg.Topics {
				if err := s.cfg.readable(ctx, p, cl, t); err != nil {
					return err
				}
			}
			p.Found("The credentials may read {topics}.")
			return nil
		},
	})
}

// Preflight checks, for each stream, its sync mode, that its topic exists
// and is readable, that its retention outlasts the schedule, and with Avro
// or Protobuf that its value schema is in the registry; then that the
// consumer group the offsets are committed to may be used.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	cp := &clusterProbe{c: &s.cfg.Connection}
	names := make([]string, len(req.Streams))
	for i, st := range req.Streams {
		names[i] = st.Name
	}
	var checks []check.Check
	for i, item := range itemNames("stream", names) {
		st := req.Streams[i]
		params := check.Params{"stream": st.Name, "topic": st.Name}
		mode := check.Check{
			Name: item + ".mode", Kind: check.Compatibility, Title: "Read {stream} in its sync mode", Params: params,
			Run: func(context.Context, *check.Probe) error {
				if st.SyncMode != 0 && st.SyncMode != source.Stream {
					return readOnlyMode.Fail("mode", syncModeName(st.SyncMode))
				}
				return nil
			},
		}
		topic := check.Check{
			Name: item + ".read", Kind: check.Permission, Title: "Read the topic {topic}", Params: params, Field: "topics", Timeout: clusterTimeout,
			Run: func(ctx context.Context, p *check.Probe) error {
				cl, err := cp.client(p)
				if err != nil {
					return err
				}
				if err := s.cfg.readable(ctx, p, cl, st.Name); err != nil {
					return err
				}
				p.Found("{topic} has {partitions} partitions.")
				return nil
			},
		}
		checks = append(checks, mode, topic)
		if req.Interval > 0 {
			checks = append(checks, check.Check{
				Name: item + ".retention", Kind: check.Compatibility, Title: "Keep {topic}'s records between syncs", Params: params, Needs: []string{topic.Name}, Advisory: true,
				Run: func(ctx context.Context, p *check.Probe) error {
					cl, err := cp.client(p)
					if err != nil {
						return err
					}
					v := topicConfig(ctx, cl, st.Name, "retention.ms")
					ms, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						return check.Skip("the credentials can't describe {topic}'s configs, which takes DESCRIBE_CONFIGS on it")
					}
					p.Param("interval", req.Interval.String())
					if ms < 0 {
						p.Found("{topic} keeps records forever: its retention.ms is -1.")
						return nil
					}
					p.Param("retention", (time.Duration(ms) * time.Millisecond).String())
					if time.Duration(ms)*time.Millisecond < req.Interval {
						return shortRetention.Fail()
					}
					p.Found("{topic} keeps records for {retention}, longer than the {interval} between syncs.")
					return nil
				},
			})
		}
		if s.reg != nil && (s.cfg.Format == records.Avro || s.cfg.Format == records.Protobuf) {
			subject := st.Name + "-value"
			sp := check.Params{"stream": st.Name, "topic": st.Name, "subject": subject}
			checks = append(checks, check.Check{
				Name: item + ".schema", Kind: check.Resource, Title: "Find {topic}'s value schema under {subject}", Field: "schemaRegistryUrl", Params: sp, Advisory: true, Timeout: clusterTimeout,
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
	group := s.cfg.Group
	if group == "" {
		group = "openetl-" + req.Pipeline
	}
	return append(checks, check.Check{
		Name: "group.commit", Kind: check.Permission, Title: "Commit offsets to the consumer group {group}", Field: "group",
		Params: check.Params{"group": group}, Advisory: true, Timeout: clusterTimeout,
		Description: "Offsets go to the group only for Kafka's tools to show the read's lag: reads resume from the checkpoint.",
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := cp.client(p)
			if err != nil {
				return err
			}
			gs, err := kadm.NewClient(cl).DescribeGroups(ctx, group)
			if err == nil {
				err = gs.Error()
			}
			if errors.Is(err, kerr.GroupIDNotFound) {
				return check.Skip("{group} doesn't exist yet, so the brokers can't say what the credentials may do with it: the first commit creates it, which takes READ on it")
			}
			if err != nil {
				return s.cfg.failure(err, "READ", "the group "+group)
			}
			if ops := gs[group].AuthorizedOperations; len(ops) > 0 && !slices.Contains(ops, kmsg.ACLOperationRead) {
				return check.PrivilegeMissing.Fail("principal", s.cfg.principal(), "privilege", "READ", "object", "the group "+group)
			}
			p.Found("The credentials may commit offsets to {group}.")
			return nil
		},
	})
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

// writable fails unless topic exists and the credentials may write it, or
// it doesn't and createTopics lets the destination create it, which the
// brokers would allow. It says which.
func (d *Destination) writable(ctx context.Context, p *check.Probe, cl *kgo.Client, topic string, configs map[string]*string, ops ...kmsg.ACLOperation) error {
	c := &d.cfg.Connection
	ts, err := describeTopics(ctx, cl, topic)
	if err != nil {
		return c.failure(err, "DESCRIBE", "the topic "+topic)
	}
	t := ts[topic]
	switch {
	case errors.Is(t.err, kerr.TopicAuthorizationFailed):
		return c.failure(t.err, "DESCRIBE", "the topic "+topic)
	case errors.Is(t.err, kerr.UnknownTopicOrPartition):
		if cr := d.cfg.CreateTopics; cr != nil && !*cr {
			return noTopic.Fail("topic", topic)
		}
		partitions, rf := d.cfg.Partitions, d.cfg.ReplicationFactor
		if partitions == 0 {
			partitions = -1
		}
		if rf == 0 {
			rf = -1
		}
		res, err := kadm.NewClient(cl).ValidateCreateTopics(ctx, partitions, rf, configs, topic)
		if err == nil {
			err = res.Error()
		}
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return c.failure(err, "CREATE", "the topic "+topic)
		}
		p.Found("{topic} doesn't exist yet: the destination will create it.")
		return nil
	case t.err != nil:
		return c.failure(t.err, "", "")
	}
	for _, op := range ops {
		if !t.may(op) {
			return check.PrivilegeMissing.Fail("principal", c.principal(), "privilege", op.String(), "object", "the topic "+topic)
		}
	}
	p.Param("partitions", t.partitions)
	p.Found("{topic} has {partitions} partitions.")
	return nil
}

// Checks are the connection's, then that the marker topic is there or can be
// created, and that the brokers let transactions stay open long enough.
func (d *Destination) Checks() []check.Check {
	cp := &clusterProbe{c: &d.cfg.Connection}
	marker := d.cfg.MarkerTopic
	return append(d.cfg.connectionChecks(cp),
		check.Check{
			Name: "markers.topic", Kind: check.Permission, Title: "Write markers to {topic}", Field: "markerTopic",
			Params: check.Params{"topic": marker}, Needs: []string{"kafka.auth"}, Timeout: clusterTimeout,
			Description: "Each transaction writes its epoch's marker to this compacted topic, and recovery reads it.",
			Run: func(ctx context.Context, p *check.Probe) error {
				cl, err := cp.client(p)
				if err != nil {
					return err
				}
				compact := map[string]*string{"cleanup.policy": kadm.StringPtr("compact")}
				if err := d.writable(ctx, p, cl, marker, compact, kmsg.ACLOperationRead, kmsg.ACLOperationWrite); err != nil {
					return err
				}
				if policy := topicConfig(ctx, cl, marker, "cleanup.policy"); policy != "" && !strings.Contains(policy, "compact") {
					p.Param("policy", policy)
					return check.Warn(notCompacted.Fail())
				}
				return nil
			},
		},
		check.Check{
			Name: "cluster.transaction-timeout", Kind: check.Compatibility, Title: "Keep a transaction open for a commit interval", Needs: []string{"kafka.auth"}, Timeout: clusterTimeout,
			Description: "An epoch's transaction stays open for about two commit intervals and a minute, up to the brokers' transaction.max.timeout.ms.",
			Run: func(ctx context.Context, p *check.Probe) error {
				cl, err := cp.client(p)
				if err != nil {
					return err
				}
				limit, err := transactionLimit(ctx, kadm.NewClient(cl))
				if err != nil {
					return d.cfg.failure(err, "", "")
				}
				if limit == 0 {
					return check.Skip("the credentials can't describe the brokers' configs, which takes DESCRIBE_CONFIGS on the cluster")
				}
				p.Param("found", limit.String())
				if need := timeout(DestinationCapabilities.DefaultCommitInterval); limit < need {
					return check.SettingMismatch.Fail("setting", "The brokers' transaction.max.timeout.ms", "required", "at least "+need.String())
				}
				longest := min(DestinationCapabilities.MaxCommitInterval, (limit-time.Minute)/2).Truncate(time.Second)
				p.Param("interval", longest.String())
				p.Found("Transactions may stay open {found}, enough for commit intervals up to {interval}.")
				return nil
			},
		},
	)
}

// transactionLimit is the brokers' transaction.max.timeout.ms; 0 if the
// credentials may not describe the brokers' configs, or the brokers don't
// say.
func transactionLimit(ctx context.Context, adm *kadm.Client) (time.Duration, error) {
	brokers, err := adm.ListBrokers(ctx)
	if err != nil || len(brokers) == 0 {
		return 0, err
	}
	rcs, err := adm.DescribeBrokerConfigs(ctx, brokers[0].NodeID)
	if err == nil && len(rcs) > 0 {
		err = rcs[0].Err
	}
	if errors.Is(err, kerr.ClusterAuthorizationFailed) || err == nil && len(rcs) == 0 {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	for _, c := range rcs[0].Configs {
		if c.Key != "transaction.max.timeout.ms" || c.Value == nil {
			continue
		}
		if ms, err := strconv.ParseInt(*c.Value, 10, 64); err == nil {
			return time.Duration(ms) * time.Millisecond, nil
		}
	}
	return 0, nil
}

// Preflight checks, for each stream, its write mode, and that its topic is
// writable or can be created; then that the pipeline's transactional IDs
// are the credentials' to use.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	cp := &clusterProbe{c: &d.cfg.Connection}
	w := &writer{d: d, cfg: destination.OpenConfig{Namespace: req.Namespace}}
	names := make([]string, len(req.Streams))
	for i, st := range req.Streams {
		names[i] = st.Name
	}
	var checks []check.Check
	for i, item := range itemNames("stream", names) {
		st := req.Streams[i]
		table := records.AvroName(st.Name)
		if sc := req.Schemas[st.Name]; sc != nil {
			if t, ok := sc.Metadata().GetValue(records.TableKey); ok {
				table = t
			}
		}
		params := check.Params{"stream": st.Name, "topic": w.topic(table)}
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
				Name: item + ".write", Kind: check.Permission, Title: "Write {stream} to the topic {topic}", Params: params, Field: "createTopics", Timeout: clusterTimeout,
				Run: func(ctx context.Context, p *check.Probe) error {
					cl, err := cp.client(p)
					if err != nil {
						return err
					}
					return d.writable(ctx, p, cl, w.topic(table), nil, kmsg.ACLOperationWrite)
				},
			},
		)
	}
	id := d.cfg.TransactionalIDPrefix + req.Pipeline + "-0"
	return append(checks, check.Check{
		Name: "transactions.id", Kind: check.Permission, Title: "Use the transactional IDs {prefix}{pipeline}-N", Field: "transactionalIdPrefix",
		Params: check.Params{"prefix": d.cfg.TransactionalIDPrefix, "pipeline": req.Pipeline}, Timeout: clusterTimeout,
		Description: "Each writer lane writes in transactions under IDs that start with the prefix and the pipeline. Describing one, which their WRITE allows, doesn't fence a running writer.",
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := cp.client(p)
			if err != nil {
				return err
			}
			// A cluster creates its transaction log on first use, and has no
			// coordinator until then.
			for {
				var txns kadm.DescribedTransactions
				txns, err = kadm.NewClient(cl).DescribeTransactions(ctx, id)
				if t, ok := txns[id]; ok && err == nil {
					err = t.Err
				}
				if !errors.Is(err, kerr.CoordinatorNotAvailable) && !errors.Is(err, kerr.CoordinatorLoadInProgress) && !errors.Is(err, kerr.NotCoordinator) {
					break
				}
				select {
				case <-ctx.Done():
					return err
				case <-time.After(500 * time.Millisecond):
				}
			}
			switch {
			case errors.Is(err, kerr.TransactionalIDNotFound):
			case errors.Is(err, kerr.UnsupportedVersion):
				return check.Skip("the brokers can't describe transactions, which takes Kafka 3.0 or later")
			case err != nil:
				return d.cfg.failure(err, "WRITE", "the transactional IDs starting "+d.cfg.TransactionalIDPrefix+req.Pipeline)
			}
			p.Found("The credentials may describe the pipeline's transactional IDs. Transactions take WRITE on them, which only a producer that fenced the pipeline's writers could try.")
			return nil
		},
	})
}
