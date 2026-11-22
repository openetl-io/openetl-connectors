package kinesis

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

const (
	// Bounds each check that calls AWS or the schema registry.
	callTimeout = 20 * time.Second
	// A check's calls try at most this many times: a check reports what's
	// wrong rather than riding it out.
	checkAttempts = 3
)

// conn is the Kinesis client of one list of checks, made by the first check
// that needs it, and what the list's checks learn of the account.
type conn struct {
	c    *Connection
	once sync.Once
	cl   *awskinesis.Client
	ac   aws.Config
	err  error

	mu sync.Mutex
	// "arn:PARTITION:kinesis:REGION:ACCOUNT:", from an ARN Kinesis
	// returned; empty until one has.
	prefix string
}

func newConn(c *Connection) *conn { return &conn{c: c} }

var (
	configUnloadable = check.Reason{
		Name:    "aws_config_invalid",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The connector can't load its AWS configuration.",
		Fix:     "Check region, and the AWS settings the runner gives the connector.",
	}
	regionMissing = check.Reason{
		Name:    "region_missing",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "No AWS Region is set.",
		Fix:     "Set region in the config.",
	}
	credentialsMissing = check.Reason{
		Name:    "credentials_missing",
		Code:    protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID,
		Message: "The connector found no AWS credentials to sign in with.",
		Fix:     "Set accessKeyId and secretAccessKey, or name an IAM role in the source's or destination's aws.role, which the runner assumes for the connector.",
	}
)

// client is the list's client, once the AWS configuration has loaded with a
// Region.
func (k *conn) client(ctx context.Context) (*awskinesis.Client, error) {
	k.once.Do(func() {
		k.cl, k.ac, k.err = k.c.clientWith(ctx, checkAttempts)
		if k.err != nil {
			k.err = configUnloadable.Wrap(k.err)
		} else if k.ac.Region == "" {
			k.err = regionMissing.Fail()
		}
	})
	return k.cl, k.err
}

var arnPrefix = regexp.MustCompile(`^arn:[^:]+:kinesis:[^:]*:[^:]*:`)

// learn keeps the partition, Region and account of a Kinesis ARN, for the
// ARNs of fixes.
func (k *conn) learn(arn *string) {
	if p := arnPrefix.FindString(aws.ToString(arn)); p != "" {
		k.mu.Lock()
		k.prefix = p
		k.mu.Unlock()
	}
}

// streamARN is the ARN of the stream name, with REGION and ACCOUNT in it
// for what the list hasn't learned. Only after client.
func (k *conn) streamARN(name string) string {
	k.mu.Lock()
	p := k.prefix
	k.mu.Unlock()
	if p == "" {
		p = "arn:aws:kinesis:" + cmp.Or(k.ac.Region, "REGION") + ":ACCOUNT:"
	}
	return p + "stream/" + name
}

// consumerARN is what IAM policies name a stream's consumer by, whatever
// its registration's timestamp.
func consumerARN(streamARN, name string) string { return streamARN + "/consumer/" + name + ":*" }

// call is an AWS call a check makes, for its failures: the IAM action and
// the resource it acts on, and for a stream encrypted with a KMS key, what
// the call needs of the key.
type call struct {
	action, resource string
	// Empty: kms:Decrypt.
	kmsAction string
}

// deniedText is how AWS says which identity lacks which action on which
// resource.
var deniedText = regexp.MustCompile(`User: (\S+) is not authorized to perform: (\S+) on resource: (\S+)`)

// failure is err, which the call returned, as a check's failure. Nil for a
// nil err.
func (c call) failure(err error) error {
	if err == nil {
		return nil
	}
	code := apiCode(err)
	switch {
	case accessDenied(code):
		principal, action, resource := "The connector's AWS identity", c.action, c.resource
		if m := deniedText.FindStringSubmatch(apiMessage(err)); m != nil {
			principal, action, resource = m[1], m[2], strings.TrimRight(m[3], ".,")
		}
		f := check.PrivilegeMissing.Wrap(err, "principal", principal, "privilege", action, "object", resource)
		f.Fix = "Allow {privilege} on {object} in an IAM policy of the connector's AWS identity: the role in the source's or destination's aws.role, or the user that accessKeyId belongs to."
		return f
	case code == "KMSAccessDeniedException":
		return kmsDenied.Wrap(err, "kms_action", cmp.Or(c.kmsAction, "kms:Decrypt"))
	case code == "ResourceNotFoundException":
		return check.NotFound.Wrap(err, "resource", c.resource)
	case code == "InvalidArgumentException" || code == "ValidationException":
		return check.ConfigInvalid.Wrap(err)
	}
	if r, ok := awsReasons[code]; ok {
		return r.Wrap(err)
	}
	return check.Classify(err)
}

// signIn gets the credentials, and lists a stream with them. A destination
// needn't list streams, so for one, Kinesis refusing the list is a pass:
// it signed the connector in to refuse it.
func (k *conn) signIn(destination bool) check.Check {
	field := "region"
	if !k.c.AccessKeyID.IsZero() {
		field = "accessKeyId"
	}
	return check.Check{
		Name: "kinesis.sign-in", Kind: check.Authentication, Title: "Sign in to Kinesis",
		Description: "Gets the AWS credentials, and lists a stream with them.",
		Field:       field, Timeout: callTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			cl, err := k.client(ctx)
			if err != nil {
				return err
			}
			p.Param("region", k.ac.Region)
			if k.ac.Credentials != nil {
				if _, err := k.ac.Credentials.Retrieve(ctx); err != nil {
					return credentialsMissing.Wrap(err)
				}
			}
			_, err = cl.ListStreams(ctx, &awskinesis.ListStreamsInput{Limit: aws.Int32(1)})
			switch {
			case destination && accessDenied(apiCode(err)):
				p.Found("Kinesis in {region} accepts the credentials. They can't list streams, which the destination doesn't need.")
				return nil
			case err != nil:
				return call{action: "kinesis:ListStreams", resource: "*"}.failure(err)
			}
			p.Found("Kinesis in {region} accepts the credentials.")
			return nil
		},
	}
}

// endpointChecks are net.dns and net.connect for a custom endpoint's host
// and port; none for the Region's own, or for one a proxy reaches for the
// connector.
func endpointChecks(endpoint string) []check.Check {
	u, err := url.Parse(endpoint)
	if endpoint == "" || err != nil || u.Hostname() == "" {
		return nil
	}
	if proxy, err := http.ProxyFromEnvironment(&http.Request{URL: u}); err != nil || proxy != nil {
		return nil
	}
	port := u.Port()
	switch {
	case port != "":
	case u.Scheme == "http":
		port = "80"
	default:
		port = "443"
	}
	checks := netcheck.Reach(net.JoinHostPort(u.Hostname(), port))
	for i := range checks {
		checks[i].Field = "endpoint"
	}
	return checks
}

// connectionChecks reach Kinesis and sign in, and reach the schema registry
// if the format needs one.
func connectionChecks(k *conn, destination bool) []check.Check {
	checks := endpointChecks(k.c.Endpoint)
	signIn := k.signIn(destination)
	if len(checks) > 0 {
		signIn.Needs = []string{"net.connect"}
	}
	checks = append(checks, signIn)
	if r, ok := registryCheck(&k.c.Config); ok {
		checks = append(checks, r)
	}
	return checks
}

// registryCheck lists the schema registry's subjects, for a format that
// needs the registry.
func registryCheck(c *records.Config) (check.Check, bool) {
	if c.Format != records.Avro && c.Format != records.Protobuf {
		return check.Check{}, false
	}
	host := records.ShownURL(c.SchemaRegistryURL)
	if u, err := url.Parse(host); err == nil && u.Host != "" {
		host = u.Host
	}
	return check.Check{
		Name: "registry.reach", Kind: check.Connection, Title: "Reach the schema registry at {registry}",
		Description: "Lists the registry's subjects, which Avro and Protobuf records' schemas are registered under.",
		Field:       "schemaRegistryUrl", Params: check.Params{"registry": host}, Timeout: callTimeout,
		Run: func(ctx context.Context, p *check.Probe) error {
			reg, err := c.Registry()
			if err != nil {
				return check.ConfigInvalid.Wrap(err)
			}
			subjects, err := reg.Subjects(ctx)
			if err != nil {
				return registryFailure(err)
			}
			p.Param("subjects", len(subjects))
			p.Found("The registry answers, with {subjects} subjects.")
			return nil
		},
	}, true
}

func registryFailure(err error) error {
	re, ok := errors.AsType[*sr.ResponseError](err)
	if !ok {
		return check.Classify(err)
	}
	var f *check.Failure
	switch s := re.StatusCode; {
	case s == http.StatusUnauthorized:
		f = check.CredentialsRejected.Wrap(err)
		f.Message, f.Fix = "The schema registry rejected the credentials.", "Check schemaRegistryUsername and schemaRegistryPassword."
	case s == http.StatusForbidden:
		f = check.PermissionDenied.Wrap(err)
		f.Message, f.Fix = "The schema registry doesn't let the user list its subjects.", "Allow schemaRegistryUsername to read the registry's subjects."
	case s == http.StatusNotFound:
		f = check.NotFound.Wrap(err, "resource", "The registry's list of subjects")
		f.Fix = "Check schemaRegistryUrl: it's the registry's base URL, such as https://registry.example.com:8081."
	case s >= http.StatusInternalServerError:
		f = check.Unavailable.Wrap(err)
	default:
		f = check.Unknown.Wrap(err)
	}
	return f
}

// streamParts are each stream's part of its checks' names: the stream's
// name as check.Name fits it, with a hash of the name where two streams'
// would be the same.
func streamParts(names []string) map[string]string {
	fit := map[string][]string{}
	for _, n := range names {
		f := check.Name(n)
		if !slices.Contains(fit[f], n) {
			fit[f] = append(fit[f], n)
		}
	}
	out := make(map[string]string, len(names))
	for f, ns := range fit {
		for _, n := range ns {
			if len(ns) == 1 {
				out[n] = f
				continue
			}
			h := fnv.New32a()
			_, _ = h.Write([]byte(n))
			out[n] = fmt.Sprintf("%s-%08x", f, h.Sum32())
		}
	}
	return out
}

// found is what a check found, for the checks after it that need it.
type found[T any] struct {
	mu sync.Mutex
	v  T
}

func (f *found[T]) set(v T) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.v = v
}

func (f *found[T]) get() T {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.v
}

// describe sets the params a stream's summary gives: status, shards,
// retention and encryption.
func describe(p *check.Probe, sum *types.StreamDescriptionSummary) {
	p.Param("status", strings.ToLower(string(sum.StreamStatus)))
	p.Param("shards", aws.ToInt32(sum.OpenShardCount))
	p.Param("retention", hours(aws.ToInt32(sum.RetentionPeriodHours)))
	p.Param("encryption", encryption(sum))
}

func isActive(sum *types.StreamDescriptionSummary) bool {
	return sum.StreamStatus == types.StreamStatusActive || sum.StreamStatus == types.StreamStatusUpdating
}

func encryption(sum *types.StreamDescriptionSummary) string {
	if sum.EncryptionType == types.EncryptionTypeKms {
		return "KMS encryption"
	}
	return "no encryption"
}

func hours(h int32) string { return fmt.Sprintf("%d hours", h) }

// human is d as a Go duration without its trailing zero minutes and
// seconds: 48h, 1h30m, 1m30s.
func human(d time.Duration) string {
	s := d.Round(time.Second).String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// list is names, the first ten, then how many more.
func list(names []string) string {
	if len(names) <= 10 {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:10], ", "), len(names)-10)
}

// and joins words as a sentence does: "a, b and c".
func and(words []string) string {
	if len(words) < 2 {
		return strings.Join(words, "")
	}
	return strings.Join(words[:len(words)-1], ", ") + " and " + words[len(words)-1]
}
