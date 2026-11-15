// Package kinesis has the Kinesis source and destination. Kinesis data
// streams are streams. The destination writes each row as one record, its
// data the row as Kafka's destination writes it, and delivers and
// acknowledges. The source reads each record as one row, with its shard,
// sequence number, arrival time and partition key, and resumes from each
// shard's sequence number, reading a parent shard to its end before its
// children.
package kinesis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/smithy-go"

	"github.com/openetl-io/openetl-connectors/records"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/check"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Connection is the config both connectors share: AWS, the schema registry,
// the format and retries.
type Connection struct {
	Region string `json:"region"`
	// An emulator's, or a VPC endpoint.
	Endpoint string `json:"endpoint"`
	// Without a key, the AWS default credentials.
	AccessKeyID     sdk.Secret `json:"accessKeyId"`
	SecretAccessKey sdk.Secret `json:"secretAccessKey"`
	records.Config
	Retry Retry `json:"retry"`
}

// Retry is how calls Kinesis throttles or fails transiently are retried.
type Retry struct {
	// The first wait, doubling up to MaxBackoff; each wait is drawn at
	// random up to it.
	InitialBackoff Duration `json:"initialBackoff"`
	MaxBackoff     Duration `json:"maxBackoff"`
	// Attempts of one call, the first included, before the call fails.
	MaxAttempts int `json:"maxAttempts"`
	// How long a shard's reads or a request's records may keep being
	// throttled, through failed calls, before the attempt fails with
	// RATE_LIMITED.
	MaxElapsed Duration `json:"maxElapsed"`
}

// Retry's defaults.
const (
	defaultInitialBackoff = Duration(100 * time.Millisecond)
	defaultMaxBackoff     = Duration(10 * time.Second)
	defaultMaxAttempts    = 10
	defaultMaxElapsed     = Duration(5 * time.Minute)
)

func (r *Retry) defaults() {
	if r.InitialBackoff == 0 {
		r.InitialBackoff = defaultInitialBackoff
	}
	if r.MaxBackoff == 0 {
		r.MaxBackoff = defaultMaxBackoff
	}
	if r.MaxAttempts == 0 {
		r.MaxAttempts = defaultMaxAttempts
	}
	if r.MaxElapsed == 0 {
		r.MaxElapsed = defaultMaxElapsed
	}
}

func (r *Retry) validate() error {
	switch {
	case r.InitialBackoff < Duration(time.Millisecond):
		return errors.New("retry.initialBackoff is under 1ms")
	case r.MaxBackoff < r.InitialBackoff:
		return errors.New("retry.maxBackoff is under retry.initialBackoff")
	case r.MaxAttempts < 1 || r.MaxAttempts > 100:
		return fmt.Errorf("retry.maxAttempts %d isn't between 1 and 100", r.MaxAttempts)
	case r.MaxElapsed < Duration(time.Second):
		return errors.New("retry.maxElapsed is under 1s")
	}
	return nil
}

// backoff is the wait before retry n, from 1: drawn at random up to
// InitialBackoff doubled n-1 times, capped at MaxBackoff.
func (r *Retry) backoff(n int) time.Duration {
	ceil := time.Duration(r.MaxBackoff)
	n = max(n, 1)
	if n < 40 {
		ceil = min(ceil, time.Duration(r.InitialBackoff)<<(n-1))
	}
	return time.Duration(rand.Int64N(int64(ceil))) + 1
}

// BackoffDelay makes Retry the AWS SDK's backoff.
func (r *Retry) BackoffDelay(attempt int, _ error) (time.Duration, error) {
	return r.backoff(attempt), nil
}

// Duration is a duration in Go's syntax in JSON, such as "200ms" or "1m30s".
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("a duration is a string such as \"5s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	if v < 0 {
		return fmt.Errorf("duration %s is negative", s)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (c *Connection) validate(formats ...string) error {
	if err := c.Validate(formats...); err != nil {
		return fmt.Errorf("kinesis: %w", err)
	}
	if c.AccessKeyID.IsZero() != c.SecretAccessKey.IsZero() {
		return errors.New("kinesis: accessKeyId and secretAccessKey go together")
	}
	c.Retry.defaults()
	if err := c.Retry.validate(); err != nil {
		return fmt.Errorf("kinesis: %w", err)
	}
	return nil
}

func invalid(err error) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
}

func decodeConfig(config []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return invalid(fmt.Errorf("kinesis: config: %w", err))
	}
	return nil
}

func (c *Connection) client(ctx context.Context) (*awskinesis.Client, error) {
	rc := c.Retry
	rc.defaults()
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = rc.MaxAttempts
				o.MaxBackoff = time.Duration(rc.MaxBackoff)
				o.Backoff = &rc
				// Throttling is retried here rather than failing the call;
				// the standard retryer's quota would give up under load.
				o.RateLimiter = ratelimit.None
			})
		}),
	}
	if c.Region != "" {
		opts = append(opts, awsconfig.WithRegion(c.Region))
	}
	if !c.AccessKeyID.IsZero() {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKeyID.Reveal(), c.SecretAccessKey.Reveal(), "")))
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("kinesis: AWS config: %w", err)
	}
	return awskinesis.NewFromConfig(ac, func(o *awskinesis.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	}), nil
}

// connect checks the credentials reach Kinesis, and the schema registry if
// there is one. It reports whether Kinesis was reached.
func connect(ctx context.Context, c *Connection, cl *awskinesis.Client, r *check.Report) bool {
	ok := r.Run("connect", "Reach Kinesis and list streams", func() error {
		_, err := cl.ListStreams(ctx, &awskinesis.ListStreamsInput{Limit: aws.Int32(1)})
		return allow(classify(wrap("list streams", err)), "kinesis:ListStreams", "*")
	}, check.Field("region"))
	reg, err := c.Registry()
	switch {
	case err != nil:
		r.Fail("schema-registry", "Reach the schema registry", invalid(err), check.Field("schemaRegistryUrl"))
	case reg != nil:
		r.Run("schema-registry", "Reach the schema registry and list its subjects", func() error {
			if _, err := reg.Subjects(ctx); err != nil {
				return check.Fix(fmt.Errorf("kinesis: schema registry: %w", err), "Check schemaRegistryUrl, and the user and password if the registry needs them.")
			}
			return nil
		}, check.Field("schemaRegistryUrl"))
	}
	return ok
}

// wrap names what failed, or is nil.
func wrap(what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("kinesis: %s: %w", what, err)
}

// allow says what to do about a classified error: the action to allow on
// resource for a permission failure, or what else to check.
func allow(err error, action, resource string) error {
	if err == nil {
		return nil
	}
	switch codeOf(err) {
	case protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED:
		return check.Fix(err, fmt.Sprintf("Allow %s on %s in the IAM policy of the credentials, or of the role in aws.role.", action, resource))
	case protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED:
		return check.Fix(err, "Check accessKeyId and secretAccessKey, or the role in aws.role, and the region.")
	case protocolv1.ErrorCode_ERROR_CODE_TRANSIENT:
		return check.Fix(err, "Check that the runner reaches Kinesis in the region, or endpoint if it's set.")
	}
	return err
}

func codeOf(err error) protocolv1.ErrorCode {
	if c, ok := errors.AsType[interface {
		error
		ErrorCode() protocolv1.ErrorCode
	}](err); ok {
		return c.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

// classify gives Kinesis's errors their taxonomy codes.
func classify(err error) error {
	if _, ok := errors.AsType[net.Error](err); ok && !errors.Is(err, context.Canceled) {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	var ae smithy.APIError
	if err == nil || !errors.As(err, &ae) {
		return err
	}
	code := func(c protocolv1.ErrorCode) error { return sdk.WithCode(c, err) }
	switch ae.ErrorCode() {
	case "AccessDeniedException", "KMSAccessDeniedException":
		return code(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED)
	case "UnrecognizedClientException", "InvalidSignatureException", "IncompleteSignature", "MissingAuthenticationToken", "InvalidClientTokenId":
		return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
	case "ExpiredTokenException", "ExpiredToken":
		return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED)
	case "ProvisionedThroughputExceededException", "LimitExceededException", "ThrottlingException", "KMSThrottlingException":
		return code(protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED)
	case "ResourceNotFoundException":
		return code(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID)
	case "InternalFailure", "ServiceUnavailable":
		return code(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT)
	}
	return err
}

// throttled says err is Kinesis throttling, which waiting cures.
func throttled(err error) bool {
	switch apiCode(err) {
	case "ProvisionedThroughputExceededException", "LimitExceededException", "ThrottlingException", "KMSThrottlingException":
		return true
	}
	return false
}

func apiCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// throttle follows a run of throttled calls.
type throttle struct {
	retry *Retry
	since time.Time
	n     int
}

// wait waits before the next try of a throttled call, and fails with
// RATE_LIMITED once calls have been throttled for MaxElapsed.
func (t *throttle) wait(ctx context.Context, err error) error {
	now := time.Now()
	if t.n == 0 {
		t.since = now
	}
	t.n++
	if d := now.Sub(t.since); d >= time.Duration(t.retry.MaxElapsed) {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED, fmt.Errorf("throttled for %s: %w", d.Round(time.Second), err))
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(t.retry.backoff(t.n)):
		return nil
	}
}

// ok ends a run of throttled calls.
func (t *throttle) ok() { t.n = 0 }
