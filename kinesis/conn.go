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
	cl, _, err := c.clientWith(ctx, 0)
	return cl, err
}

// clientWith is a client and its AWS config, whose calls try at most
// attempts times; 0: retry.maxAttempts.
func (c *Connection) clientWith(ctx context.Context, attempts int) (*awskinesis.Client, aws.Config, error) {
	rc := c.Retry
	rc.defaults()
	if attempts > 0 {
		rc.MaxAttempts = min(rc.MaxAttempts, attempts)
	}
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
		return nil, aws.Config{}, fmt.Errorf("kinesis: AWS config: %w", err)
	}
	return awskinesis.NewFromConfig(ac, func(o *awskinesis.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	}), ac, nil
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

// AWS error codes that mean a call was refused for want of an IAM
// permission.
func accessDenied(code string) bool {
	return code == "AccessDeniedException" || code == "AccessDenied"
}

// awsReasons are the reasons of Kinesis's error codes that mean the same
// whatever the call: a failed check's, and in their Code, a read's or a
// write's. Access denied, which names an action, isn't here.
var awsReasons = map[string]check.Reason{
	"UnrecognizedClientException": credentialsRejected,
	"InvalidSignatureException":   credentialsRejected,
	"IncompleteSignature":         credentialsRejected,
	"MissingAuthenticationToken":  credentialsRejected,
	"InvalidClientTokenId":        credentialsRejected,
	"ExpiredTokenException":       check.CredentialsExpired,
	"ExpiredToken":                check.CredentialsExpired,

	"ProvisionedThroughputExceededException": check.RateLimited,
	"LimitExceededException":                 check.RateLimited,
	"ThrottlingException":                    check.RateLimited,
	"KMSThrottlingException":                 check.RateLimited,

	"KMSAccessDeniedException":  kmsDenied,
	"KMSDisabledException":      kmsUnusable,
	"KMSInvalidStateException":  kmsUnusable,
	"KMSNotFoundException":      kmsUnusable,
	"KMSOptInRequired":          kmsUnusable,
	"ResourceNotFoundException": check.NotFound,

	"InternalFailure":          check.Unavailable,
	"InternalFailureException": check.Unavailable,
	"ServiceUnavailable":       check.Unavailable,
}

var (
	credentialsRejected = check.Reason{
		Name:    check.CredentialsRejected.Name,
		Code:    check.CredentialsRejected.Code,
		Message: "AWS rejected the credentials.",
		Fix:     "Check accessKeyId and secretAccessKey, or the IAM role in the source's or destination's aws.role. In a Region that's opt-in, also check that the account has it enabled.",
	}
	kmsDenied = check.Reason{
		Name:    check.PrivilegeMissing.Name,
		Code:    check.PrivilegeMissing.Code,
		Message: "Kinesis can't use the stream's KMS key for the connector's AWS identity.",
		Fix:     "Allow {kms_action} on the stream's KMS key, in the key's policy or in an IAM policy of the connector's identity.",
	}
	kmsUnusable = check.Reason{
		Name:    "kms_key_unusable",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The KMS key that encrypts the stream can't be used: it's disabled, pending deletion, or gone.",
		Fix:     "Enable the stream's KMS key in AWS KMS, or encrypt the stream with a key that's enabled.",
	}
)

// classify gives Kinesis's errors their taxonomy codes.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[net.Error](err); ok && !errors.Is(err, context.Canceled) {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	code := apiCode(err)
	if accessDenied(code) {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, err)
	}
	if r, ok := awsReasons[code]; ok {
		return sdk.WithCode(r.Code, err)
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

func apiMessage(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorMessage()
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
