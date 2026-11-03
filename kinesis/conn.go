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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/smithy-go"

	"github.com/unsuitable001/openetl-connectors/records"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// Connection is the config both connectors share: AWS, the schema registry
// and the format.
type Connection struct {
	Region string `json:"region"`
	// An emulator's, or a VPC endpoint.
	Endpoint string `json:"endpoint"`
	// Without a key, the AWS default credentials.
	AccessKeyID     sdk.Secret `json:"accessKeyId"`
	SecretAccessKey sdk.Secret `json:"secretAccessKey"`
	records.Config
}

func (c *Connection) validate(formats ...string) error {
	if err := c.Config.Validate(formats...); err != nil {
		return fmt.Errorf("kinesis: %w", err)
	}
	if c.AccessKeyID.IsZero() != c.SecretAccessKey.IsZero() {
		return errors.New("kinesis: accessKeyId and secretAccessKey go together")
	}
	return nil
}

func decodeConfig(config []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("kinesis: config: %w", err))
	}
	return nil
}

func (c *Connection) client(ctx context.Context) (*awskinesis.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		// Throttling is retried here, with backoff, rather than failing the
		// attempt; the standard retryer's quota would give up under load.
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = 10
				o.MaxBackoff = 10 * time.Second
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

// check lists streams and, if there is one, the schema registry's subjects.
func check(ctx context.Context, c *Connection) error {
	cl, err := c.client(ctx)
	if err != nil {
		return err
	}
	if _, err := cl.ListStreams(ctx, &awskinesis.ListStreamsInput{Limit: aws.Int32(1)}); err != nil {
		return classify(fmt.Errorf("kinesis: list streams: %w", err))
	}
	reg, err := c.Registry()
	if err != nil || reg == nil {
		return err
	}
	if _, err := reg.Subjects(ctx); err != nil {
		return fmt.Errorf("kinesis: schema registry: %w", err)
	}
	return nil
}

// classify gives Kinesis's errors their taxonomy codes.
func classify(err error) error {
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

func apiCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}
