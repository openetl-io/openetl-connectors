// Package dynamodb has the Amazon DynamoDB source and destination. The
// source reads each table as a stream keyed on the table's key: in full by
// Scan, in parallel segments, incrementally by a scan filtered on a cursor
// attribute, or change by change from the table's DynamoDB stream after a
// snapshot. The destination writes each stream into a table keyed on the
// stream's primary key, staging each epoch in a table of its own and
// applying it with conditional writes behind a fence.
package dynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Connection is the config both connectors share: where DynamoDB is, and
// the credentials.
type Connection struct {
	Region string `json:"region"`
	// An emulator's, such as DynamoDB Local's, or a VPC endpoint.
	Endpoint string `json:"endpoint"`
	// Without a key, the AWS default credentials.
	AccessKeyID     sdk.Secret `json:"accessKeyId"`
	SecretAccessKey sdk.Secret `json:"secretAccessKey"`
	SessionToken    sdk.Secret `json:"sessionToken"`
}

func (c *Connection) validate() error {
	if c.AccessKeyID.IsZero() != c.SecretAccessKey.IsZero() {
		return errors.New("dynamodb: accessKeyId and secretAccessKey go together")
	}
	if !c.SessionToken.IsZero() && c.AccessKeyID.IsZero() {
		return errors.New("dynamodb: sessionToken needs accessKeyId and secretAccessKey")
	}
	if c.Endpoint != "" {
		if u, err := url.Parse(c.Endpoint); err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("dynamodb: endpoint %q isn't an http or https URL", c.Endpoint)
		}
	}
	return nil
}

func decodeConfig(config []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return configInvalid(fmt.Errorf("dynamodb: config: %w", err))
	}
	return nil
}

func configInvalid(err error) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
}

// clients is what the connectors call DynamoDB with.
type clients struct {
	db      *dynamodb.Client
	streams *dynamodbstreams.Client
	region  string
}

// connect makes the clients. attempts is how often a call is tried:
// throttling and failures to reach DynamoDB are retried with backoff
// rather than failing the attempt, unless a check needs a quick answer.
func (c *Connection) connect(ctx context.Context, attempts int) (*clients, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = attempts
				o.MaxBackoff = 10 * time.Second
				// The standard retryer's quota gives up under sustained
				// throttling, which DynamoDB's on-demand scaling needs to ride.
				o.RateLimiter = ratelimit.None
			})
		}),
		awsconfig.WithLogger(logging.LoggerFunc(func(c logging.Classification, format string, v ...any) {
			msg := fmt.Sprintf(format, v...)
			// The SDK says so of each error response it closes unread, as a
			// refused conditional write's is, which isn't news.
			if strings.HasPrefix(msg, "failed to close HTTP response body") {
				return
			}
			level := slog.LevelInfo
			if c == logging.Warn {
				level = slog.LevelWarn
			}
			slog.Log(context.Background(), level, "aws sdk: "+msg)
		})),
	}
	if c.Region != "" {
		opts = append(opts, awsconfig.WithRegion(c.Region))
	}
	if !c.AccessKeyID.IsZero() {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKeyID.Reveal(), c.SecretAccessKey.Reveal(), c.SessionToken.Reveal())))
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, configInvalid(fmt.Errorf("dynamodb: AWS config: %w", err))
	}
	if ac.Region == "" {
		return nil, configInvalid(errors.New("dynamodb: no region: set region in the config, or AWS_REGION"))
	}
	out := &clients{region: ac.Region}
	out.db = dynamodb.NewFromConfig(ac, func(o *dynamodb.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
	out.streams = dynamodbstreams.NewFromConfig(ac, func(o *dynamodbstreams.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
	return out, nil
}

// address is the host:port DynamoDB is at, and whether it speaks TLS there;
// empty if the region isn't known.
func (c *Connection) address(region string) (string, bool) {
	if c.Endpoint == "" {
		if region == "" {
			return "", false
		}
		return net.JoinHostPort("dynamodb."+region+".amazonaws.com", "443"), true
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return "", false
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	return net.JoinHostPort(u.Hostname(), port), u.Scheme == "https"
}

// classify gives DynamoDB's errors their taxonomy codes, and failures to
// reach it TRANSIENT.
func classify(err error) error {
	if err == nil {
		return nil
	}
	code := func(c protocolv1.ErrorCode) error { return sdk.WithCode(c, err) }
	switch apiCode(err) {
	case "AccessDeniedException", "KMSAccessDeniedException":
		return code(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED)
	case "UnrecognizedClientException", "InvalidSignatureException", "IncompleteSignature", "MissingAuthenticationToken", "MissingAuthenticationTokenException", "InvalidClientTokenId":
		return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
	case "ExpiredTokenException", "ExpiredToken":
		return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED)
	case "ProvisionedThroughputExceededException", "ThrottlingException", "RequestLimitExceeded", "LimitExceededException", "KMSThrottlingException":
		return code(protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED)
	case "ResourceNotFoundException":
		return code(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID)
	case "TrimmedDataAccessException":
		return code(protocolv1.ErrorCode_ERROR_CODE_POSITION_LOST)
	case "InternalServerError", "ServiceUnavailable", "ResourceInUseException":
		return code(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT)
	case "ItemCollectionSizeLimitExceededException":
		return code(protocolv1.ErrorCode_ERROR_CODE_DATA_INVALID)
	}
	if _, ok := errors.AsType[net.Error](err); ok && !errors.Is(err, context.Canceled) {
		return code(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT)
	}
	return err
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}
	return ""
}
