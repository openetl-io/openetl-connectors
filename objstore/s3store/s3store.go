// Package s3store is the objstore Store of Amazon S3 and S3-compatible
// stores.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Config is how to reach the store.
type Config struct {
	// An S3-compatible store's endpoint, with path-style addressing; empty
	// for Amazon S3.
	Endpoint string `json:"endpoint"`
	Region   string `json:"region"`
	// Without a key, the AWS default credentials.
	AccessKeyID     sdk.Secret `json:"accessKeyId"`
	SecretAccessKey sdk.Secret `json:"secretAccessKey"`
}

// IsZero: nothing is set.
func (c Config) IsZero() bool {
	return c.Endpoint == "" && c.Region == "" && c.AccessKeyID.IsZero() && c.SecretAccessKey.IsZero()
}

type Store struct{ cfg Config }

func New(cfg Config) (*Store, error) {
	if cfg.AccessKeyID.IsZero() != cfg.SecretAccessKey.IsZero() {
		return nil, errors.New("s3: accessKeyId and secretAccessKey go together")
	}
	return &Store{cfg: cfg}, nil
}

func (s *Store) Scheme() string    { return "s3" }
func (s *Store) Container() string { return "bucket" }

func (s *Store) Open(ctx context.Context, bucket string) (*blob.Bucket, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if s.cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(s.cfg.Region))
	}
	if !s.cfg.AccessKeyID.IsZero() {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s.cfg.AccessKeyID.Reveal(), s.cfg.SecretAccessKey.Reveal(), "")))
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("AWS config: %w", err)
	}
	ac.Credentials = credentialsSource{ac.Credentials}
	client := awss3.NewFromConfig(ac, func(o *awss3.Options) {
		// Ranged reads have no checksum to validate, which it would log.
		o.DisableLogOutputChecksumValidationSkipped = true
		if s.cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(s.cfg.Endpoint)
			o.UsePathStyle = true
		}
	})
	return s3blob.OpenBucket(ctx, client, bucket, nil)
}

// credentialsSource codes its provider's failures AUTH_INVALID, so a
// connector without AWS credentials isn't taken for one the store refuses.
type credentialsSource struct{ aws.CredentialsProvider }

func (c credentialsSource) Retrieve(ctx context.Context) (aws.Credentials, error) {
	creds, err := c.CredentialsProvider.Retrieve(ctx)
	if err != nil {
		return creds, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID,
			fmt.Errorf("no AWS credentials: set accessKeyId and secretAccessKey, or name a role in the connector's aws.role: %w", err))
	}
	return creds, nil
}

func (s *Store) Endpoint() (endpoint, setting string) {
	switch {
	case s.cfg.Endpoint != "":
		return s.cfg.Endpoint, "endpoint"
	case strings.HasPrefix(s.cfg.Region, "cn-"):
		return "https://s3." + s.cfg.Region + ".amazonaws.com.cn", ""
	case s.cfg.Region != "":
		return "https://s3." + s.cfg.Region + ".amazonaws.com", ""
	}
	return "https://s3.amazonaws.com", ""
}

func (s *Store) Credential() string {
	if !s.cfg.AccessKeyID.IsZero() {
		return "accessKeyId"
	}
	return ""
}

func (s *Store) Privilege(action string) string {
	return map[string]string{"list": "s3:ListBucket", "read": "s3:GetObject", "write": "s3:PutObject", "delete": "s3:DeleteObject"}[action]
}

func (s *Store) Code(err error) protocolv1.ErrorCode {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		switch ae.ErrorCode() {
		case "AccessDenied", "Forbidden", "AllAccessDisabled":
			return protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
		case "ExpiredToken", "ExpiredTokenException":
			return protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED
		case "Throttling", "SlowDown":
			return protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED
		case "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidToken":
			return protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID
		}
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

func (s *Store) Conflict(err error) bool {
	ae, ok := errors.AsType[smithy.APIError](err)
	return ok && (ae.ErrorCode() == "PreconditionFailed" || ae.ErrorCode() == "ConditionalRequestConflict")
}

func (s *Store) IfMatch(as func(any) bool, etag string, _ int64) error {
	var in *transfermanager.UploadObjectInput
	if !as(&in) {
		return errors.New("s3: the store has no conditional writes")
	}
	in.IfMatch = aws.String(etag)
	return nil
}

func (s *Store) Generation(*blob.Attributes) int64 { return 0 }
