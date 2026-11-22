// Package s3store is the objstore Store of Amazon S3 and S3-compatible
// stores.
package s3store

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
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
		o.APIOptions = append(o.APIOptions, func(st *middleware.Stack) error {
			return st.Initialize.Add(middleware.InitializeMiddlewareFunc("bucketRegion", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				out, md, err := next.HandleInitialize(ctx, in)
				if r := bucketRegion(err); r != "" && r != ac.Region {
					// In place of S3's answer, as gocloud takes a redirect for a
					// bucket that doesn't exist.
					err = sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
						fmt.Errorf("the bucket is in the region %s, not %s: set region to %s", r, cmp.Or(ac.Region, "none"), r))
				}
				return out, md, err
			}), middleware.Before)
		})
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
// connector without AWS credentials isn't taken for one the store refuses,
// but for a provider that doesn't answer or fails, such as STS or the
// runner's endpoint of a role's credentials: TRANSIENT.
type credentialsSource struct{ aws.CredentialsProvider }

func (c credentialsSource) Retrieve(ctx context.Context) (aws.Credentials, error) {
	creds, err := c.CredentialsProvider.Retrieve(ctx)
	if _, ok := errors.AsType[net.Error](err); ok || serverError(err) {
		return creds, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, fmt.Errorf("AWS credentials: %w", err))
	}
	if err != nil {
		return creds, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID,
			fmt.Errorf("no AWS credentials: set accessKeyId and secretAccessKey, or name a role in the connector's aws.role: %w", err))
	}
	return creds, nil
}

var expecting = regexp.MustCompile(`expecting '([a-z0-9-]+)'`)

// bucketRegion is the bucket's region where err is S3's answer that the
// bucket is in another region than the request's: a redirect, or a refusal
// of a request signed for another; empty otherwise.
func bucketRegion(err error) string {
	re, ok := errors.AsType[*awshttp.ResponseError](err)
	if !ok {
		return ""
	}
	ae, _ := errors.AsType[smithy.APIError](err)
	malformed := ae != nil && ae.ErrorCode() == "AuthorizationHeaderMalformed"
	if re.HTTPStatusCode() != http.StatusMovedPermanently && !malformed {
		return ""
	}
	if r := re.Response.Header.Get("X-Amz-Bucket-Region"); r != "" {
		return r
	}
	if m := expecting.FindStringSubmatch(err.Error()); malformed && m != nil {
		return m[1]
	}
	return ""
}

// serverError: an AWS service answered err with a server error.
func serverError(err error) bool {
	re, ok := errors.AsType[*awshttp.ResponseError](err)
	return ok && re.HTTPStatusCode() >= http.StatusInternalServerError
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
	if serverError(err) {
		return protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
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

// Version is a listed or opened object's ETag, unquoted.
func (s *Store) Version(as func(any) bool) string {
	var o types.Object
	if as(&o) {
		return strings.Trim(aws.ToString(o.ETag), `"`)
	}
	var r awss3.GetObjectOutput
	if as(&r) {
		return strings.Trim(aws.ToString(r.ETag), `"`)
	}
	return ""
}

// BeforeWrite gives an upload a minute of its own to abort a multipart
// upload that failed, which the context that ended it can't, and, on an
// endpoint, only the checksums the store asks for: S3 takes a CRC32 of every
// upload, which some S3-compatible stores refuse.
func (s *Store) BeforeWrite(as func(any) bool) error {
	var tm *transfermanager.Client
	var c *awss3.Client
	if !as(&tm) || !as(&c) {
		return nil
	}
	*tm = *transfermanager.New(c, func(o *transfermanager.Options) {
		o.FailTimeout = time.Minute
		if s.cfg.Endpoint != "" {
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		}
	})
	return nil
}
