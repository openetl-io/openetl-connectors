package s3

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// StoreConfig is where the files are, and how to reach them: the settings
// the source and the destination share.
type StoreConfig struct {
	// s3://bucket/prefix, gs://bucket/prefix, azblob://container/prefix, or
	// file:// and an absolute path.
	URL string `json:"url"`
	// S3: an S3-compatible endpoint, with path-style addressing. GCS: the
	// JSON API's endpoint, such as an emulator's. Azure: the blob service's
	// URL. Empty: the store's own.
	Endpoint string `json:"endpoint"`
	Region   string `json:"region"`
	// Without a key, the AWS default credentials.
	AccessKeyID     sdk.Secret `json:"accessKeyId"`
	SecretAccessKey sdk.Secret `json:"secretAccessKey"`
	// A service account's JSON key. Without one, Google's default
	// credentials, or none with an endpoint.
	ServiceAccountKey sdk.Secret `json:"serviceAccountKey"`
	AccountName       string     `json:"accountName"`
	// At most one of them. Without any, Azure's default credentials.
	AccountKey       sdk.Secret `json:"accountKey"`
	SASToken         sdk.Secret `json:"sasToken"`
	ConnectionString sdk.Secret `json:"connectionString"`
}

// store is a parsed StoreConfig.
type store struct {
	cfg StoreConfig
	u   *url.URL
	// The bucket or container; empty for file.
	bucket string
	// The path under it, without slashes around it; for file, the
	// directory's path.
	prefix string
}

func parseStore(cfg StoreConfig) (*store, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("s3: url: %w", err)
	}
	s := &store{cfg: cfg, u: u}
	switch {
	case u.RawQuery != "" || u.Fragment != "":
	case (u.Scheme == "s3" || u.Scheme == "gs" || u.Scheme == "azblob") && u.Host != "":
		s.bucket, s.prefix = u.Host, strings.Trim(u.Path, "/")
	case u.Scheme == "file" && (u.Host == "" || u.Host == "localhost") && path.IsAbs(u.Path):
		s.prefix = u.Path
	}
	if s.bucket == "" && s.prefix == "" {
		return nil, fmt.Errorf("s3: url %q must be s3://bucket/prefix, gs://bucket/prefix, azblob://container/prefix, or file:// and an absolute path", cfg.URL)
	}
	set := func(xs ...sdk.Secret) int {
		n := 0
		for _, x := range xs {
			if !x.IsZero() {
				n++
			}
		}
		return n
	}
	aws := set(cfg.AccessKeyID, cfg.SecretAccessKey) > 0 || cfg.Region != ""
	gcs := set(cfg.ServiceAccountKey) > 0
	azure := set(cfg.AccountKey, cfg.SASToken, cfg.ConnectionString) > 0 || cfg.AccountName != ""
	switch {
	case aws && u.Scheme != "s3":
		return nil, errors.New("s3: region, accessKeyId and secretAccessKey are for s3:// URLs")
	case gcs && u.Scheme != "gs":
		return nil, errors.New("s3: serviceAccountKey is for gs:// URLs")
	case azure && u.Scheme != "azblob":
		return nil, errors.New("s3: accountName, accountKey, sasToken and connectionString are for azblob:// URLs")
	case cfg.Endpoint != "" && u.Scheme == "file":
		return nil, errors.New("s3: a file:// URL takes no endpoint")
	case cfg.AccessKeyID.IsZero() != cfg.SecretAccessKey.IsZero():
		return nil, errors.New("s3: accessKeyId and secretAccessKey go together")
	case set(cfg.AccountKey, cfg.SASToken, cfg.ConnectionString) > 1:
		return nil, errors.New("s3: set one of accountKey, sasToken and connectionString")
	case u.Scheme == "azblob" && cfg.AccountName == "" && cfg.ConnectionString.IsZero() && cfg.Endpoint == "":
		return nil, errors.New("s3: an azblob:// URL needs accountName, a connectionString or an endpoint")
	}
	return s, nil
}

// open opens the bucket, below the URL's path.
func (s *store) open(ctx context.Context) (*blob.Bucket, error) {
	var b *blob.Bucket
	var err error
	switch s.u.Scheme {
	case "file":
		b, err = fileblob.OpenBucket(s.prefix, &fileblob.Options{CreateDir: true, NoTempDir: true})
	case "s3":
		b, err = s.openS3(ctx)
	case "gs":
		b, err = s.openGCS(ctx)
	case "azblob":
		b, err = s.openAzure(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("s3: open %s: %w", s.cfg.URL, err)
	}
	if s.bucket != "" && s.prefix != "" {
		b = blob.PrefixedBucket(b, s.prefix+"/")
	}
	return b, nil
}

func (s *store) openS3(ctx context.Context) (*blob.Bucket, error) {
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
	return s3blob.OpenBucket(ctx, client, s.bucket, nil)
}

// gcsAttempts bounds the client's retries, which otherwise go on while ctx
// lasts, as they do for a store that doesn't answer.
const gcsAttempts = 4

func (s *store) openGCS(ctx context.Context) (*blob.Bucket, error) {
	var opts []option.ClientOption
	if s.cfg.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(strings.TrimSuffix(s.cfg.Endpoint, "/")+"/storage/v1/"))
	}
	switch {
	case !s.cfg.ServiceAccountKey.IsZero():
		opts = append(opts, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(s.cfg.ServiceAccountKey.Reveal())))
	case s.cfg.Endpoint != "":
		opts = append(opts, option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{}))
	}
	c, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, &credentialsError{store: "Google Cloud", err: err}
	}
	c.SetRetry(storage.WithMaxAttempts(gcsAttempts))
	return gcsblob.OpenBucket(ctx, nil, s.bucket, &gcsblob.Options{Client: c})
}

func (s *store) openAzure(ctx context.Context) (*blob.Bucket, error) {
	opts := &container.ClientOptions{ClientOptions: policy.ClientOptions{Retry: policy.RetryOptions{MaxRetries: 3, TryTimeout: time.Minute}}}
	service := s.cfg.Endpoint
	if service == "" {
		service = "https://" + s.cfg.AccountName + ".blob.core.windows.net"
	}
	containerURL := strings.TrimSuffix(service, "/") + "/" + s.bucket
	var c *container.Client
	var err error
	switch {
	case !s.cfg.ConnectionString.IsZero():
		c, err = container.NewClientFromConnectionString(s.cfg.ConnectionString.Reveal(), s.bucket, opts)
	case !s.cfg.AccountKey.IsZero():
		var cred *container.SharedKeyCredential
		if cred, err = container.NewSharedKeyCredential(s.accountName(), s.cfg.AccountKey.Reveal()); err == nil {
			c, err = container.NewClientWithSharedKeyCredential(containerURL, cred, opts)
		}
	case !s.cfg.SASToken.IsZero():
		c, err = container.NewClientWithNoCredential(containerURL+"?"+strings.TrimPrefix(s.cfg.SASToken.Reveal(), "?"), opts)
	default:
		var cred azcore.TokenCredential
		if cred, err = azidentity.NewDefaultAzureCredential(nil); err == nil {
			c, err = container.NewClient(containerURL, cred, opts)
		}
	}
	if err != nil {
		return nil, &credentialsError{store: "Azure", err: err}
	}
	return azureblob.OpenBucket(ctx, c, nil)
}

// accountName is the configured account's, or the one an emulator's
// endpoint names in its path, as Azurite's do.
func (s *store) accountName() string {
	if s.cfg.AccountName != "" {
		return s.cfg.AccountName
	}
	if u, err := url.Parse(s.cfg.Endpoint); err == nil {
		return strings.Trim(u.Path, "/")
	}
	return ""
}

// location is key as the URL names it.
func (s *store) location(key string) string {
	u := *s.u
	u.Path = path.Join(u.Path, key)
	return u.String()
}

// key is the key of a location the URL names, and whether it's under it.
func (s *store) key(location string) (string, bool) {
	base := strings.TrimSuffix(s.u.String(), "/") + "/"
	if !strings.HasPrefix(location, base) {
		return "", false
	}
	return strings.TrimPrefix(location, base), true
}

// version is what a conditional write compares an object with: S3's and
// Azure's ETag, GCS's generation. Nil: the object doesn't exist.
type version struct {
	etag       string
	generation int64
}

// versionOf is key's version, from its attributes.
func versionOf(a *blob.Attributes) *version {
	v := &version{etag: a.ETag}
	var oa storage.ObjectAttrs
	if a.As(&oa) {
		v.generation = oa.Generation
	}
	return v
}

// errConflict: a conditional write found the object changed.
var errConflict = errors.New("s3: the object changed since it was read")

// swap writes data to key if key is still at was: a compare and swap with
// each store's conditional write (If-Match and If-None-Match on S3 and
// Azure, ifGenerationMatch on GCS), and under an exclusive lock on the file
// for file:// URLs, whose lock check must run within it. It returns
// errConflict if key moved.
func (s *store) swap(ctx context.Context, b *blob.Bucket, key string, data []byte, md map[string]string, was *version, check func() (*version, error)) error {
	if s.u.Scheme == "file" {
		unlock, err := lockFile(filepath.Join(s.prefix, filepath.FromSlash(key)) + ".lock")
		if err != nil {
			return err
		}
		defer unlock()
		now, err := check()
		if err != nil {
			return err
		}
		if (now == nil) != (was == nil) || now != nil && now.etag != was.etag {
			return errConflict
		}
		return b.WriteAll(ctx, key, data, &blob.WriterOptions{Metadata: md})
	}
	opts := &blob.WriterOptions{Metadata: md, IfNotExist: was == nil, BeforeWrite: func(as func(any) bool) error {
		if was == nil {
			return nil
		}
		var in *transfermanager.UploadObjectInput
		var oh **storage.ObjectHandle
		var az *azblob.UploadStreamOptions
		switch {
		case as(&in):
			in.IfMatch = aws.String(was.etag)
		case as(&oh):
			*oh = (*oh).If(storage.Conditions{GenerationMatch: was.generation})
		case as(&az):
			etag := azcore.ETag(was.etag)
			az.AccessConditions = &azblobblob.AccessConditions{ModifiedAccessConditions: &azblobblob.ModifiedAccessConditions{IfMatch: &etag}}
		default:
			return errors.New("s3: the store has no conditional writes")
		}
		return nil
	}}
	err := b.WriteAll(ctx, key, data, opts)
	if gcerrors.Code(err) == gcerrors.FailedPrecondition || conflict(err) {
		return errConflict
	}
	return err
}

// conflict: a store refused a conditional write.
func conflict(err error) bool {
	if err == nil {
		return false
	}
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode() == "PreconditionFailed" || ae.ErrorCode() == "ConditionalRequestConflict"
	}
	if re, ok := errors.AsType[*azcore.ResponseError](err); ok {
		return re.StatusCode == http.StatusPreconditionFailed || re.StatusCode == http.StatusConflict
	}
	if ge, ok := errors.AsType[*googleapi.Error](err); ok {
		return ge.Code == http.StatusPreconditionFailed
	}
	return false
}

// lockFile takes an exclusive lock on name, which it creates, until unlock.
func lockFile(name string) (unlock func(), err error) {
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

// credentialsSource marks its provider's failures as credentialsError, so
// classify can tell a connector without AWS credentials from one the store
// refuses.
type credentialsSource struct{ aws.CredentialsProvider }

func (c credentialsSource) Retrieve(ctx context.Context) (aws.Credentials, error) {
	creds, err := c.CredentialsProvider.Retrieve(ctx)
	if err != nil {
		return creds, &credentialsError{store: "AWS", err: err}
	}
	return creds, nil
}

// credentialsError: the connector has no credentials for the store, or
// can't use those it has.
type credentialsError struct {
	store string
	err   error
}

func (e *credentialsError) Error() string {
	switch e.store {
	case "AWS":
		return "no AWS credentials: set accessKeyId and secretAccessKey, or name a role in the connector's aws.role: " + e.err.Error()
	case "Google Cloud":
		return "Google Cloud credentials: set serviceAccountKey to a service account's JSON key: " + e.err.Error()
	}
	return "Azure credentials: set accountKey, sasToken or connectionString: " + e.err.Error()
}

func (e *credentialsError) Unwrap() error { return e.err }

// classify makes failures to reach the store TRANSIENT, and gives each
// store's refusals and a missing credential their taxonomy codes.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[interface {
		error
		ErrorCode() protocolv1.ErrorCode
	}](err); ok {
		return err
	}
	code := func(c protocolv1.ErrorCode) error { return sdk.WithCode(c, err) }
	if errors.Is(err, context.Canceled) {
		return err
	}
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		switch ae.ErrorCode() {
		case "AccessDenied", "Forbidden", "AllAccessDisabled":
			return code(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED)
		case "ExpiredToken", "ExpiredTokenException":
			return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED)
		case "Throttling", "SlowDown":
			return code(protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED)
		case "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidToken":
			return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
		}
	}
	if re, ok := errors.AsType[*azcore.ResponseError](err); ok {
		switch {
		// Azurite answers a wrong key with AuthorizationFailure, but in the
		// words of Azure's AuthenticationFailed.
		case re.ErrorCode == "AuthenticationFailed" || re.StatusCode == http.StatusUnauthorized ||
			re.StatusCode == http.StatusForbidden && strings.Contains(re.Error(), "failed to authenticate the request"):
			return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
		case re.StatusCode == http.StatusForbidden:
			return code(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED)
		case re.StatusCode == http.StatusTooManyRequests || re.ErrorCode == "ServerBusy":
			return code(protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED)
		case re.StatusCode >= 500:
			return code(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT)
		}
	}
	if ge, ok := errors.AsType[*googleapi.Error](err); ok {
		switch {
		case ge.Code == http.StatusUnauthorized:
			return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
		case ge.Code == http.StatusForbidden:
			return code(protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED)
		case ge.Code == http.StatusTooManyRequests:
			return code(protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED)
		case ge.Code >= 500:
			return code(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT)
		}
	}
	if _, ok := errors.AsType[*oauth2.RetrieveError](err); ok {
		return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
	}
	if _, ok := errors.AsType[*azidentity.AuthenticationFailedError](err); ok {
		return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
	}
	if _, ok := errors.AsType[*credentialsError](err); ok {
		return code(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID)
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return code(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT)
	}
	return err
}
