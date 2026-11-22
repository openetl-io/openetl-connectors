// Package azurestore is the objstore Store of Azure Blob Storage.
package azurestore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azblobblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Config is how to reach the store.
type Config struct {
	// The blob service's URL; empty for https://ACCOUNT.blob.core.windows.net.
	Endpoint    string `json:"endpoint"`
	AccountName string `json:"accountName"`
	// At most one of them. Without any, Azure's default credentials.
	AccountKey       sdk.Secret `json:"accountKey"`
	SASToken         sdk.Secret `json:"sasToken"`
	ConnectionString sdk.Secret `json:"connectionString"`
}

type Store struct{ cfg Config }

func New(cfg Config) (*Store, error) {
	set := 0
	for _, x := range []sdk.Secret{cfg.AccountKey, cfg.SASToken, cfg.ConnectionString} {
		if !x.IsZero() {
			set++
		}
	}
	switch {
	case set > 1:
		return nil, errors.New("azureblob: set one of accountKey, sasToken and connectionString")
	case cfg.AccountName == "" && cfg.ConnectionString.IsZero() && cfg.Endpoint == "":
		return nil, errors.New("azureblob: set accountName, a connectionString or an endpoint")
	case cfg.Endpoint != "" && !cfg.ConnectionString.IsZero():
		return nil, errors.New("azureblob: set endpoint or connectionString, not both: a connection string names its endpoint")
	// A request's URL with one fails to parse, in an error that shows it.
	case strings.ContainsFunc(cfg.SASToken.Reveal()+cfg.ConnectionString.Reveal(), unicode.IsControl):
		return nil, errors.New("azureblob: sasToken and connectionString can't have a line break or other control character")
	}
	s := &Store{cfg: cfg}
	// A SAS goes in each request's URL, as it is; an account's key signs
	// requests, and an emulator takes it over http.
	if endpoint, _ := s.Endpoint(); s.sas() && endpoint != "" && !strings.HasPrefix(strings.ToLower(endpoint), "https://") {
		return nil, errors.New("azureblob: a SAS needs an https endpoint")
	}
	return s, nil
}

// sas: the credentials are a shared access signature.
func (s *Store) sas() bool {
	if !s.cfg.ConnectionString.IsZero() {
		kv := connection(s.cfg.ConnectionString.Reveal())
		// As Azure's SDK reads it: an account's key before a SAS.
		return kv["sharedaccesssignature"] != "" && (kv["accountname"] == "" || kv["accountkey"] == "")
	}
	return !s.cfg.SASToken.IsZero()
}

func (s *Store) Scheme() string    { return "azblob" }
func (s *Store) Container() string { return "container" }

func (s *Store) Open(ctx context.Context, bucket string) (*blob.Bucket, error) {
	opts := &container.ClientOptions{ClientOptions: policy.ClientOptions{
		Retry:            policy.RetryOptions{MaxRetries: 3, TryTimeout: time.Minute},
		PerRetryPolicies: []policy.Policy{hideQuery{}},
	}}
	service := s.cfg.Endpoint
	if service == "" {
		service = "https://" + s.cfg.AccountName + ".blob.core.windows.net"
	}
	containerURL := strings.TrimSuffix(service, "/") + "/" + bucket
	var c *container.Client
	var err error
	switch {
	case !s.cfg.ConnectionString.IsZero():
		c, err = container.NewClientFromConnectionString(s.cfg.ConnectionString.Reveal(), bucket, opts)
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
		return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID,
			fmt.Errorf("azureblob: Azure credentials: set accountKey, sasToken or connectionString: %w", err))
	}
	return azureblob.OpenBucket(ctx, c, nil)
}

// hideQuery drops a request's query, which holds a SAS, from the error of
// one that got no answer: net/http's error shows the whole URL.
type hideQuery struct{}

func (hideQuery) Do(req *policy.Request) (*http.Response, error) {
	resp, err := req.Next()
	if ue, ok := errors.AsType[*url.Error](err); ok {
		ue.URL, _, _ = strings.Cut(ue.URL, "?")
	}
	return resp, err
}

// accountName is the configured account's, or the one an emulator's
// endpoint names in its path, as Azurite's do.
func (s *Store) accountName() string {
	if s.cfg.AccountName != "" {
		return s.cfg.AccountName
	}
	if u, err := url.Parse(s.cfg.Endpoint); err == nil {
		return strings.Trim(u.Path, "/")
	}
	return ""
}

// A connection string's endpoint is the store's own, but for a BlobEndpoint
// it names.
func (s *Store) Endpoint() (endpoint, setting string) {
	if !s.cfg.ConnectionString.IsZero() {
		kv := connection(s.cfg.ConnectionString.Reveal())
		if e := kv["blobendpoint"]; e != "" {
			return e, "connectionString"
		}
		if kv["accountname"] == "" {
			return "", ""
		}
		return cmp.Or(kv["defaultendpointsprotocol"], "https") + "://" + kv["accountname"] + ".blob." + cmp.Or(kv["endpointsuffix"], "core.windows.net"), ""
	}
	if s.cfg.Endpoint != "" {
		return s.cfg.Endpoint, "endpoint"
	}
	return "https://" + s.accountName() + ".blob.core.windows.net", ""
}

// connection is a connection string's settings, by their names in lower
// case.
func connection(cs string) map[string]string {
	kv := map[string]string{}
	for part := range strings.SplitSeq(cs, ";") {
		if k, v, ok := strings.Cut(part, "="); ok {
			kv[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}
	return kv
}

func (s *Store) Credential() string {
	for _, c := range []struct {
		name string
		v    sdk.Secret
	}{{"accountKey", s.cfg.AccountKey}, {"sasToken", s.cfg.SASToken}, {"connectionString", s.cfg.ConnectionString}} {
		if !c.v.IsZero() {
			return c.name
		}
	}
	return ""
}

func (s *Store) Privilege(action string) string {
	return map[string]string{"list": "List Blobs", "read": "Read (Get Blob)", "write": "Write (Put Blob)", "delete": "Delete (Delete Blob)"}[action]
}

func (s *Store) Code(err error) protocolv1.ErrorCode {
	if re, ok := errors.AsType[*azcore.ResponseError](err); ok {
		switch {
		// Azurite answers a wrong key with AuthorizationFailure, but in the
		// words of Azure's AuthenticationFailed.
		case re.ErrorCode == "AuthenticationFailed" || re.StatusCode == http.StatusUnauthorized ||
			re.StatusCode == http.StatusForbidden && strings.Contains(re.Error(), "failed to authenticate the request"):
			return protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID
		case re.StatusCode == http.StatusForbidden:
			return protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
		case re.StatusCode == http.StatusTooManyRequests || re.ErrorCode == "ServerBusy":
			return protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED
		case re.StatusCode >= 500:
			return protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
		}
	}
	if _, ok := errors.AsType[*azidentity.AuthenticationFailedError](err); ok {
		return protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

func (s *Store) Conflict(err error) bool {
	re, ok := errors.AsType[*azcore.ResponseError](err)
	return ok && (re.StatusCode == http.StatusPreconditionFailed || re.StatusCode == http.StatusConflict)
}

func (s *Store) IfMatch(as func(any) bool, etag string, _ int64) error {
	var az *azblob.UploadStreamOptions
	if !as(&az) {
		return errors.New("azureblob: the store has no conditional writes")
	}
	e := azcore.ETag(etag)
	az.AccessConditions = &azblobblob.AccessConditions{ModifiedAccessConditions: &azblobblob.ModifiedAccessConditions{IfMatch: &e}}
	return nil
}

func (s *Store) Generation(*blob.Attributes) int64 { return 0 }
