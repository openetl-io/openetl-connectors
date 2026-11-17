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
	}
	return &Store{cfg: cfg}, nil
}

func (s *Store) Scheme() string    { return "azblob" }
func (s *Store) Container() string { return "container" }

func (s *Store) Open(ctx context.Context, bucket string) (*blob.Bucket, error) {
	opts := &container.ClientOptions{ClientOptions: policy.ClientOptions{Retry: policy.RetryOptions{MaxRetries: 3, TryTimeout: time.Minute}}}
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

func (s *Store) Endpoint() (endpoint, setting string) {
	if s.cfg.Endpoint != "" {
		return s.cfg.Endpoint, "endpoint"
	}
	if !s.cfg.ConnectionString.IsZero() {
		if e := connectionEndpoint(s.cfg.ConnectionString.Reveal()); e != "" {
			return e, "connectionString"
		}
	}
	return "https://" + s.accountName() + ".blob.core.windows.net", ""
}

// connectionEndpoint is a connection string's blob endpoint.
func connectionEndpoint(cs string) string {
	kv := map[string]string{}
	for part := range strings.SplitSeq(cs, ";") {
		if k, v, ok := strings.Cut(part, "="); ok {
			kv[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}
	if e := kv["blobendpoint"]; e != "" {
		return e
	}
	if kv["accountname"] == "" {
		return ""
	}
	return cmp.Or(kv["defaultendpointsprotocol"], "https") + "://" + kv["accountname"] + ".blob." + cmp.Or(kv["endpointsuffix"], "core.windows.net")
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
