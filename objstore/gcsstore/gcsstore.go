// Package gcsstore is the objstore Store of Google Cloud Storage.
package gcsstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Config is how to reach the store.
type Config struct {
	// The JSON API's endpoint, such as an emulator's; empty for Google's.
	Endpoint string `json:"endpoint"`
	// A service account's JSON key. Without one, Google's default
	// credentials, or none with an endpoint. With an endpoint, it needs
	// https.
	ServiceAccountKey sdk.Secret `json:"serviceAccountKey"`
}

type Store struct{ cfg Config }

func New(cfg Config) (*Store, error) {
	// The key's tokens go in each request's headers, as they are.
	if u, err := url.Parse(cfg.Endpoint); cfg.Endpoint != "" && !cfg.ServiceAccountKey.IsZero() && (err != nil || u.Scheme != "https") {
		return nil, errors.New("gcs: serviceAccountKey needs an https endpoint; an emulator's takes none")
	}
	return &Store{cfg: cfg}, nil
}

func (s *Store) Scheme() string    { return "gs" }
func (s *Store) Container() string { return "bucket" }

// attempts bounds the client's retries, which otherwise go on while ctx
// lasts, as they do for a store that doesn't answer.
const attempts = 4

func (s *Store) Open(ctx context.Context, bucket string) (*blob.Bucket, error) {
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
		return nil, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID,
			fmt.Errorf("gcs: Google Cloud credentials: set serviceAccountKey to a service account's JSON key: %w", err))
	}
	c.SetRetry(storage.WithMaxAttempts(attempts))
	return gcsblob.OpenBucket(ctx, nil, bucket, &gcsblob.Options{Client: c})
}

func (s *Store) Endpoint() (endpoint, setting string) {
	if s.cfg.Endpoint != "" {
		return s.cfg.Endpoint, "endpoint"
	}
	return "https://storage.googleapis.com", ""
}

func (s *Store) Credential() string {
	if !s.cfg.ServiceAccountKey.IsZero() {
		return "serviceAccountKey"
	}
	return ""
}

func (s *Store) Privilege(action string) string {
	return "storage.objects." + map[string]string{"list": "list", "read": "get", "write": "create", "delete": "delete"}[action]
}

func (s *Store) Code(err error) protocolv1.ErrorCode {
	if ge, ok := errors.AsType[*googleapi.Error](err); ok {
		switch {
		case ge.Code == http.StatusUnauthorized:
			return protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID
		case ge.Code == http.StatusForbidden:
			return protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
		case ge.Code == http.StatusTooManyRequests:
			return protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED
		case ge.Code >= 500:
			return protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
		}
	}
	if _, ok := errors.AsType[*oauth2.RetrieveError](err); ok {
		return protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

func (s *Store) Conflict(err error) bool {
	ge, ok := errors.AsType[*googleapi.Error](err)
	return ok && ge.Code == http.StatusPreconditionFailed
}

func (s *Store) IfMatch(as func(any) bool, _ string, generation int64) error {
	var oh **storage.ObjectHandle
	if !as(&oh) {
		return errors.New("gcs: the store has no conditional writes")
	}
	*oh = (*oh).If(storage.Conditions{GenerationMatch: generation})
	return nil
}

func (s *Store) Generation(a *blob.Attributes) int64 {
	var oa storage.ObjectAttrs
	if a.As(&oa) {
		return oa.Generation
	}
	return 0
}

// Version is a listed or opened object's generation.
func (s *Store) Version(as func(any) bool) string {
	var gen int64
	var oa storage.ObjectAttrs
	var r *storage.Reader
	switch {
	case as(&oa):
		gen = oa.Generation
	case as(&r) && r != nil:
		gen = r.Attrs.Generation
	}
	if gen == 0 {
		return ""
	}
	return strconv.FormatInt(gen, 10)
}
