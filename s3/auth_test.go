package s3

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// A destination without AWS credentials, or one that S3 refuses, fails its
// check with an auth or permission code, which the platform doesn't retry,
// rather than INTERNAL.
func TestAuthErrors(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none")
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "", "AWS_SESSION_TOKEN": "", "AWS_PROFILE": "", "AWS_ROLE_ARN": "", "AWS_WEB_IDENTITY_TOKEN_FILE": "",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI": "", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
		"AWS_CONFIG_FILE": none, "AWS_SHARED_CREDENTIALS_FILE": none, "AWS_EC2_METADATA_DISABLED": "true",
	} {
		t.Setenv(k, v)
	}
	// answer refuses every request as S3 does, with code.
	answer := func(status int, code string) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(status)
			_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>refused</Message><RequestId>r</RequestId></Error>`, code)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	for _, tc := range []struct {
		name     string
		endpoint string
		keys     bool
		want     protocolv1.ErrorCode
		says     string
	}{
		{"no credentials", answer(http.StatusOK, ""), false, protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID,
			"no AWS credentials: set accessKeyId and secretAccessKey, or name a role in the destination's aws.role"},
		{"unknown keys", answer(http.StatusForbidden, "InvalidAccessKeyId"), true, protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, "InvalidAccessKeyId"},
		{"no access to the bucket", answer(http.StatusForbidden, "AccessDenied"), true, protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "AccessDenied"},
		{"an expired session", answer(http.StatusBadRequest, "ExpiredToken"), true, protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED, "ExpiredToken"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]any{"url": "s3://lake/files", "region": "us-east-1", "endpoint": tc.endpoint}
			if tc.keys {
				cfg["accessKeyId"], cfg["secretAccessKey"] = "AKIAEXAMPLE", "secret"
			}
			b, _ := json.Marshal(cfg)
			d, err := NewDestination(b)
			if err != nil {
				t.Fatal(err)
			}
			err = d.Check(t.Context())
			if got := codeOf(err); got != tc.want {
				t.Errorf("Check: %s (%v), want %s", got, err, tc.want)
			}
			if err == nil || !strings.Contains(err.Error(), tc.says) {
				t.Errorf("Check: %v\nwant it to say %q", err, tc.says)
			}
		})
	}
}

// codeOf is err's taxonomy code, as the SDK reads it: INTERNAL when it has
// none.
func codeOf(err error) protocolv1.ErrorCode {
	if c, ok := errors.AsType[interface {
		error
		ErrorCode() protocolv1.ErrorCode
	}](err); ok {
		return c.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_INTERNAL
}
