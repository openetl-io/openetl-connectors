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

	"github.com/openetl-io/openetl/sdk/go/check"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// A destination without AWS credentials, or one that S3 refuses, fails its
// check with an auth or permission code, which the platform doesn't retry,
// rather than INTERNAL; and one that S3 fails, with TRANSIENT, which it does.
func TestAuthErrors(t *testing.T) {
	noAWS(t)
	answer := func(status int, code string) string { return answer(t, status, code) }
	for _, tc := range []struct {
		name     string
		endpoint string
		keys     bool
		want     protocolv1.ErrorCode
		says     string
	}{
		{"no credentials", answer(http.StatusOK, ""), false, protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID,
			"no AWS credentials: set accessKeyId and secretAccessKey, or name a role in the connector's aws.role"},
		{"unknown keys", answer(http.StatusForbidden, "InvalidAccessKeyId"), true, protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, "InvalidAccessKeyId"},
		{"no access to the bucket", answer(http.StatusForbidden, "AccessDenied"), true, protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "AccessDenied"},
		{"an expired session", answer(http.StatusBadRequest, "ExpiredToken"), true, protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED, "ExpiredToken"},
		{"a server error", answer(http.StatusInternalServerError, "InternalError"), true, protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, "InternalError"},
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
			err = check.Run(t.Context(), d.Checks()).Err()
			if got := codeOf(err); got != tc.want {
				t.Errorf("Check: %s (%v), want %s", got, err, tc.want)
			}
			if err == nil || !strings.Contains(err.Error(), tc.says) {
				t.Errorf("Check: %v\nwant it to say %q", err, tc.says)
			}
		})
	}
}

// Credentials that the connector can't get, as when the runner's endpoint
// of a role's doesn't answer, fail the check with TRANSIENT, which the
// platform retries.
func TestCredentialsUnreachable(t *testing.T) {
	noAWS(t)
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1:1/credentials")
	b, _ := json.Marshal(map[string]any{"url": "s3://lake/files", "region": "us-east-1", "endpoint": answer(t, http.StatusOK, "")})
	d, err := NewDestination(b)
	if err != nil {
		t.Fatal(err)
	}
	err = check.Run(t.Context(), d.Checks()).Err()
	if got := codeOf(err); got != protocolv1.ErrorCode_ERROR_CODE_TRANSIENT || !strings.Contains(err.Error(), "AWS credentials") {
		t.Errorf("Check: %s (%v)", got, err)
	}
}

// No item or error of a check shows the keys' values: S3 gets the secret
// key's signature, not the key, and the access key's ID that its answer
// echoes isn't in its error's message.
func TestKeysNotShown(t *testing.T) {
	noAWS(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>SignatureDoesNotMatch</Code><Message>refused</Message>`+
			`<AWSAccessKeyId>AKIAKEYID</AWSAccessKeyId><RequestId>r</RequestId></Error>`)
	}))
	t.Cleanup(srv.Close)
	for _, endpoint := range []string{srv.URL, "http://127.0.0.1:1"} {
		b, _ := json.Marshal(map[string]any{"url": "s3://lake/files", "region": "us-east-1", "endpoint": endpoint,
			"accessKeyId": "AKIAKEYID", "secretAccessKey": "SECRETKEY"})
		d, err := NewDestination(b)
		if err != nil {
			t.Fatal(err)
		}
		rep := check.Run(t.Context(), d.Checks())
		shown := fmt.Sprint(rep.Err())
		for _, it := range rep.Items {
			shown += fmt.Sprint(it)
		}
		if rep.Err() == nil || strings.Contains(shown, "AKIAKEYID") || strings.Contains(shown, "SECRETKEY") {
			t.Errorf("%s: %s", endpoint, shown)
		}
	}
}

// A bucket in another region than region fails the check, and the read or
// write, with CONFIG_INVALID, naming the bucket's region to set: S3 answers
// with a redirect, whose body a HEAD hasn't, or, to a request signed for
// another region, AuthorizationHeaderMalformed.
func TestWrongRegion(t *testing.T) {
	noAWS(t)
	for name, answer := range map[string]func(w http.ResponseWriter){
		"redirect": func(w http.ResponseWriter) {
			w.Header().Set("X-Amz-Bucket-Region", "eu-west-1")
			w.WriteHeader(http.StatusMovedPermanently)
			_, _ = fmt.Fprint(w, `<Error><Code>PermanentRedirect</Code><Message>The bucket you are attempting to access must be addressed using the specified endpoint.</Message>`+
				`<Endpoint>lake.s3.eu-west-1.amazonaws.com</Endpoint><Bucket>lake</Bucket></Error>`)
		},
		"redirect without a body": func(w http.ResponseWriter) {
			w.Header().Set("X-Amz-Bucket-Region", "eu-west-1")
			w.WriteHeader(http.StatusMovedPermanently)
		},
		"signed for another region": func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprint(w, `<Error><Code>AuthorizationHeaderMalformed</Code><Message>The authorization header is malformed; the region 'us-east-1' is wrong; expecting 'eu-west-1'</Message>`+
				`<Region>eu-west-1</Region></Error>`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { answer(w) }))
			t.Cleanup(srv.Close)
			b, _ := json.Marshal(map[string]any{"url": "s3://lake/files", "region": "us-east-1", "endpoint": srv.URL, "accessKeyId": "a", "secretAccessKey": "b"})
			d, err := NewDestination(b)
			if err != nil {
				t.Fatal(err)
			}
			err = check.Run(t.Context(), d.Checks()).Err()
			if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID || err == nil || !strings.Contains(err.Error(), "set region to eu-west-1") {
				t.Errorf("Check: %s (%v)", codeOf(err), err)
			}
		})
	}
}

// The check's write to an endpoint carries no checksum the store didn't ask
// for, as some S3-compatible stores refuse S3's.
func TestNoChecksumOnAnEndpoint(t *testing.T) {
	noAWS(t)
	var checksums []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k := range r.Header {
			if strings.Contains(strings.ToLower(k), "checksum") {
				checksums = append(checksums, r.Method+" "+k)
			}
		}
		w.Header().Set("ETag", `"e"`)
	}))
	t.Cleanup(srv.Close)
	b, _ := json.Marshal(map[string]any{"url": "s3://lake/files", "region": "us-east-1", "endpoint": srv.URL, "accessKeyId": "a", "secretAccessKey": "b"})
	d, err := NewDestination(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Run(t.Context(), d.Checks()).Err(); err != nil || len(checksums) > 0 {
		t.Errorf("check: %v; %v", err, checksums)
	}
}

// noAWS gives the test no AWS credentials, as the runner gives a connector
// by default.
func noAWS(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none")
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "", "AWS_SESSION_TOKEN": "", "AWS_PROFILE": "", "AWS_ROLE_ARN": "", "AWS_WEB_IDENTITY_TOKEN_FILE": "",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI": "", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
		"AWS_CONFIG_FILE": none, "AWS_SHARED_CREDENTIALS_FILE": none, "AWS_EC2_METADATA_DISABLED": "true",
	} {
		t.Setenv(k, v)
	}
}

// answer is the URL of a server that answers every request as S3 does, with
// status and code.
func answer(t *testing.T, status int, code string) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>refused</Message><RequestId>r</RequestId></Error>`, code)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
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
