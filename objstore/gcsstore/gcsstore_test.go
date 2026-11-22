package gcsstore

import (
	"encoding/json"
	"strings"
	"testing"

	"gocloud.dev/blob"

	sdk "github.com/openetl-io/openetl/sdk/go"
)

// A service account key is refused with an endpoint over plain HTTP, which
// would carry its tokens as they are, and taken over HTTPS.
func TestKeyOverHTTP(t *testing.T) {
	for _, c := range []struct {
		config string
		ok     bool
	}{
		{`{"endpoint": "http://127.0.0.1:4443"}`, true},
		{`{"serviceAccountKey": "{}"}`, true},
		{`{"endpoint": "https://storage.example.com", "serviceAccountKey": "{}"}`, true},
		{`{"endpoint": "http://127.0.0.1:4443", "serviceAccountKey": "{}"}`, false},
		{`{"endpoint": "HTTP://storage.example.com", "serviceAccountKey": "{}"}`, false},
	} {
		var cfg Config
		if err := json.Unmarshal([]byte(c.config), &cfg); err != nil {
			t.Fatal(err)
		}
		if _, err := New(cfg); (err == nil) != c.ok {
			t.Errorf("%s: %v", c.config, err)
		}
	}
}

// No error of a key that's wrong, or of a token endpoint that doesn't answer,
// shows the key.
func TestKeyNotShown(t *testing.T) {
	for _, key := range []string{
		`{"type": "service_account", "private_key": "SECRETKEY", "client_email": "a@b.c", "token_uri": "https://127.0.0.1:1/token"}`,
		`{"type": "service_account", "private_key": "-----BEGIN PRIVATE KEY-----\nSECRETKEY\n-----END PRIVATE KEY-----\n", "client_email": "a@b.c", "token_uri": "https://127.0.0.1:1/token"}`,
		`SECRETKEY`,
		`{"SECRETKEY": 1`,
	} {
		s, err := New(Config{Endpoint: "https://127.0.0.1:1", ServiceAccountKey: sdk.NewSecret(key)})
		if err != nil {
			t.Fatal(err)
		}
		b, err := s.Open(t.Context(), "c")
		if err == nil {
			_, _, err = b.ListPage(t.Context(), blob.FirstPageToken, 1, nil)
			_ = b.Close()
		}
		if err == nil || strings.Contains(err.Error(), "SECRETKEY") {
			t.Errorf("%.20s: %v", key, err)
		}
	}
}
