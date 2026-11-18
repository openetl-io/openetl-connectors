package gcsstore

import (
	"encoding/json"
	"testing"
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
