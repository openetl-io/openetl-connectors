package azurestore

import (
	"encoding/json"
	"testing"
)

const key = "a2V5"

// A SAS is refused over plain HTTP, where its request's URL carries it as it
// is, and an account's key, which signs requests, isn't.
func TestCredentialsOverHTTP(t *testing.T) {
	for _, c := range []struct {
		config string
		ok     bool
	}{
		{`{"accountName": "a", "sasToken": "sv=1&sig=x"}`, true},
		{`{"endpoint": "https://a.example.com", "sasToken": "sv=1&sig=x"}`, true},
		{`{"endpoint": "http://127.0.0.1:10000/a", "sasToken": "sv=1&sig=x"}`, false},
		{`{"connectionString": "BlobEndpoint=http://127.0.0.1:10000/a;SharedAccessSignature=sv=1&sig=x"}`, false},
		{`{"connectionString": "DefaultEndpointsProtocol=http;AccountName=a;SharedAccessSignature=sv=1&sig=x"}`, false},
		{`{"connectionString": "AccountName=a;SharedAccessSignature=sv=1&sig=x"}`, true},
		{`{"endpoint": "http://127.0.0.1:10000/a", "accountKey": "` + key + `"}`, true},
		{`{"connectionString": "BlobEndpoint=http://127.0.0.1:10000/a;AccountName=a;AccountKey=` + key + `;SharedAccessSignature=sv=1&sig=x"}`, true},
		{`{"endpoint": "https://a.example.com", "connectionString": "AccountName=a;AccountKey=` + key + `"}`, false},
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

// A connection string's endpoint is its setting's only where it names a
// BlobEndpoint, so the network checks of Azure's own address only warn.
func TestEndpoint(t *testing.T) {
	for _, c := range []struct {
		config, endpoint, setting string
	}{
		{`{"accountName": "a"}`, "https://a.blob.core.windows.net", ""},
		{`{"endpoint": "http://127.0.0.1:10000/a", "accountKey": "` + key + `"}`, "http://127.0.0.1:10000/a", "endpoint"},
		{`{"connectionString": "AccountName=a;AccountKey=` + key + `;EndpointSuffix=core.windows.net"}`, "https://a.blob.core.windows.net", ""},
		{`{"connectionString": "BlobEndpoint=http://127.0.0.1:10000/a;AccountName=a;AccountKey=` + key + `"}`, "http://127.0.0.1:10000/a", "connectionString"},
	} {
		var cfg Config
		if err := json.Unmarshal([]byte(c.config), &cfg); err != nil {
			t.Fatal(err)
		}
		s, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if endpoint, setting := s.Endpoint(); endpoint != c.endpoint || setting != c.setting {
			t.Errorf("%s: %s, %q", c.config, endpoint, setting)
		}
	}
}
