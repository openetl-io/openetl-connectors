package azurestore

import (
	"encoding/json"
	"strings"
	"testing"

	"gocloud.dev/blob"

	sdk "github.com/openetl-io/openetl/sdk/go"
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

// A request that fails before the store answers doesn't show its URL's
// query, which holds the SAS, in its error.
func TestSASNotShown(t *testing.T) {
	for name, cfg := range map[string]Config{
		"sasToken":         {Endpoint: "https://127.0.0.1:1/a", SASToken: sdk.NewSecret("sv=2024-11-04&sig=SECRETSIG")},
		"connectionString": {ConnectionString: sdk.NewSecret("BlobEndpoint=https://127.0.0.1:1/a;SharedAccessSignature=sv=2024-11-04&sig=SECRETSIG")},
		"newline":          {Endpoint: "https://127.0.0.1:1/a", SASToken: sdk.NewSecret("sv=2024-11-04&sig=SECRETSIG\n")},
		"newlineInString":  {ConnectionString: sdk.NewSecret("BlobEndpoint=https://127.0.0.1:1/a;SharedAccessSignature=sv=2024-11-04&sig=SECRETSIG\n")},
		"escape":           {Endpoint: "https://127.0.0.1:1/a", SASToken: sdk.NewSecret("sv=2024-11-04&sig=SECRETSIG%zz")},
		"accountKey":       {Endpoint: "https://127.0.0.1:1/a", AccountKey: sdk.NewSecret("SECRETSIG!")},
		"malformed":        {ConnectionString: sdk.NewSecret("SECRETSIG")},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, err := New(cfg)
			if err != nil {
				if strings.Contains(err.Error(), "SECRETSIG") {
					t.Errorf("new: %v", err)
				}
				return
			}
			b, err := s.Open(t.Context(), "c")
			if err != nil {
				if strings.Contains(err.Error(), "SECRETSIG") {
					t.Errorf("open: %v", err)
				}
				return
			}
			defer func() { _ = b.Close() }()
			_, _, err = b.ListPage(t.Context(), blob.FirstPageToken, 1, nil)
			if err == nil || strings.Contains(err.Error(), "SECRETSIG") {
				t.Errorf("list: %v", err)
			}
			if err := b.WriteAll(t.Context(), "k", []byte("x"), nil); err == nil || strings.Contains(err.Error(), "SECRETSIG") {
				t.Errorf("write: %v", err)
			}
		})
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
