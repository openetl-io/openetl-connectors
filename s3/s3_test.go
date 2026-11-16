//go:build integration

package s3

import (
	"bytes"
	"cmp"
	"crypto/rand"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/openetl-io/openetl/sdk/go/check"
)

// The emulators' addresses, and Azurite's well-known account.
var (
	s3Endpoint    = cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
	gcsEndpoint   = cmp.Or(os.Getenv("OPENETL_TEST_GCS_ENDPOINT"), "http://127.0.0.1:4443")
	azureEndpoint = cmp.Or(os.Getenv("OPENETL_TEST_AZURE_ENDPOINT"), "http://127.0.0.1:10000/devstoreaccount1")
)

const (
	azureAccount = "devstoreaccount1"
	azureKey     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	testBucket   = "openetl-test"
)

// stores are a config for each store the tests run against, at a prefix of
// their own in a bucket they create: file, S3, Google Cloud Storage and
// Azure Blob Storage.
func stores(t *testing.T) map[string]map[string]any {
	t.Helper()
	prefix := strings.ToLower(rand.Text())[:10]
	return map[string]map[string]any{
		"file": {"url": "file://" + t.TempDir()},
		"s3": {
			"url": "s3://" + s3Bucket(t) + "/" + prefix, "endpoint": s3Endpoint, "region": "us-east-1",
			"accessKeyId": "openetl", "secretAccessKey": "openetl",
		},
		"gcs":   {"url": "gs://" + gcsBucket(t) + "/" + prefix, "endpoint": gcsEndpoint},
		"azure": {"url": "azblob://" + azureContainer(t) + "/" + prefix, "endpoint": azureEndpoint, "accountName": azureAccount, "accountKey": azureKey},
	}
}

func s3Bucket(t *testing.T) string {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, s3Endpoint+"/"+testBucket, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create the S3 bucket (is the S3 store running at %s?): %v", s3Endpoint, err)
	}
	_ = res.Body.Close()
	return testBucket
}

func gcsBucket(t *testing.T) string {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, gcsEndpoint+"/storage/v1/b?project=openetl",
		bytes.NewReader([]byte(`{"name": "`+testBucket+`"}`)))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create the GCS bucket (is fake-gcs-server running at %s?): %v", gcsEndpoint, err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusConflict {
		t.Fatalf("create the GCS bucket: %s", res.Status)
	}
	return testBucket
}

func azureContainer(t *testing.T) string {
	t.Helper()
	cred, err := container.NewSharedKeyCredential(azureAccount, azureKey)
	if err != nil {
		t.Fatal(err)
	}
	c, err := container.NewClientWithSharedKeyCredential(azureEndpoint+"/"+testBucket, cred, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create(t.Context(), nil); err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		if errors.Is(err, t.Context().Err()) {
			t.Fatal(err)
		}
		t.Fatalf("create the Azure container (is Azurite running at %s?): %v", azureEndpoint, err)
	}
	return testBucket
}

// Every store the destination's tests run against.
func TestStores(t *testing.T) {
	for name, cfg := range stores(t) {
		if name == "file" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			suite(t, func(t *testing.T, extra map[string]any) *Destination {
				c := map[string]any{}
				for k, v := range cfg {
					c[k] = v
				}
				c["url"] = cfg["url"].(string) + "/" + strings.ToLower(rand.Text())[:8]
				return newDest(t, c, extra)
			})
		})
	}
}

func TestSourceStores(t *testing.T) {
	for name, cfg := range stores(t) {
		if name == "file" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			sourceTests(t, func(t *testing.T) map[string]any {
				c := maps.Clone(cfg)
				c["url"] = cfg["url"].(string) + "/" + strings.ToLower(rand.Text())[:8]
				return c
			})
		})
	}
}

// A check reports the store's network path, sign-in, bucket and listing
// item by item, and names a bucket that doesn't exist.
func TestCheckItems(t *testing.T) {
	for name, cfg := range stores(t) {
		t.Run(name, func(t *testing.T) {
			items := func(c map[string]any) map[string]string {
				t.Helper()
				b, _ := json.Marshal(c)
				d, err := NewDestination(b)
				if err != nil {
					t.Fatal(err)
				}
				out := map[string]string{}
				for _, it := range check.Run(t.Context(), d.Checks()).Items {
					out[it.Name] = it.Status.String()
					if it.Failure != nil {
						out[it.Name] += " " + it.Failure.Reason
					}
				}
				return out
			}
			want := map[string]string{"store.list": "passed", "store.write": "passed", "store.delete": "passed"}
			if name != "file" {
				want["net.dns"], want["net.connect"], want["store.auth"], want["store.bucket"] = "passed", "passed", "passed", "passed"
			}
			if got := items(cfg); !maps.Equal(got, want) {
				t.Errorf("good config: %v", got)
			}
			if name == "file" {
				return
			}
			u, _ := url.Parse(cfg["url"].(string))
			u.Host = "missing-" + strings.ToLower(rand.Text())[:8]
			c := maps.Clone(cfg)
			c["url"] = u.String()
			want["store.bucket"], want["store.list"], want["store.write"], want["store.delete"] = "failed not_found", "skipped", "skipped", "skipped"
			if got := items(c); !maps.Equal(got, want) {
				t.Errorf("a missing bucket: %v", got)
			}
		})
	}
}
