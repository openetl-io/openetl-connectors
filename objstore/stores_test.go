//go:build integration

package objstore

import (
	"crypto/rand"
	"encoding/json"
	"maps"
	"net/url"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/check"
)

// stores are the configs of every store the tests run against.
func stores(t *testing.T) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, st := range []TestStore{FileStore(t), S3Store(t), GCSStore(t), AzureStore(t)} {
		out[st.Name] = st.Config
	}
	return out
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
				d, err := testDestination(b)
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
