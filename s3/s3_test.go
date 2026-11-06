//go:build integration

package s3

import (
	"cmp"
	"crypto/rand"
	"net/http"
	"os"
	"strings"
	"testing"
)

// Runs against the compose S3 store (make deps-up), or OPENETL_TEST_S3_ENDPOINT,
// in its s3-destination bucket.
func TestS3(t *testing.T) {
	endpoint := cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, endpoint+"/s3-destination", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create the bucket (is the S3 store running? make deps-up): %v", err)
	}
	_ = res.Body.Close()
	suite(t, func(t *testing.T, extra map[string]any) *Destination {
		return newDest(t, map[string]any{
			"url": "s3://s3-destination/" + strings.ToLower(rand.Text())[:10], "endpoint": endpoint, "region": "us-east-1",
			"accessKeyId": "openetl", "secretAccessKey": "openetl",
		}, extra)
	})
}
