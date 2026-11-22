//go:build integration

package gcs

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"gocloud.dev/blob"

	"github.com/openetl-io/openetl-connectors/objstore"
	"github.com/openetl-io/openetl-connectors/objstore/gcsstore"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func TestLive(t *testing.T) {
	bucket := os.Getenv("OPENETL_TEST_GCS_BUCKET")
	if bucket == "" {
		t.Skip("OPENETL_TEST_GCS_BUCKET isn't set")
	}
	prefixBase := cmp.Or(strings.Trim(os.Getenv("OPENETL_TEST_GCS_PREFIX"), "/"), "ci-smoke")
	runPrefix := prefixBase + "/" + strings.ToLower(rand.Text())[:10]
	t.Cleanup(func() { cleanupGCS(t, bucket, runPrefix) })

	badKey := invalidServiceAccountKey(t)
	cfg := map[string]any{
		"url": "gs://" + bucket + "/" + runPrefix,
	}
	st := objstore.TestStore{
		Name:           "gcs-live",
		Config:         cfg,
		Unreachable:    map[string]any{"endpoint": "http://127.0.0.1:1"},
		BadCredentials: map[string]any{"serviceAccountKey": badKey},
	}

	t.Run("conformance", func(t *testing.T) {
		objstore.Conformance(t, "gcs", st)
	})

	t.Run("checks", func(t *testing.T) {
		d, err := NewDestination(mustMarshal(t, cfg))
		if err != nil {
			t.Fatal(err)
		}
		rep := check.Run(t.Context(), d.Checks())
		if err := rep.Err(); err != nil {
			t.Fatalf("destination checks failed: %v", err)
		}
		assertPassed(t, rep, "net.dns", "net.connect", "net.tls", "store.auth", "store.bucket", "store.list", "store.write", "store.delete")

		preflight := check.Run(t.Context(), d.Preflight(destination.PreflightRequest{
			Pipeline: "live-smoke",
			Streams:  []destination.Stream{{Name: "events", Mode: destination.Overwrite}},
		}))
		if err := preflight.Err(); err != nil {
			t.Fatalf("destination preflight failed: %v", err)
		}
		assertPassed(t, preflight, "store.conditional-write")

		srcCfg := map[string]any{
			"url": cfg["url"],
			"streams": []map[string]any{
				{"name": "events", "glob": "events/*.parquet", "format": "parquet"},
			},
		}
		s, err := NewSource(mustMarshal(t, srcCfg))
		if err != nil {
			t.Fatal(err)
		}
		srcRep := check.Run(t.Context(), s.Checks())
		if err := srcRep.Err(); err != nil {
			t.Fatalf("source checks failed: %v", err)
		}
		assertPassed(t, srcRep, "net.dns", "net.connect", "net.tls", "store.auth", "store.bucket", "store.list")
	})

	t.Run("bad-credentials", func(t *testing.T) {
		d, err := NewDestination(mustMarshal(t, map[string]any{
			"url":               cfg["url"],
			"serviceAccountKey": badKey,
		}))
		if err != nil {
			t.Fatal(err)
		}
		err = check.Run(t.Context(), d.Checks()).Err()
		if got := codeOf(err); got != protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID {
			t.Fatalf("bad credentials code = %s (%v), want ERROR_CODE_AUTH_INVALID", got, err)
		}
	})

	t.Run("denied-bucket", func(t *testing.T) {
		denied := os.Getenv("OPENETL_TEST_GCS_DENIED_BUCKET")
		if denied == "" {
			t.Skip("OPENETL_TEST_GCS_DENIED_BUCKET isn't set")
		}
		d, err := NewDestination(mustMarshal(t, map[string]any{
			"url": "gs://" + denied + "/" + runPrefix,
		}))
		if err != nil {
			t.Fatal(err)
		}
		rep := check.Run(t.Context(), d.Checks())
		if got := codeOf(rep.Err()); got != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
			t.Fatalf("denied bucket code = %s (%v), want ERROR_CODE_PERMISSION_DENIED", got, rep.Err())
		}
		var listStatus check.Status
		for _, it := range rep.Items {
			if it.Name == "store.list" {
				listStatus = it.Status
			}
		}
		if listStatus != check.Failed {
			t.Fatalf("store.list status = %s, want failed", listStatus)
		}
	})
}

func cleanupGCS(t *testing.T, bucketName, prefix string) {
	t.Helper()
	st, err := gcsstore.New(gcsstore.Config{})
	if err != nil {
		t.Logf("cleanup gcsstore.New: %v", err)
		return
	}
	ctx := context.Background()
	b, err := st.Open(ctx, bucketName)
	if err != nil {
		t.Logf("cleanup open %s: %v", bucketName, err)
		return
	}
	defer func() { _ = b.Close() }()
	it := b.List(&blob.ListOptions{Prefix: prefix + "/"})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Logf("cleanup list %s/%s: %v", bucketName, prefix, err)
			return
		}
		if err := b.Delete(ctx, obj.Key); err != nil {
			t.Logf("cleanup delete %s: %v", obj.Key, err)
		}
	}
}

func invalidServiceAccountKey(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der := x509.MarshalPKCS1PrivateKey(key)
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: der})
	return string(mustMarshal(t, map[string]string{
		"type":                        "service_account",
		"project_id":                  "openetl-invalid",
		"private_key_id":              "invalid-key-id",
		"private_key":                 string(pemKey),
		"client_email":                "invalid@openetl-invalid.iam.gserviceaccount.com",
		"client_id":                   "0",
		"auth_uri":                    "https://accounts.google.com/o/oauth2/auth",
		"token_uri":                   "https://oauth2.googleapis.com/token",
		"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
	}))
}

func assertPassed(t *testing.T, rep check.Report, names ...string) {
	t.Helper()
	statuses := map[string]check.Status{}
	for _, it := range rep.Items {
		statuses[it.Name] = it.Status
	}
	for _, name := range names {
		st, ok := statuses[name]
		if !ok {
			t.Errorf("check %q missing from report: %v", name, rep.Items)
			continue
		}
		if st != check.Passed {
			t.Errorf("check %q = %s, want passed", name, st)
		}
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func codeOf(err error) protocolv1.ErrorCode {
	if c, ok := errors.AsType[interface {
		error
		ErrorCode() protocolv1.ErrorCode
	}](err); ok {
		return c.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_INTERNAL
}
