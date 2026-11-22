package s3store

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"gocloud.dev/blob"

	sdk "github.com/openetl-io/openetl/sdk/go"
)

// A multipart upload whose writer's context ends is aborted, though the
// abort can't use that context, and an upload to an endpoint carries no
// checksum the store didn't ask for.
func TestUploads(t *testing.T) {
	var mu sync.Mutex
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		q := r.URL.Query()
		mu.Lock()
		got = append(got, r.Method)
		for k := range r.Header {
			if strings.Contains(strings.ToLower(k), "checksum") {
				got = append(got, k)
			}
		}
		mu.Unlock()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key><UploadId>u</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut && q.Has("partNumber"):
			time.Sleep(200 * time.Millisecond)
			w.Header().Set("ETag", `"e"`)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.Header().Set("ETag", `"e"`)
		}
	}))
	defer srv.Close()
	s, err := New(Config{Endpoint: srv.URL, Region: "us-east-1", AccessKeyID: sdk.NewSecret("a"), SecretAccessKey: sdk.NewSecret("b")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Open(t.Context(), "b")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	// As objstore gives a write the store's options.
	opts := &blob.WriterOptions{BeforeWrite: func(as func(any) bool) error {
		return s.BeforeWrite(func(i any) bool { return as(i) || b.As(i) })
	}}
	if err := b.WriteAll(t.Context(), "small", []byte("x"), opts); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	w, err := b.NewWriter(ctx, "big", opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(w, bytes.NewReader(make([]byte, 20<<20))); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := w.Close(); err == nil || strings.Contains(err.Error(), "failed to abort") {
		t.Errorf("close: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Contains(got, http.MethodDelete) || slices.ContainsFunc(got, func(s string) bool { return strings.Contains(s, "-") }) {
		t.Errorf("requests %v", got)
	}
}
