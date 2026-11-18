package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// dirStore is a Store of a local directory, whose write over a version does
// what ifMatch makes of it: the directory has no versions.
type dirStore struct {
	dir      string
	ifMatch  func(as func(any) bool, etag string) error
	conflict func(error) bool
}

func (s *dirStore) Scheme() string    { return "dir" }
func (s *dirStore) Container() string { return "bucket" }
func (s *dirStore) Open(context.Context, string) (*blob.Bucket, error) {
	return fileblob.OpenBucket(s.dir, &fileblob.Options{CreateDir: true, NoTempDir: true})
}
func (s *dirStore) Endpoint() (endpoint, setting string) { return "", "" }
func (s *dirStore) Credential() string                   { return "" }
func (s *dirStore) Privilege(string) string              { return "" }
func (s *dirStore) Code(error) protocolv1.ErrorCode {
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}
func (s *dirStore) Conflict(err error) bool                               { return s.conflict != nil && s.conflict(err) }
func (s *dirStore) Generation(*blob.Attributes) int64                     { return 0 }
func (s *dirStore) IfMatch(as func(any) bool, etag string, _ int64) error { return s.ifMatch(as, etag) }

func dirDestination(t *testing.T, st *dirStore) *Destination {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"url": "dir://bucket/files"})
	d, err := NewDestination(b, &struct{}{}, func(string) (Store, error) { return st, nil })
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// The conditional-write preflight warns of a store that ignores If-Match.
func TestIfMatchIgnored(t *testing.T) {
	d := dirDestination(t, &dirStore{dir: t.TempDir(), ifMatch: func(func(any) bool, string) error { return nil }})
	req := destination.PreflightRequest{Pipeline: "p", Streams: []destination.Stream{{Name: "a", Mode: destination.Overwrite}}}
	for _, it := range check.Run(t.Context(), d.Preflight(req)).Items {
		if it.Name != "store.conditional-write" {
			continue
		}
		if it.Failure == nil || it.Failure.Reason != unconditional.Name {
			t.Errorf("%s: %+v", it.Status, it.Failure)
		}
		return
	}
	t.Error("no conditional write")
}

// A manifest the store keeps refusing to replace fails the commit after a
// few attempts.
func TestPublishGivesUp(t *testing.T) {
	held := errors.New("held")
	d := dirDestination(t, &dirStore{dir: t.TempDir(),
		ifMatch:  func(func(any) bool, string) error { return held },
		conflict: func(err error) bool { return errors.Is(err, held) },
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	w, err := d.Open(ctx, destination.OpenConfig{Pipeline: "p", Streams: []destination.Stream{{Name: "users", Mode: destination.Overwrite}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close(ctx) }()
	if err := w.(*writer).b.WriteAll(ctx, "users/"+manifestPath, nil, nil); err != nil {
		t.Fatal(err)
	}
	err = w.(*writer).publish(ctx, staged{Generation: 1, Epoch: 1}, stagedStream{Name: "users", Table: "users", Read: "1", Ended: true})
	if !errors.Is(err, errConflict) || ctx.Err() != nil {
		t.Fatalf("%v, %v", err, ctx.Err())
	}
}

// A table named outside a file:// URL's directory gets no lock file there.
func TestSwapStaysInDirectory(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "files")
	b, _ := json.Marshal(map[string]any{"url": "file://" + dir})
	d, err := testDestination(b)
	if err != nil {
		t.Fatal(err)
	}
	bk, err := d.bucket(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bk.Close() }()
	key := "../outside/" + manifestPath
	if err := d.store.swap(t.Context(), bk, key, nil, nil, nil, func() (*version, error) { return nil, nil }); err == nil {
		t.Error("swapped a key outside the directory")
	}
	if _, err := os.Stat(filepath.Join(root, "outside")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("outside the directory: %v", err)
	}
}
