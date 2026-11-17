package objstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"

	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/gcerrors"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Store is what's particular to a store: its buckets, how to reach them,
// and what its answers mean. A connector's package has one; file:// URLs
// have none.
type Store interface {
	// Scheme is its URLs', and Container what it calls a bucket.
	Scheme() string
	Container() string
	// Open opens a bucket, the whole of it.
	Open(ctx context.Context, bucket string) (*blob.Bucket, error)
	// Endpoint is the URL its API answers at, and the setting that names
	// it; empty for the store's own.
	Endpoint() (endpoint, setting string)
	// Credential is the setting of the credentials it uses; empty for its
	// default ones.
	Credential() string
	// Privilege is what it calls the access an action needs: list, read,
	// write or delete.
	Privilege(action string) string
	// Code is the code of an error it gave; unspecified for one it doesn't
	// know.
	Code(err error) protocolv1.ErrorCode
	// Conflict: it refused a conditional write.
	Conflict(err error) bool
	// IfMatch makes a write, whose driver's options as gives, write only over
	// the object at etag, or generation where the store numbers versions.
	IfMatch(as func(any) bool, etag string, generation int64) error
	// Generation is an object's version number; 0 where ETags tell versions.
	Generation(a *blob.Attributes) int64
}

// NewStore is a connector's store of settings, which the config decoded
// into, for url; nil for a file:// URL the connector takes.
type NewStore func(url string) (Store, error)

// decode decodes config into the core's cfg and the store's settings,
// each strictly, so a setting that neither has, at any depth, is refused.
func decode(config []byte, cfg, settings any) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(config, &m); err != nil {
		return fmt.Errorf("objstore: config: %w", err)
	}
	known := map[string]bool{}
	jsonNames(reflect.TypeOf(settings).Elem(), known)
	core, store := map[string]json.RawMessage{}, map[string]json.RawMessage{}
	for k, v := range m {
		if known[strings.ToLower(k)] {
			store[k] = v
		} else {
			core[k] = v
		}
	}
	for _, p := range []struct {
		m map[string]json.RawMessage
		v any
	}{{core, cfg}, {store, settings}} {
		b, err := json.Marshal(p.m)
		if err != nil {
			return fmt.Errorf("objstore: config: %w", err)
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		if err := dec.Decode(p.v); err != nil {
			return fmt.Errorf("objstore: config: %w", err)
		}
	}
	return nil
}

// jsonNames adds the JSON names of t's fields to known, in lower case, as
// encoding/json matches them.
func jsonNames(t reflect.Type, known map[string]bool) {
	for i := range t.NumField() {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		switch {
		case name == "-":
		case f.Anonymous && name == "":
			jsonNames(f.Type, known)
		case name == "":
			known[strings.ToLower(f.Name)] = true
		default:
			known[strings.ToLower(name)] = true
		}
	}
}

// store is where the files are: a bucket of a Store, or a local directory.
type store struct {
	url string
	u   *url.URL
	// Nil for file.
	st Store
	// The bucket or container; empty for file.
	bucket string
	// The path under it, without slashes around it; for file, the
	// directory's path.
	prefix string
}

func newStore(raw string, open NewStore) (*store, error) {
	st, err := open(raw)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("objstore: url: %w", err)
	}
	s := &store{url: raw, u: u, st: st}
	want := "file:// and an absolute path"
	switch {
	case u.RawQuery != "" || u.Fragment != "":
	case st != nil:
		want = st.Scheme() + "://" + st.Container() + "/prefix"
		if u.Scheme == st.Scheme() && u.Host != "" {
			s.bucket, s.prefix = u.Host, strings.Trim(u.Path, "/")
		}
	case u.Scheme == "file" && (u.Host == "" || u.Host == "localhost") && path.IsAbs(u.Path):
		s.prefix = u.Path
	}
	if s.bucket == "" && s.prefix == "" {
		return nil, fmt.Errorf("objstore: url %q must be %s", raw, want)
	}
	return s, nil
}

// open opens the bucket, below the URL's path.
func (s *store) open(ctx context.Context) (*blob.Bucket, error) {
	if s.st == nil {
		b, err := fileblob.OpenBucket(s.prefix, &fileblob.Options{CreateDir: true, NoTempDir: true})
		if err != nil {
			return nil, fmt.Errorf("objstore: open %s: %w", s.url, err)
		}
		return b, nil
	}
	b, err := s.st.Open(ctx, s.bucket)
	if err != nil {
		return nil, fmt.Errorf("objstore: open %s: %w", s.url, err)
	}
	if s.prefix != "" {
		b = blob.PrefixedBucket(b, s.prefix+"/")
	}
	return b, nil
}

// location is key as the URL names it.
func (s *store) location(key string) string {
	u := *s.u
	u.Path = path.Join(u.Path, key)
	return u.String()
}

// key is the key of a location the URL names, and whether it's under it.
func (s *store) key(location string) (string, bool) {
	base := strings.TrimSuffix(s.u.String(), "/") + "/"
	if !strings.HasPrefix(location, base) {
		return "", false
	}
	return strings.TrimPrefix(location, base), true
}

// version is what a conditional write compares an object with: its ETag,
// or its generation where the store numbers versions. Nil: the object
// doesn't exist.
type version struct {
	etag       string
	generation int64
}

func (s *store) version(a *blob.Attributes) *version {
	v := &version{etag: a.ETag}
	if s.st != nil {
		v.generation = s.st.Generation(a)
	}
	return v
}

// errConflict: a conditional write found the object changed.
var errConflict = errors.New("objstore: the object changed since it was read")

// swap writes data to key if key is still at was: a compare and swap with
// the store's conditional write, and under an exclusive lock on the file for
// file:// URLs, whose lock check must run within it. It returns errConflict
// if key moved.
func (s *store) swap(ctx context.Context, b *blob.Bucket, key string, data []byte, md map[string]string, was *version, check func() (*version, error)) error {
	if s.st == nil {
		unlock, err := lockFile(filepath.Join(s.prefix, filepath.FromSlash(key)) + ".lock")
		if err != nil {
			return err
		}
		defer unlock()
		now, err := check()
		if err != nil {
			return err
		}
		if (now == nil) != (was == nil) || now != nil && now.etag != was.etag {
			return errConflict
		}
		return b.WriteAll(ctx, key, data, &blob.WriterOptions{Metadata: md})
	}
	opts := &blob.WriterOptions{Metadata: md, IfNotExist: was == nil, BeforeWrite: func(as func(any) bool) error {
		if was == nil {
			return nil
		}
		return s.st.IfMatch(as, was.etag, was.generation)
	}}
	err := b.WriteAll(ctx, key, data, opts)
	if gcerrors.Code(err) == gcerrors.FailedPrecondition || err != nil && s.st.Conflict(err) {
		return errConflict
	}
	return err
}

// lockFile takes an exclusive lock on name, which it creates, until unlock.
func lockFile(name string) (unlock func(), err error) {
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

// classify makes failures to reach the store TRANSIENT, and gives the
// store's refusals and a missing credential their taxonomy codes.
func (s *store) classify(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}
	if _, ok := errors.AsType[interface {
		error
		ErrorCode() protocolv1.ErrorCode
	}](err); ok {
		return err
	}
	if s.st != nil {
		if c := s.st.Code(err); c != protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
			return sdk.WithCode(c, err)
		}
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
	}
	return err
}
