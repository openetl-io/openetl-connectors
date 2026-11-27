package objstore

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openetl-io/openetl-connectors/objstore/s3store"
	"github.com/openetl-io/openetl/sdk/go/check"
)

// fakeS3 is one S3 bucket in memory, of the requests the destination makes:
// PUT, conditional on If-None-Match and If-Match, or a copy; HEAD; GET;
// DELETE; and ListObjectsV2.
type fakeS3 struct {
	srv *httptest.Server
	mu  sync.Mutex
	// By key, the bucket's name not in it.
	objects map[string]fakeObject
	// Called before each request is served, outside mu, with its operation
	// (put, copy, head, get, delete or list) and key. A status from it is
	// the answer, as S3's error; 0 serves the request. Nil: none.
	hook func(ctx context.Context, op, key string) int
}

type fakeObject struct {
	data        []byte
	etag        string
	contentType string
	// x-amz-meta-* headers.
	meta     http.Header
	modified time.Time
}

func newFakeS3(t testing.TB) *fakeS3 {
	f := &fakeS3{objects: map[string]fakeObject{}}
	f.srv = httptest.NewServer(f)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeS3) setHook(h func(ctx context.Context, op, key string) int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hook = h
}

func (f *fakeS3) put(key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = fakeObject{data: data, etag: etagOf(data), meta: http.Header{}, modified: time.Now()}
}

func (f *fakeS3) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

func etagOf(data []byte) string {
	sum := md5.Sum(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// errorCodes are S3's codes of the statuses fakeS3 answers with.
var errorCodes = map[int]string{
	http.StatusForbidden: "AccessDenied", http.StatusNotFound: "NoSuchKey",
	http.StatusPreconditionFailed: "PreconditionFailed", http.StatusInternalServerError: "InternalError",
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	op := map[string]string{http.MethodPut: "put", http.MethodHead: "head", http.MethodGet: "get", http.MethodDelete: "delete"}[r.Method]
	switch {
	case r.Method == http.MethodGet && key == "":
		op = "list"
	case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
		op = "copy"
	}
	f.mu.Lock()
	hook := f.hook
	f.mu.Unlock()
	if hook != nil {
		if status := hook(r.Context(), op, key); status != 0 {
			f.fail(w, r, status)
			return
		}
	}
	switch op {
	case "list":
		f.list(w, r)
	case "put", "copy":
		f.write(w, r, op, key, body)
	case "head", "get":
		f.mu.Lock()
		o, ok := f.objects[key]
		f.mu.Unlock()
		if !ok {
			f.fail(w, r, http.StatusNotFound)
			return
		}
		h := w.Header()
		for k, v := range o.meta {
			h[k] = v
		}
		h.Set("ETag", o.etag)
		h.Set("Content-Type", o.contentType)
		h.Set("Content-Length", strconv.Itoa(len(o.data)))
		h.Set("Last-Modified", o.modified.UTC().Format(http.TimeFormat))
		if op == "get" {
			_, _ = w.Write(o.data)
		}
	case "delete":
		f.mu.Lock()
		delete(f.objects, key)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		f.fail(w, r, http.StatusForbidden)
	}
}

func (f *fakeS3) fail(w http.ResponseWriter, r *http.Request, status int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>fake</Message></Error>`, errorCodes[status])
	}
}

func (f *fakeS3) write(w http.ResponseWriter, r *http.Request, op, key string, body []byte) {
	o, status := f.store(r, op, key, body)
	switch {
	case status != 0:
		f.fail(w, r, status)
	case op == "copy":
		_, _ = fmt.Fprintf(w, `<CopyObjectResult><ETag>%s</ETag><LastModified>%s</LastModified></CopyObjectResult>`,
			xmlText(o.etag), o.modified.UTC().Format(time.RFC3339))
	default:
		w.Header().Set("ETag", o.etag)
	}
}

// store writes a PUT's object, or a copy's, unless its condition fails or
// its source is missing, whose status it returns.
func (f *fakeS3) store(r *http.Request, op, key string, body []byte) (fakeObject, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur, exists := f.objects[key]
	if r.Header.Get("If-None-Match") == "*" && exists ||
		r.Header.Get("If-Match") != "" && (!exists || strings.Trim(r.Header.Get("If-Match"), `"`) != strings.Trim(cur.etag, `"`)) {
		return fakeObject{}, http.StatusPreconditionFailed
	}
	o := fakeObject{data: body, etag: etagOf(body), contentType: r.Header.Get("Content-Type"), meta: http.Header{}}
	for k, v := range r.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			o.meta[k] = v
		}
	}
	if op == "copy" {
		src, _ := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
		_, src, _ = strings.Cut(strings.TrimPrefix(src, "/"), "/")
		from, ok := f.objects[src]
		if !ok {
			return fakeObject{}, http.StatusNotFound
		}
		o = from
	}
	o.modified = time.Now()
	f.objects[key] = o
	return o, 0
}

func xmlText(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// list answers a ListObjectsV2 in one page.
func (f *fakeS3) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	type content struct {
		Key          string
		LastModified string
		ETag         string
		Size         int
	}
	type common struct{ Prefix string }
	out := struct {
		XMLName        xml.Name `xml:"ListBucketResult"`
		Prefix         string
		KeyCount       int
		MaxKeys        int
		IsTruncated    bool
		Contents       []content
		CommonPrefixes []common
	}{Prefix: prefix, MaxKeys: 1000}
	f.mu.Lock()
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	seen := map[string]bool{}
	for _, k := range keys {
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				p := k[:len(prefix)+i+len(delim)]
				if !seen[p] {
					seen[p] = true
					out.CommonPrefixes = append(out.CommonPrefixes, common{p})
				}
				continue
			}
		}
		o := f.objects[k]
		out.Contents = append(out.Contents, content{Key: k, LastModified: o.modified.UTC().Format(time.RFC3339), ETag: o.etag, Size: len(o.data)})
	}
	f.mu.Unlock()
	out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(out)
}

// fakeDestination is a destination at s3://b/p on f, of the settings in
// extra besides.
func fakeDestination(t testing.TB, f *fakeS3, extra map[string]any) *Destination {
	t.Helper()
	cfg := map[string]any{"url": "s3://b/p", "endpoint": f.srv.URL, "region": "us-east-1", "accessKeyId": "a", "secretAccessKey": "b"}
	maps.Copy(cfg, extra)
	config, _ := json.Marshal(cfg)
	var c s3store.Config
	d, err := NewDestination(config, &c, func(string) (Store, error) { return s3store.New(c) })
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// The destination's tests on fakeS3, which the tests of commits that fail
// partway need.
func TestFakeS3(t *testing.T) {
	suite(t, func(t *testing.T, extra map[string]any) *Destination {
		d := fakeDestination(t, newFakeS3(t), extra)
		if err := check.Run(t.Context(), d.Checks()).Err(); err != nil {
			t.Fatal(err)
		}
		return d
	})
}

// fakeEnv is an env of a destination on a new fakeS3.
func fakeEnv(t *testing.T) (*env, *fakeS3) {
	t.Helper()
	f := newFakeS3(t)
	d := fakeDestination(t, f, nil)
	b, err := d.bucket(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return &env{t: t, d: d, b: b}, f
}
