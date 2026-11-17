package objstore

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/openetl-io/openetl-connectors/records"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	noFiles = check.Reason{
		Name: "no_files", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "No file under {url} matches {glob}.",
		Fix:     "Check the stream's glob, or put its files there: until then, its reads find nothing.",
	}
	wrongFormat = check.Reason{
		Name: "file_unreadable", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{file} isn't a {format} file the stream can read.",
		Fix:     "Check the stream's format and glob, and that the file's name ends as its compression does, such as .gz.",
	}
	unconditional = check.Reason{
		Name: "conditional_writes_ignored", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{url} wrote a file that a conditional write should have refused.",
		Fix:     "Use a store that honors If-None-Match and If-Match, or write the stream in append mode: a stale overwrite can put an older read's manifest back.",
	}
	typeChange = check.Reason{
		Name: "type_change_refused", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The column {column} of {table} is {from} in its files, and the stream would write {to}.",
		Fix:     "Once the table's readers take {to}, remove {record}, which records the table's columns, or let the pipeline change types.",
	}
)

// denied is err as a check's failure: a permission the store refused names
// the privilege action needs on where.
func (s *store) denied(err error, action, where string) error {
	err = s.classify(err)
	if f := check.Classify(err); f != nil && f.Code == protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		privilege := map[string]string{"list": "read and execute", "read": "read", "write": "write", "delete": "write"}[action]
		if s.st != nil {
			privilege = s.st.Privilege(action)
		}
		return check.PrivilegeMissing.Wrap(err, "principal", "The connector's identity", "privilege", privilege, "object", where)
	}
	return err
}

// hostPort is the host and port of an endpoint's URL, and whether a
// connection to it starts with TLS.
func hostPort(endpoint string) (addr string, tls bool) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return endpoint, false
	}
	port := u.Port()
	if port == "" {
		port = "443"
		if u.Scheme == "http" {
			port = "80"
		}
	}
	return net.JoinHostPort(u.Hostname(), port), u.Scheme != "http"
}

// list lists a file under the URL.
func (s *store) list(ctx context.Context) error {
	b, err := s.open(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = b.Close() }()
	_, _, err = b.ListPage(ctx, blob.FirstPageToken, 1, nil)
	return err
}

// itemNames are each name's item name: check.Name's, made unique where two
// names give one.
func itemNames(prefix string, names []string) []string {
	out := make([]string, len(names))
	used := map[string]bool{}
	for i, n := range names {
		base := check.Name(prefix, n)
		out[i] = base
		for j := 2; used[out[i]]; j++ {
			out[i] = fmt.Sprintf("%s-%d", base, j)
		}
		used[out[i]] = true
	}
	return out
}

// storeChecks are both roles' checks of the store: its network path, that it
// takes the credentials, that the bucket or container exists, and that the
// identity lists the URL.
func (s *store) storeChecks() []check.Check {
	var checks []check.Check
	if s.st == nil {
		return []check.Check{{
			Name: "store.list", Kind: check.Permission, Title: "List the files under {url}", Field: "url", Params: check.Params{"url": s.url},
			Run: func(ctx context.Context, _ *check.Probe) error {
				return s.denied(s.list(ctx), "list", s.prefix)
			},
		}}
	}
	endpoint, setting := s.st.Endpoint()
	if addr, tls := hostPort(endpoint); addr != "" {
		if tls {
			checks = netcheck.ReachTLS(addr, nil)
		} else {
			checks = netcheck.Reach(addr)
		}
		for i := range checks {
			checks[i].Field = cmp.Or(setting, "url")
			// A runner may reach the cloud's own address only through a
			// proxy, which the SDKs take and a dial doesn't.
			checks[i].Advisory = setting == ""
		}
	}
	params := check.Params{"url": s.url}
	// One listing serves the checks that follow it.
	var listed error
	var needs []string
	if len(checks) > 0 {
		needs = []string{checks[len(checks)-1].Name}
	}
	what := s.st.Container()
	bucket := check.Params{"url": s.url, "bucket": s.bucket, "what": what}
	return append(checks,
		check.Check{
			Name: "store.auth", Kind: check.Authentication, Title: "Sign in to the store of {url}", Field: s.st.Credential(), Params: params, Needs: needs,
			Run: func(ctx context.Context, _ *check.Probe) error {
				listed = s.list(ctx)
				err := s.classify(listed)
				switch f := check.Classify(err); {
				case err == nil, f.Code == protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, gcerrors.Code(listed) == gcerrors.NotFound:
					// The store knows the identity, whatever it may do.
					return nil
				}
				return err
			},
		},
		check.Check{
			Name: "store.bucket", Kind: check.Resource, Title: "Find the {what} {bucket}", Field: "url", Params: bucket, Needs: []string{"store.auth"},
			Run: func(context.Context, *check.Probe) error {
				if gcerrors.Code(listed) == gcerrors.NotFound {
					return check.NotFound.Wrap(listed, "resource", "The "+what+" "+s.bucket)
				}
				return nil
			},
		},
		check.Check{
			Name: "store.list", Kind: check.Permission, Title: "List the files under {url}", Field: "url", Params: params, Needs: []string{"store.bucket"},
			Run: func(context.Context, *check.Probe) error {
				return s.denied(listed, "list", s.bucket)
			},
		},
	)
}

// Checks are the store's: its network path, the credentials, the bucket or
// container, and listing the URL.
func (s *Source) Checks() []check.Check { return s.store.storeChecks() }

// Preflight, for each stream the pipeline reads, reads a file its glob
// matches, warning of a stream with none, and reads the file as the stream's
// format.
func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	var checks []check.Check
	names := make([]string, len(req.Streams))
	for i, cs := range req.Streams {
		names[i] = cs.Name
	}
	items := itemNames("stream", names)
	for k, cs := range req.Streams {
		spec, i := s.streams[cs.Name], -1
		for j, sc := range s.cfg.Streams {
			if sc.Name == cs.Name {
				i = j
			}
		}
		if spec == nil {
			continue
		}
		var found *object
		read := items[k] + ".read"
		params := check.Params{"stream": cs.Name, "url": s.cfg.URL, "glob": spec.cfg.Glob, "format": spec.cfg.Format}
		checks = append(checks,
			check.Check{
				Name: read, Kind: check.Permission, Title: "Read a file of {stream}",
				Field: fmt.Sprintf("streams.%d.glob", i), Params: params,
				Run: func(ctx context.Context, p *check.Probe) error {
					b, err := s.store.open(ctx)
					if err != nil {
						return s.store.classify(err)
					}
					defer func() { _ = b.Close() }()
					it := b.List(&blob.ListOptions{Prefix: spec.prefix})
					for {
						obj, err := it.Next(ctx)
						if errors.Is(err, io.EOF) {
							return check.Warn(noFiles.Fail())
						}
						if err != nil {
							return s.store.denied(fmt.Errorf("objstore: list %s: %w", spec.prefix, err), "list", s.cfg.URL)
						}
						if obj.IsDir || !spec.glob.MatchString(obj.Key) {
							continue
						}
						p.Param("file", s.store.location(obj.Key))
						r, err := b.NewRangeReader(ctx, obj.Key, 0, 1, nil)
						if err != nil {
							return s.store.denied(fmt.Errorf("objstore: read %s: %w", obj.Key, err), "read", s.store.location(obj.Key))
						}
						_ = r.Close()
						found = &object{Key: obj.Key, Modified: obj.ModTime, Size: obj.Size}
						p.Found("Read {file}.")
						return nil
					}
				},
			},
			check.Check{
				Name: items[k] + ".format", Kind: check.Compatibility, Title: "Read a file of {stream} as {format}",
				Field: fmt.Sprintf("streams.%d.format", i), Params: params, Needs: []string{read},
				Run: func(ctx context.Context, p *check.Probe) error {
					if found == nil {
						return check.Skip("{stream} has no file to read")
					}
					p.Param("file", s.store.location(found.Key))
					b, err := s.store.open(ctx)
					if err != nil {
						return s.store.classify(err)
					}
					defer func() { _ = b.Close() }()
					seen := func(string) *inferred { return &inferred{} }
					switch spec.cfg.Format {
					case formatParquet:
						_, err = parquetFields(ctx, b, *found)
					case formatAvro:
						_, err = avroFields(ctx, b, *found)
					case formatCSV:
						err = spec.sampleCSV(ctx, b, *found, seen)
					case formatJSONL:
						err = spec.sampleJSONL(ctx, b, *found, seen)
					}
					if err != nil {
						if c := check.Classify(s.store.classify(err)); c.Reason != check.Unknown.Name {
							return c
						}
						return wrongFormat.Wrap(err)
					}
					p.Found("{file} reads as {format}.")
					return nil
				},
			},
		)
	}
	return checks
}

// Checks are the store's, then writing a file under the URL and deleting
// it.
func (d *Destination) Checks() []check.Check {
	key := reserved + "/check-" + uuid.NewString()
	params := check.Params{"url": d.cfg.URL}
	return append(d.store.storeChecks(),
		check.Check{
			Name: "store.write", Kind: check.Permission, Title: "Write a file under {url}", Field: "url", Params: params, Needs: []string{"store.list"},
			Run: func(ctx context.Context, p *check.Probe) error {
				b, err := d.bucket(ctx)
				if err != nil {
					return d.store.classify(err)
				}
				defer func() { _ = b.Close() }()
				if err := b.WriteAll(ctx, key, nil, nil); err != nil {
					return d.store.denied(fmt.Errorf("objstore: write %s: %w", key, err), "write", d.cfg.URL)
				}
				return nil
			},
		},
		check.Check{
			Name: "store.delete", Kind: check.Permission, Title: "Delete a file under {url}", Field: "url", Params: params, Needs: []string{"store.write"},
			Run: func(ctx context.Context, p *check.Probe) error {
				b, err := d.bucket(ctx)
				if err != nil {
					return d.store.classify(err)
				}
				defer func() { _ = b.Close() }()
				if err := b.Delete(ctx, key); err != nil {
					return d.store.denied(fmt.Errorf("objstore: delete %s: %w", key, err), "delete", d.cfg.URL)
				}
				return nil
			},
		},
	)
}

// Preflight checks each stream's write mode, and that its columns keep the
// types its table's files have, unless the stream may change them; and,
// for a pipeline that overwrites, that the store refuses a conditional write
// it should, which overwrite relies on.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	var checks []check.Check
	overwrites := false
	ns := strings.Trim(req.Namespace, "/")
	names := make([]string, len(req.Streams))
	for i, st := range req.Streams {
		names[i] = st.Name
	}
	items := itemNames("stream", names)
	for k, st := range req.Streams {
		overwrites = overwrites || st.Mode == destination.Overwrite
		table := st.Name
		if sc := req.Schemas[st.Name]; sc != nil {
			if t, ok := sc.Metadata().GetValue("openetl.table"); ok {
				table = t
			}
		}
		if ns != "" {
			table = ns + "/" + table
		}
		params := check.Params{"stream": st.Name, "table": table}
		mode := items[k] + ".mode"
		checks = append(checks,
			check.Check{
				Name: mode, Kind: check.Compatibility, Title: "Write {stream} in its write mode", Params: params,
				Run: func(context.Context, *check.Probe) error {
					if st.Mode != destination.Append && st.Mode != destination.Overwrite {
						return check.SettingMismatch.Fail("setting", "The stream's write mode", "found", modeName(st.Mode), "required", "append or overwrite")
					}
					return nil
				},
			},
			check.Check{
				Name: items[k] + ".types", Kind: check.Compatibility, Title: "Keep the types of {table}'s columns", Params: params, Needs: []string{mode},
				Run: func(ctx context.Context, p *check.Probe) error {
					sc := req.Schemas[st.Name]
					if sc == nil || st.AllowTypeChanges {
						return nil
					}
					b, err := d.bucket(ctx)
					if err != nil {
						return d.store.classify(err)
					}
					defer func() { _ = b.Close() }()
					key := schemaKey(table)
					data, err := b.ReadAll(ctx, key)
					switch {
					case gcerrors.Code(err) == gcerrors.NotFound:
						return nil
					case err != nil:
						return d.store.denied(fmt.Errorf("objstore: read %s: %w", key, err), "read", d.store.location(key))
					}
					var rec schemaRecord
					if err := json.Unmarshal(data, &rec); err != nil {
						return fmt.Errorf("objstore: %s: %w", key, err)
					}
					have := map[string]string{}
					for _, c := range rec.Columns {
						have[c.Name] = c.Type
					}
					for _, f := range sc.Fields() {
						to := typeName(records.WithoutIntervals(f.Type))
						if from, ok := have[f.Name]; ok && from != to {
							return typeChange.Fail("column", f.Name, "from", from, "to", to, "record", d.store.location(key))
						}
					}
					return nil
				},
			},
		)
	}
	if !overwrites {
		return checks
	}
	return append(checks, check.Check{
		Name: "store.conditional-write", Kind: check.Compatibility, Title: "Write a file under {url} only if it's absent", Field: "url",
		Params: check.Params{"url": d.cfg.URL}, Advisory: true,
		Run: func(ctx context.Context, p *check.Probe) error {
			b, err := d.bucket(ctx)
			if err != nil {
				return d.store.classify(err)
			}
			defer func() { _ = b.Close() }()
			key := reserved + "/check-" + uuid.NewString()
			defer func() {
				_ = b.Delete(context.WithoutCancel(ctx), key)
				// A file:// URL's conditional write locks a file beside it.
				_ = b.Delete(context.WithoutCancel(ctx), key+".lock")
			}()
			now := func() (*version, error) {
				a, err := b.Attributes(ctx, key)
				if gcerrors.Code(err) == gcerrors.NotFound {
					return nil, nil
				}
				if err != nil {
					return nil, err
				}
				return d.store.version(a), nil
			}
			for i := range 2 {
				err := d.store.swap(ctx, b, key, []byte{byte(i)}, nil, nil, now)
				switch {
				case i == 1 && errors.Is(err, errConflict):
					return nil
				case i == 1 && err == nil:
					return unconditional.Fail()
				case err != nil:
					return d.store.denied(fmt.Errorf("objstore: write %s: %w", key, err), "write", d.cfg.URL)
				}
			}
			return nil
		},
	})
}

func modeName(m destination.WriteMode) string {
	switch m {
	case destination.Append:
		return "append"
	case destination.Overwrite:
		return "overwrite"
	case destination.Merge:
		return "merge"
	}
	return fmt.Sprint(int(m))
}
