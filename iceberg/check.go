package iceberg

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

var (
	_ destination.CheckLister = (*Destination)(nil)
	_ destination.Preflighter = (*Destination)(nil)
	_ destination.Checker     = (*Destination)(nil)
)

var (
	typeChange = check.Reason{
		Name: "type_change_refused", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The column {column} of {table} is {from}, and the stream would write {to}: {why}.",
		Fix:     "{fix}",
	}
	cantMerge = check.Reason{
		Name: "merge_unsupported", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{stream} merges into {table}, which {why}.",
		Fix:     "{fix}",
	}
	noKey = check.Reason{
		Name: "primary_key_missing", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{stream} merges, and {why}.",
		Fix:     "Give the stream a primary key of columns that aren't floating-point, or write it with append or overwrite.",
	}
)

// session is one list's catalog, opened by the first check that needs it
// and shared by the rest, and closed once the list's other cleanups, which
// may use it, have run.
type session struct {
	open func(context.Context) (catalog.Catalog, error)
	once sync.Once
	cat  catalog.Catalog
	err  error
}

// catalog is the session's catalog; p is the probe of the check that needs
// it, which closes it if it's the first.
func (s *session) catalog(ctx context.Context, p *check.Probe) (catalog.Catalog, error) {
	s.once.Do(func() {
		s.cat, s.err = s.open(ctx)
		if c, ok := s.cat.(catalog.Closer); ok && s.err == nil {
			p.Cleanup(func(context.Context) { _ = c.Close() })
		}
	})
	return s.cat, s.err
}

// endpoint is the host and port the catalog answers at, whether it starts
// with TLS, the config field that names it, and whether the client dials
// it, as the checks do: the config names it, rather than AWS's own, which
// may be reachable only through a proxy, and no proxy applies to it; empty
// if it can't tell.
func (c *conn) endpoint() (addr string, tls bool, field string, direct bool) {
	raw, field, named := c.cfg.URI, "uri", true
	if c.cfg.Catalog == "glue" {
		raw, field = c.glueEndpoint(), "properties"
		if raw == "" {
			raw, field, named = "https://glue."+cmp.Or(c.cfg.Region, "us-east-1")+".amazonaws.com", "region", false
		}
	}
	return hostPort(raw, field, named)
}

// storeEndpoint is s3Endpoint's host and port, and whether it starts with
// TLS and the client dials it, as endpoint gives them; empty without one.
func (c *conn) storeEndpoint() (addr string, tls, direct bool) {
	if c.cfg.S3Endpoint == "" {
		return "", false, false
	}
	addr, tls, _, direct = hostPort(c.cfg.S3Endpoint, "s3Endpoint", true)
	return addr, tls, direct
}

// proxyFor is the proxy an HTTP request takes, as the catalog's and the
// store's clients find it; nil for none.
var proxyFor = http.ProxyFromEnvironment

func hostPort(raw, field string, named bool) (string, bool, string, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", false, field, named
	}
	if proxy, err := proxyFor(&http.Request{URL: u}); err != nil || proxy != nil {
		named = false
	}
	port := u.Port()
	if port == "" {
		port = "443"
		if u.Scheme == "http" {
			port = "80"
		}
	}
	return net.JoinHostPort(u.Hostname(), port), u.Scheme != "http", field, named
}

// credentialField is the config's credential that's set; empty for none.
func (c *conn) credentialField() string {
	switch {
	case c.cfg.Catalog == "glue" && !c.cfg.AccessKeyID.IsZero():
		return "accessKeyId"
	case c.cfg.Catalog == "glue":
		return ""
	case !c.cfg.Credential.IsZero():
		return "credential"
	case !c.cfg.Token.IsZero():
		return "token"
	}
	return ""
}

// connectionChecks reach the catalog and sign in to it: net.dns,
// net.connect, net.tls for an HTTPS endpoint, and catalog.auth.
func (c *conn) connectionChecks(s *session) []check.Check {
	var checks []check.Check
	addr, tls, field, direct := c.endpoint()
	if addr != "" {
		if tls {
			checks = netcheck.ReachTLS(addr, nil)
		} else {
			checks = netcheck.Reach(addr)
		}
		for i := range checks {
			checks[i].Field = field
			// A proxy, which the client takes and a dial doesn't, may be
			// the only way there.
			checks[i].Advisory = !direct
		}
	}
	var needs []string
	if len(checks) > 0 {
		needs = []string{checks[len(checks)-1].Name}
	}
	what := "the REST catalog at {uri}"
	if c.cfg.Catalog == "glue" {
		what = "Glue in {region}"
	}
	return append(checks, check.Check{
		Name: "catalog.auth", Kind: check.Authentication, Title: "Sign in to " + what, Field: c.credentialField(),
		Params: check.Params{"uri": c.cfg.URI, "region": cmp.Or(c.cfg.Region, "the default region")}, Needs: needs,
		Run: func(ctx context.Context, p *check.Probe) error {
			cat, err := s.catalog(ctx, p)
			if err != nil {
				return err
			}
			// Listing signs a request: a REST catalog may hand out its
			// config to anyone, and Glue checks a signature only when it's
			// called.
			_, err = cat.ListNamespaces(ctx, nil)
			if err = classify(err); err != nil {
				if f := check.Classify(err); f != nil && f.Code == protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
					// The catalog knows the identity, whatever it may do.
					p.Found("The catalog knows the identity, which can't list namespaces.")
					return nil
				}
				return err
			}
			return nil
		},
	})
}

// namespaceCheck: ns exists, or the first write creates it.
func namespaceCheck(s *session, ns []string) check.Check {
	return check.Check{
		Name: "catalog.namespace", Kind: check.Resource, Title: "Find the namespace {namespace}", Field: "namespace",
		Params: check.Params{"namespace": strings.Join(ns, ".")}, Needs: []string{"catalog.auth"},
		Run: func(ctx context.Context, p *check.Probe) error {
			cat, err := s.catalog(ctx, p)
			if err != nil {
				return err
			}
			ok, err := cat.CheckNamespaceExists(ctx, ns)
			if err != nil {
				return classify(err)
			}
			if !ok {
				p.Found("{namespace} doesn't exist, and the first write creates it.")
				return nil
			}
			p.Found("{namespace} exists.")
			return nil
		},
	}
}

// Checks reach the catalog and sign in, find the namespace, then create a
// table there, commit to it, and write and delete a file in its location,
// as writes do, and drop it.
func (d *Destination) Checks() []check.Check { return d.checks(&session{open: d.conn.open}) }

func (d *Destination) checks(s *session) []check.Check {
	ns := d.namespace("")
	checks := append(d.connectionChecks(s), namespaceCheck(s, ns))

	var probe *table.Table
	var probeFile string
	ident := append(slices.Clone(ns), "_openetl_check_"+strings.ToLower(rand.Text())[:12])
	params := check.Params{"namespace": strings.Join(ns, "."), "table": strings.Join(ident, ".")}
	createField := "namespace"
	if d.cfg.Catalog == "glue" {
		createField = "warehouse"
	}
	storeField := "warehouse"
	if d.cfg.S3Endpoint != "" {
		storeField = "s3Endpoint"
	}
	checks = append(checks, check.Check{
		Name: "catalog.create", Kind: check.Permission, Title: "Create a table in {namespace}", Field: createField,
		Description: "The destination creates each stream's table, and its namespace if it's missing.", Params: params,
		Needs: []string{"catalog.namespace"},
		Run: func(ctx context.Context, p *check.Probe) error {
			cat, err := s.catalog(ctx, p)
			if err != nil {
				return err
			}
			created := false
			if ok, err := cat.CheckNamespaceExists(ctx, ns); err == nil && !ok {
				if err := cat.CreateNamespace(ctx, ns, nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
					return classify(fmt.Errorf("iceberg: create namespace %s: %w", strings.Join(ns, "."), err))
				}
				created = true
			}
			var opts []catalog.CreateTableOpt
			if d.cfg.Catalog == "glue" && d.cfg.Warehouse != "" {
				opts = append(opts, catalog.WithLocation(strings.TrimSuffix(d.cfg.Warehouse, "/")+"/"+strings.Join(ident, "/")))
			}
			sc := ice.NewSchema(0, ice.NestedField{ID: 1, Name: "id", Type: ice.PrimitiveTypes.Int64})
			createMu.Lock()
			probe, err = cat.CreateTable(ctx, ident, sc, opts...)
			createMu.Unlock()
			// Glue may create the table and then fail to load it.
			mayExist := !errors.Is(err, catalog.ErrTableAlreadyExists)
			p.Cleanup(func(ctx context.Context) {
				if probe != nil && probeFile != "" {
					if fs, err := probe.FS(ctx); err == nil {
						_ = fs.Remove(probeFile)
					}
				}
				if mayExist {
					// Purging takes its metadata files too.
					if pc, ok := cat.(interface {
						PurgeTable(context.Context, table.Identifier) error
					}); !ok || pc.PurgeTable(ctx, ident) != nil {
						_ = cat.DropTable(ctx, ident)
					}
				}
				if created {
					_ = cat.DropNamespace(ctx, ns)
				}
			})
			if err != nil {
				if f := check.Classify(classify(err)); f != nil && f.Reason != check.Unknown.Name {
					return f
				}
				return check.ConfigInvalid.Wrap(err)
			}
			p.Found("Created {table}, and drops it again.")
			return nil
		},
	}, check.Check{
		Name: "catalog.commit", Kind: check.Permission, Title: "Commit to a table in {namespace}", Params: params,
		Description: "Each epoch is a commit to its tables.", Needs: []string{"catalog.create"},
		Run: func(ctx context.Context, p *check.Probe) error {
			tx := probe.NewTransaction()
			if err := tx.SetProperties(ice.Properties{"openetl.check": "1"}); err != nil {
				return err
			}
			_, err := tx.Commit(ctx)
			return classify(err)
		},
	})
	if addr, tls, direct := d.storeEndpoint(); addr != "" {
		var reach []check.Check
		if tls {
			reach = netcheck.ReachTLS(addr, nil)
		} else {
			reach = netcheck.Reach(addr)
		}
		// The catalog's are net.*.
		for i := range reach {
			reach[i].Name = strings.Replace(reach[i].Name, "net.", "store.", 1)
			for j, n := range reach[i].Needs {
				reach[i].Needs[j] = strings.Replace(n, "net.", "store.", 1)
			}
			reach[i].Field = "s3Endpoint"
			reach[i].Advisory = !direct
		}
		checks = append(checks, reach...)
	}
	return append(checks, check.Check{
		Name: "store.write", Kind: check.Permission, Title: "Write a file in {table}'s location", Field: storeField, Params: params,
		Description: "The destination writes each epoch's data files itself, under its tables' locations.", Needs: []string{"catalog.create"},
		Run: func(ctx context.Context, p *check.Probe) error {
			fs, err := probe.FS(ctx)
			if err != nil {
				return check.ConfigInvalid.Wrap(err)
			}
			wfs, ok := fs.(iceio.WriteFileIO)
			if !ok {
				return check.ConfigInvalid.Wrap(fmt.Errorf("the table's store writes no files: %T", fs))
			}
			name := strings.TrimSuffix(probe.Location(), "/") + "/data/_openetl_check.parquet"
			p.Param("file", name)
			if err := wfs.WriteFile(name, nil); err != nil {
				return storeFailure(err, "s3:PutObject", name)
			}
			probeFile = name
			p.Found("Wrote {file}.")
			return nil
		},
	}, check.Check{
		Name: "store.delete", Kind: check.Permission, Title: "Delete a file in {table}'s location", Field: storeField, Params: params,
		Description: "The destination deletes the files of epochs that don't commit.", Needs: []string{"store.write"},
		Run: func(ctx context.Context, p *check.Probe) error {
			fs, err := probe.FS(ctx)
			if err != nil {
				return check.ConfigInvalid.Wrap(err)
			}
			if err := fs.Remove(probeFile); err != nil {
				return storeFailure(err, "s3:DeleteObject", probeFile)
			}
			probeFile = ""
			return nil
		},
	})
}

// storeFailure is a store's error as a check's failure: a permission it
// refused names the privilege.
func storeFailure(err error, privilege, object string) error {
	err = classify(err)
	if f := check.Classify(err); f != nil && f.Code == protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		return check.PrivilegeMissing.Wrap(err, "principal", "The destination's identity", "privilege", privilege, "object", object)
	}
	return err
}

// Check is Checks run, its first failure as an error.
func (d *Destination) Check(ctx context.Context) error { return check.Run(ctx, d.Checks()).Err() }

// checkNames gives each check a name from parts, made distinct from those it
// gave before: streams whose names differ only in case or punctuation get
// the same check.Name.
type checkNames map[string]int

func (n checkNames) name(parts ...string) string {
	name := check.Name(parts...)
	n[name]++
	if c := n[name]; c > 1 {
		name += "-" + strconv.Itoa(c)
	}
	return name
}

// Preflight signs in, finds the pipeline's namespace, and looks at each
// stream: its write mode and key, and the table it writes, if it exists:
// that it takes the stream's columns without a type change the stream
// doesn't allow, and that a merge can delete its rows.
func (d *Destination) Preflight(req destination.PreflightRequest) []check.Check {
	s := &session{open: d.conn.open}
	ns := d.namespace(req.Namespace)
	checks := append(d.connectionChecks(s), namespaceCheck(s, ns))
	n := checkNames{}
	for _, st := range req.Streams {
		sc := req.Schemas[st.Name]
		name := st.Name
		if sc != nil {
			name = targetTable(sc, st.Name)
		}
		ident := append(slices.Clone(ns), name)
		params := check.Params{"stream": st.Name, "table": strings.Join(ident, ".")}
		mode := n.name("stream", st.Name, "mode")
		checks = append(checks, check.Check{
			Name: mode, Kind: check.Compatibility, Title: "Write {stream} in its write mode", Params: params,
			Run: func(context.Context, *check.Probe) error {
				if !DestinationCapabilities.SupportsMode(st.Mode) {
					return check.SettingMismatch.Fail("setting", "The stream's write mode", "found", strconv.Itoa(int(st.Mode)), "required", "append, overwrite or merge")
				}
				if st.Mode != destination.Merge {
					return nil
				}
				if len(st.PrimaryKey) == 0 {
					return noKey.Fail("why", "has no primary key")
				}
				if sc != nil {
					if _, err := keyColumns(sc, st.PrimaryKey); err != nil {
						return noKey.Fail("why", err.Error())
					}
				}
				return nil
			},
		}, check.Check{
			Name: n.name("stream", st.Name, "table"), Kind: check.Compatibility, Title: "Write {stream} to {table}", Params: params,
			Needs: []string{"catalog.namespace", mode},
			Run: func(ctx context.Context, p *check.Probe) error {
				cat, err := s.catalog(ctx, p)
				if err != nil {
					return err
				}
				tbl, err := cat.LoadTable(ctx, ident)
				switch {
				case errors.Is(err, catalog.ErrNoSuchTable), errors.Is(err, catalog.ErrNoSuchNamespace):
					p.Found("{table} doesn't exist, and the first write creates it.")
					return nil
				case err != nil:
					return classify(err)
				}
				p.Param("version", tbl.Metadata().Version())
				if sc != nil {
					if err := tableTakes(ctx, tbl, ident, sc, st); err != nil {
						return err
					}
				}
				p.Found("{table} exists, format {version}, and takes the stream's columns.")
				return nil
			},
		})
	}
	return checks
}

// tableTakes fails a table that can't take the stream's columns: a column
// whose type would change, unless the stream allows it and Iceberg can make
// it, or a merge the table can't take.
func tableTakes(ctx context.Context, tbl *table.Table, ident table.Identifier, sc *arrow.Schema, st destination.Stream) error {
	want, err := icebergSchema(sc)
	if err != nil {
		return check.ConfigInvalid.Wrap(err)
	}
	refused, _, _ := compareSchemas(ident, tbl.Schema(), want, st.AllowTypeChanges)
	if len(refused) > 0 {
		r := refused[0]
		fix := "Iceberg can't make this change: write the stream to another table, or drop the column from it."
		if r.Fix != "" {
			fix = "Run " + r.Fix + " in an engine that changes Iceberg tables, or let the pipeline change types."
		}
		return typeChange.Fail("column", r.Column, "from", r.FromType, "to", r.ToType, "why", r.Reason, "fix", fix)
	}
	if st.Mode != destination.Merge {
		return nil
	}
	if tbl.Metadata().Version() < 2 {
		return cantMerge.Fail("why", "is format 1, which has no deletes", "fix", "Upgrade the table to format 2, as ALTER TABLE ... SET TBLPROPERTIES ('format-version' = '2') does, or write the stream with append.")
	}
	key, err := keyColumns(sc, st.PrimaryKey)
	if err != nil {
		return noKey.Fail("why", err.Error())
	}
	for _, c := range partitionColumns(tbl) {
		if !slices.Contains(key, c) {
			return cantMerge.Fail("why", "is partitioned by "+c+", a column outside the stream's key", "fix",
				"Write the stream to a table partitioned by its key's columns or not at all, or write it with append.")
		}
	}
	if err := olderSpecs(ctx, tbl); errors.Is(err, errOlderSpec) {
		return cantMerge.Fail("why", "holds files of an earlier partitioning, which its deletes can't reach", "fix",
			"Rewrite the table's files into its current partitioning, as Spark's rewrite_data_files procedure does.")
	} else if err != nil {
		return classify(err)
	}
	return nil
}
