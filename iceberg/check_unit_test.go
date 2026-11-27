package iceberg

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// The lists are valid for any config and request, streams whose names make
// the same check name included, and hold what the config calls for.
func TestCheckLists(t *testing.T) {
	// Whatever proxy the environment names.
	defer func(f func(*http.Request) (*url.URL, error)) { proxyFor = f }(proxyFor)
	proxyFor = func(*http.Request) (*url.URL, error) { return nil, nil }
	for config, want := range map[string][]string{
		`{"uri": "https://catalog.example.com/api", "token": "t"}`: {"net.dns", "net.connect", "net.tls", "catalog.auth", "catalog.namespace", "catalog.create", "catalog.commit", "store.write", "store.delete", "catalog.drop"},
		`{"uri": "http://catalog:8181", "s3Endpoint": "http://s3:8333"}`: {"net.dns", "net.connect", "catalog.auth", "catalog.namespace", "catalog.create", "catalog.commit",
			"store.dns", "store.connect", "store.write", "store.delete", "catalog.drop"},
		`{"catalog": "glue", "region": "eu-west-1", "warehouse": "s3://b/w"}`: {"net.dns", "net.connect", "net.tls", "catalog.auth", "catalog.namespace", "catalog.create", "catalog.commit", "store.write", "store.delete", "catalog.drop"},
	} {
		d, err := NewDestination([]byte(config))
		if err != nil {
			t.Fatal(err)
		}
		checks := d.Checks()
		if err := check.Validate(checks); err != nil {
			t.Errorf("%s: %v", config, err)
		}
		if got := connectortest.ItemNames(checks); !slices.Equal(got, want) {
			t.Errorf("%s: %v, want %v", config, got, want)
		}
		// AWS's own endpoint may need a proxy: reaching it only advises.
		if d.cfg.Catalog == "glue" && !checks[0].Advisory {
			t.Errorf("%s: %s isn't advisory", config, checks[0].Name)
		}
	}

	d, err := NewDestination([]byte(`{"catalog": "glue", "properties": {"glue.endpoint": "http://glue:5000"}}`))
	if err != nil {
		t.Fatal(err)
	}
	pf := d.Preflight(destination.PreflightRequest{Namespace: "n", Streams: []destination.Stream{{Name: "a b"}, {Name: "a.b", Mode: destination.Merge}}})
	if err := check.Validate(pf); err != nil {
		t.Error(err)
	}
	want := []string{"net.dns", "net.connect", "catalog.auth", "catalog.namespace", "stream.a-b.mode", "stream.a-b.table", "stream.a-b.mode-2", "stream.a-b.table-2"}
	if got := connectortest.ItemNames(pf); !slices.Equal(got, want) {
		t.Errorf("preflight %v, want %v", got, want)
	}
	if pf[0].Advisory {
		t.Error("a named Glue endpoint's reach is advisory")
	}
}

// Reaching the catalog or the store only advises where a proxy applies, as
// the clients take it and a dial doesn't.
func TestCheckThroughAProxy(t *testing.T) {
	defer func(f func(*http.Request) (*url.URL, error)) { proxyFor = f }(proxyFor)
	proxyFor = func(r *http.Request) (*url.URL, error) {
		if r.URL.Hostname() == "catalog" {
			return url.Parse("http://proxy:3128")
		}
		return nil, nil
	}
	d, err := NewDestination([]byte(`{"uri": "http://catalog:8181", "s3Endpoint": "http://s3:8333"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"net.dns": true, "net.connect": true, "store.dns": false, "store.connect": false}
	for _, c := range d.Checks() {
		if w, ok := want[c.Name]; ok && c.Advisory != w {
			t.Errorf("%s: advisory %v", c.Name, c.Advisory)
		}
	}
}

type closingCatalog struct {
	catalog.Catalog
	closed *int
}

func (c closingCatalog) Close() error { *c.closed++; return nil }

// A list's catalog is closed once, after the cleanups of the checks that
// used it.
func TestSessionCloses(t *testing.T) {
	closed, opened := 0, 0
	s := &session{open: func(context.Context) (catalog.Catalog, error) { opened++; return closingCatalog{closed: &closed}, nil }}
	use := func(name string, needs ...string) check.Check {
		return check.Check{Name: name, Kind: check.Resource, Title: name, Needs: needs, Run: func(ctx context.Context, p *check.Probe) error {
			_, err := s.catalog(ctx, p)
			p.Cleanup(func(context.Context) {
				if closed > 0 {
					t.Errorf("%s's cleanup ran after the catalog closed", name)
				}
			})
			return err
		}}
	}
	check.Run(t.Context(), []check.Check{use("a"), use("b", "a")})
	if opened != 1 || closed != 1 {
		t.Errorf("opened %d, closed %d", opened, closed)
	}
}

// probeCatalog creates no table, failing with err, as Glue does when it
// creates one and can't load it, and counts the drops.
type probeCatalog struct {
	catalog.Catalog
	err     error
	dropped *int
}

func (c probeCatalog) ListNamespaces(context.Context, table.Identifier) ([]table.Identifier, error) {
	return nil, nil
}
func (c probeCatalog) CheckNamespaceExists(context.Context, table.Identifier) (bool, error) {
	return true, nil
}
func (c probeCatalog) CreateTable(context.Context, table.Identifier, *ice.Schema, ...catalog.CreateTableOpt) (*table.Table, error) {
	return nil, c.err
}
func (c probeCatalog) PurgeTable(context.Context, table.Identifier) error { *c.dropped++; return nil }

// A check table whose creation failed is dropped all the same, as it may
// exist, unless it existed before.
func TestCheckTableDropped(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	d, err := NewDestination([]byte(`{"uri": "` + srv.URL + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	for err, want := range map[error]int{errors.New("can't load it"): 1, catalog.ErrTableAlreadyExists: 0} {
		dropped := 0
		cat := probeCatalog{err: err, dropped: &dropped}
		r := check.Run(t.Context(), d.checks(&session{open: func(context.Context) (catalog.Catalog, error) { return cat, nil }}))
		if it := r.Items[slices.IndexFunc(r.Items, func(it check.Item) bool { return it.Name == "catalog.create" })]; it.Status != check.Failed {
			t.Errorf("%v: catalog.create %v", err, it.Status)
		}
		if dropped != want {
			t.Errorf("%v: dropped %d times, want %d", err, dropped, want)
		}
	}
}

// nsCatalog is probeCatalog in a namespace that doesn't exist, which it
// creates, and drops with dropErr.
type nsCatalog struct {
	probeCatalog
	dropErr error
	ns      *[]string
}

func (c nsCatalog) CheckNamespaceExists(context.Context, table.Identifier) (bool, error) {
	return false, nil
}
func (c nsCatalog) CreateNamespace(_ context.Context, ns table.Identifier, _ ice.Properties) error {
	*c.ns = append(*c.ns, "create "+ns[0])
	return nil
}
func (c nsCatalog) DropNamespace(_ context.Context, ns table.Identifier) error {
	*c.ns = append(*c.ns, "drop "+ns[0])
	return c.dropErr
}

// A namespace the checks created is dropped by catalog.drop, which warns,
// naming it, when the catalog refuses, so that it isn't left unsaid.
func TestCheckNamespaceDropped(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	d, err := NewDestination([]byte(`{"uri": "` + srv.URL + `", "namespace": "made"}`))
	if err != nil {
		t.Fatal(err)
	}
	denied := fmt.Errorf("failed to drop namespace made: %w", errors.New("AccessDeniedException: not authorized to perform: glue:DeleteDatabase"))
	for dropErr, want := range map[error]check.Status{nil: check.Passed, denied: check.Warning} {
		var ns []string
		dropped := 0
		cat := nsCatalog{probeCatalog: probeCatalog{err: errors.New("can't load it"), dropped: &dropped}, dropErr: dropErr, ns: &ns}
		r := check.Run(t.Context(), d.checks(&session{open: func(context.Context) (catalog.Catalog, error) { return cat, nil }}))
		i := slices.IndexFunc(r.Items, func(it check.Item) bool { return it.Name == "catalog.drop" })
		if i < 0 {
			t.Fatalf("%v: no catalog.drop item in %v", dropErr, connectortest.Statuses(r))
		}
		it := r.Items[i].Rendered()
		if it.Status != want || (want == check.Warning && !strings.Contains(it.Message, "made")) {
			t.Errorf("%v: catalog.drop %v: %s", dropErr, it.Status, it.Message)
		}
		if !slices.Equal(ns, []string{"create made", "drop made"}) || dropped != 1 {
			t.Errorf("%v: namespace %v, table dropped %d times", dropErr, ns, dropped)
		}
	}
}

// A merge's table check reports a manifest list it can't read as that, not
// as files of an earlier partitioning.
func TestTableTakesUnreadManifests(t *testing.T) {
	dir := t.TempDir()
	meta, err := table.ParseMetadataString(`{"format-version": 2, "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "location": "file://` + dir + `",
		"last-sequence-number": 1, "last-updated-ms": 1, "last-column-id": 1, "current-schema-id": 0,
		"schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}],
		"default-spec-id": 0, "last-partition-id": 1000, "partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "name": "id", "transform": "identity"}]}],
		"default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}],
		"current-snapshot-id": 1, "snapshots": [{"snapshot-id": 1, "sequence-number": 1, "timestamp-ms": 1, "manifest-list": "file://` + dir + `/missing.avro", "summary": {"operation": "append"}, "schema-id": 0}]}`)
	if err != nil {
		t.Fatal(err)
	}
	tbl := table.New(table.Identifier{"n", "t"}, meta, dir+"/metadata.json", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, nil)
	sc := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	err = tableTakes(t.Context(), tbl, tbl.Identifier(), sc, destination.Stream{Name: "t", Mode: destination.Merge, PrimaryKey: []string{"id"}})
	if err == nil {
		t.Fatal("a table whose manifest list is missing takes a merge")
	}
	if f := check.Classify(err); f != nil && f.Reason == "merge_unsupported" {
		t.Errorf("a missing manifest list: %v", err)
	}
}
