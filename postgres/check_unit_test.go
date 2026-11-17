package postgres

import (
	"slices"
	"testing"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// The lists are valid for any config and request, streams whose names make
// the same check name included, and hold what the config calls for.
func TestCheckLists(t *testing.T) {
	s, err := NewSource([]byte(`{"dsn": "postgres://u:p@db.example.com:5433/app?sslmode=require", "tables": ["Orders", "orders"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Validate(s.Checks()); err != nil {
		t.Error(err)
	}
	want := []string{"net.dns", "net.connect", "db.tls", "db.connect", "schema.usage", "tables.found", "tables.read", "cdc.wal-level", "cdc.replication"}
	if got := connectortest.ItemNames(s.Checks()); !slices.Equal(got, want) {
		t.Errorf("source checks %v, want %v", got, want)
	}
	req := source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{
		{Name: "Orders", SyncMode: source.CDC}, {Name: "orders", SyncMode: source.Incremental, Cursor: "at"}, {Name: "x"},
	}}
	pf := s.Preflight(req)
	if err := check.Validate(pf); err != nil {
		t.Error(err)
	}
	want = []string{"db.connect", "stream.orders.read", "stream.orders.cdc", "stream.orders.read-2", "stream.orders.cursor", "stream.x.read",
		"cdc.wal-level", "cdc.replication", "cdc.wal-keep", "cdc.publication", "cdc.slot"}
	if got := connectortest.ItemNames(pf); !slices.Equal(got, want) {
		t.Errorf("source preflight %v, want %v", got, want)
	}
	// Without cdc, nothing of it.
	if got := connectortest.ItemNames(s.Preflight(source.PreflightRequest{Streams: req.Streams[2:]})); !slices.Equal(got, []string{"db.connect", "stream.x.read"}) {
		t.Errorf("preflight without cdc %v", got)
	}

	// Without TLS, and through a Unix socket.
	for dsn, want := range map[string][]string{
		"postgres://u:p@db.example.com/app?sslmode=disable": {"net.dns", "net.connect", "db.connect", "schema.usage", "tables.read", "cdc.wal-level", "cdc.replication"},
		"host=/var/run/postgresql user=u dbname=app":        {"db.connect", "schema.usage", "tables.read", "cdc.wal-level", "cdc.replication"},
	} {
		s, err := NewSource([]byte(`{"dsn": "` + dsn + `"}`))
		if err != nil {
			t.Fatal(err)
		}
		if got := connectortest.ItemNames(s.Checks()); !slices.Equal(got, want) {
			t.Errorf("%s: %v, want %v", dsn, got, want)
		}
	}

	d, err := NewDestination([]byte(`{"dsn": "postgres://u:p@db.example.com/app?sslmode=verify-full"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := connectortest.ItemNames(d.Checks()), []string{"net.dns", "net.connect", "db.tls", "db.connect", "schema.create"}; !slices.Equal(got, want) {
		t.Errorf("destination checks %v, want %v", got, want)
	}
	dpf := d.Preflight(destination.PreflightRequest{Namespace: "n", Streams: []destination.Stream{{Name: "a b"}, {Name: "a.b", Mode: destination.Merge}}})
	if err := check.Validate(dpf); err != nil {
		t.Error(err)
	}
	if got, want := connectortest.ItemNames(dpf), []string{"db.connect", "schema.create", "stream.a-b.write", "stream.a-b.write-2"}; !slices.Equal(got, want) {
		t.Errorf("destination preflight %v, want %v", got, want)
	}
}
