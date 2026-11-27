package postgres

import (
	"fmt"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
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

// A merge whose schema lacks its key fails its write item, as the writer
// would refuse its batches, before the item looks at the table.
func TestPreflightMergeKey(t *testing.T) {
	d, err := NewDestination([]byte(`{"dsn": "postgres://u:p@127.0.0.1:1/app?sslmode=disable&connect_timeout=5"}`))
	if err != nil {
		t.Fatal(err)
	}
	value := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	keyed := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	pf := d.Preflight(destination.PreflightRequest{
		Streams: []destination.Stream{{Name: "a", Mode: destination.Merge, PrimaryKey: []string{"id"}}, {Name: "b", Mode: destination.Merge, PrimaryKey: []string{"id"}}},
		Schemas: map[string]*arrow.Schema{"a": value, "b": keyed},
	})
	if err := check.Validate(pf); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		item, reason string
	}{{"stream.a.write", "config_invalid"}, {"stream.b.write", "connection_refused"}} {
		i := slices.IndexFunc(pf, func(ch check.Check) bool { return ch.Name == c.item })
		if got := check.Classify(pf[i].Run(t.Context(), &check.Probe{})); got == nil || got.Reason != c.reason {
			t.Errorf("%s: %v, want %s", c.item, got, c.reason)
		}
	}
}

// A schema name the server reserves is the config's to change, and a slot
// the server has no room for waits for one, as the check says of both.
func TestClassifyRefusals(t *testing.T) {
	for state, want := range map[string]protocolv1.ErrorCode{
		"42939": protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		"53400": protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED,
		"42501": protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
	} {
		err := classify(fmt.Errorf("postgres: x: %w", &pgconn.PgError{Code: state}))
		if code := connectortest.CodeOf(err); code != want {
			t.Errorf("%s: %v, want %v", state, code, want)
		}
	}
}

// Of a dsn with several hosts, the first is only advised on: an unreachable
// one leaves db.connect to try the others, where one host's fails the check
// first.
func TestCheckSeveralHosts(t *testing.T) {
	for dsn, want := range map[string]map[string]check.Status{
		"postgres://u:p@127.0.0.1:1/app?sslmode=disable":             {"net.connect": check.Failed, "db.connect": check.Skipped},
		"postgres://u:p@127.0.0.1:1,127.0.0.1:2/app?sslmode=disable": {"net.connect": check.Warning, "db.connect": check.Failed},
		"postgres://u:p@127.0.0.1:1,127.0.0.1:2/app?sslmode=require": {"net.connect": check.Warning, "db.tls": check.Warning, "db.connect": check.Failed},
	} {
		s, err := NewSource([]byte(`{"dsn": "` + dsn + `&connect_timeout=5"}`))
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]check.Status{}
		for _, it := range check.Run(t.Context(), s.Checks()).Items {
			got[it.Name] = it.Status
		}
		for name, st := range want {
			if got[name] != st {
				t.Errorf("%s: %s is %v, want %v", dsn, name, got[name], st)
			}
		}
	}
}
