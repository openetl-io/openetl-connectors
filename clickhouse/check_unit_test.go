package clickhouse

import (
	"errors"
	"slices"
	"testing"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/openetl-io/openetl-connectors/connectortest"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/configschema"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestSchemasLint(t *testing.T) {
	for name, schema := range map[string][]byte{"source": SourceConfigSchema, "destination": DestinationConfigSchema} {
		for _, err := range configschema.Lint(schema) {
			t.Errorf("%s: %v", name, err)
		}
		form, err := configschema.ReadForm(schema)
		if err != nil {
			t.Fatal(err)
		}
		if u := configschema.Undescribed(form); len(u) > 0 {
			t.Errorf("%s: no description: %v", name, u)
		}
	}
}

// The lists are valid for any config and request, streams whose names make
// the same check name included, and hold what the config calls for.
func TestCheckLists(t *testing.T) {
	s, err := NewSource([]byte(`{"addr": "ch.example.com:9440", "tls": true, "tables": ["Orders", "orders"]}`))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"net.dns", "net.connect", "net.tls", "db.connect", "server.version", "database.found", "tables.found", "tables.read"}
	if got := connectortest.ItemNames(s.Checks()); !slices.Equal(got, want) {
		t.Errorf("source checks %v, want %v", got, want)
	}
	if err := check.Validate(s.Checks()); err != nil {
		t.Error(err)
	}
	pf := s.Preflight(source.PreflightRequest{Streams: []source.ConfiguredStream{
		{Name: "Orders", SyncMode: source.CDC}, {Name: "orders", SyncMode: source.Incremental, Cursor: "at"}, {Name: "x", SyncMode: source.FullRefresh},
	}})
	want = []string{"db.connect", "stream.orders.mode", "stream.orders.read", "stream.orders.cursor", "stream.x.read", "stream.x.order"}
	if got := connectortest.ItemNames(pf); !slices.Equal(got, want) {
		t.Errorf("source preflight %v, want %v", got, want)
	}
	if err := check.Validate(pf); err != nil {
		t.Error(err)
	}

	s, err = NewSource([]byte(`{"addr": "ch.example.com:9000"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := connectortest.ItemNames(s.Checks()); !slices.Equal(got, []string{"net.dns", "net.connect", "db.connect", "server.version", "database.found", "tables.read"}) {
		t.Errorf("source checks without tls or tables %v", got)
	}

	d, err := NewDestination([]byte(`{"addr": "ch.example.com:9000"}`))
	if err != nil {
		t.Fatal(err)
	}
	want = []string{"net.dns", "net.connect", "db.connect", "server.version", "database.create",
		"grant.tables", "grant.rows", "grant.columns", "grant.partitions", "grant.update", "grant.system", "settings.write"}
	if got := connectortest.ItemNames(d.Checks()); !slices.Equal(got, want) {
		t.Errorf("destination checks %v, want %v", got, want)
	}
	if err := check.Validate(d.Checks()); err != nil {
		t.Error(err)
	}
	dp := d.Preflight(destination.PreflightRequest{Namespace: "ns", Streams: []destination.Stream{{Name: "A"}, {Name: "a"}}})
	if got := connectortest.ItemNames(dp); !slices.Equal(got, append(append([]string{"db.connect"}, want[4:]...), "stream.a.write", "stream.a.write-2")) {
		t.Errorf("destination preflight %v", got)
	}
	if err := check.Validate(dp); err != nil {
		t.Error(err)
	}
	for _, c := range dp {
		if c.Field != "" {
			t.Errorf("%s: field %s, in a preflight whose database is the pipeline's namespace", c.Name, c.Field)
		}
	}
}

func TestFailure(t *testing.T) {
	for code, reason := range map[int32]string{
		codeWrongPassword: "credentials_rejected", codeAuthenticationFailed: "credentials_rejected", codeAccessDenied: "privilege_missing",
		codeReadonly: "permission_denied", codeUnknownDatabase: "not_found", codeTooManySimultaneous: "unavailable",
	} {
		f := failure(&ch.Exception{Code: code, Message: "m"})
		if f == nil || f.Reason != reason {
			t.Errorf("%d: %+v, want %s", code, f, reason)
		}
	}
	if f := failure(errors.New("not the server's")); f != nil {
		t.Errorf("a client error: %+v", f)
	}
}

func TestOlder(t *testing.T) {
	for v, want := range map[string]bool{"26.8.37.168": false, "26.7.1.1": true, "25.12.1": true, "27.1.1": false, "head": false} {
		if got := older(v, [2]int{26, 8}); got != want {
			t.Errorf("%s: %v", v, got)
		}
	}
}
