package iceberg

import (
	"slices"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

func itemNames(checks []check.Check) []string {
	out := make([]string, len(checks))
	for i, c := range checks {
		out[i] = c.Name
	}
	return out
}

// The lists are valid for any config and request, streams whose names make
// the same check name included, and hold what the config calls for.
func TestCheckLists(t *testing.T) {
	for config, want := range map[string][]string{
		`{"uri": "https://catalog.example.com/api", "token": "t"}`: {"net.dns", "net.connect", "net.tls", "catalog.auth", "catalog.namespace", "catalog.create", "catalog.commit", "store.write", "store.delete"},
		`{"uri": "http://catalog:8181", "s3Endpoint": "http://s3:8333"}`: {"net.dns", "net.connect", "catalog.auth", "catalog.namespace", "catalog.create", "catalog.commit",
			"store.dns", "store.connect", "store.write", "store.delete"},
		`{"catalog": "glue", "region": "eu-west-1", "warehouse": "s3://b/w"}`: {"net.dns", "net.connect", "net.tls", "catalog.auth", "catalog.namespace", "catalog.create", "catalog.commit", "store.write", "store.delete"},
	} {
		d, err := NewDestination([]byte(config))
		if err != nil {
			t.Fatal(err)
		}
		checks := d.Checks()
		if err := check.Validate(checks); err != nil {
			t.Errorf("%s: %v", config, err)
		}
		if got := itemNames(checks); !slices.Equal(got, want) {
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
	if got := itemNames(pf); !slices.Equal(got, want) {
		t.Errorf("preflight %v, want %v", got, want)
	}
	if pf[0].Advisory {
		t.Error("a named Glue endpoint's reach is advisory")
	}
}
