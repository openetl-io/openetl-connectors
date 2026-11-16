package kinesis

import (
	"slices"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func itemsOf(checks []check.Check) []string {
	out := make([]string, len(checks))
	for i, c := range checks {
		out[i] = c.Name
	}
	return out
}

// The lists are valid for any config and request, and hold what the config
// calls for: AWS's own address only advises, since a proxy may reach it.
func TestCheckLists(t *testing.T) {
	s, err := NewSource([]byte(`{"region": "eu-west-1", "format": "avro", "schemaRegistryUrl": "http://r:8081", "streams": ["a.b", "a-b"]}`))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"net.dns", "net.connect", "net.tls", "kinesis.auth", "registry.connect", "registry.auth", "streams.list"}
	if got := itemsOf(s.Checks()); !slices.Equal(got, want) {
		t.Errorf("source checks %v, want %v", got, want)
	}
	if err := check.Validate(s.Checks()); err != nil {
		t.Error(err)
	}
	if c := s.Checks()[1]; !c.Advisory || c.Params["address"] != "kinesis.eu-west-1.amazonaws.com:443" {
		t.Errorf("AWS's address: %+v", c)
	}
	pf := s.Preflight(source.PreflightRequest{Interval: time.Hour, Streams: []source.ConfiguredStream{{Name: "a.b"}, {Name: "a-b", SyncMode: source.Stream}}})
	if err := check.Validate(pf); err != nil {
		t.Error(err)
	}
	want = []string{"stream.a-b.mode", "stream.a-b.describe", "stream.a-b.read", "stream.a-b.retention", "stream.a-b.schema",
		"stream.a-b-2.mode", "stream.a-b-2.describe", "stream.a-b-2.read", "stream.a-b-2.retention", "stream.a-b-2.schema"}
	if got := itemsOf(pf); !slices.Equal(got, want) {
		t.Errorf("source preflight %v, want %v", got, want)
	}

	// An emulator's endpoint, over HTTP, isn't advisory; without a region
	// or an endpoint, the address is the AWS config's, unknown here.
	d, err := NewDestination([]byte(`{"endpoint": "http://localhost:4568"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := itemsOf(d.Checks()), []string{"net.dns", "net.connect", "kinesis.auth"}; !slices.Equal(got, want) {
		t.Errorf("destination checks %v, want %v", got, want)
	}
	if c := d.Checks()[1]; c.Advisory || c.Field != "endpoint" || c.Params["address"] != "localhost:4568" {
		t.Errorf("the endpoint's check: %+v", c)
	}
	d, err = NewDestination([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := itemsOf(d.Checks()); !slices.Equal(got, []string{"kinesis.auth"}) {
		t.Errorf("destination checks without a region %v", got)
	}
	dpf := d.Preflight(destination.PreflightRequest{Streams: []destination.Stream{{Name: "x"}, {Name: "y", Mode: destination.Merge}}})
	if got, want := itemsOf(dpf), []string{"stream.x.mode", "stream.x.write", "stream.y.mode", "stream.y.write"}; !slices.Equal(got, want) {
		t.Errorf("destination preflight %v, want %v", got, want)
	}
	if err := dpf[2].Run(t.Context(), &check.Probe{}); check.Classify(err).Reason != "write_mode_unsupported" {
		t.Errorf("a merge stream's mode: %v", err)
	}
}
