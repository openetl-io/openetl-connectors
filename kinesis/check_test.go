//go:build integration

package kinesis

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// statuses are a report's items' statuses by name, failures with their
// reason.
func statuses(r check.Report) map[string]string {
	out := map[string]string{}
	for _, it := range r.Items {
		s := it.Status.String()
		if it.Failure != nil {
			s += ":" + it.Failure.Reason
		}
		out[it.Name] = s
	}
	return out
}

// want fails unless each named item has its status, logging each item as
// people see it.
func want(t *testing.T, what string, r check.Report, w map[string]string) {
	t.Helper()
	for _, it := range r.Items {
		it = it.Rendered()
		t.Logf("%s: %-8s %s: %s: %s", what, it.Status, it.Name, it.Title, it.Message)
	}
	got := statuses(r)
	for name, s := range w {
		if got[name] != s {
			t.Errorf("%s: %s is %q, want %q (all: %v)", what, name, got[name], s, got)
		}
	}
}

func TestChecksAndPreflights(t *testing.T) {
	stream := uniq("chk")
	createStream(t, stream, 2)
	s := newSource(t, map[string]any{"streams": []string{stream}})
	r := check.Run(t.Context(), s.Checks())
	want(t, "source check", r, map[string]string{"net.connect": "passed", "kinesis.auth": "passed", "streams.list": "passed"})
	if !r.OK() {
		t.Error(r.Err())
	}
	gone := uniq("missing")
	r = check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "p", Interval: 48 * time.Hour, Streams: []source.ConfiguredStream{
		{Name: stream, SyncMode: source.Stream}, {Name: gone, SyncMode: source.Stream}, {Name: "full", SyncMode: source.FullRefresh},
	}}))
	want(t, "source preflight", r, map[string]string{
		"stream." + check.Name(stream) + ".describe":  "passed",
		"stream." + check.Name(stream) + ".read":      "passed",
		"stream." + check.Name(stream) + ".retention": "warning:retention_below_interval",
		"stream." + check.Name(gone) + ".describe":    "failed:not_found",
		"stream." + check.Name(gone) + ".read":        "skipped",
		"stream.full.mode":                            "failed:sync_mode_unsupported",
	})
	r = check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "p", Interval: time.Hour, Streams: []source.ConfiguredStream{{Name: stream}}}))
	want(t, "an hourly sync", r, map[string]string{"stream." + check.Name(stream) + ".retention": "passed"})

	missing := newSource(t, map[string]any{"streams": []string{gone}})
	want(t, "a stream that doesn't exist", check.Run(t.Context(), missing.Checks()), map[string]string{"streams.list": "failed:not_found"})
	none := newSource(t, map[string]any{"streamPattern": "^no-such-stream-ever$"})
	want(t, "no stream offered", check.Run(t.Context(), none.Checks()), map[string]string{"streams.list": "failed:no_streams"})

	down := newSource(t, map[string]any{"endpoint": closedPort})
	r = check.Run(t.Context(), down.Checks())
	want(t, "unreachable", r, map[string]string{"net.connect": "failed:connection_refused", "kinesis.auth": "skipped"})
	if f := check.Classify(r.Err()); f == nil || f.Code.String() != "ERROR_CODE_TRANSIENT" {
		t.Errorf("unreachable: %v", r.Err())
	}

	d := newDest(t, nil)
	r = check.Run(t.Context(), d.Preflight(destination.PreflightRequest{Pipeline: "p", Streams: []destination.Stream{
		{Name: stream, Mode: destination.Append}, {Name: "new", Mode: destination.Append}, {Name: "merged", Mode: destination.Merge},
	}, Namespace: ""}))
	want(t, "destination preflight", r, map[string]string{
		"stream." + check.Name(stream) + ".write": "passed",
		"stream.new.write":                        "passed",
		"stream.merged.mode":                      "failed:write_mode_unsupported",
	})
	noCreate := newDest(t, map[string]any{"createStreams": false})
	r = check.Run(t.Context(), noCreate.Preflight(destination.PreflightRequest{Pipeline: "p", Namespace: "ns", Streams: []destination.Stream{{Name: "absent", Mode: destination.Append}}}))
	want(t, "createStreams off", r, map[string]string{"stream.absent.write": "failed:stream_missing"})
}

// With Avro, the checks reach the registry and sign in, and the preflight
// finds a stream's value subject, or warns that it's missing.
func TestRegistryChecks(t *testing.T) {
	registered, unregistered := uniq("reg"), uniq("unreg")
	createStream(t, registered, 1)
	reg, err := sr.NewClient(sr.URLs(registryURL()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.CreateSchema(t.Context(), registered+"-value", sr.Schema{Schema: `{"type": "record", "name": "r", "fields": [{"name": "id", "type": "long"}]}`}); err != nil {
		t.Fatal(err)
	}
	s := newSource(t, map[string]any{"format": "avro", "schemaRegistryUrl": registryURL(), "streams": []string{registered}})
	want(t, "registry", check.Run(t.Context(), s.Checks()), map[string]string{"registry.connect": "passed", "registry.auth": "passed"})
	r := check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: registered}, {Name: unregistered}}}))
	want(t, "subjects", r, map[string]string{
		"stream." + check.Name(registered) + ".schema":   "passed",
		"stream." + check.Name(unregistered) + ".schema": "warning:subject_missing",
	})
	closed := newSource(t, map[string]any{"format": "avro", "schemaRegistryUrl": "http://127.0.0.1:1", "streams": []string{registered}})
	want(t, "no registry", check.Run(t.Context(), closed.Checks()), map[string]string{"registry.connect": "failed:connection_refused", "registry.auth": "skipped"})
}
