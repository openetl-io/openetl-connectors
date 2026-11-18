//go:build integration

package iceberg

import (
	"encoding/json"
	"maps"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

func statuses(r check.Report) map[string]check.Status {
	out := map[string]check.Status{}
	for _, it := range r.Items {
		out[it.Name] = it.Status
	}
	return out
}

// The checks pass against the REST catalog and Glue, and leave nothing
// behind: neither the probe table nor a namespace they created.
func TestChecks(t *testing.T) {
	env := newEnv(t)
	configs := map[string]map[string]any{
		"rest": {"uri": uri(), "namespace": env.ns, "s3Endpoint": s3(), "region": "us-east-1", "accessKeyId": "openetl", "secretAccessKey": "openetl"},
	}
	if ep := os.Getenv("OPENETL_TEST_GLUE_ENDPOINT"); ep != "" {
		configs["glue"] = map[string]any{"catalog": "glue", "namespace": env.ns, "warehouse": "s3://iceberg/glue", "s3Endpoint": s3(), "region": "us-east-1",
			"accessKeyId": "openetl", "secretAccessKey": "openetl", "properties": map[string]string{"glue.endpoint": ep}}
	}
	for name, cfg := range configs {
		raw, _ := json.Marshal(cfg)
		d, err := NewDestination(raw)
		if err != nil {
			t.Fatal(err)
		}
		r := check.Run(t.Context(), d.Checks())
		if err := r.Err(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		for n, s := range statuses(r) {
			if s != check.Passed {
				t.Errorf("%s: %s is %v", name, n, s)
			}
		}
		cat, err := d.catalog(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if ok, _ := cat.CheckNamespaceExists(t.Context(), []string{env.ns}); ok {
			t.Errorf("%s: the checks left the namespace %s", name, env.ns)
		}
	}

	// A token the catalog refuses.
	bad := map[string]any{"uri": tokenProxy(t, uri(), "right"), "token": "wrong", "namespace": env.ns}
	raw, _ := json.Marshal(bad)
	d, err := NewDestination(raw)
	if err != nil {
		t.Fatal(err)
	}
	r := check.Run(t.Context(), d.Checks())
	if s := statuses(r); s["catalog.auth"] != check.Failed || s["catalog.create"] != check.Skipped {
		t.Errorf("with a wrong token: %v", s)
	}
	for _, it := range r.Items {
		if it.Name == "catalog.auth" && (it.Failure == nil || it.Failure.Reason != check.CredentialsRejected.Name) {
			t.Errorf("catalog.auth failed with %+v, want %s", it.Failure, check.CredentialsRejected.Name)
		}
	}
	// An unreachable catalog.
	closed := maps.Clone(bad)
	closed["uri"] = "http://127.0.0.1:1"
	raw, _ = json.Marshal(closed)
	if d, err = NewDestination(raw); err != nil {
		t.Fatal(err)
	}
	if s := statuses(check.Run(t.Context(), d.Checks())); s["net.connect"] != check.Failed || s["catalog.auth"] != check.Skipped {
		t.Errorf("unreachable: %v", s)
	}
}

// The preflight passes a stream whose table doesn't exist yet, and fails a
// type change the stream doesn't allow and a merge into a table partitioned
// by a column outside its key.
func TestPreflight(t *testing.T) {
	env := newEnv(t)
	ctx := t.Context()
	cat, err := env.d.catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(ctx, []string{env.ns}, nil); err != nil {
		t.Fatal(err)
	}
	sc := ice.NewSchema(0,
		ice.NestedField{ID: 1, Name: "id", Type: ice.PrimitiveTypes.Int64},
		ice.NestedField{ID: 2, Name: "name", Type: ice.PrimitiveTypes.Int32})
	spec := ice.NewPartitionSpec(ice.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "name", Transform: ice.IdentityTransform{}})
	if _, err := cat.CreateTable(ctx, table.Identifier{env.ns, "typed"}, sc, catalog.WithPartitionSpec(&spec)); err != nil {
		t.Fatal(err)
	}
	stream := arrow.NewSchema([]arrow.Field{idField, nameField}, nil)
	req := destination.PreflightRequest{Namespace: env.ns, Streams: []destination.Stream{
		{Name: "fresh", Mode: destination.Merge, PrimaryKey: []string{"id"}},
		{Name: "typed", Mode: destination.Append},
		{Name: "typed", Mode: destination.Merge, PrimaryKey: []string{"id"}},
		{Name: "keyless", Mode: destination.Merge},
	}, Schemas: map[string]*arrow.Schema{"fresh": stream, "typed": stream, "keyless": stream}}
	r := check.Run(ctx, env.d.Preflight(req))
	got := map[string]string{}
	for _, it := range r.Items {
		reason := ""
		if it.Failure != nil {
			reason = it.Failure.Reason
		}
		got[it.Name] = it.Status.String() + " " + reason
	}
	for name, want := range map[string]string{
		"catalog.namespace":   "passed",
		"stream.fresh.mode":   "passed",
		"stream.fresh.table":  "passed",
		"stream.typed.table":  "failed " + typeChange.Name,
		"stream.keyless.mode": "failed " + noKey.Name,
	} {
		if !strings.HasPrefix(strings.ToLower(got[name]), want) {
			t.Errorf("%s: %q, want %q", name, got[name], want)
		}
	}

	// The same column types: the merge's partitioning is what fails.
	sc2 := ice.NewSchema(0,
		ice.NestedField{ID: 1, Name: "id", Type: ice.PrimitiveTypes.Int64},
		ice.NestedField{ID: 2, Name: "name", Type: ice.PrimitiveTypes.String})
	if _, err := cat.CreateTable(ctx, table.Identifier{env.ns, "parts"}, sc2, catalog.WithPartitionSpec(&spec)); err != nil {
		t.Fatal(err)
	}
	req = destination.PreflightRequest{Namespace: env.ns, Streams: []destination.Stream{
		{Name: "parts", Mode: destination.Merge, PrimaryKey: []string{"id"}},
	}, Schemas: map[string]*arrow.Schema{"parts": stream}}
	for _, it := range check.Run(ctx, env.d.Preflight(req)).Items {
		if it.Name == "stream.parts.table" && (it.Failure == nil || it.Failure.Reason != cantMerge.Name) {
			t.Errorf("merge into a table partitioned off its key: %v %+v", it.Status, it.Failure)
		}
	}
}
