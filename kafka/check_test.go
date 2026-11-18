//go:build integration

package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

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

// logItems logs each item as people see it.
func logItems(t *testing.T, what string, r check.Report) {
	t.Helper()
	for _, it := range r.Items {
		it = it.Rendered()
		t.Logf("%s: %-8s %s: %s: %s", what, it.Status, it.Name, it.Title, it.Message)
	}
}

// want fails unless each named item has its status.
func want(t *testing.T, what string, r check.Report, w map[string]string) {
	t.Helper()
	logItems(t, what, r)
	got := statuses(r)
	for name, s := range w {
		if got[name] != s {
			t.Errorf("%s: %s is %q, want %q (all: %v)", what, name, got[name], s, got)
		}
	}
}

// allPass fails on an item that failed or warned.
func allPass(t *testing.T, what string, r check.Report) {
	t.Helper()
	logItems(t, what, r)
	for _, it := range r.Items {
		if it.Status != check.Passed {
			t.Errorf("%s: %s %s: %s", what, it.Name, it.Status, it.Rendered().Message)
		}
	}
	if !r.OK() {
		t.Errorf("%s: %v", what, r.Err())
	}
}

func TestChecksAndPreflights(t *testing.T) {
	topic, short := uniq("chk"), uniq("chk_short")
	createTopic(t, topic, 2)
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(t.Context(), 1, 1, map[string]*string{"retention.ms": kadm.StringPtr("60000")}, short); err != nil {
		t.Fatal(err)
	}
	// group.commit passes only for a group that exists: a commit creates it.
	pipeline := uniq("p")
	var offs kadm.Offsets
	offs.Add(kadm.Offset{Topic: topic, Partition: 0, At: 0, LeaderEpoch: -1})
	if err := adm.CommitAllOffsets(t.Context(), "openetl-"+pipeline, offs); err != nil {
		t.Fatal(err)
	}

	s := newSource(t, map[string]any{"topics": []string{topic, short}})
	allPass(t, "source check", check.Run(t.Context(), s.Checks()))
	gone := uniq("missing")
	r := check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: pipeline, Interval: time.Hour, Streams: []source.ConfiguredStream{
		{Name: topic, SyncMode: source.Stream}, {Name: short, SyncMode: source.Stream}, {Name: gone, SyncMode: source.Stream}, {Name: "full", SyncMode: source.FullRefresh},
	}}))
	want(t, "source preflight", r, map[string]string{
		"stream." + check.Name(topic) + ".read":      "passed",
		"stream." + check.Name(topic) + ".retention": "passed",
		"stream." + check.Name(short) + ".retention": "warning:retention_below_interval",
		"stream." + check.Name(gone) + ".read":       "failed:not_found",
		"stream." + check.Name(gone) + ".retention":  "skipped",
		"stream.full.mode":                           "failed:sync_mode_unsupported",
		"group.commit":                               "passed",
	})

	// Topics that don't exist, and an empty selection.
	missing := newSource(t, map[string]any{"topics": []string{uniq("missing")}})
	want(t, "missing topics", check.Run(t.Context(), missing.Checks()), map[string]string{"kafka.auth": "passed", "topics.read": "failed:not_found"})
	none := newSource(t, map[string]any{"topicPattern": "^no-such-topic-ever$"})
	want(t, "no topic offered", check.Run(t.Context(), none.Checks()), map[string]string{"topics.list": "failed:no_topics"})

	// Nothing listens.
	down := newSource(t, map[string]any{"brokers": []string{"127.0.0.1:1"}})
	r = check.Run(t.Context(), down.Checks())
	want(t, "unreachable", r, map[string]string{"broker.127-0-0-1-1.connect": "failed:connection_refused", "kafka.auth": "skipped"})
	if f := check.Classify(r.Err()); f == nil || f.Code.String() != "ERROR_CODE_TRANSIENT" {
		t.Errorf("unreachable: %v", r.Err())
	}
	// One of two bootstrap brokers down only warns.
	half := newSource(t, map[string]any{"brokers": []string{"127.0.0.1:1", brokers()}, "topics": []string{topic}})
	r = check.Run(t.Context(), half.Checks())
	want(t, "one broker down", r, map[string]string{"broker.127-0-0-1-1.connect": "warning:connection_refused", "kafka.auth": "passed", "topics.read": "passed"})
	if !r.OK() {
		t.Errorf("one broker down: %v", r.Err())
	}

	d := newDest(t, nil)
	allPass(t, "destination check", check.Run(t.Context(), d.Checks()))
	r = check.Run(t.Context(), d.Preflight(destination.PreflightRequest{Pipeline: uniq("p"), Namespace: "", Streams: []destination.Stream{
		{Name: topic, Mode: destination.Append}, {Name: uniq("new"), Mode: destination.Append}, {Name: "merged", Mode: destination.Merge},
	}}))
	want(t, "destination preflight", r, map[string]string{
		"stream." + check.Name(topic) + ".write": "passed",
		"stream.merged.mode":                     "failed:write_mode_unsupported",
		"transactions.id":                        "passed",
	})
	// Not newDest: its checks fail where the markers topic doesn't exist yet.
	noCreate, err := NewDestination(config(t, map[string]any{"createTopics": false}))
	if err != nil {
		t.Fatal(err)
	}
	r = check.Run(t.Context(), noCreate.Preflight(destination.PreflightRequest{Pipeline: "p", Streams: []destination.Stream{{Name: "absent", Mode: destination.Append}}}))
	want(t, "createTopics off", r, map[string]string{"stream.absent.write": "failed:topic_missing"})
}

// With Avro, the checks reach the registry and sign in, and the preflight
// finds a stream's value subject, or warns that it's missing.
func TestRegistryChecks(t *testing.T) {
	registered, unregistered := uniq("reg"), uniq("unreg")
	createTopic(t, registered, 1)
	reg, err := sr.NewClient(sr.URLs(registryURL()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.CreateSchema(t.Context(), registered+"-value", sr.Schema{Schema: `{"type": "record", "name": "r", "fields": [{"name": "id", "type": "long"}]}`}); err != nil {
		t.Fatal(err)
	}
	s := newSource(t, map[string]any{"format": "avro", "schemaRegistryUrl": registryURL(), "topics": []string{registered}})
	want(t, "registry", check.Run(t.Context(), s.Checks()), map[string]string{"registry.connect": "passed", "registry.auth": "passed"})
	r := check.Run(t.Context(), s.Preflight(source.PreflightRequest{Pipeline: "p", Streams: []source.ConfiguredStream{{Name: registered}, {Name: unregistered}}}))
	want(t, "subjects", r, map[string]string{
		"stream." + check.Name(registered) + ".schema":   "passed",
		"stream." + check.Name(unregistered) + ".schema": "warning:subject_missing",
	})
	closed := newSource(t, map[string]any{"format": "avro", "schemaRegistryUrl": "http://127.0.0.1:1", "topics": []string{registered}})
	want(t, "no registry", check.Run(t.Context(), closed.Checks()), map[string]string{"registry.connect": "failed:connection_refused", "registry.auth": "skipped"})
}
