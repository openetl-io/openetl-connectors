package connectortest

import (
	"strings"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/check"
)

// ItemNames are the checks' names, in order.
func ItemNames(checks []check.Check) []string {
	out := make([]string, len(checks))
	for i, c := range checks {
		out[i] = c.Name
	}
	return out
}

// Statuses are each item's status and reason, as "status reason", by name.
func Statuses(r check.Report) map[string]string {
	out := map[string]string{}
	for _, it := range r.Items {
		s := it.Status.String()
		if it.Failure != nil {
			s += " " + it.Failure.Reason
		}
		out[it.Name] = s
	}
	return out
}

// ExpectItems checks the items named in want, and logs every item, as it
// renders, if one isn't as wanted.
func ExpectItems(t *testing.T, what string, r check.Report, want map[string]string) {
	t.Helper()
	got := Statuses(r)
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s: %s is %q, want %q", what, name, got[name], w)
		}
	}
	if t.Failed() {
		for _, it := range r.Items {
			it = it.Rendered()
			t.Logf("%s %s: %s: %s", it.Status, it.Name, it.Title, it.Message)
		}
	}
}

// Items runs checks, and gives each item's status, and its failure's reason
// after it, by name; each item's text has its params filled in.
func Items(t *testing.T, checks []check.Check) map[string]string {
	t.Helper()
	if err := check.Validate(checks); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, it := range check.Run(t.Context(), checks).Items {
		s := it.Status.String()
		if f := it.Failure; f != nil {
			s += " " + f.Reason
		}
		if text, ok := unfilled(it); ok {
			t.Errorf("%s: a placeholder unfilled: %s", it.Name, text)
		}
		out[it.Name] = s
	}
	return out
}

// unfilled is it's text, rendered, and whether one of its templates has a
// brace that isn't a placeholder its params fill. A param's value may have
// braces of its own.
func unfilled(it check.Item) (string, bool) {
	r := it.Rendered()
	blank := make(check.Params, len(r.Params))
	for k := range r.Params {
		blank[k] = ""
	}
	templates, text := []string{it.Title, it.Message}, r.Title+" "+r.Message
	if f := it.Failure; f != nil {
		templates = append(templates, f.Message, f.Fix)
		text += " " + r.Failure.Message + " " + r.Failure.Fix
	}
	for _, tm := range templates {
		if strings.ContainsAny(check.Render(tm, blank), "{}") {
			return text, true
		}
	}
	return text, false
}

// ExpectStatuses compares the items want names, of got, the statuses Items
// gives; the rest pass.
func ExpectStatuses(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for name, status := range got {
		w, ok := want[name]
		switch {
		case ok && status != w:
			t.Errorf("%s: %s, want %s", name, status, w)
		case !ok && status != "passed":
			t.Errorf("%s: %s", name, status)
		}
	}
	for name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("no item %s", name)
		}
	}
}
