package connectortest

import (
	"testing"

	"github.com/openetl-io/openetl/sdk/go/check"
)

func TestUnfilled(t *testing.T) {
	for _, c := range []struct {
		name string
		it   check.Item
		want bool
	}{
		{"filled", check.Item{Title: "Read {stream}", Params: check.Params{"stream": "orders"}}, false},
		{"a value's braces", check.Item{Title: "Read {stream}", Params: check.Params{"stream": "orders{2026}"}}, false},
		{"a value that's a placeholder", check.Item{Title: "Read {stream}", Params: check.Params{"stream": "{table}"}}, false},
		{"a failure's param", check.Item{Title: "Sign in", Failure: &check.Failure{Reason: "x", Message: "{principal} can't", Params: check.Params{"principal": "{}"}}}, false},
		{"no param", check.Item{Title: "Read {stream}"}, true},
		{"a failure's fix without its param", check.Item{Title: "Sign in", Failure: &check.Failure{Reason: "x", Fix: "Grant {privilege}"}}, true},
		{"a brace in a template", check.Item{Title: "Read", Message: "got {"}, true},
	} {
		if _, got := unfilled(c.it); got != c.want {
			t.Errorf("%s: unfilled is %v, want %v", c.name, got, c.want)
		}
	}
}
