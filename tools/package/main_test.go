package main

import "testing"

func TestSplit(t *testing.T) {
	for in, want := range map[string][2]string{
		"source-postgres":     {"source", "postgres"},
		"destination-s3":      {"destination", "s3"},
		"source-my-connector": {"source", "my-connector"},
	} {
		kind, name, err := split(in)
		if err != nil || kind != want[0] || name != want[1] {
			t.Errorf("split(%q) = %q, %q, %v; want %q, %q", in, kind, name, err, want[0], want[1])
		}
	}
	for _, in := range []string{"postgres", "sink-postgres", "source-"} {
		if _, _, err := split(in); err == nil {
			t.Errorf("split(%q): no error", in)
		}
	}
}

func TestCheck(t *testing.T) {
	doc := []byte("metadata:\n  name: postgres\nspec:\n  kind: source\n")
	if err := check(doc, "source", "postgres"); err != nil {
		t.Error(err)
	}
	if err := check(doc, "destination", "postgres"); err == nil {
		t.Error("a source passed as a destination")
	}
	if err := check(doc, "source", "mysql"); err == nil {
		t.Error("the wrong name passed")
	}
}
