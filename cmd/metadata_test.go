package cmd_test

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestEveryConnectorHasMetadata(t *testing.T) {
	b, err := os.ReadFile("../connectors.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var meta map[string]struct {
		Author string `yaml:"author"`
		Logo   string `yaml:"logo"`
	}
	if err := yaml.Unmarshal(b, &meta); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	connectors := map[string]bool{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		connectors[e.Name()] = true
		m, ok := meta[e.Name()]
		switch {
		case !ok:
			t.Errorf("%s: not in connectors.yaml", e.Name())
		case m.Author == "":
			t.Errorf("%s: no author", e.Name())
		case !strings.HasPrefix(m.Logo, "https://"):
			t.Errorf("%s: logo %q isn't an https URL", e.Name(), m.Logo)
		}
	}
	for name := range meta {
		if !connectors[name] {
			t.Errorf("connectors.yaml names %s, which cmd/ doesn't have", name)
		}
	}
}
