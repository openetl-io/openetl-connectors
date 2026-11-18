package records

import (
	"strings"
	"testing"
	"time"
)

func TestRegistryChecksShowNoCredentials(t *testing.T) {
	for raw, want := range map[string]string{
		"https://registry.example:8081":                "https://registry.example:8081",
		"https://alice:secret@registry.example/sr":     "https://registry.example/sr",
		"https://registry.example/sr?api_key=secret#x": "https://registry.example/sr",
		"https://alice:se?cret@registry.example/sr":    "https://registry.example/sr",
		"https://alice:se/cret@registry.example/sr":    "https://registry.example/sr",
		"https://alice:se%zzcret@registry.example/sr":  "https://registry.example/sr",
	} {
		c := Config{SchemaRegistryURL: raw}
		for _, ch := range c.RegistryChecks(time.Second) {
			if got := ch.Params["url"]; got != want || strings.Contains(got, "secret") {
				t.Errorf("%s: %s's url is %q, want %q", raw, ch.Name, got, want)
			}
		}
	}
}
