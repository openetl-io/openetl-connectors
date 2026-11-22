package records

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sr/srfake"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/check"
)

// A registry URL's credentials sign in, and its errors don't show them.
func TestRegistryErrorsShowNoCredentials(t *testing.T) {
	fake := srfake.New(srfake.WithAuth("Basic " + base64.StdEncoding.EncodeToString([]byte("alice:s3cret"))))
	defer fake.Close()
	u, err := url.Parse(fake.URL())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("alice", "s3cret")
	reg, err := (&Config{SchemaRegistryURL: u.String()}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Subjects(t.Context()); err != nil {
		t.Errorf("the URL's credentials didn't sign in: %v", err)
	}
	reg, err = (&Config{SchemaRegistryURL: "http://alice:s3cret@127.0.0.1:1/sr"}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewDecoder(Avro, reg).Decode(t.Context(), []byte{0, 0, 0, 0, 1, 2})
	if err == nil || strings.Contains(err.Error(), "s3cret") || !Unanswered(err) {
		t.Errorf("a registry that doesn't answer: %v", err)
	}
}

// Credentials for a registry reached over http pass with a warning.
func TestRegistryCredentialsOverHTTPWarn(t *testing.T) {
	fake := srfake.New(srfake.WithAuth("Basic " + base64.StdEncoding.EncodeToString([]byte("alice:s3cret"))))
	defer fake.Close()
	open := srfake.New()
	defer open.Close()
	u, _ := url.Parse(fake.URL())
	u.User = url.UserPassword("alice", "s3cret")
	for _, tc := range []struct {
		c    Config
		want string
	}{
		{Config{SchemaRegistryURL: fake.URL(), SchemaRegistryUsername: "alice", SchemaRegistryPassword: sdk.NewSecret("s3cret")}, "warning:credentials_unencrypted"},
		{Config{SchemaRegistryURL: u.String()}, "warning:credentials_unencrypted"},
		{Config{SchemaRegistryURL: open.URL()}, "passed"},
	} {
		for _, it := range check.Run(t.Context(), tc.c.RegistryChecks(5*time.Second)).Items {
			got := it.Status.String()
			if it.Failure != nil {
				got += ":" + it.Failure.Reason
			}
			if it.Name == "registry.auth" && got != tc.want {
				t.Errorf("%s with user %q: %s %s, want %s", ShownURL(tc.c.SchemaRegistryURL), tc.c.SchemaRegistryUsername, it.Name, got, tc.want)
			}
		}
	}
}

// A registry that doesn't answer fails the checks with errors that don't
// show the URL's credentials, whatever their characters.
func TestFailedRegistryChecksShowNoCredentials(t *testing.T) {
	for _, raw := range []string{
		"http://alice:s3cret@127.0.0.1:1/sr",
		"http://alice:s3?cret@127.0.0.1:1/sr",
		"http://alice:s3/cret@127.0.0.1:1/sr",
		"http://alice:s3#cret@127.0.0.1:1/sr",
		"http://alice:s3%zzcret@127.0.0.1:1/sr",
	} {
		c := Config{SchemaRegistryURL: raw}
		got := fmt.Sprintf("%+v", check.Run(t.Context(), c.RegistryChecks(5*time.Second)).Items)
		if strings.Contains(got, "cret") || !strings.Contains(got, "failed") {
			t.Errorf("%s: %s", ShownURL(raw), got)
		}
	}
}

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
