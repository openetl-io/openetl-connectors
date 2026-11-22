package records

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/openetl-io/openetl/sdk/go/check"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// RegistryChecks reach the schema registry and sign in to it, each taking up
// to timeout; none without one.
func (c *Config) RegistryChecks(timeout time.Duration) []check.Check {
	if c.SchemaRegistryURL == "" {
		return nil
	}
	var answered error
	params := check.Params{"url": ShownURL(c.SchemaRegistryURL)}
	authField := "schemaRegistryUrl"
	if c.SchemaRegistryUsername != "" {
		authField = "schemaRegistryPassword"
	}
	return []check.Check{
		{
			Name: "registry.connect", Kind: check.Connection, Title: "Reach the schema registry at {url}", Field: "schemaRegistryUrl", Params: params, Timeout: timeout,
			Run: func(ctx context.Context, p *check.Probe) error {
				reg, err := c.Registry()
				if err != nil {
					return check.ConfigInvalid.Wrap(err)
				}
				_, answered = reg.Subjects(ctx)
				var re *sr.ResponseError
				switch {
				case answered == nil:
				case errors.As(answered, &re):
					if re.StatusCode >= http.StatusInternalServerError {
						return check.Unavailable.Wrap(answered, "status", strconv.Itoa(re.StatusCode))
					}
				default:
					return answered
				}
				return nil
			},
		},
		{
			Name: "registry.auth", Kind: check.Authentication, Title: "Sign in to the schema registry", Field: authField, Params: params, Needs: []string{"registry.connect"},
			Run: func(context.Context, *check.Probe) error {
				var re *sr.ResponseError
				if errors.As(answered, &re) {
					switch re.StatusCode {
					case http.StatusUnauthorized:
						return check.CredentialsRejected.Wrap(answered)
					case http.StatusForbidden:
						principal := c.SchemaRegistryUsername
						if principal == "" {
							principal = "an anonymous client"
						}
						return check.PrivilegeMissing.Wrap(answered, "principal", principal, "privilege", "read access", "object", "the registry's subjects")
					}
					return check.Unknown.Wrap(answered)
				}
				if answered == nil && c.sendsCredentialsInTheClear() {
					return check.Warn(clearCredentials.Fail())
				}
				return answered
			},
		},
	}
}

var clearCredentials = check.Reason{
	Name: "credentials_unencrypted", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
	Message: "The schema registry's URL is http, so its username and password cross the network unencrypted.",
	Fix:     "Use the registry's https URL.",
}

// sendsCredentialsInTheClear: credentials for a registry reached over http.
func (c *Config) sendsCredentialsInTheClear() bool {
	u, err := url.Parse(c.SchemaRegistryURL)
	return err == nil && u.Scheme == "http" && (c.SchemaRegistryUsername != "" || u.User != nil)
}

// ShownURL is raw without its userinfo, query and fragment, which can hold
// credentials, for params, which people see. A "?" or "#" in a password,
// which ends the authority for url.Parse, doesn't keep the userinfo: an "@"
// before the path ends it, wherever such a character is.
func ShownURL(raw string) string {
	scheme, rest := "", raw
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme, rest = raw[:i+3], raw[i+3:]
	}
	authority := rest
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		authority = rest[:i]
	}
	if u, err := url.Parse(raw); err == nil && (u.User != nil || !strings.Contains(authority, "@")) {
		u.User, u.RawQuery, u.ForceQuery, u.Fragment, u.RawFragment = nil, "", false, "", ""
		return u.String()
	}
	// The userinfo ends at the last "@" before the path, or, with none
	// there, at the last "@" of all, as a "/" in a password puts it after.
	if i := strings.LastIndexByte(authority, '@'); i >= 0 {
		rest = rest[i+1:]
	} else if i := strings.LastIndexByte(rest, '@'); i >= 0 {
		rest = rest[i+1:]
	}
	if i := strings.IndexAny(rest, "?#"); i >= 0 {
		rest = rest[:i]
	}
	return scheme + rest
}
