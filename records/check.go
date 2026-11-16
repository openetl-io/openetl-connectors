package records

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/openetl-io/openetl/sdk/go/check"
)

// RegistryChecks reach the schema registry and sign in to it, each taking up
// to timeout; none without one.
func (c *Config) RegistryChecks(timeout time.Duration) []check.Check {
	if c.SchemaRegistryURL == "" {
		return nil
	}
	var answered error
	params := check.Params{"url": c.SchemaRegistryURL}
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
				return answered
			},
		},
	}
}
