package iceberg

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdk "github.com/openetl-io/openetl/sdk/go"
)

// restCatalog serves a REST catalog's config to a request whose
// Authorization header auth accepts, and tokens at /idp/token and the
// catalog's own endpoint, recording where and for what scope and audience.
func restCatalog(t *testing.T, auth func(string) bool) (uri string, tokens *[]string) {
	tokens = new([]string)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/idp/token", "/v1/oauth/tokens":
			_ = r.ParseForm()
			*tokens = append(*tokens, r.URL.Path+" "+r.Form.Get("scope")+" "+r.Form.Get("audience"))
			_, _ = fmt.Fprint(w, `{"access_token": "t0k", "token_type": "Bearer", "expires_in": 3600}`)
		case "/v1/config":
			if !auth(r.Header.Get("Authorization")) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = fmt.Fprint(w, `{"error": {"message": "not signed in", "type": "NotAuthorizedException", "code": 401}}`)
				return
			}
			_, _ = fmt.Fprint(w, `{"defaults": {}, "overrides": {}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, tokens
}

// The first request a REST catalog gets, for its config, signs in as the
// properties say: a catalog behind an identity provider of its own, such as
// Polaris with Keycloak, takes a token only from that provider, for the
// scope it names, and AWS's REST endpoints take only signed requests.
func TestRESTSignInTakesItsProperties(t *testing.T) {
	t.Run("oauth2", func(t *testing.T) {
		uri, tokens := restCatalog(t, func(h string) bool { return h == "Bearer t0k" })
		cfg := defaultCatalogConfig()
		cfg.URI, cfg.Credential = uri, sdk.NewSecret("id:secret")
		cfg.Properties = map[string]string{"oauth2-server-uri": uri + "/idp/token", "scope": "PRINCIPAL_ROLE:ALL", "audience": "lake"}
		c, err := newConn(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.open(t.Context()); err != nil {
			t.Fatalf("open: %v", err)
		}
		if want := "/idp/token PRINCIPAL_ROLE:ALL lake"; strings.Join(*tokens, ", ") != want {
			t.Errorf("tokens were asked for as %q; want only %q", *tokens, want)
		}
	})
	t.Run("sigv4", func(t *testing.T) {
		uri, _ := restCatalog(t, func(h string) bool {
			return strings.HasPrefix(h, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/") && strings.Contains(h, "/eu-west-1/glue/aws4_request")
		})
		cfg := defaultCatalogConfig()
		cfg.URI, cfg.Region = uri, "eu-west-1"
		cfg.AccessKeyID, cfg.SecretAccessKey = sdk.NewSecret("AKIDEXAMPLE"), sdk.NewSecret("secret")
		cfg.Properties = map[string]string{"rest.sigv4-enabled": "true", "rest.signing-name": "glue"}
		c, err := newConn(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.open(t.Context()); err != nil {
			t.Fatalf("open: %v", err)
		}
	})
}
