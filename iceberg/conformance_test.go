//go:build integration

package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"

	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl/sdk/go/conformance"
)

// tokenProxy is a REST catalog in front of the one at target that answers
// 401 to a request without the bearer token.
func tokenProxy(t *testing.T, target, token string) string {
	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error": {"message": "Not authorized", "type": "NotAuthorizedException", "code": 401}}`))
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestDestinationConformance(t *testing.T) {
	env := newEnv(t)
	cat, err := env.d.catalog(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The REST fixture takes any token, and SeaweedFS any key: the suite's
	// catalog is a proxy that takes one token.
	auth := tokenProxy(t, uri(), "openetl")
	cfg := map[string]string{"uri": auth, "token": "openetl", "s3Endpoint": s3(), "region": "us-east-1", "accessKeyId": "openetl", "secretAccessKey": "openetl"}
	unreachable := maps.Clone(cfg)
	unreachable["uri"] = "http://127.0.0.1:1"
	wrong := maps.Clone(cfg)
	wrong["token"] = "wrong"
	config, _ := json.Marshal(cfg)
	closed, _ := json.Marshal(unreachable)
	bad, _ := json.Marshal(wrong)
	conformance.DestinationSuite{
		Launch:         conformance.Exec(conformance.Build(t, "../cmd/destination-iceberg")),
		Config:         config,
		BadCredentials: bad,
		Unreachable:    closed,
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			tbl, err := cat.LoadTable(ctx, table.Identifier{namespace, stream})
			if errors.Is(err, catalog.ErrNoSuchTable) || errors.Is(err, catalog.ErrNoSuchNamespace) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			_, it, err := tbl.Scan().ToArrowRecords(ctx)
			if err != nil {
				return nil, err
			}
			var out []map[string]any
			for b, err := range it {
				if err != nil {
					return nil, err
				}
				for r := range int(b.NumRows()) {
					row := map[string]any{}
					for j, f := range b.Schema().Fields() {
						if c := b.Column(j); !c.IsNull(r) {
							row[f.Name] = c.GetOneForMarshal(r)
						}
					}
					out = append(out, row)
				}
				b.Release()
			}
			return out, nil
		},
		Drop: func(ctx context.Context, namespace string) error {
			ns := table.Identifier{namespace}
			for ident, err := range cat.ListTables(ctx, ns) {
				if err != nil {
					break
				}
				_ = cat.DropTable(ctx, ident)
			}
			return cat.DropNamespace(ctx, ns)
		},
		NotNull: func(ctx context.Context, namespace, stream, column string) error {
			tbl, err := cat.LoadTable(ctx, table.Identifier{namespace, stream})
			if err != nil {
				return err
			}
			tx := tbl.NewTransaction()
			err = tx.UpdateSchema(true, true).UpdateColumn([]string{column}, table.ColumnUpdate{Required: ice.Optional[bool]{Valid: true, Val: true}}).Commit()
			if err != nil {
				return err
			}
			_, err = tx.Commit(ctx)
			return err
		},
	}.Test(t)
}
