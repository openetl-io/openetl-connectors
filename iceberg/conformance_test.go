//go:build integration

package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"testing"

	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl/sdk/go/conformance"
)

func TestDestinationConformance(t *testing.T) {
	env := newEnv(t)
	cat, err := env.d.catalog(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cfg := map[string]string{"uri": uri(), "s3Endpoint": s3(), "region": "us-east-1", "accessKeyId": "openetl", "secretAccessKey": "openetl"}
	unreachable := maps.Clone(cfg)
	unreachable["uri"] = "http://127.0.0.1:1"
	config, _ := json.Marshal(cfg)
	closed, _ := json.Marshal(unreachable)
	conformance.DestinationSuite{
		Launch:      conformance.Exec(conformance.Build(t, "../cmd/destination-iceberg")),
		Config:      config,
		Unreachable: closed,
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
