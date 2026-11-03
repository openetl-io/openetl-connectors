//go:build integration

package s3

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocloud.dev/blob"

	"github.com/unsuitable001/openetl/sdk/go/conformance"
)

func TestDestinationConformance(t *testing.T) {
	exe := conformance.Build(t, "../cmd/destination-s3")
	t.Run("file", func(t *testing.T) {
		conformanceSuite(t, exe, map[string]any{"url": "file://" + t.TempDir()}, nil).Test(t)
	})
	t.Run("s3", func(t *testing.T) {
		endpoint := cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, endpoint+"/s3-destination", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("create the bucket (is the S3 store running? make deps-up): %v", err)
		}
		_ = res.Body.Close()
		cfg := map[string]any{
			"url": "s3://s3-destination/" + strings.ToLower(rand.Text())[:10], "endpoint": endpoint, "region": "us-east-1",
			"accessKeyId": "openetl", "secretAccessKey": "openetl",
		}
		unreachable := maps.Clone(cfg)
		unreachable["endpoint"] = "http://127.0.0.1:1"
		conformanceSuite(t, exe, cfg, unreachable).Test(t)
	})
}

func conformanceSuite(t *testing.T, exe string, cfg, unreachable map[string]any) conformance.DestinationSuite {
	t.Helper()
	b, _ := json.Marshal(cfg)
	d, err := NewDestination(b)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := d.bucket(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	s := conformance.DestinationSuite{
		Launch: conformance.Exec(exe),
		Config: b,
		Read: func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
			return readTable(ctx, bucket, namespace+"/"+stream+"/")
		},
	}
	if unreachable != nil {
		s.Unreachable, _ = json.Marshal(unreachable)
	}
	return s
}

// readTable reads the rows of the Parquet files under prefix.
func readTable(ctx context.Context, b *blob.Bucket, prefix string) ([]map[string]any, error) {
	var out []map[string]any
	it := b.List(&blob.ListOptions{Prefix: prefix})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		data, err := b.ReadAll(ctx, obj.Key)
		if err != nil {
			return nil, err
		}
		pf, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1 << 16}, memory.DefaultAllocator)
		if err != nil {
			return nil, err
		}
		tbl, err := fr.ReadTable(ctx)
		if err != nil {
			return nil, err
		}
		tr := array.NewTableReader(tbl, -1)
		for tr.Next() {
			rec := tr.RecordBatch()
			for i := range int(rec.NumRows()) {
				row := map[string]any{}
				for c, f := range rec.Schema().Fields() {
					row[f.Name] = rec.Column(c).GetOneForMarshal(i)
				}
				out = append(out, row)
			}
		}
		tr.Release()
		tbl.Release()
		_ = pf.Close()
	}
}
