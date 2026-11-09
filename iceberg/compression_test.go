//go:build integration

package iceberg

import (
	"maps"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	ice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// codecs returns the compression of every column chunk in the table's files.
func (e *env) codecs(name string) map[compress.Compression]bool {
	e.t.Helper()
	tbl := e.table(name)
	fs, err := tbl.FS(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	tasks, err := tbl.Scan().PlanFiles(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	found := map[compress.Compression]bool{}
	for _, task := range tasks {
		f, err := fs.Open(task.File.FilePath())
		if err != nil {
			e.t.Fatal(err)
		}
		r, err := file.NewParquetReader(f)
		if err != nil {
			_ = f.Close()
			e.t.Fatal(err)
		}
		for i := range r.NumRowGroups() {
			rg := r.MetaData().RowGroup(i)
			for c := range rg.NumColumns() {
				chunk, err := rg.ColumnChunk(c)
				if err != nil {
					_ = r.Close()
					e.t.Fatal(err)
				}
				found[chunk.Compression()] = true
			}
		}
		if err := r.Close(); err != nil {
			e.t.Fatal(err)
		}
	}
	return found
}

func writeOneEpoch(t *testing.T, env *env, stream string) {
	t.Helper()
	w := env.open(1, destination.ScopeStream, appends(stream)...)
	recov(t, w, 1, destination.Epoch{}, nil)
	e := destination.Epoch{Generation: 1, N: 1}
	write(t, w, e, stream, 1, 2)
	commit(t, w, e, precommit(t, w, e))
	env.wantIDs(stream, 1, 2)
}

func TestANewTableUsesZstdLevel1(t *testing.T) {
	env := newEnv(t)
	writeOneEpoch(t, env, "users")

	props := env.table("users").Properties()
	if got := props[table.ParquetCompressionKey]; got != "zstd" {
		t.Errorf("codec property: %q", got)
	}
	if got := props[table.ParquetCompressionLevelKey]; got != "1" {
		t.Errorf("level property: %q", got)
	}
	want := map[compress.Compression]bool{compress.Codecs.Zstd: true}
	if got := env.codecs("users"); !maps.Equal(got, want) {
		t.Errorf("files' codecs: %v", got)
	}
}

func TestAnExistingTableKeepsItsCompression(t *testing.T) {
	env := newEnv(t)
	c, err := env.d.catalog(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateNamespace(t.Context(), table.Identifier{env.ns}, nil); err != nil {
		t.Fatal(err)
	}
	sc := ice.NewSchema(0,
		ice.NestedField{ID: 1, Name: "id", Type: ice.PrimitiveTypes.Int64},
		ice.NestedField{ID: 2, Name: "name", Type: ice.PrimitiveTypes.String})
	ident := table.Identifier{env.ns, "users"}
	if _, err := c.CreateTable(t.Context(), ident, sc, catalog.WithProperties(ice.Properties{
		table.ParquetCompressionKey: "snappy",
	})); err != nil {
		t.Fatal(err)
	}

	writeOneEpoch(t, env, "users")

	props := env.table("users").Properties()
	if got := props[table.ParquetCompressionKey]; got != "snappy" {
		t.Errorf("codec property: %q", got)
	}
	if got, ok := props[table.ParquetCompressionLevelKey]; ok {
		t.Errorf("level property set on an existing table: %q", got)
	}
	want := map[compress.Compression]bool{compress.Codecs.Snappy: true}
	if got := env.codecs("users"); !maps.Equal(got, want) {
		t.Errorf("files' codecs: %v", got)
	}
}
