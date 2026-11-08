package iceberg

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table"
)

func TestIcebergTypes(t *testing.T) {
	ext := func(name string) arrow.Metadata { return arrow.NewMetadata([]string{extensionNameKey}, []string{name}) }
	for _, c := range []struct {
		field arrow.Field
		want  string
	}{
		{arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int8}, "int"},
		{arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Uint32}, "long"},
		{arrow.Field{Name: "a", Type: arrow.Null}, "string"},
		{arrow.Field{Name: "a", Type: arrow.BinaryTypes.String, Metadata: ext("arrow.json")}, "string"},
		{arrow.Field{Name: "a", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: ext(uuidExtension)}, "uuid"},
		{arrow.Field{Name: "a", Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}}, "fixed[8]"},
		{arrow.Field{Name: "a", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Europe/Paris"}}, "timestamptz"},
		{arrow.Field{Name: "a", Type: arrow.FixedWidthTypes.Time64ns}, "time"},
		{arrow.Field{Name: "a", Type: arrow.ListOf(arrow.PrimitiveTypes.Int16)}, "list<int>"},
		{arrow.Field{Name: "a", Type: arrow.FixedWidthTypes.MonthDayNanoInterval}, "struct<2: months: optional int, 3: days: optional int, 4: nanoseconds: optional long>"},
	} {
		sc, err := icebergSchema(arrow.NewSchema([]arrow.Field{c.field}, nil))
		if err != nil {
			t.Errorf("%s: %v", c.field.Type, err)
			continue
		}
		if f := sc.Field(0); f.Type.String() != c.want || f.Required {
			t.Errorf("%s: %s (required %v), want optional %s", c.field.Type, f.Type, f.Required, c.want)
		}
	}
	for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Uint64} {
		if _, err := icebergSchema(arrow.NewSchema([]arrow.Field{{Name: "a", Type: dt}}, nil)); err == nil {
			t.Errorf("%s: no error", dt)
		}
	}
}

func TestConformCastsAndFills(t *testing.T) {
	target, err := icebergSchema(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "name", Type: arrow.BinaryTypes.String},
	}, nil))
	if err != nil {
		t.Fatal(err)
	}
	sc, err := table.SchemaToArrowSchema(target, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	in, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
	}, nil), strings.NewReader(`[{"id": 1}, {"id": null}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer in.Release()
	out, err := conform(t.Context(), in, sc)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if !out.Schema().Equal(sc) || out.NumRows() != 2 {
		t.Fatalf("schema %s, %d rows", out.Schema(), out.NumRows())
	}
	ids := out.Column(0).(*array.Int64)
	if ids.Value(0) != 1 || !ids.IsNull(1) || out.Column(1).NullN() != 2 {
		t.Errorf("columns %v %v", out.Column(0), out.Column(1))
	}

	extra, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "other", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil), strings.NewReader(`[{"other": 1}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Release()
	if _, err := conform(t.Context(), extra, sc); err == nil {
		t.Error("a column the table lacks: no error")
	}
}

func TestApplied(t *testing.T) {
	desc := staged{Generation: 3, Epoch: 7}
	for v, want := range map[string]bool{
		"":     false,
		"6:3":  false,
		"7:3":  true,
		"7:2":  false, // epoch 7 of an earlier generation, which was lost
		"8:4":  true,
		"junk": false,
	} {
		props := map[string]string{}
		if v != "" {
			props[loadedKey("p")] = v
		}
		if got := applied(props, "p", desc); got != want {
			t.Errorf("%q: %v, want %v", v, got, want)
		}
	}
}
