package objstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"gocloud.dev/blob"

	"github.com/openetl-io/openetl-connectors/records"
	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// column is a typed format's column as discovery declares it and a read
// sends it.
type column struct {
	field arrow.Field
	// The stream's files have the column with types that differ, and field
	// is one every one of them fits: each file's is sent as text, which
	// OpenETL parses to the declared type, quarantining a value that
	// doesn't fit.
	text bool
}

// columns are a Parquet or Avro stream's columns, across its files.
type columns struct {
	order []string
	by    map[string]*column
}

func (c *columns) add(f arrow.Field) {
	if f.Name == fileColumn || f.Name == lineColumn || f.Name == modifiedColumn {
		return
	}
	if c.by == nil {
		c.by = map[string]*column{}
	}
	have, ok := c.by[f.Name]
	if !ok {
		c.by[f.Name] = &column{field: f}
		c.order = append(c.order, f.Name)
		return
	}
	if !sameType(have.field, f) {
		have.field, have.text = widen(have.field, f), true
	}
}

func (c *columns) fields() []arrow.Field {
	out := make([]arrow.Field, len(c.order))
	for i, n := range c.order {
		out[i] = c.by[n].field
	}
	return out
}

func extName(f arrow.Field) string {
	v, _ := f.Metadata.GetValue(records.ExtensionNameKey)
	return v
}

func sameType(a, b arrow.Field) bool {
	return arrow.TypeEqual(a.Type, b.Type) && extName(a) == extName(b)
}

// widen is a type that values of a and b both fit: the wider number, a
// timestamp for a date and a timestamp, and otherwise text.
func widen(a, b arrow.Field) arrow.Field {
	out := arrow.Field{Name: a.Name, Nullable: true, Type: arrow.BinaryTypes.String}
	if extName(a) != "" || extName(b) != "" {
		return out
	}
	if t := widenNumbers(a.Type, b.Type); t != nil {
		out.Type = t
		return out
	}
	for _, p := range [][2]arrow.DataType{{a.Type, b.Type}, {b.Type, a.Type}} {
		if p[0].ID() == arrow.DATE32 && p[1].ID() == arrow.TIMESTAMP {
			out.Type = p[1]
		}
	}
	return out
}

// widenNumbers is nil unless both are int64, float64 or decimal128.
func widenNumbers(a, b arrow.DataType) arrow.DataType {
	asDecimal := func(t arrow.DataType) (p, s int32, ok bool) {
		switch t := t.(type) {
		case *arrow.Int64Type:
			return 19, 0, true
		case *arrow.Decimal128Type:
			return t.Precision, t.Scale, true
		}
		return 0, 0, false
	}
	isFloat := func(t arrow.DataType) bool { return t.ID() == arrow.FLOAT64 }
	pa, sa, da := asDecimal(a)
	pb, sb, db := asDecimal(b)
	switch {
	case da && db:
		s := max(sa, sb)
		p := max(pa-sa, pb-sb) + s
		if p > 38 {
			return nil
		}
		return &arrow.Decimal128Type{Precision: p, Scale: s}
	case (da || isFloat(a)) && (db || isFloat(b)):
		return arrow.PrimitiveTypes.Float64
	}
	return nil
}

// textOf is a column, already of its file's logical type lf, as text that
// OpenETL parses to the declared type to.
func textOf(ctx context.Context, a arrow.Array, lf, to arrow.Field) (arrow.Array, error) {
	switch a.DataType().ID() {
	case arrow.STRING:
		a.Retain()
		return a, nil
	case arrow.LIST:
		return convert(ctx, a, lf, asJSON)
	}
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	day := ""
	if ts, ok := to.Type.(*arrow.TimestampType); ok && a.DataType().ID() == arrow.DATE32 {
		day = "2006-01-02T00:00:00"
		if ts.TimeZone != "" {
			day += "Z"
		}
	}
	for i := range a.Len() {
		switch {
		case a.IsNull(i):
			b.AppendNull()
		case day != "":
			b.Append(a.(*array.Date32).Value(i).ToTime().Format(day))
		default:
			b.Append(a.ValueStr(i))
		}
	}
	return b.NewArray(), nil
}

// How many files' footers or headers a stream's schema reads at a time.
const schemaReads = 16

// schemaOf is a Parquet or Avro stream's columns, from every file's footer
// or header.
func (s *streamSpec) schemaOf(ctx context.Context, b *blob.Bucket, files []object) (*columns, error) {
	read := parquetFields
	if s.cfg.Format == formatAvro {
		read = avroFields
	}
	got := make([][]arrow.Field, len(files))
	errs := make([]error, len(files))
	var wg sync.WaitGroup
	sem := make(chan struct{}, schemaReads)
	for i, o := range files {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			got[i], errs[i] = read(ctx, b, o)
		})
	}
	wg.Wait()
	c := &columns{}
	for i := range files {
		if errs[i] != nil {
			return nil, errs[i]
		}
		for _, f := range got[i] {
			c.add(f)
		}
	}
	// A type the config declares replaces the files', as text.
	for _, d := range s.declared {
		if _, ok := c.by[d.Name]; !ok {
			c.add(d)
		}
		c.by[d.Name].field, c.by[d.Name].text = d, true
	}
	return c, nil
}

// parquetFields are a Parquet file's columns, of their logical types.
func parquetFields(ctx context.Context, b *blob.Bucket, o object) ([]arrow.Field, error) {
	sc, err := parquetSchema(ctx, b, o)
	if err != nil {
		return nil, err
	}
	out := make([]arrow.Field, sc.NumFields())
	for i, f := range sc.Fields() {
		out[i], _ = logical(f)
	}
	return out, nil
}

// typedColumns reads, once per Read, the columns of each typed stream it
// reads, which its splits share.
type typedColumns struct {
	mu   sync.Mutex
	by   map[string]*columns
	errs map[string]error
	once map[string]*sync.Once
}

func (t *typedColumns) of(ctx context.Context, b *blob.Bucket, spec *streamSpec) (*columns, error) {
	t.mu.Lock()
	if t.once == nil {
		t.once, t.by, t.errs = map[string]*sync.Once{}, map[string]*columns{}, map[string]error{}
	}
	once, ok := t.once[spec.cfg.Name]
	if !ok {
		once = &sync.Once{}
		t.once[spec.cfg.Name] = once
	}
	t.mu.Unlock()
	once.Do(func() {
		files, err := spec.list(ctx, b, wholeRange, false)
		var c *columns
		if err == nil {
			c, err = spec.schemaOf(ctx, b, files)
		}
		t.mu.Lock()
		t.by[spec.cfg.Name], t.errs[spec.cfg.Name] = c, err
		t.mu.Unlock()
	})
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.by[spec.cfg.Name], t.errs[spec.cfg.Name]
}

// unplanned is the error of a file whose column the read's schema doesn't
// have, or has with another type: one written since the read started,
// which the next attempt reads.
func unplanned(key, column string) error {
	return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT,
		fmt.Errorf("objstore: %s has column %q of a type the stream's other files don't, and was written after the read started: the next attempt reads it", key, column))
}
