package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"github.com/apache/arrow-go/v18/arrow"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

var _ destination.SchemaApplier = (*writer)(nil)

// schemaRecord is a table's columns and their types, as its files have them.
type schemaRecord struct {
	Columns []schemaColumn `json:"columns"`
}

type schemaColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// schemaKey is where a table's schema record is: no file says what the
// table's columns are, and readers take one type a column.
func schemaKey(table string) string { return reserved + "/schemas/" + table + ".json" }

// ApplySchema records each table's columns, adding new ones, and refuses to
// change a column's type: a person takes such a change by removing the
// record, as the refusal says.
func (w *writer) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (out destination.SchemaApplied, err error) {
	defer func() { err = classify(err) }()
	for _, s := range streams {
		table := w.table(s.Schema, s.Name)
		key := schemaKey(table)
		var rec schemaRecord
		b, err := w.b.ReadAll(ctx, key)
		switch {
		case gcerrors.Code(err) == gcerrors.NotFound:
		case err != nil:
			return out, fmt.Errorf("s3: read %s: %w", key, err)
		default:
			if err := json.Unmarshal(b, &rec); err != nil {
				return out, fmt.Errorf("s3: %s: %w", key, err)
			}
		}
		have := map[string]string{}
		for _, c := range rec.Columns {
			have[c.Name] = c.Type
		}
		added := false
		for _, f := range s.Schema.Fields() {
			typ := typeName(f.Type)
			was, ok := have[f.Name]
			switch {
			case !ok:
				rec.Columns = append(rec.Columns, schemaColumn{Name: f.Name, Type: typ})
				have[f.Name], added = typ, true
			case was != typ:
				out.Refused = append(out.Refused, destination.RefusedColumn{
					Stream: s.Name, Column: f.Name, FromType: was, ToType: typ,
					Reason: "it doesn't change a column's type, which its files' readers take one of",
					Fix:    "remove " + w.d.location(key) + ", which records " + table + "'s columns",
				})
			}
		}
		if !added {
			continue
		}
		b, err = json.Marshal(rec)
		if err != nil {
			return out, err
		}
		if err := w.b.WriteAll(ctx, key, b, &blob.WriterOptions{ContentType: "application/json"}); err != nil {
			return out, fmt.Errorf("s3: write %s: %w", key, err)
		}
	}
	return out, nil
}

// typeName is t's name, with a struct's and a list's fields by name, so
// types that only differ in their fields' metadata are the same.
func typeName(t arrow.DataType) string {
	switch t := t.(type) {
	case *arrow.StructType:
		s := "struct<"
		for i, f := range t.Fields() {
			if i > 0 {
				s += ", "
			}
			s += f.Name + ": " + typeName(f.Type)
		}
		return s + ">"
	case arrow.ListLikeType:
		return "list<" + typeName(t.Elem()) + ">"
	}
	return t.String()
}

// location is key as the destination's URL names it.
func (d *Destination) location(key string) string {
	u := *d.u
	u.Path = path.Join(u.Path, key)
	return u.String()
}
