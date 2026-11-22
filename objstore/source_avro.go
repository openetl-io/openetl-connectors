package objstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
	"gocloud.dev/blob"

	"github.com/openetl-io/openetl-connectors/records"
)

var uuidMeta = arrow.NewMetadata([]string{records.ExtensionNameKey}, []string{records.UUIDExtension})

// openAvro opens an Avro object container file, decompressed as its name
// says, at its first record.
func openAvro(ctx context.Context, b *blob.Bucket, o object) (*ocf.Reader, func(), error) {
	raw, err := open(ctx, b, o, 0)
	if err != nil {
		return nil, nil, err
	}
	compression, _ := compressionOf(o.Key)
	dec, err := decompress(compression, raw)
	if err != nil {
		_ = raw.Close()
		return nil, nil, fmt.Errorf("objstore: %s: %w", o.Key, err)
	}
	r, err := ocf.NewReader(dec)
	if err != nil {
		_ = dec.Close()
		_ = raw.Close()
		return nil, nil, fmt.Errorf("objstore: %s: %w", o.Key, err)
	}
	return r, func() { _ = r.Close(); _ = dec.Close(); _ = raw.Close() }, nil
}

// avroRecord is the fields of an Avro file's records, which must be a
// record. A named type that a field names again is its definition there too,
// but for a recursive one's reference to itself.
func avroRecord(key string, s *avro.Schema) ([]avro.SchemaField, error) {
	root := s.Root().ExpandReferences()
	if root.Type != "record" {
		return nil, fmt.Errorf("objstore: %s holds %s values, not records", key, root.Type)
	}
	return root.Fields, nil
}

// avroFields are an Avro file's columns, of their logical types, from its
// header.
func avroFields(ctx context.Context, b *blob.Bucket, o object) ([]arrow.Field, error) {
	r, closer, err := openAvro(ctx, b, o)
	if err != nil {
		return nil, err
	}
	defer closer()
	fs, err := avroRecord(o.Key, r.Schema())
	if err != nil {
		return nil, err
	}
	out := make([]arrow.Field, len(fs))
	for i, f := range fs {
		out[i] = avroField(f.Name, &fs[i].Type)
	}
	return out, nil
}

// nonNull is a union's branches but null.
func nonNull(n *avro.SchemaNode) []*avro.SchemaNode {
	var out []*avro.SchemaNode
	for i := range n.Branches {
		if n.Branches[i].Type != "null" {
			out = append(out, &n.Branches[i])
		}
	}
	return out
}

// avroField is the logical type of an Avro type: records, arrays, maps and
// unions of more than one type besides null are JSON.
func avroField(name string, n *avro.SchemaNode) arrow.Field {
	f := arrow.Field{Name: name, Nullable: true, Type: arrow.BinaryTypes.String}
	if n.Type == "union" {
		if b := nonNull(n); len(b) == 1 {
			return avroField(name, b[0])
		}
		f.Metadata = jsonMeta
		return f
	}
	switch n.LogicalType {
	case "date":
		f.Type = arrow.FixedWidthTypes.Date32
		return f
	case "time-millis", "time-micros":
		f.Type = arrow.FixedWidthTypes.Time64us
		return f
	case "timestamp-millis", "timestamp-micros", "timestamp-nanos":
		f.Type = utcMicros
		return f
	case "local-timestamp-millis", "local-timestamp-micros", "local-timestamp-nanos":
		f.Type = localMicros
		return f
	case "decimal":
		if n.Precision <= 38 {
			f.Type = &arrow.Decimal128Type{Precision: int32(n.Precision), Scale: int32(n.Scale)}
		}
		return f
	case "uuid":
		f.Type, f.Metadata = &arrow.FixedSizeBinaryType{ByteWidth: 16}, uuidMeta
		return f
	case "duration":
		if n.Type == "fixed" && n.Size == 12 {
			f.Metadata = jsonMeta
			return f
		}
	}
	switch n.Type {
	case "boolean":
		f.Type = arrow.FixedWidthTypes.Boolean
	case "int", "long":
		f.Type = arrow.PrimitiveTypes.Int64
	case "float", "double":
		f.Type = arrow.PrimitiveTypes.Float64
	case "string", "enum":
	case "bytes", "fixed":
		f.Type = arrow.BinaryTypes.Binary
	default:
		f.Metadata = jsonMeta
	}
	return f
}

// avroJSON is a decoded Avro value as JSON that OpenETL takes to the
// column's declared type, to: dates and times as text, decimals at their
// scale, and bytes as base64.
func avroJSON(n *avro.SchemaNode, v any, to arrow.Field) (any, error) {
	if v == nil {
		return nil, nil
	}
	val, err := avroTyped(n, v, to)
	if err != nil {
		return nil, err
	}
	// A JSON column takes the value itself, and a string column of
	// another type's values their JSON text.
	own := avroField("", n)
	text := extName(to) == "" && to.Type.ID() == arrow.STRING && (own.Type.ID() != arrow.STRING || extName(own) != "")
	if !text {
		return val, nil
	}
	if s, ok := val.(string); ok {
		return s, nil
	}
	j, err := json.Marshal(val)
	return string(j), err
}

// avroTyped is a decoded Avro value as JSON of its own type.
func avroTyped(n *avro.SchemaNode, v any, to arrow.Field) (any, error) {
	if n.Type == "union" {
		if b := nonNull(n); len(b) == 1 {
			return avroTyped(b[0], v, to)
		}
		return generic(v), nil
	}
	switch n.LogicalType {
	case "date":
		t := v.(time.Time).UTC()
		if _, ts := to.Type.(*arrow.TimestampType); ts {
			return t.Format("2006-01-02T00:00:00") + zone(to), nil
		}
		return t.Format(time.DateOnly), nil
	case "time-millis", "time-micros":
		d := v.(time.Duration)
		return time.Time{}.Add(d).Format("15:04:05.000000"), nil
	case "timestamp-millis", "timestamp-micros", "timestamp-nanos":
		return v.(time.Time).UTC().Format(time.RFC3339Nano), nil
	case "local-timestamp-millis", "local-timestamp-micros", "local-timestamp-nanos":
		return v.(time.Time).UTC().Format("2006-01-02T15:04:05.999999999"), nil
	case "decimal":
		if r, ok := v.(*big.Rat); ok {
			return r.FloatString(n.Scale), nil
		}
	case "uuid":
		switch u := v.(type) {
		case string:
			return u, nil
		case [16]byte:
			return uuid.UUID(u).String(), nil
		}
	}
	return generic(v), nil
}

func zone(f arrow.Field) string {
	if ts, ok := f.Type.(*arrow.TimestampType); ok && ts.TimeZone != "" {
		return "Z"
	}
	return ""
}

// generic is a decoded Avro value as JSON can hold it.
func generic(v any) any {
	switch v := v.(type) {
	case nil, bool, int32, int64, string:
		return v
	case float32:
		return generic(float64(v))
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return strconv.FormatFloat(v, 'g', -1, 64)
		}
		return v
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case time.Duration:
		return time.Time{}.Add(v).Format("15:04:05.000000")
	case avro.Duration:
		return map[string]any{"months": v.Months, "days": v.Days, "milliseconds": v.Milliseconds}
	case *big.Rat:
		// A decimal where its scale isn't at hand, nested in a JSON value:
		// a whole number over a power of ten, written exactly.
		if n, exact := v.FloatPrec(); exact {
			return v.FloatString(n)
		}
		return v.FloatString(18)
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = generic(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, e := range v {
			out[k] = generic(e)
		}
		return out
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Array && rv.Type().Elem().Kind() == reflect.Uint8 {
		b := make([]byte, rv.Len())
		reflect.Copy(reflect.ValueOf(b), rv)
		return base64.StdEncoding.EncodeToString(b)
	}
	if rv.Kind() == reflect.Struct || rv.Kind() == reflect.Array {
		return fmt.Sprint(v)
	}
	return v
}

// readAvro reads an Avro file's records from at, as JSON rows. A read
// resumes in a file by decoding the records before its position again: the
// library doesn't give blocks' offsets.
func (s *Source) readAvro(ctx context.Context, b *blob.Bucket, o object, at filePos, declared map[string]bool, cols *columns, out sink) error {
	r, closer, err := openAvro(ctx, b, o)
	if err != nil {
		return err
	}
	defer closer()
	fs, err := avroRecord(o.Key, r.Schema())
	if err != nil {
		return err
	}
	stream := make([]*column, len(fs))
	for i, f := range fs {
		if controlColumn(f.Name) {
			// The source's own takes its place.
			continue
		}
		if declared != nil && !declared[f.Name] {
			return undeclared(o.Key, f.Name)
		}
		c := cols.by[f.Name]
		if c == nil || !c.text && !sameType(c.field, avroField(f.Name, &fs[i].Type)) {
			return unplanned(o.Key, f.Name)
		}
		stream[i] = c
	}
	location, _ := json.Marshal(s.store.location(o.Key))
	modified, _ := json.Marshal(o.Modified.UTC().Format(time.RFC3339Nano))
	line := int64(1)
	next := func() (map[string]any, error) {
		var v any
		if err := r.Decode(&v); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("objstore: %s: record %d: %w", o.Key, line, err)
		}
		m, _ := v.(map[string]any)
		return m, nil
	}
	for ; line <= at.Records; line++ {
		if _, err := next(); err != nil {
			return err
		}
	}
	rec, err := next()
	for !errors.Is(err, io.EOF) {
		var rows []json.RawMessage
		size := 0
		for len(rows) < batchRecords && size < batchBytes && !errors.Is(err, io.EOF) {
			if err != nil {
				return err
			}
			obj := make(map[string]json.RawMessage, len(fs)+3)
			for i, f := range fs {
				if stream[i] == nil {
					continue
				}
				v, verr := avroJSON(&fs[i].Type, rec[f.Name], stream[i].field)
				if verr != nil {
					return fmt.Errorf("objstore: %s: record %d: field %q: %w", o.Key, line, f.Name, verr)
				}
				if obj[f.Name], verr = json.Marshal(v); verr != nil {
					return fmt.Errorf("objstore: %s: record %d: field %q: %w", o.Key, line, f.Name, verr)
				}
			}
			obj[fileColumn], obj[lineColumn], obj[modifiedColumn] = location, json.RawMessage(strconv.FormatInt(line, 10)), modified
			row, merr := json.Marshal(obj)
			if merr != nil {
				return merr
			}
			rows = append(rows, row)
			size += len(row)
			line++
			rec, err = next()
		}
		if err := out.rows(rows); err != nil {
			return err
		}
		if !errors.Is(err, io.EOF) {
			if err := out.resume(filePos{Records: line - 1}); err != nil {
				return err
			}
		}
	}
	return nil
}
