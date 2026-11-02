package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/bufbuild/protocompile"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	sdk "github.com/unsuitable001/openetl/sdk/go"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// rowEncoder encodes one part of a row, its key or its value, as an object
// of cols in a format.
type rowEncoder interface {
	encode(vals []any) ([]byte, error)
}

var confluent sr.ConfluentHeader

// An Avro string holding JSON has this property set to "json", so a reader
// can declare it json.
const jsonProp = "openetl.type"

// newRowEncoder builds the encoder for cols, registering its schema under
// subject for the formats that need one. name names the record or message.
func newRowEncoder(ctx context.Context, format string, reg *sr.Client, subject, name string, cols []col) (rowEncoder, error) {
	switch format {
	case formatJSON:
		return jsonEncoder(cols), nil
	case formatAvro:
		return newAvroEncoder(ctx, reg, subject, name, cols)
	case formatProtobuf:
		return newProtoEncoder(ctx, reg, subject, name, cols)
	}
	return nil, fmt.Errorf("kafka: format %q can't be written", format)
}

type jsonEncoder []col

func (e jsonEncoder) encode(vals []any) ([]byte, error) { return appendJSONObject(nil, e, vals) }

// register registers s under subject and returns its ID. A schema the
// registry's compatibility setting refuses is SCHEMA_DRIFT.
func register(ctx context.Context, reg *sr.Client, subject string, s sr.Schema) (int, error) {
	ss, err := reg.CreateSchema(ctx, subject, s)
	if err != nil {
		var re *sr.ResponseError
		if errors.As(err, &re) && (re.StatusCode == http.StatusConflict || re.ErrorCode == 409) {
			return 0, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT, fmt.Errorf("kafka: the schema registry refuses the new schema of %s: %w", subject, err))
		}
		return 0, fmt.Errorf("kafka: register %s: %w", subject, err)
	}
	return ss.ID, nil
}

// avroName makes s an Avro or Protobuf name: letters, digits and _, not
// starting with a digit.
func avroName(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" || (out[0] >= '0' && out[0] <= '9') {
		out = "_" + out
	}
	return out
}

type avroEncoder struct {
	id     int
	schema *avro.Schema
	cols   []col
}

func newAvroEncoder(ctx context.Context, reg *sr.Client, subject, name string, cols []col) (*avroEncoder, error) {
	fields := make([]any, len(cols))
	for i := range cols {
		t, err := avroType(&cols[i], name+"_"+cols[i].name)
		if err != nil {
			return nil, err
		}
		fields[i] = map[string]any{"name": cols[i].name, "type": nullable(t), "default": nil}
	}
	text, err := json.Marshal(map[string]any{"type": "record", "name": avroName(name), "fields": fields})
	if err != nil {
		return nil, err
	}
	schema, err := avro.Parse(string(text))
	if err != nil {
		return nil, fmt.Errorf("kafka: Avro schema of %s: %w", subject, err)
	}
	id, err := register(ctx, reg, subject, sr.Schema{Schema: string(text), Type: sr.TypeAvro})
	if err != nil {
		return nil, err
	}
	return &avroEncoder{id: id, schema: schema, cols: cols}, nil
}

func nullable(t any) any {
	if t == "null" {
		return t
	}
	return []any{"null", t}
}

// avroType is c's Avro type; name names the records it defines.
func avroType(c *col, name string) (any, error) {
	switch c.kind {
	case kNull:
		return "null", nil
	case kBool:
		return "boolean", nil
	case kInt32:
		return "int", nil
	case kInt64, kUint64:
		return "long", nil
	case kFloat32:
		return "float", nil
	case kFloat64:
		return "double", nil
	case kString:
		return "string", nil
	case kJSON:
		return map[string]any{"type": "string", jsonProp: "json"}, nil
	case kBytes:
		return "bytes", nil
	case kDecimal:
		return map[string]any{"type": "bytes", "logicalType": "decimal", "precision": c.prec, "scale": c.scale}, nil
	case kDate:
		return map[string]any{"type": "int", "logicalType": "date"}, nil
	case kTime:
		return map[string]any{"type": "long", "logicalType": "time-micros"}, nil
	case kTimestamp:
		lt := "timestamp-"
		if !c.zoned {
			lt = "local-timestamp-"
		}
		if c.nanos {
			lt += "nanos"
		} else {
			lt += "micros"
		}
		return map[string]any{"type": "long", "logicalType": lt}, nil
	case kUUID:
		return map[string]any{"type": "string", "logicalType": "uuid"}, nil
	case kInterval:
		return map[string]any{"type": "record", "name": avroName(name), "fields": []any{
			map[string]any{"name": "months", "type": "int"},
			map[string]any{"name": "days", "type": "int"},
			map[string]any{"name": "nanoseconds", "type": "long"},
		}}, nil
	case kList:
		t, err := avroType(c.elem, name+"_item")
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": nullable(t)}, nil
	case kStruct:
		fields := make([]any, len(c.fields))
		for i := range c.fields {
			t, err := avroType(&c.fields[i], name+"_"+c.fields[i].name)
			if err != nil {
				return nil, err
			}
			fields[i] = map[string]any{"name": c.fields[i].name, "type": nullable(t), "default": nil}
		}
		return map[string]any{"type": "record", "name": avroName(name), "fields": fields}, nil
	}
	return nil, fmt.Errorf("column %q: no Avro type", c.name)
}

func (e *avroEncoder) encode(vals []any) ([]byte, error) {
	rec, err := avroRecord(e.cols, vals)
	if err != nil {
		return nil, err
	}
	b, _ := confluent.AppendEncode(nil, e.id, nil)
	return e.schema.AppendEncode(b, rec)
}

func avroRecord(cols []col, vals []any) (map[string]any, error) {
	rec := make(map[string]any, len(cols))
	for i := range cols {
		v, err := avroValue(&cols[i], vals[i])
		if err != nil {
			return nil, err
		}
		rec[cols[i].name] = v
	}
	return rec, nil
}

func avroValue(c *col, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch c.kind {
	case kUint64:
		u := v.(uint64)
		if u > math.MaxInt64 {
			return nil, fmt.Errorf("column %q: %d doesn't fit Avro's long", c.name, u)
		}
		return int64(u), nil
	case kDecimal:
		return v.(decimal).Rat(), nil
	case kJSON:
		return string(v.(json.RawMessage)), nil
	case kInterval:
		iv := v.(interval)
		return map[string]any{"months": iv.Months, "days": iv.Days, "nanoseconds": iv.Nanoseconds}, nil
	case kList:
		in := v.([]any)
		out := make([]any, len(in))
		for i, e := range in {
			var err error
			if out[i], err = avroValue(c.elem, e); err != nil {
				return nil, err
			}
		}
		return out, nil
	case kStruct:
		return avroRecord(c.fields, v.([]any))
	}
	return v, nil
}

type protoEncoder struct {
	id   int
	md   protoreflect.MessageDescriptor
	cols []col
}

// protoSchema is a proto3 file with one message, name, holding cols. Field
// numbers follow the columns' order, which only ever grows at the end.
func protoSchema(name string, cols []col) (string, error) {
	var b strings.Builder
	b.WriteString("syntax = \"proto3\";\n\npackage openetl;\n\nimport \"google/protobuf/timestamp.proto\";\n\n")
	if err := protoMessage(&b, avroName(name), cols, ""); err != nil {
		return "", err
	}
	return b.String(), nil
}

func protoMessage(b *strings.Builder, name string, cols []col, indent string) error {
	fmt.Fprintf(b, "%smessage %s {\n", indent, name)
	for i := range cols {
		c := &cols[i]
		t, nested, err := protoType(c, indent+"  ")
		if err != nil {
			return err
		}
		b.WriteString(nested)
		label := "optional "
		if c.kind == kList {
			label = "repeated "
		} else if strings.HasPrefix(t, "M_") || t == "google.protobuf.Timestamp" {
			label = ""
		}
		fmt.Fprintf(b, "%s  %s%s %s = %d;\n", indent, label, t, c.name, i+1)
	}
	fmt.Fprintf(b, "%s}\n", indent)
	return nil
}

// protoType is c's field type, and any message it defines for it.
func protoType(c *col, indent string) (string, string, error) {
	switch c.kind {
	case kBool:
		return "bool", "", nil
	case kInt32:
		return "int32", "", nil
	case kInt64:
		return "int64", "", nil
	case kUint64:
		return "uint64", "", nil
	case kFloat32:
		return "float", "", nil
	case kFloat64:
		return "double", "", nil
	case kBytes:
		return "bytes", "", nil
	case kNull, kString, kJSON, kUUID, kDecimal, kDate, kTime:
		return "string", "", nil
	case kTimestamp:
		return "google.protobuf.Timestamp", "", nil
	case kInterval:
		name := "M_" + c.name
		return name, fmt.Sprintf("%smessage %s {\n%s  int32 months = 1;\n%s  int32 days = 2;\n%s  int64 nanoseconds = 3;\n%s}\n", indent, name, indent, indent, indent, indent), nil
	case kList:
		if c.elem.kind == kList {
			return "", "", fmt.Errorf("column %q: Protobuf has no lists of lists", c.name)
		}
		e := *c.elem
		e.name = c.name
		return protoType(&e, indent)
	case kStruct:
		name := "M_" + c.name
		var b strings.Builder
		if err := protoMessage(&b, name, c.fields, indent); err != nil {
			return "", "", err
		}
		return name, b.String(), nil
	}
	return "", "", fmt.Errorf("column %q: no Protobuf type", c.name)
}

func compileProto(ctx context.Context, files map[string]string, root string) (protoreflect.FileDescriptor, error) {
	c := protocompile.Compiler{Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{Accessor: protocompile.SourceAccessorFromMap(files)})}
	fds, err := c.Compile(ctx, root)
	if err != nil {
		return nil, err
	}
	return fds[0], nil
}

func newProtoEncoder(ctx context.Context, reg *sr.Client, subject, name string, cols []col) (*protoEncoder, error) {
	text, err := protoSchema(name, cols)
	if err != nil {
		return nil, err
	}
	fd, err := compileProto(ctx, map[string]string{"schema.proto": text}, "schema.proto")
	if err != nil {
		return nil, fmt.Errorf("kafka: Protobuf schema of %s: %w", subject, err)
	}
	id, err := register(ctx, reg, subject, sr.Schema{Schema: text, Type: sr.TypeProtobuf})
	if err != nil {
		return nil, err
	}
	return &protoEncoder{id: id, md: fd.Messages().Get(0), cols: cols}, nil
}

func (e *protoEncoder) encode(vals []any) ([]byte, error) {
	m := dynamicpb.NewMessage(e.md)
	if err := protoFill(m, e.cols, vals); err != nil {
		return nil, err
	}
	b, _ := confluent.AppendEncode(nil, e.id, []int{0})
	return proto.MarshalOptions{Deterministic: true}.MarshalAppend(b, m)
}

func protoFill(m protoreflect.Message, cols []col, vals []any) error {
	fields := m.Descriptor().Fields()
	for i := range cols {
		if vals[i] == nil {
			continue
		}
		fd := fields.ByNumber(protoreflect.FieldNumber(i + 1))
		c := &cols[i]
		if c.kind == kList {
			l := m.Mutable(fd).List()
			for _, e := range vals[i].([]any) {
				if e == nil {
					return fmt.Errorf("column %q: Protobuf lists can't hold nulls", c.name)
				}
				v, err := protoValue(c.elem, e, l.NewElement)
				if err != nil {
					return err
				}
				l.Append(v)
			}
			continue
		}
		v, err := protoValue(c, vals[i], func() protoreflect.Value { return m.NewField(fd) })
		if err != nil {
			return err
		}
		m.Set(fd, v)
	}
	return nil
}

// protoValue is v as a field's value; newMsg makes the field's message for
// kinds that are one.
func protoValue(c *col, v any, newMsg func() protoreflect.Value) (protoreflect.Value, error) {
	switch c.kind {
	case kBool:
		return protoreflect.ValueOfBool(v.(bool)), nil
	case kInt32:
		return protoreflect.ValueOfInt32(v.(int32)), nil
	case kInt64:
		return protoreflect.ValueOfInt64(v.(int64)), nil
	case kUint64:
		return protoreflect.ValueOfUint64(v.(uint64)), nil
	case kFloat32:
		return protoreflect.ValueOfFloat32(v.(float32)), nil
	case kFloat64:
		return protoreflect.ValueOfFloat64(v.(float64)), nil
	case kString, kUUID:
		return protoreflect.ValueOfString(v.(string)), nil
	case kJSON:
		return protoreflect.ValueOfString(string(v.(json.RawMessage))), nil
	case kBytes:
		return protoreflect.ValueOfBytes(v.([]byte)), nil
	case kDecimal:
		return protoreflect.ValueOfString(v.(decimal).String()), nil
	case kDate:
		return protoreflect.ValueOfString(v.(time.Time).Format(time.DateOnly)), nil
	case kTime:
		return protoreflect.ValueOfString(timeOfDay(v.(time.Duration))), nil
	case kTimestamp:
		ts := timestamppb.New(v.(time.Time))
		mv := newMsg()
		mv.Message().Set(mv.Message().Descriptor().Fields().ByName("seconds"), protoreflect.ValueOfInt64(ts.GetSeconds()))
		mv.Message().Set(mv.Message().Descriptor().Fields().ByName("nanos"), protoreflect.ValueOfInt32(ts.GetNanos()))
		return mv, nil
	case kInterval:
		iv := v.(interval)
		mv := newMsg()
		f := mv.Message().Descriptor().Fields()
		mv.Message().Set(f.ByNumber(1), protoreflect.ValueOfInt32(iv.Months))
		mv.Message().Set(f.ByNumber(2), protoreflect.ValueOfInt32(iv.Days))
		mv.Message().Set(f.ByNumber(3), protoreflect.ValueOfInt64(iv.Nanoseconds))
		return mv, nil
	case kStruct:
		mv := newMsg()
		return mv, protoFill(mv.Message(), c.fields, v.([]any))
	}
	return protoreflect.Value{}, fmt.Errorf("column %q: no Protobuf value", c.name)
}
