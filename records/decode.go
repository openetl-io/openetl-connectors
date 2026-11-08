package records

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Decoder turns a record's value or key into JSON: an object's fields, or
// any other JSON value.
type Decoder struct {
	format string
	reg    *sr.Client

	mu     sync.Mutex
	avros  map[int]*avroSchema
	protos map[int]protoreflect.FileDescriptor
}

type avroSchema struct {
	schema *avro.Schema
	root   *avro.SchemaNode
}

func NewDecoder(format string, reg *sr.Client) *Decoder {
	return &Decoder{format: format, reg: reg, avros: map[int]*avroSchema{}, protos: map[int]protoreflect.FileDescriptor{}}
}

var errNotRegistry = errors.New("not in the schema registry's wire format")

// decode returns b as JSON. A string format, or a key that isn't in the
// value's format, gives a JSON string.
func (d *Decoder) Decode(ctx context.Context, b []byte) (json.RawMessage, error) {
	switch d.format {
	case JSON:
		b = bytes.TrimSpace(b)
		if !json.Valid(b) {
			return nil, errors.New("not JSON")
		}
		return b, nil
	case Avro:
		id, rest, err := confluent.DecodeID(b)
		if err != nil {
			return nil, errNotRegistry
		}
		s, err := d.avro(ctx, id)
		if err != nil {
			return nil, err
		}
		var v any
		if _, err := s.schema.Decode(rest, &v); err != nil {
			return nil, fmt.Errorf("avro with schema %d: %w", id, err)
		}
		return json.Marshal(avroJSON(s.root, v))
	case Protobuf:
		id, rest, err := confluent.DecodeID(b)
		if err != nil {
			return nil, errNotRegistry
		}
		index, rest, err := confluent.DecodeIndex(rest, 64)
		if err != nil {
			return nil, fmt.Errorf("protobuf message indexes: %w", err)
		}
		fd, err := d.proto(ctx, id)
		if err != nil {
			return nil, err
		}
		md, err := messageAt(fd, index)
		if err != nil {
			return nil, fmt.Errorf("schema %d: %w", id, err)
		}
		m := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(rest, m); err != nil {
			return nil, fmt.Errorf("protobuf with schema %d: %w", id, err)
		}
		return protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(m)
	}
	return JSONText(b), nil
}

// JSONText is b as a JSON string: its text, or base64 if it isn't UTF-8.
func JSONText(b []byte) json.RawMessage {
	if !utf8.Valid(b) {
		return AppendJSONString(nil, base64.StdEncoding.EncodeToString(b))
	}
	return AppendJSONString(nil, string(b))
}

func messageAt(fd protoreflect.FileDescriptor, index []int) (protoreflect.MessageDescriptor, error) {
	if len(index) == 0 {
		index = []int{0}
	}
	msgs := fd.Messages()
	var md protoreflect.MessageDescriptor
	for _, i := range index {
		if i < 0 || i >= msgs.Len() {
			return nil, fmt.Errorf("no message at index %v", index)
		}
		md = msgs.Get(i)
		msgs = md.Messages()
	}
	return md, nil
}

func (d *Decoder) avro(ctx context.Context, id int) (*avroSchema, error) {
	d.mu.Lock()
	s := d.avros[id]
	d.mu.Unlock()
	if s != nil {
		return s, nil
	}
	sc, err := d.reg.SchemaByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("schema %d: %w", id, err)
	}
	s, err = parseAvro(ctx, d.reg, sc)
	if err != nil {
		return nil, fmt.Errorf("schema %d: %w", id, err)
	}
	d.mu.Lock()
	d.avros[id] = s
	d.mu.Unlock()
	return s, nil
}

// parseAvro parses sc after the schemas it references.
func parseAvro(ctx context.Context, reg *sr.Client, sc sr.Schema) (*avroSchema, error) {
	var cache avro.SchemaCache
	var add func(sr.Schema) (*avro.Schema, error)
	add = func(s sr.Schema) (*avro.Schema, error) {
		for _, r := range s.References {
			ref, err := reg.SchemaByVersion(ctx, r.Subject, r.Version)
			if err != nil {
				return nil, fmt.Errorf("reference %s: %w", r.Name, err)
			}
			if _, err := add(ref.Schema); err != nil {
				return nil, err
			}
		}
		return cache.Parse(s.Schema)
	}
	s, err := add(sc)
	if err != nil {
		return nil, err
	}
	return &avroSchema{schema: s, root: s.Root().ExpandReferences()}, nil
}

func (d *Decoder) proto(ctx context.Context, id int) (protoreflect.FileDescriptor, error) {
	d.mu.Lock()
	fd := d.protos[id]
	d.mu.Unlock()
	if fd != nil {
		return fd, nil
	}
	sc, err := d.reg.SchemaByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("schema %d: %w", id, err)
	}
	fd, err = protoFile(ctx, d.reg, sc)
	if err != nil {
		return nil, fmt.Errorf("schema %d: %w", id, err)
	}
	d.mu.Lock()
	d.protos[id] = fd
	d.mu.Unlock()
	return fd, nil
}

// protoFile compiles sc with the files it imports from the registry.
func protoFile(ctx context.Context, reg *sr.Client, sc sr.Schema) (protoreflect.FileDescriptor, error) {
	files := map[string]string{}
	var add func(name string, s sr.Schema) error
	add = func(name string, s sr.Schema) error {
		if _, ok := files[name]; ok {
			return nil
		}
		files[name] = s.Schema
		for _, r := range s.References {
			ref, err := reg.SchemaByVersion(ctx, r.Subject, r.Version)
			if err != nil {
				return fmt.Errorf("reference %s: %w", r.Name, err)
			}
			if err := add(r.Name, ref.Schema); err != nil {
				return err
			}
		}
		return nil
	}
	const root = "openetl-root.proto"
	if err := add(root, sc); err != nil {
		return nil, err
	}
	return compileProto(ctx, files, root)
}

// avroJSON makes v, decoded with node, JSON's: dates, times and timestamps
// as text, decimals at their scale, bytes as base64.
func avroJSON(node *avro.SchemaNode, v any) any {
	if v == nil {
		return nil
	}
	switch node.Type {
	case "union":
		var branches []avro.SchemaNode
		for _, b := range node.Branches {
			if b.Type != "null" {
				branches = append(branches, b)
			}
		}
		if len(branches) == 1 {
			return avroJSON(&branches[0], v)
		}
		return plainJSON(v)
	case "record":
		m, ok := v.(map[string]any)
		if !ok {
			return plainJSON(v)
		}
		out := make(map[string]any, len(m))
		for _, f := range node.Fields {
			if fv, ok := m[f.Name]; ok {
				out[f.Name] = avroJSON(&f.Type, fv)
			}
		}
		return out
	case "array":
		l, ok := v.([]any)
		if !ok || node.Items == nil {
			return plainJSON(v)
		}
		out := make([]any, len(l))
		for i, e := range l {
			out[i] = avroJSON(node.Items, e)
		}
		return out
	case "map":
		m, ok := v.(map[string]any)
		if !ok || node.Values == nil {
			return plainJSON(v)
		}
		out := make(map[string]any, len(m))
		for k, e := range m {
			out[k] = avroJSON(node.Values, e)
		}
		return out
	}
	if s, ok := v.(string); ok && node.Props[jsonProp] == "json" && json.Valid([]byte(s)) {
		return json.RawMessage(s)
	}
	switch node.LogicalType {
	case "date":
		if t, ok := v.(time.Time); ok {
			return t.UTC().Format(time.DateOnly)
		}
	case "time-millis", "time-micros":
		if d, ok := v.(time.Duration); ok {
			return timeOfDay(d)
		}
	case "timestamp-millis", "timestamp-micros", "timestamp-nanos":
		if t, ok := v.(time.Time); ok {
			return t.UTC().Format(time.RFC3339Nano)
		}
	case "local-timestamp-millis", "local-timestamp-micros", "local-timestamp-nanos":
		if t, ok := v.(time.Time); ok {
			return timestampText(t.UTC(), false)
		}
	case "decimal":
		if r, ok := v.(*big.Rat); ok {
			return json.Number(r.FloatString(node.Scale))
		}
	case "uuid":
		if u, ok := v.([16]byte); ok {
			return uuid.UUID(u).String()
		}
	}
	return plainJSON(v)
}

func plainJSON(v any) any {
	switch t := v.(type) {
	case float32:
		return plainJSON(float64(t))
	case float64:
		switch {
		case math.IsNaN(t):
			return "NaN"
		case math.IsInf(t, 1):
			return "Infinity"
		case math.IsInf(t, -1):
			return "-Infinity"
		}
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case time.Duration:
		return timeOfDay(t)
	case *big.Rat:
		return json.Number(t.FloatString(9))
	case [16]byte:
		return base64.StdEncoding.EncodeToString(t[:])
	case avro.Duration:
		return map[string]any{"months": t.Months, "days": t.Days, "milliseconds": t.Milliseconds}
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = plainJSON(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = plainJSON(e)
		}
		return out
	}
	return v
}

// avroArrow is the Arrow type an Avro field declares, for the catalog; json
// for what raw rows can't carry typed.
func avroArrow(node *avro.SchemaNode) (arrow.DataType, arrow.Metadata) {
	if node.Type == "union" {
		var branches []avro.SchemaNode
		for _, b := range node.Branches {
			if b.Type != "null" {
				branches = append(branches, b)
			}
		}
		if len(branches) == 1 {
			return avroArrow(&branches[0])
		}
		return jsonField()
	}
	if node.Props[jsonProp] == "json" {
		return jsonField()
	}
	switch node.LogicalType {
	case "date":
		return arrow.FixedWidthTypes.Date32, arrow.Metadata{}
	case "time-millis", "time-micros":
		return arrow.FixedWidthTypes.Time64us, arrow.Metadata{}
	case "timestamp-millis", "timestamp-micros", "timestamp-nanos":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, arrow.Metadata{}
	case "local-timestamp-millis", "local-timestamp-micros", "local-timestamp-nanos":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, arrow.Metadata{}
	case "decimal":
		if node.Precision > 0 && node.Precision <= 38 && node.Scale >= 0 && node.Scale <= 38 {
			return &arrow.Decimal128Type{Precision: int32(node.Precision), Scale: int32(node.Scale)}, arrow.Metadata{}
		}
		return arrow.BinaryTypes.String, arrow.Metadata{}
	case "uuid":
		if node.Type == "string" {
			return &arrow.FixedSizeBinaryType{ByteWidth: 16}, arrow.NewMetadata([]string{ExtensionNameKey}, []string{UUIDExtension})
		}
	}
	switch node.Type {
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, arrow.Metadata{}
	case "int":
		return arrow.PrimitiveTypes.Int32, arrow.Metadata{}
	case "long":
		return arrow.PrimitiveTypes.Int64, arrow.Metadata{}
	case "float":
		return arrow.PrimitiveTypes.Float32, arrow.Metadata{}
	case "double":
		return arrow.PrimitiveTypes.Float64, arrow.Metadata{}
	case "string", "enum":
		return arrow.BinaryTypes.String, arrow.Metadata{}
	case "bytes", "fixed":
		return arrow.BinaryTypes.Binary, arrow.Metadata{}
	}
	return jsonField()
}

func jsonField() (arrow.DataType, arrow.Metadata) {
	return arrow.BinaryTypes.String, arrow.NewMetadata([]string{ExtensionNameKey}, []string{JSONExtension})
}

// protoArrow is the Arrow type a Protobuf field declares, for the catalog.
func protoArrow(fd protoreflect.FieldDescriptor) (arrow.DataType, arrow.Metadata) {
	if fd.IsList() || fd.IsMap() {
		return jsonField()
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return arrow.FixedWidthTypes.Boolean, arrow.Metadata{}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return arrow.PrimitiveTypes.Int32, arrow.Metadata{}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind, protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return arrow.PrimitiveTypes.Int64, arrow.Metadata{}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return &arrow.Decimal128Type{Precision: 20, Scale: 0}, arrow.Metadata{}
	case protoreflect.FloatKind:
		return arrow.PrimitiveTypes.Float32, arrow.Metadata{}
	case protoreflect.DoubleKind:
		return arrow.PrimitiveTypes.Float64, arrow.Metadata{}
	case protoreflect.StringKind, protoreflect.EnumKind:
		return arrow.BinaryTypes.String, arrow.Metadata{}
	case protoreflect.BytesKind:
		return arrow.BinaryTypes.Binary, arrow.Metadata{}
	case protoreflect.MessageKind:
		if fd.Message().FullName() == "google.protobuf.Timestamp" {
			return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, arrow.Metadata{}
		}
	}
	return jsonField()
}
