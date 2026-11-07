package records

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bufbuild/protocompile"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	sdk "github.com/openetl-io/openetl/sdk/go"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Encoder encodes one part of a row, its key or its value, as an object
// of cols in a format.
type Encoder interface {
	Encode(vals []any) ([]byte, error)
}

var confluent sr.ConfluentHeader

// An Avro string holding JSON has this property set to "json", so a reader
// can declare it json.
const jsonProp = "openetl.type"

// NewEncoder builds the encoder for cols, registering its schema under
// subject for the formats that need one. name names the record or message.
func NewEncoder(ctx context.Context, format string, reg *sr.Client, subject, name string, cols []Col) (Encoder, error) {
	switch format {
	case JSON:
		return jsonEncoder(cols), nil
	case Avro:
		return newAvroEncoder(ctx, reg, subject, name, cols)
	case Protobuf:
		return newProtoEncoder(ctx, reg, subject, name, cols)
	}
	return nil, fmt.Errorf("format %q can't be written", format)
}

type jsonEncoder []Col

func (e jsonEncoder) Encode(vals []any) ([]byte, error) { return appendJSONObject(nil, e, vals) }

// register registers s under subject and returns its ID. A schema already
// registered there keeps its ID, as one a person registers to take a change
// does. Otherwise a schema that changes a field's type from the subject's
// latest is refused, since no connector changes a type by itself, and so is
// one the registry's compatibility setting refuses: both SCHEMA_DRIFT.
func register(ctx context.Context, reg *sr.Client, subject string, s sr.Schema) (int, error) {
	ss, err := reg.LookupSchema(ctx, subject, s)
	if err == nil {
		return ss.ID, nil
	}
	if !notFound(err) {
		return 0, fmt.Errorf("look up %s: %w", subject, err)
	}
	latest, err := reg.SchemaByVersion(ctx, subject, -1)
	switch {
	case notFound(err):
	case err != nil:
		return 0, fmt.Errorf("the latest schema of %s: %w", subject, err)
	case latest.Type == s.Type:
		changed, err := typeChanges(ctx, s.Type, latest.Schema.Schema, s.Schema)
		if err != nil {
			return 0, fmt.Errorf("the latest schema of %s: %w", subject, err)
		}
		if len(changed) > 0 {
			slog.Warn("a schema with a changed type, which the destination doesn't register itself", "subject", subject, "schema", s.Schema)
			return 0, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT, fmt.Errorf(
				"the schema of %s would change %s, and the destination doesn't change a field's type: to take the change, register the schema this attempt's log shows under %s yourself",
				subject, strings.Join(changed, ", "), subject))
		}
	}
	ss, err = reg.CreateSchema(ctx, subject, s)
	if err != nil {
		var re *sr.ResponseError
		if errors.As(err, &re) && (re.StatusCode == http.StatusConflict || re.ErrorCode == 409) {
			return 0, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT, fmt.Errorf("the schema registry refuses the new schema of %s: %w", subject, err))
		}
		return 0, fmt.Errorf("register %s: %w", subject, err)
	}
	return ss.ID, nil
}

// notFound: the registry has no such subject, version or schema.
func notFound(err error) bool {
	var re *sr.ResponseError
	return errors.As(err, &re) && re.StatusCode == http.StatusNotFound
}

// typeChanges are the fields, in both schemas, whose type next changes from
// prev's, as "name from type to type".
func typeChanges(ctx context.Context, t sr.SchemaType, prev, next string) ([]string, error) {
	if t == sr.TypeProtobuf {
		a, err := compileProto(ctx, map[string]string{"schema.proto": prev}, "schema.proto")
		if err != nil {
			return nil, err
		}
		b, err := compileProto(ctx, map[string]string{"schema.proto": next}, "schema.proto")
		if err != nil {
			return nil, err
		}
		if a.Messages().Len() == 0 || b.Messages().Len() == 0 {
			return nil, nil
		}
		return messageChanges("", a.Messages().Get(0), b.Messages().Get(0)), nil
	}
	var a, b any
	if err := json.Unmarshal([]byte(prev), &a); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(next), &b); err != nil {
		return nil, err
	}
	return recordChanges("", a, b), nil
}

// recordChanges compares two Avro records' fields by name, and the fields of
// the records among them.
func recordChanges(path string, a, b any) []string {
	fields := func(r any) map[string]any {
		out := map[string]any{}
		m, _ := r.(map[string]any)
		fs, _ := m["fields"].([]any)
		for _, f := range fs {
			if fm, ok := f.(map[string]any); ok {
				if name, ok := fm["name"].(string); ok {
					out[name] = fm["type"]
				}
			}
		}
		return out
	}
	// The record a type is, if it's one, nullable or not.
	record := func(t any) any {
		if u, ok := t.([]any); ok && len(u) == 2 && u[0] == "null" {
			t = u[1]
		}
		if m, ok := t.(map[string]any); ok && m["type"] == "record" {
			return m
		}
		return nil
	}
	var out []string
	before := fields(a)
	for name, t := range fields(b) {
		was, ok := before[name]
		if !ok {
			continue
		}
		if ra, rb := record(was), record(t); ra != nil && rb != nil {
			out = append(out, recordChanges(path+name+".", ra, rb)...)
			continue
		}
		ja, _ := json.Marshal(was)
		jb, _ := json.Marshal(t)
		if string(ja) != string(jb) {
			out = append(out, fmt.Sprintf("%q from %s to %s", path+name, ja, jb))
		}
	}
	slices.Sort(out)
	return out
}

// messageChanges compares two Protobuf messages' fields by name, and the
// fields of the messages they define for them.
func messageChanges(path string, a, b protoreflect.MessageDescriptor) []string {
	describe := func(f protoreflect.FieldDescriptor) string {
		t := f.Kind().String()
		if m := f.Message(); m != nil {
			t = string(m.FullName())
		}
		if f.Cardinality() == protoreflect.Repeated {
			t = "repeated " + t
		}
		return t
	}
	var out []string
	for i := range b.Fields().Len() {
		fb := b.Fields().Get(i)
		fa := a.Fields().ByName(fb.Name())
		if fa == nil {
			continue
		}
		name := path + string(fb.Name())
		ma, mb := fa.Message(), fb.Message()
		if ma != nil && mb != nil && ma.Parent() == a && mb.Parent() == b && fa.Cardinality() == fb.Cardinality() {
			out = append(out, messageChanges(name+".", ma, mb)...)
			continue
		}
		if da, db := describe(fa), describe(fb); da != db {
			out = append(out, fmt.Sprintf("%q from %s to %s", name, da, db))
		}
	}
	return out
}

// AvroName makes s an Avro or Protobuf name: letters, digits and _, not
// starting with a digit.
func AvroName(s string) string {
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
	cols   []Col
}

func newAvroEncoder(ctx context.Context, reg *sr.Client, subject, name string, cols []Col) (*avroEncoder, error) {
	text, err := avroSchemaText(name, cols)
	if err != nil {
		return nil, err
	}
	schema, err := avro.Parse(string(text))
	if err != nil {
		return nil, fmt.Errorf("avro schema of %s: %w", subject, err)
	}
	id, err := register(ctx, reg, subject, sr.Schema{Schema: string(text), Type: sr.TypeAvro})
	if err != nil {
		return nil, err
	}
	return &avroEncoder{id: id, schema: schema, cols: cols}, nil
}

// avroSchemaText is an Avro record schema, name, holding cols.
func avroSchemaText(name string, cols []Col) ([]byte, error) {
	fields := make([]any, len(cols))
	for i := range cols {
		t, err := avroType(&cols[i], name+"_"+cols[i].name)
		if err != nil {
			return nil, err
		}
		fields[i] = map[string]any{"name": cols[i].name, "type": nullable(t), "default": nil}
	}
	return json.Marshal(map[string]any{"type": "record", "name": AvroName(name), "fields": fields})
}

func nullable(t any) any {
	if t == "null" {
		return t
	}
	return []any{"null", t}
}

// avroType is c's Avro type; name names the records it defines.
func avroType(c *Col, name string) (any, error) {
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
		return map[string]any{"type": "record", "name": AvroName(name), "fields": []any{
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
		return map[string]any{"type": "record", "name": AvroName(name), "fields": fields}, nil
	}
	return nil, fmt.Errorf("column %q: no Avro type", c.name)
}

func (e *avroEncoder) Encode(vals []any) ([]byte, error) {
	rec, err := avroRecord(e.cols, vals)
	if err != nil {
		return nil, err
	}
	b, _ := confluent.AppendEncode(nil, e.id, nil)
	return e.schema.AppendEncode(b, rec)
}

func avroRecord(cols []Col, vals []any) (map[string]any, error) {
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

func avroValue(c *Col, v any) (any, error) {
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
	cols []Col
}

// protoSchema is a proto3 file with one message, name, holding cols. A
// field keeps its number in prev, the subject's latest schema, and new
// fields take numbers above any prev used, since Protobuf reads fields by
// number and the engine adds a source's new columns before its own.
func protoSchema(name string, cols []Col, prev *protoLayout) (string, error) {
	var b strings.Builder
	b.WriteString("syntax = \"proto3\";\n\npackage openetl;\n\nimport \"google/protobuf/timestamp.proto\";\n\n")
	if err := protoMessage(&b, AvroName(name), cols, "", prev); err != nil {
		return "", err
	}
	return b.String(), nil
}

// protoLayout is a message's field numbers by name, and its nested
// messages', in a registered schema.
type protoLayout struct {
	nums     map[string]protoreflect.FieldNumber
	reserved [][2]protoreflect.FieldNumber
	// The highest number used or reserved.
	max    protoreflect.FieldNumber
	nested map[string]*protoLayout
}

func layoutOf(md protoreflect.MessageDescriptor) *protoLayout {
	l := &protoLayout{nums: map[string]protoreflect.FieldNumber{}, nested: map[string]*protoLayout{}}
	for i := range md.Fields().Len() {
		f := md.Fields().Get(i)
		l.nums[string(f.Name())] = f.Number()
		l.max = max(l.max, f.Number())
		if m := f.Message(); m != nil && m.Parent() == md {
			l.nested[string(f.Name())] = layoutOf(m)
		}
	}
	for i := range md.ReservedRanges().Len() {
		r := md.ReservedRanges().Get(i)
		l.reserved = append(l.reserved, r)
		l.max = max(l.max, r[1]-1)
	}
	return l
}

func (l *protoLayout) num(name string) (protoreflect.FieldNumber, bool) {
	if l == nil {
		return 0, false
	}
	n, ok := l.nums[name]
	return n, ok
}

func (l *protoLayout) child(name string) *protoLayout {
	if l == nil {
		return nil
	}
	return l.nested[name]
}

// latestLayout is subject's latest Protobuf schema's layout, nil if it has
// none.
func latestLayout(ctx context.Context, reg *sr.Client, subject string) (*protoLayout, error) {
	ss, err := reg.SchemaByVersion(ctx, subject, -1)
	if err != nil {
		var re *sr.ResponseError
		if errors.As(err, &re) && re.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("the latest schema of %s: %w", subject, err)
	}
	if ss.Type != sr.TypeProtobuf {
		return nil, nil
	}
	fd, err := protoFile(ctx, reg, ss.Schema)
	if err != nil {
		return nil, fmt.Errorf("the latest schema of %s: %w", subject, err)
	}
	if fd.Messages().Len() == 0 {
		return nil, nil
	}
	return layoutOf(fd.Messages().Get(0)), nil
}

func protoMessage(b *strings.Builder, name string, cols []Col, indent string, prev *protoLayout) error {
	fmt.Fprintf(b, "%smessage %s {\n", indent, name)
	var next protoreflect.FieldNumber
	var gone [][2]protoreflect.FieldNumber
	if prev != nil {
		next = prev.max
		kept := map[string]bool{}
		for i := range cols {
			kept[cols[i].name] = true
		}
		gone = slices.Clone(prev.reserved)
		for n, num := range prev.nums {
			if !kept[n] {
				gone = append(gone, [2]protoreflect.FieldNumber{num, num + 1})
			}
		}
		slices.SortFunc(gone, func(a, b [2]protoreflect.FieldNumber) int { return int(a[0] - b[0]) })
	}
	if len(gone) > 0 {
		var rs []string
		for _, r := range gone {
			if r[1]-1 == r[0] {
				rs = append(rs, fmt.Sprint(r[0]))
			} else {
				rs = append(rs, fmt.Sprintf("%d to %d", r[0], r[1]-1))
			}
		}
		fmt.Fprintf(b, "%s  reserved %s;\n", indent, strings.Join(rs, ", "))
	}
	for i := range cols {
		c := &cols[i]
		t, nested, err := protoType(c, indent+"  ", prev.child(c.name))
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
		num, ok := prev.num(c.name)
		if !ok {
			next++
			num = next
		}
		fmt.Fprintf(b, "%s  %s%s %s = %d;\n", indent, label, t, c.name, num)
	}
	fmt.Fprintf(b, "%s}\n", indent)
	return nil
}

// protoType is c's field type, and any message it defines for it.
func protoType(c *Col, indent string, prev *protoLayout) (string, string, error) {
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
		return protoType(&e, indent, prev)
	case kStruct:
		name := "M_" + c.name
		var b strings.Builder
		if err := protoMessage(&b, name, c.fields, indent, prev); err != nil {
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

func newProtoEncoder(ctx context.Context, reg *sr.Client, subject, name string, cols []Col) (*protoEncoder, error) {
	prev, err := latestLayout(ctx, reg, subject)
	if err != nil {
		return nil, err
	}
	text, err := protoSchema(name, cols, prev)
	if err != nil {
		return nil, err
	}
	fd, err := compileProto(ctx, map[string]string{"schema.proto": text}, "schema.proto")
	if err != nil {
		return nil, fmt.Errorf("protobuf schema of %s: %w", subject, err)
	}
	id, err := register(ctx, reg, subject, sr.Schema{Schema: text, Type: sr.TypeProtobuf})
	if err != nil {
		return nil, err
	}
	return &protoEncoder{id: id, md: fd.Messages().Get(0), cols: cols}, nil
}

func (e *protoEncoder) Encode(vals []any) ([]byte, error) {
	m := dynamicpb.NewMessage(e.md)
	if err := protoFill(m, e.cols, vals); err != nil {
		return nil, err
	}
	b, _ := confluent.AppendEncode(nil, e.id, []int{0})
	return proto.MarshalOptions{Deterministic: true}.MarshalAppend(b, m)
}

func protoFill(m protoreflect.Message, cols []Col, vals []any) error {
	fields := m.Descriptor().Fields()
	for i := range cols {
		if vals[i] == nil {
			continue
		}
		c := &cols[i]
		fd := fields.ByName(protoreflect.Name(c.name))
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
func protoValue(c *Col, v any, newMsg func() protoreflect.Value) (protoreflect.Value, error) {
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
