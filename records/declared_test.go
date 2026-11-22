package records

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/bufbuild/protocompile/linker"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

var (
	uuidText   = regexp.MustCompile(`^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$`)
	zonedText  = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`)
	localText  = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$`)
	dateText   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	timeText   = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}(\.\d+)?$`)
	numberText = regexp.MustCompile(`^-?\d+(\.\d+)?$`)
)

// fits says whether v, a JSON value, is one the engine coerces to f's
// declared type, as its normalize crate's coerce.rs does.
func fits(f arrow.Field, v any) bool {
	if v == nil {
		return true
	}
	s, isText := v.(string)
	n, isNumber := v.(json.Number)
	if ext, _ := f.Metadata.GetValue(ExtensionNameKey); ext == JSONExtension {
		return true
	}
	switch t := f.Type.(type) {
	case *arrow.BooleanType:
		_, ok := v.(bool)
		return ok || s == "true" || s == "false"
	case *arrow.Int32Type, *arrow.Int64Type:
		if isNumber {
			s, isText = n.String(), true
		}
		_, err := strconv.ParseInt(s, 10, 64)
		return isText && err == nil
	case *arrow.Float32Type, *arrow.Float64Type:
		return isNumber || (isText && (s == "NaN" || s == "Infinity" || s == "-Infinity"))
	case *arrow.StringType:
		return true
	case *arrow.BinaryType:
		_, err := base64.StdEncoding.DecodeString(s)
		return isText && err == nil
	case *arrow.FixedSizeBinaryType:
		return t.ByteWidth == 16 && isText && uuidText.MatchString(s)
	case *arrow.Decimal128Type:
		if isNumber {
			s, isText = n.String(), true
		}
		return isText && numberText.MatchString(s)
	case *arrow.TimestampType:
		if t.TimeZone == "UTC" {
			return isText && zonedText.MatchString(s)
		}
		return isText && localText.MatchString(s)
	case *arrow.Date32Type:
		return isText && dateText.MatchString(s)
	case *arrow.Time64Type:
		return isText && timeText.MatchString(s)
	}
	return false
}

// Each Avro type's field is declared with a type its decoded values take.
func TestAvroDeclaredTypesTakeTheirValues(t *testing.T) {
	schema := `{"type":"record","name":"r","fields":[
		{"name":"b","type":"boolean"},
		{"name":"i","type":"int"},
		{"name":"l","type":"long"},
		{"name":"f","type":"float"},
		{"name":"d","type":"double"},
		{"name":"s","type":"string"},
		{"name":"by","type":"bytes"},
		{"name":"fx","type":{"type":"fixed","name":"fx4","size":4}},
		{"name":"en","type":{"type":"enum","name":"E","symbols":["A","B"]}},
		{"name":"u_string","type":{"type":"string","logicalType":"uuid"}},
		{"name":"u_fixed","type":{"type":"fixed","name":"uf","size":16,"logicalType":"uuid"}},
		{"name":"dur","type":{"type":"fixed","name":"df","size":12,"logicalType":"duration"}},
		{"name":"dec_bytes","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
		{"name":"dec_fixed","type":{"type":"fixed","name":"decf","size":8,"logicalType":"decimal","precision":10,"scale":2}},
		{"name":"dec_wide","type":{"type":"bytes","logicalType":"decimal","precision":50,"scale":10}},
		{"name":"day","type":{"type":"int","logicalType":"date"}},
		{"name":"t_ms","type":{"type":"int","logicalType":"time-millis"}},
		{"name":"t_us","type":{"type":"long","logicalType":"time-micros"}},
		{"name":"ts_ms","type":{"type":"long","logicalType":"timestamp-millis"}},
		{"name":"ts_us","type":{"type":"long","logicalType":"timestamp-micros"}},
		{"name":"ts_ns","type":{"type":"long","logicalType":"timestamp-nanos"}},
		{"name":"lts_ms","type":{"type":"long","logicalType":"local-timestamp-millis"}},
		{"name":"lts_us","type":{"type":"long","logicalType":"local-timestamp-micros"}},
		{"name":"arr","type":{"type":"array","items":"int"}},
		{"name":"m","type":{"type":"map","values":"long"}},
		{"name":"rec","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]}},
		{"name":"null_first","type":["null","string"]},
		{"name":"null_last","type":["string","null"]},
		{"name":"several","type":["null","int","string"]}
	]}`
	at := time.Date(2026, 11, 2, 10, 30, 0, 123456789, time.UTC)
	wide, _ := new(big.Rat).SetString("1234567890123456789012345678901234567890.0123456789")
	in := map[string]any{
		"b": true, "i": int32(1), "l": int64(9007199254740993), "f": float32(1.5), "d": 2.5, "s": "x",
		"by": []byte{0xff, 0}, "fx": [4]byte{1, 2, 3, 4}, "en": "B",
		"u_string":  "550e8400-e29b-41d4-a716-446655440000",
		"u_fixed":   [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0, 0},
		"dur":       avro.Duration{Months: 1, Days: 2, Milliseconds: 3},
		"dec_bytes": big.NewRat(1234, 100), "dec_fixed": big.NewRat(-1234, 100), "dec_wide": wide,
		"day": at, "t_ms": 10 * time.Second, "t_us": 10 * time.Second,
		"ts_ms": at, "ts_us": at, "ts_ns": at, "lts_ms": at, "lts_us": at,
		"arr": []any{int32(1)}, "m": map[string]any{"k": int64(1)}, "rec": map[string]any{"x": int32(1)},
		"null_first": "a", "null_last": "b", "several": int32(3),
	}
	fake := srfake.New()
	defer fake.Close()
	reg, err := sr.NewClient(sr.URLs(fake.URL()))
	if err != nil {
		t.Fatal(err)
	}
	ss, err := reg.CreateSchema(t.Context(), "r-value", sr.Schema{Schema: schema, Type: sr.TypeAvro})
	if err != nil {
		t.Fatal(err)
	}
	s, err := avro.Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := confluent.AppendEncode(nil, ss.ID, nil)
	if b, err = s.AppendEncode(b, in); err != nil {
		t.Fatal(err)
	}
	checkDeclared(t, reg, Avro, "r-value", b, len(in))
}

// checkDeclared decodes b, and checks that each field ValueFields declares
// of subject takes b's value of it.
func checkDeclared(t *testing.T, reg *sr.Client, format, subject string, b []byte, want int) {
	t.Helper()
	out, err := NewDecoder(format, reg).Decode(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytesReader(out))
	dec.UseNumber()
	var got map[string]any
	if err := dec.Decode(&got); err != nil {
		t.Fatal(err)
	}
	fields, err := ValueFields(t.Context(), reg, format, subject)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != want {
		t.Errorf("%d fields declared, want %d", len(fields), want)
	}
	for _, f := range fields {
		if !fits(f, got[f.Name]) {
			t.Errorf("%s is declared %s%v, and its value is %s", f.Name, f.Type, f.Metadata, mustJSON(t, got[f.Name]))
		}
	}
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// Each Protobuf type's field is declared with a type its decoded values
// take, an Any of a message the schema has among them.
func TestProtobufDeclaredTypesTakeTheirValues(t *testing.T) {
	schema := `syntax = "proto3";
package x;
import "google/protobuf/any.proto";
import "google/protobuf/duration.proto";
import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";
import "google/protobuf/wrappers.proto";
message M {
  enum E { A = 0; B = 1; }
  message Inner { string s = 1; }
  bool b = 1;
  int32 i32 = 2;
  sint32 s32 = 3;
  sfixed32 sf32 = 4;
  int64 i64 = 5;
  sint64 s64 = 6;
  sfixed64 sf64 = 7;
  uint32 u32 = 8;
  fixed32 f32 = 9;
  uint64 u64 = 10;
  fixed64 f64 = 11;
  float fl = 12;
  double db = 13;
  string s = 14;
  bytes by = 15;
  E e = 16;
  E unknown = 17;
  google.protobuf.Timestamp ts = 18;
  google.protobuf.Duration dur = 19;
  google.protobuf.Int64Value w = 20;
  google.protobuf.Struct st = 21;
  google.protobuf.Any any = 22;
  map<string, int64> m = 23;
  repeated string tags = 24;
  optional int32 opt = 25;
  oneof choice { string a = 26; int64 c = 27; }
  Inner inner = 28;
}
`
	fake := srfake.New()
	defer fake.Close()
	reg, err := sr.NewClient(sr.URLs(fake.URL()))
	if err != nil {
		t.Fatal(err)
	}
	ss, err := reg.CreateSchema(t.Context(), "m-value", sr.Schema{Schema: schema, Type: sr.TypeProtobuf})
	if err != nil {
		t.Fatal(err)
	}
	fd, err := compileProto(t.Context(), map[string]string{"m.proto": schema}, "m.proto")
	if err != nil {
		t.Fatal(err)
	}
	md := fd.Messages().Get(0)
	m := dynamicpb.NewMessage(md)
	in := `{"b":true,"i32":-1,"s32":-2,"sf32":-3,"i64":"9007199254740993","s64":"-5","sf64":"6","u32":4294967295,"f32":7,
		"u64":"18446744073709551615","f64":"8","fl":"NaN","db":"-Infinity","s":"x","by":"/wA=","e":"B",
		"ts":"2026-11-02T10:30:00.123456789Z","dur":"1.5s","w":"3","st":{"k":[1,"a",null]},
		"any":{"@type":"type.googleapis.com/x.M.Inner","s":"packed"},"m":{"k":"1"},"tags":["x"],"opt":0,"c":"2","inner":{"s":"y"}}`
	if err := (protojson.UnmarshalOptions{Resolver: linker.ResolverFromFile(fd.(linker.File))}).Unmarshal([]byte(in), m); err != nil {
		t.Fatal(err)
	}
	m.Set(md.Fields().ByName("unknown"), protoreflect.ValueOfEnum(7))
	body, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := confluent.AppendEncode(nil, ss.ID, []int{0})
	checkDeclared(t, reg, Protobuf, "m-value", append(b, body...), md.Fields().Len())
}

// An Any of a message neither the schema nor its imports have can't be
// spelled as JSON: its decode fails, naming the type.
func TestAnAnyOfAMessageTheSchemaLacksFails(t *testing.T) {
	schema := `syntax = "proto3";
package x;
import "google/protobuf/any.proto";
message M { google.protobuf.Any any = 1; }
`
	fake := srfake.New()
	defer fake.Close()
	reg, err := sr.NewClient(sr.URLs(fake.URL()))
	if err != nil {
		t.Fatal(err)
	}
	ss, err := reg.CreateSchema(t.Context(), "m-value", sr.Schema{Schema: schema, Type: sr.TypeProtobuf})
	if err != nil {
		t.Fatal(err)
	}
	inner := protowire.AppendString(protowire.AppendTag(nil, 1, protowire.BytesType), "y")
	packed := protowire.AppendString(protowire.AppendTag(nil, 1, protowire.BytesType), "type.googleapis.com/y.Elsewhere")
	packed = protowire.AppendBytes(protowire.AppendTag(packed, 2, protowire.BytesType), inner)
	b, _ := confluent.AppendEncode(nil, ss.ID, []int{0})
	b = protowire.AppendBytes(protowire.AppendTag(b, 1, protowire.BytesType), packed)
	_, err = NewDecoder(Protobuf, reg).Decode(t.Context(), b)
	if err == nil || !strings.Contains(err.Error(), "y.Elsewhere") || Unanswered(err) {
		t.Errorf("an Any of a message the schema lacks decoded with %v", err)
	}
}
