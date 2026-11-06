//go:build integration

package records

import (
	"cmp"
	"crypto/rand"
	"errors"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Runs against the compose schema registry (make deps-up), or
// OPENETL_TEST_SCHEMA_REGISTRY.
func TestRegistryRefusesAnIncompatibleSchema(t *testing.T) {
	cfg := Config{Format: Avro, SchemaRegistryURL: cmp.Or(os.Getenv("OPENETL_TEST_SCHEMA_REGISTRY"), "http://127.0.0.1:8081")}
	reg, err := cfg.Registry()
	if err != nil {
		t.Fatal(err)
	}
	subject := "drift_" + strings.ToLower(rand.Text())[:8] + "-value"
	if _, err := NewEncoder(t.Context(), Avro, reg, subject, "t", []Col{{name: "id", kind: kInt64}}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEncoder(t.Context(), Avro, reg, subject, "t", []Col{{name: "id", kind: kInt64}, {name: "extra", kind: kString}}); err != nil {
		t.Errorf("an added column: %v", err)
	}
	_, err = NewEncoder(t.Context(), Avro, reg, subject, "t", []Col{{name: "id", kind: kBool}})
	var coded interface{ ErrorCode() protocolv1.ErrorCode }
	if !errors.As(err, &coded) || coded.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT {
		t.Errorf("a changed type: %v", err)
	}
}

// A column added before the metadata columns, as the engine adds a
// source's, keeps every other field's number, so readers of the old schema
// read new records; a dropped column's number isn't reused.
func TestProtobufNumbersStay(t *testing.T) {
	cfg := Config{Format: Protobuf, SchemaRegistryURL: cmp.Or(os.Getenv("OPENETL_TEST_SCHEMA_REGISTRY"), "http://127.0.0.1:8081")}
	reg, err := cfg.Registry()
	if err != nil {
		t.Fatal(err)
	}
	subject := "numbers_" + strings.ToLower(rand.Text())[:8] + "-value"
	address := func(fields ...Col) Col { return Col{name: "address", kind: kStruct, fields: fields} }
	meta := []Col{{name: "_openetl_id", kind: kString}, {name: "_openetl_synced_at", kind: kTimestamp}}
	versions := []struct {
		cols []Col
		vals []any
		want map[string]int
	}{{
		cols: append([]Col{{name: "id", kind: kInt64}, address(Col{name: "city", kind: kString})}, meta...),
		vals: []any{int64(7), []any{"Pune"}, "x", time.Unix(1, 0)},
		want: map[string]int{"id": 1, "address": 2, "address.city": 1, "_openetl_id": 3, "_openetl_synced_at": 4},
	}, {
		cols: append([]Col{{name: "id", kind: kInt64}, address(Col{name: "zip", kind: kInt32}, Col{name: "city", kind: kString}), {name: "score", kind: kInt32}}, meta...),
		vals: []any{int64(7), []any{int32(411001), "Pune"}, int32(3), "x", time.Unix(1, 0)},
		want: map[string]int{"id": 1, "address": 2, "address.zip": 2, "address.city": 1, "score": 5, "_openetl_id": 3, "_openetl_synced_at": 4},
	}, {
		cols: append([]Col{{name: "id", kind: kInt64}, address(Col{name: "city", kind: kString})}, meta...),
		vals: []any{int64(7), []any{"Pune"}, "x", time.Unix(1, 0)},
		want: map[string]int{"id": 1, "address": 2, "address.city": 1, "_openetl_id": 3, "_openetl_synced_at": 4},
	}, {
		cols: append([]Col{{name: "id", kind: kInt64}, address(Col{name: "city", kind: kString}, Col{name: "zip", kind: kInt32}), {name: "email", kind: kString}}, meta...),
		vals: []any{int64(7), []any{"Pune", int32(411001)}, "a@b", "x", time.Unix(1, 0)},
		want: map[string]int{"id": 1, "address": 2, "address.city": 1, "address.zip": 3, "email": 6, "_openetl_id": 3, "_openetl_synced_at": 4},
	}}
	for v, ver := range versions {
		enc, err := NewEncoder(t.Context(), Protobuf, reg, subject, "t", ver.cols)
		if err != nil {
			t.Fatalf("version %d: %v", v+1, err)
		}
		got := map[string]int{}
		var walk func(prefix string, md protoreflect.MessageDescriptor)
		walk = func(prefix string, md protoreflect.MessageDescriptor) {
			for i := range md.Fields().Len() {
				f := md.Fields().Get(i)
				got[prefix+string(f.Name())] = int(f.Number())
				if f.Kind() == protoreflect.MessageKind && f.Message().Parent() == md {
					walk(prefix+string(f.Name())+".", f.Message())
				}
			}
		}
		walk("", enc.(*protoEncoder).md)
		if !maps.Equal(got, ver.want) {
			t.Errorf("version %d's numbers %v, want %v", v+1, got, ver.want)
		}
		if _, err := enc.Encode(ver.vals); err != nil {
			t.Errorf("version %d: encode: %v", v+1, err)
		}
	}
}
