//go:build integration

package records

import (
	"cmp"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"

	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
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
