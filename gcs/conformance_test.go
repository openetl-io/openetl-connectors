//go:build integration

package gcs

import (
	"testing"

	"github.com/openetl-io/openetl-connectors/objstore"
)

func TestConformance(t *testing.T) {
	for _, st := range []objstore.TestStore{objstore.GCSStore(t)} {
		t.Run(st.Name, func(t *testing.T) { objstore.Conformance(t, "gcs", st) })
	}
}
