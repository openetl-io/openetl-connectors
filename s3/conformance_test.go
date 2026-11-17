//go:build integration

package s3

import (
	"testing"

	"github.com/openetl-io/openetl-connectors/objstore"
)

func TestConformance(t *testing.T) {
	for _, st := range []objstore.TestStore{objstore.FileStore(t), objstore.S3Store(t)} {
		t.Run(st.Name, func(t *testing.T) { objstore.Conformance(t, "s3", st) })
	}
}
