//go:build integration

package azureblob

import (
	"testing"

	"github.com/openetl-io/openetl-connectors/objstore"
)

func TestConformance(t *testing.T) {
	for _, st := range []objstore.TestStore{objstore.AzureStore(t)} {
		t.Run(st.Name, func(t *testing.T) { objstore.Conformance(t, "azureblob", st) })
	}
}
