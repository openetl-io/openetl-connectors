// Command source-gcs serves the files source of Google Cloud Storage, as an
// external connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/gcs"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "source-gcs",
		ConfigSchema: gcs.SourceConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Source:       func(c []byte, _ host.Host) (source.Source, error) { return gcs.NewSource(c) },
		// Files are read in splits by their keys' hashes, which Resplit
		// halves.
		Splits:  true,
		Resplit: true,
	})
}
