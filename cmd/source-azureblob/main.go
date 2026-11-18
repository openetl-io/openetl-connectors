// Command source-azureblob serves the files source of Azure Blob Storage, as an
// external connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/azureblob"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "source-azureblob",
		ConfigSchema: azureblob.SourceConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Source:       func(c []byte, _ host.Host) (source.Source, error) { return azureblob.NewSource(c) },
		// Files are read in splits by their keys' hashes, which Resplit
		// halves.
		Splits:  true,
		Resplit: true,
	})
}
