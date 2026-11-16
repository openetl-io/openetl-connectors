// Command source-s3 serves the files source, for S3, Google Cloud Storage,
// Azure Blob Storage and local files, as an external connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/s3"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "source-s3",
		ConfigSchema: s3.SourceConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Source:       func(c []byte, _ host.Host) (source.Source, error) { return s3.NewSource(c) },
		// Files are read in splits by their keys' hashes, which Resplit
		// halves.
		Splits:  true,
		Resplit: true,
	})
}
