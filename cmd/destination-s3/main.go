// Command destination-s3 serves the Parquet on S3 destination as an external
// connector.
package main

import (
	"github.com/unsuitable001/openetl-connectors/s3"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-s3",
		ConfigSchema: s3.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return s3.NewDestination(c) },
		Capabilities: s3.DestinationCapabilities,
	})
}
