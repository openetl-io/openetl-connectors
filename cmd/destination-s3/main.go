// Command destination-s3 serves the files destination of Amazon S3, S3-compatible stores and local files, as an
// external connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/objstore"
	"github.com/openetl-io/openetl-connectors/s3"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-s3",
		ConfigSchema: s3.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return s3.NewDestination(c) },
		Capabilities: objstore.DestinationCapabilities,
	})
}
