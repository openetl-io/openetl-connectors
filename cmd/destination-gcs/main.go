// Command destination-gcs serves the files destination of Google Cloud Storage, as an
// external connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/gcs"
	"github.com/openetl-io/openetl-connectors/objstore"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-gcs",
		ConfigSchema: gcs.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return gcs.NewDestination(c) },
		Capabilities: objstore.DestinationCapabilities,
	})
}
