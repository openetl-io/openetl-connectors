// Command destination-azureblob serves the files destination of Azure Blob Storage, as an
// external connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/azureblob"
	"github.com/openetl-io/openetl-connectors/objstore"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-azureblob",
		ConfigSchema: azureblob.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return azureblob.NewDestination(c) },
		Capabilities: objstore.DestinationCapabilities,
	})
}
