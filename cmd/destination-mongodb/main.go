// Command destination-mongodb serves the MongoDB destination as an external
// connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/mongodb"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-mongodb",
		ConfigSchema: mongodb.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return mongodb.NewDestination(c) },
		Capabilities: mongodb.DestinationCapabilities,
	})
}
