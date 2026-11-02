// Command destination-iceberg serves the Iceberg destination as an external
// connector.
package main

import (
	"github.com/unsuitable001/openetl-connectors/iceberg"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-iceberg",
		ConfigSchema: iceberg.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return iceberg.NewDestination(c) },
		Capabilities: iceberg.DestinationCapabilities,
	})
}
