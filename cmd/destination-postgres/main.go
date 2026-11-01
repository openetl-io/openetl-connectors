// Command destination-postgres serves the Postgres destination as an
// external connector.
package main

import (
	"github.com/unsuitable001/openetl-connectors/postgres"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-postgres",
		ConfigSchema: postgres.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return postgres.NewDestination(c) },
		Capabilities: postgres.DestinationCapabilities,
	})
}
