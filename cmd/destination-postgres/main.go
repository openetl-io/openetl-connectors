// Command destination-postgres serves the Postgres destination as an
// external connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/postgres"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
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
