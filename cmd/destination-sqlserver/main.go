// Command destination-sqlserver serves the SQL Server destination as an
// external connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/sqlserver"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-sqlserver",
		ConfigSchema: sqlserver.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return sqlserver.NewDestination(c) },
		Capabilities: sqlserver.DestinationCapabilities,
	})
}
