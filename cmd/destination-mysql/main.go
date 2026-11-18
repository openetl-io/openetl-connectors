// Command destination-mysql serves the MySQL destination as an external
// connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/mysql"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-mysql",
		ConfigSchema: mysql.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return mysql.NewDestination(c) },
		Capabilities: mysql.DestinationCapabilities,
	})
}
