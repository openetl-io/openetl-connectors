// Command destination-oracle serves the Oracle destination as an external
// connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/oracle"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-oracle",
		ConfigSchema: oracle.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return oracle.NewDestination(c) },
		Capabilities: oracle.DestinationCapabilities,
	})
}
