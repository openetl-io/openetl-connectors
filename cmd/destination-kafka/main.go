// Command destination-kafka serves the Kafka destination as an external
// connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/kafka"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-kafka",
		ConfigSchema: kafka.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return kafka.NewDestination(c) },
		Capabilities: kafka.DestinationCapabilities,
	})
}
