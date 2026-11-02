// Command destination-kafka serves the Kafka destination as an external
// connector.
package main

import (
	"github.com/unsuitable001/openetl-connectors/kafka"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
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
