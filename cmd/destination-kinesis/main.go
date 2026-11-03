// Command destination-kinesis serves the Kinesis destination as an external
// connector.
package main

import (
	"github.com/unsuitable001/openetl-connectors/kinesis"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-kinesis",
		ConfigSchema: kinesis.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return kinesis.NewDestination(c) },
		Capabilities: kinesis.DestinationCapabilities,
	})
}
