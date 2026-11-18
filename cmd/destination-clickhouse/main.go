// Command destination-clickhouse serves the ClickHouse destination as an
// external connector.
package main

import (
	"github.com/openetl-io/openetl-connectors/clickhouse"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "destination-clickhouse",
		ConfigSchema: clickhouse.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return clickhouse.NewDestination(c) },
		Capabilities: clickhouse.DestinationCapabilities,
	})
}
