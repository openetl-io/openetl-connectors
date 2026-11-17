// Command destination-dynamodb serves the Amazon DynamoDB destination as an
// external connector.
package main

import (
	"log/slog"
	"os"

	"github.com/openetl-io/openetl-connectors/dynamodb"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
)

func main() {
	// The runner keeps JSON lines' levels and attributes.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	sdk.Serve(sdk.Connector{
		Name:         "destination-dynamodb",
		ConfigSchema: dynamodb.DestinationConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Destination:  func(c []byte, _ host.Host) (destination.Destination, error) { return dynamodb.NewDestination(c) },
		Capabilities: dynamodb.DestinationCapabilities,
		// Teardown removes a deleted pipeline's fence and staging items.
		Setup: true,
	})
}
