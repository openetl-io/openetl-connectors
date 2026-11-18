// Command source-dynamodb serves the Amazon DynamoDB source as an external
// connector.
package main

import (
	"log/slog"
	"os"

	"github.com/openetl-io/openetl-connectors/dynamodb"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func main() {
	// The runner keeps JSON lines' levels and attributes.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	sdk.Serve(sdk.Connector{
		Name:         "source-dynamodb",
		ConfigSchema: dynamodb.SourceConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Source:       func(c []byte, _ host.Host) (source.Source, error) { return dynamodb.NewSource(c) },
		// Tables are read in parallel scan segments.
		Splits: true,
		// Asked how far cdc streams' positions are from the 24 hours their
		// streams keep.
		Retention: true,
	})
}
