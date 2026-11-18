// Command source-mongodb serves the MongoDB source as an external
// connector.
package main

import (
	"log/slog"
	"os"

	"github.com/openetl-io/openetl-connectors/mongodb"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func main() {
	// The runner keeps JSON lines' levels and attributes.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	sdk.Serve(sdk.Connector{
		Name:         "source-mongodb",
		ConfigSchema: mongodb.SourceConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Source:       func(c []byte, _ host.Host) (source.Source, error) { return mongodb.NewSource(c) },
		// Collections are read in _id ranges, and a range's remainder divided.
		Splits:  true,
		Resplit: true,
		// Asked how far cdc streams' positions are from leaving the oplog.
		Retention: true,
	})
}
