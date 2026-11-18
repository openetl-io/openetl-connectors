// Command source-clickhouse serves the ClickHouse source as an external
// connector.
package main

import (
	"log/slog"
	"os"

	"github.com/openetl-io/openetl-connectors/clickhouse"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func main() {
	// The runner keeps JSON lines' levels and attributes.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	sdk.Serve(sdk.Connector{
		Name:         "source-clickhouse",
		ConfigSchema: clickhouse.SourceConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Source:       func(c []byte, _ host.Host) (source.Source, error) { return clickhouse.NewSource(c) },
		// Tables are read in sorting-key ranges, and a range's remainder
		// divided.
		Splits:  true,
		Resplit: true,
	})
}
