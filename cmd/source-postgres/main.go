// Command source-postgres serves the Postgres source as an external
// connector.
package main

import (
	"log/slog"
	"os"

	"github.com/unsuitable001/openetl-connectors/postgres"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/host"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

func main() {
	// The runner keeps JSON lines' levels and attributes.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	sdk.Serve(sdk.Connector{
		Name:         "source-postgres",
		ConfigSchema: postgres.SourceConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Source:       func(c []byte, _ host.Host) (source.Source, error) { return postgres.NewSource(c) },
		// Committed advances the replication slot.
		Acknowledges: true,
	})
}
