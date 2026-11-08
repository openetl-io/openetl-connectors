// Command source-kafka serves the Kafka source as an external connector.
package main

import (
	"log/slog"
	"os"

	"github.com/openetl-io/openetl-connectors/kafka"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func main() {
	// The runner keeps JSON lines' levels and attributes.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	sdk.Serve(sdk.Connector{
		Name:         "source-kafka",
		ConfigSchema: kafka.SourceConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Source:       func(c []byte, _ host.Host) (source.Source, error) { return kafka.NewSource(c) },
		// Committed commits the offsets to the consumer group.
		Acknowledges: true,
		Splits:       true,
		Retention:    true,
	})
}
