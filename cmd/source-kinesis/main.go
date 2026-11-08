// Command source-kinesis serves the Kinesis source as an external connector.
package main

import (
	"log/slog"
	"os"

	"github.com/openetl-io/openetl-connectors/kinesis"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func main() {
	// The runner keeps JSON lines' levels and attributes.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	sdk.Serve(sdk.Connector{
		Name:         "source-kinesis",
		ConfigSchema: kinesis.SourceConfigSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressRaw},
		Source:       func(c []byte, _ host.Host) (source.Source, error) { return kinesis.NewSource(c) },
		Splits:       true,
		Retention:    true,
	})
}
