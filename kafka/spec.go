package kafka

import (
	_ "embed"
	"time"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

var (
	//go:embed source.schema.json
	SourceConfigSchema []byte
	//go:embed destination.schema.json
	DestinationConfigSchema []byte
)

// DestinationCapabilities: an epoch is one transaction across topics, and a
// transaction must stay under the brokers' transaction.max.timeout.ms, 15
// min by default, which a 7 min interval's fits.
var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.TransactionalStream,
	Scopes:                []destination.Scope{destination.ScopePipeline},
	DefaultCommitInterval: 10 * time.Second,
	MinCommitInterval:     time.Second,
	MaxCommitInterval:     7 * time.Minute,
	NanosecondTimestamps:  true,
}
