package kinesis

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

// DestinationCapabilities: Kinesis has no transactions, so records are
// delivered during the epoch, and PreCommit returns once Kinesis has
// accepted them all.
var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.DeliverAndAck,
	Scopes:                []destination.Scope{destination.ScopeStream},
	DefaultCommitInterval: 10 * time.Second,
	NanosecondTimestamps:  true,
}
