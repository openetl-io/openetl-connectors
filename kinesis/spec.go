package kinesis

import (
	_ "embed"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

var (
	//go:embed source.schema.json
	SourceConfigSchema []byte
	//go:embed destination.schema.json
	DestinationConfigSchema []byte
)

// DestinationCapabilities: Kinesis has no transactions, so records are
// delivered during the epoch, and PreCommit returns once Kinesis has
// accepted them all. Each writer lane delivers its own, and a key's rows stay
// on one lane, so their order holds; there's nothing to commit together.
var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.DeliverAndAck,
	Scopes:                []destination.Scope{destination.ScopeStream},
	DefaultCommitInterval: 10 * time.Second,
	NanosecondTimestamps:  true,
	WriterLanes:           16,
}
