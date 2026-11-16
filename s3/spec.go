package s3

import (
	_ "embed"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

var (
	//go:embed destination.schema.json
	DestinationConfigSchema []byte
	//go:embed source.schema.json
	SourceConfigSchema []byte
)

// DestinationCapabilities: files appear one at a time as Commit copies them,
// so an epoch is never visible at once across streams.
var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.StageAndApply,
	Scopes:                []destination.Scope{destination.ScopeStream},
	DefaultCommitInterval: time.Minute,
	MinCommitInterval:     time.Second,
	NanosecondTimestamps:  true,
	WriteModes:            []destination.WriteMode{destination.Append, destination.Overwrite},
	WriterLanes:           16,
}
