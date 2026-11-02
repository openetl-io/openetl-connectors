package iceberg

import (
	_ "embed"
	"time"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

//go:embed destination.schema.json
var DestinationConfigSchema []byte

// DestinationCapabilities: Glue has no multi-table commit, so its pipelines
// need stream scope.
var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.StageAndApply,
	Scopes:                []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
	DefaultCommitInterval: time.Minute,
	MinCommitInterval:     time.Second,
	WriteModes:            []destination.WriteMode{destination.Append, destination.Overwrite},
}
