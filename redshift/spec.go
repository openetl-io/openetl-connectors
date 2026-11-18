package redshift

import (
	_ "embed"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

//go:embed destination.schema.json
var DestinationConfigSchema []byte

// Each epoch's PreCommit uploads a file per stream and lane and runs a COPY
// of it, which takes seconds however few its rows, so epochs are longer than
// the row-at-a-time databases' and lanes fewer.
var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.StageAndApply,
	Scopes:                []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
	DedupeAtApply:         true,
	DefaultCommitInterval: time.Minute,
	MinCommitInterval:     10 * time.Second,
	MaxIdentifierLength:   127,
	WriteModes:            []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge},
	Holds:                 destination.Holds{ByKey: true, ByID: true, Last: true, Guarded: true},
	WriterLanes:           4,
}
