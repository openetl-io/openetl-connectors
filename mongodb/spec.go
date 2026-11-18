package mongodb

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

var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.StageAndApply,
	Scopes:                []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
	DedupeAtApply:         true,
	DefaultCommitInterval: 10 * time.Second,
	MinCommitInterval:     time.Second,
	// A namespace, database.collection, is at most 255 bytes, and a database
	// name 63; field names have no limit, but share this one.
	MaxIdentifierLength: 120,
	WriteModes:          []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge},
	Holds:               destination.Holds{ByKey: true, ByID: true, Last: true, Guarded: true},
	WriterLanes:         16,
}
