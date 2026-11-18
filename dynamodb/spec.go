package dynamodb

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
	// A table's name, NAMESPACE.TABLE, is at most 255 characters, and a key
	// attribute's name at most 255 bytes.
	MaxIdentifierLength:  120,
	NanosecondTimestamps: true,
	WriteModes:           []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge},
	Holds:                destination.Holds{ByKey: true, ByID: true, Last: true, Guarded: true},
	WriterLanes:          16,
}
