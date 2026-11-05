package postgres

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

var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.StageAndApply,
	Scopes:                []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
	DedupeAtApply:         true,
	DefaultCommitInterval: 10 * time.Second,
	MinCommitInterval:     time.Second,
	MaxIdentifierLength:   63,
	WriteModes:            []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge},
	Holds:                 destination.Holds{ByKey: true, ByID: true, Last: true, Guarded: true},
}
