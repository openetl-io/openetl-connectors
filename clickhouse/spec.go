package clickhouse

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

// DestinationCapabilities: each stream's epoch becomes visible on its own,
// as a partition moves at once but nothing moves several tables' together.
var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.StageAndApply,
	Scopes:                []destination.Scope{destination.ScopeStream},
	DedupeAtApply:         true,
	DefaultCommitInterval: 10 * time.Second,
	MinCommitInterval:     time.Second,
	// A table's name holds 213 bytes less its database's; this leaves 53
	// for the database's.
	MaxIdentifierLength:  160,
	NanosecondTimestamps: true,
	WriteModes:           []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge},
	WriterLanes:          16,
	// Patches are mutations of the rows that hold the patched versions.
	Holds: destination.Holds{ByKey: true, ByID: true, Last: true, Guarded: true},
}
