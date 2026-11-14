package kafka

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

// DestinationCapabilities: each writer lane's part of an epoch is one
// transaction across topics, and a transaction must stay under the brokers'
// transaction.max.timeout.ms, 15 min by default, which a 7 min interval's
// fits.
var DestinationCapabilities = destination.Capabilities{
	Class:                 destination.TransactionalStream,
	Scopes:                []destination.Scope{destination.ScopePipeline},
	DefaultCommitInterval: 10 * time.Second,
	MinCommitInterval:     time.Second,
	MaxCommitInterval:     7 * time.Minute,
	NanosecondTimestamps:  true,
	// As many as a job may have workers. On one pod, lane commits give one.
	WriterLanes: 64,
	LaneCommits: true,
}
