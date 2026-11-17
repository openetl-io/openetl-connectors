// Package azureblob is the files source and destination of Azure Blob Storage.
package azureblob

import (
	_ "embed"

	"github.com/openetl-io/openetl-connectors/objstore"
	"github.com/openetl-io/openetl-connectors/objstore/azurestore"
)

var (
	//go:embed destination.schema.json
	DestinationConfigSchema []byte
	//go:embed source.schema.json
	SourceConfigSchema []byte
)

func NewSource(config []byte) (*objstore.Source, error) {
	var c azurestore.Config
	return objstore.NewSource(config, &c, store(&c))
}

func NewDestination(config []byte) (*objstore.Destination, error) {
	var c azurestore.Config
	return objstore.NewDestination(config, &c, store(&c))
}

func store(c *azurestore.Config) objstore.NewStore {
	return func(string) (objstore.Store, error) { return azurestore.New(*c) }
}
