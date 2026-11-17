// Package gcs is the files source and destination of Google Cloud Storage.
package gcs

import (
	_ "embed"

	"github.com/openetl-io/openetl-connectors/objstore"
	"github.com/openetl-io/openetl-connectors/objstore/gcsstore"
)

var (
	//go:embed destination.schema.json
	DestinationConfigSchema []byte
	//go:embed source.schema.json
	SourceConfigSchema []byte
)

func NewSource(config []byte) (*objstore.Source, error) {
	var c gcsstore.Config
	return objstore.NewSource(config, &c, store(&c))
}

func NewDestination(config []byte) (*objstore.Destination, error) {
	var c gcsstore.Config
	return objstore.NewDestination(config, &c, store(&c))
}

func store(c *gcsstore.Config) objstore.NewStore {
	return func(string) (objstore.Store, error) { return gcsstore.New(*c) }
}
