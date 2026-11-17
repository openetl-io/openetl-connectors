// Package s3 is the files source and destination of Amazon S3, S3-compatible
// stores and local filesystems.
package s3

import (
	_ "embed"
	"errors"
	"strings"

	"github.com/openetl-io/openetl-connectors/objstore"
	"github.com/openetl-io/openetl-connectors/objstore/s3store"
)

var (
	//go:embed destination.schema.json
	DestinationConfigSchema []byte
	//go:embed source.schema.json
	SourceConfigSchema []byte
)

func NewSource(config []byte) (*objstore.Source, error) {
	var c s3store.Config
	return objstore.NewSource(config, &c, store(&c))
}

func NewDestination(config []byte) (*objstore.Destination, error) {
	var c s3store.Config
	return objstore.NewDestination(config, &c, store(&c))
}

// store is the S3 store of c, or none for a file:// URL.
func store(c *s3store.Config) objstore.NewStore {
	return func(url string) (objstore.Store, error) {
		if !strings.HasPrefix(url, "file:") {
			return s3store.New(*c)
		}
		if !c.IsZero() {
			return nil, errors.New("s3: endpoint, region, accessKeyId and secretAccessKey are for s3:// URLs")
		}
		return nil, nil
	}
}
