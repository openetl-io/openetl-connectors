package objstore

import "github.com/openetl-io/openetl-connectors/objstore/internal/testfiles"

var (
	parquetOf = testfiles.ParquetOf
	gz        = testfiles.Gz
	zst       = testfiles.Zst
	sz        = testfiles.Sz
	avroOf    = testfiles.AvroOf
	avroEvent = testfiles.AvroEvent
)

const avroSchema = testfiles.AvroSchema

// testDestination and testSource are the destination and the source of
// config, on the store of its URL.
func testDestination(config []byte) (*Destination, error) {
	settings, store := testStore(config)
	return NewDestination(config, settings, store)
}

func testSource(config []byte) (*Source, error) {
	settings, store := testStore(config)
	return NewSource(config, settings, store)
}
