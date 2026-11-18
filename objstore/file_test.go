//go:build !integration

package objstore

// testStore is the store of file:// URLs, the only one the tests have
// without the integration tag.
func testStore([]byte) (any, NewStore) {
	return &struct{}{}, func(string) (Store, error) { return nil, nil }
}
