package iceberg

import (
	"math"
	"testing"

	ice "github.com/apache/iceberg-go"
)

// Rows whose partition value is NaN are of one partition, which a file can
// hold alone.
func TestSamePartition(t *testing.T) {
	nan32 := float32(math.NaN())
	for _, c := range []struct {
		a, b any
		want bool
	}{
		{math.NaN(), math.NaN(), true},
		{nan32, nan32, true},
		{1.5, 1.5, true},
		{1.5, 2.5, false},
		{math.NaN(), 1.5, false},
		{float32(1.5), 1.5, false},
		{int32(3), int32(3), true},
		{[]byte("a"), []byte("a"), true},
		{nil, nil, true},
		{nil, int32(0), false},
	} {
		if got := samePartition(c.a, c.b); got != c.want {
			t.Errorf("samePartition(%v, %v) = %v", c.a, c.b, got)
		}
	}
}

type manifest struct {
	ice.ManifestFile
	content         ice.ManifestContent
	spec            int32
	added, existing bool
}

func (m manifest) ManifestContent() ice.ManifestContent { return m.content }
func (m manifest) PartitionSpecID() int32               { return m.spec }
func (m manifest) HasAddedFiles() bool                  { return m.added }
func (m manifest) HasExistingFiles() bool               { return m.existing }

// A manifest of an earlier spec fails a merge only if it lists live data
// files: one of only deleted files, as a rewrite leaves, doesn't.
func TestOlderData(t *testing.T) {
	for _, c := range []struct {
		m    manifest
		want bool
	}{
		{manifest{content: ice.ManifestContentData, spec: 0, added: true}, true},
		{manifest{content: ice.ManifestContentData, spec: 0, existing: true}, true},
		{manifest{content: ice.ManifestContentData, spec: 0}, false},
		{manifest{content: ice.ManifestContentData, spec: 1, added: true}, false},
		{manifest{content: ice.ManifestContentDeletes, spec: 0, added: true}, false},
	} {
		if got := olderData(c.m, 1); got != c.want {
			t.Errorf("%+v: %v", c.m, got)
		}
	}
}
