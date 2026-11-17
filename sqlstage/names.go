package sqlstage

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// The names of a destination's own tables, which a release keeps as the
// releases before it named them: Recover finds what they staged by them.

// StagingName names a stream's staging of epoch e on a lane.
func StagingName(pipeline, stream string, e destination.Epoch, lane int) string {
	parts := []string{pipeline, stream, strconv.FormatUint(e.Generation, 10), strconv.FormatUint(e.N, 10)}
	if lane > 0 {
		parts = append(parts, strconv.Itoa(lane))
	}
	return HashName("_openetl_stage_", parts...)
}

// ReadName names an overwrite read's table.
func ReadName(pipeline, stream string, start time.Time) string {
	return HashName("_openetl_read_", pipeline, stream, strconv.FormatInt(start.UnixMicro(), 10))
}

func IndexName(table string) string    { return HashName("_openetl_id_", table) }
func KeyIndexName(table string) string { return HashName("_openetl_key_", table) }

// HashName is prefix and, in hex, the first 12 bytes of the SHA-256 of parts
// joined by NULs.
func HashName(prefix string, parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + hex.EncodeToString(h[:12])
}

// StagingLabel labels each staging table, so that Recover can find what
// earlier generations of the pipeline staged.
type StagingLabel struct {
	Pipeline   string `json:"pipeline"`
	Generation uint64 `json:"generation"`
	Epoch      uint64 `json:"epoch"`
}

// ReadLabel labels each read table, so that applies and Recover can find
// reads that were abandoned.
type ReadLabel struct {
	Pipeline string `json:"pipeline"`
	Stream   string `json:"stream"`
	Table    string `json:"table"`
}
