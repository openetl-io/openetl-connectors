package postgres

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

// What a release persists, in descriptors, table names and labels, stays as
// releases before it wrote it.
func TestPersistedFormats(t *testing.T) {
	desc := staged{Generation: 3, Epoch: 7, Streams: []stagedStream{{
		Name: "s", Table: "t", Staging: "a", Wider: []string{"b"}, Dedupe: true, DedupeEpoch: true, Rows: 9,
		Mode: modeMerge, Key: []string{"id"}, HardDeletes: true, Read: "r", Ended: true, Last: []string{"l"},
		Patch: "p", Fills: map[string]string{"f": "g"}, WiderPatches: []string{"q"},
		Stagings: []string{"x"}, Patches: []string{"y"},
	}}}
	e := destination.Epoch{Generation: 3, N: 7}
	for _, c := range []struct {
		got  any
		want string
	}{
		{desc, `{"generation":3,"epoch":7,"streams":[{"name":"s","table":"t","staging":"a","wider":["b"],"dedupe":true,"dedupe_epoch":true,"rows":9,"mode":"merge","key":["id"],"hard_deletes":true,"read":"r","ended":true,"last":["l"],"patch":"p","fills":{"f":"g"},"wider_patches":["q"]}]}`},
		{stagingLabel{Pipeline: "p", Generation: 3, Epoch: 7}, `{"pipeline":"p","generation":3,"epoch":7}`},
		{readLabel{Pipeline: "p", Stream: "s", Table: "t"}, `{"pipeline":"p","stream":"s","table":"t"}`},
		{[]string{stagingName("p", "s", e, 0), stagingName("p", "s", e, 2), patchName("p", "s", e, 0), readName("p", "s", time.UnixMicro(1000)), indexName("t"), keyIndexName("t")},
			`["_openetl_stage_ced8e3942f116e6017cb2d67","_openetl_stage_f3ef35e747c935d0772bf64c","_openetl_stage_cec0f0e9013c894c4d4b7776","_openetl_read_7878dc485bb14cc50ee5b782","_openetl_id_e3b98a4da31a127d4bde6e43","_openetl_key_e3b98a4da31a127d4bde6e43"]`},
	} {
		b, err := json.Marshal(c.got)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != c.want {
			t.Errorf("%s, want %s", b, c.want)
		}
	}
}

func patchName(pipeline, stream string, e destination.Epoch, lane int) string {
	return stagingName(pipeline, stream+"\x00patch", e, lane)
}
